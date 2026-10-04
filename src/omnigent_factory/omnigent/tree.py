"""Recursive stage-tree scan (architecture §5.3).

A stage session is its root plus every recursive descendant. The SDK's
``child_sessions_tree`` is insufficient for safety (one unpaged page per parent, depth
cap, root excluded from ``subtree_busy``, archived children hidden), so a scan:

1. walks ``GET /v1/sessions/{id}/child_sessions`` for every node, all pages, with a
   visited set (cycle guard);
2. supplements it with the archive-inclusive inventory
   ``GET /v1/sessions?kind=any&include_archived=true&visibility=all`` (all pages, no agent
   or project filter) and closes descendants over ``parent_session_id`` - archived
   intermediate parents and their children included;
3. retains previously known node IDs even when a page omits them;
4. reads every node's current snapshot, root included.

Any failed read or the node ceiling makes the scan incomplete: quiescence is then
unknown, never idle. Archived does not mean quiescent.

Busy (work running or able to relaunch): ``launching``; ``running``/``waiting`` unless the
node is parked on a native prompt; non-terminal background tasks while a turn may be live;
queued native pending inputs; a non-terminal latest task. A node parked on an elicitation
is a *waiter*, not busy by itself - but its running siblings/children still are.

The server never refreshes a node's ``background_tasks`` once its turn ends: exited shells
stay ``running`` in the snapshot forever, even after a later turn or archiving (#627). So
an ``idle`` node with no active task (terminal or none) does not count them.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from omnigent_client import TERMINAL_TASK_STATUSES

from omnigent_factory.omnigent.rest import OmnigentReadError, OmnigentRest
from omnigent_factory.ports.omnigent import TreeScan

DEFAULT_MAX_NODES = 500
_BACKGROUND_TERMINAL = frozenset({"completed", "failed", "cancelled", "killed", "exited"})


@dataclass(frozen=True, slots=True)
class NodeState:
    node_id: str
    parent_id: str | None
    status: str | None
    archived: bool = False
    #: Prompts this session itself owns (no ``params.target_session_id``, or it names us).
    elicitations: tuple[Mapping[str, Any], ...] = ()
    #: Descendant prompts the server mirrors into this snapshot, tagged with
    #: ``params.target_session_id`` (helpers.py:1584-1624 @1c0153aa). Never ours.
    mirrored: tuple[Mapping[str, Any], ...] = ()
    pending_inputs: tuple[Mapping[str, Any], ...] = ()
    background_active: bool = False
    task_status: str | None = None
    total_cost_usd: float | None = None
    items_hint: int = 0

    @property
    def elicitation_ids(self) -> tuple[str, ...]:
        return tuple(
            e["elicitation_id"]
            for e in self.elicitations
            if isinstance(e.get("elicitation_id"), str)
        )

    @property
    def parked(self) -> bool:
        return bool(self.elicitations)

    @property
    def turn_active(self) -> bool:
        """A turn may be live (anything but ``idle``, or a non-terminal task)."""
        return self.status != "idle" or (
            self.task_status is not None and self.task_status not in TERMINAL_TASK_STATUSES
        )

    @property
    def background_live(self) -> bool:
        """Background tasks count only while a turn may be live (see module docstring)."""
        return self.background_active and self.turn_active

    @property
    def busy(self) -> bool:
        if self.status == "launching" or self.background_live or self.pending_inputs:
            return True
        if self.status in ("running", "waiting") and not self.parked:
            return True
        return (
            self.task_status is not None
            and self.task_status not in TERMINAL_TASK_STATUSES
            and not self.parked
        )

    @property
    def productive(self) -> bool:
        """Counts toward active time (measured lower bound)."""
        return self.background_live or (self.status == "running" and not self.parked)

    @property
    def maybe_productive(self) -> bool:
        """Counts toward the conservative upper bound (provisioning, async waits)."""
        return self.productive or (self.busy and not self.parked)


@dataclass(frozen=True, slots=True)
class TreeObservation:
    root_id: str
    complete: bool
    nodes: Mapping[str, NodeState]
    errors: tuple[str, ...] = ()

    @property
    def busy(self) -> bool:
        return any(n.busy for n in self.nodes.values())

    @property
    def pending_waiter(self) -> bool:
        return bool(self.owned_elicitations())

    @property
    def quiescent(self) -> bool:
        return self.complete and not self.busy

    @property
    def root(self) -> NodeState | None:
        return self.nodes.get(self.root_id)

    def node_with_elicitation(self, elicitation_id: str) -> NodeState | None:
        """The session that *owns* ``elicitation_id`` (never an ancestor's mirror)."""
        for node in self.nodes.values():
            if elicitation_id in node.elicitation_ids:
                return node
        for node in self.nodes.values():
            for event in node.mirrored:
                if event.get("elicitation_id") == elicitation_id:
                    owner = self.nodes.get(target_session_of(event) or "")
                    if owner is not None:
                        return owner
        return None

    def owned_elicitations(self) -> dict[str, tuple[str, Mapping[str, Any]]]:
        """``elicitation_id -> (owner node, event)``, deduplicated by true owner."""
        out: dict[str, tuple[str, Mapping[str, Any]]] = {}
        for node in self.nodes.values():
            for event in node.elicitations:
                eid = event.get("elicitation_id")
                if isinstance(eid, str):
                    out[eid] = (node.node_id, event)
        for node in self.nodes.values():
            for event in node.mirrored:
                eid, owner = event.get("elicitation_id"), target_session_of(event)
                if isinstance(eid, str) and eid not in out and owner in self.nodes:
                    out[eid] = (str(owner), event)
        return out

    def to_scan(self) -> TreeScan:
        return TreeScan(
            root_id=self.root_id,
            complete=self.complete,
            busy=self.busy,
            pending_waiter=self.pending_waiter,
            node_ids=frozenset(self.nodes),
        )


def _background_active(tasks: object) -> bool:
    if not isinstance(tasks, list):
        return False
    for t in tasks:
        status = t.get("status") if isinstance(t, dict) else None
        if status is None or str(status) not in _BACKGROUND_TERMINAL:
            return True
    return False


def target_session_of(event: Mapping[str, Any]) -> str | None:
    params = event.get("params")
    target = params.get("target_session_id") if isinstance(params, dict) else None
    return target if isinstance(target, str) and target else None


def node_from_snapshot(
    node_id: str,
    parent_id: str | None,
    snap: Mapping[str, Any],
    summary: Mapping[str, Any] | None = None,
) -> NodeState:
    events = [e for e in snap.get("pending_elicitations") or () if isinstance(e, dict)]
    elicitations = tuple(e for e in events if target_session_of(e) in (None, node_id))
    mirrored = tuple(e for e in events if target_session_of(e) not in (None, node_id))
    inputs = tuple(p for p in snap.get("pending_inputs") or () if isinstance(p, dict))
    task_status = summary.get("current_task_status") if summary else None
    count = snap.get("background_task_count")
    background = _background_active(snap.get("background_tasks")) or (
        isinstance(count, int) and count > 0 and snap.get("background_tasks") is None
    )
    cost = snap.get("total_cost_usd")
    items = snap.get("items")
    return NodeState(
        node_id=node_id,
        parent_id=parent_id if parent_id is not None else snap.get("parent_session_id"),
        status=snap.get("status") if isinstance(snap.get("status"), str) else None,
        archived=bool(snap.get("archived")),
        elicitations=elicitations,
        mirrored=mirrored,
        pending_inputs=inputs,
        background_active=background,
        task_status=task_status if isinstance(task_status, str) else None,
        total_cost_usd=float(cost) if isinstance(cost, int | float) else None,
        items_hint=len(items) if isinstance(items, list) else 0,
    )


@dataclass
class _Walk:
    parents: dict[str, str | None] = field(default_factory=dict)
    summaries: dict[str, Mapping[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


async def scan_tree(
    rest: OmnigentRest,
    root_id: str,
    *,
    known_ids: Iterable[str] = (),
    max_nodes: int = DEFAULT_MAX_NODES,
) -> TreeObservation:
    walk = _Walk(parents={root_id: None})
    await _walk_children(rest, root_id, walk, max_nodes)
    await _close_over_inventory(rest, walk, max_nodes)
    for kid in known_ids:
        if kid not in walk.parents:
            walk.parents[kid] = None  # retained: parent resolved from its snapshot
    nodes: dict[str, NodeState] = {}
    pending = list(walk.parents.items())
    attempted: set[str] = set()
    while pending:
        node_id, parent = pending.pop(0)
        if node_id in attempted:
            continue
        if len(attempted) >= max_nodes:
            walk.errors.append("node-ceiling")
            break
        attempted.add(node_id)
        try:
            snap = await rest.get_json(f"/v1/sessions/{node_id}")
        except OmnigentReadError as exc:
            walk.errors.append(f"snapshot {node_id}: {exc.reason}")
            continue
        node = node_from_snapshot(node_id, parent, snap, walk.summaries.get(node_id))
        nodes[node_id] = node
        # A mirrored prompt names its owning session: that is discovered tree evidence
        # (e.g. a child created after enumeration). Read it too, and close over whatever
        # it mirrors in turn; never let an unread owner's prompt vanish from the scan.
        for event in node.mirrored:
            target = target_session_of(event)
            if target is not None and target not in attempted and target not in walk.parents:
                walk.parents[target] = None
                pending.append((target, None))
    for event_owner in _unread_mirror_owners(nodes):
        walk.errors.append(f"mirrored prompt owner unread: {event_owner}")
    return TreeObservation(root_id, not walk.errors, nodes, tuple(walk.errors))


def _unread_mirror_owners(nodes: Mapping[str, NodeState]) -> list[str]:
    owners = {
        target
        for node in nodes.values()
        for event in node.mirrored
        if (target := target_session_of(event)) is not None and target not in nodes
    }
    return sorted(owners)


async def _walk_children(rest: OmnigentRest, root_id: str, walk: _Walk, max_nodes: int) -> None:
    frontier = [root_id]
    visited: set[str] = set()
    while frontier:
        parent = frontier.pop(0)
        if parent in visited:
            continue
        visited.add(parent)
        if len(walk.parents) > max_nodes:
            return
        try:
            rows = await rest.paginate(f"/v1/sessions/{parent}/child_sessions", {"order": "asc"})
        except OmnigentReadError as exc:
            walk.errors.append(f"children {parent}: {exc.reason}")
            continue
        for row in rows:
            child = row.get("id")
            if not isinstance(child, str) or child in walk.parents:
                continue
            walk.parents[child] = parent
            walk.summaries[child] = row
            frontier.append(child)


async def _close_over_inventory(rest: OmnigentRest, walk: _Walk, max_nodes: int) -> None:
    try:
        rows = await rest.paginate(
            "/v1/sessions",
            {"kind": "any", "include_archived": "true", "visibility": "all", "order": "asc"},
        )
    except OmnigentReadError as exc:
        walk.errors.append(f"inventory: {exc.reason}")
        return
    by_parent: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        sid, parent = row.get("id"), row.get("parent_session_id")
        if isinstance(sid, str) and isinstance(parent, str):
            by_parent[parent].append(sid)
    stack = list(walk.parents)
    while stack and len(walk.parents) <= max_nodes:
        current = stack.pop()
        for child in by_parent.get(current, ()):
            if child not in walk.parents:
                walk.parents[child] = current
                stack.append(child)
