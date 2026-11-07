"""Recursive stage-tree scan (architecture §5.3).

A stage session is its root plus every recursive descendant. The SDK's
``child_sessions_tree`` is insufficient for safety (one unpaged page per parent, depth
cap, root excluded from ``subtree_busy``, archived children hidden), so a scan:

1. lists the whole (non-archived) subtree with one paged
   ``GET /v1/sessions/{root}/child_sessions?descendants=true`` (:func:`list_subtree`);
   a server without it answers with the root's direct children only (rows without
   ``depth``), and then every further node's ``child_sessions`` is walked, all pages,
   with a visited set (cycle guard), as before;
2. supplements it with the archive-inclusive inventory of every session
   (:class:`~omnigent_factory.omnigent.inventory.SessionIndex`, refreshed incrementally
   by a read that starts after the scan does) and closes descendants over
   ``parent_session_id`` - archived intermediate parents and their children included;
3. retains previously known node IDs even when a page omits them;
4. reads the current snapshot of the root and of every node whose listing row changed
   since it was last read (or that has no row, was busy or holds a prompt); an idle
   node whose row is unchanged keeps its cached state (:func:`finish_scan`).

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

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from omnigent_client import TERMINAL_TASK_STATUSES

from omnigent_factory.omnigent.inventory import SessionIndex
from omnigent_factory.omnigent.rest import (
    LITE_NODE_SNAPSHOT,
    LITE_SNAPSHOT,
    OmnigentReadError,
    OmnigentRest,
)
from omnigent_factory.ports.omnigent import TreeScan

DEFAULT_MAX_NODES = 500
_BACKGROUND_TERMINAL = frozenset({"completed", "failed", "cancelled", "killed", "exited"})


def snapshot_params(node_id: str, root_id: str) -> Mapping[str, str]:
    """Every node is read lite (no items, no liveness); only the root keeps its usage
    (the subtree-inclusive cost the observer samples)."""
    return LITE_SNAPSHOT if node_id == root_id else LITE_NODE_SNAPSHOT


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
    #: ``len(items)`` of a full snapshot; 0 from the lite reads every scan now makes
    #: (it only fed the unchanged-tree fingerprint; ``updated_at`` carries activity).
    items_hint: int = 0
    #: The session's ``updated_at`` (UTC microseconds; 0 when the server sent none): the
    #: node's last *conversation* activity. The server advances it only when a write
    #: inserts at least one new item that is not a ``resource_event``; a pure stable-id
    #: duplicate re-post and server bookkeeping (``resource_event``, e.g. a sub-agent
    #: spawn marker) leave it unchanged (``sqlalchemy_store.py:2981-2987,3041-3048``
    #: @42e17e47). So it is stall progress, never evidence of topology.
    updated_at_us: int = 0
    #: ``last_task_error.code`` of its latest turn (e.g. ``runner_failed_to_start``).
    last_error: str | None = None
    #: :func:`row_signature` of its child-list row when its snapshot was read after that
    #: row (None: read without one, or the root): a later row that differs means the
    #: node changed and is read again; an equal one lets an idle node keep this state.
    row: tuple[object, ...] | None = None

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
    #: Remembered node IDs (from earlier scans only) the server no longer has (404):
    #: deleted sessions, forgotten by the caller rather than retried forever.
    gone: frozenset[str] = frozenset()
    #: Nodes this walk or the inventory listed whose snapshot was not found (404): an
    #: error here (the scan stays incomplete); the inventory may list a deleted session
    #: until its next full read.
    not_found: frozenset[str] = frozenset()

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

    @property
    def last_activity_us(self) -> int:
        """The newest ``updated_at`` anywhere in the tree (0: none reported)."""
        return max((n.updated_at_us for n in self.nodes.values()), default=0)

    @property
    def workers(self) -> tuple[NodeState, ...]:
        """Every node but the root, in node-id order."""
        return tuple(n for nid, n in sorted(self.nodes.items()) if nid != self.root_id)

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


#: ``ChildSessionSummary`` fields that change when the session does (schemas.py:834-939
#: @42e17e47): activity (``updated_at``), the latest task and its status, the live-loop
#: flag, labels (archive, routing...), the failure detail and open prompts. A preview-only
#: change (``last_message_preview``) is not a change of state.
ROW_FIELDS = (
    "updated_at",
    "current_task_id",
    "current_task_status",
    "busy",
    "labels",
    "last_task_error",
    "pending_elicitations_count",
)


def _frozen(value: object) -> object:
    if isinstance(value, Mapping):
        return tuple(sorted((str(k), _frozen(v)) for k, v in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_frozen(v) for v in value)
    return value


def row_signature(row: Mapping[str, Any]) -> tuple[object, ...]:
    """What a child-list row says about its session's state (see :data:`ROW_FIELDS`)."""
    return tuple(_frozen(row.get(f)) for f in ROW_FIELDS)


#: The only child-row task status that is lifecycle evidence of an idle session. The
#: server derives ``busy``/``current_task_status`` from its process-local status cache
#: (helpers.py:11470-11483,11577-11583 @42e17e47): ``completed`` only from a cache hit of
#: ``idle``, with no failure label; and on a hit the lite snapshot reports that same cached
#: status (orchestration.py:14073-14092), so reading it would prove nothing more. A cache
#: miss gives ``busy=false`` and a null status whatever the persisted lifecycle is, and a
#: status change does not move ``updated_at`` (sqlalchemy_store.py:4438-4443): null is
#: unknown. ``failed`` is forced by the failure label even on a miss (a later run can be
#: live under it), and ``in_progress`` is busy: neither certifies anything.
IDLE_TASK_STATUS = "completed"


def row_unchanged(previous: NodeState | None, row: Mapping[str, Any] | None) -> bool:
    """``previous`` (a cached node) is still current by its listing ``row``: the row equals
    the one it was read with, the row and the cached node both show an idle, completed
    turn (:data:`IDLE_TASK_STATUS`: positive lifecycle evidence, never a cache-miss
    false/null), and the node was neither busy nor holding or mirroring a prompt (a prompt
    is not in the row; busy must be proven over). Anything else is unknown, and unknown
    forces a read."""
    return (
        previous is not None
        and row is not None
        and previous.row is not None
        and previous.row == row_signature(row)
        and row.get("current_task_status") == IDLE_TASK_STATUS
        and row.get("busy") is False
        and previous.task_status == IDLE_TASK_STATUS
        and previous.status == "idle"
        and not previous.busy
        and not previous.elicitations
        and not previous.mirrored
        and not previous.pending_inputs
        and not row.get("pending_elicitations_count")
    )


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
    updated = epoch_us(snap.get("updated_at"))
    if not updated and summary is not None:
        updated = epoch_us(summary.get("updated_at"))
    error = snap.get("last_task_error")
    code = error.get("code") if isinstance(error, dict) else None
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
        updated_at_us=updated,
        last_error=code if isinstance(code, str) else None,
        row=row_signature(summary) if summary is not None else None,
    )


def root_prompts(root_id: str, snap: Mapping[str, Any]) -> dict[str, tuple[str, Mapping[str, Any]]]:
    """``elicitation_id -> (owner node, event)`` from the root's snapshot alone: its own
    prompts and those Omnigent mirrors from every descendant (``target_session_id``)."""
    node = node_from_snapshot(root_id, None, snap)
    out: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for event in (*node.elicitations, *node.mirrored):
        eid = event.get("elicitation_id")
        if isinstance(eid, str) and eid not in out:
            out[eid] = (target_session_of(event) or root_id, event)
    return out


def epoch_us(value: object) -> int:
    """An Omnigent timestamp (epoch seconds, possibly fractional, or ISO 8601) in UTC
    microseconds; 0 when absent or unreadable."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int | float):
        seconds = float(value)
        if seconds > 1e14:  # already microseconds
            return int(seconds)
        if seconds > 1e11:  # milliseconds
            return int(seconds * 1_000)
        return int(seconds * 1_000_000) if seconds > 0 else 0
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return 0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp() * 1_000_000)
    return 0


#: ``max_depth`` of a ``?descendants=true`` listing: the server's cap
#: (``_MAX_DESCENDANT_DEPTH``, routes_items.py:57-60 @42e17e47; default 3). A node at this
#: depth may have children the listing omits: they are walked per parent.
DESCENDANT_DEPTH = 10
#: Rows per page of a subtree listing (the route's ``limit`` maximum).
DESCENDANT_PAGE = 1000
#: Statuses with which a server rejects the subtree parameters themselves (a stricter or
#: different build): the scan then walks every parent, as against a server without them.
_LISTING_REJECTED = frozenset({400, 422})


@dataclass(frozen=True, slots=True)
class Subtree:
    """One ``child_sessions?descendants=true`` listing of a root (:func:`list_subtree`)."""

    #: Every row, breadth-first: the whole non-archived subtree, or (``whole`` False) the
    #: root's direct children only.
    rows: tuple[Mapping[str, Any], ...]
    #: Each listed node's parent.
    parents: Mapping[str, str]
    #: Nodes whose every non-archived child is in ``rows``: the root, and (``whole``) each
    #: listed node above the depth cap. Any other node's children are listed per parent.
    covered: frozenset[str]
    #: The server returned the subtree (every row carries ``depth``).
    whole: bool


async def list_subtree(rest: OmnigentRest, root_id: str) -> Subtree:
    """The non-archived subtree of ``root_id`` in one paged read.

    ``GET /v1/sessions/{root}/child_sessions?descendants=true&max_depth=10``
    (routes_items.py:139-310, helpers in orchestration.py:12878 @42e17e47): rows
    breadth-first, each ``ChildSessionSummary`` with ``parent_session_id`` set to its own
    parent and ``depth`` set; an archived node and its subtree are omitted (as from a
    per-parent walk); ``limit``/``after`` page it. A server without the flag, or a spawn
    tree over 2000 sub-agents, answers with the root's direct children (no ``depth``):
    then only the root is ``covered`` and the caller walks the rest. An empty listing is
    complete either way (the root has no non-archived child). Raises
    :class:`OmnigentReadError`.
    """
    rows = await rest.paginate(
        f"/v1/sessions/{root_id}/child_sessions",
        {
            "descendants": "true",
            "max_depth": DESCENDANT_DEPTH,
            "order": "asc",  # the one-level fallback's order (ignored for a subtree)
            "limit": DESCENDANT_PAGE,
        },
    )
    listed = [r for r in rows if isinstance(r.get("id"), str)]
    whole = all(
        isinstance(r.get("depth"), int)
        and not isinstance(r.get("depth"), bool)
        and isinstance(r.get("parent_session_id"), str)
        for r in listed
    )
    parents: dict[str, str] = {}
    for row in listed:
        parent = row.get("parent_session_id")
        parents[str(row["id"])] = parent if whole and isinstance(parent, str) else root_id
    covered = {root_id}
    if whole:
        covered.update(str(r["id"]) for r in listed if int(r["depth"]) < DESCENDANT_DEPTH)
    return Subtree(tuple(listed), parents, frozenset(covered), whole and bool(listed))


@dataclass
class ScanWalk:
    parents: dict[str, str | None] = field(default_factory=dict)
    summaries: dict[str, Mapping[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


async def scan_tree(
    rest: OmnigentRest,
    root_id: str,
    *,
    known_ids: Iterable[str] = (),
    max_nodes: int = DEFAULT_MAX_NODES,
    index: SessionIndex | None = None,
    cached: TreeObservation | None = None,
) -> TreeObservation:
    """Scan ``root_id``'s tree; ``index`` is shared across scans (a one-off otherwise);
    ``cached`` (its previous observation) spares reads of unchanged idle nodes."""
    walk = await begin_scan(rest, root_id, max_nodes=max_nodes)
    await _close_over_inventory(index or SessionIndex(rest), walk, max_nodes)
    return await finish_scan(
        rest, root_id, walk, known_ids=known_ids, max_nodes=max_nodes, cached=cached
    )


async def begin_scan(rest: OmnigentRest, root_id: str, *, max_nodes: int) -> ScanWalk:
    """Step 1 of a scan: list the (non-archived) subtree of ``root_id`` (one listing where
    the server supports it, else a walk of every child list).

    Steps 2-3 (:func:`close_over_index`, :func:`finish_scan`) must follow with an
    inventory refreshed by a read that started after this walk: several scans may share
    one such refresh (an observer pass walks every due tree, refreshes once, then
    finishes each scan).
    """
    walk = ScanWalk(parents={root_id: None})
    await _walk_children(rest, root_id, walk, max_nodes)
    return walk


def close_over_index(
    index: SessionIndex, walk: ScanWalk, max_nodes: int, inventory_error: str | None
) -> None:
    """Step 2: close the walk over the refreshed inventory (or record its failure, which
    makes the scan incomplete: never idle)."""
    if inventory_error is not None:
        walk.errors.append(f"inventory: {inventory_error}")
        return
    _close_over_index(index, walk, max_nodes)


async def finish_scan(
    rest: OmnigentRest,
    root_id: str,
    walk: ScanWalk,
    *,
    known_ids: Iterable[str] = (),
    max_nodes: int = DEFAULT_MAX_NODES,
    cached: TreeObservation | None = None,
) -> TreeObservation:
    """Step 3: read the current snapshot of the root and of every node not proven current.

    With ``cached`` (this root's previous observation), a listed node whose row is
    unchanged since it was read and that was idle and prompt-free keeps its cached state
    (:func:`row_unchanged`), refreshed with the row's task status; every other node, the
    root, nodes known only from the inventory or earlier scans (no row) included, is read.

    A node known only from earlier scans (absent from this walk and the inventory) whose
    snapshot is not found was deleted: it is reported ``gone``, not an error, so one
    deleted descendant never keeps every later scan incomplete (#889). Any other failed
    read (a node this walk found, the root, a 5xx, a timeout, auth) stays an error.
    """
    remembered: set[str] = set()
    for kid in known_ids:
        if kid not in walk.parents:
            walk.parents[kid] = None  # retained: parent resolved from its snapshot
            remembered.add(kid)
    remembered.discard(root_id)
    gone: set[str] = set()
    not_found: set[str] = set()
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
        summary = walk.summaries.get(node_id)
        previous = cached.nodes.get(node_id) if cached is not None else None
        if node_id != root_id and summary is not None and row_unchanged(previous, summary):
            assert previous is not None  # noqa: S101 - row_unchanged proved it
            status = summary.get("current_task_status")
            nodes[node_id] = replace(
                previous,
                parent_id=parent if parent is not None else previous.parent_id,
                task_status=status if isinstance(status, str) else None,
            )
            continue
        try:
            snap = await rest.get_json(f"/v1/sessions/{node_id}", snapshot_params(node_id, root_id))
        except OmnigentReadError as exc:
            if exc.status == 404 and node_id in remembered:
                gone.add(node_id)
                continue
            if exc.status == 404:
                not_found.add(node_id)
            walk.errors.append(f"snapshot {node_id}: {exc.reason}")
            continue
        node = node_from_snapshot(node_id, parent, snap, summary)
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
    return TreeObservation(
        root_id,
        not walk.errors,
        nodes,
        tuple(walk.errors),
        gone=frozenset(gone),
        not_found=frozenset(not_found),
    )


def _unread_mirror_owners(nodes: Mapping[str, NodeState]) -> list[str]:
    owners = {
        target
        for node in nodes.values()
        for event in node.mirrored
        if (target := target_session_of(event)) is not None and target not in nodes
    }
    return sorted(owners)


async def _walk_children(rest: OmnigentRest, root_id: str, walk: ScanWalk, max_nodes: int) -> None:
    """One subtree listing; a per-parent walk only below what it does not cover."""
    frontier = [root_id]
    visited: set[str] = set()
    try:
        subtree: Subtree | None = await list_subtree(rest, root_id)
    except OmnigentReadError as exc:
        if exc.status not in _LISTING_REJECTED:
            walk.errors.append(f"children {root_id}: {exc.reason}")
            return
        subtree = None  # the parameters are refused: walk every parent
    if subtree is not None:
        for listed in subtree.rows:
            node = str(listed["id"])
            if node in walk.parents or len(walk.parents) > max_nodes:
                continue
            walk.parents[node] = subtree.parents[node]
            walk.summaries[node] = listed
        visited = set(subtree.covered)
        frontier = [n for n in walk.parents if n not in visited]
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


async def _close_over_inventory(index: SessionIndex, walk: ScanWalk, max_nodes: int) -> None:
    try:
        await index.refresh()
    except OmnigentReadError as exc:
        walk.errors.append(f"inventory: {exc.reason}")
        return
    _close_over_index(index, walk, max_nodes)


def _close_over_index(index: SessionIndex, walk: ScanWalk, max_nodes: int) -> None:
    stack = list(walk.parents)
    while stack and len(walk.parents) <= max_nodes:
        current = stack.pop()
        for child in index.children(current):
            if child not in walk.parents:
                walk.parents[child] = current
                stack.append(child)


@dataclass(frozen=True, slots=True)
class TreeRefresh:
    """The result of :func:`refresh_tree`."""

    observation: TreeObservation
    #: Nodes whose snapshot this refresh read successfully.
    read: frozenset[str]
    #: Whether it confirmed the tree (``confirm``): every previously busy, unknown and
    #: dirty node re-read, their task statuses re-listed, the inventory refreshed.
    confirmed: bool = False


#: Session statuses in which a turn is live. The server's public vocabulary is
#: ``idle | running | waiting | failed`` for a snapshot or list row
#: (``SessionResponse.status``, ``SessionListItem.status``: ``schemas.py:2450,3217``) plus
#: ``launching`` on the stream (``SessionStatusEvent.status``, ``schemas.py:3571``)
#: @42e17e47. Any other (``idle``, ``failed``, absent or unknown) says the turn is over.
LIVE_SESSION_STATUSES = frozenset({"launching", "running", "waiting"})


def _unresolved_task(node: NodeState) -> bool:
    """A cached live task status on a node whose fresh snapshot says its turn is not live
    (any status outside :data:`LIVE_SESSION_STATUSES`): not proof either way until its
    parent's child list is re-read."""
    return (
        node.status not in LIVE_SESSION_STATUSES
        and node.task_status is not None
        and node.task_status not in TERMINAL_TASK_STATUSES
    )


async def refresh_tree(
    rest: OmnigentRest,
    cached: TreeObservation,
    dirty: Iterable[str],
    *,
    spawned: Iterable[str] = (),
    confirm: bool = False,
    confirm_busy: Iterable[str] = (),
    topology: bool = False,
    index: SessionIndex | None = None,
    inventory_ready: bool = False,
    inventory_error: str | None = None,
    max_nodes: int = DEFAULT_MAX_NODES,
) -> TreeRefresh:
    """Re-read what is not proven fresh in a complete cached scan.

    The rule: anything no read in this refresh proved current is unknown, and unknown
    forces a read.

    * every ``dirty`` node gets one lite snapshot read; a node not in the cache is
      *unknown*: it is read and its children listed;
    * every ``spawned`` node (a feed spawn hint names the parent) has its children
      listed; new children are unknown nodes;
    * a node read without a fresh child-list row whose cached task status is still live
      while its snapshot is idle is unresolved: its parent's child list (the only source
      of ``current_task_status``) is re-read in the same refresh; a child the list no
      longer shows (archived) gets no task status, as in a full scan;
    * a mirrored prompt naming an unread owner reads that owner (as a full scan does);
    * ``topology`` (a channel that cannot announce spawns): the root is re-read, its
      children listed and the node set closed over the archive-inclusive inventory;
    * ``confirm`` (before the tree may be declared idle) additionally re-reads the root,
      every cached busy node and every node in ``confirm_busy`` (busy in the last emitted
      state), lists the children of those nodes (they ran: they may have spawned) and of
      their parents, and closes over the inventory.

    The inventory is refreshed here unless the caller already did (``inventory_ready``,
    or ``inventory_error`` when that failed). Any failed read makes the result
    incomplete (never idle); the caller then falls back to a full scan. ``cached`` must
    be complete.
    """
    root_id = cached.root_id
    nodes: dict[str, NodeState] = dict(cached.nodes)
    parents: dict[str, str | None] = {nid: n.parent_id for nid, n in nodes.items()}
    errors: list[str] = []
    summaries: dict[str, Mapping[str, Any]] = {}
    reads: list[str] = list(dirty)
    lists: list[str] = list(spawned)
    if topology:
        reads.append(root_id)
        lists.append(root_id)
    if confirm:
        ran = [*confirm_busy, *(n.node_id for n in nodes.values() if n.busy)]
        reads.extend(ran)
        reads.append(root_id)
        lists.extend(ran)
        lists.extend(p for nid in reads if (p := parents.get(nid)) is not None)
    lists.extend(nid for nid in reads if nid not in nodes)  # unknown: list its children
    if topology or confirm:
        inventory = index or SessionIndex(rest)
        failure = inventory_error
        if failure is None and not inventory_ready:
            try:
                await inventory.refresh()
            except OmnigentReadError as exc:
                failure = exc.reason
        if failure is not None:
            errors.append(f"inventory: {failure}")
        else:
            stack = list(parents)
            while stack and len(parents) <= max_nodes:
                current = stack.pop()
                for child in inventory.children(current):
                    if child not in parents:
                        parents[child] = current
                        reads.append(child)
                        lists.append(child)
                        stack.append(child)
    listed: set[str] = set()
    read_ok: set[str] = set()
    attempted: set[str] = set()
    #: Read nodes whose task status came from the cache, not a fresh child-list row.
    carried: set[str] = set()
    #: Whether this refresh listed the subtree (once: it re-lists every covered parent).
    subtree_listed = False
    dirty_set = set(reads)
    while lists or reads:
        while lists:
            parent = lists.pop(0)
            if parent in listed:
                continue
            if not subtree_listed:
                # Any child list needed: the whole subtree in one listing, which re-lists
                # every covered parent at once; a changed row is read (it is a change).
                subtree_listed = True
                try:
                    subtree: Subtree | None = await list_subtree(rest, root_id)
                except OmnigentReadError as exc:
                    subtree = None
                    if exc.status not in _LISTING_REJECTED:
                        errors.append(f"children {root_id}: {exc.reason}")
                        listed.update(parents)  # incomplete anyway: list nothing more
                        listed.add(parent)
                        lists.clear()
                        continue
                if subtree is not None:
                    for listed_row in subtree.rows:
                        node_id = str(listed_row["id"])
                        summaries[node_id] = listed_row
                        if node_id not in parents:
                            parents[node_id] = subtree.parents[node_id]
                            reads.append(node_id)
                            lists.append(node_id)
                        elif node_id not in dirty_set and node_id not in reads:
                            known = nodes.get(node_id)
                            if known is None or known.row != row_signature(listed_row):
                                reads.append(node_id)
                    listed.update(subtree.covered)
                    if parent in listed:
                        continue
            listed.add(parent)
            try:
                rows = await rest.paginate(
                    f"/v1/sessions/{parent}/child_sessions", {"order": "asc"}
                )
            except OmnigentReadError as exc:
                errors.append(f"children {parent}: {exc.reason}")
                continue
            for row in rows:
                kid = row.get("id")
                if not isinstance(kid, str):
                    continue
                summaries[kid] = row
                if kid not in parents:
                    parents[kid] = parent
                    reads.append(kid)
                    lists.append(kid)
        if not reads:
            break
        node_id = reads.pop(0)
        if node_id in attempted:
            continue
        if len(attempted) >= max_nodes or len(parents) > max_nodes:
            errors.append("node-ceiling")
            break
        attempted.add(node_id)
        try:
            snap = await rest.get_json(f"/v1/sessions/{node_id}", snapshot_params(node_id, root_id))
        except OmnigentReadError as exc:
            errors.append(f"snapshot {node_id}: {exc.reason}")
            continue
        node = node_from_snapshot(node_id, parents.get(node_id), snap, summaries.get(node_id))
        previous = nodes.get(node_id)
        if node_id not in summaries and previous is not None:
            # Its last row still describes it: a later equal row means no change since.
            node = replace(node, task_status=previous.task_status, row=previous.row)
            carried.add(node_id)
            if _unresolved_task(node) and node.parent_id is not None:
                lists.append(node.parent_id)  # unknown task status: re-list it now
        nodes[node_id] = node
        parents[node_id] = node.parent_id
        read_ok.add(node_id)
        for event in node.mirrored:
            target = target_session_of(event)
            if target is not None and target not in nodes and target not in attempted:
                parents.setdefault(target, None)
                reads.append(target)
                lists.append(target)
    for nid, known in list(nodes.items()):
        summary = summaries.get(nid)
        if summary is not None and (nid not in read_ok or nid in carried):
            # A fresh child-list row refreshes the task status of a node not re-read, or
            # of one read without its row.
            status = summary.get("current_task_status")
            nodes[nid] = replace(known, task_status=status if isinstance(status, str) else None)
        elif (
            summary is None
            and nid in carried
            and _unresolved_task(known)
            and known.parent_id in listed
        ):
            # Its parent's (non-archived) child list no longer shows it: archived. A full
            # scan reads such a node without a task status; so does this refresh.
            nodes[nid] = replace(known, task_status=None)
    errors.extend(f"mirrored prompt owner unread: {o}" for o in _unread_mirror_owners(nodes))
    complete = cached.complete and not errors
    return TreeRefresh(
        TreeObservation(root_id, complete, nodes, tuple(errors)),
        frozenset(read_ok),
        confirmed=confirm and complete,
    )
