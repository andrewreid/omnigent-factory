"""Change-driven tree observation: a feed marks nodes dirty; only those are re-read.

Covers the push observer's acceptance: events only mark nodes dirty; dirty nodes get lite
reads (children only for spawns and unknown nodes); a tree turns idle only after a
confirming pass; a failed read keeps it unknown; a missed event is repaired by the scan
after a reconnect; SSE falls back to the WebSocket and a dead feed to the timer; nonce
lookups no longer page every root; the status counters.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import AsyncIterator, Callable, Generator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from websockets.asyncio.server import ServerConnection, serve

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import DecisionImpact, Parcel, Via
from omnigent_factory.omnigent.directory import MemoryNonceLedger
from omnigent_factory.omnigent.feed import (
    MAX_OPEN_FAILURES,
    SSE,
    STREAM_PARK_S,
    TIMER,
    WS,
    ChangeFeed,
    DirtyMark,
)
from omnigent_factory.omnigent.inventory import SessionIndex
from omnigent_factory.omnigent.rest import FileTokenAuth, OmnigentReadError, OmnigentRest
from omnigent_factory.omnigent.tree import (
    LIVE_SESSION_STATUSES,
    NodeState,
    TreeObservation,
    TreeRefresh,
    node_from_snapshot,
    refresh_tree,
    scan_tree,
)
from omnigent_factory.service.durable import StoreNonceLedger
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.session_retention import (
    ARCHIVED_AT_LABEL,
    RECHECK_US,
    Candidate,
    SessionRetention,
)
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import T0, FakeOmnigentServer, FakeSession
from tests.omnigent.support import AGENT, make_rig


async def eventually(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < end, "condition was not reached"
        await asyncio.sleep(0.001)


async def _no_sleep(_seconds: float) -> None:
    await asyncio.sleep(0.001)


# ============================================================ fake channels


class FakeStreams:
    """Root SSE streams: ``push`` delivers an event; ``drop`` ends the connection."""

    def __init__(self) -> None:
        self.queues: dict[str, asyncio.Queue[Mapping[str, Any] | None]] = {}
        self.opens: Counter[str] = Counter()
        self.refuse: int | None = None
        #: Per-root refusal (overrides ``refuse``), and refusals of the next N opens.
        self.refuse_for: dict[str, int | None] = {}
        self.refuse_times: dict[str, int] = {}
        self.refuse_status = 401
        self.failed: Counter[str] = Counter()
        #: The connect snapshot each open delivers first (the server's, routes_events.py:
        #: 3956-4040): child summaries per root, then a changed-files invalidation. None:
        #: a server without one (the stream goes live on its first live event).
        self.snapshot: dict[str, list[dict[str, Any]]] | None = {}

    def connect(self, root: str, on_open: Callable[[], None]) -> AsyncIterator[dict[str, Any]]:
        async def gen() -> AsyncIterator[dict[str, Any]]:
            status = self.refuse_for.get(root, self.refuse)
            if self.refuse_times.get(root, 0) > 0:
                self.refuse_times[root] -= 1
                status = self.refuse_status
            if status is not None:
                self.failed[root] += 1
                raise OmnigentReadError(f"stream {root}: HTTP {status}", status)
            queue: asyncio.Queue[Mapping[str, Any] | None] = asyncio.Queue()
            self.queues[root] = queue
            self.opens[root] += 1
            on_open()
            yield {"type": "session.heartbeat"}
            if self.snapshot is not None:
                for child in self.snapshot.get(root, []):
                    yield {
                        "type": "session.child_session.updated",
                        "conversation_id": root,
                        "child_session_id": child["id"],
                        "child": child,
                    }
                yield {"type": "session.changed_files.invalidated", "session_id": root}
            while (event := await queue.get()) is not None:
                yield dict(event)

        return gen()

    def push(self, root: str, event: Mapping[str, Any]) -> None:
        self.queues[root].put_nowait(event)

    def drop(self, root: str) -> None:
        self.queues[root].put_nowait(None)


class FakeSocket:
    """The session-updates WebSocket: answers each watch with a snapshot of ``rows``."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.watches: list[list[str]] = []
        self.connects = 0
        self.down = False
        self._inbox: asyncio.Queue[str | None] | None = None

    def row(self, sid: str, **fields: Any) -> dict[str, Any]:
        base = {"id": sid, "status": "idle", "updated_at": T0, "labels": {}}
        self.rows[sid] = {**self.rows.get(sid, base), **fields}
        return self.rows[sid]

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[FakeSocket]:
        if self.down:
            raise OSError("connection refused")
        self.connects += 1
        self._inbox = asyncio.Queue()
        yield self

    async def send(self, message: str) -> None:
        frame = json.loads(message)
        ids = frame["session_ids"]
        self.watches.append(ids)
        items = [self.rows.get(i) or self.row(i) for i in ids]
        assert self._inbox is not None
        self._inbox.put_nowait(json.dumps({"type": "snapshot", "items": items}))

    async def recv(self) -> str:
        assert self._inbox is not None
        message = await self._inbox.get()
        if message is None:
            raise ConnectionError("socket closed")
        return message

    def changed(self, *rows: dict[str, Any]) -> None:
        assert self._inbox is not None
        self._inbox.put_nowait(json.dumps({"type": "changed", "items": list(rows)}))

    def drop(self) -> None:
        assert self._inbox is not None
        self._inbox.put_nowait(None)


def make_feed(
    streams: FakeStreams | None = None, socket: FakeSocket | None = None
) -> tuple[ChangeFeed, FakeStreams, FakeSocket, OmnigentRest]:
    streams = streams or FakeStreams()
    socket = socket or FakeSocket()
    rest = OmnigentRest("http://omnigent.test")
    feed = ChangeFeed(
        rest,
        FakeClock(),
        sse_connect=streams.connect,
        ws_connect=socket.connect,
        sleep=_no_sleep,
    )
    return feed, streams, socket, rest


# ============================================================ the feed


@pytest.mark.asyncio
async def test_stream_events_only_mark_nodes_dirty() -> None:
    feed, streams, _socket, rest = make_feed()
    feed.set_targets({"r"}, {"r": {"r": None, "c1": "r"}})
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    streams.push("r", {"type": "session.status", "conversation_id": "r", "status": "idle"})
    child = {"busy": True, "current_task_status": "in_progress"}
    streams.push(
        "r",
        {
            "type": "session.child_session.updated",
            "conversation_id": "r",
            "child_session_id": "c1",
            "child": child,
        },
    )
    streams.push("r", {"type": "response.output_text.delta", "delta": "tokens"})
    await eventually(lambda: len(feed._dirty.get("r", {})) == 2)
    marks = feed.take_dirty("r")
    assert set(marks) == {"r", "c1"} and all(m.edge for m in marks.values())
    # A preview-only delta is ordinary progress, not an edge.
    streams.push(
        "r",
        {
            "type": "session.child_session.updated",
            "conversation_id": "r",
            "child_session_id": "c1",
            "child": {"last_message_preview": "hi"},
        },
    )
    await eventually(lambda: feed.has_dirty("r"))
    assert not feed.take_dirty("r")["c1"].edge
    assert sum(rest.requests.values()) == 0  # marking never reads anything
    await feed.close()


@pytest.mark.asyncio
async def test_a_deep_tree_uses_its_stream_only_once_subtree_push_is_proven() -> None:
    feed, streams, socket, _rest = make_feed()
    tree = {"r": None, "a": "r", "b": "a"}
    feed.set_targets({"r"}, {"r": tree})
    await eventually(lambda: "r" in streams.queues)
    # An old server's root stream carries only direct children: the WS covers it.
    await eventually(lambda: feed.coverage("r")[0] == WS)
    assert socket.watches and set(socket.watches[-1]) == {"r", "a", "b"}
    streams.push(
        "r",
        {
            "type": "session.child_session.updated",
            "conversation_id": "a",  # b's own parent: published on r's stream too
            "child_session_id": "b",
            "child": {"busy": True},
        },
    )
    await eventually(lambda: feed.subtree_push)
    assert feed.coverage("r")[0] == SSE
    # Nothing is left to watch: the socket is closed, not kept open idle.
    await eventually(lambda: feed.status()["ws_connected"] is False)
    await feed.close()


@pytest.mark.asyncio
async def test_no_root_stream_falls_back_to_the_websocket() -> None:
    streams = FakeStreams()
    streams.refuse = 405
    feed, _streams, socket, _rest = make_feed(streams)
    feed.set_targets({"r"}, {"r": {"r": None, "c1": "r"}})
    await eventually(lambda: feed.sse_supported is False)
    await eventually(lambda: feed.coverage("r")[0] == WS)
    socket.changed(socket.row("c1", status="running"))
    await eventually(lambda: feed.has_dirty("r"))
    assert feed.take_dirty("r")["c1"].edge
    socket.changed(socket.row("c1", updated_at=T0 + 5))  # progress only
    await eventually(lambda: feed.has_dirty("r"))
    assert not feed.take_dirty("r")["c1"].edge
    await feed.close()


@pytest.mark.asyncio
async def test_a_dead_websocket_degrades_to_the_timer_and_reconnects_with_a_new_epoch() -> None:
    streams = FakeStreams()
    streams.refuse = 501
    feed, _streams, socket, _rest = make_feed(streams)
    feed.set_targets(set(), {"r": {"r": None}})
    await eventually(lambda: feed.coverage("r")[0] == WS)
    first = feed.coverage("r")
    socket.down = True
    socket.drop()
    await eventually(lambda: feed.coverage("r")[0] == TIMER)
    socket.down = False
    await eventually(lambda: feed.coverage("r")[0] == WS)
    assert feed.coverage("r") != first  # a new epoch: the observer reconciles
    assert socket.connects == 2
    await feed.close()


@pytest.mark.asyncio
async def test_a_dropped_stream_falls_back_to_the_websocket_until_it_reconnects() -> None:
    feed, streams, socket, _rest = make_feed()
    feed.set_targets({"r"}, {"r": {"r": None}})
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    first = feed.coverage("r")
    streams.drop("r")
    # Reconnects at once in this test; between, the tree is watched on the WS.
    await eventually(lambda: streams.opens["r"] == 2)
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    assert feed.coverage("r") != first
    assert feed.counters["sse_drops"] == 1
    assert socket.connects <= 1
    await feed.close()


# ============================================================ targeted reads


def _server() -> tuple[FakeOmnigentServer, OmnigentRest]:
    server = FakeOmnigentServer()
    server.add(FakeSession(id="root", agent_id=AGENT, status="running"))
    for kid in ("c1", "c2", "c3"):
        server.add(FakeSession(id=kid, agent_id=AGENT, parent_session_id="root"))
    return server, OmnigentRest("http://omnigent.test", transport=server.transport())


@pytest.mark.asyncio
async def test_full_scans_read_every_node_lite() -> None:
    server, rest = _server()
    obs = await scan_tree(rest, "root")
    assert obs.complete and set(obs.nodes) == {"root", "c1", "c2", "c3"}
    assert rest.requests["full_read"] == 0
    assert rest.requests["lite_read"] == 4
    snapshots = [p for m, path, p in server.requests if m == "GET" and path == "/v1/sessions/c1"]
    assert snapshots  # the fake records every request; the params are the lite ones
    await rest.aclose()


@pytest.mark.asyncio
async def test_only_dirty_nodes_are_read() -> None:
    server, rest = _server()
    cached = await scan_tree(rest, "root")
    rest.requests.clear()
    server.sessions["c2"].status = "running"
    result = await refresh_tree(rest, cached, ["c2"])
    assert dict(rest.requests) == {"lite_read": 1}  # no child lists, no other nodes
    assert result.read == {"c2"}
    assert result.observation.complete and result.observation.nodes["c2"].status == "running"
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_spawn_lists_the_parents_children_and_reads_the_new_node() -> None:
    server, rest = _server()
    cached = await scan_tree(rest, "root")
    rest.requests.clear()
    server.add(FakeSession(id="c4", agent_id=AGENT, parent_session_id="c1", status="running"))
    result = await refresh_tree(rest, cached, [], spawned=["c1"])
    assert rest.requests["children_page"] == 2  # c1's, then the new c4's
    assert rest.requests["lite_read"] == 1
    assert result.observation.nodes["c4"].parent_id == "c1"
    assert result.observation.busy
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_failed_read_keeps_the_tree_unknown() -> None:
    server, rest = _server()
    server.sessions["root"].status = "idle"
    cached = await scan_tree(rest, "root")
    assert cached.quiescent
    server.fail_paths.add("/v1/sessions/c1")
    result = await refresh_tree(rest, cached, ["c1"])
    assert not result.observation.complete and not result.observation.quiescent
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_turn_ending_reaches_quiescence_without_waiting_for_reconcile() -> None:
    """F2, on the real refresh path: ``running``/``in_progress`` -> ``idle``/``completed``.
    The lite snapshot carries no task status; the cached ``in_progress`` is unresolved,
    so the parent's child list is re-read in the same refresh (not kept busy)."""
    server, rest = _server()
    server.sessions["root"].status = "idle"
    server.sessions["c1"].status = "running"
    server.sessions["c1"].current_task_status = "in_progress"
    cached = await scan_tree(rest, "root")
    assert cached.busy and cached.nodes["c1"].task_status == "in_progress"
    server.sessions["c1"].status = "idle"
    server.sessions["c1"].current_task_status = "completed"
    rest.requests.clear()
    result = await refresh_tree(rest, cached, ["c1"])
    assert result.observation.nodes["c1"].task_status == "completed"
    assert result.observation.complete and result.observation.quiescent
    assert dict(rest.requests) == {"lite_read": 1, "children_page": 1}
    # The confirming pass the observer then runs also ends quiescent.
    confirmed = await refresh_tree(rest, result.observation, [], confirm=True, confirm_busy=["c1"])
    assert confirmed.confirmed and confirmed.observation.quiescent
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_live_task_on_an_idle_session_stays_busy_after_its_list_is_reread() -> None:
    """Unknown forces a read, not idle: a re-listed task that is still queued is busy."""
    server, rest = _server()
    server.sessions["root"].status = "idle"
    server.sessions["c1"].current_task_status = "in_progress"
    cached = await scan_tree(rest, "root")
    server.sessions["c1"].current_task_status = "queued"
    result = await refresh_tree(rest, cached, ["c1"])
    assert result.observation.nodes["c1"].task_status == "queued" and result.observation.busy
    server.fail_paths.add("/v1/sessions/root/child_sessions")
    failed = await refresh_tree(rest, cached, ["c1"])
    assert not failed.observation.complete  # the list could not be re-read: unknown
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_topology_poll_finds_a_spawned_child_with_three_requests() -> None:
    server, rest = _server()
    server.sessions["root"].status = "idle"
    cached = await scan_tree(rest, "root")
    server.add(FakeSession(id="c9", agent_id=AGENT, parent_session_id="root", status="running"))
    rest.requests.clear()
    index = SessionIndex(rest)
    await index.refresh()
    rest.requests.clear()
    result = await refresh_tree(rest, cached, [], topology=True, index=index)
    assert result.observation.nodes["c9"].busy and result.observation.complete
    # The root and the new child read lite, the root's child list, the inventory page,
    # and the new child's (empty) child list. Never every node.
    assert rest.requests["lite_read"] == 2 and rest.requests["inventory"] == 1
    assert rest.requests["children_page"] == 2
    await rest.aclose()


# ============================================================ the observer


@dataclass
class FakeFeed:
    """Coverage set by the test; marks as the real feed keeps them."""

    cover: dict[str, tuple[str, int]] = field(default_factory=dict)
    marks: dict[str, dict[str, DirtyMark]] = field(default_factory=dict)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    targets: list[tuple[frozenset[str], frozenset[str]]] = field(default_factory=list)

    def coverage(self, root: str) -> tuple[str, int]:
        return self.cover.get(root, (TIMER, 0))

    def mark(self, root: str, node: str, *, edge: bool = True, spawn: bool = False) -> None:
        self.marks.setdefault(root, {})[node] = DirtyMark(edge, spawn, marked_at_us=10**15)

    def take_dirty(
        self, root: str, keep: Callable[[str, DirtyMark], bool] | None = None
    ) -> dict[str, DirtyMark]:
        marks = self.marks.pop(root, {})
        kept = {n: m for n, m in marks.items() if keep is not None and keep(n, m)}
        if kept:
            self.marks[root] = kept
        return {n: m for n, m in marks.items() if n not in kept}

    def clear_before(self, root: str, started_us: int) -> None:
        del root, started_us

    snapshot_ok: bool = True
    gap: set[str] = field(default_factory=set)

    def snapshot_complete(self, root: str) -> bool:
        return self.snapshot_ok

    def resuming(self, root: str) -> bool:
        return root in self.gap

    def set_targets(self, sse_roots: Any, trees: Mapping[str, Any]) -> None:
        self.targets.append((frozenset(sse_roots), frozenset(trees)))

    def status(self) -> dict[str, object]:
        return {"fake": True}

    async def close(self) -> None:
        return None


@dataclass
class WorldAdapter:
    """The observer's adapter over a scripted world of node states (one root tree)."""

    world: dict[str, NodeState]
    root: str
    calls: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)
    fail_refresh: bool = False
    rest: Any = field(default_factory=lambda: SimpleNamespace(requests=Counter()))

    def _obs(self, nodes: Mapping[str, NodeState], complete: bool = True) -> TreeObservation:
        return TreeObservation(self.root, complete, dict(nodes), () if complete else ("x",))

    async def observe_trees(self, roots: Sequence[str]) -> list[Any]:
        self.calls.append(("full", tuple(roots)))
        self.rest.requests["lite_read"] += len(self.world)
        return [self._obs(self.world) for _ in roots]

    async def refresh_tree(
        self,
        cached: TreeObservation,
        dirty: Sequence[str],
        *,
        spawned: Sequence[str] = (),
        confirm: bool = False,
        confirm_busy: Sequence[str] = (),
        topology: bool = False,
        inventory_ready: bool = False,
        inventory_error: str | None = None,
    ) -> TreeRefresh:
        reads = set(dirty)
        if topology:
            reads |= {self.root, *(n for n in self.world if n not in cached.nodes)}
            self.calls.append(("topology", tuple(sorted(reads))))
        if confirm:
            reads |= {self.root, *confirm_busy, *(n for n, s in cached.nodes.items() if s.busy)}
        self.calls.append(("confirm" if confirm else "refresh", tuple(sorted(reads))))
        self.rest.requests["lite_read"] += len(reads)
        if self.fail_refresh:
            return TreeRefresh(replace(cached, complete=False, errors=("down",)), frozenset())
        nodes = dict(cached.nodes)
        nodes.update({n: self.world[n] for n in reads if n in self.world})
        return TreeRefresh(self._obs(nodes), frozenset(reads), confirmed=confirm)

    async def pending_prompts(self, root: str) -> dict[str, Any]:
        self.calls.append(("probe", (root,)))
        return {}

    async def refresh_inventory(self) -> str | None:
        return None


def _active_run() -> tuple[Parcel, str]:
    h = Harness()
    h.eligible("I_push")
    h.send("I_push", ev.RequestPlan(via=Via.DRAG))
    session = h.create_ok("I_push")
    return h.p("I_push"), str(session.root_id)


def _observer(
    adapter: WorldAdapter, feed: FakeFeed | None, clock: FakeClock, parcel: Parcel
) -> tuple[OmnigentObserver, list[TreeObservation]]:
    observer = OmnigentObserver(
        None,  # type: ignore[arg-type]
        adapter,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        clock,
        interval_seconds=5,
        settled_interval_seconds=60,
        feed=feed,  # type: ignore[arg-type]
    )
    seen: list[TreeObservation] = []

    async def parcels() -> list[Parcel]:
        return [parcel]

    async def observe(p: Parcel, obs: TreeObservation, **_: Any) -> None:
        cur = p.current_session
        assert cur is not None
        seen.append(obs)
        observer._last_tree[cur.session_id] = (obs.complete, obs.busy, obs.pending_waiter)

    async def load(_parcel_id: str) -> Parcel:
        return parcel

    observer._parcels = parcels  # type: ignore[method-assign]
    observer._observe = observe  # type: ignore[method-assign]
    observer._load_parcel = load  # type: ignore[method-assign]
    return observer, seen


def _world(root_id: str, **status: str) -> dict[str, NodeState]:
    root = root_id
    nodes = {root: NodeState(root, None, status.pop("root", "idle"))}
    for nid, st in status.items():
        nodes[nid] = NodeState(nid, root, st)
    return nodes


@pytest.mark.asyncio
async def test_events_drive_lite_reads_and_idle_waits_for_a_confirming_pass() -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running", w1="running", w2="idle"), root)
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, seen = _observer(adapter, feed, clock, parcel)
    await observer.observe_once()  # first sight: a full scan
    assert adapter.calls == [("full", (root,))] and seen[-1].busy
    for _ in range(10):  # nothing changed: no reads at all
        clock.advance(5_000_000)
        await observer.observe_once()
    assert len(adapter.calls) == 1
    # w1 ends its turn: only w1 is read; the tree is still busy (the root runs).
    adapter.world["w1"] = NodeState("w1", root, "idle")
    feed.mark(root, "w1")
    await observer.observe_once()
    assert adapter.calls[-1] == ("refresh", ("w1",)) and seen[-1].busy
    # The root ends its turn: read, then confirmed (root + every busy node) before idle.
    adapter.world[root] = NodeState(root, None, "idle")
    feed.mark(root, root)
    await observer.observe_once()
    assert adapter.calls[-2:] == [("refresh", (root,)), ("confirm", (root,))]
    assert seen[-1].quiescent
    assert observer.passes["confirm"] == 1 and observer.passes["targeted"] == 2
    # Already idle: a later edge needs no second confirmation.
    feed.mark(root, "w2")
    await observer.observe_once()
    assert adapter.calls[-1] == ("refresh", ("w2",)) and seen[-1].quiescent


@pytest.mark.asyncio
async def test_a_failed_targeted_read_keeps_the_tree_unknown_and_rescans_it() -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running"), root)
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, seen = _observer(adapter, feed, clock, parcel)
    await observer.observe_once()
    adapter.world[root] = NodeState(root, None, "idle")
    adapter.fail_refresh = True
    feed.mark(root, root)
    await observer.observe_once()
    assert not seen[-1].complete and not seen[-1].quiescent  # unknown, never idle
    clock.advance(5_000_000)
    await observer.observe_once()
    assert adapter.calls[-1] == ("full", (root,))  # retried by a full scan
    assert observer.passes["full:retry"] == 1 and seen[-1].quiescent


@pytest.mark.asyncio
async def test_a_missed_event_is_repaired_by_the_scan_after_a_reconnect() -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running"), root)
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, seen = _observer(adapter, feed, clock, parcel)
    await observer.observe_once()
    adapter.world[root] = NodeState(root, None, "idle")  # the edge happened while down
    clock.advance(5_000_000)
    await observer.observe_once()
    assert seen[-1].busy and len(adapter.calls) == 1  # no event, no read
    feed.cover[root] = (WS, 2)  # the stream is gone: the tree moved to the WebSocket
    await observer.observe_once()
    assert adapter.calls[-1] == ("full", (root,)) and seen[-1].quiescent
    assert observer.passes["full:reconnect"] == 1


@pytest.mark.asyncio
async def test_the_safety_scan_runs_on_the_reconcile_interval() -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running"), root)
    feed = FakeFeed(cover={root: (WS, 7)})
    clock = FakeClock()
    observer, _seen = _observer(adapter, feed, clock, parcel)
    for _ in range(int(observer.reconcile_active_seconds // 5) + 1):
        await observer.observe_once()
        clock.advance(5_000_000)
    assert [c for c in adapter.calls if c[0] == "full"] == [("full", (root,))] * 2
    assert observer.passes["full:safety"] == 1


@pytest.mark.asyncio
async def test_without_coverage_the_tree_is_polled_at_the_old_cadence() -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running"), root)
    feed = FakeFeed()  # neither channel covers it: timer
    clock = FakeClock()
    observer, _seen = _observer(adapter, feed, clock, parcel)
    for _ in range(10):  # 10 passes, 1 s apart (feed wakes can make passes frequent)
        await observer.observe_once()
        clock.advance(1_000_000)
    assert [c[0] for c in adapter.calls].count("full") == 2  # at 0 s and 5 s
    assert observer.status()["mode"] == TIMER
    # The probe still reads prompts of a polled tree; a covered one relies on the feed.
    assert await observer.probe_prompts_once()
    feed.cover[root] = (WS, 1)
    assert not await observer.probe_prompts_once()


@pytest.mark.asyncio
async def test_feed_mode_transitions_are_logged_and_reported(
    caplog: pytest.LogCaptureFixture,
) -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running"), root)
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, _seen = _observer(adapter, feed, clock, parcel)
    with caplog.at_level(logging.INFO, logger="omnigent_factory.service.observer"):
        await observer.observe_once()
        feed.cover[root] = (WS, 2)
        await observer.observe_once()
        feed.cover[root] = (TIMER, 0)
        await observer.observe_once()
    modes = [r.getMessage() for r in caplog.records if "observer feed mode" in r.getMessage()]
    assert [m.split(" (")[0] for m in modes] == [
        "observer feed mode start -> sse",
        "observer feed mode sse -> ws",
        "observer feed mode ws -> timer",
    ]
    status = observer.status()
    assert status["mode"] == TIMER
    assert status["trees"] == {SSE: 0, WS: 0, TIMER: 1}
    assert status["passes"] == {"full:initial": 1, "full:reconnect": 1, "full:timer": 1}
    assert status["omnigent_requests"] == {"lite_read": 3}
    assert status["feed"] == {"fake": True}


@pytest.mark.asyncio
async def test_progress_marks_are_read_at_most_once_per_interval() -> None:
    parcel, root = _active_run()
    adapter = WorldAdapter(_world(root, root="running"), root)
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, _seen = _observer(adapter, feed, clock, parcel)
    await observer.observe_once()
    feed.mark(root, root, edge=False)
    await observer.observe_once()
    assert len(adapter.calls) == 1  # read 0 s ago: held back
    clock.advance(5_000_000)
    await observer.observe_once()
    assert adapter.calls[-1] == ("refresh", (root,))


# ============================================================ nonce lookup


class TrustingLedger(MemoryNonceLedger):
    """A ledger whose recording began at ``since`` (as the durable one does)."""

    def __init__(self, since: int) -> None:
        super().__init__()
        self.since = since

    async def since_us(self) -> int | None:
        return self.since


@pytest.mark.asyncio
async def test_nonce_lookup_reads_no_pages_for_an_unsent_nonce(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    for i in range(40):
        rig.server.add(FakeSession(id=f"conv_old{i:02d}", agent_id=AGENT, created_at=T0 - 86_400))
    rig.adapter.nonces = TrustingLedger(rig.clock.now_utc_us() - 1)
    rows = await rig.adapter._nonce_rows("ranking-new", minted_at_us=rig.clock.now_utc_us())
    assert rows == [] and sum(rig.rest.requests.values()) == 0
    # Without a minted time (or before recording began) it searches as before: every page.
    legacy = await rig.adapter._nonce_rows("ranking-new")
    assert legacy == [] and rig.rest.requests["nonce"] >= 20


@pytest.mark.asyncio
async def test_nonce_lookup_after_a_create_reads_one_window_not_every_root(
    git_env: GitEnv,
) -> None:
    rig = make_rig(git_env, page_limit=5)
    for i in range(40):
        rig.server.add(FakeSession(id=f"conv_old{i:02d}", agent_id=AGENT, created_at=T0 - 86_400))
    await rig.adapter.record_create_intent("nonce-x")
    rig.server.add(FakeSession(id="conv_x", agent_id=AGENT, labels={"factory.dispatch": "nonce-x"}))
    await rig.adapter.record_created_root("nonce-x", "conv_x")
    rig.rest.requests.clear()
    rows = await rig.adapter._nonce_rows("nonce-x")
    assert [r["id"] for r in rows] == ["conv_x"]
    assert dict(rig.rest.requests) == {"nonce": 1}  # one newest-first page
    # A foreign root carrying the label inside the window is still seen (never adopted).
    rig.server.add(
        FakeSession(id="conv_forged", agent_id="ag_other", labels={"factory.dispatch": "nonce-x"})
    )
    assert {r["id"] for r in await rig.adapter._nonce_rows("nonce-x")} == {"conv_x", "conv_forged"}


def test_the_nonce_ledger_survives_a_restart(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    since = store.nonce_ledger_since_us()
    store.record_nonce_intent("n1", 123)
    store.record_nonce_root("n1", "conv_1")
    store.record_nonce_intent("n1", 456)  # a later attempt keeps the first time
    store.close()
    clock.advance(10)
    again = SqliteStore.open(db, clock)
    assert again.nonce_record("n1") == (("conv_1",), 123)
    assert again.nonce_record("n2") is None
    assert again.nonce_ledger_since_us() == since
    again.close()
    assert StoreNonceLedger  # the async wrapper over these (wired in composition)


@pytest.mark.asyncio
async def test_a_targeted_pass_closes_a_prompt_only_once_its_owner_was_re_read() -> None:
    """A targeted pass re-reads only some nodes: a prompt absent from a stale cached node
    is not gone. Only an owner read at or after the prompt opened proves it."""
    h = Harness()
    h.to_building()
    pid = "I_parcel_1"
    h.send(
        pid,
        ev.ElicitationOpened(
            session_id=h.cur().session_id,
            elicitation_id="e1",
            impact=DecisionImpact.UNKNOWN,
            summary="Which export format?",
            permission=False,
            detail="",
        ),
    )
    parcel = h.p(pid)
    session = parcel.current_session
    assert session is not None and session.root_id is not None
    root = session.root_id
    [decision] = parcel.open_decisions
    service = SimpleNamespace(
        config=SimpleNamespace(repo_id="repo", permission_prompt_grace_seconds=60.0)
    )
    observer = OmnigentObserver(
        service,  # type: ignore[arg-type]
        WorldAdapter({}, root),  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        FakeClock(),
        interval_seconds=5,
    )
    applied: list[ev.EventBody] = []

    async def apply(_parcel: Parcel, body: ev.EventBody, _event_id: str) -> None:
        applied.append(body)

    async def load(_parcel_id: str) -> Parcel:
        return parcel

    observer._apply = apply  # type: ignore[method-assign]
    observer._load_parcel = load  # type: ignore[method-assign]
    observer._prompt_owners[session.session_id] = {"e1": "w1"}
    obs = TreeObservation(
        root, True, {root: NodeState(root, None, "idle"), "w1": NodeState("w1", root, "idle")}
    )

    def gone() -> bool:
        return any(isinstance(b, ev.ElicitationGone) for b in applied)

    await observer._observe(parcel, obs, read_at={root: decision.opened_at_us})
    assert not gone()  # the owner w1 was not re-read since the prompt opened
    await observer._observe(parcel, obs, read_at={"w1": decision.opened_at_us - 1})
    assert not gone()  # read before it opened
    await observer._observe(parcel, obs, read_at={"w1": decision.opened_at_us})
    assert gone()
    applied.clear()
    await observer._observe(parcel, obs)  # a full scan read every node
    assert gone()


# ============================================================ the real socket


@pytest.mark.asyncio
async def test_auth_headers_run_the_whole_auth_flow(tmp_path: Path) -> None:
    token = tmp_path / "token"
    token.write_text("sentinel-token\n", encoding="utf-8")
    rest = OmnigentRest("http://omnigent.test", auth=FileTokenAuth(token))
    assert await rest.auth_headers() == {"Authorization": "Bearer sentinel-token"}

    class Refreshing(httpx.Auth):
        def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
            response = yield httpx.Request("POST", "http://omnigent.test/oauth/token")
            request.headers["Authorization"] = f"Bearer {response.json()['access_token']}"
            yield request

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/oauth/token"
        return httpx.Response(200, json={"access_token": "sentinel-refreshed"})

    refreshing = OmnigentRest(
        "http://omnigent.test", auth=Refreshing(), transport=httpx.MockTransport(handler)
    )
    assert await refreshing.auth_headers() == {"Authorization": "Bearer sentinel-refreshed"}
    await rest.aclose()
    await refreshing.aclose()


@pytest.mark.asyncio
async def test_the_updates_socket_speaks_the_watch_protocol(tmp_path: Path) -> None:
    """The default client against a real WebSocket server: authenticated like REST,
    watch-set on connect, snapshot then changed frames mark the watched node dirty."""
    seen_auth: list[str | None] = []
    watched: list[list[str]] = []

    async def handler(conn: ServerConnection) -> None:
        seen_auth.append(conn.request.headers.get("Authorization") if conn.request else None)
        assert conn.request is not None and conn.request.path == "/v1/sessions/updates"
        frame = json.loads(await conn.recv())
        watched.append(frame["session_ids"])
        rows = [{"id": i, "status": "idle", "updated_at": T0} for i in frame["session_ids"]]
        await conn.send(json.dumps({"type": "snapshot", "items": rows}))
        await conn.send(
            json.dumps({"type": "changed", "items": [{**rows[0], "status": "running"}]})
        )
        await conn.wait_closed()

    token = tmp_path / "token"
    token.write_text("sentinel-token", encoding="utf-8")
    async with serve(handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        rest = OmnigentRest(f"http://127.0.0.1:{port}", auth=FileTokenAuth(token))
        streams = FakeStreams()
        streams.refuse = 405
        feed = ChangeFeed(rest, FakeClock(), sse_connect=streams.connect, sleep=_no_sleep)
        feed.set_targets({"r"}, {"r": {"r": None}})
        await eventually(lambda: feed.coverage("r")[0] == WS, timeout=5)
        await eventually(lambda: feed.has_dirty("r"), timeout=5)
        assert feed.take_dirty("r")["r"].edge
        assert seen_auth == ["Bearer sentinel-token"] and watched == [["r"]]
        assert rest.requests["ws_connect"] == 1
        await feed.close()
        await rest.aclose()


@pytest.mark.asyncio
async def test_only_edges_wake_the_observer() -> None:
    feed, streams, _socket, _rest = make_feed()
    feed.set_targets({"r"}, {"r": {"r": None, "c1": "r"}})
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    feed.wake.clear()
    delta = {"last_message_preview": "hi"}
    update = {"type": "session.child_session.updated", "conversation_id": "r"}
    streams.push("r", {**update, "child_session_id": "c1", "child": {"busy": True}})
    await eventually(feed.wake.is_set)
    feed.take_dirty("r")
    feed.wake.clear()
    streams.push("r", {**update, "child_session_id": "c1", "child": delta})
    await eventually(lambda: feed.has_dirty("r"))
    assert not feed.wake.is_set()  # progress waits for the next timed pass
    await feed.close()


# ============================================================ spawns on idle trees (F1)


@dataclass
class RealTreeAdapter:
    """The observer's adapter over the real scan/refresh code and a fake server."""

    rest: OmnigentRest
    index: SessionIndex

    async def observe_trees(self, roots: Sequence[str]) -> list[Any]:
        return [await scan_tree(self.rest, r, index=self.index) for r in roots]

    async def refresh_tree(
        self, cached: TreeObservation, dirty: Sequence[str], **kw: Any
    ) -> TreeRefresh:
        return await refresh_tree(self.rest, cached, dirty, index=self.index, **kw)

    async def refresh_inventory(self) -> str | None:
        try:
            await self.index.refresh()
        except OmnigentReadError as exc:
            return exc.reason
        return None

    async def pending_prompts(self, root: str) -> dict[str, Any]:
        return {}


def _parked_tree() -> tuple[FakeOmnigentServer, Parcel, str, RealTreeAdapter]:
    parcel, root = _active_run()
    server = FakeOmnigentServer()
    server.add(FakeSession(id=root, agent_id=AGENT, status="idle"))
    server.add(FakeSession(id="w1", agent_id=AGENT, parent_session_id=root))
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    return server, parcel, root, RealTreeAdapter(rest, SessionIndex(rest))


@pytest.mark.asyncio
async def test_an_idle_parked_root_spawning_is_seen_within_one_topology_interval_on_ws() -> None:
    server, parcel, root, adapter = _parked_tree()
    feed = FakeFeed(cover={root: (WS, 1)})
    clock = FakeClock()
    observer, seen = _observer(adapter, feed, clock, parcel)  # type: ignore[arg-type]
    observer._parked = lambda _p, _s: True  # type: ignore[method-assign]
    observer.parked_interval_seconds = 300
    await observer.observe_once()
    assert seen[-1].quiescent and set(seen[-1].nodes) == {root, "w1"}
    # The idle root spawns a working child: the watch-set does not know its id, and the
    # spawn marker does not even move the root's row. No event reaches the feed.
    server.add(FakeSession(id="w2", agent_id=AGENT, parent_session_id=root, status="running"))
    clock.advance(299_000_000)
    await observer.observe_once()
    assert "w2" not in seen[-1].nodes
    clock.advance(1_000_000)  # one parked topology interval (the old 300 s cadence)
    await observer.observe_once()
    assert "w2" in seen[-1].nodes and seen[-1].busy
    assert observer.passes["topology"] == 1
    assert observer.passes.get("full:safety", 0) == 0  # not the 1800 s reconcile
    # A WS tree's reading is never widened past the sweep's own gate.
    reading = observer.reading(parcel.current_session_id or "")
    assert reading is None or reading.fresh_for_us == 0
    await adapter.rest.aclose()


@pytest.mark.asyncio
async def test_an_idle_parked_root_spawning_is_seen_at_once_on_its_stream() -> None:
    server, parcel, root, adapter = _parked_tree()
    streams = FakeStreams()
    feed = ChangeFeed(
        OmnigentRest("http://omnigent.test"),
        FakeClock(),
        sse_connect=streams.connect,
        ws_connect=FakeSocket().connect,
        sleep=_no_sleep,
    )
    clock = FakeClock()
    observer, seen = _observer(adapter, None, clock, parcel)  # type: ignore[arg-type]
    observer.feed = feed
    observer._parked = lambda _p, _s: True  # type: ignore[method-assign]
    await observer.observe_once()  # boot: on the timer until the stream opens
    # A parked tree keeps its root stream too.
    await eventually(lambda: feed.coverage(root)[0] == SSE)
    await observer.observe_once()  # the reconnect scan
    assert observer.passes["full:reconnect"] == 1 and seen[-1].quiescent
    server.add(FakeSession(id="w2", agent_id=AGENT, parent_session_id=root, status="running"))
    streams.push(
        root,
        {
            "type": "session.created",
            "conversation_id": root,
            "child_session_id": "w2",
            "parent_session_id": root,
        },
    )
    await eventually(lambda: feed.has_dirty(root))
    await observer.observe_once()  # no clock advance
    assert "w2" in seen[-1].nodes and seen[-1].busy
    assert observer.passes.get("topology", 0) == 0  # pushed: no topology polling
    await feed.close()
    await adapter.rest.aclose()


# ============================================================ activity (F4)


def test_updated_at_is_the_only_activity_signal_a_node_carries() -> None:
    """What counts as activity is the server's ``updated_at``: it advances only for a
    write inserting a new item that is not a ``resource_event`` (messages, function calls
    and outputs, reasoning); a ``resource_event`` (e.g. a sub-agent spawn marker) or a
    pure stable-id duplicate leaves it (``sqlalchemy_store.py:2981-2987,3041-3048``
    @42e17e47). The factory reads no items, so item classes reach it only through it."""
    base = {"id": "n", "status": "idle", "updated_at": T0}
    quiet = node_from_snapshot("n", None, {**base, "items": [{"type": "resource_event"}]})
    same = node_from_snapshot("n", None, base)
    assert quiet.updated_at_us == same.updated_at_us == T0 * 1_000_000
    assert quiet.items_hint == 1 and same.items_hint == 0  # never used as activity
    moved = node_from_snapshot("n", None, {**base, "updated_at": T0 + 1})
    tree = TreeObservation("n", True, {"n": quiet})
    assert tree.last_activity_us == T0 * 1_000_000
    assert TreeObservation("n", True, {"n": moved}).last_activity_us > tree.last_activity_us


# ============================================================ nonce skew (F3)


@pytest.mark.asyncio
async def test_a_skewed_clock_window_miss_falls_back_to_every_page(git_env: GitEnv) -> None:
    rig = make_rig(git_env, page_limit=5)
    # This host's clock runs 2 h ahead of the server: the ambiguous root's created_at is
    # below the window floor (attempt - 1 h), and 12 newer roots fill the first pages.
    server_now = rig.clock.now_utc_us() // 1_000_000 - 7200
    await rig.adapter.record_create_intent("nonce-skew")
    rig.server.add(
        FakeSession(
            id="conv_skew",
            agent_id=AGENT,
            labels={"factory.dispatch": "nonce-skew"},
            created_at=server_now,
        )
    )
    for i in range(12):
        rig.server.add(
            FakeSession(id=f"conv_new{i:02d}", agent_id=AGENT, created_at=server_now + 60 + i)
        )
    rig.rest.requests.clear()
    rows = await rig.adapter._nonce_rows("nonce-skew")
    assert [r["id"] for r in rows] == ["conv_skew"]
    assert rig.rest.requests["nonce"] > 3  # the one-page window, then every page


# ============================================================ every non-live edge (F2)

#: The server's public session statuses (``schemas.py:2450,3217,3571`` @42e17e47) outside
#: the live set, plus an absent status: each ends a turn, so a cached live task status on
#: such a node is unresolved. ``(snapshot status, fresh task status)``.
NON_LIVE_EDGES = [("idle", "completed"), ("failed", "failed"), (None, "cancelled")]


@pytest.mark.parametrize(("status", "task"), NON_LIVE_EDGES)
@pytest.mark.asyncio
async def test_every_non_live_status_re_lists_a_cached_live_task(
    status: str | None, task: str
) -> None:
    server, rest = _server()
    server.sessions["root"].status = "idle"
    server.sessions["c1"].status = "running"
    server.sessions["c1"].current_task_status = "in_progress"
    cached = await scan_tree(rest, "root")
    assert cached.busy
    server.sessions["c1"].status = status  # type: ignore[assignment]
    server.sessions["c1"].current_task_status = task
    rest.requests.clear()
    result = await refresh_tree(rest, cached, ["c1"])
    assert result.observation.nodes["c1"].task_status == task
    assert result.observation.complete and result.observation.quiescent
    assert dict(rest.requests) == {"lite_read": 1, "children_page": 1}
    await rest.aclose()


@pytest.mark.parametrize("status", sorted(LIVE_SESSION_STATUSES))
@pytest.mark.asyncio
async def test_a_live_status_keeps_its_cached_task_without_a_relist(status: str) -> None:
    server, rest = _server()
    server.sessions["c1"].status = "running"
    server.sessions["c1"].current_task_status = "in_progress"
    cached = await scan_tree(rest, "root")
    server.sessions["c1"].status = status
    rest.requests.clear()
    result = await refresh_tree(rest, cached, ["c1"])
    assert result.observation.nodes["c1"].task_status == "in_progress"
    assert result.observation.busy
    assert dict(rest.requests) == {"lite_read": 1}  # nothing conflicts: no re-list
    await rest.aclose()


@pytest.mark.parametrize(("status", "task"), NON_LIVE_EDGES)
@pytest.mark.asyncio
async def test_a_worker_turn_ending_in_any_non_live_status_makes_the_tree_idle(
    status: str | None, task: str
) -> None:
    """Observer level: running/in_progress -> each non-live edge reaches quiescence on
    the edge's own pass (confirmed), never waiting for the safety reconcile."""
    parcel, root = _active_run()
    server = FakeOmnigentServer()
    server.add(FakeSession(id=root, agent_id=AGENT, status="idle"))
    server.add(
        FakeSession(
            id="w1",
            agent_id=AGENT,
            parent_session_id=root,
            status="running",
            current_task_status="in_progress",
        )
    )
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    adapter = RealTreeAdapter(rest, SessionIndex(rest))
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, seen = _observer(adapter, feed, clock, parcel)  # type: ignore[arg-type]
    await observer.observe_once()
    assert seen[-1].busy
    server.sessions["w1"].status = status  # type: ignore[assignment]
    server.sessions["w1"].current_task_status = task
    feed.mark(root, "w1")
    await observer.observe_once()
    assert seen[-1].complete and seen[-1].quiescent
    assert seen[-1].nodes["w1"].task_status == task
    assert observer.passes["confirm"] == 1
    assert observer.passes.get("full:safety", 0) == 0
    await rest.aclose()


# ============================================================ live defect A: streams


class _EndlessBody(httpx.AsyncByteStream):
    """An SSE body that never ends (a live stream)."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'event: session.heartbeat\ndata: {"type": "session.heartbeat"}\n\n'
        await asyncio.Event().wait()


class _BodyReadingAuth(httpx.Auth):
    """Like ``CliStoreAuth``: ``requires_response_body`` (for a token refresh)."""

    requires_response_body = True

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response]:
        request.headers["Authorization"] = "Bearer sentinel-token"
        yield request


@pytest.mark.asyncio
async def test_a_stream_opens_at_once_under_an_auth_that_reads_response_bodies() -> None:
    """Live defect A: httpx reads the whole body of every response for an auth with
    ``requires_response_body``; a stream's never ends, so the old ``stream()`` returned
    nothing until the peer closed it (the live proxy, ~121 s): never connected."""
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Authorization"))
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=_EndlessBody()
        )

    rest = OmnigentRest(
        "http://omnigent.test", auth=_BodyReadingAuth(), transport=httpx.MockTransport(handler)
    )
    opened: list[bool] = []
    events = rest.stream("r", on_open=lambda: opened.append(True))
    first = await asyncio.wait_for(events.__anext__(), timeout=2)
    assert first["type"] == "session.heartbeat" and opened == [True]
    assert seen == ["Bearer sentinel-token"]  # still authenticated
    await events.aclose()
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_stream_is_live_only_after_its_connect_snapshot() -> None:
    streams = FakeStreams()
    gate = asyncio.Event()
    original = streams.connect

    def slow(root: str, on_open: Callable[[], None]) -> AsyncIterator[dict[str, Any]]:
        async def gen() -> AsyncIterator[dict[str, Any]]:
            inner = original(root, on_open)
            first = await inner.__anext__()  # opened, heartbeat
            yield first
            await gate.wait()  # the snapshot is still on its way
            async for event in inner:
                yield event

        return gen()

    streams.snapshot = {"r": [{"id": "c1", "busy": False, "updated_at": T0}]}
    feed, _s, _socket, _rest = make_feed(streams)
    feed._sse_connect = slow
    feed.set_targets({"r"}, {"r": {"r": None, "c1": "r"}})
    await eventually(lambda: feed.counters["sse_connects"] == 1)
    assert feed.coverage("r")[0] != SSE  # open, but not live before the snapshot
    gate.set()
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    assert feed.snapshot_complete("r")
    await feed.close()


@pytest.mark.asyncio
async def test_a_server_without_a_connect_snapshot_goes_live_on_its_first_event() -> None:
    streams = FakeStreams()
    streams.snapshot = None
    feed, _s, _socket, _rest = make_feed(streams)
    feed.set_targets({"r"}, {"r": {"r": None}})
    await eventually(lambda: feed.counters["sse_connects"] == 1)
    assert feed.coverage("r")[0] != SSE
    streams.push("r", {"type": "session.status", "conversation_id": "r", "status": "idle"})
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    assert not feed.snapshot_complete("r")  # its children must be re-listed on resume
    await feed.close()


@pytest.mark.asyncio
async def test_a_reconnect_snapshot_marks_only_children_that_changed() -> None:
    streams = FakeStreams()
    kids = [
        {"id": "c1", "busy": False, "current_task_status": "completed", "updated_at": T0},
        {"id": "c2", "busy": False, "current_task_status": "completed", "updated_at": T0},
    ]
    streams.snapshot = {"r": kids}
    feed, _s, _socket, _rest = make_feed(streams)
    feed.set_targets({"r"}, {"r": {"r": None, "c1": "r", "c2": "r"}})
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    feed.take_dirty("r")  # the first connect's (followed by a full scan anyway)
    streams.snapshot = {"r": [kids[0], {**kids[1], "busy": True, "updated_at": T0 + 9}]}
    first = feed.coverage("r")
    streams.drop("r")
    await eventually(lambda: feed.coverage("r")[0] == SSE and feed.coverage("r") != first)
    marks = feed.take_dirty("r")
    assert set(marks) == {"c2"} and marks["c2"].edge  # c1's identical row marks nothing
    await feed.close()


@pytest.mark.asyncio
async def test_stream_drops_are_warned_with_their_reason_at_most_every_ten_minutes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    feed, streams, _socket, _rest = make_feed()
    clock: FakeClock = feed._clock  # type: ignore[assignment]
    feed.set_targets({"r"}, {"r": {"r": None}})
    with caplog.at_level(logging.WARNING, logger="omnigent_factory.omnigent.feed"):
        for n in range(1, 4):
            await eventually(lambda n=n: streams.opens["r"] == n and feed.coverage("r")[0] == SSE)
            streams.drop("r")
        await eventually(lambda: streams.opens["r"] == 4)
        clock.advance(600_000_000)
        await eventually(lambda: feed.coverage("r")[0] == SSE)
        streams.drop("r")
        await eventually(lambda: streams.opens["r"] == 5)
    warnings = [r.getMessage() for r in caplog.records if "root stream drop" in r.getMessage()]
    assert len(warnings) == 2
    assert warnings[0].startswith("change feed: 1 root stream drop(s)")
    assert "after connecting: closed by the server" in warnings[0]
    assert warnings[1].startswith("change feed: 3 root stream drop(s)")
    await feed.close()


@pytest.mark.asyncio
async def test_a_transient_401_on_every_root_never_disables_streams() -> None:
    """40 roots whose first open is refused (an auth refresh, an outage): each backs off
    on its own and reconnects; nothing is given up process-wide."""
    streams = FakeStreams()
    roots = [f"r{i:02d}" for i in range(40)]
    streams.refuse_times = dict.fromkeys(roots, 1)
    feed, _s, _socket, _rest = make_feed(streams)
    feed.set_targets(set(roots), {r: {r: None} for r in roots})
    await eventually(lambda: all(feed.coverage(r)[0] == SSE for r in roots), timeout=5)
    assert feed.sse_supported is True
    assert all(streams.failed[r] == 1 and streams.opens[r] == 1 for r in roots)
    assert feed.status()["sse_parked"] == 0
    await feed.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 405, 406, 415, 501])
async def test_an_explicit_unsupported_status_disables_streams(status: int) -> None:
    streams = FakeStreams()
    streams.refuse = status
    feed, _s, _socket, _rest = make_feed(streams)
    feed.set_targets({"r"}, {"r": {"r": None}})
    await eventually(lambda: feed.sse_supported is False)
    await eventually(lambda: feed.coverage("r")[0] == WS)  # watched on the WebSocket
    feed.set_targets({"r"}, {"r": {"r": None}})
    await asyncio.sleep(0.05)
    assert streams.failed["r"] == 1  # never reopened
    await feed.close()


@pytest.mark.asyncio
async def test_a_200_that_is_not_an_event_stream_is_unsupported() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"html": "a login page"})

    rest = OmnigentRest("http://omnigent.test", transport=httpx.MockTransport(handler))
    with pytest.raises(OmnigentReadError) as raised:
        await rest.stream("r").__anext__()
    assert raised.value.status == 415  # one of the feed's explicit "unsupported" statuses
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_root_that_keeps_failing_is_parked_then_recovers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    streams = FakeStreams()
    streams.refuse_for = {"a": 502}  # a gateway error on one root only
    parked = asyncio.Event()
    release = asyncio.Event()

    async def sleep(seconds: float) -> None:
        if seconds == STREAM_PARK_S:
            parked.set()
            await release.wait()
        else:
            await asyncio.sleep(0.001)

    feed = ChangeFeed(
        OmnigentRest("http://omnigent.test"),
        FakeClock(),
        sse_connect=streams.connect,
        ws_connect=FakeSocket().connect,
        sleep=sleep,
    )
    with caplog.at_level(logging.WARNING, logger="omnigent_factory.omnigent.feed"):
        feed.set_targets({"a"}, {"a": {"a": None}})
        await parked.wait()
    assert streams.failed["a"] == MAX_OPEN_FAILURES
    assert feed.status()["sse_parked"] == 1 and feed.sse_supported is not False
    assert any("retrying in 600s" in r.getMessage() for r in caplog.records)
    await eventually(lambda: feed.coverage("a")[0] == WS)  # its tree is still watched
    streams.refuse_for = {}
    release.set()  # the park ends: it retries and recovers
    await eventually(lambda: feed.coverage("a")[0] == SSE)
    assert feed.status()["sse_parked"] == 0
    await feed.close()


@pytest.mark.asyncio
async def test_another_roots_stream_opening_unparks_a_parked_root() -> None:
    streams = FakeStreams()
    streams.refuse_for = {"a": 503}

    async def sleep(seconds: float) -> None:
        if seconds == STREAM_PARK_S:
            await asyncio.Event().wait()  # only an unpark ends it
        await asyncio.sleep(0.001)

    feed = ChangeFeed(
        OmnigentRest("http://omnigent.test"),
        FakeClock(),
        sse_connect=streams.connect,
        ws_connect=FakeSocket().connect,
        sleep=sleep,
    )
    feed.set_targets({"a"}, {"a": {"a": None}})
    await eventually(lambda: feed.status()["sse_parked"] == 1)
    streams.refuse_for = {}
    feed.set_targets({"a", "b"}, {"a": {"a": None}, "b": {"b": None}})
    await eventually(lambda: feed.coverage("b")[0] == SSE)
    await eventually(lambda: feed.coverage("a")[0] == SSE)  # retried without waiting
    await feed.close()


# ============================================================ live defect B: full reads


@pytest.mark.asyncio
async def test_a_stream_reconnect_resumes_with_a_root_read_not_a_full_scan() -> None:
    """The live proxy ends every stream after ~121 s: each reconnect used to bump the
    epoch into a full scan of its tree. A resume reads the root (and re-lists children
    only when the connect snapshot did not)."""
    _server, parcel, root, adapter = _parked_tree()
    feed = FakeFeed(cover={root: (SSE, 1)})
    clock = FakeClock()
    observer, seen = _observer(adapter, feed, clock, parcel)  # type: ignore[arg-type]
    await observer.observe_once()
    adapter.rest.requests.clear()
    feed.cover[root] = (TIMER, 0)  # the stream dropped: a pass in the gap waits
    feed.gap.add(root)
    clock.advance(5_000_000)
    await observer.observe_once()
    assert sum(adapter.rest.requests.values()) == 0
    feed.cover[root] = (SSE, 2)
    feed.gap.clear()
    await observer.observe_once()
    assert dict(adapter.rest.requests) == {"lite_read": 1}  # the root
    assert observer.passes["resume"] == 1 and seen[-1].quiescent
    assert observer.passes.get("full:reconnect", 0) == 0
    adapter.rest.requests.clear()
    feed.snapshot_ok = False
    feed.cover[root] = (SSE, 3)
    await observer.observe_once()
    assert dict(adapter.rest.requests) == {"lite_read": 1, "children_page": 1}
    await adapter.rest.aclose()


@pytest.mark.asyncio
async def test_retention_reads_lite_and_skips_roots_that_cannot_be_eligible_yet() -> None:
    server = FakeOmnigentServer()
    server.add(FakeSession(id="live", agent_id=AGENT))  # finished issue, not archived
    server.add(
        FakeSession(
            id="fresh",
            agent_id=AGENT,
            archived=True,
            labels={"factory.dispatch": "n", ARCHIVED_AT_LABEL: str(T0 - 86_400)},
        )
    )
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    clock = FakeClock()
    service = SimpleNamespace(clock=clock)
    retention = SessionRetention(service, SimpleNamespace(rest=rest))  # type: ignore[arg-type]
    cutoff = clock.now_utc_us() - 7 * 86_400_000_000
    live, fresh = Candidate("live", "I_1"), Candidate("fresh", "I_2")
    assert await retention._eligible(live, cutoff) == "not archived"
    assert await retention._eligible(fresh, cutoff) == "archived too recently"
    assert dict(rest.requests) == {"lite_read": 2}  # never the items
    for _ in range(3):  # sweeps every 15 minutes re-read neither
        clock.advance(900_000_000)
        cutoff += 900_000_000
        assert await retention._eligible(live, cutoff) == "not archived"
        assert await retention._eligible(fresh, cutoff) == "archived too recently"
    assert dict(rest.requests) == {"lite_read": 2}
    clock.advance(RECHECK_US)  # the unarchived root is looked at again
    assert await retention._eligible(live, cutoff + RECHECK_US) == "not archived"
    assert dict(rest.requests) == {"lite_read": 3}
    await rest.aclose()
