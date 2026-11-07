"""Whole-tree reads in one call, lite reads only on changed rows, cheap retention.

``GET /v1/sessions/{root}/child_sessions?descendants=true`` (routes_items.py:139-310
@42e17e47) lists the whole non-archived subtree with ``depth`` and each row's own
``parent_session_id``: a tree pass is one listing instead of one child list per parent.
A server without it (rows without ``depth``) or one refusing the parameters is walked
parent by parent, as before. A node is read only when its row changed since it was read
(or it has no row, was busy or holds a prompt); quiescence stays fail-closed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx
import pytest

from omnigent_factory.omnigent.feed import SSE
from omnigent_factory.omnigent.inventory import SessionIndex
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.omnigent.tree import (
    DESCENDANT_DEPTH,
    TreeObservation,
    list_subtree,
    refresh_tree,
    scan_tree,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.observer import RECONCILE_ACTIVE_SECONDS
from omnigent_factory.service.session_retention import (
    ARCHIVED_AT_LABEL,
    Candidate,
    SessionRetention,
)
from omnigent_factory.testing.fakes import FakeClock
from tests.omnigent.fake_server import T0, FakeOmnigentServer, FakeSession, elicitation
from tests.omnigent.support import AGENT

CHILDREN = "/v1/sessions/root/child_sessions"


def _deep_server(*, supported: bool = True) -> tuple[FakeOmnigentServer, OmnigentRest]:
    """root -> a, b; a -> a1, a2; a1 -> a11 (three levels, five sub-agents)."""
    server = FakeOmnigentServer()
    server.supports_descendants = supported
    server.add(FakeSession(id="root", agent_id=AGENT))
    for kid, parent in (("a", "root"), ("b", "root"), ("a1", "a"), ("a2", "a"), ("a11", "a1")):
        server.add(FakeSession(id=kid, agent_id=AGENT, parent_session_id=parent))
    return server, OmnigentRest("http://omnigent.test", transport=server.transport())


def _parents(obs: TreeObservation) -> dict[str, str | None]:
    return {nid: n.parent_id for nid, n in sorted(obs.nodes.items())}


EXPECTED = {"a": "root", "a1": "a", "a11": "a1", "a2": "a", "b": "root", "root": None}


# ============================================================ one call per tree pass


@pytest.mark.asyncio
async def test_a_full_scan_lists_the_whole_tree_in_one_request() -> None:
    server, rest = _deep_server()
    server.sessions["a11"].current_task_status = "in_progress"
    server.sessions["a11"].status = "running"
    obs = await scan_tree(rest, "root")
    assert obs.complete and _parents(obs) == EXPECTED
    assert rest.requests["descendants"] == 1 and rest.requests["children_page"] == 0
    assert obs.nodes["a11"].task_status == "in_progress" and obs.busy
    listing = [p for m, p, _ in server.requests if m == "GET" and p.endswith("child_sessions")]
    assert listing == [CHILDREN]
    await rest.aclose()


@pytest.mark.asyncio
async def test_the_listing_is_paged_with_the_routes_cursor() -> None:
    server, _rest = _deep_server()
    seen: list[Mapping[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return server.handle(request)

    rest = OmnigentRest("http://omnigent.test", transport=httpx.MockTransport(handler))
    subtree = await list_subtree(rest, "root")
    assert subtree.whole and dict(subtree.parents) == {k: v for k, v in EXPECTED.items() if v}
    assert seen[0]["descendants"] == "true"
    assert seen[0]["max_depth"] == str(DESCENDANT_DEPTH) and seen[0]["limit"] == "1000"
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_server_without_the_flag_is_walked_per_parent() -> None:
    """Rows without ``depth`` are the root's direct children (an older server, or a
    spawn tree over the server's read cap): every further parent is listed, as before."""
    _server, rest = _deep_server(supported=False)
    obs = await scan_tree(rest, "root")
    assert obs.complete and _parents(obs) == EXPECTED
    # The first (flagged) request doubles as the root's child list: no extra request.
    assert rest.requests["descendants"] == 1
    assert rest.requests["children_page"] == 5  # a, b, a1, a2, a11
    await rest.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 422])
async def test_a_server_refusing_the_parameters_is_walked_per_parent(status: int) -> None:
    server, rest = _deep_server()
    server.faults[("GET", CHILDREN)].append(status)
    obs = await scan_tree(rest, "root")
    assert obs.complete and _parents(obs) == EXPECTED
    assert rest.requests["children_page"] == 6  # root, a, b, a1, a2, a11
    await rest.aclose()


@pytest.mark.asyncio
async def test_nodes_below_the_depth_cap_are_walked() -> None:
    server = FakeOmnigentServer()
    server.add(FakeSession(id="root", agent_id=AGENT))
    parent = "root"
    for depth in range(1, DESCENDANT_DEPTH + 3):
        server.add(FakeSession(id=f"d{depth:02d}", agent_id=AGENT, parent_session_id=parent))
        parent = f"d{depth:02d}"
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    obs = await scan_tree(rest, "root")
    assert obs.complete and len(obs.nodes) == DESCENDANT_DEPTH + 3
    assert obs.nodes[f"d{DESCENDANT_DEPTH + 2:02d}"].parent_id == f"d{DESCENDANT_DEPTH + 1:02d}"
    # One listing, then the cap's last level and below, one parent each.
    assert rest.requests["descendants"] == 1 and rest.requests["children_page"] == 3
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_failed_listing_keeps_the_scan_incomplete() -> None:
    server, rest = _deep_server()
    server.fail_paths.add(CHILDREN)
    obs = await scan_tree(rest, "root")
    assert not obs.complete and not obs.quiescent  # unknown, never idle
    await rest.aclose()


# ============================================================ lite reads only on change


@pytest.mark.asyncio
async def test_a_rescan_reads_only_the_root_and_changed_rows() -> None:
    server, rest = _deep_server()
    first = await scan_tree(rest, "root")
    assert rest.requests["lite_read"] == 6  # first sight: every node
    rest.requests.clear()
    again = await scan_tree(rest, "root", cached=first)
    assert again.complete and again.quiescent and _parents(again) == EXPECTED
    # The root only.
    assert dict(rest.requests) == {"descendants": 1, "inventory": 1, "lite_read": 1}
    # Activity (updated_at) on one node: that node is read, nothing else.
    server.sessions["a2"].updated_at = T0 + 60
    rest.requests.clear()
    third = await scan_tree(rest, "root", cached=again)
    reads = [p for m, p, _ in server.requests[-2:] if m == "GET"]
    assert dict(rest.requests) == {"descendants": 1, "inventory": 1, "lite_read": 2}
    assert "/v1/sessions/a2" in reads
    assert third.nodes["a2"].updated_at_us == (T0 + 60) * 1_000_000
    # A pending prompt (its count in the row) is always read.
    server.sessions["b"].pending_elicitations.append(elicitation("el_b"))
    rest.requests.clear()
    fourth = await scan_tree(rest, "root", cached=third)
    assert dict(rest.requests) == {"descendants": 1, "inventory": 1, "lite_read": 2}
    assert fourth.pending_waiter and "el_b" in fourth.owned_elicitations()
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_busy_node_is_read_until_proven_idle() -> None:
    """Busy must be proven over: a cached busy node is read even with an unchanged row,
    and a node whose row says it went idle is read before the tree is idle."""
    server, rest = _deep_server()
    server.sessions["a1"].status = "running"
    server.sessions["a1"].current_task_status = "in_progress"
    first = await scan_tree(rest, "root")
    assert first.busy
    rest.requests.clear()
    again = await scan_tree(rest, "root", cached=first)
    assert again.busy
    assert dict(rest.requests) == {"descendants": 1, "inventory": 1, "lite_read": 2}
    server.sessions["a1"].status = "idle"
    server.sessions["a1"].current_task_status = "completed"
    done = await scan_tree(rest, "root", cached=again)
    assert done.quiescent and done.nodes["a1"].task_status == "completed"
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_refresh_needs_one_listing_and_reads_only_changed_rows() -> None:
    server, rest = _deep_server()
    server.sessions["root"].status = "running"
    cached = await scan_tree(rest, "root")
    server.add(FakeSession(id="a21", agent_id=AGENT, parent_session_id="a2", status="running"))
    server.sessions["b"].updated_at = T0 + 30  # changed row: read
    rest.requests.clear()
    result = await refresh_tree(rest, cached, [], spawned=["a2"])
    assert result.observation.complete and result.observation.nodes["a21"].parent_id == "a2"
    assert result.read == {"a21", "b"}  # never the unchanged a, a1, a11
    assert dict(rest.requests) == {"descendants": 1, "lite_read": 2}
    # A confirming pass (before idle) is one listing too, whatever ran.
    rest.requests.clear()
    confirmed = await refresh_tree(
        rest, result.observation, [], confirm=True, confirm_busy=["a1", "a11", "b"]
    )
    assert rest.requests["descendants"] == 1 and rest.requests["children_page"] == 0
    assert rest.requests["inventory"] >= 1 and not confirmed.observation.quiescent
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_refresh_against_a_server_without_the_flag_lists_per_parent() -> None:
    server, rest = _deep_server(supported=False)
    cached = await scan_tree(rest, "root")
    server.add(FakeSession(id="a21", agent_id=AGENT, parent_session_id="a2", status="running"))
    rest.requests.clear()
    result = await refresh_tree(rest, cached, [], spawned=["a2"])
    assert result.observation.nodes["a21"].busy and result.observation.complete
    assert rest.requests["descendants"] == 1  # the root's children (no depth)
    assert rest.requests["children_page"] == 2  # a2's, then the new a21's
    await rest.aclose()


@pytest.mark.asyncio
async def test_a_failed_refresh_listing_is_never_idle() -> None:
    server, rest = _deep_server()
    server.sessions["a1"].status = "running"
    cached = await scan_tree(rest, "root")
    server.sessions["a1"].status = "idle"
    server.fail_paths.add(CHILDREN)
    result = await refresh_tree(rest, cached, ["a1"], confirm=True, confirm_busy=["a1"])
    assert not result.observation.complete and not result.observation.quiescent
    assert not result.confirmed
    await rest.aclose()


# ============================================================ cadence


def test_live_trees_reconcile_every_ten_minutes_by_default() -> None:
    assert RECONCILE_ACTIVE_SECONDS == 600.0
    fields = ServiceConfig.model_fields
    assert fields["observer_reconcile_active_seconds"].default == 600.0
    assert fields["observer_reconcile_settled_seconds"].default == 900.0
    assert fields["observer_reconcile_parked_seconds"].default == 1800.0
    assert SSE == "sse"


# ============================================================ retention


class _RetentionAdapter:
    """The two adapter calls retention makes, over a real inventory and scan."""

    def __init__(self, rest: OmnigentRest, index: SessionIndex) -> None:
        self.rest = rest
        self.index = index
        self.scans = 0
        self.last: dict[str, TreeObservation] = {}

    def inventory_root(self, root_id: str) -> Mapping[str, Any] | None:
        return self.index.root_row(root_id)

    async def observe_tree(self, root_id: str, *, reuse: bool = True) -> TreeObservation:
        """As the real adapter: the last complete scan stands in unless ``reuse=False``."""
        self.scans += 1
        obs = await scan_tree(
            self.rest, root_id, index=self.index, cached=self.last.get(root_id) if reuse else None
        )
        if obs.complete:
            self.last[root_id] = obs
        return obs


@pytest.mark.asyncio
async def test_retention_skips_finished_roots_from_the_inventory_without_reads() -> None:
    """Finished runs are not re-read every sweep: the inventory's rows decide which
    roots cannot be eligible; an eligible one is still confirmed, scanned and deleted."""
    server = FakeOmnigentServer()
    old = str(T0 - 40 * 86_400)
    label = {"factory.dispatch": "n"}
    server.add(FakeSession(id="open", agent_id=AGENT, labels=dict(label)))
    server.add(
        FakeSession(
            id="fresh",
            agent_id=AGENT,
            archived=True,
            labels={**label, ARCHIVED_AT_LABEL: str(T0 - 86_400)},
        )
    )
    server.add(
        FakeSession(
            id="done", agent_id=AGENT, archived=True, labels={**label, ARCHIVED_AT_LABEL: old}
        )
    )
    server.add(FakeSession(id="w", agent_id=AGENT, parent_session_id="done"))
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    index = SessionIndex(rest)
    await index.refresh()  # the observer's shared inventory (one full read)
    adapter = _RetentionAdapter(rest, index)
    clock = FakeClock()
    retention = SessionRetention(SimpleService(clock), adapter)  # type: ignore[arg-type]
    cutoff = T0 * 1_000_000 - 30 * 86_400_000_000
    rest.requests.clear()
    for _ in range(4):  # an hour of 15-minute sweeps
        assert await retention._eligible(Candidate("open", "I_1"), cutoff) == "not archived"
        assert await retention._eligible(Candidate("fresh", "I_2"), cutoff) == (
            "archived too recently"
        )
        clock.advance(900_000_000)
    assert sum(rest.requests.values()) == 0  # no request for roots that cannot be eligible
    assert await retention._eligible(Candidate("done", "I_3"), cutoff) is None
    assert rest.requests["lite_read"] >= 1 and adapter.scans == 1  # confirmed, then scanned
    await rest.aclose()


@pytest.mark.asyncio
async def test_retention_holds_a_busy_tree_instead_of_rescanning_it_every_sweep() -> None:
    server = FakeOmnigentServer()
    labels = {"factory.dispatch": "n", ARCHIVED_AT_LABEL: str(T0 - 40 * 86_400)}
    server.add(FakeSession(id="done", agent_id=AGENT, archived=True, labels=labels))
    server.add(FakeSession(id="w", agent_id=AGENT, parent_session_id="done", status="running"))
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    adapter = _RetentionAdapter(rest, SessionIndex(rest))
    clock = FakeClock()
    retention = SessionRetention(SimpleService(clock), adapter)  # type: ignore[arg-type]
    cutoff = T0 * 1_000_000 - 30 * 86_400_000_000
    candidate = Candidate("done", "I_3")
    assert await retention._eligible(candidate, cutoff) == "a session in its tree is not idle"
    for _ in range(3):
        clock.advance(900_000_000)
        assert await retention._eligible(candidate, cutoff) == "a session in its tree is not idle"
    assert adapter.scans == 1
    clock.advance(3_600_000_000)
    server.sessions["w"].status = "idle"
    assert await retention._eligible(candidate, cutoff) is None
    assert adapter.scans == 2
    await rest.aclose()


class SimpleService:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock


# ============================================================ feed


@pytest.mark.asyncio
async def test_a_pushed_row_carries_its_time_on_a_progress_mark() -> None:
    from tests.omnigent.test_push_observer import eventually, make_feed

    feed, streams, _socket, _rest = make_feed()
    feed.set_targets({"r"}, {"r": {"r": None, "c1": "r"}})
    await eventually(lambda: feed.coverage("r")[0] == SSE)
    row = {"busy": True, "current_task_status": "in_progress", "updated_at": T0}
    streams.push(
        "r",
        {
            "type": "session.child_session.updated",
            "conversation_id": "r",
            "child_session_id": "c1",
            "child": row,
        },
    )
    await eventually(lambda: feed.has_dirty("r"))
    feed.take_dirty("r")
    feed.wake.clear()
    streams.push(
        "r",
        {
            "type": "session.child_session.updated",
            "conversation_id": "r",
            "child_session_id": "c1",
            "child": {**row, "updated_at": T0 + 5},
        },
    )
    await eventually(lambda: feed.has_dirty("r"))
    mark = feed.take_dirty("r")["c1"]
    assert not mark.edge and not mark.spawn and mark.updated_at == T0 + 5
    assert not feed.wake.is_set()  # progress wakes no pass (and is never read)
    await feed.close()


# ============================================================ lifecycle evidence (review F1/F2)


@pytest.mark.asyncio
async def test_a_cache_miss_row_is_not_evidence_the_child_is_still_idle() -> None:
    """The server lists ``busy=false``/null status on a status-cache miss, and a status
    change does not move ``updated_at``: such an unchanged row never stands in for a read."""
    server, rest = _deep_server()
    server.sessions["b"].status_cached = False
    first = await scan_tree(rest, "root")
    assert first.quiescent and first.nodes["b"].task_status is None
    server.sessions["b"].status = "running"  # the row is identical: false/null, same time
    rest.requests.clear()
    again = await scan_tree(rest, "root", cached=first)
    assert dict(rest.requests) == {"descendants": 1, "inventory": 1, "lite_read": 2}
    assert again.nodes["b"].status == "running" and again.busy and not again.quiescent
    await rest.aclose()


@pytest.mark.asyncio
async def test_only_a_completed_row_on_a_completed_idle_node_skips_its_read() -> None:
    server, rest = _deep_server()
    server.sessions["a2"].current_task_status = "failed"  # a label forces it: no evidence
    first = await scan_tree(rest, "root")
    assert first.nodes["a"].task_status == "completed"
    rest.requests.clear()
    await scan_tree(rest, "root", cached=first)
    reads = {p for m, p, _ in server.requests if m == "GET" and p.count("/") == 3}
    assert rest.requests["lite_read"] == 2  # the root and the failed a2, never a/a1/a11/b
    assert "/v1/sessions/a2" in reads
    # A completed node that starts running moves its row (busy, in_progress): read.
    server.sessions["a1"].status = "running"
    again = await scan_tree(rest, "root", cached=first)
    assert again.nodes["a1"].status == "running" and again.busy
    await rest.aclose()


@pytest.mark.asyncio
async def test_retention_never_deletes_on_a_cached_idle_child() -> None:
    """A tree the adapter scanned before (idle, cache-miss rows) whose child is now
    running: retention reads every node, so the root is not eligible."""
    server = FakeOmnigentServer()
    labels = {"factory.dispatch": "n", ARCHIVED_AT_LABEL: str(T0 - 40 * 86_400)}
    server.add(FakeSession(id="done", agent_id=AGENT, archived=True, labels=labels))
    server.add(FakeSession(id="w", agent_id=AGENT, parent_session_id="done", status_cached=False))
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    adapter = _RetentionAdapter(rest, SessionIndex(rest))
    assert (await adapter.observe_tree("done")).quiescent  # cached by an earlier scan
    server.sessions["w"].status = "running"
    retention = SessionRetention(SimpleService(FakeClock()), adapter)  # type: ignore[arg-type]
    cutoff = T0 * 1_000_000 - 30 * 86_400_000_000
    verdict = await retention._eligible(Candidate("done", "I_3"), cutoff)
    assert verdict == "a session in its tree is not idle"
    assert server.count("DELETE", "/v1/sessions/done") == 0
    await rest.aclose()


@pytest.mark.asyncio
async def test_retention_declines_an_inventory_older_than_its_resync_bound() -> None:
    """No observer scans refresh the inventory: past ``resync_s`` its rows are not
    evidence, so a root archived since its last full read is read and found eligible."""
    server = FakeOmnigentServer()
    server.add(FakeSession(id="root", agent_id=AGENT, labels={"factory.dispatch": "n"}))
    rest = OmnigentRest("http://omnigent.test", transport=server.transport())
    clock = FakeClock()
    index = SessionIndex(rest, clock=clock, resync_s=6 * 3600.0)
    await index.refresh()  # the last refresh anyone makes
    adapter = _RetentionAdapter(rest, index)
    retention = SessionRetention(SimpleService(clock), adapter)  # type: ignore[arg-type]
    cutoff = T0 * 1_000_000 - 30 * 86_400_000_000
    candidate = Candidate("root", "I_1")
    rest.requests.clear()
    assert await retention._eligible(candidate, cutoff) == "not archived"
    assert sum(rest.requests.values()) == 0  # a fresh row: no request
    server.sessions["root"].archived = True
    server.sessions["root"].labels[ARCHIVED_AT_LABEL] = str(T0 - 40 * 86_400)
    clock.advance(6 * 3_600_000_000 + 1)  # the hold and the inventory both expire
    assert await retention._eligible(candidate, cutoff) is None
    assert rest.requests["lite_read"] >= 1
    await rest.aclose()
