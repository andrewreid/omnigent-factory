"""Tree-scan load on the Omnigent server: cost proportional to the factory's own trees.

A scan used to re-read the archive-inclusive inventory of *every* session in the
instance (all pages) on each call, so its cost grew with unrelated history. The shared
:class:`SessionIndex` now reads it once and then only the newest page(s); every safety
property of the scan (archived intermediate parents, fail-closed reads) is unchanged.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import httpx
import pytest

from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.types import Lifecycle, Parcel
from omnigent_factory.omnigent.inventory import SessionIndex
from omnigent_factory.omnigent.rest import OmnigentReadError, OmnigentRest
from omnigent_factory.omnigent.tree import scan_tree
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import T0, FakeOmnigentServer, FakeSession
from tests.omnigent.support import AGENT, CTX, intent, make_rig

pytestmark = pytest.mark.asyncio

ROOT = "conv_root"
LIST = ("GET", "/v1/sessions")
NEW = T0 + 100_000  # the factory's tree is newer than the unrelated history


def _unrelated(server: FakeOmnigentServer, n: int) -> None:
    """``n`` older sessions of other agents: roots, children, archived, 10 s apart."""
    for i in range(n):
        parent = f"conv_u{i - 1:05d}" if i % 3 == 1 else None
        server.add(
            FakeSession(
                id=f"conv_u{i:05d}",
                agent_id="ag_other",
                parent_session_id=parent,
                archived=i % 2 == 0,
                created_at=T0 - 10 * (n - i),
            )
        )


def _tree(server: FakeOmnigentServer) -> None:
    """Root, two live children and an archived intermediate parent of a running child."""
    server.add(FakeSession(id=ROOT, agent_id=AGENT, created_at=NEW))
    for i in range(2):
        server.add(
            FakeSession(id=f"conv_c{i}", agent_id=AGENT, parent_session_id=ROOT, created_at=NEW)
        )
    server.add(
        FakeSession(
            id="conv_arch", agent_id=AGENT, parent_session_id=ROOT, archived=True, created_at=NEW
        )
    )
    server.add(
        FakeSession(
            id="conv_hidden",
            agent_id=AGENT,
            parent_session_id="conv_arch",
            status="running",
            created_at=NEW,
        )
    )


def _requests(server: FakeOmnigentServer, since: int) -> list[tuple[str, str]]:
    return [(m, p) for m, p, _ in server.requests[since:]]


async def _steady_scan_requests(git_env: GitEnv, unrelated: int) -> list[tuple[str, str]]:
    rig = make_rig(git_env, page_limit=100)
    _unrelated(rig.server, unrelated)
    _tree(rig.server)
    await rig.adapter.observe_tree(ROOT)  # first scan: one full inventory read
    before = len(rig.server.requests)
    obs = await rig.adapter.observe_tree(ROOT)
    assert obs.complete and obs.busy
    assert {"conv_arch", "conv_hidden"} <= set(obs.nodes)
    return _requests(rig.server, before)


async def test_scan_cost_is_independent_of_unrelated_instance_sessions(git_env: GitEnv) -> None:
    small = await _steady_scan_requests(git_env, 50)
    large = await _steady_scan_requests(git_env, 5_000)
    assert len(large) == len(small)
    assert large.count(LIST) == 1  # one incremental page, not ~51 inventory pages
    # Everything else is the tree's own: child lists of the live nodes, then snapshots.
    # inventory page + child lists of the live nodes (root, c0, c1) + five snapshots
    assert len(large) == 1 + 3 + 5


def _yielding(server: FakeOmnigentServer) -> OmnigentRest:
    """A transport that yields to the loop per request, like a real socket."""

    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0)
        return server.handle(request)

    return OmnigentRest("http://o.test", transport=httpx.MockTransport(handler), page_limit=100)


async def test_concurrent_callers_share_inventory_reads() -> None:
    server = FakeOmnigentServer()
    _unrelated(server, 1_000)
    _tree(server)
    index = SessionIndex(_yielding(server))
    # The first caller starts the full read; the four arriving while it runs share one
    # follow-up read (they may not be answered from a read older than their request).
    await asyncio.gather(*(index.refresh() for _ in range(5)))
    assert server.count(*LIST) == 11 + 1  # 1,005 rows at 100 a page, then one page
    before = server.count(*LIST)
    await asyncio.gather(*(index.refresh() for _ in range(20)))
    assert server.count(*LIST) - before == 2  # however many callers
    assert index.children("conv_arch") == ("conv_hidden",)


async def test_a_caller_never_joins_a_read_that_started_before_it_asked() -> None:
    server = FakeOmnigentServer()
    server.add(FakeSession(id=ROOT, agent_id=AGENT, created_at=NEW))
    entered, release = asyncio.Event(), asyncio.Event()
    gated = [True]

    async def handler(request: httpx.Request) -> httpx.Response:
        response = server.handle(request)  # the first read's page is taken now...
        if gated[0] and request.url.path == "/v1/sessions":
            gated[0] = False
            entered.set()
            await release.wait()  # ...and answered later
        return response

    index = SessionIndex(OmnigentRest("http://o.test", transport=httpx.MockTransport(handler)))
    first = asyncio.create_task(index.refresh())
    await asyncio.wait_for(entered.wait(), 5)
    # Created after the first read began: a caller asking now must see it.
    server.add(FakeSession(id="conv_late", agent_id=AGENT, parent_session_id=ROOT, created_at=NEW))
    second = asyncio.create_task(index.refresh())
    await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second), 5)
    assert index.children(ROOT) == ("conv_late",)
    assert server.count(*LIST) == 2


async def test_archived_parent_created_after_the_first_read_is_still_closed_over(
    git_env: GitEnv,
) -> None:
    rig = make_rig(git_env, page_limit=2)
    _unrelated(rig.server, 50)
    for i in range(1, 6):  # recent history just below the factory's root
        rig.server.add(FakeSession(id=f"conv_r{i}", agent_id="ag_other", created_at=NEW - 10 * i))
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, created_at=NEW))
    assert (await rig.adapter.observe_tree(ROOT)).quiescent
    # A replica with a slow clock stamps the new intermediate parent 2 min in the past:
    # it sorts below rows the last read already had, inside the re-read overlap.
    rig.server.add(
        FakeSession(
            id="conv_arch",
            agent_id=AGENT,
            parent_session_id=ROOT,
            archived=True,
            created_at=NEW - 120,
        )
    )
    rig.server.add(
        FakeSession(
            id="conv_hidden",
            agent_id=AGENT,
            parent_session_id="conv_arch",
            status="running",
            created_at=NEW + 5,
        )
    )
    obs = await rig.adapter.observe_tree(ROOT)
    assert {"conv_arch", "conv_hidden"} <= set(obs.nodes)
    assert obs.complete and obs.busy and not obs.quiescent


@pytest.mark.parametrize("fault", [503, "timeout-before"])
async def test_failed_incremental_read_fails_closed_then_recovers(
    git_env: GitEnv, fault: Any
) -> None:
    rig = make_rig(git_env, page_limit=100)
    _unrelated(rig.server, 300)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, created_at=NEW))
    assert (await rig.adapter.observe_tree(ROOT)).quiescent
    rig.server.faults[LIST].append(fault)
    obs = await rig.adapter.observe_tree(ROOT)
    assert not obs.complete and not obs.quiescent
    assert any(e.startswith("inventory:") for e in obs.errors)
    assert (await rig.adapter.observe_tree(ROOT)).quiescent


async def test_a_full_read_failing_mid_pagination_is_retried_by_the_next_scan() -> None:
    server = FakeOmnigentServer()
    _unrelated(server, 30)
    server.add(FakeSession(id=ROOT, agent_id=AGENT, created_at=NEW))
    rest = OmnigentRest("http://o.test", transport=server.transport(), page_limit=5)
    index = SessionIndex(rest)
    server.faults[LIST].extend([None, None, 503])  # full read dies on its third page
    with pytest.raises(OmnigentReadError):
        await index.refresh()
    obs = await scan_tree(rest, ROOT, index=index)
    assert obs.complete and obs.root is not None


async def test_periodic_full_read_drops_deleted_sessions() -> None:
    server = FakeOmnigentServer()
    _unrelated(server, 30)
    server.add(FakeSession(id=ROOT, agent_id=AGENT, created_at=NEW))
    server.add(FakeSession(id="conv_gone", agent_id=AGENT, parent_session_id=ROOT, created_at=NEW))
    clock = FakeClock()
    rest = OmnigentRest("http://o.test", transport=server.transport(), page_limit=100)
    index = SessionIndex(rest, clock=clock, resync_s=60)
    await index.refresh()
    del server.sessions["conv_gone"]
    await index.refresh()
    assert index.children(ROOT) == ("conv_gone",)  # incremental reads only add
    clock.advance(60_000_000)
    await index.refresh()
    assert index.children(ROOT) == ()


async def test_nonce_search_pages_roots_only(git_env: GitEnv) -> None:
    rig = make_rig(git_env, page_limit=10)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, labels={"factory.dispatch": "n-1"}))
    for i in range(200):
        rig.server.add(FakeSession(id=f"conv_k{i}", agent_id=AGENT, parent_session_id=ROOT))
    found = await rig.adapter.find_by_nonce("n-1")
    assert [m.root_id for m in found] == [ROOT]  # type: ignore[union-attr]
    assert rig.server.count(*LIST) == 1


# ------------------------------------------------------------------ scan pacing


async def test_drain_rescans_of_a_busy_tree_are_spaced_out(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, status="running"))
    scan = intent(EffectKind.SCAN_TREE, root_id=ROOT)
    first = await rig.adapter.execute(scan, CTX)
    assert isinstance(first, Ack) and first.detail["busy"] and rig.sleeps == []
    reads = rig.server.count("GET", f"/v1/sessions/{ROOT}")
    second = await rig.adapter.execute(scan, CTX)
    assert isinstance(second, Ack) and second.detail["busy"]
    assert rig.sleeps == [5.0]
    assert rig.server.count("GET", f"/v1/sessions/{ROOT}") == reads + 1  # read after waiting
    rig.server.sessions[ROOT].status = "idle"
    await rig.adapter.execute(scan, CTX)  # waits once more, then reads idle
    await rig.adapter.execute(scan, CTX)  # latest read quiescent: no wait
    assert rig.sleeps == [5.0, 5.0]


async def test_rescan_after_the_interval_has_passed_does_not_wait(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, status="running"))
    scan = intent(EffectKind.SCAN_TREE, root_id=ROOT)
    await rig.adapter.execute(scan, CTX)
    rig.clock.advance(5_000_000)
    await rig.adapter.execute(scan, CTX)
    assert rig.sleeps == []


# ------------------------------------------------------------------ observer cadence


class _CountingAdapter:
    def __init__(self) -> None:
        self.reads: list[str] = []

    async def observe_tree(self, root_id: str) -> Any:
        self.reads.append(root_id)
        raise OmnigentReadError("not needed")

    async def observe_trees(self, root_ids: list[str]) -> list[Any]:
        self.reads.extend(root_ids)
        return [OmnigentReadError("not needed") for _ in root_ids]


def _with_lifecycle(p: Parcel, lifecycle: Lifecycle) -> Parcel:
    cur = p.current_session
    assert cur is not None
    return replace(
        p,
        sessions=tuple(
            replace(s, lifecycle=lifecycle) if s.session_id == cur.session_id else s
            for s in p.sessions
        ),
    )


async def test_observer_reads_settled_trees_at_the_slower_cadence() -> None:
    h = Harness()
    building = h.to_building()
    assert building.root_id is not None and not building.execution_closed
    live = h.p()
    fenced = _with_lifecycle(live, Lifecycle.FENCED)
    assert fenced.current_session is not None and not fenced.current_session.execution_closed
    clock = FakeClock()
    adapter = _CountingAdapter()
    observer = OmnigentObserver(
        None,  # type: ignore[arg-type]
        adapter,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        clock,
        interval_seconds=5,
        settled_interval_seconds=30,
    )
    current = [fenced]

    async def parcels() -> list[Parcel]:
        return current

    async def activity(*_: Any) -> None:
        return None

    observer._parcels = parcels  # type: ignore[method-assign]
    observer._activity = activity  # type: ignore[method-assign]
    for _ in range(6):  # 30 s of 5 s passes: one read
        await observer.observe_once()
        clock.advance(5_000_000)
    assert len(adapter.reads) == 1
    await observer.observe_once()
    assert len(adapter.reads) == 2
    current = [live]  # no longer settled: every pass again
    for _ in range(3):
        await observer.observe_once()
    assert len(adapter.reads) == 5


async def test_an_observer_pass_shares_one_inventory_read(git_env: GitEnv) -> None:
    """``observe_trees``: every walk, then one inventory read, then each scan's snapshots.

    The read still starts after each tree's walk (an archived intermediate parent is
    found), and a failed read makes every scan of the pass incomplete, never idle.
    """
    rig = make_rig(git_env, page_limit=100)
    _unrelated(rig.server, 50)
    _tree(rig.server)
    for i in range(2):
        rig.server.add(FakeSession(id=f"conv_r{i}", agent_id=AGENT, created_at=NEW))
    roots = [ROOT, "conv_r0", "conv_r1"]
    await rig.adapter.observe_tree(ROOT)  # the first, full inventory read
    before = rig.server.count(*LIST)
    results = await rig.adapter.observe_trees(roots)
    assert rig.server.count(*LIST) == before + 1  # one read for three trees
    first = results[0]
    assert not isinstance(first, Exception)
    assert first.complete and first.busy and {"conv_arch", "conv_hidden"} <= set(first.nodes)
    assert all(not isinstance(r, Exception) and r.quiescent for r in results[1:])
    rig.server.fail_paths.add("/v1/sessions")
    failed = await rig.adapter.observe_trees(["conv_r0", "conv_r1"])
    for result in failed:
        assert not isinstance(result, Exception)
        assert not result.complete and not result.quiescent
