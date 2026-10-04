"""#627: a stale background-task list held a checkpoint drain forever.

An idle Claude Code worker's snapshot kept listing two exited shells as ``running`` (the
server never refreshes ``background_tasks`` once a turn ends, not even after archiving),
so every scan was busy and the drain looped interrupt/scan every ~40 s while an accepted
/continue waited. Stale background tasks no longer count on an idle node with no active
task, and a drain that still cannot settle times out.
"""

from __future__ import annotations

import logging
from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event
from omnigent_factory.core.types import (
    MICROS_PER_HOUR,
    MICROS_PER_MINUTE,
    BotState,
    FenceKind,
    Lifecycle,
)
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.omnigent.tree import NodeState, scan_tree
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.omnigent.fake_server import FakeOmnigentServer, FakeSession
from tests.omnigent.support import AGENT

P = "I_parcel_1"
ROOT = "conv_root"
WORKER = "conv_216d10e0"
STALE_SHELLS = [
    {"id": "b3s512y8r", "type": "shell", "status": "running", "description": "Run DB tier"},
    {"id": "bm9w4x4hq", "type": "shell", "status": "running", "description": "DB tier done"},
]


def _server(**worker: object) -> tuple[FakeOmnigentServer, OmnigentRest]:
    server = FakeOmnigentServer()
    server.add(FakeSession(id=ROOT, agent_id=AGENT))
    server.add(
        FakeSession(
            id=WORKER,
            agent_id=AGENT,
            parent_session_id=ROOT,
            background_tasks=[dict(t) for t in STALE_SHELLS],
            **worker,  # type: ignore[arg-type]
        )
    )
    return server, OmnigentRest("http://o.test", transport=server.transport(), page_limit=2)


def _kinds(r) -> list[EffectKind]:
    return [e.kind for e in r.effects]


# ------------------------------------------------------------------ tree rule


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "worker",
    [
        # As first observed: idle, last task completed, two exited shells still "running".
        {"current_task_status": "completed"},
        # After the operator archived it: idle, archived, no task status at all.
        {"archived": True, "current_task_status": None},
    ],
    ids=["idle-completed", "archived-idle-no-task"],
)
async def test_stale_background_tasks_on_an_idle_node_are_quiescent(worker) -> None:
    _, rest = _server(**worker)
    obs = await scan_tree(rest, ROOT)
    node = obs.nodes[WORKER]
    assert node.background_active  # the snapshot still says "running"...
    assert not node.busy and not node.productive and not node.maybe_productive  # ...ignored
    assert obs.complete and obs.quiescent and not obs.to_scan().busy


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "worker",
    [
        {"status": "running", "current_task_status": "completed"},
        {"status": "waiting", "current_task_status": "completed"},
        {"status": "launching"},
        {"current_task_status": "in_progress"},
    ],
)
async def test_background_tasks_still_count_while_a_turn_is_active(worker) -> None:
    _, rest = _server(**worker)
    obs = await scan_tree(rest, ROOT)
    assert obs.nodes[WORKER].background_live and obs.busy and not obs.quiescent


def test_background_tasks_count_on_a_parked_live_turn() -> None:
    parked = NodeState(
        node_id=WORKER,
        parent_id=ROOT,
        status="waiting",
        elicitations=({"elicitation_id": "e1"},),
        background_active=True,
        task_status="completed",
    )
    assert parked.busy and parked.productive
    assert not replace(parked, background_active=False).busy  # a plain waiter


# ------------------------------------------------------------------ #627 replay


def _draining_with_pending_continue(h: Harness):
    """Checkpoint grace expired with the tree busy; /continue accepted; its PolicyReady
    landed while still draining (deferred)."""
    b = h.to_building()
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    assert h.cur().lifecycle == Lifecycle.DRAINING and FenceKind.CHECKPOINT in h.cur().fences
    assert h.send(P, ev.Continue(duration_us=2 * MICROS_PER_HOUR)).audit.accepted
    g = h.cur().grant
    assert not h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=g.grant_id)).effects
    return b, g


def _scan_event(session_id: str, obs) -> ev.TreeQuiescent:
    scan = obs.to_scan()
    return ev.TreeQuiescent(
        session_id=session_id,
        complete=scan.complete,
        busy=scan.busy,
        pending_waiter=scan.pending_waiter,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "worker",
    [{"current_task_status": "completed"}, {"archived": True, "current_task_status": None}],
    ids=["idle-completed", "archived-idle-no-task"],
)
async def test_627_drain_completes_and_pending_continue_resumes_once(worker) -> None:
    h = Harness()
    b, g = _draining_with_pending_continue(h)
    _, rest = _server(**worker)

    r = h.send(P, _scan_event(b.session_id, await scan_tree(rest, ROOT)))
    s = h.cur()
    assert s.lifecycle == Lifecycle.FENCED, "stale background tasks kept the drain looping"
    assert EffectKind.INTERRUPT_TREE not in _kinds(r)
    assert len(Harness.of(r, EffectKind.VERIFY_POLICIES)) == 1
    r = h.send(P, ev.PoliciesVerified(session_id=b.session_id, ok=True))
    s = h.cur()
    assert s.lifecycle == Lifecycle.ACTIVE and not s.fences and s.grant == replace(g, ready=True)
    assert len(Harness.of(r, EffectKind.SEND_MESSAGE)) == 1
    assert h.p().bot == BotState.WORKING
    # Later scans of the same stale snapshot never resume (or message) a second time.
    for _ in range(3):
        r = h.send(P, _scan_event(b.session_id, await scan_tree(rest, ROOT)))
        assert not Harness.of(r, EffectKind.SEND_MESSAGE)
        assert not Harness.of(r, EffectKind.VERIFY_POLICIES)
    sent = [e for _, res in h.log for e in res.effects if e.kind == EffectKind.SEND_MESSAGE]
    assert len([e for e in sent if e.preconditions.grant_id == g.grant_id]) == 1


# ------------------------------------------------------------------ drain timeout


def test_drain_records_its_start_and_clears_it_when_finished() -> None:
    h = Harness()
    b, _ = _draining_with_pending_continue(h)
    started = h.cur().drain_started_us
    assert started > 0
    h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    assert h.cur().drain_started_us == started  # a busy scan keeps the original start
    h.quiesce(P, b.session_id)
    assert h.p().session(b.session_id).drain_started_us == 0


def test_drain_timeout_finishes_a_checkpoint_drain_and_resumes_the_continue() -> None:
    h = Harness()
    b, g = _draining_with_pending_continue(h)
    h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    r = h.send(P, ev.StopTimeout(session_id=b.session_id))
    assert r.audit.accepted
    s = h.cur()
    assert s.lifecycle == Lifecycle.FENCED and s.drain_target is None
    assert len(Harness.of(r, EffectKind.VERIFY_POLICIES)) == 1
    r = h.send(P, ev.PoliciesVerified(session_id=b.session_id, ok=True))
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.cur().grant == replace(g, ready=True)
    assert len(Harness.of(r, EffectKind.SEND_MESSAGE)) == 1
    # A repeated timeout for the same (now settled) drain is refused.
    assert not h.send(P, ev.StopTimeout(session_id=b.session_id)).audit.accepted


def test_drain_timeout_without_continue_parks_at_the_checkpoint() -> None:
    h = Harness()
    b = h.to_building()
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    r = h.send(P, ev.StopTimeout(session_id=b.session_id))
    s = h.cur()
    assert s.lifecycle == Lifecycle.FENCED and FenceKind.CHECKPOINT in s.fences
    assert not Harness.of(r, EffectKind.SEND_MESSAGE)
    assert h.p().bot == BotState.CHECKPOINT


def _service(service_config: ServiceConfig, clock: FakeClock, **overrides: object):
    service = FactoryService(service_config.model_copy(update=overrides), clock=clock)
    applied: list[Event] = []

    async def record(event: Event, **_: object) -> None:
        applied.append(event)

    service.apply_event = record  # type: ignore[method-assign]
    service.busy_nodes = lambda root: (WORKER,) if root else ()
    return service, applied


@pytest.mark.asyncio
async def test_scheduler_fires_the_drain_timeout_once_due_and_names_busy_nodes(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
) -> None:
    h = Harness()
    b, _ = _draining_with_pending_continue(h)
    parcel = h.p()
    started = parcel.session(b.session_id).drain_started_us
    clock = FakeClock(start_us=started + 30 * MICROS_PER_MINUTE - 1)
    service, applied = _service(service_config, clock)
    assert service.config.drain_timeout_minutes == 30  # the default

    await service._expire_drains(parcel)
    assert applied == []  # not yet due
    clock.advance(1)
    with caplog.at_level(logging.WARNING):
        await service._expire_drains(parcel)
    assert [type(e.body) for e in applied] == [ev.StopTimeout]
    assert applied[0].body.session_id == b.session_id
    assert applied[0].event_id == f"drain-timeout:{b.session_id}:{started}"
    assert "drain timed out" in caplog.text and WORKER in caplog.text

    # The applied timeout settles the drain; the next tick has nothing to do.
    h.apply(applied[0])
    applied.clear()
    await service._expire_drains(h.p())
    assert applied == []


@pytest.mark.asyncio
async def test_drain_timeout_is_configurable_and_hot_reloadable(
    service_config: ServiceConfig,
) -> None:
    assert "drain_timeout_minutes" in HOT_RELOAD_KEYS
    h = Harness()
    b, _ = _draining_with_pending_continue(h)
    started = h.p().session(b.session_id).drain_started_us
    clock = FakeClock(start_us=started + 5 * MICROS_PER_MINUTE)
    service, applied = _service(service_config, clock, drain_timeout_minutes=60)
    await service._expire_drains(h.p())
    assert applied == []
    service.config = service.config.model_copy(update={"drain_timeout_minutes": 5})
    await service._expire_drains(h.p())
    assert len(applied) == 1


@pytest.mark.asyncio
async def test_drains_recorded_before_the_start_time_existed_are_left_alone(
    service_config: ServiceConfig,
) -> None:
    h = Harness()
    b, _ = _draining_with_pending_continue(h)
    s = h.p().session(b.session_id)
    parcel = replace(h.p(), sessions=(replace(s, drain_started_us=0),))
    service, applied = _service(service_config, FakeClock(start_us=s.drain_started_us * 2))
    await service._expire_drains(parcel)
    assert applied == []
