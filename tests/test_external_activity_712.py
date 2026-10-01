"""#712: the build run's own first turn was held as "external session activity".

The observer read the shared issue-session root after the build run was admitted but
labelled the busy tree with the parcel snapshot taken at the start of its pass, i.e. the
settled plan run. That parked the card at Needs you, left the plan run's activity flag
set for good (only the current run is observed) and, at Needs you, let an unexplained
Ready observation stand while the build ran; the owner's drag back then fenced the run.
"""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.predicates import all_settled
from omnigent_factory.core.projection import project_note
from omnigent_factory.core.types import (
    BotState,
    DecisionImpact,
    Hold,
    Lifecycle,
    Parcel,
    SessionKind,
    Stage,
    StageSession,
    Via,
)
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def plan_run(p: Parcel) -> StageSession:
    [plan] = [s for s in p.sessions if s.kind == SessionKind.PLAN]
    return plan


def run(p: Parcel, session_id: str) -> StageSession:
    s = p.session(session_id)
    assert s is not None
    return s


def test_busy_root_labelled_with_the_earlier_run_is_the_build_runs_own_work():
    h = Harness()
    build = h.to_building()
    plan = plan_run(h.p())
    assert plan.root_id == build.root_id and plan.lifecycle == Lifecycle.RETIRED
    r = h.send(P, ev.RuntimeActivity(session_id=plan.session_id, busy=True))
    p = h.p()
    assert Hold.EXTERNAL_ACTIVITY not in p.holds
    assert not any(s.external_active for s in p.sessions)
    assert p.bot == BotState.WORKING
    assert not Harness.of(r, EffectKind.INTERRUPT_TREE)
    assert run(p, build.session_id).lifecycle == Lifecycle.ACTIVE


def test_genuine_activity_on_a_settled_run_still_holds_but_asks_nothing_of_the_owner():
    h = Harness()
    build = h.to_building()
    h.send(P, ev.Stop())
    h.quiesce(P, build.session_id)
    h.send(P, ev.RuntimeActivity(session_id=build.session_id, busy=True))
    p = h.p()
    assert Hold.EXTERNAL_ACTIVITY in p.holds and not all_settled(p)
    assert p.bot != BotState.NEEDS_YOU
    # "Stopped: /stop" is the latest reason here; without one the derived note says why.
    note = project_note(replace(p, note=""), p.bot)
    assert note.startswith("Waiting: the Omnigent session is busy outside the factory")
    h.quiesce(P, build.session_id)
    assert Hold.EXTERNAL_ACTIVITY not in h.p().holds


def test_idle_root_clears_an_earlier_runs_stuck_activity_flag():
    """Live #712 state: the retired plan run kept ``external_active`` with no way out."""
    h = Harness()
    build = h.to_building()
    p = h.p()
    plan = plan_run(p)
    stuck = replace(plan, external_active=True, quiescent=False)
    h.parcels[P] = replace(
        p,
        sessions=tuple(stuck if s.session_id == plan.session_id else s for s in p.sessions),
        holds=p.holds | {Hold.EXTERNAL_ACTIVITY},
    )
    assert not all_settled(h.parcels[P])
    h.quiesce(P, build.session_id)
    p = h.p()
    assert not run(p, plan.session_id).external_active
    assert Hold.EXTERNAL_ACTIVITY not in p.holds


@pytest.mark.parametrize("via", ["snapshot", "column"])
def test_ready_while_the_build_runs_is_moved_back_even_at_needs_you(via):
    """Needs you outranks Working: the Bot value must not decide that no work runs."""
    h = Harness()
    build = h.to_building()
    h.send(
        P,
        ev.ElicitationOpened(
            session_id=build.session_id, elicitation_id="q1", impact=DecisionImpact.UNKNOWN
        ),
    )
    assert h.p().bot == BotState.NEEDS_YOU
    f = h.f()
    if via == "snapshot":
        body: ev.EventBody = ev.GitHubSnapshot()
        r = h.apply(f.make(body, evidence=snapshot(stage=Stage.READY, read_at_us=f.now)))
    else:
        r = h.send(P, ev.ColumnObserved(stage=Stage.READY))
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    assert move.args["to"] == Stage.BUILDING.value
    p = h.p()
    assert p.stage == Stage.BUILDING
    assert p.board_note == "Kept in Building: the bot is still working"
    assert run(p, build.session_id).lifecycle == Lifecycle.WAITING and not p.holds


class _Service:
    def __init__(self, fresh: Parcel) -> None:
        self.fresh = fresh
        self.applied: list[Any] = []
        self.config = SimpleNamespace(repo_id=fresh.repo_id)

    async def apply_event(self, event: Any) -> None:
        self.applied.append(event)


class _Adapter:
    async def observe_tree(self, root_id: str) -> Any:
        return SimpleNamespace(busy=True, complete=True, pending_waiter=False, root=None)


@pytest.mark.asyncio
async def test_observer_drops_a_tree_read_whose_run_changed_meanwhile():
    """The pass loaded the parcel before admission; the slow tree read saw the build."""
    h = Harness()
    h.plan_published()
    h.send(P, ev.ApprovePlan(via=Via.DRAG))
    stale = h.p()  # loaded before the plan run's quiescence closed it
    plan = stale.current_session
    assert plan is not None and plan.root_id is not None and not plan.execution_closed
    h.quiesce(P, plan.session_id)
    h.admit()
    assert h.cur().root_id == plan.root_id
    service = _Service(h.p())
    observer = OmnigentObserver(
        service,  # type: ignore[arg-type]
        _Adapter(),  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        FakeClock(),
        interval_seconds=1,
    )

    async def load(parcel_id: str) -> Parcel:
        return service.fresh

    observer._load_parcel = load  # type: ignore[method-assign]
    await observer._observe(stale)
    assert service.applied == []
