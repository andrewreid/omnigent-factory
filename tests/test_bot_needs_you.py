"""Bot "Needs you" only when the current stage cannot continue without the owner.

An open question or a hold that needs the owner's decision is Needs you. A stage that
finished with the owner's move next (triage posted in Triaged, plan posted awaiting
approval in Scoped, Ready) is Idle. Cards persisted with the old value self-correct on
the next event (the periodic reconcile), written once.
"""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.projection import project_bot, project_note
from omnigent_factory.core.types import BotState, DecisionImpact, Hold, Stage, Via
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def test_triage_posted_is_idle_not_needs_you():
    h = Harness()
    h.triage(P)
    h.ack_moves(P)
    p = h.p()
    assert p.stage == Stage.TRIAGED and Hold.AWAITING_OWNER in p.holds
    assert p.bot == BotState.IDLE and project_note(p, p.bot) == ""


def test_plan_posted_awaiting_approval_is_idle():
    h = Harness()
    s = h.plan_published(P)
    h.quiesce(P, s.session_id)
    p = h.p()
    assert p.current_contract is not None and Hold.AWAITING_OWNER in p.holds
    assert p.bot == BotState.IDLE


def test_ready_is_idle():
    h = Harness()
    h.to_building(P)
    h.build_ready(P)
    assert h.p().stage == Stage.READY and h.p().bot == BotState.IDLE


def test_open_question_and_decision_holds_are_needs_you():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.create_ok(P)
    h.send(
        P,
        ev.OwnerQuestion(
            session_id=s.session_id, question_key="k", summary="?", impact=DecisionImpact.UNKNOWN
        ),
    )
    assert h.p().bot == BotState.NEEDS_YOU
    idle = replace(h.p(), decisions=())
    for hold in (Hold.READINESS_FAILED, Hold.CHECKS_FAILED, Hold.PR_CLOSED):
        assert project_bot(replace(idle, holds=frozenset({hold}))) == BotState.NEEDS_YOU
    assert project_bot(replace(idle, holds=frozenset({Hold.AWAITING_OWNER}))) != (
        BotState.NEEDS_YOU
    )


def test_stale_needs_you_card_self_corrects_once_on_reconcile():
    h = Harness()
    h.triage(P)
    h.ack_moves(P)
    # Persisted by the previous release: Needs you for a posted triage.
    h.parcels[P] = replace(h.p(), bot=BotState.NEEDS_YOU, board_note="")
    r = h.send(P, ev.ReconcileDue())
    [write] = Harness.of(r, EffectKind.SET_BOT)
    assert write.args == {"bot": BotState.IDLE.value} and h.p().bot == BotState.IDLE
    again = h.send(P, ev.ReconcileDue())
    assert not Harness.of(again, EffectKind.SET_BOT)  # written only on change
