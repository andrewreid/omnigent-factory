"""A published plan with an unanswered question must resume when the owner answers."""

from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import (
    BotState,
    DecisionImpact,
    DecisionStatus,
    FenceKind,
    Hold,
    Lifecycle,
    Via,
    WaitReason,
)
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def test_342_answer_reactivates_plan_waiting_for_approval():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    run = h.create_ok()
    h.send(
        P,
        ev.OwnerQuestion(
            session_id=run.session_id,
            question_key="api",
            summary="Keep the API?",
            impact=DecisionImpact.PLAN_REVISION,
        ),
    )
    h.publish_plan()
    assert h.cur().wait_reason == WaitReason.PLAN_APPROVAL
    assert Hold.AWAITING_OWNER in h.p().holds
    refused = h.send(P, ev.ApprovePlan())
    assert refused.audit.reason == "plan-not-approvable"
    before = h.p().revision
    answered = h.send(P, ev.PlanFeedback(text_digest="keep"))
    assert h.cur().lifecycle == Lifecycle.ACTIVE
    assert h.cur().wait_reason is None
    assert Hold.AWAITING_OWNER not in h.p().holds
    assert h.p().bot == BotState.WORKING
    assert h.p().revision == before + 1 and h.p().revision_pending
    assert h.cur().revision == h.p().revision
    assert h.cur().grant == run.grant and h.cur().fences == run.fences
    assert any(
        e.args.get("purpose") == "answer_relay" for e in h.of(answered, EffectKind.SEND_MESSAGE)
    )
    h.publish_plan(goal="Keep the API")
    assert not h.p().revision_pending
    assert h.send(P, ev.ApprovePlan()).audit.accepted


def test_plan_feedback_still_resumes_with_existing_grant():
    h = Harness()
    h.plan_published()
    run = h.cur()
    result = h.send(P, ev.PlanFeedback(text_digest="Revise the scope"))
    assert result.audit.accepted
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.cur().wait_reason is None
    assert h.cur().grant == run.grant and h.cur().fences == run.fences
    assert Hold.AWAITING_OWNER not in h.p().holds
    assert not h.cur().comment_pending and not h.cur().quiescent
    assert h.p().revision_pending and h.cur().revision == run.revision + 1
    assert any(e.args.get("purpose") == "feedback" for e in result.effects)


@pytest.mark.parametrize("via", [Via.COMMAND, Via.DRAG])
@pytest.mark.parametrize(
    "condition", ["question", "revision", "unpublished", "edited", "superseded"]
)
def test_approval_refusal_names_the_failing_condition(via, condition):
    h = Harness()
    h.plan_published()
    if condition == "question":
        h.send(
            P, ev.OwnerQuestion(session_id=h.cur().session_id, question_key="q", summary="Which?")
        )
        detail = f"an owner question is open ({h.p().open_decisions[0].decision_id})"
    elif condition == "revision":
        h.send(P, ev.PlanFeedback(text_digest="Revise"))
        detail = "the plan is being revised after your answer/feedback"
    else:
        contract = h.p().current_contract
        assert contract is not None
        changed = replace(
            contract,
            published=condition != "unpublished",
            intact=condition != "edited",
            superseded=condition == "superseded",
        )
        h.parcels[P] = replace(h.p(), contracts=(changed,))
        detail = "the plan is not current/intact"
    result = h.send(P, ev.ApprovePlan(via=via))
    assert not result.audit.accepted and result.audit.reason == "plan-not-approvable"
    suffix = "; card moved back to Scoped" if via == Via.DRAG else ""
    assert h.p().note == f"Cannot approve: {detail}{suffix}"


@pytest.mark.parametrize("reason", list(WaitReason))
def test_plan_answer_resumes_other_wait_reasons(reason):
    h = Harness()
    h.plan_published()
    h.send(P, ev.OwnerQuestion(session_id=h.cur().session_id, question_key="q", summary="Which?"))
    h.parcels[P] = replace(
        h.p(),
        sessions=tuple(
            replace(s, wait_reason=reason) if s.session_id == h.cur().session_id else s
            for s in h.p().sessions
        ),
    )
    result = h.send(P, ev.PlanFeedback(text_digest="Yes"))
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.cur().wait_reason is None
    assert Hold.AWAITING_OWNER not in h.p().holds
    assert any(e.args.get("purpose") == "answer_relay" for e in result.effects)


@pytest.mark.parametrize("barrier", ["stop", "checkpoint", "policy"])
def test_answer_does_not_bypass_execution_barriers(barrier):
    h = Harness()
    h.plan_published()
    h.send(P, ev.OwnerQuestion(session_id=h.cur().session_id, question_key="q", summary="Which?"))
    if barrier == "stop":
        h.send(P, ev.Stop())
    elif barrier == "checkpoint":
        run = h.cur()
        h.send(P, ev.ActiveLimitReached(session_id=run.session_id, grant_id=run.grant.grant_id))
    else:
        h.parcels[P] = replace(
            h.p(),
            sessions=tuple(replace(s, policy_ready=False) for s in h.p().sessions),
        )
    before = h.cur()
    result = h.send(P, ev.PlanFeedback(text_digest="Yes"))
    assert h.p().revision_pending
    assert h.cur().lifecycle == before.lifecycle and h.cur().fences == before.fences
    assert not any(e.args.get("purpose") == "answer_relay" for e in result.effects)
    if barrier == "policy":
        assert h.p().decisions[-1].status == DecisionStatus.ANSWERED
    if barrier == "stop":
        assert FenceKind.STOPPED in h.cur().fences
