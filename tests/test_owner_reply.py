"""The owner's plain reply drives the factory (reducer rows).

While an owner question (``factory_ask_owner`` / a decision record) is open, the owner's
next plain comment on the issue or its PR answers it: the record is answered with that
comment, the answer is relayed to the run that asked, and the card returns to Working.
``/decide`` stays a (hidden) fallback. On a Building card at Needs you with no automatic
fix attempt left (``readiness_failed``) the reply clears that hold and starts a rework
with a fresh fix budget and time block (#477, owner comment 5903549471).
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import (
    BotState,
    DecisionImpact,
    DecisionStatus,
    Hold,
    Lifecycle,
    QueueStatus,
    SessionKind,
    Stage,
    Via,
    WaitReason,
)
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def ask(h: Harness, summary: str = "Re F2: change the label?", key: str = "k") -> str:
    s = h.cur()
    r = h.send(
        P,
        ev.OwnerQuestion(
            session_id=s.session_id,
            question_key=key,
            summary=summary,
            impact=DecisionImpact.WITHIN_CONTRACT,
        ),
    )
    assert r.audit.accepted
    [d] = h.p().open_decisions
    assert h.p().bot == BotState.NEEDS_YOU
    return d.decision_id


def reply(h: Harness, text: str = "Re: F2 - no change", pr: int = 0):
    return h.send(P, ev.PlanFeedback(text_digest=text, pr_number=pr))


def messages(result, purpose: MessagePurpose) -> list:
    return [
        e for e in Harness.of(result, EffectKind.SEND_MESSAGE) if e.args["purpose"] == purpose.value
    ]


# ------------------------------------------------------------------ A1: answers


def test_plain_comment_answers_the_open_build_question_and_the_card_works_again():
    h = Harness()
    h.to_building(P)
    decision_id = ask(h)
    assert h.cur().lifecycle == Lifecycle.WAITING and h.cur().wait_reason == WaitReason.DECISION
    r = reply(h)
    assert r.audit.accepted
    d = h.p().decision(decision_id)
    assert d is not None and d.status == DecisionStatus.RELAYED
    assert d.answer_event_id == r.audit.event_id and d.answer is None  # text: the comment
    [relay] = messages(r, MessagePurpose.ANSWER_RELAY)
    assert relay.args["decision_id"] == decision_id
    assert not messages(r, MessagePurpose.FEEDBACK)  # the answer, not also feedback
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.p().bot == BotState.WORKING
    assert h.p().current_approval.valid  # a build is never revoked by a plain reply


def test_plain_pr_comment_answers_the_open_question():
    h = Harness()
    h.to_building(P)
    decision_id = ask(h)
    r = reply(h, "keep it as is", pr=7)
    assert r.audit.accepted
    assert h.p().decision(decision_id).status == DecisionStatus.RELAYED
    assert messages(r, MessagePurpose.ANSWER_RELAY) and h.p().bot == BotState.WORKING


def test_plain_comment_answers_a_plan_question_as_a_revision():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    h.create_ok(P)
    revision = h.p().revision
    decision_id = ask(h, "Keep the v1 API?")
    r = reply(h, "keep it")
    assert r.audit.accepted
    assert h.p().decision(decision_id).status == DecisionStatus.RELAYED
    assert messages(r, MessagePurpose.ANSWER_RELAY)
    assert not messages(r, MessagePurpose.FEEDBACK)
    assert h.p().revision == revision + 1 and h.p().revision_pending
    assert h.p().bot == BotState.WORKING


def test_plain_comment_answers_a_triage_question():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    h.create_ok(P)
    decision_id = ask(h, "Is this a duplicate of #12?")
    r = reply(h, "no, it is not")
    assert r.audit.accepted
    assert h.p().decision(decision_id).status == DecisionStatus.RELAYED
    assert messages(r, MessagePurpose.ANSWER_RELAY) and h.p().bot == BotState.WORKING


def test_decide_is_still_a_fallback():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    h.create_ok(P)
    decision_id = ask(h, "Keep the v1 API?")
    r = h.send(P, ev.Decide(decision_id=decision_id, answer="keep"))
    assert r.audit.accepted
    d = h.p().decision(decision_id)
    assert d.status == DecisionStatus.RELAYED and d.answer == "keep"


# ------------------------------------------------------------------ A2: #477


def needs_you_with_open_question(h: Harness) -> str:
    """#477 on 2026-09-30: the build asked (F2), then submitted build_ready with the
    question still open; the review-bot finding had no outcome, and with a question open
    the one automatic wake could not run: Needs you, ``readiness_failed``."""
    h.to_building(P)
    decision_id = ask(h, 'Re F2: "Leave" is not a defined leave code. Change it?')
    s = h.cur()
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha=HEAD,
        ),
    )
    assert r.audit.accepted
    h.send(
        P,
        ev.PRObserved(pr_number=7, head_sha=HEAD, open=True, bot_authored=True, parcel_branch=True),
    )
    h.quiesce(P, s.session_id)
    h.send(
        P,
        ev.ReadinessEvidence(
            session_id=s.session_id,
            pr_number=7,
            head_sha=HEAD,
            verified=False,
            checks=ev.ChecksState.GREEN,
            findings_open=True,
            review_accepted=True,
            closes_issue=True,
        ),
    )
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert Hold.READINESS_FAILED in p.holds and p.open_decisions
    assert h.cur().lifecycle == Lifecycle.WAITING and h.cur().wait_reason == WaitReason.CHECKS
    return decision_id


def test_477_owner_reply_with_question_open_and_readiness_failed_starts_rework():
    h = Harness()
    decision_id = needs_you_with_open_question(h)
    old = h.cur()
    approval = h.p().current_approval_id
    r = reply(h, "Re: F2 - no change - not reachable, covered by API-level checks")
    assert r.audit.accepted
    p = h.p()
    # The question is answered by this comment; its text reaches the rework run with it.
    d = p.decision(decision_id)
    assert d.status == DecisionStatus.RELAYED and d.answer_event_id == r.audit.event_id
    assert not p.open_decisions
    # The hold is cleared and a rework starts: fresh fix budget, fresh time block.
    assert not p.holds & {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED}
    assert p.readiness_wakes == 0 and p.readiness is None
    assert p.note == "Rework: owner feedback" and p.current_approval_id == approval
    entry = h.admission.queue_entry(P)
    assert entry is not None and entry.status == QueueStatus.QUEUED
    auth = p.authorizations[-1]
    assert auth.kind == SessionKind.BUILD and auth.rework and auth.approval_id == approval
    # The idle run at Needs you makes way (drained, never messaged twice).
    assert h.p().session(old.session_id).lifecycle == Lifecycle.DRAINING
    assert not Harness.of(r, EffectKind.SEND_MESSAGE)
    assert p.bot != BotState.NEEDS_YOU
    # Once the old tree is idle, the rework run starts in the same issue session.
    h.quiesce(P, old.session_id)
    assert h.send(P, ev.CapacityAvailable()).audit.accepted
    run = h.create_ok(P)
    assert run.session_id != old.session_id and run.root_id == old.root_id
    assert run.lifecycle == Lifecycle.ACTIVE and h.p().bot == BotState.WORKING
    assert run.grant.duration_us == h.cfg.block_us(h.p().size)
    assert run.grant.grant_id != old.grant.grant_id


def test_readiness_failed_without_a_question_also_reworks_on_a_reply():
    h = Harness()
    h.to_building(P)
    s = h.cur()
    assert s.root_id is not None
    submit = result_candidate(
        s.session_id, s.root_id, s.revision, ev.ResultKind.BUILD_READY, pr_number=7, head_sha=HEAD
    )
    evidence = ev.ReadinessEvidence(
        session_id=s.session_id,
        pr_number=7,
        head_sha=HEAD,
        verified=False,
        checks=ev.ChecksState.GREEN,
        findings_open=True,
        review_accepted=True,
        closes_issue=True,
    )
    h.send(P, submit)
    h.send(
        P,
        ev.PRObserved(pr_number=7, head_sha=HEAD, open=True, bot_authored=True, parcel_branch=True),
    )
    h.quiesce(P, s.session_id)
    h.send(P, evidence)  # the one automatic wake
    assert h.p().readiness_wakes == 1
    h.send(P, submit)
    h.quiesce(P, s.session_id)
    h.send(P, evidence)
    assert Hold.READINESS_FAILED in h.p().holds and not h.p().open_decisions
    assert h.cur().lifecycle == Lifecycle.WAITING
    r = reply(h, "leave it")
    assert r.audit.accepted and Hold.READINESS_FAILED not in h.p().holds
    assert h.p().authorizations[-1].rework and h.p().readiness_wakes == 0
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED
    assert not Harness.of(r, EffectKind.SEND_MESSAGE)
