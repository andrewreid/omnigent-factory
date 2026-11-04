"""R3: an acknowledged ``answer_relay`` is the read of its answering comment, but only when
that comment is the sole unread one (matched by event id); 👍 posts once, never twice."""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import DecisionImpact
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
ANSWER = "github:comment:7001:created"
OTHER = "github:comment:7002:created"


def reactions(result) -> list[tuple[int, str]]:
    return [
        (e.args["comment_id"], e.args["content"])
        for e in Harness.of(result, EffectKind.REACT_COMMENT)
    ]


def asked_and_answered(h: Harness) -> tuple[str, str]:
    """A build asks the owner; the owner's comment answers it: (decision, relay effect)."""
    b = h.to_building(P)
    h.send(
        P,
        ev.OwnerQuestion(
            session_id=b.session_id,
            question_key="k",
            summary="Keep the v1 API?",
            impact=DecisionImpact.WITHIN_CONTRACT,
        ),
    )
    [d] = h.p().open_decisions
    r = h.send(P, ev.PlanFeedback(text_digest="keep it"), event_id=ANSWER)
    assert reactions(r) == [(7001, "eyes")]
    [relay] = [
        e
        for e in Harness.of(r, EffectKind.SEND_MESSAGE)
        if e.args["purpose"] == MessagePurpose.ANSWER_RELAY.value
    ]
    decision = h.p().decision(d.decision_id)
    assert decision is not None and decision.relay_effect_id == relay.effect_id
    assert not decision.relay_acked
    return d.decision_id, relay.effect_id


def ack(h: Harness, effect_id: str):
    return h.send(
        P, ev.MessageAck(session_id=h.cur().session_id, effect_id=effect_id, item_id="item-1")
    )


def test_ack_of_the_relay_reads_its_sole_unread_comment_once():
    h = Harness()
    decision_id, relay = asked_and_answered(h)
    r = ack(h, relay)
    assert r.audit.accepted and reactions(r) == [(7001, "+1")]
    up = Harness.of(r, EffectKind.REACT_COMMENT)[0]
    assert up.args["supersedes"] == "eyes"  # the 👀 is replaced, not left behind
    assert h.p().unread_feedback == ()
    decision = h.p().decision(decision_id)
    assert decision is not None and decision.relay_acked
    # The run's later factory_get_feedback finds nothing unread: no second 👍.
    again = h.send(P, ev.FeedbackRead(session_id=h.cur().session_id, event_ids=(ANSWER,)))
    assert not again.audit.accepted and not reactions(again)
    # A repeated ack (marker adoption after a restart) marks nothing again.
    assert not reactions(ack(h, relay))


def test_ack_marks_nothing_when_another_comment_is_unread():
    h = Harness()
    decision_id, relay = asked_and_answered(h)
    h.send(P, ev.PlanFeedback(text_digest="and rename the flag"), event_id=OTHER)
    assert h.p().unread_feedback == (ANSWER, OTHER)
    r = ack(h, relay)
    assert r.audit.accepted and not reactions(r)
    assert h.p().unread_feedback == (ANSWER, OTHER)  # factory_get_feedback reads both
    decision = h.p().decision(decision_id)
    assert decision is not None and decision.relay_acked
    read = h.send(P, ev.FeedbackRead(session_id=h.cur().session_id, event_ids=(ANSWER, OTHER)))
    assert sorted(reactions(read)) == [(7001, "+1"), (7002, "+1")]


def test_a_comment_already_read_gets_no_second_thumbs_up_at_ack():
    h = Harness()
    _, relay = asked_and_answered(h)
    read = h.send(P, ev.FeedbackRead(session_id=h.cur().session_id, event_ids=(ANSWER,)))
    assert reactions(read) == [(7001, "+1")]
    assert not reactions(ack(h, relay))


def test_an_ack_for_another_message_marks_no_answer_read():
    h = Harness()
    decision_id, _ = asked_and_answered(h)
    first = next(e for e, sid in h.p().sent_effects if sid == h.cur().session_id)
    assert not reactions(ack(h, first))
    decision = h.p().decision(decision_id)
    assert decision is not None and not decision.relay_acked
    assert h.p().unread_feedback == (ANSWER,)


# ------------------------------------------------------------------ held for build capacity

A, B = "I_slot_a", "I_slot_b"
HELD_ANSWER = "github:comment:7301:created"


def _admitted(h: Harness, pid: str):
    h.plan_published(pid)
    h.approve(pid)
    return h.admit(pid)


def _park(h: Harness, pid: str) -> None:
    from omnigent_factory.testing.builders import result_candidate

    s = h.cur(pid)
    r = h.send(pid, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    assert r.audit.accepted, r.audit.reason
    h.quiesce(pid, s.session_id)


def _relays(result) -> list:
    return [
        e
        for e in Harness.of(result, EffectKind.SEND_MESSAGE)
        if e.args["purpose"] == MessagePurpose.ANSWER_RELAY.value
    ]


def held_and_replayed(h: Harness) -> tuple[str, str]:
    """Build A asks and parks, B takes the slot, the owner's comment answers A (held for a
    slot), B parks, A resumes: the relay goes out under a fresh effect id."""
    a = _admitted(h, A)
    h.send(
        A,
        ev.OwnerQuestion(
            session_id=a.session_id,
            question_key="k",
            summary="Keep the v1 API?",
            impact=DecisionImpact.WITHIN_CONTRACT,
        ),
    )
    [d] = h.p(A).open_decisions
    h.quiesce(A, a.session_id)
    assert h.p(A).slot_parked
    _admitted(h, B)
    r = h.send(A, ev.PlanFeedback(text_digest="keep it"), event_id=HELD_ANSWER)
    assert r.audit.accepted and not _relays(r) and h.p(A).held_wakes  # held
    first = h.p(A).decision(d.decision_id)
    assert first is not None and not first.relay_acked
    _park(h, B)
    r = h.send(A, ev.CapacityAvailable())
    assert r.audit.accepted, r.audit.reason
    [relay] = _relays(r)
    decision = h.p(A).decision(d.decision_id)
    # Rebound to the effect actually sent (the held one was dropped from sent_effects).
    assert decision is not None and decision.relay_effect_id == relay.effect_id
    assert first.relay_effect_id != relay.effect_id
    assert not decision.relay_acked
    return d.decision_id, relay.effect_id


def test_a_relay_held_for_capacity_is_read_on_the_ack_of_its_replay():
    h = Harness()
    decision_id, relay = held_and_replayed(h)
    r = h.send(A, ev.MessageAck(session_id=h.cur(A).session_id, effect_id=relay, item_id="i"))
    assert r.audit.accepted and reactions(r) == [(7301, "+1")]
    decision = h.p(A).decision(decision_id)
    assert decision is not None and decision.relay_acked
    assert h.p(A).unread_feedback == ()


def test_a_relay_held_for_capacity_is_read_on_positive_reconciliation_only():
    h = Harness()
    decision_id, relay = held_and_replayed(h)
    sid = h.cur(A).session_id
    h.send(A, ev.EffectUnknown(effect_id=relay, effect_kind="send_message", session_id=sid))
    # Another session's reconciliation of the same id is refused (exact session match).
    wrong = h.send(
        A, ev.EffectReconciled(effect_id=relay, session_id="ss_other", delivered=True, item_id="i")
    )
    assert not reactions(wrong)
    r = h.send(A, ev.EffectReconciled(effect_id=relay, session_id=sid, delivered=True, item_id="i"))
    assert r.audit.accepted and reactions(r) == [(7301, "+1")]
    decision = h.p(A).decision(decision_id)
    assert decision is not None and decision.relay_acked


def test_a_relay_held_for_capacity_reconciled_absent_marks_nothing():
    h = Harness()
    decision_id, relay = held_and_replayed(h)
    sid = h.cur(A).session_id
    h.send(A, ev.EffectUnknown(effect_id=relay, effect_kind="send_message", session_id=sid))
    r = h.send(A, ev.EffectReconciled(effect_id=relay, session_id=sid, delivered=False))
    assert not reactions(r)
    decision = h.p(A).decision(decision_id)
    assert decision is not None and not decision.relay_acked
    assert h.p(A).unread_feedback == (HELD_ANSWER,)
