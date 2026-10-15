"""Owner feedback reaches a busy build at once (#884), coalesced, and is acknowledged.

#884: the owner commented while the build's root had ended its turn and only a Codex
worker ran. The factory saw the tree busy and only set ``comment_pending``; the comment
reached the run 15 minutes later, when it tried to submit. A busy build now gets the
comment as a mid-build message at once; later comments inside the coalescing window are
folded into one relay on the next reconcile after the window (or the turn's end); no
comment is relayed twice, across a restart included. Every accepted issue comment gets
👀, and 👍 once a run has read it.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.reducer import (
    FEEDBACK_COALESCE_US,
    FEEDBACK_QUEUED_NOTE,
    FEEDBACK_RELAYED_NOTE,
)
from omnigent_factory.core.types import Lifecycle
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
C1 = "github:comment:6096243669:created"
C2 = "github:comment:6096243700:created"


def feedback(result) -> list:
    return [
        e
        for e in Harness.of(result, EffectKind.SEND_MESSAGE)
        if e.args.get("purpose") == MessagePurpose.FEEDBACK.value
    ]


def reactions(result) -> list[tuple[int, str]]:
    return [
        (e.args["comment_id"], e.args["content"])
        for e in Harness.of(result, EffectKind.REACT_COMMENT)
    ]


def comment(h: Harness, event_id: str | None = None, text: str = "use the group", **kw):
    return h.send(P, ev.PlanFeedback(text_digest=text), event_id=event_id, **kw)


def busy_build(h: Harness):
    """#884's shape: the build root ended its turn, a worker still runs (tree busy)."""
    b = h.to_building()
    h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    assert h.cur().lifecycle == Lifecycle.ACTIVE and not h.cur().quiescent
    return b


def restart(h: Harness) -> None:
    """The daemon restarts: the parcel is reloaded from its stored JSON."""
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))


# ------------------------------------------------------------------ Fix 1: relay


def test_884_replay_busy_build_gets_the_comment_at_once_and_once():
    h = Harness()
    b = busy_build(h)
    r = comment(h, C1)
    assert r.audit.accepted
    [msg] = feedback(r)
    assert msg.args["midturn"] is True and msg.preconditions.session_id == b.session_id
    run = h.cur()
    assert not run.comment_pending and run.feedback_relayed_us == h.f(P).now
    assert run.feedback_wakes == 1 and not run.fences
    assert h.p().note == FEEDBACK_RELAYED_NOTE
    # The same webhook again (a redelivery, or a replay after a crash): a duplicate.
    again = h.apply(h.f(P).make(ev.PlanFeedback(text_digest="use the group"), event_id=C1))
    assert again.duplicate and again.effects == ()
    # Restart, then the turn ends: nothing pending, so nothing is relayed again.
    restart(h)
    assert not feedback(h.quiesce(P, b.session_id))
    assert not feedback(h.send(P, ev.ReconcileDue(), time_us=h.f(P).now + FEEDBACK_COALESCE_US))


def test_884_second_comment_inside_the_window_is_coalesced_then_flushed_once():
    h = Harness()
    busy_build(h)
    [_] = feedback(comment(h, C1))
    relayed_at = h.cur().feedback_relayed_us
    r = comment(h, C2, text="and drop the password")
    assert r.audit.accepted and not feedback(r)
    assert h.cur().comment_pending and h.p().note == FEEDBACK_QUEUED_NOTE
    # A reconcile inside the window sends nothing.
    assert not feedback(h.send(P, ev.ReconcileDue()))
    # Restart before the window ends: the folded comment survives in stored state.
    restart(h)
    assert h.cur().comment_pending and h.cur().feedback_relayed_us == relayed_at
    # The first reconcile after the window relays it, once.
    [msg] = feedback(h.send(P, ev.ReconcileDue(), time_us=relayed_at + FEEDBACK_COALESCE_US))
    assert msg.args["midturn"] is True
    assert not h.cur().comment_pending and h.cur().feedback_wakes == 2
    assert not feedback(h.send(P, ev.ReconcileDue()))
    assert not feedback(h.send(P, ev.ReconcileDue(), time_us=h.f(P).now + FEEDBACK_COALESCE_US))


def test_a_comment_after_the_window_is_relayed_at_once():
    h = Harness()
    busy_build(h)
    [_] = feedback(comment(h, C1))
    later = h.cur().feedback_relayed_us + FEEDBACK_COALESCE_US
    [msg] = feedback(comment(h, C2, time_us=later))
    assert msg.args["midturn"] is True and h.cur().feedback_wakes == 2


def test_a_folded_comment_goes_with_the_turn_end_when_that_comes_first():
    h = Harness()
    b = busy_build(h)
    feedback(comment(h, C1))
    comment(h, C2)
    [msg] = feedback(h.quiesce(P, b.session_id))  # idle relay, as before
    assert "midturn" not in msg.args
    assert not h.cur().comment_pending
    # The reconcile after the window finds nothing left to send.
    assert not feedback(
        h.send(P, ev.ReconcileDue(), time_us=h.cur().feedback_relayed_us + FEEDBACK_COALESCE_US)
    )


def test_a_gated_busy_build_only_records_the_comment():
    """Stopped: the work gate is closed, nothing is relayed (comment_pending as before)."""
    h = Harness()
    busy_build(h)
    h.send(P, ev.Stop())
    r = comment(h, C1)
    assert r.audit.accepted and not feedback(r)
    assert not feedback(h.send(P, ev.ReconcileDue(), time_us=h.f(P).now + FEEDBACK_COALESCE_US))


# ------------------------------------------------------------------ Fix 4: acknowledge


def test_accepted_issue_comment_gets_eyes_then_thumbs_up_once_read():
    h = Harness()
    b = busy_build(h)
    assert reactions(comment(h, C1)) == [(6096243669, "eyes")]
    assert reactions(comment(h, C2)) == [(6096243700, "eyes")]
    assert h.p().unread_feedback == (C1, C2)
    # The run read the first comment only (C2 had not arrived in this sketch).
    r = h.send(P, ev.FeedbackRead(session_id=b.session_id, event_ids=(C1,)))
    assert r.audit.accepted and reactions(r) == [(6096243669, "+1")]
    assert h.p().unread_feedback == (C2,)
    assert h.p().note.startswith("Feedback received")  # one still unread
    r = h.send(P, ev.FeedbackRead(session_id=b.session_id, event_ids=(C1, C2)))
    assert reactions(r) == [(6096243700, "+1")]
    assert h.p().unread_feedback == () and h.p().note == ""
    # Nothing left unread: a later read changes nothing.
    r = h.send(P, ev.FeedbackRead(session_id=b.session_id, event_ids=(C1, C2)))
    assert not r.audit.accepted and r.audit.reason == "nothing-unread"
    assert not reactions(r)


def test_a_review_has_no_reaction_and_a_redelivered_comment_no_second_eyes():
    h = Harness()
    busy_build(h)
    r = comment(h, "github:review:5398537732:submitted")
    assert r.audit.accepted and not reactions(r) and h.p().unread_feedback == ()
    first = comment(h, C1)
    assert reactions(first) == [(6096243669, "eyes")]
    restart(h)
    again = h.apply(h.f(P).make(ev.PlanFeedback(text_digest="x"), event_id=C1))
    assert again.duplicate and not reactions(again)
