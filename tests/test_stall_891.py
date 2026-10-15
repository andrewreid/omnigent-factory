"""Stalled runs (#891): one nudge per stall episode, then Needs you.

#891: Rosie ended her turn waiting on a worker; Omnigent never woke her for the worker's
second turn. The build sat ACTIVE and quiescent with no result for hours: the factory had
no path for a run that goes quiet before submitting anything. The health sweep proposes
``StallObserved``; these tests drive the reducer side (gates, episodes, escalation,
idempotency across restarts).
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.events import StallAction
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    BotState,
    Hold,
    Lifecycle,
    StallEscalation,
)
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
ESCALATE_AFTER = 30 * MICROS_PER_MINUTE


def nudges(result) -> list:
    return [
        e
        for e in Harness.of(result, EffectKind.SEND_MESSAGE)
        if e.args.get("purpose") == MessagePurpose.STALL_NUDGE.value
    ]


def comments(result) -> list:
    return Harness.of(result, EffectKind.POST_COMMENT)


def quiet_build(h: Harness):
    """#891's shape: the build root ended its turn (tree quiescent), no result."""
    b = h.to_building()
    h.quiesce(P, b.session_id)
    run = h.cur()
    assert run.lifecycle == Lifecycle.ACTIVE and run.quiescent and h.p().readiness is None
    return b


def stall(
    h: Harness,
    action: StallAction,
    *,
    progress: str = "w1:aaa:h",
    commit: str = "",
    recovery: bool = False,
    after_us: int = 0,
    max_nudges: int = 3,
    idle_since_us: int = 0,
):
    f = h.f(P)
    return h.send(
        P,
        ev.StallObserved(
            session_id=h.cur().session_id,
            action=action,
            idle_since_us=idle_since_us or f.now,
            progress=progress,
            commit=commit,
            recovery=recovery,
            escalate_after_us=ESCALATE_AFTER,
            max_nudges=max_nudges,
        ),
        time_us=f.now + after_us if after_us else None,
    )


def restart(h: Harness) -> None:
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))


def resume_turn(h: Harness) -> None:
    """The nudged root runs a turn and goes quiet again (a bare reply)."""
    s = h.cur().session_id
    h.send(P, ev.TreeQuiescent(session_id=s, complete=True, busy=True))
    h.quiesce(P, s)


# ----------------------------------------------------------------------- case A


def test_891_replay_one_nudge_then_needs_you_with_one_comment_never_duplicated():
    h = Harness()
    b = quiet_build(h)
    r = stall(h, StallAction.NUDGE)
    assert r.audit.accepted
    [msg] = nudges(r)
    assert msg.preconditions.session_id == b.session_id
    assert msg.dedupe_key == f"stall:{b.session_id}:1"
    w = h.cur().stall
    assert w.nudged_at_us == h.f(P).now and w.budget == 1 and w.nudges_sent == 1
    assert h.p().bot == BotState.WORKING  # a nudge is not the owner's business
    resume_turn(h)  # the run answers the nudge without any progress
    # Restart mid-way: the next sweep proposes again; nothing new goes out.
    restart(h)
    again = stall(h, StallAction.NUDGE)
    assert not again.audit.accepted and again.audit.reason == "nudge-outstanding"
    assert not nudges(again)
    early = stall(h, StallAction.ESCALATE, after_us=ESCALATE_AFTER // 2)
    assert early.audit.reason == "escalation-not-due" and not comments(early)
    # 30 minutes after the nudge with no progress: Needs you and one comment.
    nudged_at = h.cur().stall.nudged_at_us
    r = h.send(
        P,
        ev.StallObserved(
            session_id=b.session_id,
            action=StallAction.ESCALATE,
            idle_since_us=nudged_at,
            progress="w1:aaa:h",
            escalate_after_us=ESCALATE_AFTER,
        ),
        time_us=nudged_at + ESCALATE_AFTER,
    )
    assert r.audit.accepted and not nudges(r)
    [c] = comments(r)
    assert c.args["template"] == "stalled-quiet"
    assert c.dedupe_key == f"stall:{b.session_id}:escalation:1"
    assert Hold.STALLED in h.p().holds and h.p().bot == BotState.NEEDS_YOU
    assert h.p().note.startswith("Stalled: nudged ")
    assert h.cur().stall.escalated == StallEscalation.QUIET
    # Restart again: no second comment, no second nudge.
    restart(h)
    for action in (StallAction.ESCALATE, StallAction.NUDGE, StallAction.FROZEN):
        later = stall(h, action, after_us=ESCALATE_AFTER)
        assert not later.audit.accepted and not comments(later) and not nudges(later)


def test_real_progress_after_a_nudge_allows_a_second_episode():
    h = Harness()
    b = quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE, progress="w1:aaa:h"))
    resume_turn(h)
    # A worker reported (its updated_at moved): a new episode, a new nudge.
    [msg] = nudges(stall(h, StallAction.NUDGE, progress="w1:bbb:h"))
    assert msg.dedupe_key == f"stall:{b.session_id}:2"
    assert h.cur().stall.budget == 2


def test_a_bare_reply_is_not_progress():
    h = Harness()
    quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE))
    resume_turn(h)
    # Only the root's own items moved (idle_since is later), the signature did not.
    r = stall(h, StallAction.NUDGE, idle_since_us=h.f(P).now)
    assert r.audit.reason == "nudge-outstanding" and not nudges(r)
    r = stall(h, StallAction.ESCALATE, after_us=ESCALATE_AFTER)
    assert r.audit.accepted and Hold.STALLED in h.p().holds


def test_loop_guard_needs_you_after_three_nudges_without_a_commit_or_result():
    h = Harness()
    quiet_build(h)
    for n in range(3):
        [_] = nudges(stall(h, StallAction.NUDGE, progress=f"w1:{n}:h", commit="c0"))
        resume_turn(h)
    r = stall(h, StallAction.NUDGE, progress="w1:3:h", commit="c0")
    assert r.audit.accepted and not nudges(r)
    [c] = comments(r)
    assert c.args["template"] == "stalled-loop"
    assert h.cur().stall.escalated == StallEscalation.LOOP
    assert h.p().bot == BotState.NEEDS_YOU
    assert h.p().note == "Stalled: 3 nudges without a new commit or result"


def test_a_new_commit_resets_the_loop_guard():
    h = Harness()
    quiet_build(h)
    for n in range(3):
        [_] = nudges(stall(h, StallAction.NUDGE, progress=f"w1:{n}:h", commit="c0"))
        resume_turn(h)
    [_] = nudges(stall(h, StallAction.NUDGE, progress="w1:3:hc1", commit="c1"))
    assert h.cur().stall.budget == 1 and h.cur().stall.commit == "c1"


def test_owner_comment_resets_the_count_and_clears_needs_you():
    h = Harness()
    quiet_build(h)
    for n in range(3):
        [_] = nudges(stall(h, StallAction.NUDGE, progress=f"w1:{n}:h"))
        resume_turn(h)
    stall(h, StallAction.NUDGE, progress="w1:3:h")
    assert Hold.STALLED in h.p().holds
    r = h.send(P, ev.PlanFeedback(text_digest="the worker finished, carry on"))
    assert r.audit.accepted
    assert Hold.STALLED not in h.p().holds and h.p().bot == BotState.WORKING
    w = h.cur().stall
    assert w.budget == 0 and w.escalated is None and w.nudges_sent == 3
    # The comment itself is relayed to the idle build as before.
    assert any(
        e.args.get("purpose") == MessagePurpose.FEEDBACK.value
        for e in Harness.of(r, EffectKind.SEND_MESSAGE)
    )


def test_a_result_clears_the_stall():
    h = Harness()
    b = quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE))
    resume_turn(h)
    stall(h, StallAction.ESCALATE, after_us=ESCALATE_AFTER)
    assert Hold.STALLED in h.p().holds
    assert b.root_id is not None
    h.send(
        P,
        result_candidate(b.session_id, b.root_id, h.cur().revision, ev.ResultKind.BLOCKED),
    )
    assert Hold.STALLED not in h.p().holds
    assert h.cur().stall.escalated is None and h.cur().stall.budget == 0


def test_progress_after_escalation_clears_needs_you():
    h = Harness()
    quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE))
    resume_turn(h)
    stall(h, StallAction.ESCALATE, after_us=ESCALATE_AFTER)
    assert h.p().bot == BotState.NEEDS_YOU
    assert not stall(h, StallAction.PROGRESS).audit.accepted  # same signature
    r = stall(h, StallAction.PROGRESS, progress="w2:ccc:h")
    assert r.audit.accepted and Hold.STALLED not in h.p().holds
    assert h.p().note == "" and h.p().bot == BotState.WORKING


# ----------------------------------------------------------------------- case B


def test_runner_dead_recovery_nudge_is_not_counted_against_the_budget():
    h = Harness()
    quiet_build(h)
    for n in range(4):
        r = stall(h, StallAction.NUDGE, progress=f"w1:{n}:h", recovery=True)
        [msg] = nudges(r)
        assert msg.args["recovery"] is True
        resume_turn(h)
    w = h.cur().stall
    assert w.budget == 0 and w.nudges_sent == 4 and w.escalated is None
    # Still one nudge per episode.
    assert stall(h, StallAction.NUDGE, progress="w1:3:h", recovery=True).audit.reason == (
        "nudge-outstanding"
    )


# ----------------------------------------------------------------------- case C


def test_frozen_busy_run_gets_a_note_and_needs_you_but_no_message():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    since = h.f(P).now
    r = stall(h, StallAction.FROZEN, idle_since_us=since)
    assert r.audit.accepted
    assert not Harness.of(r, EffectKind.SEND_MESSAGE)
    assert not Harness.of(r, EffectKind.INTERRUPT_TREE)
    [c] = comments(r)
    assert c.args["template"] == "stalled-frozen"
    assert h.p().bot == BotState.NEEDS_YOU
    assert h.p().note.startswith("Stalled: no activity since ")
    # Busy runs are never nudged.
    assert stall(h, StallAction.NUDGE).audit.reason == "escalated"
    # Any new activity in the tree clears a frozen escalation.
    r = stall(h, StallAction.PROGRESS, idle_since_us=since + MICROS_PER_MINUTE)
    assert r.audit.accepted and Hold.STALLED not in h.p().holds


def test_escalation_waits_for_the_nudged_turn_to_end():
    h = Harness()
    quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE))
    # The nudged root is running (not seen quiet again): never escalated mid-turn.
    r = stall(h, StallAction.ESCALATE, after_us=ESCALATE_AFTER)
    assert r.audit.reason == "busy" and not comments(r)


def test_a_quiet_run_is_never_frozen_and_a_busy_one_never_nudged():
    h = Harness()
    b = quiet_build(h)
    assert stall(h, StallAction.FROZEN).audit.reason == "not-busy"
    h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    assert stall(h, StallAction.NUDGE).audit.reason == "busy"


# ----------------------------------------------------------------------- gates


def test_a_stopped_run_is_never_nudged():
    h = Harness()
    quiet_build(h)
    h.send(P, ev.Stop())
    r = stall(h, StallAction.NUDGE)
    assert not r.audit.accepted and not nudges(r)


def test_a_run_with_an_open_question_is_never_nudged_and_a_question_ends_the_episode():
    h = Harness()
    b = quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE))
    h.send(
        P,
        ev.OwnerQuestion(session_id=b.session_id, question_key="q1", summary="Which DB?"),
    )
    assert h.p().open_decisions
    assert h.cur().stall.nudged_at_us == 0  # asking is progress: the episode ended
    r = stall(h, StallAction.NUDGE)
    assert r.audit.reason in ("open-question", "run-not-active") and not nudges(r)


def test_awaiting_owner_and_submitted_results_are_never_nudged():
    h = Harness()
    h.triage(P)  # triage posted: awaiting the owner
    r = h.send(P, ev.StallObserved(session_id=h.cur().session_id, action=StallAction.NUDGE))
    assert not r.audit.accepted and not nudges(r)
    h2 = Harness()
    quiet_build(h2)
    h2.build_ready(P)  # build_ready submitted: readiness owns the run
    r = stall(h2, StallAction.NUDGE)
    assert not r.audit.accepted and not nudges(r)


def test_a_pending_owner_comment_is_left_to_the_relay():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True))
    h.send(P, ev.PlanFeedback(text_digest="one"))
    h.send(P, ev.PlanFeedback(text_digest="two"))  # folded: comment_pending
    assert h.cur().comment_pending
    # The turn ends: the folded comment is relayed (immediate-feedback path), not a nudge.
    r = h.quiesce(P, b.session_id)
    assert any(
        e.args.get("purpose") == MessagePurpose.FEEDBACK.value
        for e in Harness.of(r, EffectKind.SEND_MESSAGE)
    )
    assert h.cur().stall.nudges_sent == 0


def test_a_checkpointed_run_is_never_nudged():
    h = Harness()
    b = quiet_build(h)
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    assert h.cur().lifecycle != Lifecycle.ACTIVE
    r = stall(h, StallAction.NUDGE)
    assert r.audit.reason == "run-not-active" and not nudges(r)


def test_stall_state_survives_a_restart():
    h = Harness()
    quiet_build(h)
    [_] = nudges(stall(h, StallAction.NUDGE, commit="c0"))
    before = h.cur().stall
    restart(h)
    assert h.cur().stall == before
