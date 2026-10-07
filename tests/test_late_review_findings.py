"""Review-bot answers after Ready, and the wait while the bot is visibly reviewing.

#638 / PR #799 (2026-10-07 ACDT): after a rework push (9bb7f7f) and an `@codex review`
ping, the round's check wake went to a red check; the 10-minute review grace then ran
out with Codex still reviewing (👀, no webhook) and the card went to Ready with its build
run closed. Two minutes later Codex posted two P1 findings on 9bb7f7f. The next read
pulled the card back to Building with ``readiness_failed`` + ``rework_control_required``:
no wake although the round's findings wake was unused, and no comment, so the owner saw
Needs you with nothing to act on while the factory re-read the PR every 2 minutes.

Now: while the bot shows 👀 on its latest trigger the card is not Ready (up to a cap);
findings that still arrive after Ready get the round's findings wake once (the closed
run is re-opened, under its own authority); and every readiness Needs you posts one
comment naming the open threads.
"""

from __future__ import annotations

import json
from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectIntent, EffectKind, MessagePurpose
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    BotState,
    Hold,
    Lifecycle,
    Reservation,
    ReservationKind,
    Stage,
    WaitReason,
)
from omnigent_factory.testing.builders import config, result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PR = 799
FIX = "9bb7f7f" + "0" * 33  # the rework push Codex reviewed late
NEXT = "c" * 40
MIN = MICROS_PER_MINUTE
GREEN = "20 checks: 20 success"
FINDINGS = (
    ev.FindingRef(
        path="api/src/export.ts",
        severity="P1",
        title="Keep the delivery row when the retry fails",
        url="https://github.com/o/r/pull/799#discussion_r1",
    ),
    ev.FindingRef(
        path="api/src/retry.ts",
        severity="P1",
        title="Bound the retry loop",
        url="https://github.com/o/r/pull/799#discussion_r2",
    ),
)


def harness(**kw: int) -> Harness:
    cfg = replace(
        config(),
        review_grace_us=10 * MIN,
        review_ack_us=5 * MIN,
        review_cap_us=45 * MIN,
        **kw,
    )
    return Harness(cfg=cfg)


def sends(result) -> list[EffectIntent]:
    return [e for e in result.effects if e.kind == EffectKind.SEND_MESSAGE]


def comments(result, template: str = "ready-blocked") -> list[EffectIntent]:
    return [
        e
        for e in result.effects
        if e.kind == EffectKind.POST_COMMENT and e.args.get("template") == template
    ]


def all_effects(h: Harness, since: int) -> list[EffectIntent]:
    """Every effect from log entry ``since`` on (a move's ack is its own transition)."""
    return [e for _, r in h.log[since:] for e in r.effects]


def evidence(h: Harness, head: str = FIX, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def green(h: Harness, head: str = FIX, **kw) -> ev.ReadinessEvidence:
    kw.setdefault("checks_summary", GREEN)
    return evidence(h, head, verified=True, checks=ev.ChecksState.GREEN, **kw)


def late_findings(h: Harness, head: str = FIX, **kw) -> ev.ReadinessEvidence:
    """Codex answered the head with findings that have no outcome yet."""
    kw.setdefault("review_bot_pending_since_us", 0)
    kw.setdefault("review_bot_verdict", f"reviewed `{head[:7]}`, findings without outcomes")
    kw.setdefault("findings_earlier_rounds", True)
    return evidence(
        h,
        head,
        checks=ev.ChecksState.GREEN,
        checks_summary=GREEN,
        findings_open=True,
        open_findings=FINDINGS,
        **kw,
    )


def submit(h: Harness, head: str = FIX) -> None:
    """The build run reports build_ready for ``head`` and ends its turn."""
    s = h.cur(P)
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=head,
        ),
    )
    assert r.audit.accepted, r.audit.reason
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=head, bot_authored=True, parcel_branch=True))
    h.quiesce(P, h.cur(P).session_id)


def ready_after_silent_grace(h: Harness, *, check_wake: bool = True) -> int:
    """#799 up to 22:38: the check wake went to a red check, the run re-submitted, and
    the grace ran out with no Codex answer (legacy read: no 👀 state): Ready, run closed.
    Returns the trigger time (the push / ping)."""
    h.to_building()
    submit(h)
    if check_wake:
        r = h.send(P, evidence(h, checks=ev.ChecksState.FAILED, checks_summary="1 failure"))
        [wake] = sends(r)
        assert wake.args["wake"] == "checks"
        submit(h)  # the red check was outside the change: same head again
    pinged = h.f(P).now
    h.f(P).tick(11 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pinged))
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED
    assert p.findings_wakes == 0
    return pinged


# ------------------------------------------------------------ late findings after Ready


def test_799_replay_late_findings_after_ready_get_the_unused_findings_wake_once():
    h = harness()
    ready_after_silent_grace(h)
    run = h.p().readiness.session_id
    assert h.admission.building_count == 0  # Ready released the slot
    h.f(P).tick(2 * MIN)
    # 22:40:42: the Codex review webhook (same head) is judged from a fresh read at once.
    r = h.send(P, ev.ReviewChanged(pr_number=PR, head_sha=FIX))
    assert [e.args["head_sha"] for e in r.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE] == [
        FIX
    ]
    mark = len(h.log)
    r = h.send(P, late_findings(h))
    p = h.p()
    assert p.stage == Stage.BUILDING
    assert not p.holds & {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED}
    assert not comments(r)  # the run handles them: nothing for the owner yet
    s = p.session(run)
    assert s is not None
    assert p.current_session_id == run and s.reopened and not s.execution_closed
    assert (s.lifecycle, s.wait_reason) == (Lifecycle.WAITING, WaitReason.CHECKS)
    assert h.admission.building_count == 1  # the slot is held again
    # The move landed (auto-ack): the read that wakes the run is issued.
    later = all_effects(h, mark)
    assert EffectKind.FETCH_PR_EVIDENCE in [e.kind for e in later]
    r = h.send(P, late_findings(h))
    [wake] = sends(r)
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value
    assert wake.args["wake"] == "findings"
    assert "review-bot findings have no outcome" in str(wake.args["reason"])
    assert EffectKind.ENABLE_ISSUANCE in [e.kind for e in r.effects]
    p = h.p()
    assert p.findings_wakes == 1 and p.bot == BotState.WORKING
    assert h.cur(P).lifecycle == Lifecycle.ACTIVE
    # Re-reads while the woken run works: no second wake, no comment.
    r = h.send(P, late_findings(h))
    assert not sends(r) and not comments(r)


def test_late_findings_with_the_findings_wake_spent_stay_needs_you_with_one_comment():
    h = harness()
    h.to_building()
    submit(h)
    # The round's findings wake goes to an earlier Codex round in Building.
    r = h.send(P, late_findings(h, findings_earlier_rounds=False))
    [wake] = sends(r)
    assert wake.args["wake"] == "findings" and h.p().findings_wakes == 1
    submit(h, NEXT)  # fixed, pushed, re-reported
    h.f(P).tick(11 * MIN)
    h.send(P, green(h, NEXT, review_bot_pending_since_us=h.f(P).now - 11 * MIN))
    assert h.p().stage == Stage.READY
    # Codex reviews the fix late and finds more: today's Needs you, now with a comment.
    r = h.send(P, late_findings(h, NEXT))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED} <= p.holds
    assert not sends(r)
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED
    [comment] = comments(r)
    assert comment.args["findings"] == [
        {"path": f.path, "severity": f.severity, "title": f.title, "url": f.url} for f in FINDINGS
    ]
    assert comment.args["further_round"] is True
    assert "review-bot findings have no outcome" in str(comment.args["reason"])
    # Unchanged re-reads, also after a restart: never posted again.
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    for _ in range(2):
        r = h.send(P, late_findings(h, NEXT))
        assert not comments(r) and not sends(r)


def test_a_re_opened_run_is_never_re_opened_again():
    """Anti-whack-a-mole: one findings wake per round. The woken run fixes, reaches Ready
    again, and Codex finds more: Needs you (with the comment), no second re-open."""
    h = harness()
    ready_after_silent_grace(h)
    h.send(P, late_findings(h))
    r = h.send(P, late_findings(h))
    assert sends(r) and h.p().findings_wakes == 1
    submit(h, NEXT)
    h.send(P, green(h, NEXT, review_bot_pending_since_us=0, review_bot_verdict="👍"))
    p = h.p()
    assert p.stage == Stage.READY
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED
    r = h.send(P, late_findings(h, NEXT))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU and not sends(r)
    assert len(comments(r)) == 1
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED


def test_late_findings_without_a_free_building_slot_need_the_owner():
    h = harness()
    ready_after_silent_grace(h)
    other = Reservation("rs_other", "I_parcel_2", ReservationKind.BUILDING, "ap_other")
    h.admission = replace(h.admission, reservations=(*h.admission.reservations, other))
    r = h.send(P, late_findings(h))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU and not sends(r)
    assert len(comments(r)) == 1


def test_a_card_the_old_rule_withdrew_for_late_findings_is_woken_by_the_next_read():
    """Recovery of #638: the live parcel sits in Building with readiness_failed +
    rework_control_required, its run retired, the findings wake unused. After deploy the
    next catch-up read re-opens the run in place and wakes it with the findings wake."""
    h = harness()
    ready_after_silent_grace(h)
    p = h.p()
    note = (
        "No longer ready: review bot findings have no outcome (fixed, follow up or "
        f"advisory) on {FIX[:7]}"
    )
    stored = replace(
        p,
        stage=Stage.BUILDING,
        bot=BotState.NEEDS_YOU,
        holds=p.holds | {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED},
        note=note[:120],
        readiness=replace(
            p.readiness, ready=False, verified=False, report_effect_id="", report_key=""
        ),
    )
    data = json.loads(parcel_to_json(stored))  # stored before the new fields existed
    for key in ("reopened",):
        for s in data["parcel"]["sessions"]:
            s.pop(key, None)
    for key in ("eyes_trigger_us", "unknown_since_us"):
        data["parcel"]["readiness"].pop(key, None)
    h.parcels[P] = parcel_from_json(json.dumps(data))
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE in [e.kind for e in r.effects]
    r = h.send(P, late_findings(h))
    [wake] = sends(r)
    assert wake.args["wake"] == "findings"
    p = h.p()
    assert not p.holds & {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED}
    assert p.findings_wakes == 1 and p.bot == BotState.WORKING
    assert h.cur(P).reopened and h.cur(P).lifecycle == Lifecycle.ACTIVE
    assert h.admission.building_count == 1


def test_owner_base_sync_red_on_ready_stays_ready_blocked():
    """Unchanged: a red required check on a base-sync head of a Ready card is Bot Blocked
    in Ready (no wake, no comment, no re-open)."""
    h = harness()
    ready_after_silent_grace(h, check_wake=False)
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=NEXT, bot_authored=True, parcel_branch=True))
    h.send(P, evidence(h, FIX, observed_head_sha=NEXT, checks=ev.ChecksState.PENDING))
    r = h.send(
        P,
        evidence(
            h,
            NEXT,
            checks=ev.ChecksState.FAILED,
            checks_summary="1 failure",
            failing_checks="api / audit",
            base_sync=True,
            review_bot_pending_since_us=0,
        ),
    )
    p = h.p()
    assert p.stage == Stage.READY and p.bot == BotState.BLOCKED
    assert not sends(r) and not comments(r)
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED


def test_merged_pr_is_final_even_with_late_findings():
    h = harness()
    ready_after_silent_grace(h)
    h.send(P, evidence(h, pr_open=False, merged=True, checks=ev.ChecksState.GREEN))
    assert Hold.COMPLETED in h.p().holds
    r = h.send(P, late_findings(h))
    assert not sends(r) and not comments(r) and Hold.COMPLETED in h.p().holds


def test_needs_you_comment_in_building_names_the_open_threads():
    h = harness()
    h.to_building()
    submit(h)
    h.send(P, late_findings(h))  # the findings wake
    submit(h)
    r = h.send(P, late_findings(h))
    [comment] = comments(r)
    assert [f["severity"] for f in comment.args["findings"]] == ["P1", "P1"]
    assert h.p().bot == BotState.NEEDS_YOU


# ------------------------------------------------------- the 👀 ("reviewing") wait


def built(h: Harness) -> int:
    h.to_building()
    submit(h)
    return h.f(P).now


def test_eyes_keep_the_card_out_of_ready_past_the_grace():
    h = harness()
    pushed = built(h)
    h.f(P).tick(1 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    h.f(P).tick(20 * MIN)  # well past the 10-minute grace
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    assert h.p().stage == Stage.BUILDING and not h.p().readiness.ready
    # Re-read on each reconcile while waiting (reactions send no webhook).
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE in [e.kind for e in r.effects]
    h.f(P).tick(23 * MIN)  # 44 minutes after the push
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    assert h.p().stage == Stage.BUILDING


def test_the_bots_answer_ends_the_eyes_wait_at_once():
    h = harness()
    pushed = built(h)
    h.f(P).tick(1 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    h.f(P).tick(19 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=0, review_bot_verdict="👍 on `9bb7f7f`"))
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.review_bot == "👍 on `9bb7f7f`"


def test_answer_with_findings_during_the_eyes_wait_goes_to_the_build():
    h = harness()
    pushed = built(h)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    h.f(P).tick(30 * MIN)
    r = h.send(P, late_findings(h))
    [wake] = sends(r)
    assert wake.args["wake"] == "findings" and h.p().stage == Stage.BUILDING


def test_no_eyes_within_the_ack_window_ends_the_wait():
    h = harness()
    pushed = built(h)
    h.f(P).tick(3 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.ABSENT))
    assert h.p().stage == Stage.BUILDING  # within the 5-minute ack window
    h.f(P).tick(3 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.ABSENT))
    p = h.p()
    assert p.stage == Stage.READY  # 6 minutes: before the old 10-minute grace
    assert p.readiness.review_bot == "no response within the grace window"


def test_the_cap_ends_an_eyes_wait_with_no_answer():
    h = harness()
    pushed = built(h)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    h.f(P).tick(46 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    assert h.p().stage == Stage.READY


def test_an_unreadable_bot_state_waits_but_only_up_to_the_cap():
    h = harness()
    built(h)
    h.send(P, green(h, review_bot_pending_since_us=None, review_bot_eyes=ev.BotEyes.UNKNOWN))
    h.f(P).tick(30 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=None, review_bot_eyes=ev.BotEyes.UNKNOWN))
    assert h.p().stage == Stage.BUILDING
    h.f(P).tick(16 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=None, review_bot_eyes=ev.BotEyes.UNKNOWN))
    assert h.p().stage == Stage.READY


def test_seen_eyes_survive_a_restart_and_a_vanished_reaction():
    h = harness()
    pushed = built(h)
    h.f(P).tick(1 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))  # daemon restart
    assert h.p().readiness.eyes_trigger_us == pushed
    h.f(P).tick(15 * MIN)
    # The 👀 is gone but no answer arrived: still the bot's review of this trigger.
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.ABSENT))
    assert h.p().stage == Stage.BUILDING
    # A new re-ping without 👀 is a new trigger: its own ack window.
    pinged = h.f(P).now
    h.f(P).tick(6 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pinged, review_bot_eyes=ev.BotEyes.ABSENT))
    assert h.p().stage == Stage.READY


def test_without_eyes_state_the_grace_applies_as_before():
    h = harness()
    pushed = built(h)
    h.f(P).tick(6 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed))
    assert h.p().stage == Stage.BUILDING
    h.f(P).tick(5 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed))
    assert h.p().stage == Stage.READY


def test_the_running_config_decides_the_windows():
    """The windows are read from the trusted config of each transition (hot reload)."""
    h = harness()
    pushed = built(h)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    h.cfg = replace(h.cfg, review_cap_us=20 * MIN)  # `reload` lowered the cap
    h.f(P).tick(21 * MIN)
    h.send(P, green(h, review_bot_pending_since_us=pushed, review_bot_eyes=ev.BotEyes.SEEN))
    assert h.p().stage == Stage.READY


def test_late_findings_on_an_owner_sync_head_keep_the_existing_rule():
    """An owner "Update branch" head keeps its rule: findings there are the owner's call
    (now with the comment), never a re-open."""
    h = harness()
    ready_after_silent_grace(h, check_wake=False)
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=NEXT, bot_authored=True, parcel_branch=True))
    h.send(P, evidence(h, FIX, observed_head_sha=NEXT, checks=ev.ChecksState.PENDING))
    r = h.send(P, late_findings(h, NEXT, base_sync=True))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU and not sends(r)
    assert len(comments(r)) == 1
