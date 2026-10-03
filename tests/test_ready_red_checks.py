"""Ready means the bot's work is done; required checks only decide Bot there (#461).

A card belongs in Ready once its PR is open and closes the issue, the cross-vendor
review is accepted for the head, every review-bot finding has an outcome and the bot
has nothing left to do. Red required checks then give Bot Blocked with a note naming
them (no comment, no wake). They still send the card to Building with the one fix wake
only while that wake is unused and the head is not a mere base sync. An owner's drag to
Ready is accepted on the same terms, or moved back with the reason in the note.
"""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import BotState, Hold, Lifecycle, Stage, WaitReason
from omnigent_factory.testing.builders import OWNER_ID, result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PR = 710
#: #461: Rosie's reviewed head, then the owner's merge of main (3ef7452).
BUILT = "3d224eff2d7b6bc5cb575fcc116bc80e757456e4"
SYNCED = "3ef7452d080336c2a637f44cdd39fae58f1ecdbe"
AUDIT = "api / Dependency audit"
WEB = "web / Typecheck, test, lint"
REWORK_HOLDS = {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED}


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def evidence(h: Harness, head: str, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def red(h: Harness, head: str, *names: str, **kw) -> ev.ReadinessEvidence:
    names = names or (AUDIT,)
    return evidence(
        h,
        head,
        checks=ev.ChecksState.FAILED,
        checks_summary=f"17 checks: 15 success, {len(names)} failure",
        failing_checks="; ".join(names),
        **kw,
    )


def submit_ready(h: Harness, head: str) -> None:
    s = h.cur()
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


def submit_blocked(h: Harness) -> None:
    s = h.cur()
    assert s.root_id is not None
    r = h.send(P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    assert r.audit.accepted, r.audit.reason


def built(h: Harness, head: str = BUILT) -> None:
    """Building; the run submitted build_ready for ``head`` and its turn ended."""
    h.to_building()
    submit_ready(h, head)
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=head, bot_authored=True, parcel_branch=True))
    h.quiesce(P, h.cur().session_id)


def drag_to_ready(h: Harness):
    return h.send(
        P, ev.ColumnObserved(stage=Stage.READY), actor=OWNER_ID, provenance=Provenance.WEBHOOK
    )


def assert_quiet(result) -> None:
    """No comment, no wake, no new session."""
    assert EffectKind.POST_COMMENT not in kinds(result)
    assert EffectKind.SEND_MESSAGE not in kinds(result)
    assert EffectKind.CREATE_SESSION not in kinds(result)
    assert EffectKind.ENABLE_ISSUANCE not in kinds(result)


def assert_ready_blocked(h: Harness, *names: str) -> None:
    p = h.p()
    assert p.stage == Stage.READY and p.readiness is not None
    assert p.readiness.sync_red and not p.readiness.ready
    assert p.bot == BotState.BLOCKED
    assert p.board_note == f"Required check red: {'; '.join(names or (AUDIT,))}"
    assert not p.holds & (REWORK_HOLDS | {Hold.CHECKS_FAILED})
    assert h.cur().lifecycle == Lifecycle.RETIRED and h.cur().execution_closed


# ------------------------------------------------------------- 1. the #461 replay


def test_461_replay_lands_in_ready_blocked_and_an_owner_drag_is_not_bounced():
    """#461 / PR #710 as it happened: red checks from outside the change, the one wake
    used, Rosie blocked, the owner merged main and commented, Rosie re-submitted on the
    merged head, the checks stayed red. Base: Needs you + a "not ready" comment, then the
    owner's drag to Ready was bounced to Building. Now: Ready, Bot Blocked, quiet."""
    h = Harness()
    built(h)
    first = h.cur()
    # 13:36 red on Rosie's head: the one automatic wake.
    r = h.send(P, red(h, BUILT, AUDIT, WEB))
    assert [e.args["purpose"] for e in r.effects if e.kind == EffectKind.SEND_MESSAGE] == [
        "readiness_wake"
    ]
    assert h.p().stage == Stage.BUILDING and h.p().readiness_wakes == 1
    # 14:07 Rosie: blocked, the red checks are not hers (main + a flaky test).
    submit_blocked(h)
    assert Hold.AGENT_BLOCKED in h.p().holds
    h.quiesce(P, first.session_id)
    r = h.send(P, red(h, BUILT, AUDIT, WEB))
    assert_quiet(r)
    # 16:02 the owner merges main into the branch.
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=SYNCED, bot_authored=True, parcel_branch=True))
    h.send(P, evidence(h, BUILT, observed_head_sha=SYNCED, checks=ev.ChecksState.PENDING))
    assert h.p().readiness.head_sha == SYNCED
    # 18:11 the owner comments: relayed to the idle run, agent_blocked cleared.
    r = h.send(P, ev.PlanFeedback(text_digest="main is broken; re-submit"))
    assert "feedback" in [e.args.get("purpose") for e in r.effects]
    assert Hold.AGENT_BLOCKED not in h.p().holds
    # 18:13 Rosie re-submits build_ready on the merged head (reviewed it there).
    submit_ready(h, SYNCED)
    h.quiesce(P, first.session_id)
    # 18:14 the read: only the required checks are red, the wake is spent.
    r = h.send(P, red(h, SYNCED, AUDIT, base_sync=False))
    assert_quiet(r)
    [move] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert move.args == {"to": "Ready", "expected_from": "Building"}
    assert_ready_blocked(h, AUDIT)
    assert h.cur().session_id == first.session_id  # the same run, now retired
    # GitHub's rollup flips (17 vs 18 runs): the hidden failure joins the note, stays.
    r = h.send(P, red(h, SYNCED, AUDIT, WEB))
    assert_quiet(r) and EffectKind.MOVE_CARD not in kinds(r)
    assert_ready_blocked(h, AUDIT, WEB)
    r = h.send(P, red(h, SYNCED, AUDIT))
    assert EffectKind.SET_NOTE not in kinds(r) and EffectKind.SET_BOT not in kinds(r)
    assert_ready_blocked(h, AUDIT, WEB)
    # 18:16 the owner (re)drags it to Ready: already there, nothing bounces.
    drag_to_ready(h)
    r = h.send(P, red(h, SYNCED, AUDIT, WEB))
    assert_quiet(r) and EffectKind.MOVE_CARD not in kinds(r)
    assert_ready_blocked(h, AUDIT, WEB)
    # Fixed on main, re-run green: Idle.
    r = h.send(P, evidence(h, SYNCED, verified=True, checks=ev.ChecksState.GREEN))
    assert_quiet(r)
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and p.bot == BotState.IDLE
    assert p.board_note == ""


def stuck_461(h: Harness) -> None:
    """The live #461 shape the old rule left: Building, Needs you, readiness_failed +
    rework_control_required, the run WAITING on checks, the wake spent and the note of
    the bounce after the owner's drag."""
    built(h)
    h.send(P, red(h, BUILT, AUDIT, WEB))  # the wake
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=SYNCED, bot_authored=True, parcel_branch=True))
    h.send(P, evidence(h, BUILT, observed_head_sha=SYNCED, checks=ev.ChecksState.PENDING))
    submit_ready(h, SYNCED)
    h.quiesce(P, h.cur().session_id)
    p = h.p()
    note = (
        "No longer ready: required checks failed "
        "(18 checks: 10 success, 7 skipped, 1 failure) on 3ef7452"
    )
    h.parcels[P] = replace(
        p,
        holds=p.holds | REWORK_HOLDS,
        bot=BotState.NEEDS_YOU,
        note=note,
        board_note=note,
        readiness=replace(p.readiness, reviewed_head=SYNCED),
    )
    p = h.p()
    assert p.stage == Stage.BUILDING and p.readiness_wakes == 1
    s = h.cur()
    assert s.lifecycle == Lifecycle.WAITING and s.wait_reason == WaitReason.CHECKS
    assert s.quiescent and not s.execution_closed


def test_stuck_461_self_corrects_on_the_first_reconcile_after_deploy():
    h = Harness()
    stuck_461(h)
    session = h.cur()
    r = h.send(P, ev.ReconcileDue())
    [fetch] = [e for e in r.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE]
    assert fetch.args["head_sha"] == SYNCED
    r = h.send(P, red(h, SYNCED, AUDIT))
    assert r.audit.accepted
    assert_quiet(r)
    [move] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert move.args == {"to": "Ready", "expected_from": "Building"}
    assert EffectKind.DISABLE_ISSUANCE in kinds(r)
    assert_ready_blocked(h, AUDIT)
    assert h.cur().session_id == session.session_id


def test_stuck_461_whose_checks_turned_green_lands_in_ready_idle():
    h = Harness()
    stuck_461(h)
    r = h.send(P, evidence(h, SYNCED, verified=True, checks=ev.ChecksState.GREEN))
    assert_quiet(r)
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and p.bot == BotState.IDLE
    assert not p.holds & REWORK_HOLDS and h.cur().lifecycle == Lifecycle.RETIRED


# ---------------------------------------------------- 2. automatic placement rules


def test_red_after_the_wake_is_spent_goes_to_ready_blocked_not_needs_you():
    h = Harness()
    built(h)
    r = h.send(P, red(h, BUILT))
    assert EffectKind.SEND_MESSAGE in kinds(r)  # the one wake
    h.send(P, ev.RuntimeActivity(session_id=h.cur().session_id, busy=True))
    submit_ready(h, BUILT)  # Rosie: the red check is outside the change
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, red(h, BUILT))
    assert_quiet(r)
    assert_ready_blocked(h)
    # A later green read: Idle, still no comment.
    r = h.send(P, evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN))
    assert_quiet(r)
    assert h.p().readiness.ready and h.p().bot == BotState.IDLE


def test_red_on_bot_commits_with_the_wake_available_still_wakes_in_building():
    h = Harness()
    built(h)
    r = h.send(P, red(h, BUILT))
    assert [e.args["purpose"] for e in r.effects if e.kind == EffectKind.SEND_MESSAGE] == [
        "readiness_wake"
    ]
    p = h.p()
    assert p.stage == Stage.BUILDING and not p.readiness.sync_red
    assert EffectKind.POST_COMMENT not in kinds(r)


def test_red_on_a_base_sync_head_in_building_needs_no_wake():
    """The head only merges the base onto the reviewed head: nothing for the bot."""
    h = Harness()
    built(h)
    r = h.send(P, red(h, BUILT, base_sync=True))
    assert_quiet(r)
    assert_ready_blocked(h)
    assert h.p().readiness_wakes == 0


def test_another_failure_after_the_spent_wake_still_needs_you():
    h = Harness()
    built(h)
    h.send(P, red(h, BUILT))
    h.send(P, ev.RuntimeActivity(session_id=h.cur().session_id, busy=True))
    submit_ready(h, BUILT)
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, red(h, BUILT, findings_open=True))
    assert EffectKind.POST_COMMENT in kinds(r)
    p = h.p()
    assert p.stage == Stage.BUILDING and Hold.READINESS_FAILED in p.holds
    assert p.bot == BotState.NEEDS_YOU


def test_an_in_scope_blocked_report_keeps_the_card_with_the_owner():
    """Rosie reported a defect in the change she cannot fix: Bot Blocked with her
    reason, not Ready."""
    h = Harness()
    built(h)
    h.send(P, red(h, BUILT))
    submit_blocked(h)
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, red(h, BUILT))
    assert EffectKind.MOVE_CARD not in kinds(r)
    p = h.p()
    assert p.stage == Stage.BUILDING and Hold.AGENT_BLOCKED in p.holds
    assert p.bot == BotState.BLOCKED


def test_parking_waits_for_the_review_bot_grace():
    from omnigent_factory.testing.builders import config

    h = Harness(cfg=config(review_grace_us=600_000_000))
    built(h)
    h.send(P, red(h, BUILT, base_sync=True))
    p = h.p()
    assert p.stage == Stage.BUILDING and not p.holds & REWORK_HOLDS
    assert p.bot == BotState.WORKING
    r = h.send(P, red(h, BUILT, base_sync=True), time_us=h.f(P).now + 600_000_000)
    assert_quiet(r)
    assert_ready_blocked(h)


# ---------------------------------------------------------------- 3. owner drag


def test_owner_drag_to_ready_with_red_checks_is_accepted_as_blocked():
    """The wake was still unused: the owner's move says the bot's work is done."""
    h = Harness()
    built(h)
    r = drag_to_ready(h)
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(r)
    assert h.p().stage == Stage.READY
    r = h.send(P, red(h, BUILT, AUDIT, WEB))
    assert_quiet(r)
    assert EffectKind.MOVE_CARD not in kinds(r)
    assert_ready_blocked(h, AUDIT, WEB)
    assert h.p().readiness_wakes == 0


def test_owner_drag_to_ready_with_pending_checks_is_accepted_as_idle():
    h = Harness()
    built(h)
    drag_to_ready(h)
    r = h.send(P, evidence(h, BUILT, checks=ev.ChecksState.PENDING))
    assert_quiet(r) and EffectKind.MOVE_CARD not in kinds(r)
    p = h.p()
    # Ready is never Working: the bot waits on CI, with the note saying so.
    assert p.stage == Stage.READY and p.bot == BotState.IDLE
    assert p.board_note == f"Checks running on `{BUILT[:7]}`"
    assert h.cur().lifecycle == Lifecycle.RETIRED
    h.send(P, evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN))
    assert h.p().readiness.ready and h.p().bot == BotState.IDLE


def test_owner_drag_to_ready_with_open_findings_is_refused_with_the_reason():
    h = Harness()
    built(h)
    drag_to_ready(h)
    r = h.send(P, red(h, BUILT, findings_open=True))
    [move] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert move.args == {"to": "Building", "expected_from": "Ready"}
    assert EffectKind.POST_COMMENT not in kinds(r)
    p = h.p()
    assert p.stage == Stage.BUILDING
    assert p.board_note == "Kept in Building: review-bot findings have no outcome"
    assert h.cur().lifecycle == Lifecycle.WAITING  # the run keeps its turn
    # The next read is judged in Building as before: the one wake.
    r = h.send(P, red(h, BUILT, findings_open=True))
    assert EffectKind.SEND_MESSAGE in kinds(r)


def test_owner_drag_while_the_build_run_is_working_is_refused_with_the_reason():
    h = Harness()
    built(h)
    h.send(P, red(h, BUILT))  # the wake: the run is working again
    # Before its tree is seen busy: the wake still counts as work. Ready never shows
    # the bot working, so the drag itself is answered with a move back.
    r = drag_to_ready(h)
    [move] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert move.args == {"to": "Building", "expected_from": "Ready"}
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.WORKING
    assert p.board_note == "Kept in Building: the bot is still working"


def test_owner_drag_without_an_accepted_review_names_the_head():
    h = Harness()
    built(h)
    drag_to_ready(h)
    h.send(P, red(h, BUILT, review_accepted=False))
    p = h.p()
    assert p.stage == Stage.BUILDING
    assert p.board_note == f"Kept in Building: no accepted cross-vendor review of {BUILT[:7]}"


# ---------------------------------------------------------- 5. terminal and rework


def parked(h: Harness) -> None:
    built(h)
    h.send(P, red(h, BUILT, base_sync=True))
    assert_ready_blocked(h)


def test_merge_while_ready_blocked_is_final():
    h = Harness()
    parked(h)
    h.send(P, evidence(h, BUILT, pr_open=False, merged=True, checks=ev.ChecksState.FAILED))
    p = h.p()
    assert Hold.COMPLETED in p.holds and p.bot == BotState.IDLE
    r = h.send(P, red(h, BUILT))
    assert EffectKind.MOVE_CARD not in kinds(r) and EffectKind.POST_COMMENT not in kinds(r)


def test_owner_comment_while_ready_blocked_still_starts_rework():
    h = Harness()
    parked(h)
    r = h.send(P, ev.PlanFeedback(text_digest="also handle X"))
    assert r.audit.accepted
    p = h.p()
    assert p.stage == Stage.BUILDING and p.note == "Rework: owner feedback"
    assert p.readiness is None


# ---------------------------------------------------------- 6. the Ready report


def ready_reports(result) -> list:
    return [
        e
        for e in result.effects
        if e.kind == EffectKind.PUBLISH_REPORT and e.args.get("report") == "ready"
    ]


def all_ready_reports(h: Harness) -> list:
    return [e for _, res in h.log for e in ready_reports(res)]


def test_auto_park_posts_the_ready_report_once_with_the_red_checks():
    h = Harness()
    built(h)
    h.send(P, red(h, BUILT))  # the wake
    h.send(P, ev.RuntimeActivity(session_id=h.cur().session_id, busy=True))
    submit_ready(h, BUILT)
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, red(h, BUILT, AUDIT, WEB))
    [report] = ready_reports(r)
    assert report.args["pr_number"] == PR and report.args["head_sha"] == BUILT
    assert report.args["red_checks"] == f"{AUDIT}; {WEB}"
    assert report.args["checks_summary"] == "17 checks: 15 success, 2 failure"
    assert h.cur().lifecycle == Lifecycle.RETIRED  # parking needs an open run: no repeat
    assert EffectKind.POST_COMMENT not in kinds(r) and EffectKind.SEND_MESSAGE not in kinds(r)
    # Re-reads in Ready (flipping rollups, pending, red again): no second report.
    for body in (
        red(h, BUILT, AUDIT),
        evidence(h, BUILT, checks=ev.ChecksState.PENDING),
        red(h, BUILT, AUDIT),
        evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN),
    ):
        assert not ready_reports(h.send(P, body))
    assert len(all_ready_reports(h)) == 1


def test_base_sync_park_posts_the_ready_report_once():
    h = Harness()
    built(h)
    r = h.send(P, red(h, BUILT, base_sync=True))
    assert len(ready_reports(r)) == 1
    assert not ready_reports(h.send(P, red(h, BUILT, base_sync=True)))


def test_no_ready_report_when_a_stuck_card_self_corrects():
    h = Harness()
    stuck_461(h)
    h.send(P, ev.ReconcileDue())
    r = h.send(P, red(h, SYNCED, AUDIT))
    assert h.p().stage == Stage.READY and not ready_reports(r)
    h.send(P, red(h, SYNCED, AUDIT, WEB))
    assert not all_ready_reports(h)


def test_no_ready_report_when_the_owner_drags_to_ready():
    h = Harness()
    built(h)
    drag_to_ready(h)
    r = h.send(P, red(h, BUILT, AUDIT))
    assert h.p().stage == Stage.READY and not ready_reports(r)
    h.send(P, evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN))
    assert not all_ready_reports(h)


def test_stuck_461_self_corrects_even_after_an_incomplete_tree_scan():
    """Live #461 at 18:57: an incomplete, non-busy scan cleared ``quiescent``. The owner
    already moved the card to Ready, so the waiting run still counts as done."""
    h = Harness()
    stuck_461(h)
    h.send(P, ev.TreeQuiescent(session_id=h.cur().session_id, busy=False, complete=False))
    assert not h.cur().quiescent
    h.send(P, ev.ReconcileDue())
    r = h.send(P, red(h, SYNCED, AUDIT))
    assert_quiet(r) and not ready_reports(r)
    assert_ready_blocked(h, AUDIT)


def test_auto_park_waits_for_an_idle_scan_of_the_run():
    """Without the owner's move the factory parks only a run seen idle."""
    h = Harness()
    built(h)
    h.send(P, ev.TreeQuiescent(session_id=h.cur().session_id, busy=False, complete=False))
    r = h.send(P, red(h, BUILT, base_sync=True))
    assert h.p().stage == Stage.BUILDING and not ready_reports(r)
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, red(h, BUILT, base_sync=True))
    assert len(ready_reports(r)) == 1
    assert_ready_blocked(h)


# ------------------------------------- 7. a woken run that ends without a result (#675)

#: #675 / PR #723: Rosie's rework head, approved by the owner, only the audit red.
REWORKED = "917a8f97231335b0bc7da8718fdae93da67b171e"


def reworked_and_woken(h: Harness) -> None:
    """#675 up to 12:22 ACST: Ready, the owner's review starts a rework, the rework run
    submits build_ready, the read shows only the audit red (outside the change), and the
    one readiness wake goes to the idle run."""
    built(h)
    h.send(P, evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN))
    assert h.p().stage == Stage.READY
    # 02:16Z the owner's PR review: rework in the same issue session.
    h.send(P, ev.PlanFeedback(text_digest="generate the route list", pr_number=PR))
    assert h.p().stage == Stage.BUILDING and h.p().readiness is None
    h.send(P, ev.CapacityAvailable())
    h.create_ok()
    # 02:47Z push, 02:51Z build_ready; the first read lands before the turn ends.
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=REWORKED, bot_authored=True, parcel_branch=True))
    submit_ready(h, REWORKED)
    h.send(P, red(h, REWORKED))
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, red(h, REWORKED))
    assert [e.args["purpose"] for e in r.effects if e.kind == EffectKind.SEND_MESSAGE] == [
        "readiness_wake"
    ]
    assert h.p().readiness_wakes == 1


def test_675_wake_answered_without_a_new_result_parks_ready_blocked():
    """Live #675: woken, Rosie explained the red audit on the PR, her same-head
    build_ready was refused (spent slot) and her turn ended. Base: Building, Bot Working
    on every reconcile forever. Now: Ready, Bot Blocked, one Ready report, no new run."""
    h = Harness()
    reworked_and_woken(h)
    run = h.cur()
    sessions = len(h.p().sessions)
    # A read before the woken run starts its turn does not take it as finished.
    r = h.send(P, red(h, REWORKED))
    assert h.p().stage == Stage.BUILDING and not ready_reports(r)
    h.send(P, ev.RuntimeActivity(session_id=run.session_id, busy=True))
    h.quiesce(P, run.session_id)  # turn over, no result
    h.send(P, ev.ReconcileDue())
    r = h.send(P, red(h, REWORKED))
    assert EffectKind.POST_COMMENT not in kinds(r) and EffectKind.SEND_MESSAGE not in kinds(r)
    assert len(ready_reports(r)) == 1
    assert_ready_blocked(h)
    assert h.cur().session_id == run.session_id and len(h.p().sessions) == sessions
    for _ in range(3):  # later reconcile reads: quiet, still one report
        r = h.send(P, red(h, REWORKED))
        assert_quiet(r) and not ready_reports(r)
    assert_ready_blocked(h)


def test_wake_answered_without_a_result_and_now_green_lands_in_ready_idle():
    h = Harness()
    reworked_and_woken(h)
    h.send(P, ev.RuntimeActivity(session_id=h.cur().session_id, busy=True))
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, evidence(h, REWORKED, verified=True, checks=ev.ChecksState.GREEN))
    assert len(ready_reports(r)) == 1
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and p.bot == BotState.IDLE
    assert h.cur().lifecycle == Lifecycle.RETIRED


def test_comment_relay_answered_without_a_result_needs_you_not_working():
    """An owner comment relayed to the waiting run; its turn ends without a new
    build_ready. Its last build_ready may not reflect the comment: Needs you, never an
    endless Working."""
    h = Harness()
    built(h)
    r = h.send(P, ev.PlanFeedback(text_digest="is the audit ours?"))
    assert "feedback" in [e.args.get("purpose") for e in r.effects]
    h.send(P, ev.RuntimeActivity(session_id=h.cur().session_id, busy=True))
    h.quiesce(P, h.cur().session_id)
    h.send(P, evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert p.board_note.endswith("without submitting a new build_ready")
    # A red read says the same, with the not-ready comment once.
    r = h.send(P, red(h, BUILT))
    assert h.p().bot == BotState.NEEDS_YOU and EffectKind.SEND_MESSAGE not in kinds(r)
    # The run then re-submits: back on the normal path (green: Ready).
    submit_ready(h, BUILT)
    h.quiesce(P, h.cur().session_id)
    h.send(P, evidence(h, BUILT, verified=True, checks=ev.ChecksState.GREEN))
    assert h.p().stage == Stage.READY and h.p().bot == BotState.IDLE
