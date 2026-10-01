"""Ready + owner "Update branch" + a red required check from the base branch (#651).

#461 generalised this to any red-check-only read in Ready (tests/test_ready_red_checks.py).

The owner merged main into a Ready PR; the review was carried forward (base sync only),
but a required check that main itself broke went red. Nothing the build can fix: the
card stays in Ready with Bot Blocked and a note naming the check, no comment, no rework
hold and no wake; a green read (or a newer owner sync) restores Idle. Cards the old rule
already withdrew to Building self-correct on the next read.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectIntent, EffectKind
from omnigent_factory.core.types import BotState, Hold, Lifecycle, Stage
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"
PR = 7
OLD = HEAD
#: The owner's "Update branch" merge commit (#651: f0bf4423).
SYNC = "f" * 40
SYNC2 = "e" * 40
CHECK = "api / Dependency audit"
REWORK_HOLDS = {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED}


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def comments(result) -> list[EffectIntent]:
    return [e for e in result.effects if e.kind == EffectKind.POST_COMMENT]


def evidence(h: Harness, head: str, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def red(h: Harness, head: str = SYNC, *, base_sync: bool = True) -> ev.ReadinessEvidence:
    return evidence(
        h,
        head,
        checks=ev.ChecksState.FAILED,
        checks_summary="20 checks: 19 success, 1 failure",
        failing_checks=CHECK,
        base_sync=base_sync,
    )


def ready(h: Harness) -> None:
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY and h.p().readiness.ready


def update_branch(h: Harness, head: str = SYNC) -> None:
    """The owner's "Update branch": synchronize hint, then the read reports the new head."""
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=head, bot_authored=True, parcel_branch=True))
    current = h.p().readiness.head_sha
    h.send(P, evidence(h, current, observed_head_sha=head, checks=ev.ChecksState.PENDING))
    assert h.p().readiness.head_sha == head


def assert_quiet(result) -> None:
    """No comment, no wake, no card move, no session work."""
    assert not comments(result)
    assert EffectKind.SEND_MESSAGE not in kinds(result)
    assert EffectKind.MOVE_CARD not in kinds(result)
    assert EffectKind.CREATE_SESSION not in kinds(result)
    assert EffectKind.ENABLE_ISSUANCE not in kinds(result)


def assert_sync_blocked(h: Harness) -> None:
    p = h.p()
    assert p.stage == Stage.READY and not p.readiness.ready and p.readiness.sync_red
    assert p.bot == BotState.BLOCKED
    assert p.board_note == f"Required check red: {CHECK}"
    assert not p.holds & (REWORK_HOLDS | {Hold.CHECKS_FAILED})
    assert h.cur().lifecycle == Lifecycle.RETIRED  # the Ready run stays closed


# ------------------------------------------------------- 1-2. the #651 sequence


def test_651_base_sync_red_check_keeps_the_card_in_ready_blocked_then_green_is_idle():
    h = Harness()
    ready(h)
    update_branch(h)
    assert h.p().stage == Stage.READY and h.p().bot == BotState.WORKING
    r = h.send(P, red(h))
    assert r.audit.accepted
    assert_quiet(r)
    assert_sync_blocked(h)
    assert [e.args["bot"] for e in r.effects if e.kind == EffectKind.SET_BOT] == ["Blocked"]
    # The periodic reconcile keeps reading; still red: no repeated writes or comments.
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(r)
    r = h.send(P, red(h))
    assert_quiet(r)
    assert EffectKind.SET_BOT not in kinds(r) and EffectKind.SET_NOTE not in kinds(r)
    assert_sync_blocked(h)
    # main is fixed and the checks re-run green on the same head: Idle, note cleared.
    r = h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=SYNC, state=ev.ChecksState.GREEN))
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(r)
    r = h.send(P, evidence(h, SYNC, verified=True, checks=ev.ChecksState.GREEN, base_sync=True))
    assert_quiet(r)
    assert EffectKind.PUBLISH_REPORT not in kinds(r)
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and not p.readiness.sync_red
    assert p.bot == BotState.IDLE and p.board_note == "" and p.note == ""


def test_a_later_owner_sync_head_re_evaluates_and_its_green_read_restores_idle():
    h = Harness()
    ready(h)
    update_branch(h)
    h.send(P, red(h))
    assert_sync_blocked(h)
    # The owner fixes main and updates the branch again: waiting, then green.
    update_branch(h, SYNC2)
    p = h.p()
    assert p.stage == Stage.READY and not p.readiness.sync_red and p.bot == BotState.WORKING
    assert p.board_note == ""
    r = h.send(P, evidence(h, SYNC2, checks=ev.ChecksState.PENDING, base_sync=True))
    assert_quiet(r)
    assert h.p().bot == BotState.WORKING
    r = h.send(P, evidence(h, SYNC2, verified=True, checks=ev.ChecksState.GREEN, base_sync=True))
    assert_quiet(r)
    assert h.p().readiness.ready and h.p().bot == BotState.IDLE and h.p().board_note == ""


def test_a_re_run_pending_after_the_red_read_is_waiting_not_blocked():
    h = Harness()
    ready(h)
    update_branch(h)
    h.send(P, red(h))
    r = h.send(P, evidence(h, SYNC, checks=ev.ChecksState.PENDING, base_sync=True))
    assert_quiet(r)
    p = h.p()
    assert p.stage == Stage.READY and not p.readiness.sync_red
    assert p.bot == BotState.WORKING and p.board_note == ""


def test_a_sync_head_with_another_failure_keeps_the_existing_rule():
    """Only a red check on a carried-forward review stays in Ready."""
    h = Harness()
    ready(h)
    update_branch(h)
    h.send(P, replace(red(h), findings_open=True))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.holds >= REWORK_HOLDS and not p.readiness.sync_red


# ------------------------------------------------------- 3. non-sync heads (#461)


def test_red_check_alone_on_a_reviewed_non_sync_head_also_stays_in_ready_blocked():
    """#461 generalised the rule: the Ready run is retired, so there is no wake to use;
    with the review accepted for the head a red check only makes Bot Blocked."""
    h = Harness()
    ready(h)
    update_branch(h)
    r = h.send(P, red(h, base_sync=False))
    assert_quiet(r)
    assert_sync_blocked(h)


def test_red_check_on_an_unreviewed_non_sync_head_still_withdraws_ready():
    h = Harness()
    ready(h)
    update_branch(h)
    r = h.send(P, replace(red(h, base_sync=False), review_accepted=False))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.holds >= REWORK_HOLDS
    assert p.bot == BotState.NEEDS_YOU and not p.readiness.sync_red
    assert p.note.startswith("No longer ready: ") and "required checks failed" in p.note
    assert EffectKind.SEND_MESSAGE not in kinds(r)
    # A later read never "self-corrects" it: two reasons, not just the checks.
    h.send(P, red(h, base_sync=False))
    assert h.p().stage == Stage.BUILDING and h.p().bot == BotState.NEEDS_YOU


def test_red_check_on_a_building_head_still_wakes_the_idle_build_once():
    h = Harness()
    h.to_building()
    s = h.cur()
    h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=OLD,
        ),
    )
    h.quiesce(P, s.session_id)
    r = h.send(P, red(h, OLD, base_sync=False))
    assert [e.args["purpose"] for e in r.effects if e.kind == EffectKind.SEND_MESSAGE] == [
        "readiness_wake"
    ]
    assert h.p().stage == Stage.BUILDING and not h.p().readiness.sync_red


# ------------------------------------------------------- 4. stuck cards self-correct


#: The live #651 note, written by the release that withdrew it (before the note rename).
LIVE_NOTE = (
    "Ready withdrawn: required checks failed "
    "(17 checks: 6 success, 6 skipped, 4 pending, 1 failure) on f0bf442"
)


#: The note the old rule wrote for #651 under the current wording.
OLD_RULE_NOTE = (
    "No longer ready: required checks failed (20 checks: 19 success, 1 failure) on fffffff"
)


def stuck_651(h: Harness, note: str | None = None) -> None:
    """The live #651 shape, as the old rule left it: withdrawn to Building for the red
    check alone, Ready run retired, persisted before the new Readiness fields existed."""
    ready(h)
    update_branch(h)
    h.send(P, replace(red(h, base_sync=False), review_accepted=False))  # a withdrawal
    p = h.p()
    assert p.stage == Stage.BUILDING and p.holds >= REWORK_HOLDS
    assert p.bot == BotState.NEEDS_YOU and h.cur().lifecycle == Lifecycle.RETIRED
    data = json.loads(parcel_to_json(p))
    for key in ("sync_red", "red_checks"):
        data["parcel"]["readiness"].pop(key)
    data["parcel"]["note"] = OLD_RULE_NOTE if note is None else note
    h.parcels[P] = parcel_from_json(json.dumps(data))


@pytest.mark.parametrize("note", [None, LIVE_NOTE])
def test_stuck_651_card_returns_to_ready_blocked_on_the_first_reconcile(note):
    h = Harness()
    stuck_651(h, note)
    session = h.cur()
    r = h.send(P, ev.ReconcileDue())
    [fetch] = [e for e in r.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE]
    assert fetch.args["head_sha"] == SYNC and fetch.args["reviewed_head"] == OLD
    r = h.send(P, red(h))
    assert r.audit.accepted
    assert not comments(r) and EffectKind.SEND_MESSAGE not in kinds(r)
    assert EffectKind.CREATE_SESSION not in kinds(r)
    assert EffectKind.ENABLE_ISSUANCE not in kinds(r)
    [move] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert move.args == {"to": "Ready", "expected_from": "Building"}
    assert_sync_blocked(h)
    assert h.cur() == session  # no session reopened
    # Then green: Idle with no comment.
    r = h.send(P, evidence(h, SYNC, verified=True, checks=ev.ChecksState.GREEN, base_sync=True))
    assert not comments(r) and EffectKind.PUBLISH_REPORT not in kinds(r)
    assert h.p().readiness.ready and h.p().bot == BotState.IDLE


def test_stuck_651_card_whose_checks_are_already_green_returns_to_ready_idle():
    h = Harness()
    stuck_651(h)
    r = h.send(P, evidence(h, SYNC, verified=True, checks=ev.ChecksState.GREEN, base_sync=True))
    assert not comments(r) and EffectKind.SEND_MESSAGE not in kinds(r)
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and not p.holds & REWORK_HOLDS
    assert p.bot == BotState.IDLE and p.board_note == ""


def test_stuck_shape_returns_on_a_reviewed_non_sync_read_too():
    h = Harness()
    stuck_651(h)
    r = h.send(P, red(h, base_sync=False))
    assert not comments(r) and EffectKind.SEND_MESSAGE not in kinds(r)
    assert EffectKind.CREATE_SESSION not in kinds(r)
    assert [e.args for e in r.effects if e.kind == EffectKind.MOVE_CARD] == [
        {"to": "Ready", "expected_from": "Building"}
    ]
    assert_sync_blocked(h)


def test_stuck_shape_with_an_unaccepted_review_read_is_left_alone():
    h = Harness()
    stuck_651(h)
    r = h.send(P, replace(red(h, base_sync=False), review_accepted=False))
    assert EffectKind.MOVE_CARD not in kinds(r)
    assert h.p().stage == Stage.BUILDING and h.p().bot == BotState.NEEDS_YOU


def test_owner_change_request_withdrawal_is_not_self_corrected():
    """An owner's change request is a rework instruction, never undone by a read."""
    h = Harness()
    ready(h)
    update_branch(h)
    h.send(P, ev.ReviewChanged(pr_number=PR, head_sha=SYNC, changes_requested=True))
    h.send(P, red(h))
    p = h.p()
    assert p.stage == Stage.BUILDING and Hold.REWORK_CONTROL_REQUIRED in p.holds
    assert not p.readiness.sync_red


# ------------------------------------------------------- 5. terminal and owner control


def test_merge_while_sync_blocked_is_final():
    h = Harness()
    ready(h)
    update_branch(h)
    h.send(P, red(h))
    h.send(P, evidence(h, SYNC, pr_open=False, merged=True, checks=ev.ChecksState.FAILED))
    p = h.p()
    assert Hold.COMPLETED in p.holds and p.bot == BotState.IDLE
    r = h.send(P, red(h))
    assert not comments(r) and EffectKind.MOVE_CARD not in kinds(r)
    assert Hold.COMPLETED in h.p().holds and h.p().bot == BotState.IDLE


def test_owner_comment_while_sync_blocked_still_starts_rework():
    h = Harness()
    ready(h)
    update_branch(h)
    h.send(P, red(h))
    r = h.send(P, ev.PlanFeedback(text_digest="please also do X", pr_number=0))
    assert r.audit.accepted
    p = h.p()
    assert p.stage == Stage.BUILDING and p.note == "Rework: owner feedback"
    assert p.readiness is None and p.bot == BotState.QUEUED
