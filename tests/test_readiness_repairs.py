"""Readiness repairs on the per-stage model (design-r2 §E, workstream 1).

Each test is a regression for a live failure: a head change dead-ending as
``checks-for-stale-head`` (#683), webhooks before the build result being lost, a merged
parcel regressing to Building (#682), drag + /plan restarting a running plan, the one
readiness fix wake, and credential issuance re-enabled on every reconcile.
"""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectIntent, EffectKind, MessagePurpose, Preconditions
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import BotState, Hold, Lifecycle, Size, Stage, Via, WaitReason
from omnigent_factory.testing.builders import contract_text, result_candidate
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"
PR = 7
OLD = HEAD
#: The owner's merge-from-main commit on the PR branch (#683: 2041e6e2 -> beae9656).
NEW = "b" * 40


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def sends(result) -> list[EffectIntent]:
    return [e for e in result.effects if e.kind == EffectKind.SEND_MESSAGE]


def comments(result, template: str) -> list[EffectIntent]:
    return [
        e
        for e in result.effects
        if e.kind == EffectKind.POST_COMMENT and e.args.get("template") == template
    ]


def build_ready(h: Harness, head: str = OLD):
    s = h.cur(P)
    assert s.root_id is not None
    return h.send(
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


def evidence(h: Harness, head: str, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def fetch_head(result) -> str:
    [fetch] = [e for e in result.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE]
    return str(fetch.args["head_sha"])


# --------------------------------------------------------------- 1. head changes


def test_owner_merge_from_main_reevaluates_the_new_head_and_retires_old_failures():
    """#683: stuck on the old head with checks/readiness failures while the PR is green."""
    h = Harness()
    h.to_building()
    build_ready(h, OLD)
    h.quiesce(P, h.cur(P).session_id)
    # The old head's failure holds, as the live parcel had them.
    h.parcels[P] = replace(h.p(), holds=h.p().holds | {Hold.CHECKS_FAILED, Hold.READINESS_FAILED})
    # The owner merges main into the branch: synchronize is a hint, a fresh read decides.
    r = h.send(P, ev.PRObserved(pr_number=PR, head_sha=NEW, bot_authored=True, parcel_branch=True))
    assert fetch_head(r) == OLD
    # Checks for the new head are not a stale-head dead end: they also re-read.
    r = h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=NEW, state=ev.ChecksState.GREEN))
    assert r.audit.accepted and fetch_head(r) == OLD
    # The read reports the actual head: the new head becomes current and is re-read.
    r = h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.GREEN))
    p = h.p()
    assert p.readiness.head_sha == NEW and not p.readiness.verified
    assert not p.holds & {Hold.CHECKS_FAILED, Hold.READINESS_FAILED}
    assert fetch_head(r) == NEW and not sends(r)
    # Current-head evidence verified (e.g. attested after the refresh) reaches Ready.
    h.send(P, evidence(h, NEW, verified=True, checks=ev.ChecksState.GREEN))
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and p.readiness.head_sha == NEW


def test_late_old_head_webhook_never_rolls_the_head_back_or_sets_a_failure():
    h = Harness()
    h.to_building()
    build_ready(h, OLD)
    h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    assert h.p().readiness.head_sha == NEW
    r = h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=OLD, state=ev.ChecksState.FAILED))
    assert fetch_head(r) == NEW  # re-reads the current head only
    r = h.send(P, evidence(h, NEW, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    p = h.p()
    assert p.readiness.head_sha == NEW and not p.holds & {Hold.CHECKS_FAILED, Hold.READINESS_FAILED}
    # A read that was asked about the old head is stale: audited, no effect.
    r = h.send(P, evidence(h, OLD, checks=ev.ChecksState.FAILED))
    assert not r.audit.accepted and h.p().readiness.head_sha == NEW


# ------------------------------------------------ 3. checks before the build result


def test_checks_before_build_ready_are_caught_up_and_pending_is_waiting_not_needs_you():
    h = Harness()
    h.to_building()
    r = h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=OLD, state=ev.ChecksState.GREEN))
    assert r.audit.reason != "checks-for-unknown-pr"
    r = build_ready(h, OLD)
    assert fetch_head(r) == OLD  # the result always triggers a fresh read
    h.quiesce(P, h.cur(P).session_id)
    h.send(P, evidence(h, OLD, checks=ev.ChecksState.PENDING))
    p = h.p()
    assert Hold.READINESS_FAILED not in p.holds and p.bot != BotState.NEEDS_YOU
    # No further webhook arrives: the periodic reconcile re-reads the unverified head.
    r = h.send(P, ev.ReconcileDue())
    assert fetch_head(r) == OLD
    h.send(P, evidence(h, OLD, verified=True, checks=ev.ChecksState.GREEN))
    assert h.p().stage == Stage.READY


def test_check_webhook_without_a_pr_number_refers_to_the_linked_pr():
    h = Harness()
    h.to_building()
    build_ready(h, OLD)
    r = h.send(P, ev.ChecksChanged(pr_number=0, head_sha=OLD, state=ev.ChecksState.GREEN))
    assert r.audit.accepted and fetch_head(r) == OLD


# ------------------------------------------------------------ 4. terminal state


def test_merged_ready_parcel_never_regresses_to_building():
    """#682: merged, then a late read/check/head/review said "no longer Ready"."""
    h = Harness()
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY
    h.send(
        P,
        ev.PRObserved(
            pr_number=PR,
            head_sha=OLD,
            open=False,
            merged=True,
            bot_authored=True,
            parcel_branch=True,
        ),
    )
    late = [
        evidence(h, OLD, verified=False, pr_open=False, merged=True, checks=ev.ChecksState.GREEN),
        ev.ChecksChanged(pr_number=PR, head_sha=NEW, state=ev.ChecksState.FAILED),
        ev.ReviewChanged(pr_number=PR, head_sha=NEW, changes_requested=True),
        ev.PRObserved(pr_number=PR, head_sha=NEW, bot_authored=True, parcel_branch=True),
    ]
    for body in late:
        r = h.send(P, body)
        p = h.p()
        assert p.stage == Stage.READY, body
        assert not comments(r, "ready-invalidated") and EffectKind.MOVE_CARD not in kinds(r)
        assert Hold.REWORK_CONTROL_REQUIRED not in p.holds and p.bot != BotState.NEEDS_YOU


def test_fresh_read_showing_the_merge_is_terminal_even_without_the_pr_webhook():
    h = Harness()
    h.to_building()
    h.build_ready()
    r = h.send(P, evidence(h, OLD, pr_open=False, merged=True, checks=ev.ChecksState.GREEN))
    p = h.p()
    assert Hold.COMPLETED in p.holds and p.stage == Stage.READY
    assert not comments(r, "ready-invalidated")


def test_issue_closed_is_terminal_for_readiness():
    h = Harness()
    h.to_building()
    build_ready(h, OLD)
    h.send(P, ev.Closed())
    r = h.send(P, evidence(h, OLD, checks=ev.ChecksState.FAILED))
    assert not sends(r) and not comments(r, "ready-blocked")
    assert Hold.READINESS_FAILED not in h.p().holds


def test_queued_regression_is_cancelled_at_the_call_boundary_after_the_merge():
    h = Harness()
    h.to_building()
    h.build_ready()
    h.send(
        P,
        ev.PRObserved(
            pr_number=PR,
            head_sha=OLD,
            open=False,
            merged=True,
            bot_authored=True,
            parcel_branch=True,
        ),
    )
    p = h.p()
    queued = [
        EffectIntent(
            "ef_c",
            EffectKind.POST_COMMENT,
            P,
            P,
            Preconditions(1, 0),
            {"template": "ready-blocked", "reason": "checks-or-head-changed"},
        ),
        EffectIntent(
            "ef_m",
            EffectKind.MOVE_CARD,
            P,
            P,
            Preconditions(1, 0),
            {"to": Stage.BUILDING.value, "expected_from": Stage.READY.value},
        ),
    ]
    assert [effect_still_valid(p, e) for e in queued] == ["parcel-completed"] * 2
    ok = EffectIntent(
        "ef_ok", EffectKind.POST_COMMENT, P, P, Preconditions(1, 0), {"template": "stopped"}
    )
    assert effect_still_valid(p, ok) is None


# ------------------------------------------------------- 5. equivalent requests


def test_drag_then_plan_command_attaches_to_the_running_plan():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.create_ok(P)
    assert s.lifecycle == Lifecycle.ACTIVE
    before = h.p()
    h.f(P).tick(60_000_000)  # a minute later
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    p = h.p()
    assert not r.audit.accepted and r.audit.reason == "equivalent-run-in-flight"
    assert r.effects == () and p.current_session_id == s.session_id
    assert p.revision == before.revision and p.authorizations == before.authorizations
    assert h.cur(P).lifecycle == Lifecycle.ACTIVE and not h.cur(P).fences
    # Before the session exists (pending authority) the same holds.
    h2 = Harness()
    h2.eligible(P)
    h2.send(P, ev.RequestPlan(via=Via.DRAG))
    auths = h2.p().authorizations
    r = h2.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert not r.audit.accepted and h2.p().authorizations == auths


def test_plan_after_a_stop_is_a_new_run_not_a_duplicate():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    h.create_ok(P)
    h.send(P, ev.Stop())
    auths = len(h.p().authorizations)
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert r.audit.accepted and len(h.p().authorizations) == auths + 1


def test_duplicate_triage_request_attaches_to_the_running_triage():
    h = Harness()
    h.eligible(P)
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok(P)
    r = h.send(P, ev.RequestTriage(via=Via.COMMAND))
    assert not r.audit.accepted and r.effects == ()
    assert h.cur(P).session_id == s.session_id and not h.cur(P).fences


# -------------------------------------------------------------- 6. the one wake


def _idle_build_ready(h: Harness, head: str = OLD) -> None:
    build_ready(h, head)
    h.quiesce(P, h.cur(P).session_id)


def test_genuine_failure_wakes_the_idle_build_once_then_needs_you_with_the_reason():
    h = Harness()
    h.to_building()
    _idle_build_ready(h)
    r = h.send(
        P,
        evidence(
            h, OLD, checks=ev.ChecksState.FAILED, checks_summary="3 checks: 2 success, 1 failure"
        ),
    )
    [wake] = sends(r)
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value
    assert "required checks failed" in str(wake.args["reason"])
    p = h.p()
    assert p.readiness_wakes == 1 and h.cur(P).lifecycle == Lifecycle.ACTIVE
    assert Hold.READINESS_FAILED not in p.holds and p.bot == BotState.WORKING
    # The fix is pushed and re-reported; still red: no second wake, Needs you + reason.
    _idle_build_ready(h, NEW)
    r = h.send(P, evidence(h, NEW, checks=ev.ChecksState.GREEN, findings_open=True))
    assert not sends(r)
    [blocked] = comments(r, "ready-blocked")
    assert "review-bot findings" in str(blocked.args["reason"])
    p = h.p()
    assert Hold.READINESS_FAILED in p.holds and p.bot == BotState.NEEDS_YOU
    r = h.send(P, evidence(h, NEW, checks=ev.ChecksState.GREEN, findings_open=True))
    assert not sends(r) and not comments(r, "ready-blocked")  # no comment spam


def test_pending_checks_and_a_busy_tree_never_wake_or_need_you():
    h = Harness()
    h.to_building()
    build_ready(h, OLD)  # tree not yet observed quiescent
    r = h.send(P, evidence(h, OLD, checks=ev.ChecksState.FAILED))
    assert not sends(r) and Hold.READINESS_FAILED not in h.p().holds
    h.quiesce(P, h.cur(P).session_id)
    r = h.send(P, evidence(h, OLD, checks=ev.ChecksState.PENDING))
    p = h.p()
    assert not sends(r) and p.readiness_wakes == 0
    assert Hold.READINESS_FAILED not in p.holds and p.bot == BotState.WORKING


def test_missing_current_head_review_after_a_head_change_uses_the_same_single_wake():
    h = Harness()
    h.to_building()
    _idle_build_ready(h, OLD)
    h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.GREEN))
    r = h.send(P, evidence(h, NEW, checks=ev.ChecksState.GREEN, review_accepted=False))
    [wake] = sends(r)
    assert NEW[:12] in str(wake.args["reason"]) and h.p().readiness_wakes == 1


def test_wake_count_is_persisted_with_the_parcel():
    h = Harness()
    h.to_building()
    p = replace(h.p(), readiness_wakes=1)
    assert parcel_from_json(parcel_to_json(p)).readiness_wakes == 1


# ------------------------------------------------------------ 7. issuance


def test_reconcile_does_not_re_enable_issuance_that_is_already_enabled():
    h = Harness()
    h.to_building()
    assert h.cur(P).issuance_enabled
    for _ in range(2):
        r = h.send(P, ev.ReconcileDue())
        assert EffectKind.ENABLE_ISSUANCE not in kinds(r)
    # Unknown to the reducer (e.g. state from before this fix): enabled once, then quiet.
    s = h.cur(P)
    h.parcels[P] = replace(
        h.p(),
        sessions=tuple(
            replace(x, issuance_enabled=False) if x.session_id == s.session_id else x
            for x in h.p().sessions
        ),
    )
    assert EffectKind.ENABLE_ISSUANCE in kinds(h.send(P, ev.ReconcileDue()))
    assert EffectKind.ENABLE_ISSUANCE not in kinds(h.send(P, ev.ReconcileDue()))


# ------------------------------------------ Ready + owner "Update branch" (base sync)


def _ready(h: Harness):
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY and h.p().readiness.ready


def _no_regression(r) -> None:
    assert not comments(r, "ready-invalidated") and not comments(r, "ready-blocked")
    assert EffectKind.MOVE_CARD not in kinds(r) and EffectKind.PUBLISH_REPORT not in kinds(r)


def test_update_branch_on_a_ready_pr_is_re_evaluated_in_ready_without_rework():
    h = Harness()
    _ready(h)
    r = h.send(P, ev.PRObserved(pr_number=PR, head_sha=NEW, bot_authored=True, parcel_branch=True))
    _no_regression(r)
    r = h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    _no_regression(r)
    [fetch] = [e for e in r.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE]
    # The accepted review stays bound to the head it attested; the read decides whether
    # the newer commits only sync the base branch.
    assert fetch.args["head_sha"] == NEW and fetch.args["reviewed_head"] == OLD
    p = h.p()
    assert p.stage == Stage.READY and Hold.REWORK_CONTROL_REQUIRED not in p.holds
    assert p.bot == BotState.IDLE  # waiting on the new head's checks: nothing to do
    assert p.board_note == f"Checks running on `{NEW[:7]}`"
    for _ in range(2):  # pending, pending: still waiting, no comment spam
        r = h.send(P, evidence(h, NEW, checks=ev.ChecksState.PENDING))
        _no_regression(r)
        assert h.p().stage == Stage.READY and h.p().bot == BotState.IDLE
    r = h.send(P, evidence(h, NEW, verified=True, checks=ev.ChecksState.GREEN))
    _no_regression(r)
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and p.readiness.head_sha == NEW
    assert p.bot == BotState.IDLE and not p.holds & {Hold.REWORK_CONTROL_REQUIRED}


def test_same_head_check_rerun_on_a_ready_card_waits_in_ready():
    h = Harness()
    _ready(h)
    r = h.send(P, ev.ChecksChanged(pr_number=PR, head_sha=OLD, state=ev.ChecksState.PENDING))
    assert fetch_head(r) == OLD
    r = h.send(P, evidence(h, OLD, checks=ev.ChecksState.PENDING))
    _no_regression(r)
    assert h.p().stage == Stage.READY and h.p().bot == BotState.IDLE
    assert h.p().board_note == f"Checks running on `{OLD[:7]}`"
    h.send(P, evidence(h, OLD, verified=True, checks=ev.ChecksState.GREEN))
    assert h.p().readiness.ready and h.p().bot == BotState.IDLE
    assert h.p().board_note == ""  # green: the note is cleared


def test_red_unreviewed_new_head_on_a_ready_card_needs_you_with_the_reason():
    """New commits the accepted review does not cover (not a base sync) leave Ready. A
    red check alone on a reviewed head stays in Ready, Bot Blocked (#461)."""
    h = Harness()
    _ready(h)
    h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    r = h.send(
        P,
        evidence(
            h, NEW, checks=ev.ChecksState.FAILED, checks_summary="1 failure", review_accepted=False
        ),
    )
    assert not comments(r, "ready-invalidated")  # informational: the card's note, no comment
    p = h.p()
    assert p.note.startswith("No longer ready: ") and "required checks failed" in p.note
    assert "no accepted cross vendor review" in p.note
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU and not sends(r)


def test_red_check_alone_on_a_reviewed_new_head_stays_in_ready_blocked():
    h = Harness()
    _ready(h)
    h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    r = h.send(P, evidence(h, NEW, checks=ev.ChecksState.FAILED, checks_summary="1 failure"))
    assert not comments(r, "ready-invalidated") and not sends(r)
    p = h.p()
    assert p.stage == Stage.READY and p.bot == BotState.BLOCKED
    assert p.note == "Required check red: see the PR checks"


def test_owner_approval_of_the_new_head_is_not_a_rework_instruction():
    h = Harness()
    _ready(h)
    r = h.send(P, ev.ReviewChanged(pr_number=PR, head_sha=NEW, changes_requested=False))
    _no_regression(r)
    assert h.p().stage == Stage.READY and fetch_head(r) == OLD
    r = h.send(P, ev.ReviewChanged(pr_number=PR, head_sha=NEW, changes_requested=True))
    assert h.p().stage == Stage.BUILDING  # an explicit change request still is


# ------------------------------------------------- review round 1 (F3, F4, G1)


def _plan_result_unpublished(h: Harness) -> EffectIntent:
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.create_ok(P)
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            h.p().revision,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.CONTRACT,
            contract_canonical=contract_text("M", "Ship it"),
            size=Size.M,
            open_decision_ids=(),
        ),
    )
    [publish] = [e for e in r.effects if e.kind == EffectKind.PUBLISH_CONTRACT]
    assert h.cur(P).wait_reason == WaitReason.PLAN_APPROVAL
    return publish


def test_duplicate_plan_while_the_contract_publication_is_pending_attaches():
    h = Harness()
    publish = _plan_result_unpublished(h)
    s = h.cur(P)
    auths = h.p().authorizations
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert not r.audit.accepted and r.audit.reason == "equivalent-run-in-flight"
    assert r.effects == () and h.p().authorizations == auths
    assert h.cur(P).session_id == s.session_id and h.cur(P).lifecycle == Lifecycle.WAITING
    # After a restart (aggregate reloaded from the store) and on replay of the same event.
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    event = h.f(P).make(ev.RequestPlan(via=Via.COMMAND))
    for _ in range(2):
        r = h.apply(event)
        assert not r.audit.accepted and h.p().authorizations == auths
    assert h.cur(P).lifecycle == Lifecycle.WAITING and not h.cur(P).fences
    # Once the owner can see the contract, a new /plan is a deliberate new run.
    cid = str(publish.args["contract_id"])
    h.send(
        P,
        ev.ContractPublished(
            contract_id=cid, comment_id=f"c-{cid}", verified=True, posted_at_us=h.f(P).now
        ),
    )
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert r.audit.accepted and len(h.p().authorizations) == len(auths) + 1


def test_catch_up_read_returns_a_ready_card_to_ready_without_a_completion_webhook():
    h = Harness()
    _ready(h)
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=NEW, bot_authored=True, parcel_branch=True))
    h.send(P, evidence(h, OLD, observed_head_sha=NEW, checks=ev.ChecksState.PENDING))
    h.send(P, evidence(h, NEW, checks=ev.ChecksState.PENDING))
    assert h.p().stage == Stage.READY and h.p().bot == BotState.IDLE
    # The completion webhook never arrives: the periodic reconcile re-reads the head.
    r = h.send(P, ev.ReconcileDue())
    assert fetch_head(r) == NEW
    r = h.send(P, evidence(h, NEW, verified=True, checks=ev.ChecksState.GREEN))
    _no_regression(r)
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready and p.bot == BotState.IDLE
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE not in kinds(r)  # verified: no more polling


def test_catch_up_read_covers_needs_you_without_a_live_build_session():
    h = Harness()
    _ready(h)
    # Ready -> Needs you (a red check alone would stay in Ready, Bot Blocked: #461)
    h.send(P, evidence(h, OLD, checks=ev.ChecksState.FAILED, findings_open=True))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED
    assert fetch_head(h.send(P, ev.ReconcileDue())) == OLD


def test_merged_pr_read_is_terminal_with_the_issue_open_and_frees_the_slots():
    """#683 merged without `Closes #462`: the issue stayed open and held the slot."""
    h = Harness()
    h.to_building()
    _idle_build_ready(h)
    assert h.admission.building_count == 1 and h.admission.prospective_pr_count == 1
    r = h.send(P, evidence(h, OLD, pr_open=False, merged=True, checks=ev.ChecksState.GREEN))
    p = h.p()
    assert Hold.COMPLETED in p.holds and p.eligible  # the issue is still open
    assert EffectKind.CLEANUP_WORKSPACE in kinds(r) and h.admission.prospective_pr_count == 0
    assert h.cur(P).lifecycle == Lifecycle.DRAINING
    h.quiesce(P, h.cur(P).session_id)
    assert h.admission.building_count == 0
    assert EffectKind.FETCH_PR_EVIDENCE not in kinds(h.send(P, ev.ReconcileDue()))


def test_pr_without_a_closing_reference_is_not_ready_and_the_wake_names_it():
    h = Harness()
    h.to_building()
    _idle_build_ready(h)
    r = h.send(P, evidence(h, OLD, checks=ev.ChecksState.GREEN, closes_issue=False))
    [wake] = sends(r)
    assert "`Closes #1`" in str(wake.args["reason"]) and h.p().stage == Stage.BUILDING
    _idle_build_ready(h, NEW)
    r = h.send(P, evidence(h, NEW, checks=ev.ChecksState.GREEN, closes_issue=False))
    [blocked] = comments(r, "ready-blocked")
    assert "does not close issue #1" in str(blocked.args["reason"])
    assert h.p().bot == BotState.NEEDS_YOU
