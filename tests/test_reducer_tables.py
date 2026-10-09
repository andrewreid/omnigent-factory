"""Every §2.6 transition-table row (A-E) plus rejecting defaults and totality (§2.8 #17).

Test names carry the table/row they cover, e.g. ``test_A03_*``.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind, MessagePurpose
from omnigent_factory.core.events import BODY_TYPES, EventKind, Provenance
from omnigent_factory.core.reducer import HANDLERS, transition
from omnigent_factory.core.types import (
    MICROS_PER_HOUR,
    ApprovalKind,
    BotState,
    DecisionImpact,
    DecisionStatus,
    FenceKind,
    Hold,
    Lifecycle,
    Parcel,
    QueueStatus,
    SessionKind,
    Size,
    Stage,
    State,
    Via,
)
from omnigent_factory.testing.builders import (
    OTHER_USER_ID,
    OWNER_ID,
    config,
    contract_text,
    result_candidate,
    snapshot,
)
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"
Q = "I_parcel_2"


def starts_run(result):
    """A new stage run: a fresh issue session (create) or the live one reused (prepare)."""
    return bool({EffectKind.CREATE_SESSION, EffectKind.PREPARE_SESSION} & set(kinds(result)))


def kinds(result):
    return [e.kind for e in result.effects]


def work(result):
    return [e for e in result.effects if e.kind in WORK_BEARING_KINDS]


# ================================================================== table A


def test_A01_duplicate_event_is_unchanged_without_effects():
    h = Harness()
    h.eligible()
    event = h.f().make(ev.RequestTriage(via=Via.DRAG))
    first = h.apply(event)
    assert first.audit.accepted
    before = h.p()
    again = h.apply(event)
    assert again.duplicate and again.effects == () and h.p() == before


def test_A02_wrong_repository_or_parcel_rejected_without_dispatch():
    h = Harness()
    h.eligible()
    bad = replace(h.f().make(ev.RequestTriage()), repo_id="R_other")
    r = h.apply(bad)
    assert not r.audit.accepted and r.audit.reason == "wrong-repository" and r.effects == ()
    state = State(h.p(), h.admission, h.cfg)
    wrong = replace(h.f().make(ev.RequestTriage()), parcel_id=Q)
    r = transition(state, wrong)
    assert r.audit.reason == "wrong-parcel" and r.effects == ()


@pytest.mark.parametrize(
    "body",
    [ev.AssignedHuman(), ev.Closed(), ev.Transferred(), ev.Deleted(), ev.ItemRemoved()],
)
def test_A03_negative_eligibility_fences_and_drains(body):
    h = Harness()
    h.to_building()
    epoch = h.p().eligibility_epoch
    s = h.cur()
    r = h.send(P, body, actor=OTHER_USER_ID)
    p = h.p()
    cur = p.session(s.session_id)
    assert FenceKind.SAFETY in cur.fences and cur.lifecycle == Lifecycle.DRAINING
    assert p.eligibility_epoch == epoch + 1 and p.barrier_time_us > 0
    assert {EffectKind.DISABLE_ISSUANCE, EffectKind.INTERRUPT_TREE, EffectKind.SCAN_TREE} <= set(
        kinds(r)
    )
    assert not work(r)
    assert p.bot == BotState.WORKING  # safety drain is never Idle before Q
    assert h.admission.building_count == 1  # capacity held until verified stop


def test_A03_ineligible_snapshot_is_a_safety_fact_from_reconciler():
    h = Harness()
    h.to_building()
    r = h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(human_assigned=True),
        provenance=Provenance.RECONCILER,
    )
    assert r.audit.accepted
    assert FenceKind.SAFETY in h.cur().fences


def test_A04_eligibility_restored_is_still_held():
    h = Harness()
    h.eligible()
    h.send(P, ev.AssignedHuman(), actor=OTHER_USER_ID)
    r = h.send(P, ev.GitHubSnapshot(), evidence=snapshot())
    p = h.p()
    assert p.eligible and Hold.SAFETY in p.holds
    assert not work(r) and EffectKind.CREATE_SESSION not in kinds(r)


def test_A05_leftward_move_fences_but_daemon_echo_is_acknowledged():
    h = Harness()
    h.to_building()
    # A daemon MOVE_CARD echo is an acknowledgement, not a safety event.
    h.send(P, ev.StopTimeout(session_id="nope"))  # irrelevant, rejected
    s = h.cur()
    r = h.send(
        P, ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.INBOX), actor=OTHER_USER_ID
    )
    assert FenceKind.SAFETY in h.p().session(s.session_id).fences
    assert h.p().stage == Stage.INBOX
    assert not work(r)


def test_A05_daemon_move_echo_not_safety():
    h = Harness()
    h.plan_published()
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND))
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    # Echo of our own leftward rollback/rightward write is ack only.
    before = h.p().barrier_time_us
    h.send(P, ev.ColumnObserved(stage=Stage.BUILDING, daemon_effect_id=move.effect_id))
    assert h.p().barrier_time_us == before and move.effect_id not in h.p().pending_moves


def test_A06_owner_stop_fences_and_cancels_queue():
    h = Harness()
    h.plan_published()
    h.approve()
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED
    r = h.send(P, ev.Stop())
    assert h.admission.queue_entry(P).status == QueueStatus.CANCELLED
    assert Hold.STOPPED in h.p().holds and EffectKind.POST_COMMENT not in kinds(r)
    assert h.p().note == "Stopped: /stop" and EffectKind.SET_NOTE in kinds(r)
    h2 = Harness()
    b = h2.to_building()
    h2.send(P, ev.Stop())
    assert h2.p().session(b.session_id).fences == {FenceKind.STOPPED}


def test_A07_approval_invalidation_revokes_running_build():
    h = Harness()
    b = h.to_building()
    approval = h.p().current_approval_id
    r = h.send(P, ev.ApprovalInvalidated(approval_id=approval, reason="contract-edited"))
    p = h.p()
    assert p.current_approval_id is None and not p.approval(approval).valid
    assert p.revision_pending
    assert FenceKind.REVOKED in p.session(b.session_id).fences
    assert EffectKind.MOVE_CARD in kinds(r)
    assert not h.send(P, ev.ApprovalInvalidated(approval_id="other")).audit.accepted


def test_A08_quiescent_drain_fences_and_releases_capacity():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Stop())
    h.quiesce(P, b.session_id)
    s = h.p().session(b.session_id)
    assert s.lifecycle == Lifecycle.FENCED and s.fences == {FenceKind.STOPPED}
    assert h.admission.building_count == 0
    assert h.p().bot == BotState.IDLE


def test_A09_busy_or_incomplete_scan_keeps_draining():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Stop())
    for complete, busy in [(False, False), (True, True)]:
        r = h.send(P, ev.TreeQuiescent(session_id=b.session_id, complete=complete, busy=busy))
        assert h.p().session(b.session_id).lifecycle == Lifecycle.DRAINING
        assert {EffectKind.INTERRUPT_TREE, EffectKind.SCAN_TREE} <= set(kinds(r))


def test_A10_stop_timeout_blocks_and_holds_capacity():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Stop())
    h.send(P, ev.StopTimeout(session_id=b.session_id))
    s = h.p().session(b.session_id)
    assert s.lifecycle == Lifecycle.BLOCKED and FenceKind.STOPPED in s.fences
    assert h.p().bot == BotState.BLOCKED and h.admission.building_count == 1
    h.quiesce(P, b.session_id)  # later complete evidence closes the drain
    assert h.p().session(b.session_id).lifecycle == Lifecycle.FENCED
    assert Hold.STOP_UNVERIFIED not in h.p().holds


def test_A11_external_activity_on_fenced_tree_blocks_successor():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Stop())
    h.quiesce(P, b.session_id)
    r = h.send(P, ev.OwnerDirectOmnigentMessage(session_id=b.session_id, item_id="i"))
    assert not work(r)
    h.send(P, ev.RuntimeActivity(session_id=b.session_id, busy=True))
    h.send(P, ev.RequestTriage())
    # Fresh owner control accepted, but no successor while the old tree is active.
    assert h.p().current_session_id == b.session_id
    h.quiesce(P, b.session_id)
    assert h.cur().kind == SessionKind.TRIAGE


def test_A12_pause_is_operator_only_and_flag_only():
    h = Harness()
    h.eligible()
    r = h.send(P, ev.Pause(), provenance=Provenance.WEBHOOK, actor=OWNER_ID)
    assert not r.audit.accepted and not h.admission.paused
    r = h.apply(h.f().make(ev.Pause(), parcel_id=None))
    assert r.audit.accepted and h.admission.paused and r.effects == ()
    r = h.apply(h.f().make(ev.Unpause(), parcel_id=None))
    assert not h.admission.paused and kinds(r) == [EffectKind.WAKE_SCHEDULER]


def test_A13_irrelevant_events_are_audited_self_loops():
    h = Harness()
    h.eligible()
    before = h.p()
    for body in (
        ev.SessionCreated(session_id="missing"),
        ev.TreeQuiescent(session_id="missing", complete=True, busy=False),
        ev.CapacityAvailable(),
        ev.ChecksChanged(pr_number=1),
        ev.Continue(),
    ):
        r = h.send(P, body)
        assert not r.audit.accepted and not work(r)
    p = h.p()
    # Only the card's status note explains the refused owner control.
    assert p.note == "Command refused: nothing-to-continue"
    assert (
        replace(
            p,
            version=before.version,
            applied_event_ids=before.applied_event_ids,
            note=before.note,
            board_note=before.board_note,
        )
        == before
    )


def test_A13_non_owner_controls_never_trigger_work():
    h = Harness()
    h.eligible()
    for body in (ev.RequestTriage(), ev.RequestPlan(), ev.WaivePlan(), ev.Stop(), ev.Continue()):
        r = h.send(P, body, actor=OTHER_USER_ID)
        assert r.audit.reason == "control-from-non-owner" and r.effects == ()


def test_A13_control_must_postdate_barrier_and_carry_authenticated_source():
    h = Harness()
    h.eligible()
    h.send(P, ev.AssignedHuman(), actor=OTHER_USER_ID)
    barrier = h.p().barrier_time_us
    stale = h.f().make(ev.RequestTriage(), time_us=barrier)
    assert h.apply(stale).audit.reason == "control-not-fresh-after-barrier"
    r = h.send(P, ev.RequestTriage(), provenance=Provenance.RECONCILER)
    assert r.audit.reason == "provenance-not-admitted"


# ================================================================== table B


def test_B01_request_triage_creates_session_after_quiescence():
    h = Harness()
    h.eligible()
    h.auto_ack_moves = False
    r = h.send(P, ev.RequestTriage(via=Via.COMMAND))
    assert h.p().stage == Stage.TRIAGED
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    # No new tree until the executor acknowledges the exact board write.
    assert EffectKind.CREATE_SESSION not in kinds(r)
    r = h.send(P, ev.ColumnObserved(stage=Stage.TRIAGED, daemon_effect_id=move.effect_id))
    assert EffectKind.CREATE_SESSION in kinds(r)
    s = h.cur()
    assert s.kind == SessionKind.TRIAGE and s.lifecycle == Lifecycle.INTENT
    assert s.grant.duration_us == 2 * MICROS_PER_HOUR  # S for initial triage


def test_B01_early_control_without_project_item_is_retained():
    h = Harness()
    r = h.send(P, ev.RequestTriage(), evidence=snapshot(in_project=False))
    assert r.audit.accepted and EffectKind.ENSURE_PROJECT_ITEM in kinds(r)
    assert EffectKind.CREATE_SESSION not in kinds(r)
    assert Hold.NO_PROJECT_ITEM in h.p().holds
    r = h.send(P, ev.GitHubSnapshot(), evidence=snapshot())
    assert EffectKind.CREATE_SESSION in kinds(r)


def test_B02_request_plan_opens_revision():
    h = Harness()
    h.triage()
    r = h.send(P, ev.RequestPlan(via=Via.DRAG))
    p = h.p()
    assert p.stage == Stage.SCOPED and p.revision == 1 and p.revision_pending
    # The plan run reuses the triage run's issue session (prepare, no second create).
    assert EffectKind.PREPARE_SESSION in kinds(r) and EffectKind.CREATE_SESSION not in kinds(r)
    assert h.cur().kind == SessionKind.PLAN
    assert h.cur().grant.duration_us == 4 * MICROS_PER_HOUR


@pytest.mark.parametrize("how", ["command", "drag"])
def test_B03_replan_from_building_revokes_then_plans_after_quiescence(how):
    h = Harness()
    b = h.to_building()
    old_approval = h.p().current_approval_id
    if how == "command":
        r = h.send(P, ev.RequestReplan())
    else:
        r = h.send(
            P, ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED), actor=OWNER_ID
        )
    p = h.p()
    s = p.session(b.session_id)
    assert {FenceKind.SAFETY, FenceKind.REVOKED} <= s.fences
    assert not p.approval(old_approval).valid and p.revision_pending
    assert not starts_run(r)  # not before Q
    assert p.pending_authorization_id is not None
    r = h.quiesce(P, b.session_id)
    assert starts_run(r)
    assert h.cur().kind == SessionKind.PLAN
    assert h.cur().root_id == b.root_id  # same issue session; the build run stays fenced
    assert h.p().session(b.session_id).fences >= {FenceKind.SAFETY, FenceKind.REVOKED}


def test_B03_non_owner_building_to_scoped_records_negative_half_only():
    h = Harness()
    b = h.to_building()
    h.send(
        P, ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED), actor=OTHER_USER_ID
    )
    p = h.p()
    assert FenceKind.REVOKED in p.session(b.session_id).fences
    assert p.current_approval_id is None and p.revision_pending
    assert p.pending_authorization_id is None
    r = h.quiesce(P, b.session_id)
    assert EffectKind.CREATE_SESSION not in kinds(r)


def test_B04_feedback_on_current_plan_relays_and_opens_revision():
    h = Harness()
    h.plan_published()
    rev = h.p().revision
    r = h.send(P, ev.PlanFeedback(text_digest="d"))
    p = h.p()
    assert p.revision == rev + 1 and p.revision_pending
    [msg] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert msg.args["purpose"] == MessagePurpose.FEEDBACK.value
    assert h.cur().revision == p.revision


def test_B05_feedback_without_current_plan_starts_new_root():
    h = Harness()
    plan = h.plan_published()
    h.send(P, ev.Stop())
    h.quiesce(P, plan.session_id)
    r = h.send(P, ev.PlanFeedback(text_digest="d"))
    assert starts_run(r)
    assert h.cur().session_id != plan.session_id


def test_B06_older_output_cannot_clear_revision():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    s = h.create_ok()
    old_rev = s.revision
    h.send(P, ev.PlanFeedback(text_digest="d"))
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            old_rev,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.CONTRACT,
            contract_canonical=contract_text(),
            size=Size.M,
        ),
    )
    assert r.audit.reason == "result-for-stale-revision" and h.p().revision_pending


def test_B06_publication_of_superseded_revision_keeps_revision_pending():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    s = h.create_ok()
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.CONTRACT,
            contract_canonical=contract_text(),
            size=Size.M,
        ),
    )
    [pub] = Harness.of(r, EffectKind.PUBLISH_CONTRACT)
    h.send(P, ev.PlanFeedback(text_digest="late"))
    h.send(
        P,
        ev.ContractPublished(
            contract_id=pub.args["contract_id"],
            comment_id="c",
            verified=True,
            posted_at_us=h.f().now,
        ),
    )
    p = h.p()
    assert p.revision_pending and p.current_contract_id is None


def test_B07_triage_final_publishes_and_retires_after_quiescence():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.create_ok()
    r = h.send(
        P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE, size=Size.L)
    )
    assert EffectKind.PUBLISH_TRIAGE in kinds(r) and h.p().size == Size.L
    # Not Needs you until the owner can actually see the triage outcome.
    assert h.p().bot != BotState.NEEDS_YOU
    h.quiesce(P, s.session_id)
    assert h.p().session(s.session_id).lifecycle == Lifecycle.RETIRED
    [pub] = Harness.of(r, EffectKind.PUBLISH_TRIAGE)
    h.send(
        P,
        ev.PublicationAcked(
            effect_id=pub.effect_id,
            effect_kind=pub.kind.value,
            session_id=s.session_id,
            comment_id="c-1",
        ),
    )
    assert h.p().bot == BotState.IDLE  # triage posted: the owner's move


@pytest.mark.parametrize(
    "kind", [EffectKind.PUBLISH_TRIAGE, EffectKind.PUBLISH_CONTRACT, EffectKind.PUBLISH_REPORT]
)
def test_B07b_failed_publication_is_blocked_not_needs_you(kind):
    """Pilot bug 3: a definitively failed publication left the card at Needs you."""
    h = Harness()
    h.triage()
    h.send(P, ev.EffectCancelled(effect_id="ef_failed", effect_kind=kind.value))
    p = h.p()
    assert Hold.PUBLICATION_FAILED in p.holds and p.bot == BotState.BLOCKED
    # A later successful publication (operator retry) clears it.
    h.send(
        P,
        ev.PublicationAcked(
            effect_id="ef_failed", effect_kind=EffectKind.PUBLISH_TRIAGE.value, comment_id="c"
        ),
    )
    assert Hold.PUBLICATION_FAILED not in h.p().holds


def test_B07c_failed_triage_publication_never_reports_needs_you():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.create_ok()
    r = h.send(
        P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE, size=Size.S)
    )
    [pub] = Harness.of(r, EffectKind.PUBLISH_TRIAGE)
    h.send(
        P,
        ev.EffectCancelled(
            effect_id=pub.effect_id, effect_kind=pub.kind.value, session_id=s.session_id
        ),
    )
    h.quiesce(P, s.session_id)
    assert h.p().bot == BotState.BLOCKED


def test_B07d_enable_issuance_failure_blocks_the_stage():
    """Pilot bug 4: a stage ran on without its credential after enable failed."""
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.create_ok()
    h.send(
        P,
        ev.EffectCancelled(
            effect_id="ef_enable",
            effect_kind=EffectKind.ENABLE_ISSUANCE.value,
            session_id=s.session_id,
            failed=True,
        ),
    )
    p = h.p()
    assert Hold.PREPARE_FAILED in p.holds and p.bot == BotState.BLOCKED
    assert p.session(s.session_id).lifecycle == Lifecycle.DRAINING


def test_B07e_precondition_cancelled_enable_is_not_a_failure():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.create_ok()
    h.send(
        P,
        ev.EffectCancelled(
            effect_id="ef_enable",
            effect_kind=EffectKind.ENABLE_ISSUANCE.value,
            session_id=s.session_id,
        ),
    )
    assert Hold.PREPARE_FAILED not in h.p().holds


def test_B08_waiver_build_info_plan_is_informational():
    h = Harness()
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.DRAG))
    b = h.admit()
    r = h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.INFO,
            contract_canonical=contract_text(),
            size=Size.M,
        ),
    )
    assert EffectKind.PUBLISH_REPORT in kinds(r) and EffectKind.PUBLISH_CONTRACT not in kinds(r)
    assert h.p().current_approval.kind == ApprovalKind.SKIP and h.p().current_approval.valid
    assert h.p().contracts == ()


def test_B09_B10_plan_result_then_verified_publication():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    s = h.create_ok()
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.PLAN,
            publication_kind=ev.PublicationKind.CONTRACT,
            contract_canonical=contract_text(),
            size=Size.M,
        ),
    )
    assert h.p().revision_pending and EffectKind.PUBLISH_CONTRACT in kinds(r)
    [pub] = Harness.of(r, EffectKind.PUBLISH_CONTRACT)
    cid = pub.args["contract_id"]
    bad = h.send(P, ev.ContractPublished(contract_id=cid, comment_id="c", verified=False))
    assert bad.audit.accepted and Hold.PUBLICATION_FAILED in h.p().holds
    h.send(
        P,
        ev.ContractPublished(
            contract_id=cid, comment_id="c", verified=True, posted_at_us=h.f().now
        ),
    )
    p = h.p()
    assert not p.revision_pending and p.current_contract_id == cid
    assert Hold.PUBLICATION_FAILED not in p.holds and p.bot == BotState.IDLE


def test_B10_new_publication_supersedes_prior_and_voids_approval():
    h = Harness()
    h.plan_published(goal="first")
    first = h.p().current_contract_id
    h.send(P, ev.PlanFeedback(text_digest="more"))
    h.publish_plan(goal="second")
    p = h.p()
    assert p.contract(first).superseded and p.current_contract_id != first


def test_B11_stale_root_result_is_audit_only():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    s = h.create_ok()
    r = h.send(P, result_candidate(s.session_id, "other-root", s.revision, ev.ResultKind.PLAN))
    assert r.audit.reason == "result-from-stale-root" and r.effects == ()


def test_B12_invalid_drag_approval_rolls_back_card():
    h = Harness()
    h.plan_published()
    h.send(P, ev.PlanFeedback(text_digest="x"))  # revision pending
    r = h.send(P, ev.ApprovePlan(via=Via.DRAG))
    assert not r.audit.accepted and r.audit.reason == "plan-not-approvable"
    assert EffectKind.POST_COMMENT not in kinds(r) and h.p().stage == Stage.SCOPED
    assert h.p().note == "Command refused: plan-not-approvable; card moved back to Scoped"
    assert h.p().current_approval_id is None and not work(r)


def test_B13_malformed_result_blocks_without_correction_round_trip():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    s = h.create_ok()
    bad = result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.PLAN, valid=False)
    r = h.send(P, bad)
    assert not work(r) and Hold.RESULT_INVALID in h.p().holds
    assert h.p().bot == BotState.BLOCKED


def test_B14_feedback_during_checkpoint_is_recorded_without_time():
    h = Harness()
    plan = h.plan_published()
    h.send(P, ev.ActiveLimitReached(session_id=plan.session_id, grant_id=plan.grant.grant_id))
    r = h.send(P, ev.PlanFeedback(text_digest="x"))
    assert h.p().revision_pending and not work(r)
    assert h.cur().grant == h.p().session(plan.session_id).grant


# ================================================================== table C


def test_C01_plan_approval_queues_in_approval_order():
    h = Harness()
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(Q)
    h.approve(P)
    assert h.admission.queue_entry(Q).sequence < h.admission.queue_entry(P).sequence
    a = h.p(P).current_approval
    assert a.kind == ApprovalKind.PLAN and a.owner_id == OWNER_ID
    assert h.p(P).stage == Stage.BUILDING and h.p(P).bot == BotState.QUEUED
    assert h.p(P).note == "Queued: 2nd in line"
    assert h.p(Q).note == "Queued: 1st in line"


@pytest.mark.parametrize(
    ("stage_via", "ok"),
    [
        ((None, Via.DRAG), True),
        ((Stage.TRIAGED, Via.DRAG), True),
        ((Stage.SCOPED, Via.LABEL), True),
        ((Stage.SCOPED, Via.DRAG), False),
    ],
)
def test_C02_waiver_from_inbox_triaged_or_label(stage_via, ok):
    stage, via = stage_via
    h = Harness()
    h.eligible()
    if stage is not None:
        h.parcels[P] = replace(h.p(), stage=stage)
    r = h.send(P, ev.WaivePlan(via=via))
    assert r.audit.accepted is ok
    if ok:
        a = h.p().current_approval
        assert a.kind == ApprovalKind.SKIP and a.snapshot_canonical is not None
        assert h.admission.queue_entry(P).status == QueueStatus.QUEUED


def test_C02_waiver_requires_fresh_snapshot():
    h = Harness()
    h.eligible()
    r = h.send(P, ev.WaivePlan(), evidence=None)
    assert r.audit.reason == "waiver-without-fresh-snapshot"


def test_C03_duplicate_semantic_approval_acknowledged_once():
    h = Harness()
    h.plan_published()
    h.approve()
    seq = h.admission.next_sequence
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND))
    assert r.audit.accepted and EffectKind.POST_COMMENT not in kinds(r)
    assert h.p().note == "Approved: build starts when capacity allows"
    assert h.admission.next_sequence == seq and len(h.p().approvals) == 1


def test_C04_capacity_available_reserves_and_creates():
    h = Harness()
    h.plan_published()
    h.approve()
    r = h.send(P, ev.CapacityAvailable())
    assert h.admission.queue_entry(P).status == QueueStatus.RESERVED
    assert h.admission.building_count == 1
    s = h.cur()
    # The build run reuses the plan run's issue session: prepare it, no create.
    assert s.kind == SessionKind.BUILD and s.lifecycle == Lifecycle.PREPARING
    [prepare] = Harness.of(r, EffectKind.PREPARE_SESSION)
    assert prepare.args["reuse"] is True and prepare.preconditions.approval_id is not None
    assert not Harness.of(r, EffectKind.CREATE_SESSION)


def test_C05_admission_guards():
    h = Harness(cfg=config(max_building=1))
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(P)
    h.approve(Q)
    assert h.send(Q, ev.CapacityAvailable()).audit.reason == "not-queue-head"
    h.apply(h.f(P).make(ev.Pause(), parcel_id=None))
    assert h.send(P, ev.CapacityAvailable()).audit.reason == "paused"
    h.apply(h.f(P).make(ev.Unpause(), parcel_id=None))
    h.admit(P)
    assert h.send(Q, ev.CapacityAvailable()).audit.reason == "building-cap"


def test_C05_open_pr_cap():
    h = Harness(cfg=config(max_open_bot_prs=1, max_building=2))
    h.plan_published(P)
    h.approve(P)
    h.send(P, ev.PRObserved(pr_number=99, head_sha=HEAD, open=True, bot_authored=True))
    assert h.send(P, ev.CapacityAvailable()).audit.reason == "open-pr-cap"


def test_C06_session_created_prepares_and_verifies_nonce():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    assert not h.send(
        P, ev.SessionCreated(session_id=s.session_id, root_id="r", nonce="forged")
    ).audit.accepted
    r = h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="r", nonce=s.nonce))
    assert h.cur().lifecycle == Lifecycle.PREPARING
    [prep] = Harness.of(r, EffectKind.PREPARE_SESSION)
    assert prep.args["profile"] == "read_only"


def test_C07_definitive_create_rejection_blocks_without_retry():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    r = h.send(P, ev.CreateRejected(session_id=s.session_id, reason="bad agent"))
    assert EffectKind.CREATE_SESSION not in kinds(r)
    assert h.p().session(s.session_id).lifecycle == Lifecycle.RETIRED
    assert h.p().bot == BotState.BLOCKED


def test_C08_ambiguous_create_stays_unknown_until_exact_adoption():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    r = h.send(
        P, ev.EffectUnknown(effect_id="e", effect_kind="create_session", session_id=s.session_id)
    )
    assert h.cur().lifecycle == Lifecycle.UNKNOWN and kinds(r)[0] == EffectKind.RECONCILE_SESSION
    assert h.p().bot == BotState.BLOCKED
    for matches in (0, 2):
        r = h.send(P, ev.AdoptionResult(session_id=s.session_id, matches=matches, nonce=s.nonce))
        assert h.cur().lifecycle == Lifecycle.UNKNOWN and not work(r)
        assert EffectKind.CREATE_SESSION not in kinds(r)
    h.send(P, ev.AdoptionResult(session_id=s.session_id, matches=1, root_id="r1", nonce=s.nonce))
    assert h.cur().lifecycle == Lifecycle.PREPARING and h.cur().root_id == "r1"


def test_C09_prepared_starts_first_message_under_authority():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="r", nonce=s.nonce))
    r = h.send(P, ev.Prepared(session_id=s.session_id, ok=True, policy_ready_at_us=12345))
    # Prepared is not open: wait the propagation barrier and re-verify the policy set.
    assert h.cur().lifecycle == Lifecycle.PREPARING and not work(r)
    [verify] = Harness.of(r, EffectKind.VERIFY_POLICIES)
    assert verify.args == {"root_id": "r", "not_before_us": 12345, "reconcile": False}
    r = h.verify_policies()
    assert h.cur().lifecycle == Lifecycle.ACTIVE
    enable = Harness.of(r, EffectKind.ENABLE_ISSUANCE)[0]
    msg = Harness.of(r, EffectKind.SEND_MESSAGE)[0]
    assert r.effects.index(enable) < r.effects.index(msg)
    assert msg.args["purpose"] == MessagePurpose.FIRST.value
    assert msg.preconditions.authorization_id == s.authorization_id


def test_C09_safety_during_preparation_cancels_start():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="r", nonce=s.nonce))
    h.send(P, ev.AssignedHuman(), actor=OTHER_USER_ID)
    r = h.send(P, ev.Prepared(session_id=s.session_id, ok=True))
    assert not r.audit.accepted and not work(r)


def test_C09_unexpected_turn_before_preparation_fences():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="r", nonce=s.nonce))
    r = h.send(P, ev.Prepared(session_id=s.session_id, ok=True, unexpected_turn=True))
    assert FenceKind.SAFETY in h.cur().fences and not work(r)


def test_C10_C11_message_ack_and_ambiguous_message():
    h = Harness()
    b = h.to_building()
    first = next(e for e, sid in h.p().sent_effects if sid == b.session_id)
    assert not h.send(
        P, ev.MessageAck(session_id=b.session_id, effect_id="ef", item_id="x")
    ).audit.accepted  # never issued
    h.send(P, ev.MessageAck(session_id=b.session_id, effect_id=first, item_id="item-1"))
    assert h.cur().own_items == ("item-1",)
    r = h.send(
        P, ev.EffectUnknown(effect_id=first, effect_kind="send_message", session_id=b.session_id)
    )
    assert h.cur().message_unknown and h.p().bot == BotState.BLOCKED and not work(r)
    h.send(P, ev.MessageAck(session_id=b.session_id, effect_id=first, item_id="item-2"))
    assert h.p().unknown_effects == () and not h.cur().message_unknown


def test_C12_queued_entry_cancelled_by_invalidation():
    h = Harness()
    h.plan_published()
    h.approve()
    h.send(P, ev.ApprovalInvalidated(approval_id=h.p().current_approval_id, reason="x"))
    assert h.admission.queue_entry(P).status == QueueStatus.CANCELLED
    assert h.send(P, ev.CapacityAvailable()).audit.reason == "not-queued"


def test_C13_lowering_cap_never_evicts_or_admits():
    h = Harness(cfg=config(max_building=2))
    h.to_building(P)
    h.plan_published(Q)
    h.approve(Q)
    h.cfg = config(max_building=1)
    assert h.admission.building_count == 1
    assert h.send(Q, ev.CapacityAvailable()).audit.reason == "building-cap"
    assert h.cur(P).lifecycle == Lifecycle.ACTIVE


def test_C14_approve_hash_rules():
    h = Harness()
    h.plan_published()
    full = h.p().current_contract.full_hash
    assert not h.send(P, ev.ApprovePlan(via=Via.COMMAND, hash_text=full[:8])).audit.accepted
    assert not h.send(P, ev.ApprovePlan(via=Via.COMMAND, hash_text="0" * 12)).audit.accepted
    assert h.send(P, ev.ApprovePlan(via=Via.COMMAND, hash_text=full[:12])).audit.accepted


def test_C14_drag_needs_contract_posted_strictly_before():
    h = Harness()
    h.plan_published()
    posted = h.p().current_contract.posted_at_us
    r = h.apply(h.f().make(ev.ApprovePlan(via=Via.DRAG), time_us=posted))
    assert r.audit.reason in (
        "contract-not-posted-before-control",
        "control-not-fresh-after-barrier",
    )


def test_C14_grant_duration_bounds():
    h = Harness()
    h.plan_published()
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND, duration_us=0))
    assert r.audit.reason == "grant-duration-out-of-bounds"
    plan = h.cur()
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND, duration_us=6 * MICROS_PER_HOUR))
    assert r.audit.accepted
    h.quiesce(P, plan.session_id)
    h.send(P, ev.CapacityAvailable())
    assert h.cur().grant.duration_us == 6 * MICROS_PER_HOUR


# ================================================================== table D


def open_decision(h, impact=DecisionImpact.WITHIN_CONTRACT, pid=P):
    s = h.cur(pid)
    r = h.send(
        pid,
        ev.ElicitationOpened(
            session_id=s.session_id, elicitation_id=f"el{len(h.log)}", impact=impact
        ),
    )
    return r, h.p(pid).open_decisions[-1]


def test_D01_elicitation_opens_decision_and_waits():
    h = Harness()
    h.to_building()
    r, d = open_decision(h)
    assert h.cur().lifecycle == Lifecycle.WAITING and EffectKind.POST_COMMENT in kinds(r)
    assert h.p().bot == BotState.NEEDS_YOU and d.status == DecisionStatus.OPEN
    assert not h.send(
        P, ev.ElicitationOpened(session_id=d.session_id, elicitation_id=d.elicitation_id)
    ).audit.accepted


def test_D02_decide_within_contract_resolves_exact_prompt():
    h = Harness()
    h.to_building()
    _, d = open_decision(h)
    r = h.send(P, ev.Decide(answer="yes", within_contract=True))
    [res] = Harness.of(r, EffectKind.RESOLVE_ELICITATION)
    assert res.args["elicitation_id"] == d.elicitation_id
    assert h.cur().lifecycle == Lifecycle.ACTIVE
    assert h.p().decision(d.decision_id).status == DecisionStatus.RELAYED
    assert h.cur().grant.duration_us == 4 * MICROS_PER_HOUR  # no new time


def test_D02_ambiguous_decide_rejected():
    h = Harness()
    h.to_building()
    open_decision(h)
    open_decision(h)
    r = h.send(P, ev.Decide(answer="yes"))
    assert r.audit.reason == "decision-ambiguous" and not work(r)


def test_D03_build_answer_changing_contract_revokes_and_replans():
    h = Harness()
    b = h.to_building()
    open_decision(h, DecisionImpact.UNKNOWN)
    h.send(P, ev.Decide(answer="change scope"))
    p = h.p()
    assert FenceKind.REVOKED in p.session(b.session_id).fences
    assert p.revision_pending and p.current_approval_id is None
    assert p.pending_authorization_id is not None and p.stage == Stage.SCOPED


def test_D04_prompt_gone_in_omnigent_closes_the_decision_and_resumes_work():
    """Owner direction (#462/#677 pilot): a prompt answered in Omnigent is closed."""
    h = Harness()
    h.to_building()
    _, d = open_decision(h)
    assert h.p().bot == BotState.NEEDS_YOU
    h.send(P, ev.ElicitationGone(session_id=d.session_id, elicitation_id=d.elicitation_id))
    p = h.p()
    assert p.decision(d.decision_id).status == DecisionStatus.RESOLVED_IN_OMNIGENT
    assert not p.open_decisions and p.bot == BotState.WORKING
    assert p.session(d.session_id).lifecycle == Lifecycle.ACTIVE
    # Nothing is open to answer any more, so a late /decide is explained, not relayed.
    r = h.send(P, ev.Decide(answer="yes", within_contract=True))
    assert not r.audit.accepted and not Harness.of(r, EffectKind.SEND_MESSAGE)


def test_D04b_answer_given_here_before_the_prompt_disappears_is_still_relayed():
    h = Harness()
    h.to_building()
    _, d = open_decision(h)
    h.send(P, ev.Decide(answer="yes", within_contract=True))
    r = h.send(P, ev.ElicitationGone(session_id=d.session_id, elicitation_id=d.elicitation_id))
    assert h.p().decision(d.decision_id).status in (
        DecisionStatus.ANSWERED,
        DecisionStatus.RELAYED,
    )
    assert not r.audit.accepted or h.p().decision(d.decision_id).prompt_lost


def test_D05_uncorrelated_resolution_grants_nothing():
    h = Harness()
    h.to_building()
    _, d = open_decision(h)
    r = h.send(P, ev.ElicitationResolved(session_id=d.session_id, elicitation_id=d.elicitation_id))
    closed = h.p().decision(d.decision_id)
    assert closed.status == DecisionStatus.RESOLVED_IN_OMNIGENT and closed.externally_resolved
    # No answer relay or new authority: only the session's existing credential re-enabled.
    assert [e.kind for e in work(r)] == [EffectKind.ENABLE_ISSUANCE]
    assert closed.answer is None
    assert h.p().bot == BotState.WORKING


def test_D06_active_limit_enters_grace_once():
    h = Harness()
    b = h.to_building()
    r = h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    s = h.cur()
    assert s.lifecycle == Lifecycle.CHECKPOINT_GRACE and s.grant.grace_deadline_us is not None
    [msg] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert msg.args["purpose"] == MessagePurpose.CHECKPOINT_CLEANUP.value
    assert EffectKind.ARM_TIMER in kinds(r) and h.p().bot == BotState.CHECKPOINT
    again = h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    assert not again.audit.accepted


def test_D06_cost_ask_is_a_checkpoint_without_auto_answer():
    h = Harness()
    b = h.to_building()
    r = h.send(
        P, ev.ElicitationOpened(session_id=b.session_id, elicitation_id="ask", cost_ask=True)
    )
    assert h.cur().lifecycle == Lifecycle.CHECKPOINT_GRACE
    assert EffectKind.RESOLVE_ELICITATION not in kinds(r)


def checkpointed(h):
    b = h.to_building()
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    return b


def test_D07_D09_checkpoint_report_then_quiescent_fence():
    h = Harness()
    b = checkpointed(h)
    r = h.send(P, result_candidate(b.session_id, b.root_id, b.revision, ev.ResultKind.CHECKPOINT))
    assert h.cur().lifecycle == Lifecycle.CHECKPOINT_WAIT and EffectKind.PUBLISH_REPORT in kinds(r)
    h.quiesce(P, b.session_id)
    s = h.cur()
    assert s.lifecycle == Lifecycle.FENCED and s.fences == {FenceKind.CHECKPOINT}
    # The wrap-up drain is over: the settled checkpoint parks and frees its slot.
    assert h.admission.building_count == 0
    assert h.admission.queue_entry(P).status == QueueStatus.HELD and h.p().slot_parked
    assert h.p().bot == BotState.CHECKPOINT


def test_D08_grace_expiry_with_live_tree_fences_and_drains():
    h = Harness()
    b = checkpointed(h)
    r = h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    s = h.cur()
    assert s.lifecycle == Lifecycle.DRAINING and FenceKind.CHECKPOINT in s.fences
    assert EffectKind.INTERRUPT_TREE in kinds(r)


def test_D10_continue_then_policy_ready_clears_only_checkpoint():
    h = Harness()
    b = checkpointed(h)
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.quiesce(P, b.session_id)
    r = h.send(P, ev.Continue(duration_us=2 * MICROS_PER_HOUR))
    s = h.cur()
    assert not s.grant.ready and FenceKind.CHECKPOINT in s.fences
    assert kinds(r)[0] == EffectKind.REPLACE_COST_POLICY and not work(r)
    again = h.send(P, ev.Continue())  # no extra grant or policy write while preparing
    assert h.cur().grant == s.grant and not Harness.of(again, EffectKind.REPLACE_COST_POLICY)
    r = h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=s.grant.grant_id))
    s = h.cur()
    assert s.lifecycle == Lifecycle.ACTIVE and not s.fences and s.grant.ready
    assert EffectKind.ENABLE_ISSUANCE in kinds(r) and EffectKind.SEND_MESSAGE in kinds(r)


def test_D10_continue_during_checkpoint_drain_resumes_once_the_tree_stops():
    """#627: grace expired with a busy tree (DRAINING, fence {checkpoint}); /continue was
    accepted, its PolicyReady arrived while still draining and was dropped, leaving the
    grant unready forever. The accepted grant must resume once the drain settles."""
    h = Harness()
    b = checkpointed(h)
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    busy = ev.TreeQuiescent(session_id=b.session_id, complete=True, busy=True)
    assert EffectKind.INTERRUPT_TREE in kinds(h.send(P, busy))
    assert h.cur().lifecycle == Lifecycle.DRAINING
    r = h.send(P, ev.Continue(duration_us=2 * MICROS_PER_HOUR))
    assert r.audit.accepted
    g = h.cur().grant
    assert not g.ready and kinds(r)[0] == EffectKind.REPLACE_COST_POLICY
    # The policy lands before the tree stops: nothing resumes yet, nothing is lost.
    r = h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=g.grant_id))
    assert not work(r) and h.cur().lifecycle == Lifecycle.DRAINING
    again = h.send(P, ev.Continue())
    assert not again.audit.accepted and h.cur().grant == g  # no second grant
    # The drain settles: the pending grant's policy is re-verified (a read), then resumes.
    r = h.quiesce(P, b.session_id)
    s = h.cur()
    assert s.lifecycle == Lifecycle.FENCED and not s.grant.ready
    assert len(Harness.of(r, EffectKind.VERIFY_POLICIES)) == 1
    assert not Harness.of(r, EffectKind.REPLACE_COST_POLICY)
    r = h.send(P, ev.PoliciesVerified(session_id=b.session_id, ok=True))
    s = h.cur()
    assert s.lifecycle == Lifecycle.ACTIVE and not s.fences and s.grant == replace(g, ready=True)
    assert EffectKind.ENABLE_ISSUANCE in kinds(r)
    assert len(Harness.of(r, EffectKind.SEND_MESSAGE)) == 1
    assert h.p().bot == BotState.WORKING


def test_D10_pending_grant_waits_for_its_policy_after_the_drain():
    """Drain settles before the replacement policy lands: verification fails quietly and
    the policy's own PolicyReady completes the resume."""
    h = Harness()
    b = checkpointed(h)
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, ev.Continue())
    g = h.cur().grant
    h.quiesce(P, b.session_id)
    r = h.send(P, ev.PoliciesVerified(session_id=b.session_id, ok=False))
    assert not work(r) and Hold.PREPARE_FAILED not in h.p().holds
    assert h.cur().lifecycle == Lifecycle.FENCED and not h.cur().grant.ready
    r = h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=g.grant_id))
    assert h.cur().lifecycle == Lifecycle.ACTIVE and EffectKind.SEND_MESSAGE in kinds(r)


def test_D10_repeat_continue_retries_a_half_prepared_grant_without_a_new_one():
    h = Harness()
    b = checkpointed(h)
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, ev.Continue())
    g = h.cur().grant
    h.quiesce(P, b.session_id)
    h.send(P, ev.PoliciesVerified(session_id=b.session_id, ok=False))  # lost/early read
    r = h.send(P, ev.Continue())
    assert r.audit.accepted and h.cur().grant == g
    assert EffectKind.VERIFY_POLICIES in kinds(r)
    assert not Harness.of(r, EffectKind.REPLACE_COST_POLICY) and not work(r)
    r = h.send(P, ev.PoliciesVerified(session_id=b.session_id, ok=True))
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.cur().grant == replace(g, ready=True)
    assert len(Harness.of(r, EffectKind.SEND_MESSAGE)) == 1


def test_D10_continue_resolves_surviving_cost_prompt():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.ElicitationOpened(session_id=b.session_id, elicitation_id="ask", cost_ask=True))
    h.send(P, ev.Continue())
    g = h.cur().grant.grant_id
    r = h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=g))
    [res] = Harness.of(r, EffectKind.RESOLVE_ELICITATION)
    assert res.args["elicitation_id"] == "ask" and EffectKind.SEND_MESSAGE not in kinds(r)


@pytest.mark.parametrize("hard", [ev.Stop(), ev.AssignedHuman()])
def test_D11_continue_cannot_clear_hard_fence(hard):
    h = Harness()
    b = checkpointed(h)
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, hard, actor=OWNER_ID if isinstance(hard, ev.Stop) else OTHER_USER_ID)
    h.quiesce(P, b.session_id)
    h.send(P, ev.GitHubSnapshot(), evidence=snapshot())
    r = h.send(P, ev.Continue())
    assert not r.audit.accepted and not work(r)
    assert FenceKind.CHECKPOINT in h.p().session(b.session_id).fences


def test_D12_one_crash_replacement_then_blocked():
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        ev.ActiveTimeSample(
            session_id=b.session_id, grant_id=b.grant.grant_id, consumed_us=MICROS_PER_HOUR
        ),
    )
    h.send(P, ev.SessionCrashed(session_id=b.session_id))
    r = h.quiesce(P, b.session_id)
    s2 = h.cur()
    assert s2.session_id != b.session_id and s2.attempt == 2 and s2.restart_count == 1
    assert s2.grant.duration_us == 3 * MICROS_PER_HOUR  # remaining time carried
    assert EffectKind.CREATE_SESSION in kinds(r) and h.admission.building_count == 1
    s2 = h.create_ok()
    h.send(P, ev.SessionCrashed(session_id=s2.session_id))
    assert Hold.RESTART_EXHAUSTED in h.p().holds
    r = h.quiesce(P, s2.session_id)
    assert EffectKind.CREATE_SESSION not in kinds(r) and h.p().bot == BotState.BLOCKED


def test_D13_timer_or_duplicate_continue_adds_no_grant():
    h = Harness()
    checkpointed(h)
    cont = h.f().make(ev.Continue())
    h.apply(cont)
    g = h.cur().grant
    assert h.apply(cont).duplicate and h.cur().grant == g
    assert not h.send(P, ev.RetryDue(effect_id="x")).effects


# ================================================================== table E


def test_E01_E02_build_ready_to_ready():
    h = Harness()
    b = h.to_building()
    r = h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha=HEAD,
        ),
    )
    assert h.cur().wait_reason.value == "checks" and EffectKind.FETCH_PR_EVIDENCE in kinds(r)
    h.send(P, ev.PRObserved(pr_number=7, head_sha=HEAD, bot_authored=True, parcel_branch=True))
    h.send(
        P, ev.ReadinessEvidence(session_id=b.session_id, pr_number=7, head_sha=HEAD, verified=True)
    )
    assert h.p().stage == Stage.BUILDING  # not before Q
    h.quiesce(P, b.session_id)
    p = h.p()
    assert p.stage == Stage.READY and p.session(b.session_id).lifecycle == Lifecycle.RETIRED
    assert h.admission.building_count == 0 and h.admission.prospective_pr_count == 1
    assert p.bot == BotState.IDLE


def test_E03_checks_webhook_is_a_hint_for_a_fresh_read_no_repair_loop():
    """One failed suite is not the aggregate: it re-reads the PR; a failed read while the
    build tree is still busy waits (no hold, no work) until the build is idle."""
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha=HEAD,
        ),
    )
    r = h.send(P, ev.ChecksChanged(pr_number=7, head_sha=HEAD, state=ev.ChecksState.FAILED))
    assert kinds(r).count(EffectKind.FETCH_PR_EVIDENCE) == 1 and not work(r)
    assert Hold.CHECKS_FAILED not in h.p().holds
    r = h.send(
        P,
        ev.ReadinessEvidence(
            session_id=b.session_id,
            pr_number=7,
            head_sha=HEAD,
            checks=ev.ChecksState.FAILED,
        ),
    )
    assert not work(r) and not h.p().holds & {Hold.CHECKS_FAILED, Hold.READINESS_FAILED}


def test_E04_remediation_exhausted_blocks_ready():
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha=HEAD,
        ),
    )
    h.send(
        P,
        ev.ReadinessEvidence(
            session_id=b.session_id,
            pr_number=7,
            head_sha=HEAD,
            verified=True,
            remediation_exhausted=True,
        ),
    )
    h.quiesce(P, b.session_id)
    assert h.p().stage == Stage.BUILDING and h.p().bot == BotState.NEEDS_YOU


def test_E05_owner_ready_to_building_is_rework_under_the_same_approval():
    h = Harness()
    h.to_building()
    h.build_ready()
    approval = h.p().current_approval_id
    r = h.send(P, ev.LeftwardMove(from_stage=Stage.READY, to_stage=Stage.BUILDING), actor=OWNER_ID)
    assert r.audit.accepted and Hold.UNSUPPORTED_REWORK not in h.p().holds
    assert h.p().stage == Stage.BUILDING and h.p().current_approval_id == approval
    assert h.admission.queue_entry(P) is not None  # admitted as a build (capacity decides)
    # A second control while the rework is queued adds nothing.
    r = h.send(P, ev.RequestRework())
    assert not r.audit.accepted and not work(r)


@pytest.mark.parametrize(
    "body",
    [
        ev.ReviewChanged(pr_number=7, head_sha=HEAD, changes_requested=True),
        ev.ChecksChanged(pr_number=7, head_sha=HEAD, state=ev.ChecksState.FAILED),
        # A new head is re-evaluated in Ready, not rework (tests/test_readiness_repairs.py).
    ],
)
def test_E06_ready_invalidated_by_observation_without_new_episode(body):
    h = Harness()
    h.to_building()
    h.build_ready()
    r = h.send(P, body)
    if not isinstance(body, ev.ReviewChanged):
        # Check/PR webhooks are hints: a fresh read decides (and supplies the real head).
        assert h.p().stage == Stage.READY and not work(r)
        [fetch] = [e for e in r.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE]
        r = h.send(
            P,
            ev.ReadinessEvidence(
                session_id=str(fetch.args["session_id"]),
                pr_number=7,
                head_sha=HEAD,
                observed_head_sha=body.head_sha,
                checks=getattr(body, "state", ev.ChecksState.GREEN),
            ),
        )
    p = h.p()
    assert not work(r) and EffectKind.CREATE_SESSION not in kinds(r)
    if isinstance(body, ev.ChecksChanged):
        # A red check alone: the bot's work is done, so Ready stays, Bot Blocked (#461).
        assert p.stage == Stage.READY and p.bot == BotState.BLOCKED
        assert Hold.REWORK_CONTROL_REQUIRED not in p.holds
        return
    assert p.stage == Stage.BUILDING and Hold.REWORK_CONTROL_REQUIRED in p.holds


def test_E07_pr_closed_unmerged():
    h = Harness()
    h.to_building()
    h.build_ready()
    h.send(
        P,
        ev.PRObserved(
            pr_number=7, head_sha=HEAD, open=False, bot_authored=True, parcel_branch=True
        ),
    )
    assert Hold.PR_CLOSED in h.p().holds and h.admission.prospective_pr_count == 0


def test_E08_merge_and_close_release_everything():
    h = Harness()
    h.to_building()
    h.build_ready()
    h.send(
        P,
        ev.PRObserved(
            pr_number=7,
            head_sha=HEAD,
            open=False,
            merged=True,
            bot_authored=True,
            parcel_branch=True,
        ),
    )
    r = h.send(P, ev.Closed(), actor=OWNER_ID)
    assert h.admission.prospective_pr_count == 0 and not h.p().eligible and not work(r)


# ================================================================ totality


def _sample_bodies(p: Parcel):
    s = p.current_session
    sid = s.session_id if s else "missing"
    gid = s.grant.grant_id if s else "missing"
    root = (s.root_id or "r") if s else "r"
    nonce = s.nonce if s else "n"
    return {
        EventKind.REQUEST_TRIAGE: ev.RequestTriage(),
        EventKind.REQUEST_PLAN: ev.RequestPlan(),
        EventKind.REQUEST_REPLAN: ev.RequestReplan(),
        EventKind.PLAN_FEEDBACK: ev.PlanFeedback(text_digest="t"),
        EventKind.APPROVE_PLAN: ev.ApprovePlan(),
        EventKind.WAIVE_PLAN: ev.WaivePlan(),
        EventKind.DECIDE: ev.Decide(answer="a"),
        EventKind.CONTINUE: ev.Continue(),
        EventKind.STOP: ev.Stop(),
        EventKind.REQUEST_REWORK: ev.RequestRework(),
        EventKind.PAUSE: ev.Pause(),
        EventKind.UNPAUSE: ev.Unpause(),
        EventKind.LEFTWARD_MOVE: ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.INBOX),
        EventKind.ASSIGNED_HUMAN: ev.AssignedHuman(),
        EventKind.CLOSED: ev.Closed(),
        EventKind.TRANSFERRED: ev.Transferred(),
        EventKind.DELETED: ev.Deleted(),
        EventKind.ITEM_REMOVED: ev.ItemRemoved(),
        EventKind.APPROVAL_INVALIDATED: ev.ApprovalInvalidated(
            approval_id=p.current_approval_id or "x"
        ),
        EventKind.WAIVER_EDITED: ev.WaiverEdited(),
        EventKind.CONTRACT_TAMPERED: ev.ContractTampered(contract_id=p.current_contract_id or "x"),
        EventKind.GITHUB_SNAPSHOT: ev.GitHubSnapshot(),
        EventKind.COLUMN_OBSERVED: ev.ColumnObserved(stage=Stage.READY),
        EventKind.PR_OBSERVED: ev.PRObserved(
            pr_number=7, head_sha=HEAD, bot_authored=True, parcel_branch=True
        ),
        EventKind.CHECKS_CHANGED: ev.ChecksChanged(pr_number=7, head_sha=HEAD),
        EventKind.REVIEW_CHANGED: ev.ReviewChanged(pr_number=7, head_sha=HEAD),
        EventKind.READINESS_EVIDENCE: ev.ReadinessEvidence(
            session_id=sid, pr_number=7, head_sha=HEAD, verified=True
        ),
        EventKind.CONTRACT_PUBLISHED: ev.ContractPublished(contract_id="x", verified=True),
        EventKind.SESSION_CREATED: ev.SessionCreated(session_id=sid, root_id="new", nonce=nonce),
        EventKind.CREATE_REJECTED: ev.CreateRejected(session_id=sid),
        EventKind.ADOPTION_RESULT: ev.AdoptionResult(
            session_id=sid, matches=1, root_id="z", nonce=nonce
        ),
        EventKind.EFFECT_UNKNOWN: ev.EffectUnknown(
            effect_id="e", effect_kind="send_message", session_id=sid
        ),
        EventKind.EFFECT_CANCELLED: ev.EffectCancelled(
            effect_id="e", effect_kind="create_session", session_id=sid
        ),
        EventKind.PREPARED: ev.Prepared(session_id=sid, ok=True),
        EventKind.MESSAGE_ACK: ev.MessageAck(session_id=sid, effect_id="e", item_id="i"),
        EventKind.EFFECT_RECONCILED: ev.EffectReconciled(effect_id="e", session_id=sid),
        EventKind.RUNTIME_ACTIVITY: ev.RuntimeActivity(session_id=sid, busy=True),
        EventKind.OWNER_DIRECT_MESSAGE: ev.OwnerDirectOmnigentMessage(session_id=sid),
        EventKind.ELICITATION_OPENED: ev.ElicitationOpened(session_id=sid, elicitation_id="q"),
        EventKind.ELICITATION_RESOLVED: ev.ElicitationResolved(session_id=sid, elicitation_id="q"),
        EventKind.ELICITATION_GONE: ev.ElicitationGone(session_id=sid, elicitation_id="q"),
        EventKind.RESULT_CANDIDATE: result_candidate(
            sid, root, p.revision, ev.ResultKind.BUILD_READY, pr_number=7, head_sha=HEAD
        ),
        EventKind.TREE_QUIESCENT: ev.TreeQuiescent(session_id=sid, complete=True, busy=False),
        EventKind.STOP_TIMEOUT: ev.StopTimeout(session_id=sid),
        EventKind.SESSION_CRASHED: ev.SessionCrashed(session_id=sid),
        EventKind.ACTIVE_TIME_SAMPLE: ev.ActiveTimeSample(
            session_id=sid, grant_id=gid, consumed_us=10
        ),
        EventKind.COST_SAMPLE: ev.CostSample(session_id=sid),
        EventKind.POLICY_READY: ev.PolicyReady(session_id=sid, grant_id=gid),
        EventKind.ACTIVE_LIMIT_REACHED: ev.ActiveLimitReached(session_id=sid, grant_id=gid),
        EventKind.GRACE_EXPIRED: ev.GraceExpired(session_id=sid, grant_id=gid),
        EventKind.CAPACITY_AVAILABLE: ev.CapacityAvailable(),
        EventKind.RETRY_DUE: ev.RetryDue(effect_id="e"),
        EventKind.RECONCILE_DUE: ev.ReconcileDue(),
    }


def _scenarios():
    def fresh(h):
        h.eligible()

    def triage_active(h):
        h.eligible()
        h.send(P, ev.RequestTriage())
        h.create_ok()

    def plan_waiting(h):
        h.plan_published()

    def queued(h):
        h.plan_published()
        h.approve()

    def building(h):
        h.to_building()

    def checkpoint(h):
        checkpointed(h)

    def fenced(h):
        b = h.to_building()
        h.send(P, ev.Stop())
        h.quiesce(P, b.session_id)

    def ready(h):
        h.to_building()
        h.build_ready()

    return [fresh, triage_active, plan_waiting, queued, building, checkpoint, fenced, ready]


def test_every_event_kind_has_a_handler_and_body():
    assert set(HANDLERS) == set(EventKind) == set(BODY_TYPES)


@pytest.mark.parametrize("scenario", _scenarios(), ids=lambda f: f.__name__)
def test_totality_every_state_and_event_kind(scenario):
    base = Harness()
    scenario(base)
    for kind, body in _sample_bodies(base.p()).items():
        h = Harness(
            cfg=base.cfg,
            admission=base.admission,
            parcels=dict(base.parcels),
            factories={P: base.f()},
        )
        before = h.p()
        result = h.apply(h.f().make(body))  # must never raise
        assert result.audit.kind == kind.value
        if not result.audit.accepted:
            after = h.p()
            assert not work(result), (kind, result.audit.reason)
            assert {e.kind for e in result.effects} <= {
                EffectKind.POST_COMMENT,
                EffectKind.MOVE_CARD,
                EffectKind.SET_BOT,
                EffectKind.SET_NOTE,
                EffectKind.REACT_COMMENT,
            }
            assert after.sessions == before.sessions and after.approvals == before.approvals


def test_B07f_owner_plan_command_resumes_after_refused_prepare():
    """Resume path for a Scoped parcel Blocked by prepare_failed: an owner ``/plan``."""
    h = Harness()
    h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="root-x", nonce=s.nonce))
    h.send(P, ev.Prepared(session_id=s.session_id, ok=False))
    h.quiesce(P, s.session_id)
    assert h.p().bot == BotState.BLOCKED and Hold.PREPARE_FAILED in h.p().holds
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    p = h.p()
    assert r.audit.accepted and Hold.PREPARE_FAILED not in p.holds
    assert starts_run(r)
    assert p.current_session.kind == SessionKind.PLAN
    assert [x.kind for x in p.sessions].count(SessionKind.TRIAGE) == 1  # triage not re-run


def test_B07g_owner_plan_command_resumes_after_result_invalid():
    """Resume path when a plan result is invalid: ``/plan``."""
    h = Harness()
    h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    s = h.create_ok()
    bad = result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.PLAN)
    first = h.send(P, replace(bad, valid=False))
    assert not Harness.of(first, EffectKind.SEND_MESSAGE)
    assert Hold.RESULT_INVALID in h.p().holds and h.p().bot == BotState.BLOCKED
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert r.audit.accepted, r.audit.reason
    assert Hold.RESULT_INVALID not in h.p().holds
    assert EffectKind.INTERRUPT_TREE in kinds(r)  # the stale plan tree is drained first
    after = h.quiesce(P, s.session_id)
    p = h.p()
    assert starts_run(after)
    assert p.current_session.session_id != s.session_id
    assert p.current_session.kind == SessionKind.PLAN
    assert [x.kind for x in p.sessions].count(SessionKind.TRIAGE) == 1


def test_D12_reconcile_closes_legacy_stale_decisions_and_resumes_the_build():
    """#677/#462 recovery: records left open (prompt already gone) self-heal on reconcile."""
    h = Harness()
    h.to_building()
    _, d = open_decision(h)
    p = h.p()
    legacy = replace(p.decision(d.decision_id), prompt_lost=True)  # pre-fix Gone handling
    h.parcels[P] = replace(
        p, decisions=tuple(legacy if x.decision_id == d.decision_id else x for x in p.decisions)
    )
    assert h.p().bot == BotState.NEEDS_YOU and h.cur().lifecycle == Lifecycle.WAITING
    r = h.send(P, ev.ReconcileDue())
    p = h.p()
    assert p.decision(d.decision_id).status == DecisionStatus.RESOLVED_IN_OMNIGENT
    assert p.bot == BotState.WORKING and h.cur().lifecycle == Lifecycle.ACTIVE
    [bot] = Harness.of(r, EffectKind.SET_BOT)
    assert bot.args == {"bot": "Working"}


def test_D13_bot_is_written_only_when_the_board_differs():
    """No churn: reconcile writes Bot only when a fresh read shows a different value."""
    h = Harness()
    h.to_building()
    assert not Harness.of(h.send(P, ev.ReconcileDue()), EffectKind.SET_BOT)
    f = h.f()
    same = h.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(read_at_us=f.now, bot="Working", stage=Stage.BUILDING),
        )
    )
    assert not Harness.of(same, EffectKind.SET_BOT)
    drift = h.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(read_at_us=f.now, bot="Needs you", stage=Stage.BUILDING),
        )
    )
    [bot] = Harness.of(drift, EffectKind.SET_BOT)  # the owner (or GitHub) changed it
    assert bot.args == {"bot": "Working"}


def test_D14_finished_session_orphans_its_open_decisions():
    h = Harness()
    s = h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    stale = [d for d in h.p().decisions if d.session_id == s.session_id]
    assert all(d.status != DecisionStatus.OPEN for d in stale)
    h2 = Harness()
    h2.eligible()
    h2.send(P, ev.RequestTriage())
    t = h2.create_ok()
    _, d = open_decision(h2)
    h2.send(P, ev.Stop())
    h2.quiesce(P, t.session_id)
    assert h2.p().decision(d.decision_id).status == DecisionStatus.ORPHANED
    assert not h2.p().open_decisions


def test_C02b_refused_waiver_drag_rolls_the_card_back_and_explains_next_step():
    """#462: WaivePlan refused for open decisions left the card in Building, idle."""
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    h.create_ok()
    _, d = open_decision(h)
    assert h.p().stage == Stage.TRIAGED
    f = h.f()
    r = h.apply(
        f.make(
            ev.WaivePlan(via=Via.DRAG),
            evidence=snapshot(read_at_us=f.now, stage=Stage.BUILDING),
        )
    )
    assert not r.audit.accepted and r.audit.reason == "open-decisions"
    assert not Harness.of(r, EffectKind.POST_COMMENT)
    assert h.p().note == "Command refused: open-decisions; card moved back to Triaged"
    [note] = Harness.of(r, EffectKind.SET_NOTE)
    assert note.args == {"note": h.p().note}
    assert d.decision_id in {x.decision_id for x in h.p().open_decisions}
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    assert move.args == {"to": "Triaged", "expected_from": "Building"}


def test_D15_677_gate_reopens_when_the_prompt_is_answered_in_omnigent():
    """#677: elicitation opened → answered in Omnigent (Gone) → token gate must allow."""
    from omnigent_factory.core.predicates import work_allowed

    h = Harness()
    b = h.to_building()
    assert work_allowed(h.p(), h.cur())
    _, d = open_decision(h, impact=DecisionImpact.UNKNOWN)
    assert not work_allowed(h.p(), h.cur())  # an open question pauses the build
    h.send(P, ev.ElicitationGone(session_id=b.session_id, elicitation_id=d.elicitation_id))
    p, s = h.p(), h.cur()
    assert not p.open_decisions and s.lifecycle == Lifecycle.ACTIVE
    assert work_allowed(p, s)  # StoreExecutionGate.token_gate allows exactly when this holds


def _stuck_677(h):
    """Build with a pre-fix stale decision and a result_invalid block (live #677 shape)."""
    b = h.to_building()
    _, d = open_decision(h, impact=DecisionImpact.UNKNOWN)
    p = h.p()
    legacy = replace(p.decision(d.decision_id), prompt_lost=True)
    h.parcels[P] = replace(
        p,
        decisions=tuple(legacy if x.decision_id == d.decision_id else x for x in p.decisions),
        holds=p.holds | {Hold.RESULT_INVALID},
    )
    return b, d


def test_D16_operator_resume_reopens_the_same_session_and_relays_one_note():
    from omnigent_factory.core.predicates import work_allowed

    h = Harness()
    b, d = _stuck_677(h)
    assert not work_allowed(h.p(), h.cur())
    f = h.f()
    r = h.apply(
        f.make(
            ev.OperatorResume(text="Publish the staged candidate."), provenance=Provenance.OPERATOR
        )
    )
    assert r.audit.accepted, r.audit.reason
    p, s = h.p(), h.cur()
    assert s.session_id == b.session_id and not Harness.of(r, EffectKind.CREATE_SESSION)
    assert p.decision(d.decision_id).status == DecisionStatus.RESOLVED_IN_OMNIGENT
    assert Hold.RESULT_INVALID not in p.holds and work_allowed(p, s)
    [msg] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert msg.args == {"purpose": "operator_note", "text": "Publish the staged candidate."}
    assert p.bot == BotState.WORKING


def test_D16b_operator_resume_adds_no_authority():
    h = Harness()
    h.to_building()
    f = h.f()
    owner = h.apply(f.make(ev.OperatorResume(text="x"), provenance=Provenance.WEBHOOK))
    assert not owner.audit.accepted  # operator provenance only
    h.send(P, ev.Stop())
    stopped = h.apply(f.make(ev.OperatorResume(text="x"), provenance=Provenance.OPERATOR))
    assert not stopped.audit.accepted and not Harness.of(stopped, EffectKind.SEND_MESSAGE)


def test_D17_blocked_result_is_an_honest_blocked_state_not_a_schema_error():
    h = Harness()
    b = h.to_building()
    r = h.send(P, result_candidate(b.session_id, b.root_id, b.revision, ev.ResultKind.BLOCKED))
    assert r.audit.accepted
    p = h.p()
    assert Hold.AGENT_BLOCKED in p.holds and Hold.RESULT_INVALID not in p.holds
    assert p.bot == BotState.BLOCKED
    [report] = Harness.of(r, EffectKind.PUBLISH_REPORT)
    assert report.args == {"report": "blocked", "session_id": b.session_id}
