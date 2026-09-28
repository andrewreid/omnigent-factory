"""Regression traces for review checkpoint-4e322522-r1 findings F1-F4.

These use only the event API that existed at the reviewed tree, so they can be run
against it to prove each one fails on the old behaviour.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import (
    ApprovalKind,
    DecisionImpact,
    FenceKind,
    Lifecycle,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, snapshot
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def kinds(r):
    return [e.kind for e in r.effects]


def work(r):
    return [e for e in r.effects if e.kind in WORK_BEARING_KINDS]


def unknown_message(h, session_id, effect_id=None, kind="send_message"):
    if effect_id is None:  # the latest work-bearing effect actually issued to the session
        issued = [e for e, sid in getattr(h.p(), "sent_effects", ()) if sid == session_id]
        effect_id = issued[-1] if issued else "lost-msg"
    return h.send(P, ev.EffectUnknown(effect_id=effect_id, effect_kind=kind, session_id=session_id))


# ------------------------------------------------------------------------ F1


def test_F1_feedback_sends_no_message_while_message_outcome_unknown():
    h = Harness()
    plan = h.plan_published()
    unknown_message(h, plan.session_id)
    r = h.send(P, ev.PlanFeedback(text_digest="more"))
    assert r.audit.accepted and h.p().revision_pending  # recorded
    assert not work(r)  # but no overlapping turn


def test_F1_no_ready_while_message_outcome_unknown():
    h = Harness()
    b = h.to_building()
    unknown_message(h, b.session_id)
    h.build_ready()
    assert h.p().stage == Stage.BUILDING and not h.p().readiness.ready
    assert h.cur().lifecycle != Lifecycle.RETIRED


def test_F1_no_ready_while_any_effect_outcome_unknown():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.EffectUnknown(effect_id="c1", effect_kind="post_comment"))
    h.build_ready()
    assert h.p().stage == Stage.BUILDING
    assert b.session_id == h.cur().session_id


def test_F1_ack_for_another_effect_does_not_clear_ambiguity():
    h = Harness()
    plan = h.plan_published()
    unknown_message(h, plan.session_id)
    h.send(P, ev.MessageAck(session_id=plan.session_id, effect_id="other", item_id="x"))
    assert h.cur().message_unknown
    r = h.send(P, ev.PlanFeedback(text_digest="more"))
    assert not work(r)


def test_F1_ack_with_wrong_session_is_rejected():
    h = Harness()
    plan = h.plan_published()
    unknown_message(h, plan.session_id)
    [lost] = [u.effect_id for u in h.p().unknown_effects]
    r = h.send(P, ev.MessageAck(session_id="someone-else", effect_id=lost, item_id="x"))
    assert not r.audit.accepted and h.cur().message_unknown


def test_F1_exact_ack_restores_work():
    h = Harness()
    plan = h.plan_published()
    unknown_message(h, plan.session_id)
    [lost] = [u.effect_id for u in h.p().unknown_effects]
    h.send(P, ev.MessageAck(session_id=plan.session_id, effect_id=lost, item_id="i1"))
    assert not h.cur().message_unknown and "i1" in h.cur().own_items
    r = h.send(P, ev.PlanFeedback(text_digest="more"))
    assert [e.kind for e in work(r)] == [EffectKind.SEND_MESSAGE]


def test_F1_unknown_elicitation_resolve_blocks_answer_relay():
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        ev.ElicitationOpened(
            session_id=b.session_id, elicitation_id="q1", impact=DecisionImpact.WITHIN_CONTRACT
        ),
    )
    unknown_message(h, b.session_id, "lost-resolve", "resolve_elicitation")
    [d] = h.p().open_decisions
    r = h.send(P, ev.Decide(answer="yes", within_contract=True, decision_id=d.decision_id))
    assert r.audit.accepted and not work(r)  # answer recorded, relay withheld


def test_F1_no_successor_tree_while_ambiguous():
    h = Harness()
    b = h.to_building()
    unknown_message(h, b.session_id)
    h.send(P, ev.Stop())
    r = h.quiesce(P, b.session_id)  # tree idle, but the lost message may be queued
    h.send(P, ev.RequestTriage())
    assert EffectKind.CREATE_SESSION not in kinds(r)
    assert h.cur().session_id == b.session_id


def test_F1_no_continuation_while_ambiguous():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    unknown_message(h, b.session_id)
    h.send(P, ev.Continue())
    g = h.cur().grant.grant_id
    r = h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=g))
    assert not work(r)


def test_F1_checkpoint_cleanup_precondition_respects_ambiguity():
    from omnigent_factory.core.preconditions import effect_still_valid

    h = Harness()
    b = h.to_building()
    r = h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    [cleanup] = [e for e in r.effects if e.kind == EffectKind.SEND_MESSAGE]
    assert effect_still_valid(h.p(), cleanup) is None
    unknown_message(h, b.session_id)
    assert effect_still_valid(h.p(), cleanup) is not None


# ------------------------------------------------------------------------ F2


def snap_event(h, stage, **kw):
    return h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(stage=stage, **kw),
        provenance=Provenance.RECONCILER,
    )


def test_F2_reconciled_leftward_snapshot_fences_active_build():
    h = Harness()
    b = h.to_building()
    approval = h.p().current_approval_id
    r = snap_event(h, Stage.SCOPED)
    p = h.p()
    s = p.session(b.session_id)
    assert {FenceKind.SAFETY, FenceKind.REVOKED} <= s.fences
    assert p.stage == Stage.SCOPED and not p.approval(approval).valid and p.revision_pending
    assert p.pending_authorization_id is None  # no actor: negative half only
    assert not work(r)


def test_F2_reconciled_inbox_snapshot_fences_triage():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    t = h.create_ok()
    snap_event(h, Stage.INBOX)
    assert FenceKind.SAFETY in h.p().session(t.session_id).fences
    assert h.p().stage == Stage.INBOX


def test_F2_source_snapshot_during_pending_write_gates_until_trusted_outcome():
    # Superseded by review r2 F2: a source-column read during the write window is not
    # proof of staleness (it may be a human drag back), so it is never silently safe:
    # work stays gated until the executor reports the exact outcome.
    h = Harness(auto_ack_moves=False)
    h.plan_published()
    plan = h.cur()
    h.send(P, ev.ApprovePlan(via=Via.COMMAND))  # daemon writes Scoped -> Building
    h.quiesce(P, plan.session_id)
    [move] = h.p().pending_moves
    barrier = h.p().barrier_time_us
    snap_event(h, Stage.SCOPED)  # predates our write, or a human drag back
    assert h.p().barrier_time_us == barrier and h.p().pending_moves == (move,)
    assert not h.send(P, ev.CapacityAvailable()).audit.accepted  # gated
    snap_event(h, Stage.BUILDING)  # untrusted read of the target does not retire it
    assert h.p().pending_moves == (move,)
    # Trusted read-after-write of the exact target retires it; admission proceeds.
    h.send(P, ev.ColumnObserved(stage=Stage.BUILDING, daemon_effect_id=move.effect_id))
    assert not h.p().pending_moves and h.p().barrier_time_us == barrier
    snap_event(h, Stage.SCOPED)  # source observed after the trusted ack: real leftward move
    assert h.p().barrier_time_us > barrier and h.p().current_approval_id is None


def test_F2_column_observation_by_owner_is_negative_half_only():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.ColumnObserved(stage=Stage.SCOPED), actor=OWNER_ID)
    p = h.p()
    assert FenceKind.REVOKED in p.session(b.session_id).fences
    assert p.pending_authorization_id is None


def test_F2_snapshot_with_edited_text_voids_waiver():
    h = Harness()
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.DRAG), evidence=snapshot(title="T", body="B"))
    b = h.admit()
    snap_event(h, Stage.BUILDING, title="T edited", body="B")
    p = h.p()
    assert p.current_approval_id is None and FenceKind.REVOKED in p.session(b.session_id).fences


def test_F2_rightward_snapshot_is_observation_only():
    h = Harness()
    h.eligible()
    r = snap_event(h, Stage.BUILDING)
    assert h.p().stage == Stage.BUILDING and not work(r)
    assert EffectKind.CREATE_SESSION not in kinds(r)


# ------------------------------------------------------------------------ F3


def ready_parcel():
    h = Harness()
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY
    return h


def test_F3_ready_parcel_rejects_triage_plan_and_replan():
    for body in (ev.RequestTriage(), ev.RequestPlan(), ev.RequestReplan()):
        h = ready_parcel()
        r = h.send(P, body)
        assert not r.audit.accepted, body
        assert EffectKind.CREATE_SESSION not in kinds(r) and h.p().stage == Stage.READY


def test_F3_ready_parcel_stopped_still_cannot_rework():
    h = ready_parcel()
    h.send(P, ev.Stop())
    r = h.send(P, ev.RequestTriage())
    assert not r.audit.accepted and EffectKind.CREATE_SESSION not in kinds(r)


def test_F3_triage_rejected_outside_inbox_without_stop():
    h = Harness()
    h.plan_published()
    r = h.send(P, ev.RequestTriage())
    assert not r.audit.accepted and h.cur().kind == SessionKind.PLAN


def test_F3_stopped_stage_recovery_is_allowed():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Stop())
    h.quiesce(P, b.session_id)
    r = h.send(P, ev.RequestTriage(via=Via.DRAG))
    assert r.audit.accepted and EffectKind.PREPARE_SESSION in kinds(r)  # reused issue session


def test_F3_plan_from_building_is_the_replan_row():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.RequestPlan())
    assert {FenceKind.REVOKED, FenceKind.SAFETY} <= h.p().session(b.session_id).fences


# ------------------------------------------------------------------------ F4


def test_F4_waiver_edit_sets_revision_pending_and_blocks_fresh_waiver():
    h = Harness()
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.DRAG), evidence=snapshot(title="T", body="B"))
    h.send(P, ev.WaiverEdited(), actor=OTHER_USER_ID)
    assert h.p().revision_pending
    for via in (Via.LABEL, Via.DRAG):
        r = h.send(P, ev.WaivePlan(via=via), evidence=snapshot(title="T2", body="B"))
        assert not r.audit.accepted and h.p().current_approval_id is None


def test_F4_recovery_after_waiver_edit_via_published_plan():
    h = Harness()
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.DRAG), evidence=snapshot(title="T", body="B"))
    b = h.admit()
    h.send(P, ev.WaiverEdited(), actor=OTHER_USER_ID)
    h.quiesce(P, b.session_id)
    h.send(P, ev.RequestPlan())
    h.create_ok()
    h.publish_plan(goal="replanned")
    assert not h.p().revision_pending
    h.approve()
    assert h.p().current_approval.kind == ApprovalKind.PLAN
    assert h.send(P, ev.CapacityAvailable()).audit.accepted
    _ = HEAD
