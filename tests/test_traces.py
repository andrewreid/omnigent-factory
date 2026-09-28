"""Named model traces required by §2.8 plus explicit invariant examples (#5, #8, #11)."""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.accounting import ActivityInterval, union_active_us
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind
from omnigent_factory.core.types import (
    ApprovalKind,
    BotState,
    FenceKind,
    Hold,
    Lifecycle,
    QueueStatus,
    SessionKind,
    Size,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import (
    OTHER_USER_ID,
    OWNER_ID,
    config,
    result_candidate,
    snapshot,
)
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
Q = "I_parcel_2"


def work(result):
    return [e for e in result.effects if e.kind in WORK_BEARING_KINDS]


def test_invariant5_revoke_quiesce_replan_publish_approve_new_build():
    h = Harness()
    old = h.to_building()
    h.send(P, ev.RequestReplan())
    h.quiesce(P, old.session_id)
    plan = h.create_ok()
    assert plan.kind == SessionKind.PLAN
    h.publish_plan(goal="revised")
    h.approve()
    new = h.admit()
    p = h.p()
    assert new.kind == SessionKind.BUILD and new.session_id != old.session_id
    # One issue session across the revoked build, the replan and the new build.
    assert new.root_id == old.root_id and new.lifecycle == Lifecycle.ACTIVE
    # No old build fence was removed.
    assert p.session(old.session_id).fences >= {FenceKind.SAFETY, FenceKind.REVOKED}
    assert p.session(old.session_id).lifecycle == Lifecycle.RETIRED


def test_invariant8_waiver_edit_then_revert_never_restores():
    h = Harness()
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.DRAG), evidence=snapshot(title="T", body="B"))
    waiver = h.p().current_approval
    assert waiver.kind == ApprovalKind.SKIP
    edit = h.f().make(ev.WaiverEdited())
    h.apply(edit)
    h.send(P, ev.WaiverEdited())  # revert to the original text
    h.apply(edit)  # duplicate delivery
    p = h.p()
    assert not p.approval(waiver.approval_id).valid and p.current_approval_id is None
    assert h.admission.queue_entry(P).status == QueueStatus.CANCELLED
    assert h.send(P, ev.CapacityAvailable()).audit.reason == "not-queued"


def test_control_before_and_after_safety():
    h = Harness()
    h.eligible()
    t_assign = h.f().tick()
    h.apply(h.f().make(ev.AssignedHuman(), actor=OTHER_USER_ID, time_us=t_assign))
    early = h.f().make(ev.RequestTriage(), time_us=t_assign - 1)  # delayed older control
    assert not h.apply(early).audit.accepted
    h.send(P, ev.GitHubSnapshot(), evidence=snapshot())
    later = h.send(P, ev.RequestTriage(via=Via.DRAG))
    assert later.audit.accepted and EffectKind.CREATE_SESSION in [e.kind for e in later.effects]


def test_feedback_during_publication_admission_and_first_message():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    h.send(P, ev.PlanFeedback(text_digest="during-create"))  # before first message
    assert h.cur().revision == h.p().revision
    h.create_ok()
    h.publish_plan()
    h.approve()
    h.send(P, ev.PlanFeedback(text_digest="late"), evidence=snapshot())
    # Stage is Building: plain feedback is not a Scoped revision; queue unaffected.
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED


def test_stop_during_create_then_late_ack_drains():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    h.send(P, ev.Stop())
    assert h.cur().lifecycle == Lifecycle.DRAINING
    r = h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="late", nonce=s.nonce))
    assert {e.kind for e in r.effects} >= {EffectKind.INTERRUPT_TREE, EffectKind.SCAN_TREE}
    assert not work(r) and EffectKind.PREPARE_SESSION not in [e.kind for e in r.effects]
    h.quiesce(P, s.session_id)
    assert h.cur().lifecycle == Lifecycle.FENCED


def test_stop_before_create_claim_cancels_cleanly():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage())
    s = h.cur()
    h.send(P, ev.Stop())
    h.send(
        P, ev.EffectCancelled(effect_id="x", effect_kind="create_session", session_id=s.session_id)
    )
    assert h.cur().lifecycle == Lifecycle.FENCED and h.p().bot == BotState.IDLE


def test_two_concurrent_approvals_at_cap_one():
    h = Harness(cfg=config(max_building=1))
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(P)
    h.approve(Q)
    a = h.send(P, ev.CapacityAvailable())
    b = h.send(Q, ev.CapacityAvailable())
    assert a.audit.accepted and not b.audit.accepted
    assert h.admission.building_count == 1
    h.create_ok(P)
    h.build_ready(P)
    assert h.admission.building_count == 0
    assert h.send(Q, ev.CapacityAvailable()).audit.accepted


def test_checkpoint_plus_assignment_stays_closed():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, ev.AssignedHuman(), actor=OTHER_USER_ID)
    s = h.p().session(b.session_id)
    assert FenceKind.SAFETY in s.fences and s.lifecycle == Lifecycle.DRAINING
    h.quiesce(P, b.session_id)
    h.send(P, ev.GitHubSnapshot(), evidence=snapshot())
    assert not h.send(P, ev.Continue()).audit.accepted
    assert h.admission.building_count == 0


def test_deleted_native_prompt_is_mirrored_as_orphan_or_relayed():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Stop())
    r = h.send(P, ev.ElicitationOpened(session_id=b.session_id, elicitation_id="late-q"))
    assert h.p().decisions[-1].status.value == "orphaned" and not work(r)
    assert h.p().bot != BotState.NEEDS_YOU or Hold.STOPPED in h.p().holds


def test_delayed_result_from_old_attempt_is_ignored():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.SessionCrashed(session_id=b.session_id))
    h.quiesce(P, b.session_id)
    h.create_ok()
    r = h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha="a" * 40,
            size=Size.M,
        ),
    )
    assert r.audit.reason == "stale-session" and h.p().readiness is None


def test_owner_label_waiver_from_scoped_is_explicit():
    h = Harness()
    h.plan_published()
    r = h.send(P, ev.WaivePlan(via=Via.LABEL), actor=OWNER_ID)
    assert r.audit.accepted and h.p().current_approval.kind == ApprovalKind.SKIP
    assert h.p().stage == Stage.BUILDING


def test_invariant11_union_time_not_summed():
    # Root idle while a child runs still counts; parallel children count union time.
    intervals = [
        ActivityInterval("root", 0, 10, running=True),
        ActivityInterval("root", 10, 100, running=False),  # parked on owner prompt
        ActivityInterval("child-a", 20, 60),
        ActivityInterval("child-b", 40, 80),
        ActivityInterval("child-c", 90, None),
    ]
    assert union_active_us(intervals, now_us=95) == 10 + 60 + 5
    # Owner waits alone consume zero.
    assert union_active_us([ActivityInterval("root", 0, 50, running=False)], now_us=60) == 0
    assert union_active_us([], now_us=10) == 0
