"""Owner steering through plain issue comments, per column (reducer rows).

A plain owner comment is recorded in every column; Triaged re-runs triage in the issue
session, Scoped revises the plan, Building relays it to an idle build within its
approval, and nothing starts elsewhere. A run executing a turn gets no extra message (the
MCP submit gate makes it read every comment first), so a burst never queues several runs.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind, MessagePurpose
from omnigent_factory.core.types import (
    FenceKind,
    Hold,
    Lifecycle,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import result_candidate, snapshot
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def kinds(result):
    return [e.kind for e in result.effects]


def starts_run(result):
    return bool({EffectKind.CREATE_SESSION, EffectKind.PREPARE_SESSION} & set(kinds(result)))


def messages(result):
    return Harness.of(result, EffectKind.SEND_MESSAGE)


def comment(h: Harness, text: str = "you missed X", **kw):
    return h.send(P, ev.PlanFeedback(text_digest=text), **kw)


# ------------------------------------------------------------------ B: Triaged


def test_B_comment_after_published_triage_reruns_triage_in_same_session():
    h = Harness()
    first = h.triage()
    assert h.p().stage == Stage.TRIAGED and Hold.AWAITING_OWNER in h.p().holds
    r = comment(h, "you missed X, Y, Z; try again")
    assert r.audit.accepted
    p = h.p()
    assert p.stage == Stage.TRIAGED  # the card does not move
    assert starts_run(r) and EffectKind.PREPARE_SESSION in kinds(r)  # the live root, reused
    run = h.cur()
    assert run.kind == SessionKind.TRIAGE and run.session_id != first.session_id
    assert run.root_id == first.root_id
    assert Hold.AWAITING_OWNER not in p.holds
    h.create_ok()
    assert h.cur().lifecycle == Lifecycle.ACTIVE


def test_B_comments_during_a_rerun_never_start_a_second_run():
    h = Harness()
    h.triage()
    comment(h, "one")
    rerun = h.cur()
    # Pending (prepare in flight), then running: later comments start nothing.
    r = comment(h, "two")
    assert r.audit.accepted and not starts_run(r) and not messages(r)
    h.create_ok()
    for text in ("three", "four"):
        r = comment(h, text)
        assert r.audit.accepted and r.effects == ()
    assert h.cur().session_id == rerun.session_id
    assert sum(1 for s in h.p().sessions if s.kind == SessionKind.TRIAGE) == 2


def test_B_comment_during_first_triage_is_folded_into_it():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    run = h.create_ok()
    r = comment(h, "also check Y")
    assert r.audit.accepted and r.effects == ()
    assert h.cur().session_id == run.session_id


def test_B_comment_after_blocked_triage_reruns_it():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    run = h.create_ok()
    assert run.root_id is not None
    h.send(P, result_candidate(run.session_id, run.root_id, run.revision, ev.ResultKind.BLOCKED))
    assert Hold.AGENT_BLOCKED in h.p().holds
    r = comment(h, "here is the missing detail")
    assert r.audit.accepted and Hold.AGENT_BLOCKED not in h.p().holds
    assert h.p().pending_authorization_id is not None  # re-run once the old run drains
    h.quiesce(P, run.session_id)
    assert h.cur().kind == SessionKind.TRIAGE and h.cur().session_id != run.session_id


def test_B_drag_to_scoped_during_rerun_wins_and_stops_the_triage_run():
    h = Harness()
    h.triage()
    comment(h, "only fix A, B and D; C is not proceeding")
    rerun = h.create_ok()
    r = h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert r.audit.accepted and h.p().stage == Stage.SCOPED
    assert h.p().session(rerun.session_id).lifecycle == Lifecycle.DRAINING
    assert EffectKind.INTERRUPT_TREE in kinds(r)
    h.quiesce(P, rerun.session_id)
    plan = h.cur()
    assert plan.kind == SessionKind.PLAN and plan.root_id == rerun.root_id


# ------------------------------------------------------------------ C: Scoped


def test_C_plan_comment_messages_an_idle_plan_once_per_burst():
    h = Harness()
    plan = h.plan_published()
    assert h.cur().lifecycle == Lifecycle.WAITING
    rev = h.p().revision
    r = comment(h, "use the existing helper")
    [msg] = messages(r)
    assert msg.args["purpose"] == MessagePurpose.FEEDBACK.value
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.p().revision == rev + 1
    # While it works on the revision, further comments open a revision but send nothing.
    r = comment(h, "and keep the API")
    assert r.audit.accepted and not messages(r) and not starts_run(r)
    assert h.cur().session_id == plan.session_id
    assert h.cur().revision == h.p().revision == rev + 2 and h.p().revision_pending


def test_C_comment_while_plan_turn_runs_sends_no_message():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    run = h.create_ok()
    assert run.lifecycle == Lifecycle.ACTIVE
    r = comment(h, "only A")
    assert r.audit.accepted and not messages(r) and not starts_run(r)
    assert h.cur().revision == h.p().revision


# ------------------------------------------------------------------ D: Building


def _waiting_build(h: Harness):
    b = h.to_building()
    assert b.root_id is not None
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
    assert h.cur().wait_reason is not None and h.cur().lifecycle == Lifecycle.WAITING
    return b


def test_D_build_comment_wakes_waiting_build_without_voiding_approval():
    h = Harness()
    b = _waiting_build(h)
    approval = h.p().current_approval_id
    r = comment(h, "rename the flag")
    assert r.audit.accepted
    [msg] = messages(r)
    assert msg.args["purpose"] == MessagePurpose.FEEDBACK.value
    p = h.p()
    assert p.current_approval_id == approval and p.current_approval.valid
    assert not p.revision_pending and p.stage == Stage.BUILDING
    run = h.cur()
    assert run.session_id == b.session_id and run.lifecycle == Lifecycle.ACTIVE
    assert run.feedback_wakes == 1 and not run.fences
    assert not starts_run(r) and EffectKind.INTERRUPT_TREE not in kinds(r)
    # A second comment while it works: no second message.
    r = comment(h, "and the docs")
    assert r.audit.accepted and not messages(r)
    # Evidence for the old head cannot move it to Ready while it works.
    h.send(P, ev.PRObserved(pr_number=7, head_sha=HEAD, bot_authored=True, parcel_branch=True))
    h.send(
        P, ev.ReadinessEvidence(session_id=b.session_id, pr_number=7, head_sha=HEAD, verified=True)
    )
    h.quiesce(P, b.session_id)
    assert h.p().stage == Stage.BUILDING


def test_D_comment_while_build_turn_runs_is_recorded_only():
    h = Harness()
    b = h.to_building()
    approval = h.p().current_approval_id
    r = comment(h, "prefer the smaller change")
    assert r.audit.accepted and r.effects == ()
    assert h.p().current_approval_id == approval
    assert h.cur().session_id == b.session_id and not h.cur().fences


# ------------------------------------------------------------------ E: elsewhere


def test_E_comments_in_inbox_ready_or_opted_out_start_nothing():
    h = Harness()
    h.eligible()
    r = comment(h, "note for later")
    assert r.audit.accepted and r.effects == ()  # recorded for later stages
    h2 = Harness()
    b = _waiting_build(h2)
    h2.send(P, ev.PRObserved(pr_number=7, head_sha=HEAD, bot_authored=True, parcel_branch=True))
    h2.send(
        P, ev.ReadinessEvidence(session_id=b.session_id, pr_number=7, head_sha=HEAD, verified=True)
    )
    h2.quiesce(P, b.session_id)
    assert h2.p().stage == Stage.READY
    r = comment(h2, "looks good")
    assert r.audit.accepted and not [e for e in r.effects if e.kind in WORK_BEARING_KINDS]
    assert not starts_run(r) and h2.p().stage == Stage.READY
    h3 = Harness()
    h3.triage()
    h3.send(P, ev.AssignedHuman())
    r = comment(h3, "try again", evidence=snapshot(human_assigned=True))
    assert not r.audit.accepted and not starts_run(r)


# ------------------------------------------------------------------ F: commands


def test_F_stop_command_still_stops_a_feedback_rerun():
    h = Harness()
    h.triage()
    comment(h, "try again")
    rerun = h.create_ok()
    r = h.send(P, ev.Stop())
    assert r.audit.accepted and Hold.STOPPED in h.p().holds
    assert FenceKind.STOPPED in h.p().session(rerun.session_id).fences
    assert not starts_run(r) and not messages(r)
