"""A build that says the plan must change replans by itself (reducer rows, #1141).

A build or rework run whose owner direction goes beyond the approved plan submits
``blocked`` with ``replan_required`` (reason, changes). The factory then supersedes the
approved plan, moves the card to Planning and starts a plan run in the same issue
session, without an owner command; with an owner question still open it waits for the
owner's reply and starts on it. At most ``auto_replan_limit`` such replans since the
owner's last plan control: past it the card is Needs you with a comment. Stops, closures,
assignment and epics win. ``/approve`` of the plan the running build already has is
refused visibly instead of a 👍 that changes nothing.
"""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.reducer import NOTHING_TO_APPROVE_NOTE, REPLANNING_NOTE
from omnigent_factory.core.types import (
    BotState,
    DecisionImpact,
    DecisionStatus,
    FenceKind,
    Hold,
    IssueLinks,
    Lifecycle,
    LinkedIssue,
    QueueStatus,
    ReplanStatus,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import config, result_candidate, snapshot
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
REASON = "The owner's late-finish threshold reverses part of the approved plan."
CHANGES = ("Flag late finishes after 30 minutes, not 15", "Drop the overtime banner")


def rework_run(h: Harness | None = None) -> Harness:
    """#1141: built, Ready, then owner feedback starts a rework run (same issue session)."""
    h = h or Harness()
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY
    h.send(P, ev.PlanFeedback(text_digest="Change the threshold and drop the banner"))
    h.send(P, ev.CapacityAvailable())
    run = h.create_ok()
    assert run.kind == SessionKind.BUILD and run.lifecycle == Lifecycle.ACTIVE
    assert h.p().authorization(run.authorization_id).rework
    return h


def ask(h: Harness) -> str:
    s = h.cur()
    h.send(
        P,
        ev.OwnerQuestion(
            session_id=s.session_id,
            question_key="late-finish",
            summary="Which late-finish threshold?",
            impact=DecisionImpact.PLAN_REVISION,
        ),
    )
    [d] = h.p().open_decisions
    return d.decision_id


def replan_result(h: Harness, **over: object):
    s = h.cur()
    assert s.root_id is not None
    fields: dict[str, object] = {"replan_reason": REASON, "replan_changes": CHANGES, **over}
    return h.send(
        P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED, **fields)
    )


def plan_auths(h: Harness) -> list:
    return [a for a in h.p().authorizations if a.kind == SessionKind.PLAN]


def assert_replanning(h: Harness, build_id: str, approval_id: str, plans_before: int) -> None:
    p = h.p()
    assert p.stage == Stage.SCOPED and p.note == REPLANNING_NOTE
    assert p.revision_pending and p.current_approval_id is None
    old = p.approval(approval_id)
    assert old is not None and not old.valid and old.invalidated_reason == "replan-required"
    assert p.current_contract is not None and p.current_contract.superseded  # kept on record
    assert Hold.AGENT_BLOCKED not in p.holds
    build = p.session(build_id)
    assert build is not None and FenceKind.REVOKED in build.fences
    assert build.lifecycle == Lifecycle.DRAINING
    [*_, auth] = plan_auths(h)
    assert len(plan_auths(h)) == plans_before + 1
    assert p.pending_authorization_id == auth.authorization_id
    request = p.replan_request
    assert request is not None and request.status == ReplanStatus.STARTED
    assert request.plan_authorization_id == auth.authorization_id
    assert request.reason == REASON and request.changes == CHANGES
    assert p.auto_replans == 1


def start_plan_run(h: Harness, build_id: str) -> None:
    """The revoked build's tree goes quiet: the plan run starts in the same session."""
    root = h.p().session(build_id).root_id
    h.quiesce(P, build_id)
    plan = h.cur()
    assert plan.kind == SessionKind.PLAN and plan.root_id == root  # same issue session
    assert plan.lifecycle == Lifecycle.PREPARING
    assert plan.authorization_id == h.p().replan_request.plan_authorization_id
    h.create_ok()
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.p().bot == BotState.WORKING


# ------------------------------------------------------------- #1141 replay


def test_1141_replan_waits_for_the_owners_reply_then_starts_in_the_same_session():
    h = rework_run()
    build = h.cur()
    approval = h.p().current_approval_id
    decision = ask(h)
    plans = len(plan_auths(h))
    r = replan_result(h)
    assert r.audit.accepted
    p = h.p()
    # Held on the open question: nothing replans yet, the card asks for the answer.
    assert p.replan_request.status == ReplanStatus.HELD
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert p.current_approval_id == approval and len(plan_auths(h)) == plans
    assert not Harness.of(r, EffectKind.PUBLISH_REPORT)  # no blocked comment
    # The owner's plain reply starts the replan at once: no /replan needed.
    r = h.send(P, ev.PlanFeedback(text_digest="30 minutes"))
    assert r.audit.accepted
    assert_replanning(h, build.session_id, approval, plans)
    assert h.p().decision(decision).status == DecisionStatus.RELAYED  # read by the plan run
    assert not [
        e for e in Harness.of(r, EffectKind.SEND_MESSAGE) if e.target == build.session_id
    ]  # the revoked build is not sent the answer
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    assert move.args["to"] == Stage.SCOPED.value  # the board's Planning column
    start_plan_run(h, build.session_id)


def test_immediate_replan_when_no_question_is_open():
    h = rework_run()
    build = h.cur()
    approval = h.p().current_approval_id
    plans = len(plan_auths(h))
    r = replan_result(h)
    assert r.audit.accepted
    assert_replanning(h, build.session_id, approval, plans)
    assert Harness.of(r, EffectKind.INTERRUPT_TREE)  # the /replan stop path, nothing more
    assert not Harness.of(r, EffectKind.PUBLISH_REPORT)
    start_plan_run(h, build.session_id)


def test_a_plain_blocked_result_still_only_blocks():
    h = rework_run()
    s = h.cur()
    r = h.send(P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    p = h.p()
    assert Hold.AGENT_BLOCKED in p.holds and p.bot == BotState.BLOCKED
    assert p.stage == Stage.BUILDING and getattr(p, "replan_request", None) is None
    assert Harness.of(r, EffectKind.PUBLISH_REPORT)


def test_approving_the_revised_plan_continues_as_rework_on_the_same_branch_and_pr():
    h = rework_run()
    build = h.cur()
    replan_result(h)
    start_plan_run(h, build.session_id)
    h.publish_plan(goal="Ship it with a 30 minute threshold")
    assert h.p().current_contract.revision == h.p().revision
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND), event_id="github:comment:91:created")
    assert r.audit.accepted
    assert [e.args for e in Harness.of(r, EffectKind.REACT_COMMENT)] == [
        {"comment_id": 91, "content": "+1"}
    ]
    auth = h.p().authorizations[-1]
    assert auth.kind == SessionKind.BUILD and auth.rework  # rework template, PR #7
    assert auth.approval_id == h.p().current_approval_id
    assert h.p().stage == Stage.BUILDING


# ------------------------------------------------------------- loop guard


def build_of_revised_plan(h: Harness) -> None:
    build = h.cur()
    replan_result(h)
    start_plan_run(h, build.session_id)
    h.publish_plan(goal="Revised")
    h.approve()
    h.admit()


def test_loop_guard_the_revised_plans_build_asking_again_needs_you():
    h = rework_run()
    build_of_revised_plan(h)
    approval = h.p().current_approval_id
    plans = len(plan_auths(h))
    r = replan_result(h)
    assert r.audit.accepted
    p = h.p()
    assert p.replan_request.status == ReplanStatus.LIMITED and p.auto_replans == 1
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert Hold.REPLAN_LIMIT in p.holds and len(plan_auths(h)) == plans
    assert p.current_approval_id == approval and p.current_approval.valid
    [report] = Harness.of(r, EffectKind.PUBLISH_REPORT)
    assert report.args["replan_limit"] is True  # one short comment for the owner
    # The owner's /replan revises it with the build's changes; the guard starts afresh.
    h.send(P, ev.RequestReplan(via=Via.COMMAND))
    p = h.p()
    assert p.stage == Stage.SCOPED and p.auto_replans == 0
    assert Hold.REPLAN_LIMIT not in p.holds
    assert p.replan_request.status == ReplanStatus.STARTED
    assert p.replan_request.plan_authorization_id == p.pending_authorization_id


def test_loop_guard_reply_keeps_the_approved_plan():
    h = rework_run()
    build_of_revised_plan(h)
    replan_result(h)
    h.quiesce(P, h.cur().session_id)  # its turn ended
    r = h.send(P, ev.PlanFeedback(text_digest="No, keep to the plan"))
    assert r.audit.accepted and h.p().stage == Stage.BUILDING
    assert Hold.REPLAN_LIMIT not in h.p().holds
    [msg] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert msg.args["purpose"] == MessagePurpose.FEEDBACK.value  # relayed to the build


def test_loop_guard_limit_is_configurable():
    h = rework_run(Harness(cfg=config(auto_replan_limit=2)))
    build_of_revised_plan(h)
    build = h.cur()
    approval = h.p().current_approval_id
    plans = len(plan_auths(h))
    replan_result(h)
    p = h.p()
    assert p.replan_request.status == ReplanStatus.STARTED and p.auto_replans == 2
    assert len(plan_auths(h)) == plans + 1 and p.stage == Stage.SCOPED
    assert not p.approval(approval).valid
    assert p.session(build.session_id).fences
    h2 = rework_run(Harness(cfg=config(auto_replan_limit=0)))
    replan_result(h2)
    assert h2.p().replan_request.status == ReplanStatus.LIMITED


# ------------------------------------------------------------- restart safety


def test_restart_mid_replan_never_starts_a_second_plan_run():
    h = rework_run()
    build = h.cur()
    ask(h)
    replan_result(h)
    reply = h.f(P).make(ev.PlanFeedback(text_digest="30 minutes"))
    h.apply(reply)
    plans = len(plan_auths(h))
    # A restart reloads the persisted aggregate; the reply's redelivery is a duplicate.
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    again = h.apply(reply)
    assert again.duplicate and len(plan_auths(h)) == plans
    h.send(P, ev.ReconcileDue())
    start_plan_run(h, build.session_id)
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    h.send(P, ev.ReconcileDue())
    h.quiesce(P, h.cur().session_id)
    assert len(plan_auths(h)) == plans
    assert len([s for s in h.p().sessions if s.kind == SessionKind.PLAN]) == 2  # first + 1
    assert h.p().replan_request.status == ReplanStatus.STARTED
    # A later reply cannot start it again (the request is no longer held).
    h.send(P, ev.PlanFeedback(text_digest="also this"))
    assert len(plan_auths(h)) == plans


# ------------------------------------------------------------- owner safety wins


def held(h: Harness | None = None) -> Harness:
    h = rework_run(h)
    ask(h)
    replan_result(h)
    assert h.p().replan_request.status == ReplanStatus.HELD
    return h


def test_a_stopped_issue_is_not_replanned_by_the_reply():
    h = held()
    plans = len(plan_auths(h))
    h.send(P, ev.Stop())
    assert h.p().replan_request.status == ReplanStatus.CANCELLED
    h.send(P, ev.PlanFeedback(text_digest="30 minutes"))
    p = h.p()
    assert len(plan_auths(h)) == plans and p.stage == Stage.BUILDING
    assert Hold.STOPPED in p.holds


def test_an_issue_assigned_to_a_person_is_not_replanned():
    h = held()
    plans = len(plan_auths(h))
    h.send(P, ev.AssignedHuman())
    assert h.p().replan_request.status == ReplanStatus.CANCELLED
    assert len(plan_auths(h)) == plans and h.p().stage == Stage.BUILDING


def test_no_automatic_replan_after_stop_closure_assignment_or_on_an_epic():
    # The result itself is refused once the run is fenced by a stop or closure.
    for body in (ev.Stop(), ev.Closed(), ev.AssignedHuman()):
        h = rework_run()
        s = h.cur()
        h.send(P, body)
        r = h.send(
            P,
            result_candidate(
                s.session_id,
                s.root_id,
                s.revision,
                ev.ResultKind.BLOCKED,
                replan_reason=REASON,
                replan_changes=CHANGES,
            ),
        )
        assert not r.audit.accepted and h.p().replan_request is None
    # An epic is never replanned by itself: the result is an ordinary block.
    h = rework_run()
    epic = IssueLinks(sub_issues=(LinkedIssue(5, True),))
    h.send(P, ev.GitHubSnapshot(), evidence=snapshot(links=epic, read_at_us=h.f(P).now))
    assert h.p().links.epic
    plans = len(plan_auths(h))
    replan_result(h)
    p = h.p()
    assert p.replan_request is None and len(plan_auths(h)) == plans
    assert Hold.AGENT_BLOCKED in p.holds and p.stage == Stage.BUILDING


# ------------------------------------------------------------- /approve


def command(h: Harness, body: ev.EventBody, n: int):
    return h.send(P, body, event_id=f"github:comment:{n}:created")


def reactions(r) -> list[str]:
    return [e.args["content"] for e in Harness.of(r, EffectKind.REACT_COMMENT)]


def test_approve_of_the_plan_the_running_build_has_is_refused_with_confused():
    h = rework_run()
    before = h.p()
    r = command(h, ev.ApprovePlan(via=Via.COMMAND), 70)
    assert not r.audit.accepted and r.audit.reason == "nothing-new-to-approve"
    assert reactions(r) == ["confused"]  # never a thumbs-up
    p = h.p()
    assert p.note == NOTHING_TO_APPROVE_NOTE
    assert p.approvals == before.approvals and p.authorizations == before.authorizations
    # With the plan's hash too.
    full = p.current_contract.full_hash
    r = command(h, ev.ApprovePlan(via=Via.COMMAND, hash_text=full[:12]), 71)
    assert not r.audit.accepted and reactions(r) == ["confused"]
    # #1141: the same after a blocked result, where the 👍 used to look like a success.
    s = h.cur()
    h.send(P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    r = command(h, ev.ApprovePlan(via=Via.COMMAND), 72)
    assert not r.audit.accepted and reactions(r) == ["confused"]
    assert h.p().note == NOTHING_TO_APPROVE_NOTE


def test_valid_approve_paths_are_unchanged():
    # 1. A posted plan awaiting approval: accepted (👍), build queued.
    h = Harness()
    h.plan_published()
    r = command(h, ev.ApprovePlan(via=Via.COMMAND), 80)
    assert r.audit.accepted and reactions(r) == ["+1"]
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED
    # 2. Re-approving a queued build by drag only notes it (no refusal, no rollback).
    h.send(P, ev.ApprovePlan(via=Via.DRAG))
    assert h.p().note.startswith("Approved: build starts")
    # 3. A held approval while native blockers are open: repeating it is accepted.
    h = Harness()
    blocked = IssueLinks(blocked_by=(LinkedIssue(9, True),))
    h.plan_published()

    def with_blocker(body: ev.EventBody, n: int):
        f = h.f(P)
        return h.apply(
            f.make(
                body,
                event_id=f"github:comment:{n}:created",
                evidence=snapshot(links=blocked, read_at_us=f.now + 1),
            )
        )

    r = with_blocker(ev.ApprovePlan(via=Via.COMMAND), 81)
    assert r.audit.accepted and Hold.BLOCKERS in h.p().holds
    approvals = h.p().approvals
    r = with_blocker(ev.ApprovePlan(via=Via.COMMAND), 82)
    assert r.audit.accepted and reactions(r) == ["+1"] and h.p().approvals == approvals
    # 4. After /stop, /approve of the same plan restarts its build.
    h = Harness()
    h.to_building()
    h.send(P, ev.Stop())
    h.quiesce(P, h.cur().session_id)
    r = command(h, ev.ApprovePlan(via=Via.COMMAND), 83)
    assert r.audit.accepted and reactions(r) == ["+1"]
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED


def test_approving_a_revised_plan_after_a_replan_is_accepted():
    h = rework_run()
    build = h.cur()
    replan_result(h)
    # While the replan runs there is nothing to approve yet: refused as before.
    r = command(h, ev.ApprovePlan(via=Via.COMMAND), 84)
    assert not r.audit.accepted and reactions(r) == ["confused"]
    start_plan_run(h, build.session_id)
    h.publish_plan(goal="Revised")
    r = command(h, ev.ApprovePlan(via=Via.COMMAND), 85)
    assert r.audit.accepted and reactions(r) == ["+1"]


def test_a_newer_result_of_the_run_drops_its_held_replan():
    h = held()
    s = h.cur()
    plans = len(plan_auths(h))
    h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha="c" * 40,
        ),
    )
    assert h.p().replan_request.status == ReplanStatus.CANCELLED
    h.send(P, ev.PlanFeedback(text_digest="30 minutes"))
    assert len(plan_auths(h)) == plans and h.p().stage == Stage.BUILDING


def test_owner_decide_on_a_held_replan_starts_it():
    h = held()
    build = h.cur()
    approval = h.p().current_approval_id
    plans = len(plan_auths(h))
    [d] = h.p().open_decisions
    r = h.send(P, ev.Decide(decision_id=d.decision_id, answer="30", within_contract=True))
    assert r.audit.accepted
    p = h.p()
    assert p.replan_request.status == ReplanStatus.STARTED
    assert len(plan_auths(h)) == plans + 1 and p.stage == Stage.SCOPED
    assert not p.approval(approval).valid
    assert p.session(build.session_id).fences


def test_replan_request_round_trips_through_the_codec():
    h = held()
    p = h.p()
    assert parcel_from_json(parcel_to_json(p)) == p
    assert parcel_from_json(parcel_to_json(replace(p, replan_request=None))).replan_request is None


# ------------------------------------------------------------- stage templates


def test_build_rework_and_readiness_templates_describe_replan_required():
    from importlib.resources import files

    from omnigent_factory.service import directory as d

    templates = files("omnigent_factory.service") / "templates"
    for name in (
        d._FIRST_TEMPLATES[SessionKind.BUILD],
        d._REWORK_TEMPLATE,
        d._READINESS_WAKE_TEMPLATE,
    ):
        text = " ".join((templates / name).read_text("utf-8").split())
        assert "beyond the approved plan" in text and "replan_required" in text, name
        assert "the factory replans" in text, name
    # In-flight runs stay pinned to the template bytes they were dispatched with.
    for old in ("build-v13.txt", "build-rework-v10.txt", "readiness-wake-v10.txt"):
        assert "replan_required" not in (templates / old).read_text("utf-8")
    replan = (templates / d._REPLAN_TEMPLATE).read_text("utf-8")
    assert "{brief}" in replan and "{base_note}" in replan
