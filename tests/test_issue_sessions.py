"""One Omnigent issue session per parcel, reused by successive stage runs.

Stop/revoke ends a run (fences, credential, interrupt) and never the conversation; a dead
or unusable session is replaced; a terminal parcel's quiescent session is archived; owner
questions from factory_ask_owner are idempotent and answered by message.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import (
    DecisionSource,
    DecisionStatus,
    FenceKind,
    Hold,
    IssueSessionStatus,
    Lifecycle,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import OWNER_ID, result_candidate
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def kinds(result):
    return [e.kind for e in result.effects]


def test_triage_plan_build_share_one_issue_session():
    h = Harness()
    triage = h.triage()
    issue = h.p().issue_session
    assert issue is not None and issue.root_id == triage.root_id and issue.generation == 1
    r = h.send(P, ev.RequestPlan(via=Via.DRAG))
    [prepare] = Harness.of(r, EffectKind.PREPARE_SESSION)
    assert prepare.args == {"root_id": triage.root_id, "profile": "read_only", "reuse": True}
    assert EffectKind.CREATE_SESSION not in kinds(r)
    plan = h.create_ok()
    h.publish_plan()
    h.approve()
    build = h.admit()
    p = h.p()
    assert triage.root_id == plan.root_id == build.root_id
    assert [s.kind for s in p.sessions] == [SessionKind.TRIAGE, SessionKind.PLAN, SessionKind.BUILD]
    assert len({s.session_id for s in p.sessions}) == 3  # distinct runs
    creates = [e for e, r in h.log for e in r.effects if e.kind == EffectKind.CREATE_SESSION]
    assert len(creates) == 1
    # Each run gets its own cost-policy generation in the shared root.
    generations = [s.grant.policy_generation for s in p.sessions]
    assert generations == sorted(set(generations))
    assert build.lifecycle == Lifecycle.ACTIVE and p.issue_session is not None
    assert (p.issue_session.root_id, p.issue_session.generation) == (issue.root_id, 1)
    assert p.issue_session.title == "Add export button"  # learnt from reads, no rename
    assert not any(e.kind == EffectKind.RENAME_SESSION for _, r in h.log for e in r.effects)


def test_reused_run_switches_off_the_previous_runs_credential_first():
    h = Harness()
    plan = h.plan_published()
    h.approve()  # the plan run retires (no fence, so its enablement is still recorded)
    assert h.p().session(plan.session_id).lifecycle == Lifecycle.RETIRED
    assert h.p().session(plan.session_id).issuance_enabled
    r = h.send(P, ev.CapacityAvailable())
    order = [e.kind for e in r.effects]
    disable = order.index(EffectKind.DISABLE_ISSUANCE)
    assert r.effects[disable].preconditions.session_id == plan.session_id
    assert disable < order.index(EffectKind.PREPARE_SESSION)
    assert EffectKind.ENABLE_ISSUANCE not in order  # only after the new run is prepared
    assert not h.p().session(plan.session_id).issuance_enabled
    build = h.create_ok()
    assert build.issuance_enabled and build.root_id == plan.root_id


def test_stop_ends_the_run_not_the_session_and_replan_reuses_it():
    """Regression for the old deadlock: a stopped/revoked run never fences the root."""
    h = Harness()
    build = h.to_building()
    r = h.send(P, ev.Stop())
    assert EffectKind.INTERRUPT_TREE in kinds(r) and EffectKind.DISABLE_ISSUANCE in kinds(r)
    assert h.p().issue_session.status == IssueSessionStatus.LIVE
    h.quiesce(P, build.session_id)
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert r.audit.accepted, r.audit.reason
    # The run starts once the card move lands (harness auto-acks it): prepare, no create.
    started = h.log[-1][1]
    [prepare] = Harness.of(started, EffectKind.PREPARE_SESSION)
    assert prepare.args["root_id"] == build.root_id and prepare.args["reuse"] is True
    assert not any(e.kind == EffectKind.CREATE_SESSION for _, x in h.log[-2:] for e in x.effects)
    plan = h.create_ok()
    assert plan.kind == SessionKind.PLAN and plan.lifecycle == Lifecycle.ACTIVE
    assert plan.root_id == build.root_id and not plan.fences
    assert FenceKind.STOPPED in h.p().session(build.session_id).fences  # never cleared
    h.publish_plan(goal="replanned")
    assert h.p().current_contract is not None and Hold.AWAITING_OWNER in h.p().holds
    # And the next build also reuses the same conversation.
    h.approve()
    assert h.admit().root_id == build.root_id


def test_revoked_build_then_replan_same_session():
    h = Harness()
    build = h.to_building()
    h.send(P, ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED), actor=OWNER_ID)
    h.quiesce(P, build.session_id)
    plan = h.create_ok()
    assert plan.kind == SessionKind.PLAN and plan.root_id == build.root_id
    assert {FenceKind.SAFETY, FenceKind.REVOKED} <= h.p().session(build.session_id).fences


def test_unusable_issue_session_is_replaced_for_the_same_run():
    h = Harness()
    triage = h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    plan = h.cur()
    r = h.send(
        P,
        ev.Prepared(session_id=plan.session_id, ok=False, unusable=True, reason="context rollover"),
    )
    assert r.audit.accepted, r.audit.reason
    [create] = Harness.of(r, EffectKind.CREATE_SESSION)
    assert create.preconditions.session_id == plan.session_id and create.args["nonce"] == plan.nonce
    issue = h.p().issue_session
    assert issue.status == IssueSessionStatus.DEAD and issue.reason == "context rollover"
    assert h.cur().lifecycle == Lifecycle.INTENT and h.cur().root_id is None
    assert Hold.PREPARE_FAILED not in h.p().holds
    replacement = h.create_ok()
    issue = h.p().issue_session
    assert replacement.root_id != triage.root_id
    assert issue.root_id == replacement.root_id and issue.generation == 2
    assert issue.created_by == plan.session_id and issue.status == IssueSessionStatus.LIVE


def test_unusable_root_of_its_own_creating_run_is_a_prepare_failure():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="root-new", nonce=s.nonce))
    h.send(P, ev.Prepared(session_id=s.session_id, ok=False, unusable=True, reason="x"))
    assert Hold.PREPARE_FAILED in h.p().holds


def test_crashed_issue_session_is_replaced_with_a_fresh_root():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    first = h.create_ok()
    h.send(P, ev.SessionCrashed(session_id=first.session_id))
    assert h.p().issue_session.status == IssueSessionStatus.DEAD
    r = h.quiesce(P, first.session_id)
    [create] = Harness.of(r, EffectKind.CREATE_SESSION)
    second = h.create_ok()
    assert second.session_id != first.session_id and second.root_id != first.root_id
    assert h.p().issue_session.generation == 2 and h.p().issue_session.root_id == second.root_id
    assert create.preconditions.session_id == second.session_id


def test_merged_parcel_archives_its_issue_session_once():
    h = Harness()
    build = h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY
    r = h.send(
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
    [close] = Harness.of(r, EffectKind.CLOSE_SESSION)
    assert close.args == {"root_id": build.root_id}
    assert h.p().issue_session.status == IssueSessionStatus.CLOSING
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.CLOSE_SESSION not in kinds(r)
    r = h.send(P, ev.IssueSessionClosed(root_id=build.root_id))
    assert r.audit.accepted and h.p().issue_session.status == IssueSessionStatus.CLOSED
    assert not h.send(P, ev.IssueSessionClosed(root_id=build.root_id)).audit.accepted


def test_closed_issue_archives_only_after_the_tree_is_quiescent():
    h = Harness()
    build = h.to_building()
    r = h.send(P, ev.Closed())
    assert EffectKind.CLOSE_SESSION not in kinds(r)  # the build tree is still draining
    r = h.quiesce(P, build.session_id)
    [close] = Harness.of(r, EffectKind.CLOSE_SESSION)
    assert close.args["root_id"] == build.root_id


def test_fresh_control_after_closure_starts_a_new_issue_session():
    h = Harness()
    build = h.to_building()
    h.send(P, ev.Closed())
    h.quiesce(P, build.session_id)
    h.send(P, ev.IssueSessionClosed(root_id=build.root_id))
    h.eligible()  # reopened
    r = h.send(P, ev.RequestPlan(via=Via.COMMAND))
    assert r.audit.accepted, r.audit.reason
    started = [e.kind for _, x in h.log[-2:] for e in x.effects]
    assert EffectKind.CREATE_SESSION in started and EffectKind.PREPARE_SESSION not in started
    assert h.cur().kind == SessionKind.PLAN and h.cur().root_id is None


# ------------------------------------------------------------------ owner questions


def ask(h: Harness, key: str = "q1", summary: str = "Keep v1?") -> object:
    return h.send(
        P, ev.OwnerQuestion(session_id=h.cur().session_id, question_key=key, summary=summary)
    )


def test_owner_question_waits_posts_once_and_relays_the_answer():
    h = Harness()
    plan = h.plan_published()
    h.send(P, ev.PlanFeedback(text_digest="d"))  # back to ACTIVE
    r = ask(h)
    [comment] = Harness.of(r, EffectKind.POST_COMMENT)
    assert comment.args["template"] == "decision" and comment.args["summary"] == "Keep v1?"
    assert comment.args["root_id"] == plan.root_id
    [d] = h.p().open_decisions
    assert d.source == DecisionSource.MCP and d.elicitation_id == "mcp:q1"
    assert h.cur().lifecycle == Lifecycle.WAITING
    assert h.p().bot.value == "Needs you"
    again = ask(h)
    assert not again.audit.accepted and again.audit.reason == "duplicate-question"
    assert not Harness.of(again, EffectKind.POST_COMMENT)
    # Reconcile never mistakes a tool question for a vanished native prompt.
    h.send(P, ev.ReconcileDue())
    assert h.p().decision(d.decision_id).status == DecisionStatus.OPEN
    r = h.send(P, ev.Decide(decision_id=d.decision_id, answer="keep"))
    [relay] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert relay.args["purpose"] == MessagePurpose.ANSWER_RELAY.value
    assert not Harness.of(r, EffectKind.RESOLVE_ELICITATION)
    assert h.p().decision(d.decision_id).status == DecisionStatus.RELAYED


def test_owner_question_refused_once_the_run_is_stopped():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    h.create_ok()
    h.send(P, ev.Stop())
    r = ask(h)
    assert not r.audit.accepted and r.audit.reason == "question-through-closed-gate"


def test_results_and_questions_are_admitted_only_from_the_mcp_endpoint():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok()
    from omnigent_factory.core.events import Provenance

    for provenance in (Provenance.ADAPTER, Provenance.WEBHOOK, Provenance.OPERATOR):
        r = h.send(
            P,
            result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE),
            provenance=provenance,
        )
        assert not r.audit.accepted
        r = h.send(
            P,
            ev.OwnerQuestion(session_id=s.session_id, question_key="k", summary="?"),
            provenance=provenance,
        )
        assert not r.audit.accepted
