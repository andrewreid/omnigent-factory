"""Pilot follow-ups (2026-09-29): #477's stale column read and early Ready, the queue
position note, owner comments reaching an idle run, and issue-session housekeeping."""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    BotState,
    Hold,
    IssueSessionStatus,
    Lifecycle,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import config, result_candidate, snapshot
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"
Q = "I_parcel_2"
PR = 7


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def feedback_messages(result) -> list:
    return [
        e
        for e in Harness.of(result, EffectKind.SEND_MESSAGE)
        if e.args.get("purpose") == MessagePurpose.FEEDBACK.value
    ]


def comment(h: Harness, text: str = "please also do X", pid: str = P):
    return h.send(pid, ev.PlanFeedback(text_digest=text))


def build_ready(h: Harness):
    s = h.cur()
    assert s.root_id is not None
    return h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=HEAD,
        ),
    )


def evidence(h: Harness, **kw) -> ev.ReadinessEvidence:
    r = h.p().readiness
    assert r is not None
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=HEAD, **kw)


# ------------------------------------------------------- #477: stale column read


def test_read_taken_before_the_daemons_move_is_not_a_leftward_drag():
    """#477: Ready -> Building (bot findings) landed at t; a checks webhook whose snapshot
    was read just before t still showed Ready. Applied after the move, it flipped the
    stage back to Ready, and the next fresh read of Building looked like a leftward move:
    a spurious safety fence."""
    h = Harness()
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY
    before_move = h.f(P).now + 1
    h.f(P).tick()
    h.send(P, ev.ReviewChanged(pr_number=PR, head_sha=HEAD, changes_requested=True))
    assert h.p().stage == Stage.BUILDING and not h.p().pending_moves  # landed
    h.send(
        P,
        ev.ChecksChanged(pr_number=PR, head_sha=HEAD, state=ev.ChecksState.GREEN),
        evidence=snapshot(stage=Stage.READY, read_at_us=before_move),
        provenance=Provenance.WEBHOOK,
    )
    assert h.p().stage == Stage.BUILDING
    h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(stage=Stage.BUILDING, read_at_us=h.f(P).now + 1),
        provenance=Provenance.RECONCILER,
    )
    assert Hold.SAFETY not in h.p().holds and h.p().stage == Stage.BUILDING


def test_late_echo_of_an_earlier_daemon_write_is_ignored():
    h = Harness()
    h.to_building()
    h.build_ready()
    echo_time = h.f(P).now
    h.send(P, ev.ReviewChanged(pr_number=PR, head_sha=HEAD, changes_requested=True))
    h.send(
        P,
        ev.ColumnObserved(stage=Stage.READY),
        provenance=Provenance.WEBHOOK,
        time_us=echo_time,
    )
    h.send(P, ev.ColumnObserved(stage=Stage.BUILDING), provenance=Provenance.WEBHOOK)
    assert h.p().stage == Stage.BUILDING and Hold.SAFETY not in h.p().holds


# --------------------------------------------------- #477: review-bot grace


def grace_harness(minutes: int = 10) -> Harness:
    return Harness(cfg=replace(config(), review_grace_us=minutes * MICROS_PER_MINUTE))


def test_ready_waits_for_review_bots_then_needs_a_read_after_the_grace():
    """#477 went Ready 2 minutes after the PR opened; codex commented 2 minutes later and
    the parcel dead-ended. A green read inside the grace is not Ready."""
    h = grace_harness()
    h.to_building()
    build_ready(h)
    h.send(P, evidence(h, verified=True, checks=ev.ChecksState.GREEN))
    h.quiesce(P, h.cur().session_id)
    assert h.p().stage == Stage.BUILDING and h.p().bot == BotState.WORKING
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(r)  # keeps reading during the grace
    h.f(P).tick(10 * MICROS_PER_MINUTE)
    r = h.send(P, ev.ReconcileDue())
    assert EffectKind.FETCH_PR_EVIDENCE in kinds(r) and h.p().stage == Stage.BUILDING
    h.send(P, evidence(h, verified=True, checks=ev.ChecksState.GREEN))
    assert h.p().stage == Stage.READY and h.p().readiness.ready


def test_findings_inside_the_grace_wake_the_build_instead_of_needs_you():
    h = grace_harness()
    h.to_building()
    build_ready(h)
    h.quiesce(P, h.cur().session_id)
    r = h.send(P, evidence(h, checks=ev.ChecksState.GREEN, findings_open=True))
    [wake] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value
    assert Hold.READINESS_FAILED not in h.p().holds


def test_no_grace_configured_is_ready_at_once():
    h = Harness()
    h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY


# ------------------------------------------------------------- queue position


def test_queue_position_note_moves_up_when_a_build_ahead_is_admitted():
    h = Harness()
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(Q)
    h.approve(P)
    assert h.p(P).note == "Queued: 2nd in line"
    h.admit(Q)
    r = h.send(P, ev.ReconcileDue())
    assert h.p(P).note == "Queued: 1st in line" and h.p(P).bot == BotState.QUEUED
    [note] = Harness.of(r, EffectKind.SET_NOTE)
    assert note.args["note"] == "Queued: 1st in line"
    r = h.send(P, ev.ReconcileDue())
    assert not Harness.of(r, EffectKind.SET_NOTE)  # written only on change


# ------------------------------------------------ owner comments to idle runs


def test_comment_reaches_an_active_build_whose_turn_ended_once_until_idle_again():
    h = Harness()
    build = h.to_building()
    r = comment(h, "first")
    assert not feedback_messages(r)  # mid-turn: folded into the running turn
    h.quiesce(P, build.session_id)  # the turn ended without a result
    r = comment(h, "second")
    [msg] = feedback_messages(r)
    assert msg.preconditions.session_id == build.session_id
    r = comment(h, "third")
    assert not feedback_messages(r)  # debounced until the tree is seen idle again
    h.quiesce(P, build.session_id)
    assert feedback_messages(comment(h, "fourth"))
    assert sum(s.kind == SessionKind.BUILD for s in h.p().sessions) == 1  # no new run


def test_comment_reaches_a_build_that_reported_blocked_and_clears_the_block():
    """#651: the build reported blocked and went idle; the owner's comment is the answer."""
    h = Harness()
    build = h.to_building()
    assert build.root_id is not None
    h.send(
        P, result_candidate(build.session_id, build.root_id, build.revision, ev.ResultKind.BLOCKED)
    )
    h.quiesce(P, build.session_id)
    assert Hold.AGENT_BLOCKED in h.p().holds
    r = comment(h, "file the follow-up issue, reply and resolve the thread")
    assert feedback_messages(r) and Hold.AGENT_BLOCKED not in h.p().holds


def test_comment_reaches_an_idle_plan_run():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    plan = h.create_ok()
    h.quiesce(P, plan.session_id)
    r = comment(h)
    [msg] = feedback_messages(r)
    assert msg.preconditions.session_id == plan.session_id
    assert msg.args["revision"] == h.p().revision == plan.revision + 1


def test_comment_reaches_an_idle_triage_run_without_a_second_run():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    run = h.create_ok()
    h.quiesce(P, run.session_id)
    r = comment(h)
    assert feedback_messages(r) and EffectKind.PREPARE_SESSION not in kinds(r)
    assert h.cur().session_id == run.session_id


def test_busy_tree_gets_no_extra_message():
    h = Harness()
    build = h.to_building()
    h.quiesce(P, build.session_id)
    h.send(P, ev.RuntimeActivity(session_id=build.session_id, busy=True))
    assert not feedback_messages(comment(h))


# -------------------------------------------------------- session housekeeping


def test_issue_title_change_renames_the_issue_session_and_old_reads_do_not_undo_it():
    h = Harness()
    triage = h.triage()
    assert h.p().issue_session.title == "Add export button"
    r = h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(title="Export  CSV button", read_at_us=h.f(P).now + 1),
        provenance=Provenance.RECONCILER,
    )
    [rename] = Harness.of(r, EffectKind.RENAME_SESSION)
    assert rename.args == {"root_id": triage.root_id, "title": "#1 · Export CSV button"}
    r = h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(title="Add export button", read_at_us=1),
        provenance=Provenance.RECONCILER,
    )
    assert not Harness.of(r, EffectKind.RENAME_SESSION)
    assert h.p().issue_session.title == "Export CSV button"


def test_replaced_issue_session_is_archived():
    h = Harness()
    triage = h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    plan = h.cur()
    h.send(P, ev.Prepared(session_id=plan.session_id, ok=False, unusable=True, reason="full"))
    mark = len(h.log)
    replacement = h.create_ok()
    closes = [e for _, r in h.log[mark:] for e in r.effects if e.kind == EffectKind.CLOSE_SESSION]
    assert [c.args["root_id"] for c in closes] == [triage.root_id]
    r = h.send(P, ev.IssueSessionClosed(root_id=str(triage.root_id)))
    issue = h.p().issue_session
    assert r.audit.accepted and issue.root_id == replacement.root_id
    assert issue.status == IssueSessionStatus.LIVE
    assert h.p().current_session.lifecycle == Lifecycle.ACTIVE
