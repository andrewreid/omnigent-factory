"""Rework: owner feedback on the built work sends the card back to Building (reducer rows).

Any owner comment on the issue, owner PR conversation comment or owner PR review with
text (all ``PlanFeedback``), an owner drag Ready -> Building, or ``RequestRework`` on a
Ready card starts a new build episode under the same approval: admitted like any build
(it may queue), with a fresh time block and a fresh fix budget, in the issue session.
The same applies to a Building card whose build finished Needs you (e.g. #477).
"""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import (
    BotState,
    Hold,
    Lifecycle,
    QueueStatus,
    SessionKind,
    Stage,
)
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, config, result_candidate
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"
HEAD2 = "b" * 40


def kinds(result):
    return [e.kind for e in result.effects]


def ready(h: Harness | None = None, pid: str = P) -> Harness:
    h = h or Harness()
    h.to_building(pid)
    h.build_ready(pid)
    assert h.p(pid).stage == Stage.READY and h.p(pid).bot == BotState.IDLE
    return h


def comment(h: Harness, text: str = "No, don't do that, do X instead", pr: int = 0, **kw):
    return h.send(P, ev.PlanFeedback(text_digest=text, pr_number=pr), **kw)


def assert_rework_queued(h: Harness, approval: str | None) -> None:
    p = h.p()
    assert p.stage == Stage.BUILDING and p.note == "Rework: owner feedback"
    assert p.current_approval_id == approval and p.current_approval.valid  # approval kept
    assert not p.revision_pending and p.readiness is None and p.readiness_wakes == 0
    entry = h.admission.queue_entry(P)
    assert entry is not None and entry.status == QueueStatus.QUEUED
    assert entry.approval_id == approval
    auth = p.authorizations[-1]
    assert auth.kind == SessionKind.BUILD and auth.rework and auth.approval_id == approval


# ------------------------------------------------------------------ triggers


def test_owner_issue_comment_on_ready_starts_rework_in_the_same_session():
    h = ready()
    old = h.cur()
    approval = h.p().current_approval_id
    r = comment(h)
    assert r.audit.accepted
    assert_rework_queued(h, approval)
    [move] = Harness.of(r, EffectKind.MOVE_CARD)
    assert move.args == {"to": "Building", "expected_from": "Ready"}
    assert h.p().bot == BotState.QUEUED
    # A free build slot admits it: same issue session (reused root), Bot Working.
    h.send(P, ev.CapacityAvailable())
    run = h.cur()
    assert run.kind == SessionKind.BUILD and run.session_id != old.session_id
    assert run.root_id == old.root_id and run.lifecycle == Lifecycle.PREPARING
    assert h.p().note == "Rework: owner feedback"
    run = h.create_ok()
    assert run.lifecycle == Lifecycle.ACTIVE and h.p().bot == BotState.WORKING
    assert h.p().note == "Rework: owner feedback"
    # A fresh checkpoint time block (the plan's size block), not the old run's leftover.
    assert run.grant.duration_us == h.cfg.block_us(h.p().size)
    assert run.grant.grant_id != old.grant.grant_id


def test_owner_pr_comment_and_pr_review_start_rework():
    for text in ("PR conversation comment", "review: request changes"):
        h = ready()
        approval = h.p().current_approval_id
        r = comment(h, text, pr=7)
        assert r.audit.accepted
        assert_rework_queued(h, approval)


def test_owner_drag_ready_to_building_starts_rework():
    h = ready()
    approval = h.p().current_approval_id
    r = h.send(P, ev.LeftwardMove(from_stage=Stage.READY, to_stage=Stage.BUILDING), actor=OWNER_ID)
    assert r.audit.accepted
    assert_rework_queued(h, approval)
    assert not Harness.of(r, EffectKind.MOVE_CARD)  # the owner already moved the card
    assert not h.p().holds & {Hold.SAFETY, Hold.UNSUPPORTED_REWORK}


def test_request_rework_control_starts_rework():
    h = ready()
    approval = h.p().current_approval_id
    assert h.send(P, ev.RequestRework()).audit.accepted
    assert_rework_queued(h, approval)


# ------------------------------------------------------------------ non-triggers


def test_non_owner_and_bot_comments_never_start_rework():
    for actor in (OTHER_USER_ID, 334191208):
        h = ready()
        r = comment(h, actor=actor)
        assert not r.audit.accepted and h.p().stage == Stage.READY
        assert h.admission.queue_entry(P).status != QueueStatus.QUEUED


def test_approval_and_observations_on_ready_are_not_rework():
    h = ready()
    # An approval with no text reaches the reducer as an observation, never feedback.
    r = h.send(P, ev.ReviewChanged(pr_number=7, head_sha=HEAD), actor=OWNER_ID)
    assert h.p().stage == Stage.READY and EffectKind.MOVE_CARD not in kinds(r)
    assert h.admission.queue_entry(P).status != QueueStatus.QUEUED


def test_stopped_or_merged_ready_card_is_not_reworked_by_a_comment():
    h = ready()
    h.send(P, ev.Stop())
    r = comment(h)
    assert r.audit.accepted and h.p().stage == Stage.READY  # recorded only
    assert h.admission.queue_entry(P).status != QueueStatus.QUEUED
    h = ready()
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
    r = comment(h)
    assert h.p().stage == Stage.READY and Hold.COMPLETED in h.p().holds
    assert h.admission.queue_entry(P).status != QueueStatus.QUEUED


# ------------------------------------------------------------------ debounce


def test_a_burst_of_feedback_is_one_rework():
    h = ready()
    comment(h, "one")
    auths = len(h.p().authorizations)
    for text, pr in (("two", 0), ("review with 9 inline comments", 7), ("three", 7)):
        r = comment(h, text, pr=pr)
        assert r.audit.accepted and EffectKind.WAKE_SCHEDULER not in kinds(r)
    assert len(h.p().authorizations) == auths  # still one queued rework
    assert [q.parcel_id for q in h.admission.queue if q.status == QueueStatus.QUEUED] == [P]
    h.send(P, ev.CapacityAvailable())
    run = h.create_ok()
    # Mid-turn: recorded only (the MCP gate makes the run read it before build_ready).
    r = comment(h, "four")
    assert r.audit.accepted and not Harness.of(r, EffectKind.SEND_MESSAGE)
    assert h.cur().session_id == run.session_id and len(h.p().authorizations) == auths


# ------------------------------------------------------------------ slot queuing


def test_rework_waits_for_a_build_slot():
    h = Harness(cfg=config(max_building=1))
    ready(h)
    other = "I_parcel_2"
    h.to_building(other)  # holds the only build slot
    comment(h)
    assert h.p().bot == BotState.QUEUED
    r = h.send(P, ev.CapacityAvailable())
    assert not r.audit.accepted and r.audit.reason == "building-cap"
    assert h.p().stage == Stage.BUILDING and h.cur().lifecycle == Lifecycle.RETIRED
    h.send(other, ev.Stop())
    h.quiesce(other, h.cur(other).session_id)
    assert h.send(P, ev.CapacityAvailable()).audit.accepted
    assert h.cur().kind == SessionKind.BUILD and h.cur().lifecycle == Lifecycle.PREPARING


# ------------------------------------------------------------------ budget, report


def _not_ready(h: Harness, head: str = HEAD):
    s = h.cur()
    return h.send(
        P,
        ev.ReadinessEvidence(
            session_id=s.session_id,
            pr_number=7,
            head_sha=head,
            verified=False,
            checks=ev.ChecksState.FAILED,
            checks_summary="lint failed",
        ),
    )


def _submit(h: Harness, head: str) -> None:
    s = h.cur()
    assert s.root_id is not None
    h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha=head,
        ),
    )
    h.quiesce(P, s.session_id)


def test_rework_resets_the_fix_budget_and_ends_with_a_new_ready_report():
    h = ready()
    h.parcels[P] = replace(h.p(), readiness_wakes=1)  # wake used
    comment(h)
    assert h.p().readiness_wakes == 0
    h.send(P, ev.CapacityAvailable())
    h.create_ok()
    _submit(h, HEAD2)
    r = _not_ready(h, HEAD2)
    [wake] = Harness.of(r, EffectKind.SEND_MESSAGE)
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value  # the fresh wake
    _submit(h, HEAD2)
    s = h.cur()
    r = h.send(
        P, ev.ReadinessEvidence(session_id=s.session_id, pr_number=7, head_sha=HEAD2, verified=True)
    )
    assert h.p().stage == Stage.READY and h.p().readiness.head_sha == HEAD2
    [report] = Harness.of(r, EffectKind.PUBLISH_REPORT)
    assert report.args["report"] == "ready" and report.args["head_sha"] == HEAD2


# ------------------------------------------------------------------ #477 shape


def needs_you_after_budget(h: Harness) -> Harness:
    """#477: Ready withdrawn to Building, the build run closed, then Needs you with no fix
    attempt left ("PR not ready ... no fix attempt left")."""
    ready(h)
    h.send(P, ev.ReviewChanged(pr_number=7, head_sha=HEAD, changes_requested=True))
    _not_ready(h)
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.NEEDS_YOU
    assert {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED} <= p.holds
    assert h.cur().lifecycle == Lifecycle.RETIRED and h.cur().execution_closed
    return h


def test_477_owner_comment_on_building_needs_you_restarts_the_build_loop():
    h = needs_you_after_budget(Harness())
    old = h.cur()
    approval = h.p().current_approval_id
    r = comment(h, "don't do that, do X instead")
    assert r.audit.accepted
    assert_rework_queued(h, approval)
    assert not h.p().holds & {Hold.READINESS_FAILED, Hold.REWORK_CONTROL_REQUIRED}
    assert not Harness.of(r, EffectKind.MOVE_CARD)  # already in Building
    h.send(P, ev.CapacityAvailable())
    run = h.create_ok()
    assert run.session_id != old.session_id and run.root_id == old.root_id
    assert run.lifecycle == Lifecycle.ACTIVE and h.p().bot == BotState.WORKING


def test_477_shape_with_a_safety_hold_still_reworks():
    h = needs_you_after_budget(Harness())
    h.parcels[P] = replace(h.p(), holds=h.p().holds | {Hold.SAFETY})
    comment(h)
    assert_rework_queued(h, h.p().current_approval_id)
    assert Hold.SAFETY not in h.p().holds
