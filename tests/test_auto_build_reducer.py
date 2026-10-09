"""Auto-build in the pure reducer: the owner's "Auto-build" mark, its start, lapse and
the manual-first, capped admission of auto-builds."""

from __future__ import annotations

from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import ADMISSION
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import EventKind, Provenance
from omnigent_factory.core.projection import queue_head, startable_build
from omnigent_factory.core.reducer import AUTO_BUILD_UNCONFIRMED_NOTE, PLAN_REVISED_NOTE
from omnigent_factory.core.types import (
    AutoBuildStatus,
    BotState,
    Hold,
    Lifecycle,
    QueueEntry,
    QueueStatus,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, config, snapshot
from omnigent_factory.testing.harness import Harness

A, B, C = "I_auto_a", "I_auto_b", "I_auto_c"


def planned(h: Harness, pid: str = A) -> None:
    """A card in Planning with a posted plan awaiting approval."""
    h.plan_published(pid)
    p = h.p(pid)
    assert p.stage == Stage.SCOPED and p.current_contract is not None
    assert p.current_contract.published


def mark(h: Harness, pid: str = A, option: str = "Queued", **kw: object):
    return h.send(pid, ev.AutoBuildMarked(option=option), **kw)


def auto_build(h: Harness, pid: str = A, **snap: object):
    f = h.f(pid)
    fields = {"stage": Stage.SCOPED, "read_at_us": f.now + 1, **snap}
    return h.apply(f.make(ev.AutoBuild(), evidence=snapshot(**fields)))  # type: ignore[arg-type]


def field_writes(result) -> list[object]:
    return [e.args["value"] for e in result.effects if e.kind == EffectKind.SET_AUTO_BUILD]


def started(h: Harness, pid: str = A) -> None:
    """Mark, start and admit an auto-build: its build run is live."""
    planned(h, pid)
    assert mark(h, pid).audit.accepted
    plan = h.cur(pid)
    r = auto_build(h, pid)
    assert r.audit.accepted, r.audit.reason
    h.quiesce(pid, plan.session_id)
    h.admit(pid)


# ------------------------------------------------------------------ admission table


def test_marks_only_from_an_owner_webhook_and_starts_only_from_the_clock():
    assert ADMISSION[EventKind.AUTO_BUILD_MARKED].provenances == frozenset(
        {Provenance.WEBHOOK, Provenance.RECOVERY}
    )
    assert ADMISSION[EventKind.AUTO_BUILD].provenances == frozenset({Provenance.SCHEDULER})
    h = Harness()
    planned(h)
    # A non-owner's Queued (or one from any other surface) approves nothing.
    r = mark(h, actor=OTHER_USER_ID)
    assert not r.audit.accepted and r.audit.reason == "control-from-non-owner"
    for provenance in (Provenance.RECONCILER, Provenance.ADAPTER, Provenance.SCHEDULER):
        r = mark(h, provenance=provenance, actor=OWNER_ID)
        assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"
    assert h.p(A).auto_build is None
    for provenance in (Provenance.WEBHOOK, Provenance.OPERATOR, Provenance.ADAPTER):
        r = h.send(A, ev.AutoBuild(), provenance=provenance, actor=OWNER_ID)
        assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"


# ------------------------------------------------------------------ mark and start


def test_owner_queued_is_bound_to_the_posted_plan_and_starts_when_a_slot_is_free():
    h = Harness()
    planned(h)
    contract = h.p(A).current_contract
    assert contract is not None
    r = mark(h)
    assert r.audit.accepted, r.audit.reason
    p = h.p(A)
    assert p.auto_build is not None and p.auto_build.status == AutoBuildStatus.QUEUED
    assert p.auto_build.full_hash == contract.full_hash
    assert p.auto_build.owner_id == OWNER_ID
    assert p.note == "Auto-build queued: starts when a build slot is free"
    assert field_writes(r) == []  # the owner put Queued there: nothing to write
    assert p.approvals == () and p.stage == Stage.SCOPED  # nothing starts on the mark

    plan = h.cur(A)
    r = auto_build(h)
    assert r.audit.accepted, r.audit.reason
    p = h.p(A)
    # The owner's approval of exactly that plan, sourced from their mark.
    approval = p.current_approval
    assert approval is not None and approval.full_hash == contract.full_hash
    assert approval.owner_id == OWNER_ID
    assert approval.source_event_id == p.auto_build.source_event_id  # type: ignore[union-attr]
    assert p.auto_build is not None and p.auto_build.status == AutoBuildStatus.STARTED
    assert field_writes(r) == ["Started"]
    [move] = h.of(r, EffectKind.MOVE_CARD)
    assert move.args["to"] == Stage.BUILDING.value
    entry = h.admission.queue_entry(A)
    assert entry is not None and entry.auto and entry.status == QueueStatus.QUEUED
    assert p.stage == Stage.BUILDING
    # Then an ordinary build: the plan run drains, capacity admits it.
    h.quiesce(A, plan.session_id)
    s = h.admit(A)
    assert s.kind == SessionKind.BUILD and s.lifecycle == Lifecycle.ACTIVE
    assert h.admission.auto_build_count == 1
    assert h.p(A).bot == BotState.WORKING


@pytest.mark.parametrize("stage", ["inbox", "triage"])
def test_queued_outside_planning_or_without_a_plan_is_cleared_with_a_note(stage: str):
    h = Harness()
    h.eligible(A)
    if stage == "triage":
        h.triage(A)
        assert h.p(A).stage == Stage.TRIAGED
    r = mark(h)
    assert r.audit.accepted  # the field change is taken; the mark is refused
    p = h.p(A)
    assert p.auto_build is None
    assert field_writes(r) == [""]
    assert p.note.startswith("Auto-build cleared: only a card in Planning")
    assert not auto_build(h).audit.accepted


def test_queued_while_the_plan_is_still_being_written_is_cleared():
    h = Harness()
    h.eligible(A)
    h.send(A, ev.RequestPlan(via=Via.DRAG))
    h.create_ok(A)  # the plan run is working: nothing posted yet
    r = mark(h)
    assert h.p(A).auto_build is None and field_writes(r) == [""]
    assert h.p(A).note == "Auto-build cleared: there is no posted plan to approve"


def test_owner_started_is_not_an_approval():
    h = Harness()
    planned(h)
    r = mark(h, option="Started")
    assert h.p(A).auto_build is None and field_writes(r) == [""]
    assert "Started is set by the factory" in h.p(A).note
    assert not auto_build(h).audit.accepted


# ------------------------------------------------------------------ lapse


@pytest.mark.parametrize("revise", ["comment", "plan", "replan"])
def test_a_plan_revision_after_the_mark_lapses_it(revise: str):
    h = Harness()
    planned(h)
    mark(h)
    body: ev.EventBody = {
        "comment": ev.PlanFeedback(text_digest="d"),
        "plan": ev.RequestPlan(via=Via.COMMAND),
        "replan": ev.RequestReplan(via=Via.COMMAND),
    }[revise]
    r = h.send(A, body)
    assert r.audit.accepted, r.audit.reason
    p = h.p(A)
    assert p.auto_build is None
    assert field_writes(r) == [""]
    assert p.note == PLAN_REVISED_NOTE or revise != "comment"
    # A build never runs a plan the owner has not seen: the start is refused.
    r = auto_build(h)
    assert not r.audit.accepted and r.audit.reason == "no-auto-build-mark"
    assert h.p(A).approvals == ()


def test_a_new_plan_posted_after_the_mark_lapses_it():
    h = Harness()
    planned(h)
    mark(h)
    h.send(A, ev.PlanFeedback(text_digest="d"))  # revision requested ...
    h.publish_plan(A, goal="Ship it differently")  # ... and a new plan posted
    assert h.p(A).auto_build is None
    assert h.p(A).auto_build_field == ""
    # Re-queuing approves the new plan.
    assert mark(h).audit.accepted
    new = h.p(A).current_contract
    assert new is not None and h.p(A).auto_build is not None
    assert h.p(A).auto_build.full_hash == new.full_hash  # type: ignore[union-attr]


def test_a_stop_or_leftward_drag_after_the_mark_lapses_it():
    h = Harness()
    planned(h)
    mark(h)
    r = h.send(A, ev.Stop())
    assert h.p(A).auto_build is None and field_writes(r) == [""]
    h2 = Harness()
    planned(h2)
    mark(h2)
    h2.send(A, ev.LeftwardMove(from_stage=Stage.SCOPED, to_stage=Stage.TRIAGED))
    assert h2.p(A).auto_build is None and h2.p(A).auto_build_field == ""


def test_an_open_question_blocks_the_start_until_it_is_answered():
    h = Harness()
    planned(h)
    s = h.cur(A)
    h.send(A, ev.OwnerQuestion(session_id=s.session_id, question_key="q-1", summary="Which?"))
    assert h.p(A).open_decisions
    r = mark(h)
    assert r.audit.accepted and h.p(A).auto_build is not None  # queued, waiting
    assert h.p(A).note == "Auto-build queued: starts once your open question is answered"
    r = auto_build(h)
    assert not r.audit.accepted and r.audit.reason == "open-decisions"
    assert h.p(A).auto_build is not None  # still queued
    # The owner's answer (a plain comment) goes to the plan run, which re-posts the plan:
    # the mark waits for it and holds while the plan is unchanged.
    h.send(A, ev.PlanFeedback(text_digest="answer"))
    assert not h.p(A).open_decisions and h.p(A).auto_build is not None
    assert not auto_build(h).audit.accepted  # the plan is being re-posted
    h.publish_plan(A)  # the same plan
    assert h.p(A).auto_build is not None
    r = auto_build(h)
    assert r.audit.accepted, r.audit.reason


def test_an_answer_that_changes_the_plan_lapses_the_mark():
    h = Harness()
    planned(h)
    s = h.cur(A)
    h.send(A, ev.OwnerQuestion(session_id=s.session_id, question_key="q-1", summary="Which?"))
    mark(h)
    h.send(A, ev.PlanFeedback(text_digest="answer"))
    assert h.p(A).auto_build is not None
    h.publish_plan(A, goal="Ship the other option")
    assert h.p(A).auto_build is None
    assert h.p(A).note == PLAN_REVISED_NOTE
    # A further owner comment (not an answer) while waiting lapses it at once.
    h2 = Harness()
    planned(h2)
    s = h2.cur(A)
    h2.send(A, ev.OwnerQuestion(session_id=s.session_id, question_key="q-1", summary="?"))
    mark(h2)
    h2.send(A, ev.PlanFeedback(text_digest="answer"))
    h2.send(A, ev.PlanFeedback(text_digest="and also change X"))
    assert h2.p(A).auto_build is None


# ------------------------------------------------------------------ clearing


def test_clearing_before_the_start_dequeues_and_after_the_start_changes_nothing():
    h = Harness()
    planned(h)
    mark(h)
    r = mark(h, option="")
    assert r.audit.accepted and h.p(A).auto_build is None
    assert field_writes(r) == []  # the owner emptied it already
    assert not auto_build(h).audit.accepted
    h2 = Harness()
    started(h2)
    r = mark(h2, option="")
    p = h2.p(A)
    assert p.auto_build is not None and p.auto_build.status == AutoBuildStatus.STARTED
    assert h2.cur(A).kind == SessionKind.BUILD and not h2.cur(A).fences  # still building
    assert p.current_approval is not None and p.current_approval.valid


def test_a_read_showing_the_field_cleared_dequeues_a_mark_whose_webhook_was_lost():
    h = Harness()
    planned(h)
    mark(h)
    f = h.f(A)
    h.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(stage=Stage.SCOPED, auto_build="", read_at_us=f.now + 1),
        )
    )
    assert h.p(A).auto_build is None
    assert not auto_build(h).audit.accepted


# ------------------------------------------------------------------ lost webhook


def test_queued_seen_only_by_a_read_is_not_acted_on_and_noted_once():
    h = Harness()
    planned(h)
    f = h.f(A)
    r = h.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(stage=Stage.SCOPED, auto_build="Queued", read_at_us=f.now + 1),
        )
    )
    p = h.p(A)
    assert p.auto_build is None and p.note == AUTO_BUILD_UNCONFIRMED_NOTE
    assert field_writes(r) == []  # not cleared, not started: the owner re-selects it
    assert not auto_build(h).audit.accepted
    # The next read of the same value changes nothing (no loop of notes or writes).
    version = h.p(A).version
    r = h.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(stage=Stage.SCOPED, auto_build="Queued", read_at_us=f.now + 1),
        )
    )
    assert field_writes(r) == [] and h.of(r, EffectKind.SET_NOTE) == []
    assert h.p(A).auto_build is None and h.p(A).version == version + 1
    # Re-selecting it (the owner's webhook) is the approval.
    mark(h, option="")
    assert mark(h).audit.accepted and h.p(A).auto_build is not None


# ------------------------------------------------------------------ admission


def test_a_manually_approved_build_goes_first():
    h = Harness(cfg=config(max_building=2, auto_build_concurrency=2))
    planned(h, A)
    mark(h, A)
    planned(h, B)
    h.send(B, ev.ApprovePlan(via=Via.COMMAND))  # manual, queued (plan not drained yet)
    assert h.admission.queue_entry(B) is not None
    # An auto-build never starts while a manually approved build waits.
    r = auto_build(h, A)
    assert not r.audit.accepted and r.audit.reason == "auto-build-behind-queued-build"
    # And in the queue a manual entry is admitted before an older auto entry.
    auto = QueueEntry(C, "ap_c", 1, QueueStatus.QUEUED, auto=True)
    manual = QueueEntry(B, "ap_b", 5, QueueStatus.QUEUED)
    snapshot_ = replace(h.admission, queue=(auto, manual))
    assert queue_head(snapshot_) == manual
    assert startable_build(snapshot_, h.cfg) == manual


def test_auto_builds_are_capped_within_max_building():
    h = Harness(cfg=config(max_building=3, auto_build_concurrency=1))
    started(h, A)
    assert h.admission.auto_build_count == 1 and h.admission.building_count == 1
    planned(h, B)
    mark(h, B)
    r = auto_build(h, B)
    assert not r.audit.accepted and r.audit.reason == "auto-build-cap"
    # A queued auto entry is not admitted beyond the cap either (a manual one would be).
    head = QueueEntry(C, "ap_c", 9, QueueStatus.QUEUED, auto=True)
    capped = replace(h.admission, queue=(*h.admission.queue, head))
    assert startable_build(capped, h.cfg) is None
    manual = replace(head, auto=False)
    assert (
        startable_build(replace(h.admission, queue=(*h.admission.queue, manual)), h.cfg) == manual
    )
    # The auto-build counts towards max_building like any build.
    h2 = Harness(cfg=config(max_building=1, auto_build_concurrency=1))
    h2.to_building(C)  # a manual build holds the only slot
    planned(h2, B)
    mark(h2, B)
    r = auto_build(h2, B)
    assert not r.audit.accepted and r.audit.reason == "building-cap"


def test_no_double_start():
    h = Harness()
    planned(h)
    mark(h)
    assert auto_build(h).audit.accepted
    r = auto_build(h)
    assert not r.audit.accepted and r.audit.reason == "no-auto-build-mark"
    assert len(h.p(A).approvals) == 1
    assert len([a for a in h.p(A).authorizations if a.kind == SessionKind.BUILD]) == 1


# ------------------------------------------------------------------ finish


def test_done_or_closed_clears_the_field():
    h = Harness()
    started(h)
    h.build_ready(A)
    f = h.f(A)
    r = h.apply(f.make(ev.Closed(), evidence=snapshot(open=False, read_at_us=f.now + 1)))
    p = h.p(A)
    assert Hold.COMPLETED in p.holds
    assert field_writes(r) == [""] and p.auto_build_field == ""
    assert p.auto_build is not None and p.auto_build.status == AutoBuildStatus.STARTED
    # A queued mark on a closed issue is dropped with the field.
    h2 = Harness()
    planned(h2)
    mark(h2)
    f2 = h2.f(A)
    r = h2.apply(f2.make(ev.Closed(), evidence=snapshot(open=False, read_at_us=f2.now + 1)))
    assert h2.p(A).auto_build is None and field_writes(r) == [""]
