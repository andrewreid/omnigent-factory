"""#729: an owner drag racing a reconcile read of the same column change.

Live order (2026-10-08): a reconcile read started while the card was in Inbox, the owner
dragged it to Building, the read landed first and recorded Inbox -> Building as a plain
observation, then the drag (a waiver) was refused as Building -> Building. The card sat
in Building with no authority, no session and no explanation.
"""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.projection import unexplained_move_note
from omnigent_factory.core.types import (
    ApprovalKind,
    BotState,
    Lifecycle,
    QueueStatus,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.service.auto_triage import busy_reason
from omnigent_factory.testing.builders import OWNER_ID, snapshot
from omnigent_factory.testing.harness import Harness


def in_column(h: Harness, pid: str, stage: Stage) -> None:
    f = h.f(pid)
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, stage=stage)))
    assert h.p(pid).stage == stage


def read_first_then(h: Harness, pid: str, to: Stage, drag: ev.EventBody, **kw: object):
    """The reconcile read lands first; the owner's drag (made earlier) arrives after it."""
    f = h.f(pid)
    dragged_at = f.tick()
    read = h.apply(
        f.make(
            ev.GitHubSnapshot(),
            provenance=Provenance.RECONCILER,
            evidence=snapshot(read_at_us=f.tick(), stage=to),
        )
    )
    assert read.audit.accepted and h.p(pid).stage == to
    evidence = (
        None if isinstance(drag, ev.LeftwardMove) else snapshot(read_at_us=f.tick(), stage=to)
    )
    return h.apply(f.make(drag, time_us=dragged_at, evidence=evidence, **kw))  # type: ignore[arg-type]


def test_729_read_first_then_waiver_drag_builds():
    h = Harness()
    in_column(h, "A", Stage.INBOX)
    r = read_first_then(h, "A", Stage.BUILDING, ev.WaivePlan(via=Via.DRAG, board_from=Stage.INBOX))
    assert r.audit.accepted, r.audit.reason
    p = h.p("A")
    assert p.stage == Stage.BUILDING and p.observed_move is None
    assert p.current_approval is not None and p.current_approval.kind == ApprovalKind.SKIP
    entry = h.admission.queue_entry("A")
    assert entry is not None and entry.status == QueueStatus.QUEUED
    build = h.admit("A")  # normal waiver rules from here: admitted, the build runs
    assert build.kind == SessionKind.BUILD and build.lifecycle == Lifecycle.ACTIVE
    assert h.admission.building_count == 1


def test_a_read_alone_grants_no_authority_and_flags_the_card():
    """The #729 card before the drag: Building, Idle, no session, no slot, a clear note."""
    h = Harness()
    in_column(h, "A", Stage.INBOX)
    f = h.f("A")
    h.apply(
        f.make(
            ev.GitHubSnapshot(),
            provenance=Provenance.RECONCILER,
            evidence=snapshot(read_at_us=f.tick(), stage=Stage.BUILDING),
        )
    )
    p = h.p("A")
    assert p.stage == Stage.BUILDING and p.bot == BotState.IDLE
    assert not p.sessions and not p.approvals and p.current_approval_id is None
    assert h.admission.queue_entry("A") is None and h.admission.building_count == 0
    assert not h.admission.reservations
    assert p.board_note == unexplained_move_note(Stage.BUILDING)
    assert busy_reason([p]) is None  # holds nothing, runs nothing


@pytest.mark.parametrize(
    ("origin", "to", "drag", "kind"),
    [
        (Stage.INBOX, Stage.TRIAGED, ev.RequestTriage, SessionKind.TRIAGE),
        (Stage.INBOX, Stage.SCOPED, ev.RequestPlan, SessionKind.PLAN),
    ],
)
def test_read_first_then_triage_or_plan_drag_starts_the_run(origin, to, drag, kind):
    h = Harness()
    in_column(h, "A", origin)
    r = read_first_then(h, "A", to, drag(via=Via.DRAG, board_from=origin))
    assert r.audit.accepted, r.audit.reason
    assert h.cur("A").kind == kind and h.p("A").observed_move is None


def test_read_first_then_approval_drag_queues_the_build():
    h = Harness()
    h.plan_published("A")
    r = read_first_then(
        h, "A", Stage.BUILDING, ev.ApprovePlan(via=Via.DRAG, board_from=Stage.SCOPED)
    )
    assert r.audit.accepted, r.audit.reason
    assert h.p("A").current_approval is not None
    assert h.admission.queue_entry("A") is not None


def test_read_first_then_leftward_owner_drag_still_replans():
    """A read showing Building -> Scoped first raises the barrier; the owner's drag that
    made the move is judged against the barrier before it, so the replan still starts."""
    h = Harness()
    h.to_building("A")
    r = read_first_then(
        h,
        "A",
        Stage.SCOPED,
        ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED),
        actor=OWNER_ID,
    )
    assert r.audit.accepted, r.audit.reason
    p = h.p("A")
    assert p.stage == Stage.SCOPED and p.current_approval_id is None and p.observed_move is None
    pending = p.authorization(p.pending_authorization_id)
    assert pending is not None and pending.kind == SessionKind.PLAN  # after the build drains


def test_a_leftward_read_does_not_freshen_an_unrelated_or_non_owner_drag():
    h = Harness()
    h.to_building("A")
    r = read_first_then(
        h,
        "A",
        Stage.SCOPED,
        ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED),
        actor=222,  # not an owner: safety half only
    )
    assert r.audit.accepted
    assert h.p("A").pending_authorization_id is None


def test_a_drag_from_another_column_is_still_judged_by_the_recorded_stage():
    h = Harness()
    in_column(h, "A", Stage.INBOX)
    # The read saw Inbox -> Building, but this drag claims Triaged -> Building: no match.
    r = read_first_then(
        h, "A", Stage.BUILDING, ev.WaivePlan(via=Via.DRAG, board_from=Stage.TRIAGED)
    )
    assert not r.audit.accepted and r.audit.reason == "waiver-stage-invalid"
    assert h.p("A").current_approval_id is None


def test_drag_before_the_read_needs_no_help_and_leaves_no_observed_move():
    h = Harness()
    in_column(h, "A", Stage.INBOX)
    f = h.f("A")
    r = h.apply(
        f.make(
            ev.WaivePlan(via=Via.DRAG, board_from=Stage.INBOX),
            evidence=snapshot(read_at_us=f.tick(), stage=Stage.BUILDING),
        )
    )
    assert r.audit.accepted, r.audit.reason
    p = h.p("A")
    assert p.observed_move is None and p.board_note != unexplained_move_note(Stage.BUILDING)
