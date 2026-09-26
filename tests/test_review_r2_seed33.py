"""Deterministic reproductions of the two thorough-profile (seed 33) counterexamples.

Both came from ``_apply_evidence`` applying the waiver-text check before the observed
column: the waiver invalidation issued the daemon's own move to Scoped, and the column
check then mistook the observed (lost) leftward move for that pending write.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import FenceKind, Stage, Via
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def test_edited_title_snapshot_with_leftward_column_fences_and_advances_barrier():
    h = Harness()
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.DRAG), evidence=snapshot(title="T", body="B"))
    build = h.admit()
    assert h.p().stage == Stage.BUILDING
    event = h.f().make(
        ev.GitHubSnapshot(),
        evidence=snapshot(stage=Stage.SCOPED, title="T2", body="B"),
        provenance=Provenance.RECONCILER,
    )
    h.apply(event)
    p = h.p()
    assert p.barrier_time_us >= event.source_time_us
    assert {FenceKind.SAFETY, FenceKind.REVOKED} <= p.session(build.session_id).fences
    assert p.stage == Stage.SCOPED and p.current_approval_id is None


def test_control_carrying_leftward_column_and_edited_title_is_not_accepted():
    # Seed-33 trace: waiver by label (daemon card write left unacknowledged), then an
    # owner /stop whose fresh read shows the card in Scoped and an edited title.
    h = Harness(auto_ack_moves=False)
    h.eligible()
    h.send(P, ev.WaivePlan(via=Via.LABEL), evidence=snapshot(title="T", body="B"))
    assert h.p().stage == Stage.BUILDING
    event = h.f().make(ev.Stop(), evidence=snapshot(stage=Stage.SCOPED, title="T2", body="B"))
    r = h.apply(event)
    assert not r.audit.accepted  # the control cannot pass a leftward observation
    assert h.p().barrier_time_us >= event.source_time_us
    assert not [e for e in r.effects if e.kind in WORK_BEARING_KINDS]
