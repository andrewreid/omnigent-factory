"""Recheck r4: F1 owner drag matching a daemon target; F2 safety clears queued writes."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import SessionKind, Stage, Via
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, snapshot
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def land(h: Harness):
    """Trusted executor success for the in-flight write; returns effects of that event."""
    [m] = h.p().pending_moves
    return h.send(
        P,
        ev.ColumnObserved(stage=m.to_stage, daemon_effect_id=m.effect_id),
        provenance=Provenance.ADAPTER,
    )


def test_F1_owner_drag_to_queued_daemon_target_keeps_replan_authority():
    h = Harness()
    b = h.to_building()
    h.build_ready()
    h.auto_ack_moves = False
    h.send(
        P, ev.ReadinessEvidence(session_id=b.session_id, pr_number=7, head_sha=HEAD, verified=False)
    )  # D2: Ready -> Building in flight
    approval = h.p().current_approval_id
    h.send(P, ev.ApprovalInvalidated(approval_id=approval, reason="x"))  # queues Scoped
    assert h.p().queued_move == Stage.SCOPED
    r = h.send(P, ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED), actor=OWNER_ID)
    assert r.audit.accepted
    assert h.p().pending_authorization_id is not None  # owner-positive half recorded
    for _ in range(3):  # both trusted outcomes, serialised
        if not h.p().pending_moves:
            break
        land(h)
    p = h.p()
    assert p.stage == Stage.SCOPED and not p.pending_moves and p.queued_move is None
    assert h.cur().kind == SessionKind.PLAN  # successor authorised and started


def test_F1_non_owner_move_to_daemon_target_is_only_consistency():
    h = Harness(auto_ack_moves=False)
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.COMMAND))
    h.send(P, ev.RequestPlan(via=Via.COMMAND))  # Scoped queued
    before = h.p()
    h.send(
        P, ev.LeftwardMove(from_stage=Stage.BUILDING, to_stage=Stage.SCOPED), actor=OTHER_USER_ID
    )
    assert h.p().barrier_time_us == before.barrier_time_us
    assert h.p().queued_move == Stage.SCOPED


def queued_setup():
    h = Harness(auto_ack_moves=False)
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.COMMAND))  # Inbox -> Triaged in flight
    h.send(P, ev.RequestPlan(via=Via.COMMAND))  # Scoped queued
    assert h.p().queued_move == Stage.SCOPED and len(h.p().pending_moves) == 1
    return h


@pytest.mark.parametrize(
    "safety",
    [
        lambda h: h.send(P, ev.AssignedHuman(), actor=OTHER_USER_ID),
        lambda h: h.send(P, ev.Closed(), actor=OTHER_USER_ID),
        lambda h: h.send(P, ev.Deleted(), actor=OTHER_USER_ID),
        lambda h: h.send(P, ev.Transferred(), actor=OTHER_USER_ID),
        lambda h: h.send(P, ev.ItemRemoved(), actor=OTHER_USER_ID),
        lambda h: h.send(
            P,
            ev.GitHubSnapshot(),
            evidence=snapshot(human_assigned=True),
            provenance=Provenance.RECONCILER,
        ),
        lambda h: h.send(P, ev.Stop()),
    ],
    ids=[
        "assigned",
        "closed",
        "deleted",
        "transferred",
        "item-removed",
        "ineligible-read",
        "owner-stop",
    ],
)
def test_F2_safety_drops_queued_board_write(safety):
    h = queued_setup()
    safety(h)
    assert h.p().queued_move is None
    r = land(h)  # the already in-flight write is not cancelled, just not followed up
    assert EffectKind.MOVE_CARD not in [e.kind for e in r.effects]
    assert not h.p().pending_moves and h.p().queued_move is None
