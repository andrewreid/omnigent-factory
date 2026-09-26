"""Recheck r3 F1: out-of-order board-write outcomes must never regress the desired column.

Trace: Ready -> D2 invalidation (daemon write Ready->Building) -> owner /plan (daemon
write Building->Scoped). The executor then reports outcomes newest-first or oldest-first.
"""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import FenceKind, SessionKind, Stage, Via
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def issued_moves(h: Harness):
    return [e for _, result in h.log for e in result.effects if e.kind == EffectKind.MOVE_CARD]


def ack_all(h: Harness, *, newest_first: bool) -> None:
    """Report success for every issued, unacknowledged board write, in the given order,
    repeating until none remain (a serialised ledger issues the next one on retirement)."""
    acked: set[str] = set()
    for _ in range(10):
        todo = [m for m in issued_moves(h) if m.effect_id not in acked]
        if not todo:
            return
        for m in reversed(todo) if newest_first else todo:
            acked.add(m.effect_id)
            h.send(
                P,
                ev.ColumnObserved(stage=Stage(m.args["to"]), daemon_effect_id=m.effect_id),
                provenance=Provenance.ADAPTER,
            )


@pytest.mark.parametrize("newest_first", [True, False], ids=["reverse", "forward"])
def test_out_of_order_move_acks_keep_newest_desired_column(newest_first):
    h = Harness()
    b = h.to_building()
    h.build_ready()
    h.auto_ack_moves = False
    h.send(
        P, ev.ReadinessEvidence(session_id=b.session_id, pr_number=7, head_sha=HEAD, verified=False)
    )  # D2: Ready -> Building
    h.send(P, ev.RequestPlan(via=Via.COMMAND))  # owner: Building -> Scoped
    ack_all(h, newest_first=newest_first)
    p = h.p()
    assert p.stage == Stage.SCOPED and not p.pending_moves
    plan = h.cur()
    assert plan.kind == SessionKind.PLAN and not plan.fences
    barrier = p.barrier_time_us
    h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(stage=Stage.SCOPED),
        provenance=Provenance.RECONCILER,
    )  # correct board read
    p = h.p()
    assert p.barrier_time_us == barrier  # no spurious leftward safety fact
    assert FenceKind.SAFETY not in p.session(plan.session_id).fences
    assert p.stage == Stage.SCOPED


def test_queued_board_target_survives_codec_and_store_reopen(tmp_path):
    import json

    from omnigent_factory.core import codec
    from omnigent_factory.store.sqlite import SqliteStore
    from omnigent_factory.testing.fakes import FakeClock
    from tests.test_store import StoreHarness

    store = SqliteStore.open(tmp_path / "s.db", FakeClock())
    h = StoreHarness(store=store)
    store.ensure_repository(h.cfg)
    h.apply(h.f(P).make(ev.Unpause(), parcel_id=None))
    h.auto_ack_moves = False
    h.eligible(P)
    h.send(P, ev.RequestTriage(via=Via.COMMAND))  # write Inbox -> Triaged in flight
    h.send(P, ev.RequestPlan(via=Via.COMMAND))  # Scoped queued behind it
    p = h.p(P)
    assert len(p.pending_moves) == 1 and p.queued_move == Stage.SCOPED
    assert p.stage == Stage.SCOPED
    store.close()
    store = SqliteStore.open(tmp_path / "s.db", FakeClock())
    assert store.load_parcel(P) == p
    store.close()
    # Aggregates persisted before the field existed load with no queued target.
    data = json.loads(codec.parcel_to_json(p))
    del data["parcel"]["queued_move"]
    assert codec.parcel_from_json(json.dumps(data)).queued_move is None


def test_owner_drag_during_inflight_write_is_queued_not_lost():
    h = Harness(auto_ack_moves=False)
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.COMMAND))  # daemon write Inbox -> Triaged
    [inflight] = h.p().pending_moves
    h.send(P, ev.RequestPlan(via=Via.DRAG))  # owner drags to Scoped meanwhile
    p = h.p()
    assert p.stage == Stage.SCOPED and p.queued_move == Stage.SCOPED
    r = h.send(P, ev.ColumnObserved(stage=Stage.TRIAGED, daemon_effect_id=inflight.effect_id))
    [reissue] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert reissue.args == {"to": "Scoped", "expected_from": "Triaged"}
    assert h.p().stage == Stage.SCOPED and h.p().queued_move is None


def test_rightward_observation_during_pending_write_keeps_single_desired_target():
    # Seed-22 thorough counterexample: a snapshot showing Building while Triaged is in
    # flight and Scoped queued must not split the desired column from the queued target.
    h = Harness(auto_ack_moves=False)
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.COMMAND))
    h.send(P, ev.RequestPlan(via=Via.COMMAND))
    [inflight] = h.p().pending_moves
    h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(stage=Stage.BUILDING),
        provenance=Provenance.RECONCILER,
    )
    p = h.p()
    assert p.stage == Stage.SCOPED and p.queued_move == Stage.SCOPED
    h.send(P, ev.ColumnObserved(stage=Stage.TRIAGED, daemon_effect_id=inflight.effect_id))
    assert h.p().stage == Stage.SCOPED
