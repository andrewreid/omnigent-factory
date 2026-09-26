"""Reviewer r2 direct traces (checkpoint-df49c796-r2), encoded as regressions.

Written against the event API of the reviewed tree so each can be shown failing there.
"""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import FenceKind, Stage, Via
from omnigent_factory.testing.builders import OTHER_USER_ID, snapshot
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def harness() -> Harness:
    h = Harness()
    h.auto_ack_moves = False  # play executor outcomes explicitly
    return h


def work(r):
    return [e for e in r.effects if e.kind in WORK_BEARING_KINDS]


def kinds(r):
    return [e.kind for e in r.effects]


def issued_message(h, session_id):
    for _, result in reversed(h.log):
        for e in result.effects:
            if e.kind == EffectKind.SEND_MESSAGE and e.preconditions.session_id == session_id:
                return e.effect_id
    raise AssertionError("no message issued")


def approve_by_command(h):
    """Plan published, owner /approve: the daemon writes Scoped -> Building (pending)."""
    h.plan_published()
    plan = h.cur()
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND))
    h.quiesce(P, plan.session_id)
    [move] = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    return move


def test_control_snapshot_continue():
    h = harness()
    b = h.to_building()
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    # Lost leftward webhook; the owner's /continue carries a fresh read showing Scoped.
    r = h.send(P, ev.Continue(), evidence=snapshot(stage=Stage.SCOPED))
    assert not r.audit.accepted
    s = h.p().session(b.session_id)
    assert {FenceKind.SAFETY, FenceKind.REVOKED} <= s.fences
    assert h.p().stage == Stage.SCOPED and h.p().current_approval_id is None
    r = h.send(P, ev.PolicyReady(session_id=b.session_id, grant_id=h.cur().grant.grant_id))
    assert not work(r)


def test_pending_source_mask():
    h = harness()
    approve_by_command(h)
    h.send(
        P,
        ev.GitHubSnapshot(),
        evidence=snapshot(stage=Stage.SCOPED),
        provenance=Provenance.RECONCILER,
    )
    r = h.send(P, ev.CapacityAvailable())
    assert EffectKind.CREATE_SESSION not in kinds(r)  # work gated while unresolved
    assert h.admission.building_count == 0


def test_forged_move_ack():
    h = harness()
    move = approve_by_command(h)
    for stage in (Stage.BUILDING, Stage.SCOPED):
        h.send(
            P,
            ev.ColumnObserved(stage=stage, daemon_effect_id=move.effect_id),
            provenance=Provenance.WEBHOOK,
            actor=OTHER_USER_ID,
        )
        assert [m.effect_id for m in h.p().pending_moves] == [move.effect_id]
    r = h.send(P, ev.CapacityAvailable())
    assert EffectKind.CREATE_SESSION not in kinds(r) and h.admission.building_count == 0


def test_move_reconciled_absent():
    h = harness()
    move = approve_by_command(h)
    approval = h.p().current_approval_id
    barrier = h.p().barrier_time_us
    h.send(P, ev.EffectUnknown(effect_id=move.effect_id, effect_kind="move_card"))
    h.send(P, ev.EffectReconciled(effect_id=move.effect_id, delivered=False))
    p = h.p()
    assert p.pending_moves == ()
    # The card is still in Scoped: reconciled as a leftward safety fact.
    assert p.stage == Stage.SCOPED and p.barrier_time_us > barrier
    assert not p.approval(approval).valid and p.current_approval_id is None
    r = h.send(P, ev.CapacityAvailable())
    assert EffectKind.CREATE_SESSION not in kinds(r)


def test_forged_message_ack():
    h = harness()
    plan = h.plan_published()
    lost = issued_message(h, plan.session_id)
    h.send(
        P, ev.EffectUnknown(effect_id=lost, effect_kind="send_message", session_id=plan.session_id)
    )
    r = h.send(
        P,
        ev.MessageAck(session_id=plan.session_id, effect_id=lost, item_id="forged"),
        provenance=Provenance.WEBHOOK,
        actor=OTHER_USER_ID,
    )
    assert not r.audit.accepted
    assert h.cur().message_unknown and "forged" not in h.cur().own_items
    r = h.send(P, ev.PlanFeedback(text_digest="more"))
    assert not work(r)
