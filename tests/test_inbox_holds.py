"""Reducer rows for inbox holds (Task 5a; T4 recheck-2 FOLLOW_UP 1 and F-new-1).

A parked or unverified delivery for a parcel is treated as a possible safety fact: the
parcel's current tree is fenced (safety), interrupted and drained, queued authority is
cancelled and nothing dispatches while the hold remains. A release removes only the hold.
"""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.predicates import dispatchable
from omnigent_factory.core.types import (
    BotState,
    FenceKind,
    Hold,
    InboxHold,
    InboxHoldReason,
    Lifecycle,
    QueueStatus,
    Via,
)
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
Q = "I_parcel_2"
PARKED = InboxHoldReason.PARKED
UNRESOLVED = InboxHoldReason.UNRESOLVED


def hold(h: Harness, guid: str = "g1", reason: InboxHoldReason = PARKED, **kw: object):
    return h.send(P, ev.InboxHoldSet(delivery_guid=guid, reason=reason), **kw)


def release(h: Harness, guid: str = "g1", **kw: object):
    return h.send(P, ev.InboxHoldReleased(delivery_guid=guid), **kw)


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def active_triage(h: Harness):
    h.eligible(P)
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok(P)
    assert s.lifecycle == Lifecycle.ACTIVE
    return s


@pytest.mark.parametrize("reason", list(InboxHoldReason))
def test_hold_fences_interrupts_and_disables_running_tree(reason: InboxHoldReason):
    h = Harness()
    s = active_triage(h)
    r = hold(h, reason=reason)
    assert r.audit.accepted
    after = h.p(P).session(s.session_id)
    assert after is not None and FenceKind.SAFETY in after.fences
    assert after.lifecycle == Lifecycle.DRAINING
    assert {EffectKind.DISABLE_ISSUANCE, EffectKind.INTERRUPT_TREE, EffectKind.SCAN_TREE} <= set(
        kinds(r)
    )
    assert not [e for e in r.effects if e.kind in WORK_BEARING_KINDS]
    p = h.p(P)
    assert p.inbox_holds == (InboxHold("g1", reason),)
    assert Hold.INBOX in p.holds and not dispatchable(p)
    assert p.bot == BotState.BLOCKED


@pytest.mark.parametrize(
    "provenance",
    [p for p in Provenance if p != Provenance.INBOX],
)
def test_hold_is_admitted_only_from_the_inbox(provenance: Provenance):
    h = Harness()
    h.eligible(P)
    before = h.p(P)
    r = hold(h, provenance=provenance)
    assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"
    assert h.p(P) == before


def test_hold_cancels_queued_build_authority():
    h = Harness(auto_ack_moves=True)
    h.plan_published(P)
    h.approve(P)
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED  # type: ignore[union-attr]
    hold(h)
    assert h.admission.queue_entry(P).status == QueueStatus.CANCELLED  # type: ignore[union-attr]
    r = h.send(P, ev.CapacityAvailable())
    assert EffectKind.CREATE_SESSION not in kinds(r)


def test_no_dispatch_while_held_even_after_fresh_owner_control():
    h = Harness()
    s = active_triage(h)
    hold(h)
    h.quiesce(P, s.session_id)
    r = h.send(P, ev.RequestTriage(via=Via.LABEL))
    assert r.audit.accepted  # authority is recorded ...
    assert EffectKind.CREATE_SESSION not in kinds(r)  # ... but nothing dispatches
    assert h.p(P).pending_authorization_id is not None


def test_parked_release_requires_operator_and_restores_no_fence():
    h = Harness()
    s = active_triage(h)
    hold(h, reason=PARKED)
    for provenance in (Provenance.INBOX, Provenance.ADAPTER):
        r = release(h, provenance=provenance)
        assert not r.audit.accepted
        assert h.p(P).inbox_holds
    r = release(h, provenance=Provenance.OPERATOR)
    assert r.audit.accepted
    p = h.p(P)
    assert p.inbox_holds == () and Hold.INBOX not in p.holds
    old = p.session(s.session_id)
    assert old is not None and FenceKind.SAFETY in old.fences  # never cleared by release
    assert not [e for e in r.effects if e.kind in WORK_BEARING_KINDS]


def test_unresolved_release_requires_inbox():
    h = Harness()
    h.eligible(P)
    hold(h, reason=UNRESOLVED)
    assert not release(h, provenance=Provenance.OPERATOR).audit.accepted
    assert release(h, provenance=Provenance.INBOX).audit.accepted
    assert h.p(P).inbox_holds == ()


def test_unresolved_upgrades_to_parked_but_never_downgrades():
    h = Harness()
    h.eligible(P)
    hold(h, reason=UNRESOLVED)
    assert hold(h, reason=PARKED).audit.accepted
    assert h.p(P).inbox_holds == (InboxHold("g1", PARKED),)
    r = hold(h, reason=UNRESOLVED)
    assert not r.audit.accepted and r.audit.reason == "inbox-hold-already-set"
    # parked now needs the operator; the inbox cannot retire it
    assert not release(h, provenance=Provenance.INBOX).audit.accepted
    assert h.p(P).inbox_holds == (InboxHold("g1", PARKED),)


def test_release_unknown_hold_and_empty_guid_are_rejected():
    h = Harness()
    h.eligible(P)
    assert release(h, guid="nope").audit.reason == "inbox-hold-not-found"
    assert hold(h, guid="").audit.reason == "inbox-hold-without-delivery"


def test_two_holds_both_must_be_released():
    h = Harness()
    h.eligible(P)
    hold(h, guid="g1")
    hold(h, guid="g2", reason=UNRESOLVED)
    release(h, guid="g1", provenance=Provenance.OPERATOR)
    assert Hold.INBOX in h.p(P).holds and not dispatchable(h.p(P))
    release(h, guid="g2")
    assert Hold.INBOX not in h.p(P).holds


def test_after_release_fresh_owner_control_after_quiescence_dispatches():
    h = Harness()
    s = active_triage(h)
    hold(h)
    h.quiesce(P, s.session_id)
    release(h, provenance=Provenance.OPERATOR)
    r = h.send(P, ev.RequestTriage(via=Via.LABEL))
    assert r.audit.accepted
    assert EffectKind.CREATE_SESSION in kinds(r)


def test_pending_authority_recorded_during_hold_wakes_on_release():
    h = Harness()
    s = active_triage(h)
    hold(h)
    h.quiesce(P, s.session_id)
    h.send(P, ev.RequestTriage(via=Via.LABEL))
    r = release(h, provenance=Provenance.OPERATOR)
    # owner authority recorded after the barrier is legitimate once the hold is gone
    assert EffectKind.CREATE_SESSION in kinds(r)


def test_hold_is_parcel_scoped():
    h = Harness()
    active_triage(h)
    h.eligible(Q)
    h.send(Q, ev.RequestTriage(via=Via.DRAG))
    hold(h)
    q = h.p(Q)
    assert q.inbox_holds == () and dispatchable(q)
    assert q.current_session is not None and not q.current_session.fences
