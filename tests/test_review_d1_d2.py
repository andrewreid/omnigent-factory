"""Regressions for D1 (fenced build re-opened by external activity) and D2 (Ready kept
after evidence falsified one of its preconditions)."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind
from omnigent_factory.core.types import (
    FenceKind,
    Hold,
    Lifecycle,
    ReservationKind,
    SessionKind,
    Stage,
)
from omnigent_factory.testing.harness import HEAD, Harness

P = "I_parcel_1"


def building_slot(h: Harness) -> bool:
    return any(r.live and r.kind == ReservationKind.BUILDING for r in h.admission.reservations)


def fenced_released_build(h: Harness):
    """Revoked build, quiesced (slot released) while the replan waits on an ambiguity."""
    b = h.to_building()
    sent = [e for e, sid in h.p().sent_effects if sid == b.session_id][-1]
    h.send(P, ev.EffectUnknown(effect_id=sent, effect_kind="send_message", session_id=b.session_id))
    h.send(P, ev.RequestReplan())
    h.quiesce(P, b.session_id)
    s = h.p().session(b.session_id)
    assert s.lifecycle == Lifecycle.FENCED and {FenceKind.SAFETY, FenceKind.REVOKED} <= s.fences
    assert not building_slot(h) and h.p().pending_authorization_id is not None
    return b, sent


@pytest.mark.parametrize(
    "activity",
    [
        lambda sid: ev.OwnerDirectOmnigentMessage(session_id=sid, item_id="ui"),
        lambda sid: ev.RuntimeActivity(session_id=sid, busy=True),
    ],
    ids=["owner-direct", "busy-runtime"],
)
def test_D1_external_activity_never_reopens_a_fenced_released_build(activity):
    h = Harness()
    b, sent = fenced_released_build(h)
    r = h.send(P, activity(b.session_id))
    s = h.p().session(b.session_id)
    assert s.lifecycle == Lifecycle.FENCED and s.external_active
    assert not building_slot(h) and not [e for e in r.effects if e.kind in WORK_BEARING_KINDS]
    # The pending plan wake-up (any later event) must not re-open it either.
    r = h.send(P, ev.ReconcileDue())
    assert h.p().session(b.session_id).lifecycle == Lifecycle.FENCED
    # Resolving the ambiguity still waits for fresh quiescence of the external activity.
    r = h.send(P, ev.EffectReconciled(effect_id=sent, session_id=b.session_id))
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
    r = h.quiesce(P, b.session_id)
    old = h.p().session(b.session_id)
    # Successor starts only now; the old build is closed for good, fences retained.
    assert (
        old.lifecycle == Lifecycle.RETIRED and {FenceKind.SAFETY, FenceKind.REVOKED} <= old.fences
    )
    assert h.cur().kind == SessionKind.PLAN and not building_slot(h)


def ready(h: Harness):
    b = h.to_building()
    h.build_ready()
    assert h.p().stage == Stage.READY and h.p().readiness.ready
    return b


@pytest.mark.parametrize(
    "body_for",
    [
        lambda b: ev.ReadinessEvidence(
            session_id=b.session_id, pr_number=7, head_sha=HEAD, verified=False
        ),
        lambda b: ev.ReadinessEvidence(
            session_id=b.session_id,
            pr_number=7,
            head_sha=HEAD,
            verified=True,
            remediation_exhausted=True,
        ),
        # A red check alone keeps Ready (Bot Blocked, #461); with open findings it does not.
        lambda b: ev.ReadinessEvidence(
            session_id=b.session_id,
            pr_number=7,
            head_sha=HEAD,
            checks=ev.ChecksState.FAILED,
            findings_open=True,
        ),
        lambda b: ev.PRObserved(
            pr_number=7,
            head_sha=HEAD,
            open=False,
            merged=False,
            bot_authored=True,
            parcel_branch=True,
        ),
    ],
    ids=["D2-unverified", "remediation-exhausted", "checks-failed-findings", "pr-closed-unmerged"],
)
def test_D2_evidence_falsifying_a_ready_precondition_invalidates_ready(body_for):
    h = Harness()
    b = ready(h)
    r = h.send(P, body_for(b))
    p = h.p()
    assert p.stage == Stage.BUILDING and not p.readiness.ready
    assert Hold.REWORK_CONTROL_REQUIRED in p.holds
    assert EffectKind.CREATE_SESSION not in [e.kind for e in r.effects]
