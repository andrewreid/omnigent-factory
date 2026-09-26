"""Central provenance/actor admission: every EventKind x every Provenance."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import ADMISSION, ActorRule, admission_rejection
from omnigent_factory.core.events import EventKind, Provenance
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, snapshot
from omnigent_factory.testing.harness import Harness
from tests.test_reducer_tables import _sample_bodies

P = "I_parcel_1"
OWNERS = frozenset({OWNER_ID})


def test_table_is_total():
    assert set(ADMISSION) == set(EventKind)


def test_adapter_outcomes_never_admit_github_origin():
    github = {Provenance.WEBHOOK, Provenance.RECOVERY, Provenance.RECONCILER}
    for kind in (
        EventKind.MESSAGE_ACK,
        EventKind.EFFECT_RECONCILED,
        EventKind.EFFECT_CANCELLED,
        EventKind.EFFECT_UNKNOWN,
        EventKind.ADOPTION_RESULT,
        EventKind.SESSION_CREATED,
        EventKind.PREPARED,
        EventKind.TREE_QUIESCENT,
        EventKind.RESULT_CANDIDATE,
        EventKind.POLICY_READY,
        EventKind.CAPACITY_AVAILABLE,
        EventKind.GRACE_EXPIRED,
    ):
        assert not (ADMISSION[kind].provenances & github), kind


def _building():
    h = Harness()
    h.to_building()
    return h


@pytest.mark.parametrize("provenance", list(Provenance))
@pytest.mark.parametrize("actor", [OWNER_ID, OTHER_USER_ID])
def test_every_kind_and_provenance_is_enforced_before_handlers(provenance, actor):
    base = _building()
    for kind, body in _sample_bodies(base.p()).items():
        h = Harness(
            cfg=base.cfg,
            admission=base.admission,
            parcels=dict(base.parcels),
            factories={P: base.f()},
            auto_ack_moves=False,
        )
        before, adm_before = h.p(), h.admission
        event = h.f().make(body, provenance=provenance, actor=actor, evidence=snapshot())
        result = h.apply(event)
        expected = admission_rejection(kind, provenance, actor, OWNERS)
        rule = ADMISSION[kind]
        if expected is not None:
            assert result.audit.reason == expected, (kind, provenance)
            assert not result.audit.accepted and result.effects == ()
            assert h.p() == before and h.admission == adm_before, (kind, provenance)
        else:
            assert provenance in rule.provenances
            assert rule.actor != ActorRule.OWNER or actor == OWNER_ID
            assert result.audit.reason not in ("provenance-not-admitted",)
    _ = ev
