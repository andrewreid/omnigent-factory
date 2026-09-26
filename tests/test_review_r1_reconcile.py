"""F1 follow-up: resolving ambiguity via EffectReconciled (new event kind)."""

from __future__ import annotations

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import BotState, Stage
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def test_reconciled_absence_clears_and_allows_ready():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.EffectUnknown(effect_id="m", effect_kind="send_message", session_id=b.session_id))
    h.build_ready()
    assert h.p().stage == Stage.BUILDING and h.p().bot == BotState.BLOCKED
    h.send(P, ev.EffectReconciled(effect_id="m", session_id=b.session_id, delivered=False))
    assert h.p().unknown_effects == () and h.p().stage == Stage.READY


def test_reconciled_delivery_records_own_item():
    h = Harness()
    plan = h.plan_published()
    h.send(
        P, ev.EffectUnknown(effect_id="m", effect_kind="send_message", session_id=plan.session_id)
    )
    h.send(
        P,
        ev.EffectReconciled(
            effect_id="m", session_id=plan.session_id, delivered=True, item_id="it"
        ),
    )
    assert "it" in h.cur().own_items and not h.cur().message_unknown
    r = h.send(P, ev.PlanFeedback(text_digest="x"))
    assert [e for e in r.effects if e.kind in WORK_BEARING_KINDS]


def test_reconciliation_requires_trusted_source_and_known_effect():
    h = Harness()
    plan = h.plan_published()
    h.send(
        P, ev.EffectUnknown(effect_id="m", effect_kind="send_message", session_id=plan.session_id)
    )
    r = h.send(
        P,
        ev.EffectReconciled(effect_id="m", session_id=plan.session_id),
        provenance=Provenance.WEBHOOK,
    )
    assert r.audit.reason == "provenance-not-admitted"
    r = h.send(P, ev.EffectReconciled(effect_id="nope", session_id=plan.session_id))
    assert r.audit.reason == "effect-not-unknown"
    r = h.send(P, ev.EffectReconciled(effect_id="m", session_id="other"))
    assert r.audit.reason == "ack-correlation-mismatch"
    assert h.cur().message_unknown
    r = h.send(
        P,
        ev.EffectReconciled(effect_id="m", session_id=plan.session_id),
        provenance=Provenance.OPERATOR,
    )
    assert r.audit.accepted and not h.cur().message_unknown
