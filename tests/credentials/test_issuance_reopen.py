"""#677 recovery gap: a restart leaves issuance off; the gate reopening later must re-enable it.

Exact sequence against the real store, the store-backed execution gate and the real broker:
build with an open question → daemon restart (broker default-deny; boot recheck refuses
because the gate is closed) → first reconcile closes the stale decision → operator
resume → the token gate allows and the broker issues a token.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from omnigent_factory.core import codec
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import CredentialProfile, EffectKind, ExecutionContext
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import DecisionImpact
from omnigent_factory.credentials.broker import LocalCredentialBroker
from omnigent_factory.credentials.capabilities import CapabilityRegistry, read_capability_file
from omnigent_factory.service.credentials import StoreExecutionGate
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.durable import reenable_issuance_after_boot
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import REPO_ID, config
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.credentials.fakes import FakeMinter

P = "I_parcel_1"
REPO = "SA-Ambulance/timesheets"
CTX = ExecutionContext("boot", 1, 1, 1)


@pytest.mark.asyncio
async def test_restart_with_gate_closed_then_reconcile_and_resume_issue_a_token(
    tmp_path: Path,
) -> None:
    # A build with a question opened; persist the exact event history into a real store.
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        ev.ElicitationOpened(
            session_id=b.session_id, elicitation_id="el-1", impact=DecisionImpact.UNKNOWN
        ),
    )
    clock = FakeClock()
    db_path = tmp_path / "factory.sqlite3"
    store = SqliteStore.open(db_path, clock)
    store.ensure_repository(h.cfg)  # the store starts paused; the harness does not
    store.apply_event(h.f().make(ev.Unpause(), parcel_id=None, event_id="unpause"), h.cfg)
    for event, _ in h.log:
        store.apply_event(event, h.cfg)
    # Pre-fix record: the question was answered in Omnigent but the decision stayed open.
    parcel = store.load_parcel(P)
    assert parcel is not None and parcel.open_decisions
    stale = tuple(replace(d, prompt_lost=True) for d in parcel.decisions)
    store.query(  # direct fixture write of the legacy aggregate
        "UPDATE parcels SET aggregate_json = ? WHERE parcel_id = ?",
        (codec.parcel_to_json(replace(parcel, decisions=stale)), P),
    )
    store.close()

    db = StoreWorker(db_path, clock)
    await db.start()
    try:
        gate = StoreExecutionGate(db, clock)
        caps = CapabilityRegistry(
            tmp_path / "caps", tmp_path / "broker.sock", REPO, volatile_ok=True
        )
        broker = LocalCredentialBroker(
            gate=gate, minter=FakeMinter(clock), clock=clock, capabilities=caps, repository=REPO
        )
        record = await broker.provision(b.session_id)
        secret = read_capability_file(record.path).secret

        # Restart: issuance default-deny, and the boot recheck refuses (gate closed).
        loaded = await db.call(lambda s: s.load_parcel(P))
        assert await reenable_issuance_after_boot(broker, [loaded]) == ()
        refused = await broker.request_token(b.session_id, secret, REPO)
        assert getattr(refused, "reason", None) == "issuance-disabled"
        assert not (await gate.token_gate(b.session_id)).allowed

        async def apply(body: ev.EventBody, provenance: Provenance, event_id: str):
            event = h.f().make(body, provenance=provenance)
            event = replace(event, event_id=event_id)
            return await db.call(lambda s: s.apply_event(event, h.cfg))

        # First reconcile closes the stale decision and re-enables issuance.
        reconciled = await apply(ev.ReconcileDue(), Provenance.SCHEDULER, "reconcile:1")
        assert (await gate.token_gate(b.session_id)).allowed
        [enable] = [e for e in reconciled.effects if e.kind == EffectKind.ENABLE_ISSUANCE]
        # Operator resume: enable precedes the note, so the session can push immediately.
        resumed = await apply(
            ev.OperatorResume(text="Publish the staged candidate."),
            Provenance.OPERATOR,
            "operator:resume:1",
        )
        kinds = [e.kind for e in resumed.effects]
        assert kinds.index(EffectKind.ENABLE_ISSUANCE) < kinds.index(EffectKind.SEND_MESSAGE)
        for effect in (
            enable,
            *[e for e in resumed.effects if e.kind == EffectKind.ENABLE_ISSUANCE],
        ):
            await broker.execute(effect, CTX)
        grant = await broker.request_token(b.session_id, secret, REPO)
        assert getattr(grant, "profile", None) == CredentialProfile.BUILD, grant
    finally:
        await db.close()


def test_answered_question_reenables_issuance_immediately() -> None:
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        ev.ElicitationOpened(
            session_id=b.session_id, elicitation_id="el-2", impact=DecisionImpact.UNKNOWN
        ),
    )
    r = h.send(P, ev.ElicitationGone(session_id=b.session_id, elicitation_id="el-2"))
    [enable] = Harness.of(r, EffectKind.ENABLE_ISSUANCE)
    assert enable.args == {"profile": "build"}


def test_reconcile_with_the_gate_closed_does_not_enable() -> None:
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        ev.ElicitationOpened(
            session_id=b.session_id, elicitation_id="el-3", impact=DecisionImpact.UNKNOWN
        ),
    )
    r = h.send(P, ev.ReconcileDue())
    assert not Harness.of(r, EffectKind.ENABLE_ISSUANCE)  # a live question keeps it paused
    assert REPO_ID and config()  # fixture helpers stay importable
