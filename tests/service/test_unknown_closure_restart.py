"""Startup closes ``unknown`` rows already resolved by a persisted reconciliation (#694)."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from omnigent_factory.core.effects import EffectKind
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.test_unknown_effect_closure import _reconciled, unknown_send


@pytest.mark.asyncio
async def test_restart_closes_a_reconciled_unknown_send_without_resending(
    service_config: ServiceConfig,
) -> None:
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    h, session_id, effect_id = unknown_send(store)
    assert store.apply_event(_reconciled(effect_id, session_id, delivered=True), h.cfg).accepted
    store.close()
    # State as the pre-fix daemon left it: resolved in the parcel, ``unknown`` in the outbox.
    with sqlite3.connect(service_config.database_path) as raw:
        raw.execute("UPDATE effects SET state = 'unknown' WHERE effect_id = ?", (effect_id,))

    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        row = await service.db.call(lambda db: db.get_effect(effect_id))
        assert row is not None and row.state == "done"
        unknown = await service.db.call(lambda db: db.effects_in_state("unknown"))
        assert effect_id not in {u.effect.effect_id for u in unknown}
        await asyncio.sleep(0.1)  # let the outbox run: the send must never be repeated
        sent = omnigent.executed(EffectKind.SEND_MESSAGE)
        assert effect_id not in {e.effect_id for e in sent}
    finally:
        await service.stop()
