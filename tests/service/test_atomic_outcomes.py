"""The executor records every effect outcome and its reducer event in one store call
(Task 5a, T4): the legacy two-step paths are never used, so no crash can split them."""

from __future__ import annotations

import pytest

from omnigent_factory.core.effects import AmbiguousWrite, EffectKind
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)

from .test_remediation import start_triage, wait_for_async

pytestmark = pytest.mark.asyncio


def _forbidden(*args: object, **kwargs: object) -> object:
    raise AssertionError("executor used a non-atomic outcome path")


async def test_executor_outcomes_are_atomic(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    omnigent = FakeOmnigent()
    omnigent.script(EffectKind.SEND_MESSAGE, AmbiguousWrite("timeout"))
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    for name in ("complete_effect", "mark_effect_unknown", "cancel_effect"):
        monkeypatch.setattr(SqliteStore, name, _forbidden)
    try:
        await start_triage(service, "P")
        await wait_for_async(lambda: omnigent.executed(EffectKind.SEND_MESSAGE))

        async def settled() -> bool:
            rows = await service.db.call(
                lambda s: s.query(
                    "SELECT e.kind, e.state, "
                    "(SELECT COUNT(*) FROM events v WHERE v.event_id = "
                    "'effect:' || e.effect_id || ':ack' OR v.event_id = "
                    "'effect:' || e.effect_id || ':unknown') AS reported "
                    "FROM effects e WHERE e.kind IN ('create_session', 'send_message')"
                )
            )
            return {(r[0], r[1], r[2]) for r in rows} == {
                ("create_session", "done", 1),
                ("send_message", "unknown", 1),
            }

        await wait_for_async(settled)
        parcel = await service.db.call(lambda s: s.load_parcel("P"))
        assert parcel is not None and len(parcel.unknown_effects) == 1
    finally:
        monkeypatch.undo()
        await service.stop()
