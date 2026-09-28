"""F2: boot never re-enables a live run whose caller guard failed (or was just repaired)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.core.types import SessionKind, Via
from omnigent_factory.omnigent.policies import PolicyError
from omnigent_factory.service.composition import ProductionRuntime
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.credentials import StoreExecutionGate
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("outcome", ["error", "changed", "unchanged"])
async def test_boot_holds_runs_whose_guard_failed_or_changed(
    service_config: ServiceConfig, outcome: str
) -> None:
    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        f = EventFactory("P", issue_number=9)
        await service.apply_event(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now)))
        await service.apply_event(f.make(ev.RequestTriage(via=Via.DRAG)))

        async def active() -> Any:
            p = await service.db.call(lambda s: s.load_parcel("P"))
            run = p.current_session if p else None
            return run if run and run.kind == SessionKind.TRIAGE and run.issuance_enabled else None

        await eventually(active)
        parcel = await service.db.call(lambda s: s.load_parcel("P"))
        run = parcel.current_session
        # The reconcile -> barrier -> verify sequence is still pending when boot finishes.
        omnigent.script(
            EffectKind.VERIFY_POLICIES, RetryableReadFailure("still reconciling", 3_600_000_000)
        )

        async def upgrade(session_id: str) -> bool:
            assert session_id == run.session_id
            if outcome == "error":
                raise PolicyError("policy factory-caller missing or altered")
            return outcome == "changed"

        runtime = object.__new__(ProductionRuntime)
        runtime.service = service  # type: ignore[attr-defined]
        runtime.config = service_config  # type: ignore[attr-defined]
        runtime.omnigent_adapter = SimpleNamespace(upgrade_static_policies=upgrade)  # type: ignore[attr-defined]
        held = await runtime._upgrade_live_policies([parcel])
        gate = StoreExecutionGate(service.db, service.clock)
        decision = await gate.token_gate(run.session_id)
        after = (await service.db.call(lambda s: s.load_parcel("P"))).current_session
        if outcome == "unchanged":
            assert held == frozenset() and decision.allowed and after.policy_ready
        else:
            assert held == {run.session_id}
            assert not decision.allowed and not after.policy_ready
            assert not after.issuance_enabled  # its credential was switched off
    finally:
        await service.stop()
