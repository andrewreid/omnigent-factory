from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    RetryableReadFailure,
)
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import AdmissionSnapshot, FenceKind, Parcel, Size, Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.executor import EffectExecutor, ParcelSerializers
from omnigent_factory.service.interfaces import DeliveryProcessor, NonRetryableDelivery
from omnigent_factory.service.parked import ParkedDeliveryRegistry
from omnigent_factory.service.runtime import FactoryService, _admission_signature
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.builders import (
    EventFactory,
    contract_text,
    result_candidate,
    snapshot,
)
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)


async def wait_for_async(predicate: Callable[[], object], timeout: float = 2.0) -> object:
    end = asyncio.get_running_loop().time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if asyncio.get_running_loop().time() >= end:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.01)


async def start_triage(
    service: FactoryService, parcel_id: str = "P", *, issue_number: int = 1
) -> EventFactory:
    await service.operator_command("unpause", {})
    factory = EventFactory(parcel_id, issue_number=issue_number)
    await service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))
    return factory


class FailingProcessor(DeliveryProcessor):
    def __init__(self, message: str = "authorization=super-secret-token") -> None:
        self.message = message
        self.calls = 0

    async def process(self, delivery: DeliveryRecord) -> None:
        del delivery
        self.calls += 1
        raise RuntimeError(self.message)


class PoisonProcessor(DeliveryProcessor):
    def __init__(self, parcel_id: str | None = None) -> None:
        self.parcel_id = parcel_id
        self.calls = 0

    async def process(self, delivery: DeliveryRecord) -> None:
        del delivery
        self.calls += 1
        raise NonRetryableDelivery("authorization=poison-secret-token", parcel_id=self.parcel_id)


class BlockingCredential(FakeCredentialBroker):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        if effect.kind == EffectKind.ENABLE_ISSUANCE:
            self.entered.set()
            await self.release.wait()
        return await super().execute(effect, ctx)


class FairGitHub(FakeGitHub):
    def __init__(self) -> None:
        super().__init__()
        self.slow_entered = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        if effect.kind == EffectKind.RECONCILE_PARCEL and effect.parcel_id == "A-slow":
            self.slow_entered.set()
            await self.release.wait()
        return await super().execute(effect, ctx)


class RaisingOmnigent(FakeOmnigent):
    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        if effect.kind == EffectKind.CREATE_SESSION:
            self.calls.append((effect, ctx))
            raise RuntimeError("authorization=adapter-secret-token")
        return await super().execute(effect, ctx)


class SlowMessageOmnigent(FakeOmnigent):
    def __init__(self) -> None:
        super().__init__()
        self.message_entered = asyncio.Event()
        self.message_completed = False

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        if effect.kind == EffectKind.SEND_MESSAGE:
            self.message_entered.set()
            await asyncio.sleep(0.1)
            self.message_completed = True
        return await super().execute(effect, ctx)


def seed_parcels(config: ServiceConfig, clock: FakeClock, *parcel_ids: str) -> None:
    store = SqliteStore.open(config.database_path, clock)
    try:
        store.ensure_repository(config.trusted)
        for number, parcel_id in enumerate(parcel_ids, start=1):
            factory = EventFactory(parcel_id, issue_number=number)
            store.apply_event(
                factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
                config.trusted,
            )
    finally:
        store.close()


def seed_reserved_then_pause(config: ServiceConfig, clock: FakeClock) -> None:
    store = SqliteStore.open(config.database_path, clock)
    try:
        store.ensure_repository(config.trusted)
        factory = EventFactory("P-paused")
        store.apply_event(factory.make(ev.Unpause(), parcel_id=None), config.trusted)
        store.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
            config.trusted,
        )
        store.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)), config.trusted)
        store.apply_event(factory.make(ev.CapacityAvailable()), config.trusted)
        store.apply_event(factory.make(ev.Pause(), parcel_id=None), config.trusted)
        assert any(row.effect.kind == EffectKind.CREATE_SESSION for row in store.pending_effects())
    finally:
        store.close()


@pytest.mark.asyncio
async def test_transient_db_errors_do_not_kill_background_tasks(service_config: ServiceConfig):
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    original = service.db.call
    failures = 4

    async def flaky(operation):
        nonlocal failures
        if failures:
            failures -= 1
            raise RuntimeError("temporary database outage")
        return await original(operation)

    service.db.call = flaky  # type: ignore[method-assign]
    try:
        await asyncio.sleep(0.3)
        assert all(not task.done() for task in service._tasks)
        assert (await service.health())["status"] == "ok"
    finally:
        service.db.call = original  # type: ignore[method-assign]
        await service.stop()


@pytest.mark.asyncio
async def test_health_is_unhealthy_when_a_background_task_dies(service_config: ServiceConfig):
    exits: list[int] = []
    service = FactoryService(service_config, clock=FakeClock(), fatal_exit=exits.append)
    await service.start()
    service._tasks[2].cancel()
    await asyncio.gather(service._tasks[2], return_exceptions=True)
    try:
        await wait_for_async(lambda: exits)
        assert exits == [1]
        assert (await service.health())["status"] == "unhealthy"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_pending_delivery_blocks_work_but_not_safety_effects(
    service_config: ServiceConfig,
):
    config = service_config.model_copy(
        update={
            "delivery_retry_backoff_seconds": 0.1,
            "delivery_retry_max_backoff_seconds": 0.1,
        }
    )
    github = FakeGitHub()
    omnigent = FakeOmnigent()
    credentials = FakeCredentialBroker()
    processor = FailingProcessor()
    service = FactoryService(
        config,
        adapters=(github, omnigent, credentials),
        delivery_processor=processor,
        clock=FakeClock(),
    )
    await service.start()
    try:
        factory = await start_triage(service, "P-safety")
        await wait_for_async(lambda: omnigent.executed(EffectKind.SEND_MESSAGE))
        await service.persist_delivery(
            DeliveryRecord("stuck", "issues", b"{}", {"x-github-event": "issues"})
        )
        await wait_for_async(lambda: processor.calls)
        await service.apply_event(factory.make(ev.Stop()))
        await wait_for_async(lambda: omnigent.executed(EffectKind.INTERRUPT_TREE))
        assert credentials.executed(EffectKind.DISABLE_ISSUANCE)
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_transient_owner_delivery_outage_never_opens_work_gate(
    service_config: ServiceConfig,
):
    assert service_config.delivery_retry_max_backoff_seconds >= 60
    config = service_config.model_copy(
        update={
            "delivery_retry_backoff_seconds": 0.01,
            "delivery_retry_max_backoff_seconds": 0.02,
        }
    )
    processor = FailingProcessor()
    omnigent = FakeOmnigent()
    service = FactoryService(
        config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        delivery_processor=processor,
        clock=FakeClock(),
    )
    await service.start()
    try:
        await service.persist_delivery(DeliveryRecord("owner-unassigned", "issues", b"{}", {}))
        await asyncio.sleep(0.25)
        await start_triage(service, "P-owner-outage")
        await asyncio.sleep(0.2)
        rows = await service.db.call(
            lambda store: store.query(
                "SELECT status FROM deliveries WHERE delivery_guid = ?",
                ("owner-unassigned",),
            )
        )
        assert rows[0][0] == "pending"
        assert processor.calls > 3
        assert omnigent.executed(EffectKind.CREATE_SESSION) == []
        assert omnigent.executed(EffectKind.SEND_MESSAGE) == []
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_unresolved_delivery_does_not_deadlock_dispatch(service_config: ServiceConfig):
    clock = FakeClock()
    github = FakeGitHub()
    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(github, omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        delivery = DeliveryRecord("unresolved", "projects_v2_item", b"{}", {})
        await service.persist_delivery(delivery)
        await service.db.call(lambda store: store.mark_delivery("unresolved", "unresolved"))
        await start_triage(service, "P-unresolved")
        await wait_for_async(lambda: omnigent.executed(EffectKind.CREATE_SESSION))
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_processor_outage_is_bounded_and_never_logs_secret(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    processor = PoisonProcessor()
    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        delivery_processor=processor,
        clock=FakeClock(),
    )
    caplog.set_level(logging.WARNING)
    await service.start()
    try:
        await service.persist_delivery(DeliveryRecord("poison", "issues", b"{}", {}))

        async def parked() -> bool:
            pending = await service.db.call(lambda store: store.pending_deliveries())
            return not pending

        await wait_for_async(parked)
        assert processor.calls == 1
        recovery = await service.operator_command("recovery", {})
        deliveries = recovery["deliveries"]
        assert isinstance(deliveries, list)
        assert {"delivery_guid": "poison", "status": "parked", "parcel": None} in deliveries
        await start_triage(service, "P-global-park")
        await asyncio.sleep(0.1)
        assert omnigent.executed(EffectKind.CREATE_SESSION) == []
        logging.getLogger("omnigent_factory.service.runtime").warning(
            "authorization=%s", "another-secret-value"
        )
        assert "super-secret-token" not in caplog.text
        assert "another-secret-value" not in caplog.text
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_parked_delivery_scopes_work_gate_reports_health_and_requires_release(
    service_config: ServiceConfig,
):
    config = service_config.model_copy(update={"max_building": 2})
    github = FakeGitHub()
    omnigent = FakeOmnigent()
    processor = PoisonProcessor("P-parked")
    service = FactoryService(
        config,
        adapters=(github, omnigent, FakeCredentialBroker()),
        delivery_processor=processor,
        clock=FakeClock(),
    )
    await service.start()
    try:
        await service.persist_delivery(DeliveryRecord("bad-owner", "issues", b"{}", {}))

        def is_parked() -> bool:
            return service.parked.contains("bad-owner")

        await wait_for_async(is_parked)
        assert await service.health() == {
            "status": "degraded",
            "paused": True,
            "parked_deliveries": 1,
        }
        status = await service.operator_command("status", {})
        assert status["parked_deliveries"] == 1
        reloaded = ParkedDeliveryRegistry(config.state_dir)
        reloaded.load()
        assert reloaded.blocks("P-parked")
        assert not reloaded.blocks("P-open")

        parked_factory = await start_triage(service, "P-parked")
        await start_triage(service, "P-open", issue_number=2)
        await service.apply_event(parked_factory.make(ev.ReconcileDue()))
        await wait_for_async(
            lambda: any(
                effect.parcel_id == "P-open"
                for effect in omnigent.executed(EffectKind.CREATE_SESSION)
            )
        )
        assert not any(
            effect.parcel_id == "P-parked"
            for effect in omnigent.executed(EffectKind.CREATE_SESSION)
        )
        await wait_for_async(
            lambda: any(
                effect.parcel_id == "P-parked"
                for effect in github.executed(EffectKind.RECONCILE_PARCEL)
            )
        )

        service.delivery_processor = None
        released = await service.operator_command("release-delivery", {"delivery": "bad-owner"})
        assert released["released"] == "bad-owner"
        assert released["parked_deliveries"] == 0
        rows = await service.db.call(
            lambda store: store.query(
                "SELECT status FROM deliveries WHERE delivery_guid = 'bad-owner'"
            )
        )
        assert rows[0][0] == "pending"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_installs_formatted_log_redaction(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.WARNING)
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    try:
        logging.getLogger("omnigent_factory.service.runtime").warning(
            "authorization=%s", "filter-secret-value"
        )
        assert "filter-secret-value" not in caplog.text
        assert "authorization=[REDACTED]" in caplog.text
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_reconcile_ids_are_per_parcel_and_workers_are_interparcel_fair(
    service_config: ServiceConfig,
):
    config = service_config.model_copy(update={"reconcile_interval_seconds": 0.02})
    clock = FakeClock()
    seed_parcels(config, clock, "A-slow", "B-fast", "C-fast")
    github = FairGitHub()
    service = FactoryService(config, adapters=(github,), clock=clock)
    await service.start()
    try:
        await wait_for_async(lambda: github.slow_entered.is_set())
        await wait_for_async(
            lambda: (
                {effect.parcel_id for effect in github.executed(EffectKind.RECONCILE_PARCEL)}
                >= {"B-fast", "C-fast"}
            )
        )
    finally:
        github.release.set()
        await service.stop()


@pytest.mark.asyncio
async def test_pause_defers_reserved_create_until_unpause(service_config: ServiceConfig):
    clock = FakeClock()
    seed_reserved_then_pause(service_config, clock)
    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        await asyncio.sleep(0.1)
        assert omnigent.executed(EffectKind.CREATE_SESSION) == []
        await service.operator_command("unpause", {})
        await wait_for_async(lambda: omnigent.executed(EffectKind.CREATE_SESSION))
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_retryable_failure_on_write_becomes_unknown_without_retry(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    omnigent = FakeOmnigent()
    omnigent.script(EffectKind.CREATE_SESSION, RetryableReadFailure("POST timed out", 1))
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        await start_triage(service, "P-write-timeout")
        await wait_for_async(lambda: omnigent.executed(EffectKind.CREATE_SESSION))
        clock.advance(10)

        async def is_unknown() -> bool:
            rows = await service.db.call(lambda store: store.effects_in_state("unknown"))
            return any(row.effect.kind == EffectKind.CREATE_SESSION for row in rows)

        await wait_for_async(is_unknown)
        assert len(omnigent.executed(EffectKind.CREATE_SESSION)) == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_adapter_exception_becomes_unknown_without_logging_secret(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    omnigent = RaisingOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=FakeClock(),
    )
    caplog.set_level(logging.ERROR)
    await service.start()
    try:
        await start_triage(service, "P-adapter-error")

        async def is_unknown() -> bool:
            rows = await service.db.call(lambda store: store.effects_in_state("unknown"))
            return any(row.effect.kind == EffectKind.CREATE_SESSION for row in rows)

        await wait_for_async(is_unknown)
        assert len(omnigent.executed(EffectKind.CREATE_SESSION)) == 1
        assert "adapter-secret-token" not in caplog.text
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_malformed_ack_becomes_reducer_visible_unknown(service_config: ServiceConfig):
    omnigent = FakeOmnigent()
    omnigent.script(EffectKind.CREATE_SESSION, Ack())
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    try:
        await start_triage(service, "P-bad-ack")

        async def is_unknown() -> bool:
            rows = await service.db.call(lambda store: store.effects_in_state("unknown"))
            return any(row.effect.kind == EffectKind.CREATE_SESSION for row in rows)

        await wait_for_async(is_unknown)
        parcel = await service.db.call(lambda store: store.load_parcel("P-bad-ack"))
        assert parcel is not None
        assert parcel.current_session is not None
        assert parcel.current_session.lifecycle.value == "UNKNOWN"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_shutdown_does_not_start_queued_first_message(service_config: ServiceConfig):
    credentials = BlockingCredential()
    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, credentials),
        clock=FakeClock(),
    )
    await service.start()
    await start_triage(service, "P-shutdown")
    await wait_for_async(lambda: credentials.entered.is_set())
    stopping = asyncio.create_task(service.stop())
    await asyncio.sleep(0)
    credentials.release.set()
    await asyncio.wait_for(stopping, 1)
    assert omnigent.executed(EffectKind.SEND_MESSAGE) == []


@pytest.mark.asyncio
async def test_shutdown_lets_inflight_write_finish_within_timeout(
    service_config: ServiceConfig,
):
    omnigent = SlowMessageOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    await start_triage(service, "P-graceful-write")
    await wait_for_async(lambda: omnigent.message_entered.is_set())
    await service.stop()
    assert omnigent.message_completed

    store = SqliteStore.open(service_config.database_path, FakeClock())
    try:
        messages = [
            row
            for row in store.effects_in_state("done")
            if row.effect.kind == EffectKind.SEND_MESSAGE
        ]
        assert len(messages) == 1
    finally:
        store.close()


@pytest.mark.asyncio
async def test_idle_operator_client_cannot_block_shutdown(service_config: ServiceConfig):
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    reader, writer = await asyncio.open_unix_connection(service_config.operator_socket)
    del reader
    try:
        await asyncio.wait_for(service.stop(), 0.5)
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_lease_epoch_advances_after_restart(service_config: ServiceConfig):
    clock = FakeClock()
    seed_parcels(service_config, clock, "P-epoch")
    first_adapter = FakeGitHub()
    first = FactoryService(service_config, adapters=(first_adapter,), clock=clock)
    await first.start()
    await wait_for_async(lambda: first_adapter.executed(EffectKind.RECONCILE_PARCEL))
    first_epoch = first_adapter.calls[0][1].lease_epoch
    await first.stop()

    clock.advance(1)
    second_adapter = FakeGitHub()
    second = FactoryService(service_config, adapters=(second_adapter,), clock=clock)
    await second.start()
    try:
        await wait_for_async(lambda: second_adapter.executed(EffectKind.RECONCILE_PARCEL))
        second_epoch = second_adapter.calls[0][1].lease_epoch
        assert second_epoch > first_epoch
    finally:
        await second.stop()


@pytest.mark.asyncio
async def test_executor_without_process_lock_proof_refuses_foreign_lease(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    factory = EventFactory("P-foreign")
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        service_config.trusted,
    )
    store.apply_event(factory.make(ev.ReconcileDue()), service_config.trusted)
    store.acquire_lease("P-foreign", "live-foreign-boot")
    store.close()

    from omnigent_factory.service.db import StoreWorker

    worker = StoreWorker(service_config.database_path, clock)
    await worker.start()
    github = FakeGitHub()
    executor = EffectExecutor(
        worker,
        service_config.trusted,
        clock,
        (github,),
        ParcelSerializers(),
        poll_seconds=0.01,
        takeover_foreign_leases=False,
    )
    task = asyncio.create_task(executor.run())
    try:
        await asyncio.sleep(0.1)
        assert github.calls == []
    finally:
        await executor.stop()
        await task
        await worker.close()


@pytest.mark.asyncio
async def test_second_service_is_refused_while_first_holds_process_lock(
    service_config: ServiceConfig,
):
    from omnigent_factory.service.locking import AlreadyRunning

    first = FactoryService(service_config, clock=FakeClock())
    second = FactoryService(service_config, clock=FakeClock())
    await first.start()
    try:
        with pytest.raises(AlreadyRunning):
            await second.start()
    finally:
        await first.stop()


@pytest.mark.parametrize("suffix", ["failed", "cancelled"])
def test_restart_preserves_terminal_effect_state(service_config: ServiceConfig, suffix: str):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    factory = EventFactory(f"P-{suffix}")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), service_config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        service_config.trusted,
    )
    store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), service_config.trusted)
    create = next(
        row for row in store.pending_effects() if row.effect.kind == EffectKind.CREATE_SESSION
    )
    lease = store.acquire_lease(factory.parcel_id, "crashed-boot")
    assert store.claim_effect(create.effect.effect_id, lease)
    body: ev.EventBody
    if suffix == "failed":
        body = ev.CreateRejected(
            session_id=create.effect.preconditions.session_id or "", reason="rejected"
        )
    else:
        body = ev.EffectCancelled(
            effect_id=create.effect.effect_id,
            effect_kind=create.effect.kind.value,
            session_id=create.effect.preconditions.session_id,
        )
    store.apply_event(
        Event(
            event_id=f"effect:{create.effect.effect_id}:{suffix}",
            repo_id=service_config.repo_id,
            parcel_id=factory.parcel_id,
            source_time_us=clock.now_utc_us(),
            provenance=Provenance.ADAPTER,
            body=body,
        ),
        service_config.trusted,
    )
    store.close()

    async def check() -> None:
        service = FactoryService(service_config, clock=clock)
        await service.start()
        try:
            effect = await service.db.call(lambda db: db.get_effect(create.effect.effect_id))
            assert effect is not None
            assert effect.state == suffix
        finally:
            await service.stop()

    asyncio.run(check())


@pytest.mark.asyncio
async def test_restart_adopts_persisted_ack_without_second_external_call(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    factory = EventFactory("P-ack-crash")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), service_config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        service_config.trusted,
    )
    store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), service_config.trusted)
    create = next(
        row for row in store.pending_effects() if row.effect.kind == EffectKind.CREATE_SESSION
    )
    lease = store.acquire_lease(factory.parcel_id, "crashed-boot")
    assert store.claim_effect(create.effect.effect_id, lease)
    store.apply_event(
        Event(
            event_id=f"effect:{create.effect.effect_id}:ack",
            repo_id=service_config.repo_id,
            parcel_id=factory.parcel_id,
            source_time_us=clock.now_utc_us(),
            provenance=Provenance.ADAPTER,
            body=ev.SessionCreated(
                session_id=create.effect.preconditions.session_id or "",
                root_id="existing-root",
                nonce=str(create.effect.args["nonce"]),
            ),
        ),
        service_config.trusted,
    )
    store.close()

    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        effect = await service.db.call(lambda db: db.get_effect(create.effect.effect_id))
        assert effect is not None and effect.state == "done"
        assert omnigent.executed(EffectKind.CREATE_SESSION) == []
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_restart_emits_unknown_after_crash_before_unknown_event(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    factory = EventFactory("P-unknown-crash")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), service_config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        service_config.trusted,
    )
    store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), service_config.trusted)
    create = next(
        row for row in store.pending_effects() if row.effect.kind == EffectKind.CREATE_SESSION
    )
    lease = store.acquire_lease(factory.parcel_id, "crashed-boot")
    assert store.claim_effect(create.effect.effect_id, lease)
    assert store.mark_effect_unknown(create.effect.effect_id, "ambiguous-timeout")
    store.close()

    omnigent = FakeOmnigent()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        await wait_for_async(
            lambda: service.db.call(
                lambda db: db.has_event(f"effect:{create.effect.effect_id}:unknown")
            )
        )
        parcel = await service.db.call(lambda db: db.load_parcel(factory.parcel_id))
        assert parcel is not None and parcel.current_session is not None
        assert parcel.current_session.lifecycle.value == "UNKNOWN"
        assert omnigent.executed(EffectKind.CREATE_SESSION) == []
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_restart_returns_claimed_read_to_pending(service_config: ServiceConfig):
    clock = FakeClock()
    seed_parcels(service_config, clock, "P-read-crash")
    store = SqliteStore.open(service_config.database_path, clock)
    store.apply_event(
        Event(
            event_id="read-crash-reconcile",
            repo_id=service_config.repo_id,
            parcel_id="P-read-crash",
            source_time_us=clock.now_utc_us(),
            provenance=Provenance.SCHEDULER,
            body=ev.ReconcileDue(),
        ),
        service_config.trusted,
    )
    read = next(
        row for row in store.pending_effects() if row.effect.kind == EffectKind.RECONCILE_PARCEL
    )
    lease = store.acquire_lease("P-read-crash", "crashed-boot")
    assert store.claim_effect(read.effect.effect_id, lease)
    store.close()

    service = FactoryService(service_config, clock=clock)
    await service.start()
    try:

        async def is_pending() -> bool:
            effect = await service.db.call(lambda db: db.get_effect(read.effect.effect_id))
            return effect is not None and effect.state == "pending"

        await wait_for_async(is_pending)
        assert not await service.db.call(
            lambda db: db.has_event(f"effect:{read.effect.effect_id}:unknown")
        )
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_malformed_operator_request_returns_type_only(service_config: ServiceConfig):
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    try:
        reader, writer = await asyncio.open_unix_connection(service_config.operator_socket)
        writer.write(b'{"token":"operator-secret"\n')
        await writer.drain()
        response = json.loads(await asyncio.wait_for(reader.readline(), 0.5))
        assert response == {"ok": False, "error": "JSONDecodeError"}
        writer.close()
        await writer.wait_closed()
    finally:
        await service.stop()


def test_fence_and_grant_survive_service_restart(service_config: ServiceConfig):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    factory = EventFactory("P-fence")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), service_config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        service_config.trusted,
    )
    store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), service_config.trusted)
    parcel = store.load_parcel(factory.parcel_id)
    assert parcel is not None and parcel.current_session is not None
    grant_id = parcel.current_session.grant.grant_id
    store.apply_event(factory.make(ev.Stop()), service_config.trusted)
    store.close()

    async def check() -> None:
        service = FactoryService(service_config, clock=clock)
        await service.start()
        try:
            restarted = await service.db.call(lambda db: db.load_parcel(factory.parcel_id))
            assert restarted is not None and restarted.current_session is not None
            assert FenceKind.STOPPED in restarted.current_session.fences
            assert restarted.current_session.grant.grant_id == grant_id
        finally:
            await service.stop()

    asyncio.run(check())


def seed_blocked_plan(config: ServiceConfig) -> None:
    clock = FakeClock()
    store = SqliteStore.open(config.database_path, clock)
    store.ensure_repository(config.trusted)
    factory = EventFactory("P-blocked-plan")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        config.trusted,
    )
    store.apply_event(factory.make(ev.RequestPlan(via=Via.DRAG)), config.trusted)
    parcel = store.load_parcel(factory.parcel_id)
    assert parcel is not None and parcel.current_session is not None
    session = parcel.current_session
    store.apply_event(
        factory.make(
            ev.SessionCreated(
                session_id=session.session_id, root_id="root-plan", nonce=session.nonce
            )
        ),
        config.trusted,
    )
    store.apply_event(
        factory.make(ev.Prepared(session_id=session.session_id, ok=True)), config.trusted
    )
    message = next(
        row for row in store.pending_effects() if row.effect.kind == EffectKind.SEND_MESSAGE
    )
    store.apply_event(
        factory.make(
            ev.MessageAck(
                session_id=session.session_id,
                effect_id=message.effect.effect_id,
                item_id="item-plan",
            )
        ),
        config.trusted,
    )
    store.apply_event(
        factory.make(
            result_candidate(
                session.session_id,
                "root-plan",
                session.revision,
                ev.ResultKind.PLAN,
                publication_kind=ev.PublicationKind.CONTRACT,
                contract_canonical=contract_text(),
                size=Size.M,
            )
        ),
        config.trusted,
    )
    parcel = store.load_parcel(factory.parcel_id)
    assert parcel is not None and parcel.contracts
    contract = parcel.contracts[-1]
    store.apply_event(
        factory.make(
            ev.ContractPublished(
                contract_id=contract.contract_id,
                comment_id="comment-1",
                verified=True,
                posted_at_us=factory.now,
            )
        ),
        config.trusted,
    )
    store.apply_event(
        factory.make(ev.ApprovePlan(via=Via.COMMAND, hash_text=contract.prefix)),
        config.trusted,
    )
    store.close()


@pytest.mark.asyncio
async def test_blocked_admission_head_is_not_audit_spammed(service_config: ServiceConfig):
    seed_blocked_plan(service_config)
    service = FactoryService(service_config)
    await service.start()
    try:
        await asyncio.sleep(0.15)
        rows = await service.db.call(
            lambda store: store.query(
                "SELECT COUNT(*) FROM events WHERE kind = 'CapacityAvailable'"
            )
        )
        assert int(rows[0][0]) <= 1
    finally:
        await service.stop()


def test_admission_signature_tracks_aggregate_and_admission_versions(
    service_config: ServiceConfig,
):
    parcel = Parcel(parcel_id="P-signature", repo_id=service_config.repo_id, version=1)
    admission = AdmissionSnapshot(repo_id=service_config.repo_id, next_sequence=2)
    original = _admission_signature(parcel, admission, parcel.parcel_id, 1)

    changed_parcel = replace(parcel, version=2, in_project=True)
    assert _admission_signature(changed_parcel, admission, parcel.parcel_id, 1) != original

    changed_admission = replace(admission, next_sequence=3)
    assert _admission_signature(parcel, changed_admission, parcel.parcel_id, 1) != original
