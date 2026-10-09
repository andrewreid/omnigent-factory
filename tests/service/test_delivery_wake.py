"""The delivery loop is event-driven: idle it barely polls, yet new work runs at once."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.interfaces import NonRetryableDelivery
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.fakes import FakeClock

from .test_remediation import wait_for_async

pytestmark = pytest.mark.asyncio

DAY_US = 86_400 * 1_000_000


class RecordingProcessor:
    """Marks each delivery processed and records when; scripted failures first."""

    def __init__(self, service: FactoryService) -> None:
        self.service = service
        self.seen: dict[str, float] = {}
        self.fail_once: set[str] = set()
        self.poison: set[str] = set()
        self.defer: set[str] = set()

    async def process(self, delivery: DeliveryRecord) -> None:
        guid = delivery.delivery_guid
        if guid in self.fail_once:
            self.fail_once.discard(guid)
            raise RuntimeError("transient")
        if guid in self.poison:
            self.poison.discard(guid)
            raise NonRetryableDelivery("poison")
        if guid in self.defer:
            self.defer.discard(guid)
            await self.service.defer_unresolved_delivery(guid, None)
            return
        self.seen[guid] = asyncio.get_running_loop().time()
        await self.service.db.call(lambda store: store.mark_delivery(guid, "processed"))


def make(service_config: ServiceConfig, **overrides: Any) -> tuple[FactoryService, Any]:
    config = service_config.model_copy(update={"delivery_idle_poll_seconds": 30.0, **overrides})
    service = FactoryService(config)
    processor = RecordingProcessor(service)
    service.delivery_processor = processor
    return service, processor


def count_inbox_reads(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    reads = [0]
    real = SqliteStore.pending_deliveries

    def counting(self: SqliteStore) -> list[DeliveryRecord]:
        reads[0] += 1
        return real(self)

    monkeypatch.setattr(SqliteStore, "pending_deliveries", counting)
    return reads


async def test_idle_delivery_loop_reads_the_inbox_only_at_the_idle_poll(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    reads = count_inbox_reads(monkeypatch)
    service, _ = make(service_config, delivery_idle_poll_seconds=0.25)
    await service.start()
    try:
        await asyncio.sleep(0.1)
        start = reads[0]
        await asyncio.sleep(1.0)
        # Previously a 0.05 s poll: ~20 full inbox reads per second while idle.
        assert 3 <= reads[0] - start <= 5
    finally:
        await service.stop()


async def test_new_webhook_delivery_is_processed_promptly_while_idle(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    reads = count_inbox_reads(monkeypatch)
    service, processor = make(service_config)
    await service.start()
    try:
        await asyncio.sleep(0.2)
        idle_reads = reads[0]
        for guid in ("d-1", "d-2"):
            persisted = asyncio.get_running_loop().time()
            await service.persist_delivery(DeliveryRecord(guid, "issues", b"{}", {}))
            await wait_for_async(lambda guid=guid: guid in processor.seen, timeout=1.0)
            assert processor.seen[guid] - persisted < 0.1
        assert reads[0] - idle_reads <= 4  # woken per delivery, not polling
        # A duplicate (no new inbox row) does not wake the loop.
        before = reads[0]
        await service.persist_delivery(DeliveryRecord("d-1", "issues", b"{}", {}))
        await asyncio.sleep(0.2)
        assert reads[0] == before
    finally:
        await service.stop()


async def test_in_memory_retry_wakes_at_its_deadline_not_the_idle_poll(
    service_config: ServiceConfig,
):
    service, processor = make(service_config, delivery_retry_backoff_seconds=0.2)
    processor.fail_once.add("flaky")
    await service.start()
    try:
        persisted = asyncio.get_running_loop().time()
        await service.persist_delivery(DeliveryRecord("flaky", "issues", b"{}", {}))
        await wait_for_async(lambda: "flaky" in processor.seen, timeout=2.0)
        assert 0.2 <= processor.seen["flaky"] - persisted < 1.0
    finally:
        await service.stop()


async def test_durable_resolution_retry_wakes_at_its_deadline(service_config: ServiceConfig):
    service, processor = make(service_config, delivery_resolution_backoff_seconds=0.3)
    processor.defer.add("project")
    await service.start()
    try:
        persisted = asyncio.get_running_loop().time()
        await service.persist_delivery(DeliveryRecord("project", "projects_v2_item", b"{}", {}))
        await wait_for_async(lambda: "project" in processor.seen, timeout=3.0)
        assert 0.3 <= processor.seen["project"] - persisted < 1.5
    finally:
        await service.stop()


async def test_operator_release_wakes_the_idle_loop(service_config: ServiceConfig):
    service, processor = make(service_config)
    processor.poison.add("poison")
    await service.start()
    try:
        await service.persist_delivery(DeliveryRecord("poison", "issues", b"{}", {}))
        await wait_for_async(lambda: service.parked.contains("poison"))
        await asyncio.sleep(0.2)  # the loop is now idle (30 s poll)
        released = asyncio.get_running_loop().time()
        await service.operator_command("release-delivery", {"delivery": "poison"})
        await wait_for_async(lambda: "poison" in processor.seen, timeout=1.0)
        assert processor.seen["poison"] - released < 0.1
    finally:
        await service.stop()


async def test_stop_is_prompt_while_the_delivery_loop_is_idle(service_config: ServiceConfig):
    service, _ = make(service_config)
    await service.start()
    await asyncio.sleep(0.1)
    started = asyncio.get_running_loop().time()
    await service.stop()
    assert asyncio.get_running_loop().time() - started < 2.0


async def test_clock_loop_prunes_old_unreferenced_delivery_bodies(
    service_config: ServiceConfig,
):
    # Rows themselves outlive this test's 15 days (row retention is its own test).
    service_config = service_config.model_copy(update={"delivery_row_retention_days": 30})
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    for guid in ("old", "fresh"):
        if guid == "fresh":
            clock.advance(15 * DAY_US)
        store.append_delivery(DeliveryRecord(guid, "issues", b'{"big":1}', {}))
        store.mark_delivery(guid, "processed")
    store.close()

    service = FactoryService(service_config, clock=clock)
    await service.start()
    try:

        async def bodies() -> dict[str, bytes]:
            rows = await service.db.call(
                lambda store: store.query("SELECT delivery_guid, body FROM deliveries")
            )
            return {str(r[0]): bytes(r[1]) for r in rows}

        await wait_for_async(lambda: _pruned(bodies(), "old"))
        assert (await bodies())["fresh"] == b'{"big":1}'
        assert await service.prune_delivery_bodies() == 0
    finally:
        await service.stop()


async def _pruned(bodies: Any, guid: str) -> bool:
    return (await bodies)[guid] == b""
