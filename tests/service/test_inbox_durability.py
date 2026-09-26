"""Task 5a service wiring: store-backed parked deliveries, parcel holds for parked and
unresolved deliveries (T4 recheck-2 FOLLOW_UP 1 and 5), and legacy registry import."""

from __future__ import annotations

import json
import os

import pytest

from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import FenceKind, Hold, InboxHold, InboxHoldReason, Lifecycle
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.parked import LEGACY_REGISTRY
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)

from .test_remediation import PoisonProcessor, start_triage, wait_for_async

pytestmark = pytest.mark.asyncio


def make(service_config: ServiceConfig, **kw: object):
    github, omnigent, broker = FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()
    service = FactoryService(
        service_config,
        adapters=(github, omnigent, broker),
        clock=FakeClock(),
        **kw,  # type: ignore[arg-type]
    )
    return service, github, omnigent, broker


async def running_triage(service: FactoryService, omnigent: FakeOmnigent, pid: str = "P"):
    await start_triage(service, pid)
    await wait_for_async(
        lambda: any(e.parcel_id == pid for e in omnigent.executed(EffectKind.SEND_MESSAGE))
    )
    parcel = await service.db.call(lambda store: store.load_parcel(pid))
    assert parcel is not None and parcel.current_session is not None
    assert parcel.current_session.lifecycle == Lifecycle.ACTIVE
    return parcel.current_session


async def test_scoped_park_fences_and_interrupts_the_running_tree(service_config: ServiceConfig):
    processor = PoisonProcessor("P")
    service, _, omnigent, broker = make(service_config)
    await service.start()
    try:
        session = await running_triage(service, omnigent)
        assert broker.issuance_enabled(session.session_id)
        service.delivery_processor = processor
        await service.persist_delivery(DeliveryRecord("poison", "issues", b"{}", {}))
        await wait_for_async(
            lambda: any(
                e.preconditions.session_id == session.session_id
                for e in omnigent.executed(EffectKind.INTERRUPT_TREE)
            )
        )
        await wait_for_async(lambda: not broker.issuance_enabled(session.session_id))
        parcel = await service.db.call(lambda store: store.load_parcel("P"))
        assert parcel is not None
        fenced = parcel.session(session.session_id)
        assert fenced is not None and FenceKind.SAFETY in fenced.fences
        assert parcel.inbox_holds == (InboxHold("poison", InboxHoldReason.PARKED),)
        rows = await service.db.call(
            lambda store: store.query(
                "SELECT provenance, kind FROM events WHERE kind = 'InboxHoldSet'"
            )
        )
        assert [tuple(r) for r in rows] == [("inbox", "InboxHoldSet")]

        released = await service.operator_command("release-delivery", {"delivery": "poison"})
        assert released["parked_deliveries"] == 0
        parcel = await service.db.call(lambda store: store.load_parcel("P"))
        assert parcel is not None and parcel.inbox_holds == ()
        still = parcel.session(session.session_id)
        assert still is not None and FenceKind.SAFETY in still.fences  # never cleared
    finally:
        await service.stop()


async def test_parked_state_survives_restart(service_config: ServiceConfig):
    service, _, omnigent, _ = make(service_config, delivery_processor=PoisonProcessor("P"))
    await service.start()
    try:
        await service.persist_delivery(DeliveryRecord("poison", "issues", b"{}", {}))
        await wait_for_async(lambda: service.parked.contains("poison"))
    finally:
        await service.stop()
    assert not (service_config.state_dir / LEGACY_REGISTRY).exists()  # no side file

    again, _, omnigent, _ = make(service_config)
    await again.start()
    try:
        assert again.parked.records() == (("poison", "P"),)
        assert again.parked.blocks("P") and not again.parked.blocks("Q")
        await start_triage(again, "P")
        await start_triage(again, "Q", issue_number=2)
        await wait_for_async(
            lambda: any(e.parcel_id == "Q" for e in omnigent.executed(EffectKind.CREATE_SESSION))
        )
        assert not any(e.parcel_id == "P" for e in omnigent.executed(EffectKind.CREATE_SESSION))
        assert (await again.health())["status"] == "degraded"
    finally:
        await again.stop()


async def test_legacy_registry_file_is_imported_with_scope_and_retired(
    service_config: ServiceConfig,
):
    store = SqliteStore.open(service_config.database_path, FakeClock())
    store.ensure_repository(service_config.trusted)
    for guid in ("legacy-scoped", "legacy-global"):
        store.append_delivery(DeliveryRecord(guid, "issues", b"{}", {}))
    store.mark_delivery("legacy-scoped", "rejected")
    store.close()
    legacy = service_config.state_dir / LEGACY_REGISTRY
    fd = os.open(legacy, os.O_WRONLY | os.O_CREAT, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump({"legacy-scoped": "P", "legacy-global": None}, stream)

    service, _, _, _ = make(service_config)
    await service.start()
    try:
        assert service.parked.records() == (("legacy-global", None), ("legacy-scoped", "P"))
        assert not legacy.exists()
        assert legacy.with_name(LEGACY_REGISTRY + ".migrated").exists()
        statuses = await service.db.call(
            lambda s: dict(s.query("SELECT delivery_guid, status FROM deliveries"))
        )
        assert statuses == {"legacy-scoped": "rejected", "legacy-global": "rejected"}
        parcel = await service.db.call(lambda s: s.load_parcel("P"))
        assert parcel is not None and Hold.INBOX in parcel.holds
        released = await service.operator_command("release-delivery", {"delivery": "legacy-global"})
        assert released["parked_deliveries"] == 1
    finally:
        await service.stop()


async def test_corrupt_legacy_registry_aborts_startup(service_config: ServiceConfig):
    (service_config.state_dir / LEGACY_REGISTRY).write_text('{"g": 5}', encoding="utf-8")
    service, _, _, _ = make(service_config)
    with pytest.raises(RuntimeError, match="invalid parked delivery registry"):
        await service.start()


async def test_unresolved_delivery_for_known_parcel_fences_until_processed(
    service_config: ServiceConfig,
):
    service, _, omnigent, broker = make(service_config)
    await service.start()
    try:
        session = await running_triage(service, omnigent)
        await service.persist_delivery(DeliveryRecord("drag", "projects_v2_item", b"{}", {}))
        assert await service.hold_unresolved_delivery("drag", "P")
        assert await service.hold_unresolved_delivery("drag", "P")  # reprocessed: idempotent
        status = await service.db.call(
            lambda s: s.query("SELECT status FROM deliveries WHERE delivery_guid = 'drag'")
        )
        assert status[0][0] == "unresolved"
        await wait_for_async(
            lambda: any(
                e.preconditions.session_id == session.session_id
                for e in omnigent.executed(EffectKind.INTERRUPT_TREE)
            )
        )
        await wait_for_async(lambda: not broker.issuance_enabled(session.session_id))
        parcel = await service.db.call(lambda s: s.load_parcel("P"))
        assert parcel is not None
        assert parcel.inbox_holds == (InboxHold("drag", InboxHoldReason.UNRESOLVED),)
        fenced = parcel.session(session.session_id)
        assert fenced is not None and FenceKind.SAFETY in fenced.fences

        assert await service.release_resolved_inbox_holds() == 0  # still unresolved
        await service.db.call(lambda s: s.mark_delivery("drag", "processed"))
        await wait_for_async(
            lambda: service.db.call(lambda s: not s.load_parcel("P").inbox_holds)  # type: ignore[union-attr]
        )
        parcel = await service.db.call(lambda s: s.load_parcel("P"))
        assert parcel is not None and Hold.INBOX not in parcel.holds
    finally:
        await service.stop()


async def test_unresolved_delivery_without_known_parcel_only_marks_status(
    service_config: ServiceConfig,
):
    service, _, _, _ = make(service_config)
    await service.start()
    try:
        await service.persist_delivery(DeliveryRecord("other", "projects_v2_item", b"{}", {}))
        assert not await service.hold_unresolved_delivery("other", "I_unknown")
        assert not await service.hold_unresolved_delivery("other", None)
        assert await service.db.call(lambda s: s.load_parcel("I_unknown")) is None
        rows = await service.db.call(
            lambda s: s.query("SELECT status FROM deliveries WHERE delivery_guid = 'other'")
        )
        assert rows[0][0] == "unresolved"
    finally:
        await service.stop()
