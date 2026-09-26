from __future__ import annotations

import asyncio
import os
import stat
from collections.abc import Callable

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import AmbiguousWrite, EffectKind
from omnigent_factory.core.types import QueueStatus, Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.operator import operator_request
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)


async def eventually(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= end:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_operator_socket_pause_is_private_and_persists_across_restart(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    first = FactoryService(service_config, clock=clock)
    await first.start()
    try:
        assert stat.S_IMODE(service_config.operator_socket.stat().st_mode) == 0o600
        response = await operator_request(service_config.operator_socket, "pause")
        assert response["ok"] is True
        assert response["paused"] is True
    finally:
        await first.stop()
    assert not service_config.operator_socket.exists()

    second = FactoryService(service_config, clock=clock)
    await second.start()
    try:
        health = await second.health()
        assert health == {"status": "ok", "paused": True}
        response = await operator_request(service_config.operator_socket, "unpause")
        assert response["paused"] is False
    finally:
        await second.stop()


@pytest.mark.asyncio
async def test_outbox_executes_task1_fakes_through_first_message(service_config: ServiceConfig):
    clock = FakeClock()
    github = FakeGitHub()
    omnigent = FakeOmnigent()
    credentials = FakeCredentialBroker()
    service = FactoryService(
        service_config,
        adapters=(github, omnigent, credentials),
        clock=clock,
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        factory = EventFactory("P1")
        await service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))
        await eventually(lambda: bool(omnigent.executed(EffectKind.SEND_MESSAGE)))
        parcel = await service.db.call(lambda store: store.load_parcel("P1"))
        assert parcel is not None
        assert parcel.current_session is not None
        assert parcel.current_session.root_id is not None
        assert parcel.current_session.prepared
        assert credentials.issuance_enabled(parcel.current_session.session_id)
        assert all(
            effect.state == "done"
            for effect in await service.db.call(lambda store: store.effects_in_state("done"))
        )
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_ambiguous_external_write_becomes_unknown_and_is_not_retried(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    github = FakeGitHub()
    omnigent = FakeOmnigent()
    omnigent.script(EffectKind.CREATE_SESSION, AmbiguousWrite("lost response"))
    service = FactoryService(
        service_config,
        adapters=(github, omnigent, FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        factory = EventFactory("P-unknown")
        await service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await service.apply_event(factory.make(ev.RequestTriage()))

        async def unknown_seen() -> bool:
            rows = await service.db.call(lambda store: store.effects_in_state("unknown"))
            return any(row.effect.kind == EffectKind.CREATE_SESSION for row in rows)

        end = asyncio.get_running_loop().time() + 2
        while not await unknown_seen():
            if asyncio.get_running_loop().time() >= end:
                raise AssertionError("ambiguous effect was not persisted unknown")
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert len(omnigent.executed(EffectKind.CREATE_SESSION)) == 1
        parcel = await service.db.call(lambda store: store.load_parcel("P-unknown"))
        assert parcel is not None
        assert parcel.current_session is not None
        assert parcel.current_session.lifecycle.value == "UNKNOWN"
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_admission_scheduler_honours_persisted_fifo_and_cap(service_config: ServiceConfig):
    clock = FakeClock()
    service = FactoryService(service_config, clock=clock)
    await service.start()
    try:
        await service.operator_command("unpause", {})
        for issue_number, parcel_id in enumerate(("P-first", "P-second"), start=1):
            factory = EventFactory(parcel_id, issue_number=issue_number)
            await service.apply_event(
                factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
            )
            await service.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)))
            clock.advance(1)

        async def first_reserved() -> bool:
            admission = await service.db.call(
                lambda store: store.load_admission(service_config.repo_id)
            )
            first = admission.queue_entry("P-first")
            return first is not None and first.status == QueueStatus.RESERVED

        end = asyncio.get_running_loop().time() + 2
        while not await first_reserved():
            if asyncio.get_running_loop().time() >= end:
                raise AssertionError("queue head was not admitted")
            clock.advance(1)
            await asyncio.sleep(0.01)
        admission = await service.db.call(
            lambda store: store.load_admission(service_config.repo_id)
        )
        assert admission.queue_entry("P-first").status == QueueStatus.RESERVED  # type: ignore[union-attr]
        assert admission.queue_entry("P-second").status == QueueStatus.QUEUED  # type: ignore[union-attr]
        assert admission.building_count == 1
    finally:
        await service.stop()


def test_restart_recovers_claimed_write_as_unknown_without_blind_retry(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    factory = EventFactory("P-restart")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), service_config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        service_config.trusted,
    )
    store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), service_config.trusted)
    create = next(
        row for row in store.pending_effects() if row.effect.kind == EffectKind.CREATE_SESSION
    )
    lease = store.acquire_lease("P-restart", "dead-boot")
    assert store.claim_effect(create.effect.effect_id, lease) is not None
    store.close()

    async def check() -> None:
        service = FactoryService(service_config, clock=clock)
        await service.start()
        try:
            recovery = await service.operator_command("recovery", {})
            assert any(
                row["effect_id"] == create.effect.effect_id and row["state"] == "unknown"
                for row in recovery["effects"]  # type: ignore[union-attr]
            )
            parcel = await service.db.call(lambda db: db.load_parcel("P-restart"))
            assert parcel is not None
            assert parcel.current_session is not None
            assert parcel.current_session.lifecycle.value == "UNKNOWN"
            assert parcel.current_session.grant.duration_us > 0
        finally:
            await service.stop()

    asyncio.run(check())


@pytest.mark.asyncio
async def test_graceful_shutdown_releases_lock_and_removes_socket(service_config: ServiceConfig):
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    assert service.ready
    await service.stop()
    assert not service.ready
    assert not service_config.operator_socket.exists()
    lock_fd = os.open(service_config.state_dir / "daemon.lock", os.O_RDONLY)
    os.close(lock_fd)
