from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import QueueStatus, Via
from omnigent_factory.service.config import load_config
from omnigent_factory.service.operator import operator_request
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID, EventFactory, snapshot
from omnigent_factory.testing.fakes import FakeClock


def _write(path: Path, tmp_path: Path, **overrides: object) -> None:
    values: dict[str, object] = {
        "state_dir": str(tmp_path / "state"),
        "secrets_dir": str(tmp_path / "secrets"),
        "app_env": str(tmp_path / "absent.env"),
        "repo_id": REPO_ID,
        "owners": [OWNER_ID],
        "effect_poll_seconds": 0.01,
        "clock_interval_seconds": 0.01,
        "reconcile_interval_seconds": 60,
        "max_building": 1,
    }
    values.update(overrides)
    lines = ["[service]"]
    for key, value in values.items():
        rendered = f'"{value}"' if isinstance(value, str) else str(value)
        lines.append(f"{key} = {rendered}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    for name in ("state", "secrets"):
        (tmp_path / name).mkdir(mode=0o700)
        os.chmod(tmp_path / name, 0o700)
    path = tmp_path / "config.toml"
    _write(path, tmp_path)
    return path


async def _queue(service: FactoryService, clock: FakeClock, parcel_id: str, issue: int) -> None:
    factory = EventFactory(parcel_id, issue_number=issue)
    await service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    await service.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)))
    clock.advance(1)


async def _status_of(service: FactoryService, parcel_id: str) -> QueueStatus | None:
    admission = await service.db.call(lambda store: store.load_admission(REPO_ID))
    entry = admission.queue_entry(parcel_id)
    return None if entry is None else entry.status


async def _until_reserved(service: FactoryService, clock: FakeClock, parcel_id: str) -> None:
    end = asyncio.get_running_loop().time() + 3
    while await _status_of(service, parcel_id) != QueueStatus.RESERVED:
        if asyncio.get_running_loop().time() >= end:
            raise AssertionError(f"{parcel_id} was not admitted")
        clock.advance(1)
        await asyncio.sleep(0.01)


async def _building(service: FactoryService) -> int:
    admission = await service.db.call(lambda store: store.load_admission(REPO_ID))
    return admission.building_count


@pytest.mark.asyncio
async def test_reload_applies_hot_keys_rejects_others_and_never_stops_builds(
    config_file: Path, tmp_path: Path
):
    clock = FakeClock()
    service = FactoryService(load_config(config_file), clock=clock)
    service.config_path = config_file
    await service.start()
    try:
        socket = service.config.operator_socket
        await service.operator_command("unpause", {})
        response = await operator_request(socket, "reload")
        assert response["ok"] is True
        assert response["message"] == "no changes"
        assert response["building_cap"] == 1

        await _queue(service, clock, "P-first", 1)
        await _queue(service, clock, "P-second", 2)
        await _until_reserved(service, clock, "P-first")
        for _ in range(20):
            clock.advance(1)
            await asyncio.sleep(0.01)
        assert await _status_of(service, "P-second") == QueueStatus.QUEUED

        # Raising the cap (hot key) admits the queued parcel without a restart.
        _write(
            config_file,
            tmp_path,
            max_building=2,
            triage_guidance="Use area labels.",
            checkpoint_grace_minutes=20,
        )
        response = await operator_request(socket, "reload")
        assert response["ok"] is True
        assert response["changed"] == [
            "checkpoint_grace_minutes",
            "max_building",
            "triage_guidance",
        ]
        assert response["building_cap"] == 2
        assert service.executor._config.grace_us == 20 * 60 * 1_000_000
        await _until_reserved(service, clock, "P-second")
        assert await _building(service) == 2

        # A restart-only key refuses the whole reload, hot keys included.
        _write(config_file, tmp_path, max_building=3, bind_port=9999)
        response = await operator_request(socket, "reload")
        assert response["ok"] is False
        assert response["message"] == "restart required: bind_port; nothing applied"
        assert service.config.max_building == 2

        # An invalid file changes nothing.
        _write(config_file, tmp_path, max_building=0)
        response = await operator_request(socket, "reload")
        assert response["ok"] is False
        assert response["message"].startswith("invalid config, nothing applied")
        assert service.config.max_building == 2

        # Lowering the cap below the building count keeps running builds and only
        # blocks new admissions (applied via SIGHUP).
        _write(config_file, tmp_path, max_building=1)
        os.kill(os.getpid(), signal.SIGHUP)
        end = asyncio.get_running_loop().time() + 3
        while service.config.max_building != 1:
            assert asyncio.get_running_loop().time() < end, "SIGHUP reload did not apply"
            await asyncio.sleep(0.01)
        await _queue(service, clock, "P-third", 3)
        for _ in range(20):
            clock.advance(1)
            await asyncio.sleep(0.01)
        assert await _building(service) == 2
        assert await _status_of(service, "P-first") == QueueStatus.RESERVED
        assert await _status_of(service, "P-second") == QueueStatus.RESERVED
        assert await _status_of(service, "P-third") == QueueStatus.QUEUED
        status = await operator_request(socket, "status")
        assert status["building_cap"] == 1
        assert status["building"] == 2
    finally:
        await service.stop()
