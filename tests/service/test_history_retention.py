"""Daemon side of history retention: completed-parcel reconcile cadence, prune, vacuum."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest

from omnigent_factory import cli
from omnigent_factory.core import events as ev
from omnigent_factory.core.types import Hold
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.locking import ProcessLock
from omnigent_factory.service.runtime import FactoryService, _completed_idle, prune_observations
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.test_store import store_harness

DAY_US = 86_400 * 1_000_000


async def _reconciles(service: FactoryService, parcel_id: str) -> int:
    rows = await service.db.call(
        lambda store: store.query(
            "SELECT COUNT(*) FROM events WHERE parcel_id = ? AND kind = ?",
            (parcel_id, ev.EventKind.RECONCILE_DUE.value),
        )
    )
    return int(rows[0][0])


def test_completed_idle_needs_completion_without_live_work():
    h = Harness()
    h.send("I_parcel_1", ev.Closed())
    done = h.p("I_parcel_1")
    assert Hold.COMPLETED in done.holds and _completed_idle(done)
    assert not _completed_idle(replace(done, holds=done.holds - {Hold.COMPLETED}))
    building = Harness()
    building.to_building()
    assert not _completed_idle(building.p("I_parcel_1"))


@pytest.mark.asyncio
async def test_completed_parcels_reconcile_on_the_slow_cadence(service_config: ServiceConfig):
    config = service_config.model_copy(
        update={"reconcile_interval_seconds": 0.02, "completed_reconcile_interval_seconds": 3600}
    )
    clock = FakeClock()
    service = FactoryService(config, clock=clock)
    await service.start()
    try:
        factories = {
            pid: EventFactory(pid, issue_number=n) for pid, n in (("P-open", 1), ("P-done", 2))
        }
        for f in factories.values():
            await service.apply_event(
                f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now))
            )
        closed = await service.apply_event(factories["P-done"].make(ev.Closed()))
        assert closed.parcel is not None and _completed_idle(closed.parcel)
        start_done = await _reconciles(service, "P-done")
        start_open = await _reconciles(service, "P-open")
        end = asyncio.get_running_loop().time() + 3
        while await _reconciles(service, "P-open") < start_open + 5:
            assert asyncio.get_running_loop().time() < end, "active parcel not reconciled"
            clock.advance(1)  # reconcile event IDs carry the clock
            await asyncio.sleep(0.02)
        # At most the one first reconcile after completion, never one per cycle.
        assert await _reconciles(service, "P-done") <= start_done + 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_operator_prune_dry_run_reports_and_real_run_removes(service_config: ServiceConfig):
    clock = FakeClock()
    config = service_config.model_copy(update={"reconcile_interval_seconds": 3600})
    service = FactoryService(config, clock=clock)
    await service.start()
    try:
        for i in range(5):
            await service.apply_event(
                service._event("P-1", ev.ReconcileDue(), f"reconcile:P-1:{i}")
            )
        await service.db.call(lambda store: store.query("UPDATE effects SET state = 'done'"))
        clock.advance(2 * DAY_US)
        before = await _reconciles(service, "P-1")
        dry = await service.operator_command("prune", {"dry_run": True})
        assert dry["events"] >= 4 and dry["effects"] >= 4
        assert await _reconciles(service, "P-1") == before
        real = await service.operator_command("prune", {})
        assert (real["events"], real["effects"]) == (dry["events"], dry["effects"])
        assert await _reconciles(service, "P-1") == before - real["events"] >= 1
    finally:
        await service.stop()


def test_cli_prune_and_vacuum_work_directly_on_a_stopped_daemons_file(
    service_config: ServiceConfig, monkeypatch, capsys
):
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.query("PRAGMA auto_vacuum=0")
    store.close()
    monkeypatch.setattr(cli, "_load", lambda _args: (None, service_config))
    assert cli.main(["prune", "--dry-run"]) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["dry_run"] is True and dry["events"] == 0
    assert cli.main(["vacuum"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_cli_vacuum_refuses_while_the_daemon_holds_its_lock(
    service_config: ServiceConfig, monkeypatch, capsys
):
    monkeypatch.setattr(cli, "_load", lambda _args: (None, service_config))
    with ProcessLock(service_config.state_dir):
        assert cli.main(["vacuum"]) == 1
    assert "stop the daemon" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_a_bounded_sweep_stops_after_max_batches(tmp_path):
    clock = FakeClock()
    store = SqliteStore.open(tmp_path / "s.sqlite3", clock)
    h = store_harness(store)
    for i in range(12):
        store.apply_event(
            h.f("P-1").make(ev.ReconcileDue(), event_id=f"reconcile:P-1:{i}", evidence=None),
            h.cfg,
        )
    store.query("UPDATE effects SET state = 'done'")
    clock.advance(DAY_US)

    async def call(operation):
        return operation(store)

    bounded = await prune_observations(call, clock.now_utc_us(), limit=5, max_batches=1)
    assert bounded["events"] == 5
    rest = await prune_observations(call, clock.now_utc_us(), limit=5)
    assert rest["events"] == 6  # the newest wake-up stays
    store.close()
