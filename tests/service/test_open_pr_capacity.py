"""Repository open-PR capacity repairs missed close webhooks and stale reservations."""

from __future__ import annotations

import json

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.types import QueueStatus, ReservationKind
from omnigent_factory.ports.github import PullRequestState
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
DONE = "I_done_677"


def _seed(config: ServiceConfig, h: Harness) -> None:
    store = SqliteStore.open(config.database_path, FakeClock())
    try:
        store.ensure_repository(config.trusted)
        store.query("UPDATE repositories SET paused = 0")
        for event, _result in h.log:
            store.apply_event(event, h.cfg)
        store.query("UPDATE effects SET state = 'done'")
    finally:
        store.close()


@pytest.mark.asyncio
async def test_reconcile_heals_closed_pr_and_done_reservation_then_admits_head(
    service_config: ServiceConfig,
) -> None:
    config = service_config.model_copy(update={"max_open_bot_prs": 10, "max_building": 2})
    h = Harness(cfg=config.trusted)
    h.eligible(DONE)
    h.send(DONE, ev.Closed())
    h.plan_published(P)
    h.approve(P)
    _seed(config, h)

    tracked = [807, *range(1001, 1009)]
    store = SqliteStore.open(config.database_path, FakeClock())
    try:
        store.query(
            "UPDATE repositories SET open_bot_prs_json = ? WHERE repo_id = ?",
            (json.dumps(tracked), config.repo_id),
        )
        store.query(
            "INSERT INTO reservations (reservation_id, repo_id, parcel_id, kind, episode_id, "
            "pr_number, live) VALUES (?, ?, ?, ?, ?, ?, 1)",
            ("rs-done-677", config.repo_id, DONE, ReservationKind.OPEN_PR.value, "old", 682),
        )
    finally:
        store.close()

    service = FactoryService(config, clock=FakeClock())
    reads: list[int] = []

    async def state(number: int) -> PullRequestState:
        reads.append(number)
        return PullRequestState(open=number not in {682, 807}, merged=number == 682)

    service.open_pr_reader = state
    await service.open()
    try:
        before = await service.db.call(lambda db: db.load_admission(config.repo_id))
        assert before.prospective_pr_count == 10
        assert await service.reconcile_open_pr_capacity(force=True) == 10
        healed = await service.db.call(lambda db: db.load_admission(config.repo_id))
        assert 807 not in healed.open_bot_prs
        stale_live = await service.db.call(
            lambda db: int(
                db.query(
                    "SELECT live FROM reservations WHERE reservation_id = ?",
                    ("rs-done-677",),
                )[0][0]
            )
        )
        assert not stale_live and healed.prospective_pr_count == 8

        await service._admit_once()
        admitted = await service.db.call(lambda db: db.load_admission(config.repo_id))
        assert admitted.queue_entry(P).status == QueueStatus.RESERVED
        assert {682, 807} <= set(reads)
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_reconcile_never_expires_unbound_reservation_of_live_build(
    service_config: ServiceConfig,
) -> None:
    config = service_config.model_copy(update={"max_open_bot_prs": 10, "max_building": 2})
    h = Harness(cfg=config.trusted)
    h.to_building(P)
    _seed(config, h)
    service = FactoryService(config, clock=FakeClock())

    async def state(_number: int) -> PullRequestState:  # pragma: no cover - no bound PR
        raise AssertionError("an unbound reservation has no PR to read")

    service.open_pr_reader = state
    await service.open()
    try:
        assert await service.reconcile_open_pr_capacity(force=True) == 0
        admission = await service.db.call(lambda db: db.load_admission(config.repo_id))
        own = [r for r in admission.live_reservations(ReservationKind.OPEN_PR) if r.parcel_id == P]
        assert len(own) == 1 and own[0].pr_number is None
        assert admission.building_count == 1
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_rate_limit_defers_the_next_open_pr_reconcile(service_config: ServiceConfig) -> None:
    config = service_config.model_copy(update={"reconcile_interval_seconds": 60})
    h = Harness(cfg=config.trusted)
    h.eligible(P)
    _seed(config, h)
    store = SqliteStore.open(config.database_path, FakeClock())
    try:
        store.query(
            "UPDATE repositories SET open_bot_prs_json = ? WHERE repo_id = ?",
            (json.dumps([807]), config.repo_id),
        )
    finally:
        store.close()

    clock = FakeClock()
    service = FactoryService(config, clock=clock)
    reads = 0

    async def limited(_number: int) -> RetryableReadFailure:
        nonlocal reads
        reads += 1
        return RetryableReadFailure("rate limited", retry_after_us=120_000_000)

    service.open_pr_reader = limited
    await service.open()
    try:
        assert await service.reconcile_open_pr_capacity(force=True) == 1
        assert service._next_open_pr_reconcile_us == clock.now_utc_us() + 120_000_000
        assert await service.reconcile_open_pr_capacity() == 0
        admission = await service.db.call(lambda db: db.load_admission(config.repo_id))
        assert admission.open_bot_prs == frozenset({807}) and reads == 1
    finally:
        await service.stop()
