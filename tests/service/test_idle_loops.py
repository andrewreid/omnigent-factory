"""An idle factory sleeps: no DB polling, parked runs scanned every few minutes.

The background loops (outbox, admission, clock) wake on a committed change, an operator
command, a config reload or their earliest deadline, with a slow safety fallback; they
never poll the store every tick. Parked runs and runs waiting on the owner, CI or the
review bot are scanned on ``parked_observation_interval_seconds``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any, TypeVar

import pytest

from omnigent_factory.core import codec
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions, RetryClass
from omnigent_factory.core.predicates import run_closed
from omnigent_factory.core.types import BotState, Lifecycle, Parcel
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.service.board_diff import BoardDiff
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.fakes import FakeClock, FakeCredentialBroker, FakeOmnigent
from omnigent_factory.testing.harness import Harness

from .quiet_support import (
    CountingGitHub,
    CountingOmnigent,
    build_population,
    card_for,
    populate,
)

T = TypeVar("T")
#: Builds parked Blocked (run reported blocked, tree idle), like the live factory's two.
PARKED_BUILDS = ("I_live_build", "I_parked_build")


def _park(h: Harness, pid: str) -> None:
    s = h.cur(pid)
    r = h.send(pid, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    assert r.audit.accepted, r.audit.reason
    h.quiesce(pid, s.session_id)
    assert h.p(pid).bot == BotState.BLOCKED and h.p(pid).slot_parked


def _parked_population() -> Harness:
    harness, _population = build_population()
    _park(harness, "I_live_build")  # frees its building slot for the second build
    harness.plan_published("I_parked_build")
    harness.approve("I_parked_build")
    harness.admit("I_parked_build")
    _park(harness, "I_parked_build")
    return harness


def _seed(config: ServiceConfig) -> CountingGitHub:
    harness = _parked_population()
    store = SqliteStore.open(config.database_path, SystemClock())
    try:
        store.ensure_repository(config.trusted)
        populate(store, harness)
        store.query("UPDATE effects SET state = 'done'")  # the history ran long ago
        github = CountingGitHub(cards=[card_for(p) for p in harness.parcels.values()])
        store.save_board_digests({card.node_id: card.digest for card in github.cards})
    finally:
        store.close()
    return github


@dataclass
class Counted:
    service: FactoryService
    calls: list[str]


@asynccontextmanager
async def idle_factory(config: ServiceConfig) -> AsyncIterator[Counted]:
    github = _seed(config)
    service = FactoryService(
        config, adapters=(github, FakeOmnigent(), FakeCredentialBroker()), clock=SystemClock()
    )
    service.board_diff = BoardDiff(service, github.board_cards)
    calls: list[str] = []
    inner = service.db.call

    async def counting(operation: Callable[[SqliteStore], T]) -> T:
        calls.append(asyncio.current_task().get_name())  # type: ignore[union-attr]
        return await inner(operation)

    service.db.call = counting  # type: ignore[method-assign]
    await service.start()
    try:
        yield Counted(service, calls)
    finally:
        await service.stop()


def _production_cadence(config: ServiceConfig) -> ServiceConfig:
    """The shipped defaults (the shared fixture shortens them for other tests)."""
    return config.model_copy(
        update={
            name: ServiceConfig.model_fields[name].default
            for name in (
                "effect_poll_seconds",
                "clock_interval_seconds",
                "reconcile_interval_seconds",
                "board_diff_interval_minutes",
                "delivery_idle_poll_seconds",
            )
        }
    )


@pytest.mark.asyncio
async def test_an_idle_factory_makes_almost_no_db_calls(service_config: ServiceConfig) -> None:
    """5 s idle at production cadence with 142 parcels, two builds parked.

    Before (base): the outbox read the store every 50 ms and the admission and clock
    loops every second (plus one parcel load per clocked run): 141 DB calls in 5 s
    (~28/s). Now: only the 5 s delivery-inbox safety poll (2 calls).
    """
    config = _production_cadence(service_config)
    async with idle_factory(config) as rig:
        await asyncio.sleep(2.0)  # boot work (reads, board diff) settles
        before = len(rig.calls)
        await asyncio.sleep(5.0)
        idle_calls = rig.calls[before:]
    print(f"idle DB calls in 5 s: {len(idle_calls)} by task {sorted(set(idle_calls))}")
    assert len(idle_calls) <= 10, sorted(idle_calls)
    assert not {"outbox", "admission", "clock"} & set(idle_calls)


@pytest.mark.asyncio
async def test_a_new_effect_still_runs_at_once(service_config: ServiceConfig) -> None:
    """Idle loops still react within the busy cadence: an enqueued effect is executed
    and a deferred retry runs at its due time, not at the safety fallback."""
    config = _production_cadence(service_config).model_copy(update={"idle_fallback_seconds": 60.0})
    async with idle_factory(config) as rig:
        await asyncio.sleep(1.5)
        executed: list[tuple[str, float]] = []
        loop = asyncio.get_running_loop()

        class Recorder:
            handled_kinds = frozenset({EffectKind.ARM_TIMER})

            async def execute(self, effect: EffectIntent, ctx: Any) -> Any:
                del ctx
                from omnigent_factory.core.effects import Ack

                executed.append((effect.effect_id, loop.time()))
                return Ack()

        executor = rig.service.executor
        executor._adapters[EffectKind.ARM_TIMER] = Recorder()  # type: ignore[assignment]

        def enqueue(effect_id: str, next_at_us: int | None) -> Callable[[SqliteStore], None]:
            def write(store: SqliteStore) -> None:
                intent = EffectIntent(
                    effect_id=effect_id,
                    kind=EffectKind.ARM_TIMER,
                    parcel_id=None,
                    target="repo",
                    preconditions=Preconditions(0, 0),
                    retry_class=RetryClass.READ,
                    dedupe_key=effect_id,
                )
                event_id = store.query("SELECT event_id FROM events LIMIT 1")[0][0]
                store.query(
                    "INSERT INTO effects (effect_id, parcel_id, event_id, parcel_version, "
                    "kind, target, payload_json, retry_class, dedupe_key, state, next_at_us, "
                    "created_at_us, updated_at_us) "
                    "VALUES (?, NULL, ?, 0, ?, 'repo', ?, ?, ?, 'pending', ?, 0, 0)",
                    (
                        effect_id,
                        event_id,
                        intent.kind.value,
                        codec.effect_to_json(intent),
                        intent.retry_class.value,
                        effect_id,
                        next_at_us,
                    ),
                )

            return write

        started_at = loop.time()
        await rig.service.db.call(enqueue("ef_now", None))
        due_us = SystemClock().now_utc_us() + 1_500_000
        await rig.service.db.call(enqueue("ef_later", due_us))
        for _ in range(300):
            if len(executed) == 2:
                break
            await asyncio.sleep(0.01)
    times = dict(executed)
    assert times["ef_now"] - started_at < 0.1
    assert 1.4 < times["ef_later"] - started_at < 1.8


@pytest.mark.asyncio
async def test_parked_runs_are_scanned_every_five_minutes() -> None:
    """10 simulated minutes of 5 s observer passes over the open runs of an idle
    factory: two parked Blocked builds, three plans awaiting approval and an open
    question (all trees idle), and one working triage.

    Before (base): the parked builds were scanned every pass and the owner-waiting
    plans/question every minute: 92.8 Omnigent requests per minute. Now only the
    working triage keeps the 5 s cadence: 39.2 per minute.
    """
    harness = _parked_population()
    open_runs = [
        p
        for p in harness.parcels.values()
        if p.current_session is not None
        and p.current_session.root_id is not None
        and not run_closed(p, p.current_session)
    ]
    clock = FakeClock()
    omnigent = CountingOmnigent()
    observer = OmnigentObserver(
        None,  # type: ignore[arg-type]
        omnigent,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        clock,
        interval_seconds=5,
        settled_interval_seconds=60,
    )
    observer.parked_interval_seconds = 300  # the default (hot-reloadable)

    async def parcels() -> list[Parcel]:
        return open_runs

    async def observe(parcel: Parcel, observation: Any) -> None:
        cur = parcel.current_session
        assert cur is not None
        busy = cur.root_id in omnigent.busy_roots
        observer._last_tree[cur.session_id] = (True, busy, False)

    observer._parcels = parcels  # type: ignore[method-assign]
    observer._observe = observe  # type: ignore[method-assign]
    triage = harness.p("I_live_triage").current_session
    assert triage is not None
    omnigent.busy_roots.add(str(triage.root_id))  # the one run still working
    for _ in range(120):  # 10 minutes
        await observer.observe_once()
        clock.advance(5_000_000)
    per_minute = omnigent.requests / 10
    print(f"Omnigent requests per simulated minute: {per_minute:.1f}")
    for pid in PARKED_BUILDS:
        root = harness.p(pid).current_session.root_id  # type: ignore[union-attr]
        assert omnigent.scans[str(root)] == 2  # first read, then once every five minutes
    assert omnigent.scans[str(triage.root_id)] == 120  # running: every pass
    assert per_minute < 40


def test_a_parked_run_seen_busy_returns_to_the_active_cadence() -> None:
    """Fail-closed: never parked before this process saw its tree complete and idle;
    a busy or incomplete last scan, or a running/draining lifecycle, is never parked."""
    harness = _parked_population()
    observer = OmnigentObserver(
        None,  # type: ignore[arg-type]
        CountingOmnigent(),  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        FakeClock(),
        interval_seconds=5,
    )
    plan = harness.p("I_live_plan_0")
    run = plan.current_session
    assert run is not None
    assert not observer._parked(plan, run)  # never observed yet
    observer._last_tree[run.session_id] = (False, False, False)  # incomplete scan
    assert not observer._parked(plan, run)
    observer._last_tree[run.session_id] = (True, True, False)  # busy
    assert not observer._parked(plan, run)
    observer._last_tree[run.session_id] = (True, False, False)
    assert observer._parked(plan, run)
    draining = replace(run, lifecycle=Lifecycle.DRAINING)
    assert not observer._parked(plan, draining)
    triage = harness.p("I_live_triage")
    triage_run = triage.current_session
    assert triage_run is not None and triage.bot == BotState.WORKING
    observer._last_tree[triage_run.session_id] = (True, False, False)
    assert not observer._parked(triage, triage_run)  # working, between turns


def test_idle_cadence_keys_are_hot_reloadable() -> None:
    fields = ServiceConfig.model_fields
    assert fields["parked_observation_interval_seconds"].default == 300.0
    assert fields["idle_fallback_seconds"].default == 30.0
    assert {"parked_observation_interval_seconds", "idle_fallback_seconds"} <= HOT_RELOAD_KEYS
