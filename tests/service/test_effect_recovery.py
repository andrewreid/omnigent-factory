from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    AUTOMATIC_RECOVERY_KINDS,
    MARKER_ADOPTABLE_CREATE_KINDS,
    NON_IDEMPOTENT_AMBIGUOUS_KINDS,
    Ack,
    AmbiguousWrite,
    EffectKind,
    RetryableReadFailure,
)
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.projection import NOTE_MAX
from omnigent_factory.core.types import BotState, Stage, Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import FakeClock, FakeCredentialBroker, FakeGitHub, FakeOmnigent


async def _eventually(check: Callable[[], Awaitable[bool]], timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await check():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.01)


async def _start_triage(service: FactoryService, parcel_id: str) -> str:
    await service.operator_command("unpause", {})
    factory = EventFactory(parcel_id)
    await service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    result = await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))
    return next(effect.effect_id for effect in result.effects if effect.kind == EffectKind.SET_BOT)


async def _start_moving_triage(service: FactoryService, parcel_id: str) -> tuple[EventFactory, str]:
    await service.operator_command("unpause", {})
    factory = EventFactory(parcel_id)
    await service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    result = await service.apply_event(factory.make(ev.RequestTriage(via=Via.COMMAND)))
    move = next(effect for effect in result.effects if effect.kind == EffectKind.MOVE_CARD)
    return factory, move.effect_id


def _fast_recovery(config: ServiceConfig, *, attempts: int = 5) -> ServiceConfig:
    return config.model_copy(
        update={
            "effect_recovery_max_attempts": attempts,
            "effect_recovery_backoff_seconds": 1.0,
            "effect_recovery_max_backoff_seconds": 8.0,
        }
    )


@pytest.mark.asyncio
async def test_1529_set_bot_timeout_recovers_without_showing_blocked(
    service_config: ServiceConfig,
) -> None:
    config = _fast_recovery(service_config)
    clock = FakeClock()
    github = FakeGitHub()
    github.script(EffectKind.SET_BOT, AmbiguousWrite("ReadTimeout"), Ack("item-1"))
    service = FactoryService(
        config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        effect_id = await _start_triage(service, "P-1529")

        async def retry_is_pending() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "pending" and row.attempts == 1

        await _eventually(retry_is_pending)
        parcel = await service.db.call(lambda db: db.load_parcel("P-1529"))
        assert parcel is not None
        assert parcel.bot == BotState.WORKING
        assert parcel.unknown_effects == ()
        assert "Blocked" not in parcel.board_note

        clock.advance(1_000_001)
        service.db.poke()

        async def recovered() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "done"

        await _eventually(recovered)
        parcel = await service.db.call(lambda db: db.load_parcel("P-1529"))
        assert parcel is not None and parcel.unknown_effects == ()
        assert parcel.bot != BotState.BLOCKED
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_unknown_move_superseded_by_owner_drag_is_not_reapplied(
    service_config: ServiceConfig,
) -> None:
    config = _fast_recovery(service_config)
    clock = FakeClock()
    github = FakeGitHub()
    github.script(EffectKind.MOVE_CARD, AmbiguousWrite("ReadTimeout"), Ack("item-1"))
    service = FactoryService(
        config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        factory, stale_id = await _start_moving_triage(service, "P-move-owner")

        async def retry_is_pending() -> bool:
            row = await service.db.call(lambda db: db.get_effect(stale_id))
            return row is not None and row.state == "pending" and row.attempts == 1

        await _eventually(retry_is_pending)
        await service.apply_event(
            factory.make(
                ev.RequestPlan(via=Via.DRAG, board_from=Stage.TRIAGED),
                evidence=snapshot(stage=Stage.SCOPED, read_at_us=factory.now),
            )
        )
        parcel = await service.db.call(lambda db: db.load_parcel("P-move-owner"))
        assert parcel is not None and parcel.stage == Stage.SCOPED

        clock.advance(1_000_001)
        service.db.poke()

        async def latest_target_landed() -> bool:
            row = await service.db.call(lambda db: db.get_effect(stale_id))
            parcel = await service.db.call(lambda db: db.load_parcel("P-move-owner"))
            return (
                row is not None
                and row.state == "done"
                and parcel is not None
                and parcel.stage == Stage.SCOPED
                and not parcel.pending_moves
            )

        await _eventually(latest_target_landed)
        stale_calls = [
            effect
            for effect in github.executed(EffectKind.MOVE_CARD)
            if effect.effect_id == stale_id
        ]
        assert len(stale_calls) == 1  # the ambiguous first call only: no stale retry
        parcel = await service.db.call(lambda db: db.load_parcel("P-move-owner"))
        assert parcel is not None and parcel.bot != BotState.BLOCKED
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_unknown_move_is_retried_while_its_target_is_current(
    service_config: ServiceConfig,
) -> None:
    config = _fast_recovery(service_config)
    clock = FakeClock()
    github = FakeGitHub()
    github.script(EffectKind.MOVE_CARD, AmbiguousWrite("ReadTimeout"), Ack("item-1"))
    service = FactoryService(
        config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        _, effect_id = await _start_moving_triage(service, "P-move-current")

        async def retry_is_pending() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "pending" and row.attempts == 1

        await _eventually(retry_is_pending)
        clock.advance(1_000_001)
        service.db.poke()

        async def landed() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            parcel = await service.db.call(lambda db: db.load_parcel("P-move-current"))
            return (
                row is not None
                and row.state == "done"
                and parcel is not None
                and parcel.stage == Stage.TRIAGED
                and not parcel.pending_moves
            )

        await _eventually(landed)
        calls = [
            effect
            for effect in github.executed(EffectKind.MOVE_CARD)
            if effect.effect_id == effect_id
        ]
        assert len(calls) == 2
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_repeated_timeouts_back_off_then_escalate_with_specific_note(
    service_config: ServiceConfig,
) -> None:
    config = _fast_recovery(service_config, attempts=3)
    clock = FakeClock()
    github = FakeGitHub()
    github.script(
        EffectKind.SET_BOT,
        AmbiguousWrite("timeout-1"),
        AmbiguousWrite("timeout-2"),
        AmbiguousWrite("timeout-3"),
    )
    service = FactoryService(
        config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        effect_id = await _start_triage(service, "P-exhaust")
        expected_delay = 1_000_000
        for attempt in (1, 2):

            async def pending_at_attempt(attempt: int = attempt) -> bool:
                row = await service.db.call(lambda db: db.get_effect(effect_id))
                return row is not None and row.state == "pending" and row.attempts == attempt

            await _eventually(pending_at_attempt)
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            assert row is not None
            assert row.next_at_us == clock.now_utc_us() + expected_delay
            parcel = await service.db.call(lambda db: db.load_parcel("P-exhaust"))
            assert parcel is not None and parcel.bot != BotState.BLOCKED
            clock.advance(expected_delay + 1)
            service.db.poke()
            expected_delay *= 2

        async def exhausted() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "unknown"

        await _eventually(exhausted)
        parcel = await service.db.call(lambda db: db.load_parcel("P-exhaust"))
        assert parcel is not None and parcel.bot == BotState.BLOCKED
        assert len(parcel.board_note) <= NOTE_MAX
        assert parcel.board_note == (
            f"Blocked: GitHub Bot field update unconfirmed after 3 tries. "
            f"Run retry-effect {effect_id}."
        )

        reply = await service.operator_command("retry-effect", {"effect": effect_id})
        assert reply["requeued"] == effect_id

        async def manually_recovered() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "done"

        await _eventually(manually_recovered)
        parcel = await service.db.call(lambda db: db.load_parcel("P-exhaust"))
        assert parcel is not None and parcel.unknown_effects == ()
        assert parcel.bot != BotState.BLOCKED
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_rate_limit_retry_after_is_honoured(service_config: ServiceConfig) -> None:
    config = _fast_recovery(service_config)
    clock = FakeClock()
    github = FakeGitHub()
    retry_after_us = 77_000_000
    github.script(
        EffectKind.SET_BOT,
        RetryableReadFailure("rate limited", retry_after_us=retry_after_us),
    )
    service = FactoryService(
        config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        effect_id = await _start_triage(service, "P-rate")

        async def pending() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "pending" and row.attempts == 1

        await _eventually(pending)
        row = await service.db.call(lambda db: db.get_effect(effect_id))
        assert row is not None
        assert row.next_at_us == clock.now_utc_us() + retry_after_us
    finally:
        await service.stop()


def _seed_unknown_set_bot(config: ServiceConfig, clock: FakeClock, parcel_id: str) -> str:
    store = SqliteStore.open(config.database_path, clock)
    store.ensure_repository(config.trusted)
    factory = EventFactory(parcel_id)
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        config.trusted,
    )
    result = store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), config.trusted)
    effect = next(effect for effect in result.effects if effect.kind == EffectKind.SET_BOT)
    lease = store.acquire_lease(parcel_id, "crashed-boot")
    assert store.claim_effect(effect.effect_id, lease) is not None
    unknown = Event(
        event_id=f"effect:{effect.effect_id}:unknown",
        repo_id=config.repo_id,
        parcel_id=parcel_id,
        source_time_us=clock.now_utc_us(),
        provenance=Provenance.ADAPTER,
        body=ev.EffectUnknown(
            effect_id=effect.effect_id,
            effect_kind=effect.kind.value,
            attempts=1,
        ),
    )
    outcome = store.record_effect_outcome(
        effect.effect_id,
        "unknown",
        from_states=("claimed",),
        event=unknown,
        config=config.trusted,
        reason="ReadTimeout",
    )
    assert outcome.recorded
    parcel = store.load_parcel(parcel_id)
    assert parcel is not None and parcel.bot == BotState.BLOCKED
    store.close()
    return effect.effect_id


@pytest.mark.asyncio
async def test_startup_self_heals_preexisting_unknown_set_bot(
    service_config: ServiceConfig,
) -> None:
    config = _fast_recovery(service_config)
    clock = FakeClock()
    effect_id = _seed_unknown_set_bot(config, clock, "P-startup")
    service = FactoryService(
        config,
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:

        async def healed() -> bool:
            row = await service.db.call(lambda db: db.get_effect(effect_id))
            return row is not None and row.state == "done"

        await _eventually(healed)
        parcel = await service.db.call(lambda db: db.load_parcel("P-startup"))
        assert parcel is not None
        assert parcel.unknown_effects == ()
        assert parcel.bot == BotState.WORKING
        assert "unconfirmed" not in parcel.board_note
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_retry_effect_accepts_failed_set_bot(service_config: ServiceConfig) -> None:
    config = _fast_recovery(service_config)
    clock = FakeClock()
    store = SqliteStore.open(config.database_path, clock)
    store.ensure_repository(config.trusted)
    factory = EventFactory("P-manual")
    store.apply_event(factory.make(ev.Unpause(), parcel_id=None), config.trusted)
    store.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        config.trusted,
    )
    result = store.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)), config.trusted)
    effect = next(effect for effect in result.effects if effect.kind == EffectKind.SET_BOT)
    lease = store.acquire_lease("P-manual", "failed-boot")
    assert store.claim_effect(effect.effect_id, lease) is not None
    assert store.fail_effect(effect.effect_id, "configuration fixed")
    store.close()

    service = FactoryService(config, clock=clock)
    await service.open()
    try:
        reply = await service.operator_command("retry-effect", {"effect": effect.effect_id})
        assert reply["requeued"] == effect.effect_id
        row = await service.db.call(lambda db: db.get_effect(effect.effect_id))
        assert row is not None and row.state == "pending" and row.attempts == 0
    finally:
        await service.stop()


def test_effect_inventory_keeps_non_idempotent_writes_fenced() -> None:
    assert {
        EffectKind.CREATE_SESSION,
        EffectKind.SEND_MESSAGE,
        EffectKind.RESOLVE_ELICITATION,
    } == NON_IDEMPOTENT_AMBIGUOUS_KINDS
    assert NON_IDEMPOTENT_AMBIGUOUS_KINDS.isdisjoint(AUTOMATIC_RECOVERY_KINDS)
    assert MARKER_ADOPTABLE_CREATE_KINDS <= AUTOMATIC_RECOVERY_KINDS
