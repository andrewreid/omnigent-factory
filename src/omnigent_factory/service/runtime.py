"""Daemon lifecycle, durable ingest, scheduling, reconciliation, and operations."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from functools import partial
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import (
    AdmissionSnapshot,
    InboxHoldReason,
    Lifecycle,
    Parcel,
    QueueStatus,
)
from omnigent_factory.ports.adapter import EffectAdapter
from omnigent_factory.ports.clock import Clock, SystemClock
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.executor import (
    EffectExecutor,
    LocalSchedulerAdapter,
    ParcelSerializers,
)
from omnigent_factory.service.interfaces import (
    DeliveryProcessor,
    ManagedRuntime,
    NonRetryableDelivery,
)
from omnigent_factory.service.locking import ProcessLock
from omnigent_factory.service.operator import OperatorServer
from omnigent_factory.service.parked import (
    ParkedDeliveries,
    inbox_hold_event,
    inbox_release_event,
)
from omnigent_factory.service.redaction import install_redaction_filter
from omnigent_factory.store.sqlite import ApplyResult, DeliveryRecord, SqliteStore, StoredEffect

LOG = logging.getLogger(__name__)

#: Effects an operator may requeue: comment publications adopt their effect marker, and
#: their ack (PublicationAcked) clears the reducer's pending/failed/unknown state.
RETRYABLE_PUBLICATION_KINDS = frozenset(
    {
        EffectKind.PUBLISH_TRIAGE.value,
        EffectKind.PUBLISH_REPORT.value,
        EffectKind.POST_COMMENT.value,
    }
)


class FactoryService:
    def __init__(
        self,
        config: ServiceConfig,
        *,
        adapters: Iterable[EffectAdapter] = (),
        delivery_processor: DeliveryProcessor | None = None,
        clock: Clock | None = None,
        fatal_exit: Callable[[int], object] | None = None,
        process_lock: ProcessLock | None = None,
    ) -> None:
        self.config = config
        self.clock = clock or SystemClock()
        self.db = StoreWorker(config.database_path, self.clock)
        self.serializers = ParcelSerializers()
        self.process_lock = process_lock or ProcessLock(config.state_dir)
        self.delivery_processor = delivery_processor
        self._fatal_exit = fatal_exit
        self._fatal_reason: str | None = None
        self._delivery_failures: dict[str, int] = {}
        self._delivery_retry_at: dict[str, float] = {}
        self._delivery_lock = asyncio.Lock()
        self._last_admission_attempt: tuple[object, ...] | None = None
        self.parked = ParkedDeliveries(self.db, config.state_dir, config.trusted, self.clock)
        self.executor = EffectExecutor(
            self.db,
            config.trusted,
            self.clock,
            (*adapters, LocalSchedulerAdapter()),
            self.serializers,
            poll_seconds=config.effect_poll_seconds,
            # start() acquires the kernel process lock before the executor task exists.
            # Thus this is explicit proven-old-process-gone takeover, never timeout theft.
            takeover_foreign_leases=True,
            error_backoff_seconds=config.background_error_backoff_seconds,
            failure_limit=config.background_failure_limit,
            parked_blocks=self.parked.blocks,
        )
        self.operator = OperatorServer(
            config.operator_socket,
            self.operator_command,
            timeout_seconds=config.operator_timeout_seconds,
        )
        self.ready = False
        self.accepting_admission = False
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._managed: list[ManagedRuntime] = []

    def bind_integrations(
        self,
        *,
        adapters: Iterable[EffectAdapter] = (),
        delivery_processor: DeliveryProcessor | None = None,
        managed: Iterable[ManagedRuntime] = (),
    ) -> None:
        """Complete production wiring after constructing service-backed adapters."""
        if self.ready or self._tasks:
            raise RuntimeError("integrations must be bound before service start")
        for adapter in adapters:
            self.executor.install(adapter)
        if delivery_processor is not None:
            self.delivery_processor = delivery_processor
        self._managed.extend(managed)

    async def start(self) -> None:
        self.config.prepare_private_directories()
        install_redaction_filter()
        self.process_lock.acquire()
        try:
            await self.db.start()
            await self.db.call(lambda store: store.ensure_repository(self.config.trusted))
            await self.parked.load()
            await self._adopt_terminal_claims()
            await self.db.call(lambda store: store.recover_claimed())
            unknown = await self.db.call(lambda store: store.effects_in_state("unknown"))
            await self._recover_outbox(unknown)
            for managed in self._managed:
                await managed.start()
            await self.operator.start()
            self.accepting_admission = True
            self._tasks = [
                asyncio.create_task(self.executor.run(), name="outbox"),
                asyncio.create_task(self._delivery_loop(), name="deliveries"),
                asyncio.create_task(self._admission_loop(), name="admission"),
                asyncio.create_task(self._reconcile_loop(), name="reconcile"),
                asyncio.create_task(self._clock_loop(), name="clock"),
            ]
            for task in self._tasks:
                task.add_done_callback(self._background_done)
            self.ready = True
        except BaseException:
            await self._cleanup_start_failure()
            raise

    async def stop(self) -> None:
        if not self.ready and not self._tasks:
            return
        self.ready = False
        self.accepting_admission = False
        self._stop.set()
        await self.executor.stop()
        outbox = [task for task in self._tasks if task.get_name() == "outbox"]
        other_tasks = [task for task in self._tasks if task.get_name() != "outbox"]
        for task in other_tasks:
            task.cancel()
        if other_tasks:
            await asyncio.gather(*other_tasks, return_exceptions=True)
        if outbox:
            _, pending = await asyncio.wait(outbox, timeout=self.config.shutdown_timeout_seconds)
            for task in pending:
                task.cancel()
            await asyncio.gather(*outbox, return_exceptions=True)
        self._tasks.clear()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.operator.close(), self.config.shutdown_timeout_seconds)
        for managed in reversed(self._managed):
            with contextlib.suppress(Exception):
                await managed.close()
        await self.db.close()
        self.process_lock.close()

    async def _cleanup_start_failure(self) -> None:
        with contextlib.suppress(Exception):
            await self.operator.close()
        for managed in reversed(self._managed):
            with contextlib.suppress(Exception):
                await managed.close()
        with contextlib.suppress(Exception):
            await self.db.close()
        self.process_lock.close()

    async def persist_delivery(self, delivery: DeliveryRecord) -> str:
        return await self.db.call(lambda store: store.append_delivery(delivery))

    async def _recover_outbox(self, recovered: list[StoredEffect]) -> None:
        """Reconstruct reducer-visible ambiguity before any dispatch is enabled."""
        for stored in recovered:
            effect = stored.effect
            if stored.state != "unknown" or effect.parcel_id is None:
                continue
            terminal_persisted = False
            for suffix in ("ack",):
                event_id = f"effect:{effect.effect_id}:{suffix}"
                if await self.db.call(partial(_has_event, event_id=event_id)):
                    terminal_persisted = True
                    break
            if terminal_persisted:
                # The reducer already durably observed the outcome. complete_effect accepts
                # recovered unknown rows and closes this crash window without another call.
                await self.db.call(partial(_complete_effect, effect_id=effect.effect_id))
                continue
            unknown_event = Event(
                event_id=f"effect:{effect.effect_id}:unknown",
                repo_id=self.config.repo_id,
                parcel_id=effect.parcel_id,
                source_time_us=self.clock.now_utc_us(),
                provenance=Provenance.ADAPTER,
                body=ev.EffectUnknown(
                    effect_id=effect.effect_id,
                    effect_kind=effect.kind.value,
                    session_id=effect.preconditions.session_id,
                ),
            )
            await self.apply_event(unknown_event)

    async def _adopt_terminal_claims(self) -> None:
        """Close the event-before-outbox-state crash window without losing disposition."""
        claimed = await self.db.call(lambda store: store.effects_in_state("claimed"))
        for stored in claimed:
            effect_id = stored.effect.effect_id
            if await self.db.call(partial(_has_event, event_id=f"effect:{effect_id}:ack")):
                await self.db.call(partial(_complete_effect, effect_id=effect_id))
            elif await self.db.call(partial(_has_event, event_id=f"effect:{effect_id}:failed")):
                await self.db.call(partial(_fail_effect, effect_id=effect_id))
            elif await self.db.call(partial(_has_event, event_id=f"effect:{effect_id}:cancelled")):
                await self.db.call(partial(_cancel_effect, effect_id=effect_id))

    async def health(self) -> dict[str, object]:
        if (
            self._fatal_reason is not None
            or (not self._stop.is_set() and any(task.done() for task in self._tasks))
            or any(not managed.healthy() for managed in self._managed)
        ):
            return {"status": "unhealthy", "reason": self._fatal_reason or "task-stopped"}
        if not self.ready:
            return {"status": "starting"}
        try:
            admission = await self.db.call(lambda store: store.load_admission(self.config.repo_id))
        except Exception:
            return {"status": "unhealthy"}
        parked = self.parked.records()
        if parked:
            return {
                "status": "degraded",
                "paused": admission.paused,
                "parked_deliveries": len(parked),
            }
        return {"status": "ok", "paused": admission.paused}

    async def _delivery_loop(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                # Also retires holds left by a crash between a delivery's facts and its
                # hold release, so it runs even while no processor is bound.
                await self.release_resolved_inbox_holds()
                if self.delivery_processor is None:
                    await self._wait(0.1)
                    failures = 0
                    continue
                deliveries = await self.db.call(lambda store: store.pending_deliveries())
                loop_now = asyncio.get_running_loop().time()
                for delivery in deliveries:
                    async with self._delivery_lock:
                        await self._process_delivery(delivery, loop_now)
                await self.release_resolved_inbox_holds()
                failures = 0
                await self._wait(0.05)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("deliveries", failures)

    async def _process_delivery(self, delivery: DeliveryRecord, loop_now: float) -> None:
        if self._delivery_retry_at.get(delivery.delivery_guid, 0) > loop_now:
            return
        if self.parked.contains(delivery.delivery_guid):
            # Mirror says parked but the row is pending: complete the park atomically.
            scope = dict(self.parked.records()).get(delivery.delivery_guid)
            await self.parked.park(delivery.delivery_guid, scope)
            return
        if self.delivery_processor is None:
            return
        try:
            await self.delivery_processor.process(delivery)
        except NonRetryableDelivery as exc:
            # Scope row, rejected status and (scoped) parcel hold commit together.
            await self.parked.park(delivery.delivery_guid, exc.parcel_id)
            self._delivery_failures.pop(delivery.delivery_guid, None)
            self._delivery_retry_at.pop(delivery.delivery_guid, None)
            LOG.warning(
                "delivery parked delivery_guid=%s scoped=%s",
                delivery.delivery_guid,
                exc.parcel_id is not None,
            )
        except Exception:
            attempts = self._delivery_failures.get(delivery.delivery_guid, 0) + 1
            self._delivery_failures[delivery.delivery_guid] = attempts
            LOG.warning(
                "delivery processor failed delivery_guid=%s attempt=%s",
                delivery.delivery_guid,
                attempts,
            )
            delay = min(
                self.config.delivery_retry_max_backoff_seconds,
                self.config.delivery_retry_backoff_seconds * (2 ** min(attempts - 1, 30)),
            )
            self._delivery_retry_at[delivery.delivery_guid] = loop_now + delay
            return
        self._delivery_failures.pop(delivery.delivery_guid, None)
        self._delivery_retry_at.pop(delivery.delivery_guid, None)

    async def hold_unresolved_delivery(self, delivery_guid: str, parcel_id: str | None) -> bool:
        """Mark a delivery ``unresolved`` and, for a known candidate parcel, hold it.

        T4 F-new-1: an unresolved project event (identity not yet verified, §3.3(3)) may be
        a leftward drag or removal. When its candidate parcel is already known (the
        content node is a parcel issue), the parcel is fenced (safety), interrupted and
        held in the same transaction that marks the delivery. An unknown candidate cannot
        have running work, so only the status is recorded. Returns whether a hold applied.
        """
        parcel = (
            await self.db.call(partial(_load_parcel, parcel_id=parcel_id)) if parcel_id else None
        )
        if parcel is None or parcel_id is None:
            await self.db.call(
                partial(_mark_delivery, delivery_guid=delivery_guid, status="unresolved")
            )
            return False
        event = inbox_hold_event(
            self.config.trusted,
            self.clock,
            delivery_guid,
            parcel_id,
            InboxHoldReason.UNRESOLVED,
            event_id=f"inbox-hold:{delivery_guid}:unresolved",
        )
        result = await self.apply_event(
            replace(event, delivery_guid=delivery_guid), delivery_status="unresolved"
        )
        if result.duplicate:
            await self.db.call(
                partial(_mark_delivery, delivery_guid=delivery_guid, status="unresolved")
            )
        return True

    async def ignore_delivery(self, delivery_guid: str) -> None:
        """Retire a verified Project delivery proven foreign or proven to carry no change."""
        await self.db.call(partial(_mark_delivery, delivery_guid=delivery_guid, status="processed"))

    async def defer_unresolved_delivery(
        self,
        delivery_guid: str,
        candidate_parcel_id: str | None,
        *,
        retry_after_us: int | None = None,
    ) -> bool:
        """Durably back off an inconclusive Project lookup and cap all future reads.

        The delivery is never retired here. Once the cap is reached it is parked: scoped
        to (and holding) the candidate parcel when one exists, otherwise repository-wide,
        until the operator releases it. Returns whether it was parked.
        """
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT resolution_attempts FROM deliveries WHERE delivery_guid = ?",
                (delivery_guid,),
            )
        )
        attempts = int(rows[0][0]) if rows else 0
        delay_us = int(
            self.config.delivery_resolution_backoff_seconds * (2 ** min(attempts, 8)) * 1_000_000
        )
        retry_at_us = self.clock.now_utc_us() + max(delay_us, retry_after_us or 0)
        exhausted = await self.db.call(
            lambda store: store.defer_delivery_resolution(
                delivery_guid,
                retry_at_us,
                max_attempts=self.config.delivery_resolution_max_attempts,
            )
        )
        if not exhausted:
            return False
        parcel = (
            await self.db.call(partial(_load_parcel, parcel_id=candidate_parcel_id))
            if candidate_parcel_id
            else None
        )
        scope = candidate_parcel_id if parcel is not None else None
        await self.parked.park(delivery_guid, scope)
        self._delivery_failures.pop(delivery_guid, None)
        self._delivery_retry_at.pop(delivery_guid, None)
        LOG.warning(
            "unresolved project delivery parked delivery_guid=%s scoped=%s",
            delivery_guid,
            scope is not None,
        )
        return True

    async def release_resolved_inbox_holds(self) -> int:
        """Retire ``unresolved`` parcel holds whose delivery has since been processed.

        Runs after each delivery pass (and so after a crash between the delivery's facts
        committing and its hold being released). Parked holds are never touched here.
        """
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? AND holds_json LIKE ?",
                (self.config.repo_id, '%"inbox"%'),
            )
        )
        released = 0
        for row in rows:
            parcel_id = str(row[0])
            parcel = await self.db.call(partial(_load_parcel, parcel_id=parcel_id))
            if parcel is None:
                continue
            for hold in parcel.inbox_holds:
                if hold.reason != InboxHoldReason.UNRESOLVED:
                    continue
                status = await self.db.call(
                    partial(_delivery_status, delivery_guid=hold.delivery_guid)
                )
                if status != "processed":
                    continue
                result = await self.apply_event(
                    inbox_release_event(
                        self.config.trusted,
                        self.clock,
                        hold.delivery_guid,
                        parcel_id,
                        Provenance.INBOX,
                        event_id=f"inbox-release:{hold.delivery_guid}:resolved",
                    )
                )
                released += int(result.accepted)
        return released

    async def _admission_loop(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                if self.accepting_admission:
                    await self._admit_once()
                failures = 0
                await self._wait(self.config.clock_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("admission", failures)

    async def _admit_once(self) -> None:
        admission = await self.db.call(lambda store: store.load_admission(self.config.repo_id))
        queued = sorted(
            (q for q in admission.queue if q.status == QueueStatus.QUEUED),
            key=lambda q: (q.sequence, q.parcel_id),
        )
        if not queued:
            return
        head = None
        for candidate in queued:
            inbox_pending = await self.db.call(_has_pending_delivery)
            if not inbox_pending and not self.parked.blocks(candidate.parcel_id):
                head = candidate
                break
        if head is None:
            return
        if not (
            not admission.paused
            and admission.building_count < self.config.max_building
            and admission.prospective_pr_count < self.config.max_open_bot_prs
        ):
            return
        parcel = await self.db.call(partial(_load_parcel, parcel_id=head.parcel_id))
        signature = _admission_signature(parcel, admission, head.parcel_id, head.sequence)
        if signature == self._last_admission_attempt:
            return
        now = self.clock.now_utc_us()
        result = await self.apply_event(
            self._event(
                head.parcel_id,
                ev.CapacityAvailable(),
                f"capacity:{head.sequence}:{now}",
            )
        )
        self._last_admission_attempt = _admission_signature(
            result.parcel,
            result.admission,
            head.parcel_id,
            head.sequence,
        )

    async def _reconcile_loop(self) -> None:
        # Startup reconciliation occurs immediately, then on the configured cadence.
        failures = 0
        while not self._stop.is_set():
            try:
                now = self.clock.now_utc_us()
                parcel_ids = await self._parcel_ids()
                for parcel_id in parcel_ids:
                    await self.apply_event(
                        self._event(
                            parcel_id,
                            ev.ReconcileDue(),
                            f"reconcile:{parcel_id}:{now}",
                        )
                    )
                failures = 0
                await self._wait(self.config.reconcile_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("reconcile", failures)

    async def _clock_loop(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                for parcel_id in await self._parcel_ids():
                    parcel = await self.db.call(partial(_load_parcel, parcel_id=parcel_id))
                    if parcel is not None:
                        await self._sample_clock(parcel)
                failures = 0
                await self._wait(self.config.clock_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("clock", failures)

    async def _sample_clock(self, parcel: Parcel) -> None:
        session = parcel.current_session
        if session is None:
            return
        grant = session.grant
        if (
            grant.remaining_us == 0
            and session.lifecycle in (Lifecycle.ACTIVE, Lifecycle.WAITING)
            and not session.fences
        ):
            await self.apply_event(
                self._event(
                    parcel.parcel_id,
                    ev.ActiveLimitReached(session_id=session.session_id, grant_id=grant.grant_id),
                    f"active-limit:{grant.grant_id}",
                )
            )
        deadline = grant.grace_deadline_us
        if (
            deadline is not None
            and deadline <= self.clock.now_utc_us()
            and session.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT)
        ):
            await self.apply_event(
                self._event(
                    parcel.parcel_id,
                    ev.GraceExpired(session_id=session.session_id, grant_id=grant.grant_id),
                    f"grace:{grant.grant_id}",
                )
            )

    async def _parcel_ids(self) -> list[str]:
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? ORDER BY parcel_id",
                (self.config.repo_id,),
            )
        )
        return [str(row[0]) for row in rows]

    async def apply_event(self, event: Event, *, delivery_status: str | None = None) -> ApplyResult:
        key = event.parcel_id or "@repository"
        async with self.serializers.lock(key):
            return await self.db.call(
                lambda store: store.apply_event(
                    event,
                    self.config.trusted,
                    delivery_status=delivery_status,
                )
            )

    def _event(self, parcel_id: str | None, body: ev.EventBody, event_id: str) -> Event:
        return Event(
            event_id=event_id,
            repo_id=self.config.repo_id,
            parcel_id=parcel_id,
            source_time_us=self.clock.now_utc_us(),
            provenance=(
                Provenance.OPERATOR
                if isinstance(body, ev.Pause | ev.Unpause)
                else Provenance.SCHEDULER
            ),
            body=body,
        )

    async def operator_command(
        self, command: str, args: Mapping[str, object]
    ) -> Mapping[str, object]:
        if command == "status":
            return await self._status()
        if command == "doctor":
            errors = self.config.validate_paths()
            schema_version = await self.db.call(lambda store: store.schema_version())
            return {
                "healthy": not errors,
                "errors": errors,
                "schema_version": schema_version,
            }
        if command in ("pause", "unpause"):
            body: ev.EventBody = ev.Pause() if command == "pause" else ev.Unpause()
            await self.apply_event(self._event(None, body, f"operator:{command}:{uuid.uuid4()}"))
            self._last_admission_attempt = None
            return await self._status()
        if command == "explain":
            parcel_id = str(args.get("parcel", ""))
            if not parcel_id:
                raise ValueError("parcel is required")
            return await self._explain(parcel_id)
        if command == "recovery":
            return await self._recovery()
        if command == "retry-effect":
            effect_id = str(args.get("effect", ""))
            if not effect_id:
                raise ValueError("effect is required")
            requeued = await self.db.call(
                lambda store: store.requeue_effect(effect_id, RETRYABLE_PUBLICATION_KINDS)
            )
            if not requeued:
                raise ValueError("effect is not a failed/unknown publication")
            LOG.info("operator requeued effect effect_id=%s", effect_id)
            return {"requeued": effect_id, **await self._status()}
        if command == "release-delivery":
            delivery_guid = str(args.get("delivery", ""))
            if not delivery_guid:
                raise ValueError("delivery is required")
            if not self.parked.contains(delivery_guid):
                raise ValueError("delivery is not parked")
            async with self._delivery_lock:
                await self.parked.release(delivery_guid)
                self._delivery_failures.pop(delivery_guid, None)
                self._delivery_retry_at.pop(delivery_guid, None)
                return {"released": delivery_guid, **await self._status()}
        raise ValueError("unknown command")

    async def _status(self) -> dict[str, object]:
        admission = await self.db.call(lambda store: store.load_admission(self.config.repo_id))
        pending = await self.db.call(lambda store: len(store.effects_in_state("pending")))
        unknown = await self.db.call(lambda store: len(store.effects_in_state("unknown")))
        parked = self.parked.records()
        return {
            "ready": self.ready,
            "paused": admission.paused,
            "building": admission.building_count,
            "building_cap": self.config.max_building,
            "queued": sum(q.status == QueueStatus.QUEUED for q in admission.queue),
            "pending_effects": pending,
            "unknown_effects": unknown,
            "parked_deliveries": len(parked),
            "boot_id": self.executor.boot_id,
        }

    async def _explain(self, parcel_id: str) -> dict[str, object]:
        parcel = await self.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None:
            return {"found": False, "parcel": parcel_id}
        audit = await self.db.call(lambda store: store.audit_entries(parcel_id))
        session = parcel.current_session
        return {
            "found": True,
            "parcel": parcel_id,
            "stage": parcel.stage.value if parcel.stage else None,
            "bot": parcel.bot.value,
            "eligible": parcel.eligible,
            "holds": sorted(hold.value for hold in parcel.holds),
            "revision": parcel.revision,
            "session": session.session_id if session else None,
            "lifecycle": session.lifecycle.value if session else None,
            "fences": sorted(fence.value for fence in session.fences) if session else [],
            "last_reason": audit[-1]["reason"] if audit else None,
        }

    async def _recovery(self) -> dict[str, object]:
        effects: list[dict[str, Any]] = []
        for state in ("claimed", "unknown", "failed"):
            rows = await self.db.call(partial(_effects_in_state, state=state))
            effects.extend(
                {
                    "effect_id": row.effect.effect_id,
                    "parcel": row.effect.parcel_id,
                    "kind": row.effect.kind.value,
                    "state": row.state,
                    "attempts": row.attempts,
                }
                for row in rows
            )
        pending_deliveries = await self.db.call(lambda store: store.pending_deliveries())
        delivery_rows = await self.db.call(
            lambda store: store.query(
                "SELECT delivery_guid, status FROM deliveries "
                "WHERE status != 'processed' ORDER BY received_at_us, delivery_guid"
            )
        )
        parked_by_guid = dict(self.parked.records())
        return {
            "effects": effects,
            "pending_deliveries": [delivery.delivery_guid for delivery in pending_deliveries],
            "deliveries": [
                {
                    "delivery_guid": str(row[0]),
                    "status": "parked" if str(row[0]) in parked_by_guid else str(row[1]),
                    "parcel": parked_by_guid.get(str(row[0])),
                }
                for row in delivery_rows
            ],
        }

    async def _wait(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), seconds)

    async def _background_error(self, task: str, failures: int) -> int:
        failures += 1
        LOG.warning("background task failed task=%s attempt=%s", task, failures)
        if failures >= self.config.background_failure_limit:
            raise RuntimeError(f"{task} failure limit reached") from None
        await self._wait(self.config.background_error_backoff_seconds * failures)
        return failures

    def _background_done(self, task: asyncio.Task[None]) -> None:
        if self._stop.is_set():
            return
        self.ready = False
        self.accepting_admission = False
        self._fatal_reason = f"{task.get_name()}-stopped"
        LOG.error("background task stopped task=%s", task.get_name())
        if self._fatal_exit is not None:
            self._fatal_exit(1)

    def managed_task_failed(self, name: str) -> None:
        """Escalate a supervised integration task through the daemon fatal policy."""
        if self._stop.is_set():
            return
        self.ready = False
        self.accepting_admission = False
        self._fatal_reason = f"{name}-stopped"
        LOG.error("managed background task stopped task=%s", name)
        if self._fatal_exit is not None:
            self._fatal_exit(1)


def _load_parcel(store: SqliteStore, *, parcel_id: str) -> Parcel | None:
    return store.load_parcel(parcel_id)


def _effects_in_state(store: SqliteStore, *, state: str) -> list[StoredEffect]:
    return store.effects_in_state(state)


def _has_event(store: SqliteStore, *, event_id: str) -> bool:
    return store.has_event(event_id)


def _complete_effect(store: SqliteStore, *, effect_id: str) -> bool:
    return store.complete_effect(effect_id)


def _fail_effect(store: SqliteStore, *, effect_id: str) -> bool:
    return store.fail_effect(effect_id, "terminal-event-persisted")


def _cancel_effect(store: SqliteStore, *, effect_id: str) -> bool:
    return store.cancel_effect(effect_id, "terminal-event-persisted")


def _mark_delivery(store: SqliteStore, *, delivery_guid: str, status: str) -> None:
    store.mark_delivery(delivery_guid, status)


def _has_pending_delivery(store: SqliteStore) -> bool:
    return bool(store.query("SELECT 1 FROM deliveries WHERE status = 'pending' LIMIT 1"))


def _delivery_status(store: SqliteStore, *, delivery_guid: str) -> str | None:
    rows = store.query("SELECT status FROM deliveries WHERE delivery_guid = ?", (delivery_guid,))
    return str(rows[0][0]) if rows else None


def _admission_signature(
    parcel: Parcel | None, admission: AdmissionSnapshot, parcel_id: str, sequence: int
) -> tuple[object, ...]:
    semantic_parcel = (
        replace(parcel, version=0, applied_event_ids=frozenset()) if parcel is not None else None
    )
    return (
        parcel_id,
        sequence,
        semantic_parcel,
        admission,
    )
