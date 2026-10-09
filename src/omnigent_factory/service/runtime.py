"""Daemon lifecycle, durable ingest, scheduling, reconciliation, and operations."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import signal
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import (
    READ_ONLY_KINDS,
    Ack,
    AdapterOutcome,
    EffectIntent,
    EffectKind,
    RetryClass,
)
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.predicates import board_pending, run_closed, settled
from omnigent_factory.core.projection import admission_key, auto_build_capacity_available
from omnigent_factory.core.reducer import awaiting_evidence
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    AdmissionSnapshot,
    Hold,
    InboxHoldReason,
    IssueSessionStatus,
    Lifecycle,
    Parcel,
    QueueStatus,
    ReservationKind,
    Stage,
)
from omnigent_factory.ports.adapter import EffectAdapter
from omnigent_factory.ports.clock import Clock, SystemClock
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig, load_config
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
from omnigent_factory.service.redaction import (
    demote_routine_library_logs,
    install_redaction_filter,
    redact_text,
)
from omnigent_factory.store import ranking as ranking_store
from omnigent_factory.store.sqlite import (
    ApplyResult,
    DeliveryOutcome,
    DeliveryRecord,
    PruneBatch,
    SqliteStore,
    StoredEffect,
)

LOG = logging.getLogger(__name__)

#: Effects an operator may requeue: comment publications adopt their effect marker, and
#: their ack (PublicationAcked) clears the reducer's pending/failed/unknown state.
#: Completed comment publications an operator may re-render in place (same marker).
RERENDERABLE_KINDS = frozenset(
    {
        EffectKind.PUBLISH_CONTRACT.value,
        EffectKind.PUBLISH_TRIAGE.value,
        EffectKind.PUBLISH_REPORT.value,
        EffectKind.POST_COMMENT.value,
    }
)

#: Delivery-loop cadence while a delivery is due now but left pending by its pass.
DELIVERY_BUSY_POLL_SECONDS = 0.05
#: Longest wait between auto-build passes (a pass with nothing startable reads nothing
#: remote, so a free build slot is taken within this).
AUTO_BUILD_POLL_SECONDS = 30.0
#: How often the clock loop runs the history retention sweep (delivery bodies, old
#: observation events).
RETENTION_SWEEP_INTERVAL_SECONDS = 900.0
#: Observation batches (500 events each) one periodic sweep deletes at most, so the clock
#: loop is never held for long; a large backlog drains over several sweeps.
RETENTION_SWEEP_MAX_BATCHES = 20

#: Lifecycles the clock loop acts on (active-time limit, checkpoint grace, drain timeout).
CLOCKED_LIFECYCLES = frozenset(
    {
        Lifecycle.ACTIVE,
        Lifecycle.WAITING,
        Lifecycle.CHECKPOINT_GRACE,
        Lifecycle.CHECKPOINT_WAIT,
        Lifecycle.DRAINING,
    }
)
_CLOCKED_LIFECYCLES_JSON = json.dumps(sorted(x.value for x in CLOCKED_LIFECYCLES))
#: A pending read reports by itself: it does not keep its parcel live (that would make
#: every reconcile of a parcel whose read is retrying schedule another).
_READ_ONLY_KINDS_JSON = json.dumps(sorted(kind.value for kind in READ_ONLY_KINDS))
#: Admission queue states that still need the admission and reconcile loops.
_OPEN_QUEUE_STATES = frozenset({QueueStatus.QUEUED, QueueStatus.RESERVED, QueueStatus.HELD})
#: Reconcile-loop ticks per reconcile interval: per-parcel reads are spread over the
#: interval (never one burst), each parcel still read about once per interval.
RECONCILE_TICKS_PER_INTERVAL = 4

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
        #: Operator ``rerender-comment``: edits a published comment in place (composition).
        self.comment_rerenderer: Callable[[EffectIntent], Awaitable[AdapterOutcome]] | None = None
        #: Operator ``cleanup``: finished-parcel worktree/branch removal (composition).
        self.workspace_cleaner: Any = None
        #: Busy node IDs of a root's latest tree scan, for the drain-timeout warning
        #: (composition).
        self.busy_nodes: Callable[[str], tuple[str, ...]] | None = None
        #: Idle-time auto-triage and its ``auto-triage`` operator command (composition;
        #: ``service.auto_triage.AutoTriager``). None: not wired.
        self.auto_triager: Any = None
        #: Owner-marked auto-builds and the ``auto-build`` operator command (composition;
        #: ``service.auto_build.AutoBuilder``). None: not wired.
        self.auto_builder: Any = None
        #: Idle-time triage ranking and its ``ranking`` operator command (composition;
        #: ``service.ranking.Ranker``). None: not wired.
        self.ranker: Any = None
        #: Deletion of long-archived factory sessions (composition;
        #: ``service.session_retention.SessionRetention``). None: not wired.
        self.session_retention: Any = None
        #: Board-wide diff (composition; ``service.board_diff.BoardDiff``). None: not
        #: wired, and parcels without live work are read on the slow completed cadence.
        self.board_diff: Any = None
        #: GitHub-native links: epic notes and issue types after each board diff, link
        #: writes and re-reads (composition; ``service.links.NativeLinks``). None: not
        #: wired.
        self.native_links: Any = None
        #: Per tracked parcel: (loop time its next per-issue read is due, the cadence
        #: that time was scheduled for) (reconcile loop).
        self._reconcile_due: dict[str, tuple[float, float]] = {}
        #: Loop time of the next board diff (None: at the next reconcile tick).
        self._next_board_diff_at: float | None = None
        #: Board digests to store once the read they scheduled has been applied.
        self._digests_after_read: dict[str, str] = {}
        #: Lost owner label commands, recovered from the issue timeline for cards the
        #: board diff shows changed (composition; ``service.label_recovery``). None: not
        #: wired.
        self.label_recovery: Any = None
        #: Command labels a changed or unknown card showed, checked by ``label_recovery``
        #: before that card's digest is stored.
        self._label_checks: dict[str, Any] = {}
        self._fatal_exit = fatal_exit
        self._fatal_reason: str | None = None
        self._delivery_failures: dict[str, int] = {}
        self._delivery_retry_at: dict[str, float] = {}
        self._delivery_lock = asyncio.Lock()
        #: Set whenever inbox work may have appeared (a committed delivery, an operator
        #: release, shutdown). Only a hint: the delivery loop re-reads the durable inbox.
        self._delivery_wake = asyncio.Event()
        self._next_retention_at: float | None = None
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
            fallback_seconds=config.idle_fallback_seconds,
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
        #: The host config file ``reload`` re-reads (set by ``serve``).
        self.config_path: Path | None = None
        #: Called with the new config after a successful ``reload`` (composition wiring).
        self.config_listeners: list[Callable[[ServiceConfig], None]] = []
        self._reload_lock = asyncio.Lock()
        self._reload_tasks: set[asyncio.Task[None]] = set()

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
        demote_routine_library_logs()
        install_redaction_filter()
        self.process_lock.acquire()
        try:
            await self.db.start()
            await self.db.call(lambda store: store.ensure_repository(self.config.trusted))
            await self.parked.load()
            await self._adopt_terminal_claims()
            await self.db.call(lambda store: store.recover_claimed())
            unknown = await self.db.call(lambda store: store.effects_in_state("unknown"))
            unknown = await self._requeue_unknown_reads(unknown)
            await self._recover_outbox(unknown)
            for managed in self._managed:
                await managed.start()
            await self.operator.start()
            self.accepting_admission = True
            if self.config_path is not None:
                asyncio.get_running_loop().add_signal_handler(signal.SIGHUP, self._sighup)
            self._tasks = [
                asyncio.create_task(self.executor.run(), name="outbox"),
                asyncio.create_task(self._delivery_loop(), name="deliveries"),
                asyncio.create_task(self._admission_loop(), name="admission"),
                asyncio.create_task(self._reconcile_loop(), name="reconcile"),
                asyncio.create_task(self._clock_loop(), name="clock"),
            ]
            if self.auto_triager is not None:
                self._tasks.append(
                    asyncio.create_task(self._auto_triage_loop(), name="auto-triage")
                )
            if self.auto_builder is not None:
                self._tasks.append(asyncio.create_task(self._auto_build_loop(), name="auto-build"))
            if self.ranker is not None:
                self._tasks.append(asyncio.create_task(self._ranking_loop(), name="ranking"))
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
        self._delivery_wake.set()
        self.db.poke()
        if self.config_path is not None:
            asyncio.get_running_loop().remove_signal_handler(signal.SIGHUP)
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
        outcome = await self.db.call(lambda store: store.append_delivery(delivery))
        if outcome == DeliveryOutcome.INSERTED:
            self._delivery_wake.set()
        return outcome

    async def _requeue_unknown_reads(self, unknown: list[StoredEffect]) -> list[StoredEffect]:
        """A read has no side effect: an ambiguous one is simply fetched again."""
        remaining: list[StoredEffect] = []
        for stored in unknown:
            effect = stored.effect
            if effect.retry_class != RetryClass.READ:
                remaining.append(stored)
                continue
            requeued = await self.db.call(
                partial(_requeue_effect, effect_id=effect.effect_id, kind=effect.kind.value)
            )
            if requeued:
                LOG.info(
                    "unknown read requeued kind=%s effect_id=%s",
                    effect.kind.value,
                    effect.effect_id,
                )
            else:
                remaining.append(stored)
        return remaining

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
            # A later reconciliation (its own effect's ack) or adoption already resolved it.
            if await self.db.call(partial(_close_reconciled, effect_id=effect.effect_id)):
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
                # Cleared before the inbox is read: a delivery committed from here on sets
                # it again, so the wait below cannot miss it.
                self._delivery_wake.clear()
                # Also retires holds left by a crash between a delivery's facts and its
                # hold release, so it runs even while no processor is bound.
                await self.release_resolved_inbox_holds()
                if self.delivery_processor is None:
                    await self._wait_for_deliveries(await self._next_delivery_wait())
                    failures = 0
                    continue
                deliveries = await self.db.call(lambda store: store.pending_deliveries())
                loop_now = asyncio.get_running_loop().time()
                for delivery in deliveries:
                    async with self._delivery_lock:
                        await self._process_delivery(delivery, loop_now)
                await self.release_resolved_inbox_holds()
                failures = 0
                await self._wait_for_deliveries(await self._next_delivery_wait())
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("deliveries", failures)

    async def _next_delivery_wait(self) -> float:
        """Seconds until the inbox next has due work, bounded by the idle poll.

        A row due now (no durable or in-memory retry gate) keeps the busy cadence; a
        gated row wakes the loop when its gate lapses. Nothing due: the idle poll.
        """
        rows = await self.db.call(lambda store: store.inbox_due())
        loop_now = asyncio.get_running_loop().time()
        now_us = self.clock.now_utc_us()
        wait = self.config.delivery_idle_poll_seconds
        for delivery_guid, retry_at_us in rows:
            due = max(
                self._delivery_retry_at.get(delivery_guid, loop_now) - loop_now,
                ((retry_at_us or now_us) - now_us) / 1_000_000,
            )
            wait = min(wait, max(due, DELIVERY_BUSY_POLL_SECONDS))
        return wait

    async def _wait_for_deliveries(self, seconds: float) -> None:
        if self._stop.is_set():
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._delivery_wake.wait(), seconds)

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
            reason = redact_text(str(exc))[:500]  # stored and returned: never a secret
            await self.parked.park(delivery.delivery_guid, exc.parcel_id, reason=reason)
            self._delivery_failures.pop(delivery.delivery_guid, None)
            self._delivery_retry_at.pop(delivery.delivery_guid, None)
            LOG.warning(
                "delivery parked delivery_guid=%s event=%s scoped=%s reason=%s",
                delivery.delivery_guid,
                delivery.event_name,
                exc.parcel_id is not None,
                reason,
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
        """Retire a verified delivery that produced no event (foreign, ignored, or carrying
        no change); its body and headers are dropped at once (GUID and sha256 stay)."""
        await self.db.call(lambda store: store.retire_delivery(delivery_guid))

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
        """Admission decisions read only stored state: re-evaluated when it changes."""
        failures = 0
        wake = self.db.subscribe()
        while not self._stop.is_set():
            try:
                # Cleared before reading: a change committed meanwhile wakes the next wait.
                wake.clear()
                if self.accepting_admission:
                    await self._admit_once()
                failures = 0
                await self._wait_for_change(wake)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("admission", failures)

    async def _admit_once(self) -> None:
        admission = await self.db.call(lambda store: store.load_admission(self.config.repo_id))
        queued = sorted(
            (q for q in admission.queue if q.status == QueueStatus.QUEUED), key=admission_key
        )
        if not queued:
            return
        if await self.db.call(lambda store: store.has_pending_delivery()):
            return
        head = None
        auto_full = not auto_build_capacity_available(admission, self.config.trusted)
        for candidate in queued:
            if candidate.auto and auto_full:
                continue  # an auto-build waiting for an auto-build slot holds up no one
            if not self.parked.blocks(candidate.parcel_id):
                head = candidate
                break
        if head is None:
            return
        parcel = await self.db.call(partial(_load_parcel, parcel_id=head.parcel_id))
        if not (
            (not admission.paused or head.resume)  # a parked build resuming is in flight
            and admission.building_count < self.config.max_building
            and (
                _has_own_pr(parcel, admission)  # e.g. a rework: its PR is already open
                or admission.prospective_pr_count < self.config.max_open_bot_prs
            )
        ):
            return
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
        """Per-issue reads only for parcels with live work; a board diff for the rest.

        A live parcel (:func:`reconcile_live`) is read about once per reconcile interval,
        its first read placed at a stable offset within the interval so reads never
        burst (a parcel boot already read waits a whole interval). Every other parcel is
        read only when the board diff shows its card changed (``BoardDiff``) or, when no
        diff is wired, every ``completed_reconcile_interval_seconds``.
        """
        failures = 0
        while not self._stop.is_set():
            try:
                await self._reconcile_tick()
                failures = 0
                wait = self.config.reconcile_interval_seconds / RECONCILE_TICKS_PER_INTERVAL
                if self.board_diff is not None and self._next_board_diff_at is not None:
                    # A diff is never late by a tick: the bound on a missed webhook holds.
                    until = self._next_board_diff_at - asyncio.get_running_loop().time()
                    wait = min(wait, max(until, 0.0))
                await self._wait(wait)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("reconcile", failures)

    def note_reconciled(self, parcel_ids: Iterable[str]) -> None:
        """Parcels read at boot: their next periodic read is a whole interval away."""
        interval = self.config.reconcile_interval_seconds
        due = asyncio.get_running_loop().time() + interval
        for parcel_id in parcel_ids:
            self._reconcile_due[parcel_id] = (due, interval)

    async def _reconcile_tick(self) -> None:
        loop_now = asyncio.get_running_loop().time()
        interval = self.config.reconcile_interval_seconds
        live, idle = await self.live_parcel_ids()
        if self.board_diff is not None:
            await self._board_diff_tick(loop_now, live)
            # Cards never compared before: one read each, spread like live reads.
            periods = {pid: interval for pid in self._digests_after_read if pid not in live}
        else:
            slow = self.config.completed_reconcile_interval_seconds
            periods = dict.fromkeys(idle, slow)
        periods.update(dict.fromkeys(live, interval))
        for parcel_id in [pid for pid in self._reconcile_due if pid not in periods]:
            del self._reconcile_due[parcel_id]  # idle again: the board diff watches it
        for parcel_id, period in sorted(periods.items()):
            first = loop_now + _spread(parcel_id) * period
            due, scheduled = self._reconcile_due.get(parcel_id, (first, period))
            if scheduled != period:  # e.g. idle -> live: never later than the new cadence
                due = min(due, first)
            self._reconcile_due[parcel_id] = (due, period)
            if loop_now >= due:
                self._reconcile_due[parcel_id] = (loop_now + period, period)
                await self._reconcile(parcel_id)

    async def _board_diff_tick(self, loop_now: float, live: frozenset[str]) -> None:
        if self._next_board_diff_at is not None and loop_now < self._next_board_diff_at:
            return
        diff = await self.board_diff.run_once()
        if diff is None:
            # Unreadable board: try again next tick; a failed read never counts as "no
            # change".
            return
        self._next_board_diff_at = loop_now + self.config.board_diff_interval_minutes * 60
        # Checked with the read each changed or unknown card gets, before its digest is
        # stored: a label whose webhook was lost is never compared away unchecked.
        self._label_checks.update(diff.label_checks)
        for parcel_id, digest in sorted(diff.changed.items()):
            # A card the board shows changed is read now, live or not.
            await self._reconcile(parcel_id, digest=digest)
            if parcel_id in live:
                interval = self.config.reconcile_interval_seconds
                self._reconcile_due[parcel_id] = (loop_now + interval, interval)
        # Never compared before (e.g. the first diff ever): read once, spread over the
        # reconcile interval; the digest is stored after that read.
        self._digests_after_read.update(diff.unknown)
        if self.native_links is not None:
            try:
                await self.native_links.epic_pass()
            except Exception:
                LOG.warning("epic pass failed")

    async def reread(self, parcel_id: str) -> None:
        """A per-issue read now (e.g. a native link of the issue changed)."""
        await self._reconcile(parcel_id)

    async def _reconcile(self, parcel_id: str, *, digest: str | None = None) -> None:
        """Apply one ``ReconcileDue`` (the reducer issues the per-issue read)."""
        now = self.clock.now_utc_us()
        await self.apply_event(
            self._event(parcel_id, ev.ReconcileDue(), f"reconcile:{parcel_id}:{now}")
        )
        check = self._label_checks.pop(parcel_id, None)
        if (
            check is not None
            and self.label_recovery is not None
            and not await self.label_recovery.check(parcel_id, check)
        ):
            # A failed label read: keep the old digest, so the next diff checks again.
            self._digests_after_read.pop(parcel_id, None)
            return
        # The read is durable in the outbox now: the card's values may be recorded.
        stored = self._digests_after_read.pop(parcel_id, None)
        if digest is not None or stored is not None:
            value = digest or stored or ""
            await self.db.call(lambda store: store.save_board_digests({parcel_id: value}))

    async def live_parcel_ids(self) -> tuple[frozenset[str], frozenset[str]]:
        """(live, idle) parcel IDs of the repository (see :func:`reconcile_live`).

        One DB-worker call; aggregates come from the store's decoded-parcel cache.
        """
        repo_id = self.config.repo_id
        now_us = self.clock.now_utc_us()

        def classify(store: SqliteStore) -> tuple[frozenset[str], frozenset[str]]:
            admission = store.load_admission(repo_id)
            unsettled = frozenset(
                str(row[0])
                for row in store.query(
                    "SELECT DISTINCT parcel_id FROM effects "
                    "WHERE state IN ('pending', 'claimed', 'unknown') AND parcel_id IS NOT NULL "
                    "AND kind NOT IN (SELECT value FROM json_each(?))",
                    (_READ_ONLY_KINDS_JSON,),
                )
            )
            live: set[str] = set()
            idle: set[str] = set()
            for (parcel_id,) in store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? ORDER BY parcel_id", (repo_id,)
            ):
                parcel = store.load_parcel(str(parcel_id))
                if parcel is None:
                    continue
                if reconcile_live(
                    parcel, admission=admission, unsettled_effects=unsettled, now_us=now_us
                ):
                    live.add(parcel.parcel_id)
                else:
                    idle.add(parcel.parcel_id)
            return frozenset(live), frozenset(idle)

        result: tuple[frozenset[str], frozenset[str]] = await self.db.call(classify)
        return result

    async def _auto_triage_loop(self) -> None:
        """One auto-triage decision per reconcile interval (it starts at most one triage).

        Housekeeping, not a safety loop: a failed pass (GitHub unreachable, ...) is logged
        and retried next interval, never a daemon failure.
        """
        while not self._stop.is_set():
            await self._wait(self.config.reconcile_interval_seconds)
            if self._stop.is_set() or not self.accepting_admission:
                continue
            try:
                outcome = await self.auto_triager.run_once()
                LOG.debug("auto-triage pass outcome=%s", outcome)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.warning("auto-triage pass failed")

    async def _auto_build_loop(self) -> None:
        """Auto-build passes (each starts at most one build), every reconcile interval and
        at least every ``AUTO_BUILD_POLL_SECONDS``: a build slot is taken as soon as it is free.
        Housekeeping, not a safety loop: a failed pass is logged and retried."""
        while not self._stop.is_set():
            await self._wait(min(self.config.reconcile_interval_seconds, AUTO_BUILD_POLL_SECONDS))
            if self._stop.is_set() or not self.accepting_admission:
                continue
            try:
                outcome = await self.auto_builder.run_once()
                LOG.debug("auto-build pass outcome=%s", outcome)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.warning("auto-build pass failed")

    async def _ranking_loop(self) -> None:
        """Triage ranking passes: every few seconds while a run is open, else every
        reconcile interval. Housekeeping: a failed pass is logged and retried."""
        wait = self.config.reconcile_interval_seconds
        while not self._stop.is_set():
            await self._wait(wait)
            if self._stop.is_set():
                continue
            try:
                outcome = await self.ranker.run_once()
                LOG.debug("ranking pass outcome=%s", outcome)
                running = await self.db.call(
                    partial(ranking_store.open_run, repo_id=self.config.repo_id)
                )
                wait = self.ranker.next_wait(running is not None)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.warning("ranking pass failed")
                wait = self.config.reconcile_interval_seconds

    async def _clock_loop(self) -> None:
        """Time limits of clocked runs: re-checked when stored state changes and at the
        earliest deadline (grace, drain timeout), never by polling every parcel."""
        failures = 0
        wake = self.db.subscribe()
        while not self._stop.is_set():
            try:
                wake.clear()
                deadline_us: int | None = None
                for parcel_id in await self._clocked_parcel_ids():
                    parcel = await self.db.call(partial(_load_parcel, parcel_id=parcel_id))
                    if parcel is not None:
                        await self._sample_clock(parcel)
                        due = self._clock_deadline_us(parcel)
                        if due is not None and (deadline_us is None or due < deadline_us):
                            deadline_us = due
                loop_now = asyncio.get_running_loop().time()
                if self._next_retention_at is None or loop_now >= self._next_retention_at:
                    self._next_retention_at = loop_now + RETENTION_SWEEP_INTERVAL_SECONDS
                    try:
                        await self.prune_history(max_batches=RETENTION_SWEEP_MAX_BATCHES)
                    except Exception:
                        # Housekeeping only: retried next sweep, never a clock-loop failure.
                        LOG.warning("history retention sweep failed")
                    await self._session_housekeeping()
                failures = 0
                next_sweep = self._next_retention_at or loop_now
                timeout = next_sweep - asyncio.get_running_loop().time()
                if deadline_us is not None:
                    timeout = min(timeout, (deadline_us - self.clock.now_utc_us()) / 1e6)
                await self._wait_for_change(wake, timeout)
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = await self._background_error("clock", failures)

    async def _session_housekeeping(self) -> None:
        """Session retention and old ranking runs, a small batch per sweep."""
        try:
            if self.session_retention is not None:
                await self.session_retention.sweep()
            repo_id = self.config.repo_id
            require_deleted = self.config.session_retention_days > 0
            await self.db.call(
                lambda store: ranking_store.prune_runs(
                    store, repo_id, require_deleted=require_deleted
                )
            )
        except Exception:
            LOG.warning("session retention sweep failed")

    async def prune_delivery_bodies(self) -> int:
        """Empty bodies of processed deliveries past retention, in short batches."""
        return await prune_delivery_bodies(
            self.db.call, self.config, self.clock.now_utc_us(), dry_run=False
        )

    async def prune_history(
        self, *, dry_run: bool = False, max_batches: int | None = None
    ) -> dict[str, int]:
        """One retention sweep (see :func:`prune_history`)."""
        return await prune_history(
            self.db.call,
            self.config,
            self.clock.now_utc_us(),
            dry_run=dry_run,
            max_batches=max_batches,
        )

    async def _sample_clock(self, parcel: Parcel) -> None:
        await self._expire_drains(parcel)
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

    def _clock_deadline_us(self, parcel: Parcel) -> int | None:
        """The earliest wall-clock time :meth:`_sample_clock` acts on ``parcel`` without
        any stored change (a grace deadline or a drain timeout); None: none pending."""
        due: list[int] = []
        session = parcel.current_session
        if (
            session is not None
            and session.lifecycle in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT)
            and session.grant.grace_deadline_us is not None
        ):
            due.append(session.grant.grace_deadline_us)
        timeout_us = self.config.drain_timeout_minutes * MICROS_PER_MINUTE
        due.extend(
            int(s.drain_started_us + timeout_us)
            for s in parcel.sessions
            if s.lifecycle == Lifecycle.DRAINING and s.drain_started_us
        )
        return min(due) if due else None

    async def _expire_drains(self, parcel: Parcel) -> None:
        """End a drain still waiting for quiescence after ``drain_timeout_minutes``."""
        timeout_us = self.config.drain_timeout_minutes * MICROS_PER_MINUTE
        now = self.clock.now_utc_us()
        for session in parcel.sessions:
            started = session.drain_started_us
            if session.lifecycle != Lifecycle.DRAINING or not started or now - started < timeout_us:
                continue
            root = session.root_id or ""
            busy = self.busy_nodes(root) if self.busy_nodes is not None and root else ()
            LOG.warning(
                "drain timed out parcel=%s session=%s root=%s minutes=%s busy_nodes=%s",
                parcel.parcel_id,
                session.session_id,
                root or "-",
                self.config.drain_timeout_minutes,
                ",".join(busy) or "-",
            )
            await self.apply_event(
                self._event(
                    parcel.parcel_id,
                    ev.StopTimeout(session_id=session.session_id),
                    f"drain-timeout:{session.session_id}:{started}",
                )
            )

    async def _clocked_parcel_ids(self) -> list[str]:
        """Parcels with a run the clock acts on (:data:`CLOCKED_LIFECYCLES`), by the
        relational session projection: idle parcels are never loaded every tick."""
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT DISTINCT p.parcel_id FROM parcels p "
                "JOIN stage_sessions s ON s.parcel_id = p.parcel_id "
                "WHERE p.repo_id = ? AND s.lifecycle IN (SELECT value FROM json_each(?)) "
                "ORDER BY p.parcel_id",
                (self.config.repo_id, _CLOCKED_LIFECYCLES_JSON),
            )
        )
        return [str(row[0]) for row in rows]

    async def open_run_parcel_ids(self) -> list[str]:
        """Parcels whose current run is not closed and has a tree (observer candidates),
        by the relational session projection."""
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT p.parcel_id FROM parcels p "
                "JOIN stage_sessions s ON s.session_id = p.current_session_id "
                "WHERE p.repo_id = ? AND s.execution_closed = 0 "
                "AND s.issue_root_id IS NOT NULL ORDER BY p.parcel_id",
                (self.config.repo_id,),
            )
        )
        return [str(row[0]) for row in rows]

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
        if command == "reload":
            return {**await self.reload_config(), **await self._status()}
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
            self.db.poke()
            return await self._status()
        if command == "auto-triage":
            if self.auto_triager is None:
                raise ValueError("auto-triage is not wired")
            report: dict[str, object] = await self.auto_triager.command(args)
            return report
        if command == "auto-build":
            if self.auto_builder is None:
                raise ValueError("auto-build is not wired")
            built: dict[str, object] = await self.auto_builder.command(args)
            return built
        if command == "ranking":
            if self.ranker is None:
                raise ValueError("ranking is not wired")
            ranking: dict[str, object] = await self.ranker.command(args)
            return ranking
        if command == "sessions-prune":
            if self.session_retention is None:
                raise ValueError("session retention is not wired")
            pruned: dict[str, object] = await self.session_retention.sweep(
                dry_run=bool(args.get("dry_run", False)), limit=100
            )
            return pruned
        if command == "explain":
            parcel_id = str(args.get("parcel", ""))
            if not parcel_id:
                raise ValueError("parcel is required")
            return await self._explain(parcel_id)
        if command == "recovery":
            return await self._recovery()
        if command == "prune":
            return await self.prune_history(dry_run=bool(args.get("dry_run", False)))
        if command == "resume":
            parcel_id = str(args.get("parcel", ""))
            text = str(args.get("message", ""))
            if not parcel_id or not text.strip():
                raise ValueError("parcel and message are required")
            event = Event(
                event_id=f"operator:resume:{uuid.uuid4()}",
                repo_id=self.config.repo_id,
                parcel_id=parcel_id,
                source_time_us=self.clock.now_utc_us(),
                provenance=Provenance.OPERATOR,
                body=ev.OperatorResume(text=text),
            )
            result = await self.apply_event(event)
            if not result.accepted:
                raise ValueError(f"resume refused: {result.reason}")
            LOG.info("operator resumed parcel=%s", parcel_id)
            return {"resumed": parcel_id, **await self._explain(parcel_id)}
        if command == "cleanup":
            parcel_id = str(args.get("parcel", ""))
            parcel = await self.db.call(partial(_load_parcel, parcel_id=parcel_id))
            if parcel is None:
                raise ValueError("unknown parcel")
            if parcel.eligible and parcel.stage not in (Stage.READY, Stage.DONE):
                raise ValueError("parcel is not finished (Ready/Done or closed)")
            if self.workspace_cleaner is None:
                raise ValueError("workspace cleanup is not wired")
            detail = await self.workspace_cleaner.cleanup(
                parcel_id, merged=bool(args.get("merged"))
            )
            return {"cleaned": parcel_id, **detail}
        if command == "rerender-comment":
            effect_id = str(args.get("effect", ""))
            stored = await self.db.call(lambda store: store.get_effect(effect_id))
            if stored is None or stored.state != "done":
                raise ValueError("effect is not a completed publication")
            if stored.effect.kind.value not in RERENDERABLE_KINDS:
                raise ValueError("effect is not a comment publication")
            if self.comment_rerenderer is None:
                raise ValueError("comment re-rendering is not wired")
            outcome = await self.comment_rerenderer(stored.effect)
            if not isinstance(outcome, Ack):
                LOG.warning(
                    "operator re-render failed effect_id=%s outcome=%s",
                    effect_id,
                    type(outcome).__name__,
                )
                raise ValueError(f"re-render failed: {getattr(outcome, 'reason', outcome)}")
            LOG.info(
                "operator re-rendered comment effect_id=%s comment=%s",
                effect_id,
                outcome.remote_id,
            )
            return {"rerendered": effect_id, "comment_id": outcome.remote_id}
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
                self._delivery_wake.set()
                self.db.poke()
                return {"released": delivery_guid, **await self._status()}
        raise ValueError("unknown command")

    async def reload_config(self) -> dict[str, object]:
        """Re-read the host config file and apply hot keys; all-or-nothing."""
        if self.config_path is None:
            raise ValueError("reload unavailable: the daemon's config path is unknown")
        async with self._reload_lock:
            try:
                new = await asyncio.to_thread(load_config, self.config_path)
            except Exception as exc:
                raise ValueError(f"invalid config, nothing applied: {exc}") from exc
            old = self.config
            changed = sorted(
                key for key in type(old).model_fields if getattr(old, key) != getattr(new, key)
            )
            restart = [key for key in changed if key not in HOT_RELOAD_KEYS]
            if restart:
                raise ValueError(f"restart required: {', '.join(restart)}; nothing applied")
            if not changed:
                return {"reloaded": False, "changed": [], "message": "no changes"}
            # Store caps first: if it fails, nothing in memory has changed. Lowering a cap
            # never stops running builds; the store only refuses *new* reservations.
            await self.db.call(lambda store: store.ensure_repository(new.trusted))
            self.config = new
            self.executor.update_config(new.trusted)
            self.executor.fallback_seconds = new.idle_fallback_seconds
            self.parked.update_config(new.trusted)
            for listener in self.config_listeners:
                listener(new)
            # Re-evaluate admission on the next tick even if the snapshot is unchanged.
            self._last_admission_attempt = None
            self.db.poke()
            LOG.info("config reloaded changed=%s", ",".join(changed))
            return {"reloaded": True, "changed": changed}

    def _sighup(self) -> None:
        task = asyncio.create_task(self._reload_logged(), name="reload")
        self._reload_tasks.add(task)
        task.add_done_callback(self._reload_tasks.discard)

    async def _reload_logged(self) -> None:
        try:
            await self.reload_config()
        except ValueError as exc:
            LOG.warning("config reload refused: %s", exc)

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
            "building_running": [q.issue_number for q in admission.running()],
            "building_parked": [q.issue_number for q in admission.parked()],
            "open_bot_pr_cap": self.config.max_open_bot_prs,
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
        reasons = {
            str(row[0]): row[1]
            for row in await self.db.call(
                lambda store: store.query("SELECT delivery_guid, reason FROM parked_deliveries")
            )
        }
        return {
            "effects": effects,
            "pending_deliveries": [delivery.delivery_guid for delivery in pending_deliveries],
            "deliveries": [
                {
                    "delivery_guid": str(row[0]),
                    "status": "parked" if str(row[0]) in parked_by_guid else str(row[1]),
                    "parcel": parked_by_guid.get(str(row[0])),
                    "reason": reasons.get(str(row[0])),
                }
                for row in delivery_rows
            ],
        }

    async def _wait(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), seconds)

    async def _wait_for_change(self, wake: asyncio.Event, timeout: float | None = None) -> None:
        """Sleep until ``wake`` (a stored change), ``timeout`` seconds or the idle fallback.

        Never returns sooner than ``clock_interval_seconds`` after the previous pass (the
        busy cadence), so a burst of writes is one pass, not one per write.
        """
        await self._wait(self.config.clock_interval_seconds)
        if self._stop.is_set() or wake.is_set():
            return
        seconds = self.config.idle_fallback_seconds
        if timeout is not None:
            seconds = min(seconds, max(timeout - self.config.clock_interval_seconds, 0.0))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(wake.wait(), seconds)

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


def _requeue_effect(store: SqliteStore, *, effect_id: str, kind: str) -> bool:
    return store.requeue_effect(effect_id, frozenset({kind}))


def _load_parcel(store: SqliteStore, *, parcel_id: str) -> Parcel | None:
    return store.load_parcel(parcel_id)


def _effects_in_state(store: SqliteStore, *, state: str) -> list[StoredEffect]:
    return store.effects_in_state(state)


def _has_event(store: SqliteStore, *, event_id: str) -> bool:
    return store.has_event(event_id)


def _complete_effect(store: SqliteStore, *, effect_id: str) -> bool:
    return store.complete_effect(effect_id)


def _close_reconciled(store: SqliteStore, *, effect_id: str) -> bool:
    return store.close_reconciled_unknown(effect_id)


def _fail_effect(store: SqliteStore, *, effect_id: str) -> bool:
    return store.fail_effect(effect_id, "terminal-event-persisted")


def _cancel_effect(store: SqliteStore, *, effect_id: str) -> bool:
    return store.cancel_effect(effect_id, "terminal-event-persisted")


def _mark_delivery(store: SqliteStore, *, delivery_guid: str, status: str) -> None:
    store.mark_delivery(delivery_guid, status)


def _delivery_status(store: SqliteStore, *, delivery_guid: str) -> str | None:
    rows = store.query("SELECT status FROM deliveries WHERE delivery_guid = ?", (delivery_guid,))
    return str(rows[0][0]) if rows else None


def _has_own_pr(parcel: Parcel | None, admission: AdmissionSnapshot) -> bool:
    """The parcel already holds an open bot PR or a live PR reservation (no new PR slot)."""
    if parcel is None:
        return False
    if parcel.pr_number is not None and parcel.pr_number in admission.open_bot_prs:
        return True
    return any(
        r.parcel_id == parcel.parcel_id and r.kind == ReservationKind.OPEN_PR and r.live
        for r in admission.reservations
    )


def reconcile_live(
    parcel: Parcel,
    *,
    admission: AdmissionSnapshot,
    unsettled_effects: frozenset[str],
    now_us: int,
) -> bool:
    """The parcel has work a periodic per-issue read (``ReconcileDue``) can advance.

    A run that may still execute or is stopping (FENCED/draining/checkpoint included),
    a tree not yet settled, an ambiguous or unsettled write (a pending read reports by
    itself), an open owner decision, recorded stage authority waiting to start, an
    in-flight board write, a queued or admitted build, a PR whose readiness is unverified
    (review bot pending, merge unknown, report line pending) or a terminal parcel whose
    issue session still needs archiving. Anything else changes only on GitHub, which the
    board diff watches.
    """
    cur = parcel.current_session
    queued = admission.queue_entry(parcel.parcel_id)
    issue = parcel.issue_session
    return (
        (cur is not None and not run_closed(parcel, cur))
        or any(not settled(s) and not run_closed(parcel, s) for s in parcel.sessions)
        or bool(parcel.unknown_effects)
        or parcel.parcel_id in unsettled_effects
        or bool(parcel.open_decisions)
        or parcel.pending_authorization_id is not None
        or board_pending(parcel)
        or (queued is not None and queued.status in _OPEN_QUEUE_STATES)
        or awaiting_evidence(parcel, now_us)
        or (
            Hold.COMPLETED in parcel.holds
            and issue is not None
            and issue.status in (IssueSessionStatus.LIVE, IssueSessionStatus.CLOSING)
        )
    )


def _spread(parcel_id: str) -> float:
    """A stable offset in [0, 1) of an interval for ``parcel_id`` (no boot burst)."""
    digest = hashlib.sha256(parcel_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def _completed_idle(parcel: Parcel) -> bool:
    """Completed, with no live session or open decision: nothing a reconcile can advance."""
    session = parcel.current_session
    return (
        Hold.COMPLETED in parcel.holds
        and (session is None or settled(session))
        and not parcel.open_decisions
    )


StoreCall = Callable[[Callable[[SqliteStore], Any]], Awaitable[Any]]


async def prune_delivery_bodies(
    call: StoreCall, config: ServiceConfig, now_us: int, *, dry_run: bool = False
) -> int:
    """Empty bodies of processed deliveries past retention (``dry_run``: count them)."""
    before_us = now_us - int(config.delivery_body_retention_days * 86_400 * 1_000_000)
    if dry_run:
        return int(await call(lambda store: store.prune_delivery_bodies(before_us, dry_run=True)))
    pruned = 0
    while True:
        batch = int(await call(lambda store: store.prune_delivery_bodies(before_us, limit=100)))
        pruned += batch
        if batch < 100:
            break
    if pruned:
        LOG.info("delivery bodies pruned count=%s", pruned)
    return pruned


async def prune_history(
    call: StoreCall,
    config: ServiceConfig,
    now_us: int,
    *,
    dry_run: bool = False,
    max_batches: int | None = None,
) -> dict[str, int]:
    """One retention sweep: delivery bodies, old observation events, then delivery rows
    and attempts past ``delivery_row_retention_days``.

    Each batch is its own short transaction, so live work interleaves on the DB worker.
    ``max_batches`` bounds the observation batches (a large backlog then drains over
    several sweeps). Freed pages go back to the filesystem when the file uses incremental
    auto_vacuum.
    """
    bodies = await prune_delivery_bodies(call, config, now_us, dry_run=dry_run)
    before_us = now_us - int(config.observation_retention_hours * 3600 * 1_000_000)
    totals = await prune_observations(call, before_us, dry_run=dry_run, max_batches=max_batches)
    rows, attempts = await prune_delivery_rows(
        call, config, now_us, dry_run=dry_run, max_batches=max_batches
    )
    totals = {**totals, "delivery_rows": rows, "delivery_attempts": attempts}
    if not dry_run:
        free_pages = -1
        while True:
            left = int(await call(lambda store: store.incremental_vacuum(1000)))
            if left in (0, free_pages):
                break
            free_pages = left
    result = {"delivery_bodies": bodies, **totals}
    if not dry_run and any(result.values()):
        LOG.info("history pruned %s", " ".join(f"{k}={v}" for k, v in result.items()))
    return result


async def prune_delivery_rows(
    call: StoreCall,
    config: ServiceConfig,
    now_us: int,
    *,
    dry_run: bool = False,
    max_batches: int | None = None,
    limit: int = 500,
) -> tuple[int, int]:
    """Delete fully pruned delivery rows and old attempts (see
    :meth:`SqliteStore.prune_delivery_rows`); a delivery a parcel hold names is kept."""
    before_us = now_us - int(config.delivery_row_retention_days * 86_400 * 1_000_000)
    held: list[str] = []
    for (text,) in await call(
        lambda store: store.query(
            "SELECT aggregate_json FROM parcels WHERE holds_json LIKE ?", ('%"inbox"%',)
        )
    ):
        held.extend(hold.delivery_guid for hold in parcel_from_json(str(text)).inbox_holds)
    if dry_run:
        counted: tuple[int, int] = await call(
            lambda store: store.prune_delivery_rows(before_us, held, dry_run=True)
        )
        return counted
    rows = attempts = batches = 0
    while True:
        batch: tuple[int, int] = await call(
            lambda store: store.prune_delivery_rows(before_us, held, limit=limit)
        )
        rows, attempts, batches = rows + batch[0], attempts + batch[1], batches + 1
        if max(batch) < limit or (max_batches is not None and batches >= max_batches):
            return rows, attempts


async def prune_observations(
    call: StoreCall,
    before_us: int,
    *,
    dry_run: bool = False,
    limit: int = 500,
    max_batches: int | None = None,
) -> dict[str, int]:
    """Run :meth:`SqliteStore.prune_observations` batches to the end (or ``max_batches``)."""
    totals = {"events": 0, "effects": 0, "audit": 0}
    after = 0
    batches = 0
    while True:
        step = partial(
            SqliteStore.prune_observations,
            before_us=before_us,
            after_sequence=after,
            limit=limit,
            dry_run=dry_run,
        )
        batch: PruneBatch = await call(step)
        totals["events"] += batch.events
        totals["effects"] += batch.effects
        totals["audit"] += batch.audit
        batches += 1
        if batch.done or (max_batches is not None and batches >= max_batches):
            return totals
        after = batch.last_sequence


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
