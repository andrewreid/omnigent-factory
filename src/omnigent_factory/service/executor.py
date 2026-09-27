"""Fair per-parcel outbox execution with lease epochs and crash-safe outcomes."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    MessagePurpose,
    RetryableReadFailure,
    RetryClass,
)
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import IssueSnapshot, Stage, TrustedConfig
from omnigent_factory.omnigent.outcomes import observations
from omnigent_factory.ports.adapter import EffectAdapter
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.scheduler import SCHEDULER_EFFECT_KINDS
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.store.sqlite import Lease, LeaseHeld, SqliteStore, StoredEffect

LOG = logging.getLogger(__name__)


class ParcelSerializers:
    """Stable lock per parcel shared by event application and outbound mutations."""

    def __init__(self) -> None:
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def lock(self, parcel_id: str) -> asyncio.Lock:
        return self._locks[parcel_id]


class LocalSchedulerAdapter:
    """Timer intents are persisted facts; clock/admission loops perform the wake-up."""

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return SCHEDULER_EFFECT_KINDS

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        del effect, ctx
        return Ack()


class EffectExecutor:
    def __init__(
        self,
        db: StoreWorker,
        config: TrustedConfig,
        clock: Clock,
        adapters: Iterable[EffectAdapter],
        serializers: ParcelSerializers,
        *,
        poll_seconds: float,
        boot_id: str | None = None,
        takeover_foreign_leases: bool = False,
        error_backoff_seconds: float = 0.05,
        failure_limit: int = 10,
        parked_blocks: Callable[[str | None], bool] | None = None,
    ) -> None:
        self._db = db
        self._config = config
        self._clock = clock
        self._serializers = serializers
        self._poll_seconds = poll_seconds
        self.boot_id = boot_id or str(uuid.uuid4())
        self._takeover_foreign_leases = takeover_foreign_leases
        self._error_backoff_seconds = error_backoff_seconds
        self._failure_limit = failure_limit
        self._parked_blocks = parked_blocks or _never_blocks
        self._adapters: dict[EffectKind, EffectAdapter] = {}
        self._queues: dict[str, asyncio.Queue[str | None]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._scheduled: set[str] = set()
        self._leases: dict[str, Lease] = {}
        self._stopping = asyncio.Event()
        for adapter in adapters:
            self.install(adapter)

    def install(self, adapter: EffectAdapter) -> None:
        """Bind an adapter before :meth:`run`; supports the service construction cycle."""
        if self._workers or self._scheduled:
            raise RuntimeError("cannot install an adapter after the executor started")
        for kind in adapter.handled_kinds:
            if kind in self._adapters:
                raise ValueError(f"multiple adapters handle {kind.value}")
        for kind in adapter.handled_kinds:
            self._adapters[kind] = adapter

    async def run(self) -> None:
        failures = 0
        try:
            while not self._stopping.is_set():
                try:
                    pending = await self._db.call(lambda store: store.pending_effects(limit=200))
                    if self._stopping.is_set():
                        break
                    for stored in pending:
                        effect = stored.effect
                        key = effect.parcel_id or "@repository"
                        existing = self._workers.get(key)
                        if existing is not None and existing.done():
                            raise RuntimeError("parcel worker stopped") from None
                        if effect.effect_id in self._scheduled:
                            continue
                        queue = self._queues.setdefault(key, asyncio.Queue())
                        self._scheduled.add(effect.effect_id)
                        queue.put_nowait(effect.effect_id)
                        if existing is None:
                            self._workers[key] = asyncio.create_task(
                                self._parcel_worker(key, queue), name=f"effect:{key}"
                            )
                    failures = 0
                    await self._wait(self._poll_seconds)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    failures += 1
                    LOG.warning("outbox poll failed attempt=%s", failures)
                    if failures >= self._failure_limit:
                        raise RuntimeError("outbox failure limit reached") from None
                    await self._wait(self._error_backoff_seconds * failures)
        finally:
            self._stopping.set()
            for queue in self._queues.values():
                while True:
                    try:
                        queued_id = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if queued_id is not None:
                        self._scheduled.discard(queued_id)
                    queue.task_done()
                queue.put_nowait(None)
            if self._workers:
                await asyncio.gather(*self._workers.values(), return_exceptions=True)

    async def stop(self) -> None:
        self._stopping.set()

    async def _parcel_worker(self, key: str, queue: asyncio.Queue[str | None]) -> None:
        failures = 0
        while True:
            effect_id = await queue.get()
            if effect_id is None:
                queue.task_done()
                return
            try:
                if self._stopping.is_set():
                    continue
                async with self._serializers.lock(key):
                    if not self._stopping.is_set():
                        await self._execute(effect_id)
                failures = 0
            except Exception:
                # Identifiers only: exception text/tracebacks can contain credentials.
                failures += 1
                LOG.error("effect executor internal failure effect_id=%s", effect_id)
                if failures >= self._failure_limit:
                    raise RuntimeError("parcel worker failure limit reached") from None
                await self._wait(self._error_backoff_seconds * failures)
            finally:
                self._scheduled.discard(effect_id)
                queue.task_done()

    async def _execute(self, effect_id: str) -> None:
        current = await self._db.call(lambda store: store.get_effect(effect_id))
        if current is None or current.state != "pending":
            return
        effect = current.effect
        if _inbox_gated(effect):
            pending = await self._db.call(_has_pending_delivery)
            if pending or self._parked_blocks(effect.parcel_id):
                return
        if _pause_sensitive(effect):
            admission = await self._db.call(
                lambda store: store.load_admission(self._config.repo_id)
            )
            if admission.paused:
                return
        lease = await self._lease(effect)
        if lease is None:
            return
        claimed = await self._db.call(lambda store: store.claim_effect(effect_id, lease))
        if claimed is None:
            return
        parcel = (
            await self._db.call(lambda store: store.load_parcel(effect.parcel_id or ""))
            if effect.parcel_id is not None
            else None
        )
        invalid = effect_still_valid(parcel, effect)
        if invalid is not None:
            await self._cancel(claimed, invalid)
            return
        if self._stopping.is_set():
            # Leave the claim for startup recovery; never begin an external call after stop.
            return
        if _pause_sensitive(effect):
            admission = await self._db.call(
                lambda store: store.load_admission(self._config.repo_id)
            )
            if admission.paused:
                await self._db.call(
                    lambda store: store.fail_effect(
                        effect.effect_id,
                        "paused-before-execution",
                        retry_at_us=self._clock.now_utc_us(),
                    )
                )
                return
        if self._stopping.is_set():
            # The admission read above is an await point, so recheck at the call boundary.
            return
        adapter = self._adapters.get(effect.kind)
        if adapter is None:
            await self._retry_or_unknown(
                claimed,
                RetryableReadFailure("adapter-not-installed", 1_000_000),
                no_external_call=True,
            )
            return
        ctx = ExecutionContext(
            boot_id=self.boot_id,
            lease_epoch=lease.epoch,
            parcel_version=parcel.version if parcel is not None else 0,
            attempt=claimed.attempts,
        )
        try:
            outcome = await adapter.execute(effect, ctx)
        except Exception:
            # Do not include exception text: SDK errors often embed headers/tokens.
            outcome = (
                RetryableReadFailure("adapter-exception", 1_000_000)
                if effect.retry_class in (RetryClass.READ, RetryClass.LOCAL_IDEMPOTENT)
                else AmbiguousWrite("adapter-exception")
            )
        await self._finish(claimed, outcome)

    async def _lease(self, effect: EffectIntent) -> Lease | None:
        if effect.parcel_id is None:
            return Lease("@repository", self.boot_id, 0)
        cached = self._leases.get(effect.parcel_id)
        if cached is not None:
            live = await self._db.call(lambda store: store.heartbeat(cached))
            if live:
                return cached
            self._leases.pop(effect.parcel_id, None)
        try:
            # The daemon owns the state-directory process lock before execution starts.
            # That kernel lock proves any foreign boot is gone; no timeout can steal it.
            lease = await self._db.call(
                lambda store: store.acquire_lease(
                    effect.parcel_id or "",
                    self.boot_id,
                    takeover=self._takeover_foreign_leases,
                )
            )
            self._leases[effect.parcel_id] = lease
            return lease
        except LeaseHeld:
            return None

    async def _cancel(self, stored: StoredEffect, reason: str) -> None:
        effect = stored.effect
        event = self._event(
            effect,
            ev.EffectCancelled(
                effect_id=effect.effect_id,
                effect_kind=effect.kind.value,
                session_id=effect.preconditions.session_id,
            ),
            "cancelled",
        )
        await self._record(
            effect, "cancelled", event, from_states=("pending", "claimed"), reason=reason
        )

    async def _finish(self, stored: StoredEffect, outcome: AdapterOutcome) -> None:
        effect = stored.effect
        if isinstance(outcome, Ack):
            try:
                event = self._ack_event(effect, outcome)
            except (KeyError, TypeError, ValueError):
                await self._finish(stored, AmbiguousWrite("ack-invalid-required-detail"))
                return
            if event is None and effect.kind in _ACK_EVENT_REQUIRED:
                await self._finish(stored, AmbiguousWrite("ack-missing-required-detail"))
                return
            await self._record(effect, "done", event, remote_id=outcome.remote_id)
            return
        if isinstance(outcome, AmbiguousWrite):
            event = self._event(
                effect,
                ev.EffectUnknown(
                    effect_id=effect.effect_id,
                    effect_kind=effect.kind.value,
                    session_id=effect.preconditions.session_id,
                ),
                "unknown",
            )
            await self._record(effect, "unknown", event, reason=outcome.reason)
            return
        await self._retry_or_unknown(stored, outcome)

    async def _record(
        self,
        effect: EffectIntent,
        state: str,
        event: Event | None,
        *,
        from_states: tuple[str, ...] = ("claimed",),
        remote_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Effect state and the reducer event reporting it commit in one transaction."""
        await self._db.call(
            lambda store: store.record_effect_outcome(
                effect.effect_id,
                state,
                from_states=from_states,
                event=event,
                config=self._config,
                remote_id=remote_id,
                reason=reason,
            )
        )

    async def _retry_or_unknown(
        self,
        stored: StoredEffect,
        outcome: RetryableReadFailure | DefinitiveFailure,
        *,
        no_external_call: bool = False,
    ) -> None:
        effect = stored.effect
        if isinstance(outcome, RetryableReadFailure):
            if not no_external_call and effect.retry_class not in (
                RetryClass.READ,
                RetryClass.LOCAL_IDEMPOTENT,
            ):
                await self._finish(stored, AmbiguousWrite("write-returned-retryable-failure"))
                return
            delay = outcome.retry_after_us if outcome.retry_after_us is not None else 1_000_000
            retry_at = self._clock.now_utc_us() + max(delay, 1)
            await self._db.call(
                lambda store: store.fail_effect(
                    effect.effect_id, outcome.reason, retry_at_us=retry_at
                )
            )
            return
        event_body: ev.EventBody
        if effect.kind == EffectKind.CREATE_SESSION:
            event_body = ev.CreateRejected(
                session_id=effect.preconditions.session_id or "", reason=outcome.reason
            )
        elif effect.kind == EffectKind.PREPARE_SESSION:
            event_body = ev.Prepared(session_id=effect.preconditions.session_id or "", ok=False)
        else:
            event_body = ev.EffectCancelled(
                effect_id=effect.effect_id,
                effect_kind=effect.kind.value,
                session_id=effect.preconditions.session_id,
            )
        event = self._event(effect, event_body, "failed")
        await self._record(effect, "failed", event, reason=outcome.reason)

    def _ack_event(self, effect: EffectIntent, ack: Ack) -> Event | None:
        session_id = effect.preconditions.session_id or ""
        detail = ack.detail
        body: ev.EventBody | None = None
        if effect.kind == EffectKind.CREATE_SESSION and ack.remote_id:
            body = ev.SessionCreated(
                session_id=session_id,
                root_id=ack.remote_id,
                nonce=str(effect.args.get("nonce", "")),
            )
        elif effect.kind == EffectKind.PREPARE_SESSION:
            body = ev.Prepared(session_id=session_id, ok=True)
        elif effect.kind == EffectKind.SEND_MESSAGE and ack.remote_id:
            body = ev.MessageAck(
                session_id=session_id, effect_id=effect.effect_id, item_id=ack.remote_id
            )
        elif effect.kind == EffectKind.MOVE_CARD:
            body = ev.ColumnObserved(
                stage=Stage(str(effect.args["to"])), daemon_effect_id=effect.effect_id
            )
        elif effect.kind == EffectKind.PUBLISH_CONTRACT and ack.remote_id:
            body = ev.ContractPublished(
                contract_id=str(effect.args.get("contract_id", "")),
                comment_id=ack.remote_id,
                verified=detail.get("verified") is True,
                posted_at_us=_json_int(detail.get("posted_at_us"), self._clock.now_utc_us()),
            )
        elif effect.kind == EffectKind.REPLACE_COST_POLICY:
            body = ev.PolicyReady(
                session_id=session_id, grant_id=str(effect.args.get("grant_id", ""))
            )
        elif effect.kind == EffectKind.SCAN_TREE and "complete" in detail:
            body = ev.TreeQuiescent(
                session_id=session_id,
                complete=bool(detail.get("complete")),
                busy=bool(detail.get("busy", True)),
                pending_waiter=bool(detail.get("pending_waiter", False)),
            )
        elif effect.kind == EffectKind.FETCH_PR_EVIDENCE and "verified" in detail:
            body = ev.ReadinessEvidence(
                session_id=str(effect.args.get("session_id", "")),
                pr_number=_json_int(effect.args.get("pr_number"), 0),
                head_sha=str(effect.args.get("head_sha", "")),
                verified=bool(detail.get("verified")),
                remediation_exhausted=bool(detail.get("remediation_exhausted", False)),
            )
        elif effect.kind == EffectKind.RESOLVE_ELICITATION:
            body = ev.ElicitationResolved(
                session_id=session_id,
                elicitation_id=str(effect.args.get("elicitation_id", "")),
                correlated=True,
            )
        elif effect.kind == EffectKind.RECONCILE_PARCEL:
            if "read_at_us" not in detail:
                return None
            raw_stage = detail.get("stage")
            stage = Stage(raw_stage) if isinstance(raw_stage, str) else None
            body = ev.GitHubSnapshot()
            return Event(
                event_id=f"effect:{effect.effect_id}:ack",
                repo_id=self._config.repo_id,
                parcel_id=effect.parcel_id,
                source_time_us=self._clock.now_utc_us(),
                provenance=Provenance.ADAPTER,
                body=body,
                evidence=IssueSnapshot(
                    open=detail.get("open") is True,
                    human_assigned=detail.get("human_assigned") is True,
                    repo_matches=detail.get("repo_matches") is True,
                    identity_resolved=detail.get("identity_resolved") is True,
                    in_project=detail.get("in_project") is True,
                    stage=stage,
                    title=str(detail.get("title") or ""),
                    body=(str(detail["body"]) if isinstance(detail.get("body"), str) else None),
                    read_at_us=_json_int(detail.get("read_at_us"), 0),
                ),
            )
        elif effect.kind == EffectKind.RECONCILE_SESSION:
            observed = observations(effect, ack)
            if observed:
                body = observed[0]
        return self._event(effect, body, "ack") if body is not None else None

    async def _wait(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)

    def _event(self, effect: EffectIntent, body: ev.EventBody, suffix: str) -> Event | None:
        if effect.parcel_id is None:
            return None
        return Event(
            event_id=f"effect:{effect.effect_id}:{suffix}",
            repo_id=self._config.repo_id,
            parcel_id=effect.parcel_id,
            source_time_us=self._clock.now_utc_us(),
            provenance=Provenance.ADAPTER,
            body=body,
        )


def _json_int(value: object, default: int) -> int:
    return value if isinstance(value, int) else default


_ACK_EVENT_REQUIRED = frozenset(
    {
        EffectKind.CREATE_SESSION,
        EffectKind.MOVE_CARD,
        EffectKind.SEND_MESSAGE,
        EffectKind.PUBLISH_CONTRACT,
        EffectKind.SCAN_TREE,
        EffectKind.FETCH_PR_EVIDENCE,
    }
)


def _inbox_gated(effect: EffectIntent) -> bool:
    return effect.kind == EffectKind.CREATE_SESSION or effect.work_bearing


def _pause_sensitive(effect: EffectIntent) -> bool:
    if effect.kind in (EffectKind.CREATE_SESSION, EffectKind.ENABLE_ISSUANCE):
        return True
    return (
        effect.kind == EffectKind.SEND_MESSAGE
        and effect.args.get("purpose") == MessagePurpose.FIRST.value
    )


def _has_pending_delivery(store: SqliteStore) -> bool:
    return bool(store.query("SELECT 1 FROM deliveries WHERE status = 'pending' LIMIT 1"))


def _never_blocks(parcel_id: str | None) -> bool:
    del parcel_id
    return False
