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
    READ_ONLY_KINDS,
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
from omnigent_factory.omnigent.adapter import ELICITATION_NOT_PENDING
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
        fallback_seconds: float = 30.0,
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
        #: Longest the outbox sleeps with nothing due and no change seen: a safety net
        #: only (every committed change, and the earliest retry time, wake it).
        self.fallback_seconds = fallback_seconds
        self._wake: asyncio.Event | None = None
        for adapter in adapters:
            self.install(adapter)

    def update_config(self, config: TrustedConfig) -> None:
        """Adopt a hot-reloaded trusted config (same repository)."""
        self._config = config

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
        wake = self._wake = self._db.subscribe()
        try:
            while not self._stopping.is_set():
                try:
                    # Cleared before the outbox is read: a change committed from here on
                    # sets it again, so the wait below cannot miss it.
                    wake.clear()
                    pending, next_due_us = await self._db.call(_due_effects)
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
                    await self._idle(wake, next_due_us)
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
        if self._wake is not None:
            self._wake.set()

    async def _idle(self, wake: asyncio.Event, next_due_us: int | None) -> None:
        """Sleep until the database changes, the earliest retry is due, or the fallback.

        Never re-polls sooner than ``poll_seconds`` (the busy cadence): a burst of
        writes is batched into one outbox read.
        """
        await self._wait(self._poll_seconds)
        timeout = self.fallback_seconds
        if next_due_us is not None:
            timeout = min(timeout, max(next_due_us - self._clock.now_utc_us(), 0) / 1e6)
        if self._stopping.is_set() or wake.is_set():
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(wake.wait(), timeout)

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
                        generation = self._db.generation
                        await self._execute(effect_id)
                        if self._db.generation != generation and self._wake is not None:
                            # The outbox read may have skipped this (still scheduled)
                            # effect after a change it acted on stale: read it again.
                            self._wake.set()
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
            pending = await self._db.call(lambda store: store.has_pending_delivery())
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
        LOG.log(
            logging.DEBUG if effect.kind in READ_ONLY_KINDS else logging.INFO,
            "effect start kind=%s effect_id=%s parcel=%s attempt=%s",
            effect.kind.value,
            effect.effect_id,
            effect.parcel_id,
            claimed.attempts,
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
        LOG.warning(
            "effect cancelled kind=%s effect_id=%s parcel=%s reason=%s",
            effect.kind.value,
            effect.effect_id,
            effect.parcel_id,
            reason,
        )
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
                event, missing = None, "ack-invalid-required-detail"
            else:
                missing = "ack-missing-required-detail"
            if event is None and effect.kind in _ACK_EVENT_REQUIRED:
                if effect.retry_class == RetryClass.READ:
                    # A read has no side effect: fetch again rather than record ambiguity.
                    await self._retry_or_unknown(stored, RetryableReadFailure(missing, 5_000_000))
                else:
                    await self._finish(stored, AmbiguousWrite(missing))
                return
            _log_ack(effect, outcome)
            await self._record(effect, "done", event, remote_id=outcome.remote_id)
            return
        if isinstance(outcome, AmbiguousWrite):
            LOG.warning(
                "effect outcome unknown kind=%s effect_id=%s parcel=%s reason=%s",
                effect.kind.value,
                effect.effect_id,
                effect.parcel_id,
                outcome.reason,
            )
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
            adoptable = effect.retry_class == RetryClass.ADOPTABLE_WRITE
            if adoptable and not no_external_call and stored.attempts >= _ADOPTABLE_RETRY_LIMIT:
                # Proven-not-written every time: stop retrying and surface it as Blocked.
                await self._retry_or_unknown(
                    stored, DefinitiveFailure(f"retries exhausted: {outcome.reason}")
                )
                return
            if (
                not no_external_call
                and not adoptable
                and effect.retry_class
                not in (
                    RetryClass.READ,
                    RetryClass.LOCAL_IDEMPOTENT,
                )
            ):
                await self._finish(stored, AmbiguousWrite("write-returned-retryable-failure"))
                return
            if outcome.retry_after_us is not None:
                delay = outcome.retry_after_us
            elif adoptable:
                # An adoptable write re-checks its marker/current value before writing.
                delay = min(2 ** max(stored.attempts, 1), 300) * 1_000_000
            else:
                delay = 1_000_000
            retry_at = self._clock.now_utc_us() + max(delay, 1)
            LOG.warning(
                "effect retry scheduled kind=%s effect_id=%s parcel=%s attempt=%s reason=%s",
                effect.kind.value,
                effect.effect_id,
                effect.parcel_id,
                stored.attempts,
                outcome.reason,
            )
            await self._db.call(
                lambda store: store.fail_effect(
                    effect.effect_id, outcome.reason, retry_at_us=retry_at
                )
            )
            return
        LOG.warning(
            "effect failed kind=%s effect_id=%s parcel=%s reason=%s",
            effect.kind.value,
            effect.effect_id,
            effect.parcel_id,
            outcome.reason,
        )
        event_body: ev.EventBody
        if effect.kind == EffectKind.CREATE_SESSION:
            event_body = ev.CreateRejected(
                session_id=effect.preconditions.session_id or "", reason=outcome.reason
            )
        elif effect.kind == EffectKind.PREPARE_SESSION:
            event_body = ev.Prepared(session_id=effect.preconditions.session_id or "", ok=False)
        elif (
            effect.kind == EffectKind.RESOLVE_ELICITATION
            and outcome.reason == ELICITATION_NOT_PENDING
        ):
            # Same mapping as omnigent.outcomes: the prompt is gone (answered/cancelled in
            # Omnigent), which closes the decision instead of an audit-only cancellation.
            event_body = ev.ElicitationGone(
                session_id=effect.preconditions.session_id or "",
                elicitation_id=str(effect.args.get("elicitation_id") or ""),
            )
        else:
            event_body = ev.EffectCancelled(
                effect_id=effect.effect_id,
                effect_kind=effect.kind.value,
                session_id=effect.preconditions.session_id,
                failed=True,
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
            # The adapter reports a refused preparation as an Ack with ok=false (no
            # capability was provisioned): fail closed unless success is explicit.
            body = ev.Prepared(
                session_id=session_id,
                ok=detail.get("ok") is True,
                unexpected_turn=detail.get("unexpected_turn") is True,
                unusable=detail.get("unusable") is True,
                reason=(
                    str(detail.get("reason") or "")[:200] if detail.get("unusable") is True else ""
                ),
                note=str(detail.get("note") or "")[:200],
                policy_ready_at_us=_json_int(detail.get("policy_ready_at_us"), 0),
            )
        elif effect.kind == EffectKind.VERIFY_POLICIES:
            body = ev.PoliciesVerified(
                session_id=session_id,
                ok=detail.get("ok") is True,
                reconciled=detail.get("reconciled") is True,
                ready_at_us=_json_int(detail.get("ready_at_us"), 0),
            )
        elif effect.kind == EffectKind.CLOSE_SESSION:
            body = ev.IssueSessionClosed(root_id=str(effect.args.get("root_id") or ""))
        elif effect.kind in _PUBLICATION_ACK_KINDS and ack.remote_id:
            body = ev.PublicationAcked(
                effect_id=effect.effect_id,
                effect_kind=effect.kind.value,
                session_id=effect.preconditions.session_id,
                comment_id=ack.remote_id,
            )
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
                checks_summary=str(detail.get("checks_summary") or "")[:200],
                # Carry the head GitHub actually reported, not the one the read asked about.
                observed_head_sha=str(detail.get("head_sha") or ""),
                checks=_checks_state(detail.get("checks")),
                findings_open=detail.get("findings_dispositioned") is False,
                review_accepted=detail.get("review_accepted") is not False,
                pr_open=detail.get("open") is not False,
                merged=detail.get("merged") is True,
                closes_issue=detail.get("closes_issue") is not False,
                review_bot_pending_since_us=_optional_int(
                    detail.get("review_bot_pending_since_us")
                ),
                base_sync=detail.get("base_sync") is True,
                failing_checks=str(detail.get("failing_checks") or "")[:200],
                review_bot_verdict=str(detail.get("review_bot_verdict") or "")[:200],
                read_started_us=_json_int(detail.get("read_started_us"), 0),
                review_bot_eyes=_bot_eyes(detail.get("review_bot_eyes")),
                open_findings=_finding_refs(detail.get("open_findings")),
                findings_earlier_rounds=detail.get("findings_earlier_rounds") is True,
                mergeable=_mergeable_state(detail.get("mergeable")),
                base_head=str(detail.get("base_head") or "")[:64],
                base_ref=str(detail.get("base_ref") or "")[:255],
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
                    bot=str(detail["bot"]) if isinstance(detail.get("bot"), str) else None,
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


def _checks_state(value: object) -> ev.ChecksState | None:
    try:
        return ev.ChecksState(str(value)) if value is not None else None
    except ValueError:
        return None


def _bot_eyes(value: object) -> ev.BotEyes | None:
    try:
        return ev.BotEyes(str(value)) if value else None
    except ValueError:
        return None


def _mergeable_state(value: object) -> str:
    """A read's mergeability ("" for anything not a known value: never a conflict)."""
    known = (ev.MERGE_CLEAN, ev.MERGE_CONFLICT, ev.MERGE_UNKNOWN)
    return str(value) if value in known else ""


def _finding_refs(value: object) -> tuple[ev.FindingRef, ...]:
    """The open bot threads of a read, bounded (they are only shown to the owner)."""
    if not isinstance(value, list):
        return ()
    return tuple(
        ev.FindingRef(
            path=str(item.get("path") or "")[:200],
            severity=str(item.get("severity") or "")[:2],
            title=str(item.get("title") or "")[:120],
            url=str(item.get("url") or "")[:300],
        )
        for item in value[:20]
        if isinstance(item, dict)
    )


def _json_int(value: object, default: int) -> int:
    return value if isinstance(value, int) else default


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


_ACK_EVENT_REQUIRED = frozenset(
    {
        EffectKind.CREATE_SESSION,
        EffectKind.MOVE_CARD,
        EffectKind.SEND_MESSAGE,
        EffectKind.PUBLISH_CONTRACT,
        EffectKind.PUBLISH_TRIAGE,
        EffectKind.PUBLISH_REPORT,
        EffectKind.SCAN_TREE,
        EffectKind.FETCH_PR_EVIDENCE,
        EffectKind.CLOSE_SESSION,
        EffectKind.VERIFY_POLICIES,
    }
)

#: Comment effects whose ack tells the reducer the owner can now see the outcome.
_PUBLICATION_ACK_KINDS = frozenset(
    {EffectKind.PUBLISH_TRIAGE, EffectKind.PUBLISH_REPORT, EffectKind.POST_COMMENT}
)

#: Attempts (claims) before a proven-not-written adoptable write is reported failed.
_ADOPTABLE_RETRY_LIMIT = 5


def _log_ack(effect: EffectIntent, ack: Ack) -> None:
    detail = ack.detail
    if effect.kind == EffectKind.PREPARE_SESSION and detail.get("ok") is not True:
        LOG.warning(
            "effect refused kind=%s effect_id=%s parcel=%s reason=%s",
            effect.kind.value,
            effect.effect_id,
            effect.parcel_id,
            detail.get("reason"),
        )
        return
    if effect.kind == EffectKind.CREATE_SESSION:
        LOG.info(
            "session created session=%s root=%s parcel=%s stage=%s",
            effect.preconditions.session_id,
            ack.remote_id,
            effect.parcel_id,
            effect.args.get("stage"),
        )
    LOG.log(
        logging.DEBUG if effect.kind in READ_ONLY_KINDS else logging.INFO,
        "effect done kind=%s effect_id=%s parcel=%s remote_id=%s",
        effect.kind.value,
        effect.effect_id,
        effect.parcel_id,
        ack.remote_id,
    )


def _due_effects(store: SqliteStore) -> tuple[list[StoredEffect], int | None]:
    """Pending effects due now, and when the next deferred one becomes due."""
    return store.pending_effects(limit=200), store.next_effect_due_us()


def _inbox_gated(effect: EffectIntent) -> bool:
    return effect.kind == EffectKind.CREATE_SESSION or effect.work_bearing


def _pause_sensitive(effect: EffectIntent) -> bool:
    if effect.kind in (EffectKind.CREATE_SESSION, EffectKind.ENABLE_ISSUANCE):
        return True
    return (
        effect.kind == EffectKind.SEND_MESSAGE
        and effect.args.get("purpose") == MessagePurpose.FIRST.value
    )


def _never_blocks(parcel_id: str | None) -> bool:
    del parcel_id
    return False
