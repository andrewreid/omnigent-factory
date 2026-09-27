"""SQLite durable store: inbox, reducer application, outbox, leases, admission, audit.

Contract (architecture §3.2-§3.5):

* Local-disk SQLite, WAL, ``foreign_keys=ON``, ``synchronous=FULL``, busy timeout, explicit
  checksummed migrations. Autocommit connection with explicit ``BEGIN IMMEDIATE``
  transactions; a transaction is never held across network I/O.
* :meth:`SqliteStore.append_delivery` is the durable inbox: commit before 2xx; duplicate
  GUID + same bytes is a no-op; same GUID + different bytes is quarantined, except that a
  GUID already stored is a no-op whenever either copy came from App-authenticated
  recovery (GitHub may re-serialise recovered payloads, so their bytes cannot match).
* :meth:`SqliteStore.apply_event` commits the event, new aggregate, relational
  projections, admission (queue/reservations), effect intents (outbox), dispatch intents
  and audit in **one** transaction. A duplicate logical event is a durable no-op.
* :meth:`SqliteStore.record_effect_outcome` commits an effect's terminal/unknown state
  together with the reducer event that reports it, so no crash can separate them.
* The store re-checks admission caps before commit (defense in depth): a transaction that
  would raise the building or prospective-PR count above its cap is rolled back.
* Leases carry a boot UUID and monotonically increasing epoch. A lease held by another
  boot is never stolen implicitly; takeover requires the caller to assert (via the
  process lock) that the old process is gone.

One :class:`SqliteStore` instance owns one connection and must be used from one thread
(the service's serialized DB worker). Tests use one instance per thread.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

from omnigent_factory.core import codec
from omnigent_factory.core.effects import EffectIntent, EffectKind
from omnigent_factory.core.events import Event, EventKind
from omnigent_factory.core.reducer import TransitionResult, transition
from omnigent_factory.core.types import (
    FENCE_BITS,
    AdmissionSnapshot,
    Lifecycle,
    Parcel,
    QueueEntry,
    QueueStatus,
    Reservation,
    ReservationKind,
    State,
    TrustedConfig,
)
from omnigent_factory.ports.clock import Clock
from omnigent_factory.store.migrations import BOOTSTRAP_SQL, MIGRATIONS, Migration

LOG = logging.getLogger(__name__)

#: Periodic samples: logged at DEBUG unless they change the stage or Bot value.
_QUIET_KINDS = frozenset(
    {EventKind.ACTIVE_TIME_SAMPLE, EventKind.COST_SAMPLE, EventKind.RUNTIME_ACTIVITY}
)

Reducer = Callable[[State, Event], TransitionResult]
FaultHook = Callable[[str], None]


class StoreError(RuntimeError):
    pass


class MigrationError(StoreError):
    pass


class InvariantViolation(StoreError):
    pass


class LeaseHeld(StoreError):
    pass


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    delivery_guid: str
    event_name: str
    body: bytes
    headers: dict[str, str]
    provenance: str = "webhook"
    action: str | None = None
    app_id: int | None = None
    installation_id: int | None = None
    source_time_us: int | None = None
    recovery_delivery_id: str | None = None

    @property
    def body_sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


class DeliveryOutcome:
    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class ApplyResult:
    duplicate: bool
    sequence: int | None
    accepted: bool
    reason: str
    effects: tuple[EffectIntent, ...]
    parcel: Parcel | None
    admission: AdmissionSnapshot


@dataclass(frozen=True, slots=True)
class StoredEffect:
    effect: EffectIntent
    state: str
    attempts: int
    next_at_us: int | None
    lease_epoch: int | None
    remote_id: str | None


@dataclass(frozen=True, slots=True)
class EffectOutcomeResult:
    recorded: bool
    applied: ApplyResult | None


@dataclass(frozen=True, slots=True)
class OwnSendRow:
    effect_id: str
    session_id: str
    node_id: str
    kind: str
    text_sha256: str
    elicitation_id: str
    item_id: str | None


@dataclass(frozen=True, slots=True)
class CapabilityRow:
    session_key: str
    stage_session_id: str
    worker_id: str | None
    worker_profile: str | None
    capability_id: str
    secret_sha256: str
    generation: int
    path: str
    revoked: bool


@dataclass(frozen=True, slots=True)
class WorkerGrantRow:
    stage_session_id: str
    worker_id: str
    path: str
    branch: str
    profile: str


@dataclass(frozen=True, slots=True)
class Lease:
    parcel_id: str
    boot_id: str
    epoch: int


def _digest(text: str | None) -> str | None:
    if text is None:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class SqliteStore:
    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Clock,
        *,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self._conn = conn
        self._clock = clock
        self._fault = fault_hook

    # ------------------------------------------------------------ lifecycle

    @classmethod
    def open(
        cls,
        path: str | Path,
        clock: Clock,
        *,
        migrations: Sequence[Migration] = MIGRATIONS,
        fault_hook: FaultHook | None = None,
        busy_timeout_ms: int = 5000,
    ) -> SqliteStore:
        conn = sqlite3.connect(str(path), isolation_level=None, timeout=busy_timeout_ms / 1000)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
            store = cls(conn, clock, fault_hook=fault_hook)
            store._migrate(migrations)
        except BaseException:
            conn.close()
            raise
        return store

    def close(self) -> None:
        self._conn.close()

    def _hook(self, point: str) -> None:
        if self._fault is not None:
            self._fault(point)

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            self._hook("before-commit")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def _migrate(self, migrations: Sequence[Migration]) -> None:
        self._conn.execute(BOOTSTRAP_SQL)
        applied = {
            row["version"]: row["checksum"]
            for row in self._conn.execute("SELECT version, checksum FROM schema_migrations")
        }
        known = {m.version: m for m in migrations}
        for version, checksum in applied.items():
            m = known.get(version)
            if m is None:
                raise MigrationError(f"database has unknown migration {version}; refusing")
            if m.checksum != checksum:
                raise MigrationError(f"migration {version} checksum mismatch; refusing")
        for m in sorted(migrations, key=lambda x: x.version):
            if m.version in applied:
                continue
            with self._txn() as conn:
                for statement in _split_sql(m.sql):
                    conn.execute(statement)
                self._hook(f"migration-{m.version}")
                conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at_us, checksum) "
                    "VALUES (?, ?, ?, ?)",
                    (m.version, m.name, self._clock.now_utc_us(), m.checksum),
                )

    def schema_version(self) -> int:
        row = self._conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
        return int(row["v"] or 0)

    def backup(self, dest: str | Path) -> None:
        """Consistent copy via SQLite's backup API (never copy a live DB without WAL)."""
        target = sqlite3.connect(str(dest))
        try:
            self._conn.backup(target)
        finally:
            target.close()

    # ---------------------------------------------------------- repository

    def ensure_repository(self, config: TrustedConfig, *, full_name: str | None = None) -> None:
        """Create the repository admission row if missing. New rows start **paused**."""
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO repositories (repo_id, full_name, paused, next_sequence, "
                "open_bot_prs_json, max_building, max_open_bot_prs, updated_at_us) "
                "VALUES (?, ?, 1, 1, '[]', ?, ?, ?) ON CONFLICT(repo_id) DO UPDATE SET "
                "max_building = excluded.max_building, "
                "max_open_bot_prs = excluded.max_open_bot_prs, "
                "updated_at_us = excluded.updated_at_us",
                (
                    config.repo_id,
                    full_name,
                    config.max_building,
                    config.max_open_bot_prs,
                    self._clock.now_utc_us(),
                ),
            )

    def load_admission(self, repo_id: str) -> AdmissionSnapshot:
        return self._load_admission(self._conn, repo_id)

    @staticmethod
    def _load_admission(conn: sqlite3.Connection, repo_id: str) -> AdmissionSnapshot:
        row = conn.execute("SELECT * FROM repositories WHERE repo_id = ?", (repo_id,)).fetchone()
        if row is None:
            raise StoreError(f"unknown repository {repo_id}; call ensure_repository first")
        queue = tuple(
            QueueEntry(
                r["parcel_id"], r["approval_id"], r["approval_sequence"], QueueStatus(r["status"])
            )
            for r in conn.execute(
                "SELECT * FROM queue WHERE repo_id = ? ORDER BY approval_sequence", (repo_id,)
            )
        )
        reservations = tuple(
            Reservation(
                r["reservation_id"],
                r["parcel_id"],
                ReservationKind(r["kind"]),
                r["episode_id"],
                r["pr_number"],
                bool(r["live"]),
            )
            for r in conn.execute(
                "SELECT * FROM reservations WHERE repo_id = ? AND live = 1 ORDER BY reservation_id",
                (repo_id,),
            )
        )
        return AdmissionSnapshot(
            repo_id=repo_id,
            paused=bool(row["paused"]),
            next_sequence=int(row["next_sequence"]),
            queue=queue,
            reservations=reservations,
            open_bot_prs=frozenset(int(x) for x in json.loads(row["open_bot_prs_json"])),
        )

    # --------------------------------------------------------------- inbox

    def append_delivery(self, d: DeliveryRecord) -> str:
        """Durably record a verified delivery. Return a :class:`DeliveryOutcome` value."""
        now = self._clock.now_utc_us()
        with self._txn() as conn:
            row = conn.execute(
                "SELECT body_sha256, provenance FROM deliveries WHERE delivery_guid = ?",
                (d.delivery_guid,),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO deliveries (delivery_guid, app_id, installation_id, event_name, "
                    "action, headers_json, body, body_sha256, source_time_us, received_at_us, "
                    "provenance, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
                    (
                        d.delivery_guid,
                        d.app_id,
                        d.installation_id,
                        d.event_name,
                        d.action,
                        codec.dumps(d.headers),
                        d.body,
                        d.body_sha256,
                        d.source_time_us,
                        now,
                        d.provenance,
                    ),
                )
                outcome = DeliveryOutcome.INSERTED
            elif row["body_sha256"] == d.body_sha256 or "recovery" in (
                d.provenance,
                row["provenance"],
            ):
                # A recovered copy may be GitHub's re-serialisation of the original bytes:
                # the GUID alone identifies the delivery, and the stored copy wins.
                outcome = DeliveryOutcome.DUPLICATE
            else:
                outcome = DeliveryOutcome.QUARANTINED
            conn.execute(
                "INSERT INTO delivery_attempts (delivery_guid, recovery_delivery_id, body, "
                "body_sha256, received_at_us, outcome) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    d.delivery_guid,
                    d.recovery_delivery_id,
                    d.body if outcome == DeliveryOutcome.QUARANTINED else None,
                    d.body_sha256,
                    now,
                    outcome,
                ),
            )
        return outcome

    def pending_deliveries(self) -> list[DeliveryRecord]:
        rows = self._conn.execute(
            "SELECT * FROM deliveries WHERE status IN ('pending', 'unresolved') "
            "AND (resolution_retry_at_us IS NULL OR resolution_retry_at_us <= ?) "
            "ORDER BY received_at_us, delivery_guid",
            (self._clock.now_utc_us(),),
        ).fetchall()
        return [
            DeliveryRecord(
                delivery_guid=r["delivery_guid"],
                event_name=r["event_name"],
                body=bytes(r["body"]),
                headers=json.loads(r["headers_json"]),
                provenance=r["provenance"],
                action=r["action"],
                app_id=r["app_id"],
                installation_id=r["installation_id"],
                source_time_us=r["source_time_us"],
            )
            for r in rows
        ]

    def mark_delivery(self, delivery_guid: str, status: str) -> None:
        with self._txn() as conn:
            self._mark_delivery(conn, delivery_guid, status)

    def defer_delivery_resolution(
        self, delivery_guid: str, retry_at_us: int, *, max_attempts: int
    ) -> bool:
        """Persist one resolution attempt; return true once the retry cap is reached.

        Exhaustion never retires the delivery: it stays ``unresolved`` with no retry time
        so the caller parks it (and a crash before parking simply re-parks it).
        """
        with self._txn() as conn:
            row = conn.execute(
                "SELECT resolution_attempts FROM deliveries WHERE delivery_guid = ?",
                (delivery_guid,),
            ).fetchone()
            if row is None:
                raise StoreError(f"unknown delivery {delivery_guid}")
            attempts = int(row["resolution_attempts"]) + 1
            exhausted = attempts >= max_attempts
            conn.execute(
                "UPDATE deliveries SET resolution_attempts = ?, resolution_retry_at_us = ?, "
                "status = 'unresolved' WHERE delivery_guid = ? "
                "AND status IN ('pending', 'unresolved')",
                (attempts, None if exhausted else retry_at_us, delivery_guid),
            )
        return exhausted

    def _mark_delivery(self, conn: sqlite3.Connection, guid: str, status: str) -> None:
        cur = conn.execute(
            "UPDATE deliveries SET status = ?, processed_at_us = ? WHERE delivery_guid = ?",
            (status, self._clock.now_utc_us(), guid),
        )
        if cur.rowcount != 1:
            raise StoreError(f"unknown delivery {guid}")

    # -------------------------------------------------------- application

    def load_parcel(self, parcel_id: str) -> Parcel | None:
        row = self._conn.execute(
            "SELECT aggregate_json FROM parcels WHERE parcel_id = ?", (parcel_id,)
        ).fetchone()
        return None if row is None else codec.parcel_from_json(row["aggregate_json"])

    def has_event(self, event_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM events WHERE logical_key = ?", (event_id,)
        ).fetchone()
        return row is not None

    def apply_event(
        self,
        event: Event,
        config: TrustedConfig,
        *,
        reducer: Reducer = transition,
        delivery_status: str | None = None,
    ) -> ApplyResult:
        """Run the reducer and commit everything it produced in one transaction.

        ``event.entropy`` is filled with a fresh random value when empty, then persisted.
        When ``event.delivery_guid`` is set, that delivery is marked ``delivery_status``
        (default ``processed``) in the same transaction.
        """
        with self._txn() as conn:
            return self._apply_in_txn(conn, event, config, reducer, delivery_status)

    def _apply_in_txn(
        self,
        conn: sqlite3.Connection,
        event: Event,
        config: TrustedConfig,
        reducer: Reducer,
        delivery_status: str | None,
    ) -> ApplyResult:
        if not event.entropy:
            event = replace(event, entropy=secrets.token_hex(16))
        now = self._clock.now_utc_us()
        if conn.execute("SELECT 1 FROM events WHERE logical_key = ?", (event.event_id,)).fetchone():
            existing = (
                self._load_parcel_row(conn, event.parcel_id)
                if event.parcel_id is not None
                else None
            )
            return ApplyResult(
                True,
                None,
                False,
                "duplicate",
                (),
                existing,
                self._load_admission(conn, event.repo_id),
            )
        admission = self._load_admission(conn, event.repo_id)
        parcel: Parcel | None = None
        if event.kind not in (EventKind.PAUSE, EventKind.UNPAUSE) or event.parcel_id:
            if event.parcel_id is None:
                raise StoreError("parcel event without parcel_id")
            parcel = self._load_parcel_row(conn, event.parcel_id) or Parcel(
                parcel_id=event.parcel_id,
                repo_id=event.repo_id,
                issue_number=event.issue_number,
            )
        old_parcel = parcel
        result = reducer(State(parcel=parcel, admission=admission, config=config), event)
        new = result.state
        self._check_caps(admission, new.admission, config)
        seq_row = conn.execute("SELECT COALESCE(MAX(sequence), 0) + 1 AS s FROM events")
        sequence = int(seq_row.fetchone()["s"])
        if new.parcel is not None:
            self._write_parcel(conn, new.parcel, now)
        self._hook("after-parcel-write")
        conn.execute(
            "INSERT INTO events (event_id, logical_key, sequence, repo_id, parcel_id, "
            "delivery_guid, kind, class, actor_id, provenance, source_time_us, payload_json, "
            "accepted, reason, applied_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?)",
            (
                event.event_id,
                event.event_id,
                sequence,
                event.repo_id,
                event.parcel_id if new.parcel is not None else None,
                event.delivery_guid,
                event.kind.value,
                event.event_class.value,
                event.actor_id,
                event.provenance.value,
                event.source_time_us,
                codec.event_to_json(event),
                int(result.audit.accepted),
                result.audit.reason,
                now,
            ),
        )
        self._hook("after-event-insert")
        if new.parcel is not None:
            self._project(conn, old_parcel, new.parcel, event, now)
        self._write_admission(conn, new.admission, now)
        for effect in result.effects:
            self._insert_effect(conn, effect, event, now)
        self._hook("after-effects")
        conn.execute(
            "INSERT INTO audit (parcel_id, event_id, before_digest, after_digest, accepted, "
            "reason, detail_json, created_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.parcel_id,
                event.event_id,
                _digest(codec.parcel_to_json(old_parcel)) if old_parcel else None,
                _digest(codec.parcel_to_json(new.parcel)) if new.parcel else None,
                int(result.audit.accepted),
                result.audit.reason,
                codec.dumps(
                    {
                        "kind": event.kind.value,
                        "effects": [e.effect_id for e in result.effects],
                        "dropped_effects": result.audit.dropped_effects,
                    }
                ),
                now,
            ),
        )
        if event.delivery_guid is not None:
            self._mark_delivery(conn, event.delivery_guid, delivery_status or "processed")
        _log_transition(event, old_parcel, new.parcel, result)
        return ApplyResult(
            duplicate=False,
            sequence=sequence,
            accepted=result.audit.accepted,
            reason=result.audit.reason,
            effects=result.effects,
            parcel=new.parcel,
            admission=new.admission,
        )

    @staticmethod
    def _check_caps(
        before: AdmissionSnapshot, after: AdmissionSnapshot, config: TrustedConfig
    ) -> None:
        if after.building_count > config.max_building and (
            after.building_count > before.building_count
        ):
            raise InvariantViolation("building reservations would exceed max_building")
        if after.prospective_pr_count > config.max_open_bot_prs and (
            len(after.live_reservations(ReservationKind.OPEN_PR))
            > len(before.live_reservations(ReservationKind.OPEN_PR))
        ):
            raise InvariantViolation("prospective PRs would exceed max_open_bot_prs")

    @staticmethod
    def _load_parcel_row(conn: sqlite3.Connection, parcel_id: str) -> Parcel | None:
        row = conn.execute(
            "SELECT aggregate_json FROM parcels WHERE parcel_id = ?", (parcel_id,)
        ).fetchone()
        return None if row is None else codec.parcel_from_json(row["aggregate_json"])

    def _write_parcel(self, conn: sqlite3.Connection, p: Parcel, now: int) -> None:
        conn.execute(
            "INSERT INTO parcels (parcel_id, repo_id, issue_number, stage, version, "
            "eligibility_epoch, revision, revision_pending, current_session_id, "
            "current_contract_id, current_approval_id, pending_authorization_id, holds_json, "
            "bot, aggregate_json, updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?) ON CONFLICT(parcel_id) DO UPDATE SET stage = excluded.stage, "
            "version = excluded.version, eligibility_epoch = excluded.eligibility_epoch, "
            "revision = excluded.revision, revision_pending = excluded.revision_pending, "
            "current_session_id = excluded.current_session_id, "
            "current_contract_id = excluded.current_contract_id, "
            "current_approval_id = excluded.current_approval_id, "
            "pending_authorization_id = excluded.pending_authorization_id, "
            "holds_json = excluded.holds_json, bot = excluded.bot, "
            "aggregate_json = excluded.aggregate_json, updated_at_us = excluded.updated_at_us",
            (
                p.parcel_id,
                p.repo_id,
                p.issue_number,
                p.stage.value if p.stage else None,
                p.version,
                p.eligibility_epoch,
                p.revision,
                int(p.revision_pending),
                p.current_session_id,
                p.current_contract_id,
                p.current_approval_id,
                p.pending_authorization_id,
                codec.dumps(sorted(h.value for h in p.holds)),
                p.bot.value,
                codec.parcel_to_json(p),
                now,
            ),
        )

    def _project(
        self, conn: sqlite3.Connection, old: Parcel | None, p: Parcel, event: Event, now: int
    ) -> None:
        """Maintain relational projections of the aggregate (defense-in-depth constraints)."""
        for a in p.authorizations:
            conn.execute(
                "INSERT INTO stage_authorizations (authorization_id, parcel_id, kind, generation, "
                "source_event_id, revision, eligibility_epoch, approval_id, grant_duration_us, "
                "cancelled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(authorization_id) "
                "DO UPDATE SET cancelled = excluded.cancelled",
                (
                    a.authorization_id,
                    p.parcel_id,
                    a.kind.value,
                    a.generation,
                    a.source_event_id,
                    a.revision,
                    a.eligibility_epoch,
                    a.approval_id,
                    a.grant_duration_us,
                    int(a.cancelled),
                ),
            )
        old_fences = {s.session_id: s.fences for s in (old.sessions if old else ())}
        # Close gates first so the partial unique index sees the post-state.
        ordered = sorted(p.sessions, key=lambda s: not (s.execution_closed or s.fences))
        for s in ordered:
            auth = p.authorization(s.authorization_id)
            mask = 0
            for f in s.fences:
                mask |= FENCE_BITS[f]
            conn.execute(
                "INSERT INTO stage_sessions (session_id, parcel_id, kind, generation, attempt, "
                "authorization_id, omnigent_root_id, nonce, lifecycle, execution_closed, "
                "fence_mask, revision, restart_count, correction_count) VALUES (?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET "
                "omnigent_root_id = excluded.omnigent_root_id, lifecycle = excluded.lifecycle, "
                "execution_closed = excluded.execution_closed, fence_mask = excluded.fence_mask, "
                "revision = excluded.revision, restart_count = excluded.restart_count, "
                "correction_count = excluded.correction_count",
                (
                    s.session_id,
                    p.parcel_id,
                    s.kind.value,
                    auth.generation if auth else 0,
                    s.attempt,
                    s.authorization_id,
                    s.root_id,
                    s.nonce,
                    s.lifecycle.value,
                    int(s.execution_closed or s.lifecycle == Lifecycle.RETIRED),
                    mask,
                    s.revision,
                    s.restart_count,
                    s.correction_count,
                ),
            )
            before = old_fences.get(s.session_id, frozenset())
            for f in sorted(s.fences - before):
                conn.execute(
                    "INSERT INTO fences (session_id, kind, cause_event_id, set_at_us) "
                    "VALUES (?, ?, ?, ?)",
                    (s.session_id, f.value, event.event_id, now),
                )
            for f in sorted(before - s.fences):
                conn.execute(
                    "UPDATE fences SET cleared_at_us = ?, cleared_by_event_id = ? "
                    "WHERE session_id = ? AND kind = ? AND cleared_at_us IS NULL",
                    (now, event.event_id, s.session_id, f.value),
                )
            g = s.grant
            conn.execute(
                "UPDATE grants SET is_current = 0 WHERE session_id = ? AND grant_id != ?",
                (s.session_id, g.grant_id),
            )
            conn.execute(
                "INSERT INTO grants (grant_id, session_id, source_event_id, duration_us, "
                "consumed_us, ready, grace_deadline_us, policy_generation, is_current) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, 1) ON CONFLICT(grant_id) DO UPDATE SET "
                "consumed_us = excluded.consumed_us, ready = excluded.ready, "
                "grace_deadline_us = excluded.grace_deadline_us, is_current = 1",
                (
                    g.grant_id,
                    s.session_id,
                    g.source_event_id,
                    g.duration_us,
                    g.consumed_us,
                    int(g.ready),
                    g.grace_deadline_us,
                    g.policy_generation,
                ),
            )
        for c in sorted(p.contracts, key=lambda c: not c.superseded):
            conn.execute(
                "INSERT INTO contracts (contract_id, parcel_id, revision, canonical, full_hash, "
                "prefix, comment_id, published, posted_at_us, intact, superseded, "
                "source_session_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(contract_id) DO UPDATE SET comment_id = excluded.comment_id, "
                "published = excluded.published, posted_at_us = excluded.posted_at_us, "
                "intact = excluded.intact, superseded = excluded.superseded",
                (
                    c.contract_id,
                    p.parcel_id,
                    c.revision,
                    c.canonical,
                    c.full_hash,
                    c.prefix,
                    c.comment_id,
                    int(c.published),
                    c.posted_at_us,
                    int(c.intact),
                    int(c.superseded),
                    c.source_session_id,
                ),
            )
        for ap in p.approvals:
            conn.execute(
                "INSERT INTO approvals (approval_id, parcel_id, kind, full_hash, contract_id, "
                "snapshot_canonical, owner_id, source_event_id, source_time_us, sequence, "
                "invalidated_reason, invalidated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?) ON CONFLICT(approval_id) DO UPDATE SET "
                "invalidated_reason = excluded.invalidated_reason, "
                "invalidated_at_us = excluded.invalidated_at_us",
                (
                    ap.approval_id,
                    p.parcel_id,
                    ap.kind.value,
                    ap.full_hash,
                    ap.contract_id,
                    ap.snapshot_canonical,
                    ap.owner_id,
                    ap.source_event_id,
                    ap.source_time_us,
                    ap.sequence,
                    ap.invalidated_reason,
                    ap.invalidated_at_us,
                ),
            )
        for d in p.decisions:
            conn.execute(
                "INSERT INTO decisions (decision_id, parcel_id, session_id, elicitation_id, "
                "revision, impact, status, checkpoint_prompt, answer, answer_event_id) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(decision_id) DO UPDATE SET "
                "status = excluded.status, answer = excluded.answer, "
                "answer_event_id = excluded.answer_event_id",
                (
                    d.decision_id,
                    p.parcel_id,
                    d.session_id,
                    d.elicitation_id,
                    d.revision,
                    d.impact.value,
                    d.status.value,
                    int(d.checkpoint_prompt),
                    d.answer,
                    d.answer_event_id,
                ),
            )

    def _write_admission(self, conn: sqlite3.Connection, a: AdmissionSnapshot, now: int) -> None:
        conn.execute(
            "UPDATE repositories SET paused = ?, next_sequence = ?, open_bot_prs_json = ?, "
            "updated_at_us = ? WHERE repo_id = ?",
            (int(a.paused), a.next_sequence, codec.dumps(sorted(a.open_bot_prs)), now, a.repo_id),
        )
        for q in a.queue:
            conn.execute(
                "INSERT INTO queue (parcel_id, repo_id, approval_id, approval_sequence, status) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(parcel_id) DO UPDATE SET "
                "approval_id = excluded.approval_id, "
                "approval_sequence = excluded.approval_sequence, status = excluded.status",
                (q.parcel_id, a.repo_id, q.approval_id, q.sequence, q.status.value),
            )
        # Release before reserve so the partial unique index sees the post-state.
        for r in sorted(a.reservations, key=lambda r: r.live):
            conn.execute(
                "INSERT INTO reservations (reservation_id, repo_id, parcel_id, kind, episode_id, "
                "pr_number, live) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(reservation_id) "
                "DO UPDATE SET pr_number = excluded.pr_number, live = excluded.live",
                (
                    r.reservation_id,
                    a.repo_id,
                    r.parcel_id,
                    r.kind.value,
                    r.episode_id,
                    r.pr_number,
                    int(r.live),
                ),
            )

    def _insert_effect(
        self, conn: sqlite3.Connection, effect: EffectIntent, event: Event, now: int
    ) -> None:
        payload = codec.effect_to_json(effect)
        conn.execute(
            "INSERT INTO effects (effect_id, parcel_id, event_id, parcel_version, kind, target, "
            "payload_json, retry_class, dedupe_key, state, created_at_us, updated_at_us) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (
                effect.effect_id,
                effect.parcel_id,
                event.event_id,
                effect.preconditions.parcel_version,
                effect.kind.value,
                effect.target,
                payload,
                effect.retry_class.value,
                effect.dedupe_key,
                now,
                now,
            ),
        )
        if effect.kind == EffectKind.CREATE_SESSION:
            nonce = effect.args.get("nonce")
            conn.execute(
                "INSERT INTO dispatch_intents (effect_id, session_id, nonce, request_digest, "
                "request_json, create_state) VALUES (?, ?, ?, ?, ?, 'pending')",
                (
                    effect.effect_id,
                    effect.preconditions.session_id,
                    nonce,
                    hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                    payload,
                ),
            )

    # --------------------------------------------------------------- outbox

    def pending_effects(self, *, now_us: int | None = None, limit: int = 100) -> list[StoredEffect]:
        now = self._clock.now_utc_us() if now_us is None else now_us
        rows = self._conn.execute(
            "SELECT e.* FROM effects e JOIN events v ON v.event_id = e.event_id "
            "WHERE e.state = 'pending' AND (e.next_at_us IS NULL OR e.next_at_us <= ?) "
            "ORDER BY v.sequence, e.rowid LIMIT ?",
            (now, limit),
        ).fetchall()
        return [self._stored(r) for r in rows]

    def effects_in_state(self, state: str) -> list[StoredEffect]:
        rows = self._conn.execute(
            "SELECT * FROM effects WHERE state = ? ORDER BY rowid", (state,)
        ).fetchall()
        return [self._stored(r) for r in rows]

    def get_effect(self, effect_id: str) -> StoredEffect | None:
        row = self._conn.execute(
            "SELECT * FROM effects WHERE effect_id = ?", (effect_id,)
        ).fetchone()
        return None if row is None else self._stored(row)

    @staticmethod
    def _stored(r: sqlite3.Row) -> StoredEffect:
        return StoredEffect(
            effect=codec.effect_from_json(r["payload_json"]),
            state=r["state"],
            attempts=int(r["attempts"]),
            next_at_us=r["next_at_us"],
            lease_epoch=r["lease_epoch"],
            remote_id=r["remote_id"],
        )

    def claim_effect(self, effect_id: str, lease: Lease) -> StoredEffect | None:
        """Claim a pending effect under the caller's current parcel lease.

        Returns ``None`` if the effect is not pending (already claimed/done/cancelled) or
        the lease is not the live lease for the effect's parcel.
        """
        with self._txn() as conn:
            row = conn.execute(
                "SELECT parcel_id FROM effects WHERE effect_id = ?", (effect_id,)
            ).fetchone()
            if row is None:
                return None
            if row["parcel_id"] is not None:
                held = conn.execute(
                    "SELECT boot_id, epoch FROM leases WHERE parcel_id = ?", (row["parcel_id"],)
                ).fetchone()
                if held is None or (held["boot_id"], held["epoch"]) != (lease.boot_id, lease.epoch):
                    return None
            cur = conn.execute(
                "UPDATE effects SET state = 'claimed', attempts = attempts + 1, lease_epoch = ?, "
                "claimed_by_boot = ?, updated_at_us = ? WHERE effect_id = ? AND state = 'pending'",
                (lease.epoch, lease.boot_id, self._clock.now_utc_us(), effect_id),
            )
            if cur.rowcount != 1:
                return None
            if row["parcel_id"] is not None:
                self._hook("after-claim")
            r = conn.execute("SELECT * FROM effects WHERE effect_id = ?", (effect_id,)).fetchone()
        return self._stored(r)

    def _finish_effect(
        self,
        effect_id: str,
        state: str,
        *,
        from_states: tuple[str, ...],
        remote_id: str | None,
        reason: str | None,
        next_at_us: int | None = None,
    ) -> bool:
        with self._txn() as conn:
            return self._update_effect(
                conn,
                effect_id,
                state,
                from_states=from_states,
                remote_id=remote_id,
                reason=reason,
                next_at_us=next_at_us,
            )

    def _update_effect(
        self,
        conn: sqlite3.Connection,
        effect_id: str,
        state: str,
        *,
        from_states: tuple[str, ...],
        remote_id: str | None,
        reason: str | None,
        next_at_us: int | None = None,
    ) -> bool:
        placeholders = ",".join("?" for _ in from_states)
        cur = conn.execute(
            f"UPDATE effects SET state = ?, remote_id = COALESCE(?, remote_id), "
            f"outcome_reason = ?, next_at_us = ?, updated_at_us = ? "
            f"WHERE effect_id = ? AND state IN ({placeholders})",
            (
                state,
                remote_id,
                reason,
                next_at_us,
                self._clock.now_utc_us(),
                effect_id,
                *from_states,
            ),
        )
        if state == "done" and cur.rowcount == 1:
            conn.execute(
                "UPDATE dispatch_intents SET create_state = 'done', adopted_root_id = ? "
                "WHERE effect_id = ?",
                (remote_id, effect_id),
            )
        elif state in ("unknown", "cancelled", "failed") and cur.rowcount == 1:
            conn.execute(
                "UPDATE dispatch_intents SET create_state = ? WHERE effect_id = ?",
                (state, effect_id),
            )
        return cur.rowcount == 1

    def record_effect_outcome(
        self,
        effect_id: str,
        state: str,
        *,
        from_states: tuple[str, ...],
        event: Event | None,
        config: TrustedConfig,
        remote_id: str | None = None,
        reason: str | None = None,
        reducer: Reducer = transition,
    ) -> EffectOutcomeResult:
        """Atomically move an effect to ``state`` and apply the event reporting it.

        Nothing is written unless the effect is currently in one of ``from_states``: an
        outcome can never be recorded twice, nor an event applied for an effect another
        path already finished. The effect update and the reducer transition (with its
        projections, admission and new intents) commit in one transaction.
        """
        if state not in ("done", "failed", "cancelled", "unknown"):
            raise StoreError(f"not an outcome state: {state}")
        with self._txn() as conn:
            row = conn.execute(
                "SELECT state FROM effects WHERE effect_id = ?", (effect_id,)
            ).fetchone()
            if row is None or row["state"] not in from_states:
                return EffectOutcomeResult(False, None)
            applied = (
                self._apply_in_txn(conn, event, config, reducer, None)
                if event is not None
                else None
            )
            self._hook("after-outcome-event")
            updated = self._update_effect(
                conn,
                effect_id,
                state,
                from_states=from_states,
                remote_id=remote_id,
                reason=reason,
            )
            if not updated:
                raise StoreError(f"effect {effect_id} changed state inside its transaction")
        return EffectOutcomeResult(True, applied)

    def complete_effect(self, effect_id: str, *, remote_id: str | None = None) -> bool:
        return self._finish_effect(
            effect_id, "done", from_states=("claimed", "unknown"), remote_id=remote_id, reason=None
        )

    def mark_effect_unknown(self, effect_id: str, reason: str) -> bool:
        """Ambiguous write: never returns to ``pending`` automatically."""
        return self._finish_effect(
            effect_id, "unknown", from_states=("claimed",), remote_id=None, reason=reason
        )

    def fail_effect(self, effect_id: str, reason: str, *, retry_at_us: int | None = None) -> bool:
        """Definitive failure, or a retryable read failure when ``retry_at_us`` is given."""
        if retry_at_us is not None:
            return self._finish_effect(
                effect_id,
                "pending",
                from_states=("claimed",),
                remote_id=None,
                reason=reason,
                next_at_us=retry_at_us,
            )
        return self._finish_effect(
            effect_id, "failed", from_states=("claimed",), remote_id=None, reason=reason
        )

    def cancel_effect(self, effect_id: str, reason: str) -> bool:
        return self._finish_effect(
            effect_id,
            "cancelled",
            from_states=("pending", "claimed"),
            remote_id=None,
            reason=reason,
        )

    def requeue_effect(self, effect_id: str, kinds: frozenset[str]) -> bool:
        """Operator retry: a failed/unknown effect of ``kinds`` returns to ``pending``.

        Only for adoptable publications, whose adapter finds an existing marked comment
        before writing, so the retry adopts rather than duplicates.
        """
        placeholders = ",".join("?" for _ in kinds)
        with self._txn() as conn:
            cur = conn.execute(
                f"UPDATE effects SET state = 'pending', attempts = 0, next_at_us = NULL, "
                f"outcome_reason = 'operator-retry', updated_at_us = ? "
                f"WHERE effect_id = ? AND state IN ('failed', 'unknown') "
                f"AND kind IN ({placeholders})",
                (self._clock.now_utc_us(), effect_id, *sorted(kinds)),
            )
            return cur.rowcount == 1

    def recover_claimed(self) -> list[StoredEffect]:
        """On startup: claimed-but-unfinished effects become ``unknown`` (never re-sent).

        Retry-safe classes (reads, local idempotent operations) return to ``pending``.
        """
        out: list[StoredEffect] = []
        with self._txn() as conn:
            rows = conn.execute("SELECT * FROM effects WHERE state = 'claimed'").fetchall()
            now = self._clock.now_utc_us()
            for r in rows:
                safe = r["retry_class"] in ("read", "local_idempotent")
                new_state = "pending" if safe else "unknown"
                conn.execute(
                    "UPDATE effects SET state = ?, outcome_reason = 'process-restart', "
                    "updated_at_us = ? WHERE effect_id = ?",
                    (new_state, now, r["effect_id"]),
                )
                # Keep the dispatch intent coherent with its outbox row (one source of truth).
                conn.execute(
                    "UPDATE dispatch_intents SET create_state = ? WHERE effect_id = ?",
                    (new_state, r["effect_id"]),
                )
                out.append(replace(self._stored(r), state=new_state))
        return out

    # ------------------------------------------------------- own-send ledger

    def record_own_send(self, send: OwnSendRow) -> OwnSendRow:
        """Durably record a send/resolve intent BEFORE its POST. First record wins.

        Returns the persisted row: a replay after a crash keeps the original digest, so a
        lost acknowledgement is always reconciled against the text actually sent first.
        """
        if send.kind not in ("message", "resolve"):
            raise StoreError(f"unknown own-send kind {send.kind}")
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO own_sends (effect_id, session_id, node_id, kind, text_sha256, "
                "elicitation_id, item_id, recorded_at_us) VALUES (?, ?, ?, ?, ?, ?, NULL, ?) "
                "ON CONFLICT(effect_id) DO NOTHING",
                (
                    send.effect_id,
                    send.session_id,
                    send.node_id,
                    send.kind,
                    send.text_sha256,
                    send.elicitation_id,
                    self._clock.now_utc_us(),
                ),
            )
            row = conn.execute(
                "SELECT * FROM own_sends WHERE effect_id = ?", (send.effect_id,)
            ).fetchone()
        return _own_send(row)

    def record_own_item(self, effect_id: str, item_id: str) -> bool:
        """Bind the acknowledged/adopted item to a recorded send. Never rebinds."""
        with self._txn() as conn:
            row = conn.execute(
                "SELECT item_id FROM own_sends WHERE effect_id = ?", (effect_id,)
            ).fetchone()
            if row is None:
                return False
            if row["item_id"] is not None:
                if row["item_id"] != item_id:
                    raise InvariantViolation(f"own send {effect_id} is bound to another item")
                return True
            conn.execute(
                "UPDATE own_sends SET item_id = ?, acknowledged_at_us = ? WHERE effect_id = ?",
                (item_id, self._clock.now_utc_us(), effect_id),
            )
        return True

    def own_send(self, effect_id: str) -> OwnSendRow | None:
        row = self._conn.execute(
            "SELECT * FROM own_sends WHERE effect_id = ?", (effect_id,)
        ).fetchone()
        return None if row is None else _own_send(row)

    def own_item_ids(self, session_id: str) -> frozenset[str]:
        rows = self._conn.execute(
            "SELECT item_id FROM own_sends WHERE session_id = ? AND item_id IS NOT NULL",
            (session_id,),
        ).fetchall()
        return frozenset(str(r["item_id"]) for r in rows)

    # ---------------------------------------------------------- capabilities

    def save_capability(self, row: CapabilityRow) -> None:
        """Persist a (rotated) capability hash. Generations only ever increase."""
        with self._txn() as conn:
            prior = conn.execute(
                "SELECT generation FROM capability_records WHERE session_key = ?",
                (row.session_key,),
            ).fetchone()
            if prior is not None and int(prior["generation"]) >= row.generation:
                raise InvariantViolation(
                    f"capability generation for {row.session_key} must increase"
                )
            conn.execute(
                "INSERT INTO capability_records (session_key, stage_session_id, worker_id, "
                "worker_profile, capability_id, secret_sha256, generation, path, revoked, "
                "updated_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?) "
                "ON CONFLICT(session_key) DO UPDATE SET "
                "stage_session_id = excluded.stage_session_id, worker_id = excluded.worker_id, "
                "worker_profile = excluded.worker_profile, "
                "capability_id = excluded.capability_id, "
                "secret_sha256 = excluded.secret_sha256, generation = excluded.generation, "
                "path = excluded.path, revoked = 0, updated_at_us = excluded.updated_at_us",
                (
                    row.session_key,
                    row.stage_session_id,
                    row.worker_id,
                    row.worker_profile,
                    row.capability_id,
                    row.secret_sha256,
                    row.generation,
                    row.path,
                    self._clock.now_utc_us(),
                ),
            )

    def revoke_capability(self, session_key: str) -> None:
        """Mark revoked; the row (and its generation) is retained."""
        with self._txn() as conn:
            conn.execute(
                "UPDATE capability_records SET revoked = 1, updated_at_us = ? "
                "WHERE session_key = ?",
                (self._clock.now_utc_us(), session_key),
            )

    def capability_rows(self) -> list[CapabilityRow]:
        """Every capability row, revoked ones included (for generation continuity)."""
        rows = self._conn.execute(
            "SELECT * FROM capability_records ORDER BY session_key"
        ).fetchall()
        return [
            CapabilityRow(
                session_key=r["session_key"],
                stage_session_id=r["stage_session_id"],
                worker_id=r["worker_id"],
                worker_profile=r["worker_profile"],
                capability_id=r["capability_id"],
                secret_sha256=r["secret_sha256"],
                generation=int(r["generation"]),
                path=r["path"],
                revoked=bool(r["revoked"]),
            )
            for r in rows
        ]

    # --------------------------------------------------------- worker grants

    def save_worker_grant(self, row: WorkerGrantRow) -> None:
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO worker_grants (stage_session_id, worker_id, path, branch, profile, "
                "recorded_at_us) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(stage_session_id, worker_id) DO UPDATE SET path = excluded.path, "
                "branch = excluded.branch, profile = excluded.profile, "
                "recorded_at_us = excluded.recorded_at_us",
                (
                    row.stage_session_id,
                    row.worker_id,
                    row.path,
                    row.branch,
                    row.profile,
                    self._clock.now_utc_us(),
                ),
            )

    def delete_worker_grants(self, stage_session_id: str) -> None:
        with self._txn() as conn:
            conn.execute(
                "DELETE FROM worker_grants WHERE stage_session_id = ?", (stage_session_id,)
            )

    def worker_grant_rows(self) -> list[WorkerGrantRow]:
        rows = self._conn.execute(
            "SELECT * FROM worker_grants ORDER BY stage_session_id, worker_id"
        ).fetchall()
        return [
            WorkerGrantRow(
                r["stage_session_id"], r["worker_id"], r["path"], r["branch"], r["profile"]
            )
            for r in rows
        ]

    # ------------------------------------------------------ parked deliveries

    def park_delivery(
        self,
        delivery_guid: str,
        parcel_id: str | None,
        *,
        hold: Event | None = None,
        config: TrustedConfig | None = None,
        reason: str | None = None,
    ) -> None:
        """Park a poison delivery: scope row, ``rejected`` status and parcel hold, atomically.

        Re-parking never narrows scope: a repository-wide park stays repository-wide.
        """
        with self._txn() as conn:
            self._park(conn, delivery_guid, parcel_id)
            if reason is not None:
                conn.execute(
                    "UPDATE parked_deliveries SET reason = ? WHERE delivery_guid = ?",
                    (reason, delivery_guid),
                )
            self._hook("after-park-row")
            self._mark_delivery(conn, delivery_guid, "rejected")
            if hold is not None:
                if config is None:
                    raise StoreError("a parcel hold needs the trusted config")
                self._apply_in_txn(conn, hold, config, transition, None)

    def _park(self, conn: sqlite3.Connection, delivery_guid: str, parcel_id: str | None) -> None:
        conn.execute(
            "INSERT INTO parked_deliveries (delivery_guid, parcel_id, parked_at_us) "
            "VALUES (?, ?, ?) ON CONFLICT(delivery_guid) DO UPDATE SET "
            "parcel_id = CASE WHEN parked_deliveries.parcel_id = excluded.parcel_id "
            "THEN excluded.parcel_id ELSE NULL END",
            (delivery_guid, parcel_id, self._clock.now_utc_us()),
        )

    def release_parked_delivery(
        self,
        delivery_guid: str,
        *,
        release: Event | None = None,
        config: TrustedConfig | None = None,
    ) -> bool:
        """Operator release: drop the scope row, requeue the delivery, release the hold."""
        with self._txn() as conn:
            cur = conn.execute(
                "DELETE FROM parked_deliveries WHERE delivery_guid = ?", (delivery_guid,)
            )
            if cur.rowcount != 1:
                return False
            status = conn.execute(
                "SELECT status FROM deliveries WHERE delivery_guid = ?", (delivery_guid,)
            ).fetchone()
            if status is not None and status["status"] == "rejected":
                self._mark_delivery(conn, delivery_guid, "pending")
            # The operator's release grants a fresh bounded resolution budget.
            conn.execute(
                "UPDATE deliveries SET resolution_attempts = 0, resolution_retry_at_us = NULL "
                "WHERE delivery_guid = ?",
                (delivery_guid,),
            )
            if release is not None:
                if config is None:
                    raise StoreError("a hold release needs the trusted config")
                self._apply_in_txn(conn, release, config, transition, None)
        return True

    def parked_delivery_rows(self) -> tuple[tuple[str, str | None], ...]:
        rows = self._conn.execute(
            "SELECT delivery_guid, parcel_id FROM parked_deliveries ORDER BY delivery_guid"
        ).fetchall()
        return tuple((str(r["delivery_guid"]), r["parcel_id"]) for r in rows)

    def sync_parked_deliveries(
        self,
        legacy: Sequence[tuple[str, str | None]] = (),
        *,
        holds: Sequence[Event] = (),
        config: TrustedConfig | None = None,
    ) -> None:
        """Startup reconciliation, one transaction (fail closed in every direction).

        * import ``legacy`` Task-4 registry entries with their scope, plus parcel holds;
        * a ``rejected`` delivery without a scope row is parked repository-wide;
        * a parked delivery whose row is ``pending``/``unresolved`` is re-marked rejected.
        """
        with self._txn() as conn:
            for delivery_guid, parcel_id in legacy:
                self._park(conn, delivery_guid, parcel_id)
            for hold in holds:
                if config is None:
                    raise StoreError("a parcel hold needs the trusted config")
                self._apply_in_txn(conn, hold, config, transition, None)
            conn.execute(
                "INSERT INTO parked_deliveries (delivery_guid, parcel_id, parked_at_us) "
                "SELECT delivery_guid, NULL, ? FROM deliveries WHERE status = 'rejected' "
                "AND delivery_guid NOT IN (SELECT delivery_guid FROM parked_deliveries)",
                (self._clock.now_utc_us(),),
            )
            conn.execute(
                "UPDATE deliveries SET status = 'rejected', processed_at_us = ? "
                "WHERE status IN ('pending', 'unresolved') "
                "AND delivery_guid IN (SELECT delivery_guid FROM parked_deliveries)",
                (self._clock.now_utc_us(),),
            )

    # --------------------------------------------------------------- leases

    def acquire_lease(self, parcel_id: str, boot_id: str, *, takeover: bool = False) -> Lease:
        """Acquire (or re-enter) the parcel lease; epoch increases on every new holder.

        ``takeover=True`` asserts the previous holder's process is gone (the caller holds
        the state-directory process lock). Without it a foreign holder raises LeaseHeld.
        """
        now = self._clock.now_utc_us()
        with self._txn() as conn:
            row = conn.execute("SELECT * FROM leases WHERE parcel_id = ?", (parcel_id,)).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO leases (parcel_id, boot_id, epoch, heartbeat_at_us) "
                    "VALUES (?, ?, 1, ?)",
                    (parcel_id, boot_id, now),
                )
                return Lease(parcel_id, boot_id, 1)
            if row["boot_id"] == boot_id:
                conn.execute(
                    "UPDATE leases SET heartbeat_at_us = ? WHERE parcel_id = ?", (now, parcel_id)
                )
                return Lease(parcel_id, boot_id, int(row["epoch"]))
            if not takeover:
                raise LeaseHeld(f"parcel {parcel_id} leased by boot {row['boot_id']}")
            epoch = int(row["epoch"]) + 1
            conn.execute(
                "UPDATE leases SET boot_id = ?, epoch = ?, heartbeat_at_us = ? WHERE parcel_id = ?",
                (boot_id, epoch, now, parcel_id),
            )
            return Lease(parcel_id, boot_id, epoch)

    def heartbeat(self, lease: Lease) -> bool:
        with self._txn() as conn:
            cur = conn.execute(
                "UPDATE leases SET heartbeat_at_us = ? WHERE parcel_id = ? AND boot_id = ? "
                "AND epoch = ?",
                (self._clock.now_utc_us(), lease.parcel_id, lease.boot_id, lease.epoch),
            )
            return cur.rowcount == 1

    def release_lease(self, lease: Lease) -> bool:
        with self._txn() as conn:
            cur = conn.execute(
                "DELETE FROM leases WHERE parcel_id = ? AND boot_id = ? AND epoch = ?",
                (lease.parcel_id, lease.boot_id, lease.epoch),
            )
            return cur.rowcount == 1

    # ---------------------------------------------------------------- audit

    def audit_entries(self, parcel_id: str | None = None) -> list[dict[str, object]]:
        if parcel_id is None:
            rows = self._conn.execute("SELECT * FROM audit ORDER BY sequence").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM audit WHERE parcel_id = ? ORDER BY sequence", (parcel_id,)
            ).fetchall()
        return [dict(r) for r in rows]

    def events_for(self, parcel_id: str) -> list[Event]:
        rows = self._conn.execute(
            "SELECT payload_json FROM events WHERE parcel_id = ? ORDER BY sequence", (parcel_id,)
        ).fetchall()
        return [codec.event_from_json(r["payload_json"]) for r in rows]

    def query(self, sql: str, params: Sequence[object] = ()) -> list[sqlite3.Row]:
        """Read-only helper for diagnostics and tests."""
        return self._conn.execute(sql, tuple(params)).fetchall()


def _own_send(r: sqlite3.Row) -> OwnSendRow:
    return OwnSendRow(
        effect_id=r["effect_id"],
        session_id=r["session_id"],
        node_id=r["node_id"],
        kind=r["kind"],
        text_sha256=r["text_sha256"],
        elicitation_id=r["elicitation_id"],
        item_id=r["item_id"],
    )


def _split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buf))
            buf = []
    if "".join(buf).strip():
        statements.append("\n".join(buf))
    return statements


def _log_transition(
    event: Event, old: Parcel | None, new: Parcel | None, result: TransitionResult
) -> None:
    """One line per applied event: stage/Bot movement, reason and emitted effects."""
    before = (old.stage, old.bot) if old is not None else (None, None)
    after = (new.stage, new.bot) if new is not None else (None, None)
    quiet = event.kind in _QUIET_KINDS and result.audit.accepted and before == after
    LOG.log(
        logging.DEBUG if quiet else logging.INFO,
        "parcel transition kind=%s parcel=%s issue=%s accepted=%s reason=%s "
        "stage=%s->%s bot=%s->%s effects=%s",
        event.kind.value,
        event.parcel_id,
        new.issue_number if new is not None else None,
        result.audit.accepted,
        result.audit.reason,
        _value(before[0]),
        _value(after[0]),
        _value(before[1]),
        _value(after[1]),
        ",".join(e.kind.value for e in result.effects) or "-",
    )


def _value(item: object) -> object:
    value = getattr(item, "value", None)
    return item if value is None else value
