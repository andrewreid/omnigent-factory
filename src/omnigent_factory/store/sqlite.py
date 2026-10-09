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
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

from omnigent_factory.core import codec
from omnigent_factory.core.effects import READ_ONLY_KINDS, EffectIntent, EffectKind
from omnigent_factory.core.events import (
    AdoptionResult,
    CreateRejected,
    EffectReconciled,
    Event,
    EventKind,
    MessageAck,
    PublicationAcked,
    SessionCreated,
)
from omnigent_factory.core.predicates import RUNNING_LIFECYCLES
from omnigent_factory.core.reducer import TransitionResult, transition
from omnigent_factory.core.types import (
    FENCE_BITS,
    AdmissionSnapshot,
    IssueSnapshot,
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

#: Decoded parcel aggregates kept by :meth:`SqliteStore.load_parcel` (cleared when full).
_PARCEL_CACHE_LIMIT = 1024

Reducer = Callable[[State, Event], TransitionResult]

_RUNNING_LIFECYCLES_JSON = json.dumps(sorted(x.value for x in RUNNING_LIFECYCLES))


@dataclass(frozen=True, slots=True)
class AutoTriageState:
    """Durable operator state of idle-time auto-triage (see ``auto_triage`` table)."""

    #: ``auto-triage on|off`` (None: never set, or cleared).
    enabled_override: bool | None = None
    #: The config ``auto_triage`` value when the override was set.
    override_config: bool | None = None
    #: Local day (ISO date) of ``granted``.
    grant_day: str = ""
    granted: int = 0

    def enabled(self, config_value: bool) -> bool:
        """The override while the config still says what it said then; else the config."""
        if self.enabled_override is None or self.override_config != config_value:
            return config_value
        return self.enabled_override

    def granted_on(self, day: str) -> int:
        return self.granted if self.grant_day == day else 0


FaultHook = Callable[[str], None]


#: Periodic observation kinds retention may delete, with the event-ID prefixes whose IDs
#: never recur (a clock timestamp, a sequence-stamped wake-up, ``effect:<id>:<suffix>`` for
#: a one-shot effect, or a webhook delivery GUID: delivery rows are never deleted, so a
#: redelivery stays a duplicate at the inbox). ``ActiveTimeSample`` is excluded: its ID
#: is a usage counter.
PRUNABLE_OBSERVATIONS: dict[str, tuple[str, ...]] = {
    EventKind.CHECKS_CHANGED.value: ("github:workflow_run:", "github:check_suite:"),
    EventKind.BASE_PUSHED.value: ("github:push:",),
    EventKind.RECONCILE_DUE.value: ("reconcile:",),
    EventKind.GITHUB_SNAPSHOT.value: ("effect:", "startup-github:"),
    EventKind.READINESS_EVIDENCE.value: ("effect:",),
    EventKind.TREE_QUIESCENT.value: ("effect:", "tree:"),
    EventKind.COST_SAMPLE.value: ("cost:",),
    EventKind.RUNTIME_ACTIVITY.value: ("runtime:",),
    EventKind.CAPACITY_AVAILABLE.value: ("capacity:",),
}
_PRUNABLE_KINDS_JSON = json.dumps(sorted(PRUNABLE_OBSERVATIONS))
#: Events whose delivery body nothing reads back (the directory reads owner comments,
#: decision answers and issue labels; check-run and pull-request payloads carry none).
_BODY_UNREAD_KINDS_JSON = json.dumps(
    sorted(
        {
            EventKind.CHECKS_CHANGED.value,
            EventKind.PR_OBSERVED.value,
            EventKind.REVIEW_CHANGED.value,
        }
    )
)
#: Issue-read event IDs (reconcile reads, boot reads) whose unchanged reads are not
#: stored: the IDs never recur, so no duplicate check depends on them.
UNSTORED_SNAPSHOT_PREFIXES = ("effect:", "startup-github:")
_SETTLED_EFFECT_STATES = frozenset({"done", "cancelled", "failed"})
#: Effects pruned with their observation event: reads and board drift corrections, which
#: nothing (operator retry/re-render, recovery) looks up once settled.
_PRUNABLE_EFFECT_KINDS = frozenset(
    {
        EffectKind.RECONCILE_PARCEL.value,
        EffectKind.RECONCILE_SESSION.value,
        EffectKind.FETCH_PR_EVIDENCE.value,
        EffectKind.SCAN_TREE.value,
        EffectKind.SET_BOT.value,
        EffectKind.SET_NOTE.value,
        EffectKind.SET_AUTO_BUILD.value,
    }
)
_AUTO_VACUUM_INCREMENTAL = 2


@dataclass(frozen=True, slots=True)
class PruneBatch:
    """One :meth:`SqliteStore.prune_observations` step (counts deleted, or would be)."""

    events: int
    effects: int
    audit: int
    last_sequence: int
    done: bool


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
class McpReceipt:
    """An accepted factory tool mutation, committed with its reducer event."""

    receipt_key: str
    run_id: str
    tool: str
    request_sha256: str
    receipt_json: str
    event_id: str = ""


@dataclass(frozen=True, slots=True)
class ApplyResult:
    duplicate: bool
    sequence: int | None
    accepted: bool
    reason: str
    effects: tuple[EffectIntent, ...]
    parcel: Parcel | None
    admission: AdmissionSnapshot
    #: False for a periodic issue read identical to the newest stored one (nothing was
    #: written: see :meth:`SqliteStore._unchanged_snapshot`).
    persisted: bool = True


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


def _persisted(parcel: Parcel) -> Parcel:
    """The aggregate as stored: without the reducer's in-memory duplicate set.

    ``apply_event`` refuses a known ``events.logical_key`` before the reducer runs, so
    persisting ``applied_event_ids`` (one entry per event ever applied) only grew every
    aggregate write without bound.
    """
    if not parcel.applied_event_ids:
        return parcel
    return replace(parcel, applied_event_ids=frozenset())


def snapshot_key(snap: IssueSnapshot) -> tuple[object, ...]:
    """An issue read's values without its read time (body as its sha256)."""
    body = (
        hashlib.sha256(snap.body.encode("utf-8")).hexdigest()
        if snap.body is not None
        else snap.body_sha256
    )
    return (
        snap.open,
        snap.human_assigned,
        snap.repo_matches,
        snap.identity_resolved,
        snap.in_project,
        snap.stage,
        snap.title,
        body,
        snap.bot,
        snap.note,
        snap.links,
    )


def _digest(text: str | None) -> str | None:
    if text is None:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _resolution(event: Event) -> tuple[str, bool, str] | None:
    """``(effect_id, delivered, item_id)`` when ``event`` resolves an ambiguous write."""
    body = event.body
    if isinstance(body, EffectReconciled) and body.effect_id:
        return body.effect_id, body.delivered, body.item_id
    if isinstance(body, MessageAck) and body.effect_id:
        return body.effect_id, True, body.item_id
    if isinstance(body, PublicationAcked) and body.effect_id:
        return body.effect_id, True, body.comment_id
    return None


def _create_resolution(event: Event) -> tuple[str, bool, str] | None:
    """``(session_id, exists, root_id)`` when ``event`` settles whether a session exists.

    A create whose write timed out is resolved by a separate event (the adoption search's
    ``AdoptionResult``, or a later ``SessionCreated`` / ``CreateRejected``), never by the
    create effect's own outcome. Zero or several adoption matches prove nothing.
    """
    body = event.body
    if isinstance(body, SessionCreated) and body.session_id and body.root_id:
        return body.session_id, True, body.root_id
    if isinstance(body, AdoptionResult) and body.session_id:
        if body.matches == 1 and body.root_id:
            return body.session_id, True, body.root_id
        return None
    if isinstance(body, CreateRejected) and body.session_id:
        return body.session_id, False, ""
    return None


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
        self._parcel_cache: dict[str, tuple[tuple[int, int], str, Parcel]] = {}
        #: Aggregates decoded by :meth:`load_parcel` (a cache miss; for measurements).
        self.parcel_decodes = 0

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
            # Only takes effect on a new, empty file; an existing one switches in vacuum().
            conn.execute(f"PRAGMA auto_vacuum={_AUTO_VACUUM_INCREMENTAL}")
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
        # ``auto`` is derived, never written back: the entry the parcel's auto-build mark
        # started (same approval and queue sequence; a later rework is no auto-build).
        queue = tuple(
            QueueEntry(
                r["parcel_id"],
                r["approval_id"],
                r["approval_sequence"],
                QueueStatus(r["status"]),
                auto=bool(r["auto"]),
                resume=bool(r["resume"]),
                issue_number=r["issue_number"],
            )
            for r in conn.execute(
                "SELECT q.*, COALESCE(("
                "json_extract(p.aggregate_json, '$.parcel.auto_build.approval_id') "
                "= q.approval_id AND "
                "json_extract(p.aggregate_json, '$.parcel.auto_build.sequence') "
                "= q.approval_sequence), 0) AS auto, "
                "COALESCE(json_extract(p.aggregate_json, '$.parcel.slot_parked'), 0) AS resume, "
                "p.issue_number AS issue_number "
                "FROM queue q LEFT JOIN parcels p ON p.parcel_id = q.parcel_id "
                "WHERE q.repo_id = ? ORDER BY q.approval_sequence",
                (repo_id,),
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
        # Derived, never written back: the rule of ``predicates.holds_triage_slot``.
        triage_runs = frozenset(
            str(r[0])
            for r in conn.execute(
                "SELECT DISTINCT s.parcel_id FROM stage_sessions s "
                "JOIN parcels p ON p.parcel_id = s.parcel_id "
                "WHERE p.repo_id = ? AND s.kind = 'triage' "
                "AND s.lifecycle IN (SELECT value FROM json_each(?))",
                (repo_id, _RUNNING_LIFECYCLES_JSON),
            )
        )
        return AdmissionSnapshot(
            repo_id=repo_id,
            paused=bool(row["paused"]),
            next_sequence=int(row["next_sequence"]),
            queue=queue,
            reservations=reservations,
            open_bot_prs=frozenset(int(x) for x in json.loads(row["open_bot_prs_json"])),
            triage_runs=triage_runs,
        )

    # ---------------------------------------------------------- auto-triage / build

    #: Tables holding an operator switch + daily grant (same shape; see migrations).
    _SWITCH_TABLES = frozenset({"auto_triage", "auto_build"})

    def auto_triage_state(self, repo_id: str) -> AutoTriageState:
        return self._switch_state("auto_triage", repo_id)

    def set_auto_triage_override(self, repo_id: str, enabled: bool, config_value: bool) -> None:
        """The operator turned auto-triage on/off, against the current config value."""
        self._set_switch_override("auto_triage", repo_id, enabled, config_value)

    def clear_auto_triage_override(self, repo_id: str) -> None:
        self._clear_switch_override("auto_triage", repo_id)

    def grant_auto_triage(self, repo_id: str, day: str, count: int) -> int:
        """Add ``count`` auto-triages to ``day``'s budget; returns that day's total grant."""
        return self._grant_switch("auto_triage", repo_id, day, count)

    def auto_build_state(self, repo_id: str) -> AutoTriageState:
        """Operator state of auto-build (same shape and rules as auto-triage's)."""
        return self._switch_state("auto_build", repo_id)

    def set_auto_build_override(self, repo_id: str, enabled: bool, config_value: bool) -> None:
        self._set_switch_override("auto_build", repo_id, enabled, config_value)

    def grant_auto_build(self, repo_id: str, day: str, count: int) -> int:
        return self._grant_switch("auto_build", repo_id, day, count)

    def _switch_table(self, table: str) -> str:
        if table not in self._SWITCH_TABLES:
            raise StoreError(f"unknown switch table {table}")
        return table

    def _switch_state(self, table: str, repo_id: str) -> AutoTriageState:
        row = self._conn.execute(
            "SELECT enabled_override, override_config, grant_day, granted FROM "
            f"{self._switch_table(table)} WHERE repo_id = ?",
            (repo_id,),
        ).fetchone()
        if row is None:
            return AutoTriageState()
        return AutoTriageState(
            enabled_override=None if row[0] is None else bool(row[0]),
            override_config=None if row[1] is None else bool(row[1]),
            grant_day=str(row[2] or ""),
            granted=int(row[3]),
        )

    def _set_switch_override(
        self, table: str, repo_id: str, enabled: bool, config_value: bool
    ) -> None:
        with self._txn() as conn:
            conn.execute(
                f"INSERT INTO {self._switch_table(table)} (repo_id, enabled_override, "
                "override_config, granted, updated_at_us) VALUES (?, ?, ?, 0, ?) "
                "ON CONFLICT(repo_id) DO UPDATE SET "
                "enabled_override = excluded.enabled_override, "
                "override_config = excluded.override_config, "
                "updated_at_us = excluded.updated_at_us",
                (repo_id, int(enabled), int(config_value), self._clock.now_utc_us()),
            )

    def _clear_switch_override(self, table: str, repo_id: str) -> None:
        with self._txn() as conn:
            conn.execute(
                f"UPDATE {self._switch_table(table)} SET enabled_override = NULL, "
                "override_config = NULL, updated_at_us = ? WHERE repo_id = ?",
                (self._clock.now_utc_us(), repo_id),
            )

    def _grant_switch(self, table: str, repo_id: str, day: str, count: int) -> int:
        name = self._switch_table(table)
        with self._txn() as conn:
            conn.execute(
                f"INSERT INTO {name} (repo_id, grant_day, granted, updated_at_us) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(repo_id) DO UPDATE SET "
                "granted = CASE WHEN grant_day IS excluded.grant_day "
                "THEN granted + excluded.granted ELSE excluded.granted END, "
                "grant_day = excluded.grant_day, updated_at_us = excluded.updated_at_us",
                (repo_id, day, count, self._clock.now_utc_us()),
            )
            row = conn.execute(
                f"SELECT granted FROM {name} WHERE repo_id = ?", (repo_id,)
            ).fetchone()
        return int(row[0])

    def count_accepted_events(
        self, repo_id: str, kind: EventKind, since_us: int, until_us: int
    ) -> int:
        """Accepted events of ``kind`` whose source time is in ``[since_us, until_us)``."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM events WHERE kind = ? AND source_time_us >= ? "
            "AND source_time_us < ? AND repo_id = ? AND accepted = 1",
            (kind.value, since_us, until_us, repo_id),
        ).fetchone()
        return int(row[0])

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
        # ``status IN ('pending', 'unresolved')`` must match ix_deliveries_inbox exactly.
        rows = self._conn.execute(
            "SELECT delivery_guid, event_name, body, headers_json, provenance, action, app_id, "
            "installation_id, source_time_us FROM deliveries "
            "WHERE status IN ('pending', 'unresolved') "
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

    def has_pending_delivery(self) -> bool:
        """Whether any delivery is ``pending`` (an indexed probe, never a table scan)."""
        row = self._conn.execute(
            "SELECT 1 FROM deliveries WHERE status IN ('pending', 'unresolved') "
            "AND status = 'pending' LIMIT 1"
        ).fetchone()
        return row is not None

    def inbox_due(self) -> list[tuple[str, int | None]]:
        """``(delivery_guid, resolution_retry_at_us)`` of every pending/unresolved row."""
        rows = self._conn.execute(
            "SELECT delivery_guid, resolution_retry_at_us FROM deliveries "
            "WHERE status IN ('pending', 'unresolved')"
        ).fetchall()
        return [(str(r[0]), r[1]) for r in rows]

    def prune_delivery_bodies(
        self, before_us: int, *, limit: int = 500, dry_run: bool = False
    ) -> int:
        """Empty the body and headers of old processed deliveries nothing reads back.

        Only ``processed`` rows finished before ``before_us`` are touched, and never one an
        event outside :data:`_BODY_UNREAD_KINDS_JSON` points at: the directory reads those
        bodies back as parcel context (owner comments, decision answers, labels). The row,
        GUID and ``body_sha256`` stay, so duplicate and recovery matching keep working.
        Returns the rows pruned (at most ``limit``, one short transaction; call again until
        it returns fewer). ``dry_run`` counts every eligible row instead and changes nothing.
        """
        eligible = (
            "SELECT d.delivery_guid FROM deliveries d "
            "WHERE d.status = 'processed' AND d.body_pruned_at_us IS NULL "
            "AND COALESCE(d.processed_at_us, d.received_at_us) < ? "
            "AND NOT EXISTS (SELECT 1 FROM events e WHERE e.delivery_guid = d.delivery_guid "
            "AND e.kind NOT IN (SELECT value FROM json_each(?)))"
        )
        checks = _BODY_UNREAD_KINDS_JSON
        if dry_run:
            row = self._conn.execute(
                f"SELECT COUNT(*) FROM ({eligible})", (before_us, checks)
            ).fetchone()
            return int(row[0])
        with self._txn() as conn:
            cur = conn.execute(
                "UPDATE deliveries SET body = X'', headers_json = '{}', body_pruned_at_us = ? "
                f"WHERE delivery_guid IN ({eligible} LIMIT ?)",
                (self._clock.now_utc_us(), before_us, checks, limit),
            )
        return cur.rowcount

    def retire_delivery(self, delivery_guid: str) -> None:
        """Mark a delivery that produced no event ``processed`` and drop its body at once.

        Nothing reads such a body back (no event points at it). The row keeps its GUID and
        ``body_sha256``, so duplicate and recovery matching still work. A delivery an event
        references (e.g. a re-processed one) keeps its body for the normal retention.
        """
        with self._txn() as conn:
            self._mark_delivery(conn, delivery_guid, "processed")
            conn.execute(
                "UPDATE deliveries SET body = X'', headers_json = '{}', body_pruned_at_us = ? "
                "WHERE delivery_guid = ? AND body_pruned_at_us IS NULL "
                "AND NOT EXISTS (SELECT 1 FROM events e WHERE e.delivery_guid = ?) "
                "AND NOT EXISTS (SELECT 1 FROM parked_deliveries p WHERE p.delivery_guid = ?)",
                (self._clock.now_utc_us(), delivery_guid, delivery_guid, delivery_guid),
            )

    def prune_delivery_rows(
        self,
        before_us: int,
        held: Sequence[str] = (),
        *,
        limit: int = 500,
        dry_run: bool = False,
    ) -> tuple[int, int]:
        """Delete fully pruned deliveries and old delivery attempts (one short transaction).

        A delivery row goes only when it is ``processed``, its body was pruned, it finished
        before ``before_us``, and no event, parked delivery, parcel hold (``held`` GUIDs)
        or stored review comment references it. An attempt goes when it is older than
        ``before_us``, is not a quarantined copy, and its delivery row is gone or fully
        pruned and not parked. Returns (deliveries, attempts), each at most ``limit``.
        """
        rows = (
            "SELECT d.delivery_guid FROM deliveries d "
            "WHERE d.status = 'processed' AND d.body_pruned_at_us IS NOT NULL "
            "AND COALESCE(d.processed_at_us, d.received_at_us) < :before "
            "AND NOT EXISTS (SELECT 1 FROM events e WHERE e.delivery_guid = d.delivery_guid) "
            "AND NOT EXISTS (SELECT 1 FROM parked_deliveries p "
            "WHERE p.delivery_guid = d.delivery_guid) "
            "AND NOT EXISTS (SELECT 1 FROM pr_review_comments r "
            "WHERE r.delivery_guid = d.delivery_guid) "
            "AND d.delivery_guid NOT IN (SELECT value FROM json_each(:held)) LIMIT :limit"
        )
        attempts = (
            "SELECT a.id FROM delivery_attempts a WHERE a.received_at_us < :before "
            "AND a.outcome != 'quarantined' "
            "AND NOT EXISTS (SELECT 1 FROM parked_deliveries p "
            "WHERE p.delivery_guid = a.delivery_guid) "
            "AND a.delivery_guid NOT IN (SELECT value FROM json_each(:held)) "
            "AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.delivery_guid = a.delivery_guid "
            "AND (d.status != 'processed' OR d.body_pruned_at_us IS NULL)) LIMIT :limit"
        )
        params = {"before": before_us, "held": codec.dumps(list(held)), "limit": limit}
        if dry_run:
            count_rows = self._conn.execute(f"SELECT COUNT(*) FROM ({rows})", params)
            count_attempts = self._conn.execute(f"SELECT COUNT(*) FROM ({attempts})", params)
            return int(count_rows.fetchone()[0]), int(count_attempts.fetchone()[0])
        with self._txn() as conn:
            deleted = conn.execute(
                f"DELETE FROM deliveries WHERE delivery_guid IN ({rows})", params
            ).rowcount
            removed = conn.execute(
                f"DELETE FROM delivery_attempts WHERE id IN ({attempts})", params
            ).rowcount
        return deleted, removed

    # ------------------------------------------------------------ board diff

    def board_digests(self) -> dict[str, str]:
        rows = self._conn.execute("SELECT parcel_id, digest FROM board_digests").fetchall()
        return {str(r[0]): str(r[1]) for r in rows}

    def save_board_digests(self, digests: Mapping[str, str]) -> None:
        if not digests:
            return
        now = self._clock.now_utc_us()
        with self._txn() as conn:
            conn.executemany(
                "INSERT INTO board_digests (parcel_id, digest, seen_at_us) VALUES (?, ?, ?) "
                "ON CONFLICT (parcel_id) DO UPDATE SET digest = excluded.digest, "
                "seen_at_us = excluded.seen_at_us",
                [(pid, digest, now) for pid, digest in sorted(digests.items())],
            )

    def prune_observations(
        self,
        before_us: int,
        *,
        after_sequence: int = 0,
        limit: int = 500,
        dry_run: bool = False,
    ) -> PruneBatch:
        """Delete old periodic observation events with their settled effects and audit rows.

        Scans up to ``limit`` events after ``after_sequence`` (keyset: pass the returned
        ``last_sequence`` back until ``done``) that were applied before ``before_us`` and
        whose kind and event ID are in :data:`PRUNABLE_OBSERVATIONS`: IDs that never recur
        (a timestamp or a one-shot effect ack), so losing their duplicate check is safe. A
        candidate is kept when it is the parcel's newest event of its kind (read back as
        current evidence), another table references it, it acknowledges an effect that is
        not settled (startup recovery looks the ack up), or it spawned an effect that is
        unsettled, not a read or board drift correction, carries a semantic dedupe key, or
        is referenced by an idempotency row.
        Aggregates never replay events, so nothing else depends on them. One short
        transaction per call; ``dry_run`` reports the same counts and deletes nothing.
        """
        rows = self._conn.execute(
            "SELECT event_id, parcel_id, kind, sequence FROM events "
            "WHERE sequence > ? AND kind IN (SELECT value FROM json_each(?)) "
            "AND applied_at_us < ? ORDER BY sequence LIMIT ?",
            (after_sequence, _PRUNABLE_KINDS_JSON, before_us, limit),
        ).fetchall()
        if not rows:
            return PruneBatch(0, 0, 0, after_sequence, done=True)
        last = int(rows[-1]["sequence"])
        unsettled = {
            str(r[0])
            for r in self._conn.execute(
                "SELECT effect_id FROM effects WHERE state IN ('pending', 'claimed', 'unknown')"
            )
        }
        newest: dict[tuple[str | None, str], int] = {}
        candidates: list[str] = []
        for r in rows:
            event_id, kind = str(r["event_id"]), str(r["kind"])
            if not event_id.startswith(PRUNABLE_OBSERVATIONS[kind]):
                continue
            if event_id.startswith("effect:") and event_id[7:].rpartition(":")[0] in unsettled:
                continue
            key = (r["parcel_id"], kind)
            if key not in newest:
                top = self._conn.execute(
                    "SELECT MAX(sequence) FROM events WHERE parcel_id IS ? AND kind = ?", key
                ).fetchone()
                newest[key] = int(top[0])
            if int(r["sequence"]) < newest[key]:
                candidates.append(event_id)
        ids = codec.dumps(candidates)
        keep = {
            str(r[0])
            for r in self._conn.execute(
                "SELECT source_event_id FROM stage_authorizations "
                "WHERE source_event_id IN (SELECT value FROM json_each(:ids)) "
                "UNION SELECT cause_event_id FROM fences "
                "WHERE cause_event_id IN (SELECT value FROM json_each(:ids)) "
                "UNION SELECT cleared_by_event_id FROM fences "
                "WHERE cleared_by_event_id IN (SELECT value FROM json_each(:ids)) "
                "UNION SELECT source_event_id FROM approvals "
                "WHERE source_event_id IN (SELECT value FROM json_each(:ids)) "
                "UNION SELECT answer_event_id FROM decisions "
                "WHERE answer_event_id IN (SELECT value FROM json_each(:ids))",
                {"ids": ids},
            )
        }
        effects: dict[str, list[str]] = {}
        for r in self._conn.execute(
            "SELECT f.effect_id, f.event_id, f.kind, f.state, f.dedupe_key, "
            "EXISTS (SELECT 1 FROM dispatch_intents i WHERE i.effect_id = f.effect_id) "
            "OR EXISTS (SELECT 1 FROM own_items o WHERE o.effect_id = f.effect_id) "
            "OR EXISTS (SELECT 1 FROM own_sends s WHERE s.effect_id = f.effect_id) AS held "
            "FROM effects f WHERE f.event_id IN (SELECT value FROM json_each(?))",
            (ids,),
        ):
            event_id = str(r["event_id"])
            if (
                r["state"] not in _SETTLED_EFFECT_STATES
                or r["kind"] not in _PRUNABLE_EFFECT_KINDS
                or r["dedupe_key"] != r["effect_id"]
                or r["held"]
            ):
                keep.add(event_id)
            effects.setdefault(event_id, []).append(str(r["effect_id"]))
        doomed = [e for e in candidates if e not in keep]
        doomed_effects = [f for e in doomed for f in effects.get(e, ())]
        audit = 0
        if doomed and not dry_run:
            doomed_json = codec.dumps(doomed)
            with self._txn() as conn:
                conn.execute(
                    "DELETE FROM effects WHERE effect_id IN (SELECT value FROM json_each(?))",
                    (codec.dumps(doomed_effects),),
                )
                audit = conn.execute(
                    "DELETE FROM audit WHERE event_id IN (SELECT value FROM json_each(?))",
                    (doomed_json,),
                ).rowcount
                conn.execute(
                    "DELETE FROM events WHERE event_id IN (SELECT value FROM json_each(?))",
                    (doomed_json,),
                )
        elif doomed:
            audit = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM audit WHERE event_id IN (SELECT value FROM json_each(?))",
                    (codec.dumps(doomed),),
                ).fetchone()[0]
            )
        return PruneBatch(len(doomed), len(doomed_effects), audit, last, done=len(rows) < limit)

    def incremental_vacuum(self, pages: int) -> int:
        """Return up to ``pages`` free pages to the filesystem (incremental auto_vacuum only).

        Returns the free pages left. A no-op (0 work) unless :meth:`vacuum` enabled
        ``auto_vacuum=INCREMENTAL``.
        """
        if int(self._conn.execute("PRAGMA auto_vacuum").fetchone()[0]) == _AUTO_VACUUM_INCREMENTAL:
            self._conn.execute(f"PRAGMA incremental_vacuum({int(pages)})").fetchall()
        return int(self._conn.execute("PRAGMA freelist_count").fetchone()[0])

    def vacuum(self) -> None:
        """Rebuild the file compactly and switch it to incremental auto_vacuum.

        Takes the write lock for the whole rebuild: run it only with the daemon stopped.
        Afterwards the periodic sweep returns freed pages with :meth:`incremental_vacuum`.
        """
        self._conn.execute(f"PRAGMA auto_vacuum={_AUTO_VACUUM_INCREMENTAL}")
        self._conn.execute("VACUUM")
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        self._parcel_cache.clear()

    def storage_stats(self) -> dict[str, int]:
        """Page accounting of the main database file (for operator reports)."""

        def pragma(name: str) -> int:
            return int(self._conn.execute(f"PRAGMA {name}").fetchone()[0])

        page_size = pragma("page_size")
        return {
            "bytes": pragma("page_count") * page_size,
            "free_bytes": pragma("freelist_count") * page_size,
            "auto_vacuum": pragma("auto_vacuum"),
        }

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
        # Background loops reload every parcel each second; read and decode only what may
        # have changed. While no row changed through this connection (total_changes) or
        # another one (data_version), the cached aggregate is current; otherwise the
        # stored text is compared, so no write path can leave the cache stale. Parcel
        # aggregates are frozen dataclasses, safe to share between callers.
        epoch = (
            self._conn.total_changes,
            int(self._conn.execute("PRAGMA data_version").fetchone()[0]),
        )
        cached = self._parcel_cache.get(parcel_id)
        if cached is not None and cached[0] == epoch:
            return cached[2]
        row = self._conn.execute(
            "SELECT aggregate_json FROM parcels WHERE parcel_id = ?", (parcel_id,)
        ).fetchone()
        if row is None:
            self._parcel_cache.pop(parcel_id, None)
            return None
        text = row["aggregate_json"]
        if cached is not None and cached[1] == text:
            parcel = cached[2]
        else:
            parcel = codec.parcel_from_json(text)
            self.parcel_decodes += 1
        if len(self._parcel_cache) >= _PARCEL_CACHE_LIMIT:
            self._parcel_cache.clear()
        self._parcel_cache[parcel_id] = (epoch, text, parcel)
        return parcel

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
        receipt: Callable[[Parcel], McpReceipt] | None = None,
    ) -> ApplyResult:
        """Run the reducer and commit everything it produced in one transaction.

        ``event.entropy`` is filled with a fresh random value when empty, then persisted.
        When ``event.delivery_guid`` is set, that delivery is marked ``delivery_status``
        (default ``processed``) in the same transaction. ``receipt`` renders a factory
        tool receipt from the accepted post-state; it is stored in the same transaction,
        and only when the reducer accepts the event.
        """
        with self._txn() as conn:
            result = self._apply_in_txn(conn, event, config, reducer, delivery_status)
            if (
                receipt is not None
                and result.accepted
                and not result.duplicate
                and result.parcel is not None
            ):
                row = receipt(result.parcel)
                conn.execute(
                    "INSERT INTO mcp_receipts (receipt_key, parcel_id, run_id, tool, event_id, "
                    "request_sha256, receipt_json, created_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        row.receipt_key,
                        event.parcel_id,
                        row.run_id,
                        row.tool,
                        event.event_id,
                        row.request_sha256,
                        row.receipt_json,
                        self._clock.now_utc_us(),
                    ),
                )
            return result

    def mcp_receipt(self, receipt_key: str) -> McpReceipt | None:
        row = self._conn.execute(
            "SELECT receipt_key, run_id, tool, request_sha256, receipt_json, event_id "
            "FROM mcp_receipts WHERE receipt_key = ?",
            (receipt_key,),
        ).fetchone()
        if row is None:
            return None
        return McpReceipt(
            receipt_key=str(row[0]),
            run_id=str(row[1]),
            tool=str(row[2]),
            request_sha256=str(row[3]),
            receipt_json=str(row[4]),
            event_id=str(row[5]),
        )

    def record_plan_read(self, run_id: str, plan_hash: str) -> None:
        with self._txn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO mcp_plan_reads (run_id, plan_hash, read_at_us) "
                "VALUES (?, ?, ?)",
                (run_id, plan_hash, self._clock.now_utc_us()),
            )

    def plan_read(self, run_id: str, plan_hash: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM mcp_plan_reads WHERE run_id = ? AND plan_hash = ?",
            (run_id, plan_hash),
        ).fetchone()
        return row is not None

    def record_feedback_read(self, run_id: str, sequence: int) -> None:
        """``run_id`` has read every owner comment up to event ``sequence``."""
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO mcp_feedback_reads (run_id, sequence, read_at_us) VALUES (?, ?, ?) "
                "ON CONFLICT (run_id) DO UPDATE SET sequence = max(sequence, excluded.sequence), "
                "read_at_us = excluded.read_at_us",
                (run_id, sequence, self._clock.now_utc_us()),
            )

    def record_review_comments(self, delivery_guid: str, comments_json: str) -> None:
        """The inline comments of the owner review carried by ``delivery_guid``."""
        with self._txn() as conn:
            conn.execute(
                "INSERT INTO pr_review_comments (delivery_guid, comments_json, fetched_at_us) "
                "VALUES (?, ?, ?) ON CONFLICT (delivery_guid) DO UPDATE SET "
                "comments_json = excluded.comments_json, fetched_at_us = excluded.fetched_at_us",
                (delivery_guid, comments_json, self._clock.now_utc_us()),
            )

    def feedback_read(self, run_id: str) -> int:
        """The newest owner-comment event sequence ``run_id`` has read (-1: none)."""
        row = self._conn.execute(
            "SELECT sequence FROM mcp_feedback_reads WHERE run_id = ?", (run_id,)
        ).fetchone()
        return int(row[0]) if row is not None else -1

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
        if new.parcel is not None:
            new = replace(new, parcel=_persisted(new.parcel))
        if self._unchanged_snapshot(
            conn, event, old=old_parcel, new=new, admission=admission, result=result
        ):
            LOG.debug("issue read unchanged, not stored parcel=%s", event.parcel_id)
            return ApplyResult(
                duplicate=False,
                sequence=None,
                accepted=True,
                reason=result.audit.reason,
                effects=(),
                parcel=old_parcel,
                admission=admission,
                persisted=False,
            )
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
        if event.evidence is not None and new.parcel is not None:
            self._supersede_evidence(conn, new.parcel.parcel_id, sequence)
        if result.audit.accepted:
            resolution = _resolution(event)
            if resolution is not None:
                self._close_unknown(conn, *resolution)
            created = _create_resolution(event)
            if created is not None and event.parcel_id is not None:
                self._close_unknown_create(conn, event.parcel_id, *created)
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
                _digest(codec.parcel_to_json(_persisted(old_parcel))) if old_parcel else None,
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
    def _unchanged_snapshot(
        conn: sqlite3.Connection,
        event: Event,
        *,
        old: Parcel | None,
        new: State,
        admission: AdmissionSnapshot,
        result: TransitionResult,
    ) -> bool:
        """A periodic issue read that changes nothing is not stored.

        Only reconcile and startup reads (their event IDs never recur, so no duplicate
        check depends on them): accepted, no effects, the same aggregate and admission,
        and the same issue values (read time aside) as the parcel's newest stored read,
        which stays the current evidence (``issue_evidence`` reads it back).
        """
        snap = event.evidence
        if (
            event.kind != EventKind.GITHUB_SNAPSHOT
            or snap is None
            or old is None
            or new.parcel is None
            or not event.event_id.startswith(UNSTORED_SNAPSHOT_PREFIXES)
            or not result.audit.accepted
            or result.effects
            or new.admission != admission
            or replace(new.parcel, version=old.version) != _persisted(old)
        ):
            return False
        row = conn.execute(
            "SELECT payload_json FROM events WHERE parcel_id = ? AND kind = ? "
            "ORDER BY sequence DESC LIMIT 1",
            (old.parcel_id, EventKind.GITHUB_SNAPSHOT.value),
        ).fetchone()
        if row is None:
            return False
        stored = codec.event_from_json(row["payload_json"]).evidence
        return stored is not None and snapshot_key(stored) == snapshot_key(snap)

    @staticmethod
    def _supersede_evidence(conn: sqlite3.Connection, parcel_id: str, sequence: int) -> None:
        """Replace the issue body of the parcel's previous issue read with its sha256.

        Only the newest read's text is read back (``issue_evidence``: the issue as the
        agent sees it); an older read keeps its values and a body hash for change
        detection and diagnostics.
        """
        row = conn.execute(
            "SELECT event_id, payload_json FROM events WHERE parcel_id = ? AND sequence < ? "
            "AND payload_json LIKE '%\"evidence\":{%' ORDER BY sequence DESC LIMIT 1",
            (parcel_id, sequence),
        ).fetchone()
        if row is None:
            return
        data = json.loads(row["payload_json"])
        evidence = data.get("evidence")
        body = evidence.get("body") if isinstance(evidence, dict) else None
        if not isinstance(body, str):
            return
        evidence["body"] = None
        evidence["body_sha256"] = hashlib.sha256(body.encode("utf-8")).hexdigest()
        conn.execute(
            "UPDATE events SET payload_json = ? WHERE event_id = ?",
            (codec.dumps(data), row["event_id"]),
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
                codec.parcel_to_json(_persisted(p)),
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
                "authorization_id, issue_root_id, nonce, lifecycle, execution_closed, "
                "fence_mask, revision, restart_count, correction_count) VALUES (?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET "
                "issue_root_id = excluded.issue_root_id, lifecycle = excluded.lifecycle, "
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
            if applied is not None and not applied.persisted:
                # An unchanged periodic read leaves no trace: its settled read effect goes
                # too (nothing looks a settled read up; retention would delete it later).
                conn.execute(
                    "DELETE FROM effects WHERE effect_id = ? AND kind = ?",
                    (effect_id, EffectKind.RECONCILE_PARCEL.value),
                )
                return EffectOutcomeResult(True, applied)
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

    def _close_unknown(
        self, conn: sqlite3.Connection, effect_id: str, delivered: bool, item_id: str
    ) -> bool:
        """Close an ``unknown`` row the reducer resolved by a later, separate event.

        Only ``unknown`` rows move, and only to ``done`` or ``failed``: never ``pending``,
        so a resolution can never cause the original write to be sent again.
        """
        if delivered:
            return self._update_effect(
                conn,
                effect_id,
                "done",
                from_states=("unknown",),
                remote_id=item_id or None,
                reason="reconciled: delivered",
            )
        return self._update_effect(
            conn,
            effect_id,
            "failed",
            from_states=("unknown",),
            remote_id=None,
            reason="reconciled: not delivered",
        )

    def _close_unknown_create(
        self, conn: sqlite3.Connection, parcel_id: str, session_id: str, exists: bool, root: str
    ) -> bool:
        """Close the ``unknown`` create of ``session_id`` once adoption settled it.

        ``done`` (with the adopted root) when the session exists, ``failed`` when it
        provably does not; never ``pending``, so the create is never sent again.
        """
        row = conn.execute(
            "SELECT effect_id FROM effects WHERE parcel_id = ? AND kind = ? AND target = ? "
            "AND state = 'unknown'",
            (parcel_id, EffectKind.CREATE_SESSION.value, session_id),
        ).fetchone()
        if row is None:
            return False
        return self._close_unknown(conn, str(row["effect_id"]), exists, root)

    def close_reconciled_unknown(self, effect_id: str) -> bool:
        """Startup: close an ``unknown`` row whose resolution event is already persisted.

        Repairs rows left ``unknown`` before resolutions closed them in the same
        transaction. Reads accepted ``EffectReconciled`` / ``MessageAck`` /
        ``PublicationAcked`` events for the effect, and for a ``create_session`` the
        accepted ``SessionCreated`` / single-match ``AdoptionResult`` / ``CreateRejected``
        of its session.
        """
        row = self._conn.execute(
            "SELECT kind, payload_json FROM events WHERE accepted = 1 AND kind IN (?, ?, ?) "
            "AND json_extract(payload_json, '$.body.effect_id') = ? "
            "ORDER BY sequence DESC LIMIT 1",
            (
                EventKind.EFFECT_RECONCILED.value,
                EventKind.MESSAGE_ACK.value,
                EventKind.PUBLICATION_ACKED.value,
                effect_id,
            ),
        ).fetchone()
        if row is None:
            return self._close_adopted_create(effect_id)
        resolution = _resolution(codec.event_from_json(row["payload_json"]))
        if resolution is None:
            return False
        with self._txn() as conn:
            return self._close_unknown(conn, *resolution)

    def _close_adopted_create(self, effect_id: str) -> bool:
        effect = self._conn.execute(
            "SELECT parcel_id, target FROM effects WHERE effect_id = ? AND kind = ? "
            "AND state = 'unknown'",
            (effect_id, EffectKind.CREATE_SESSION.value),
        ).fetchone()
        if effect is None or effect["parcel_id"] is None:
            return False
        parcel_id, session_id = str(effect["parcel_id"]), str(effect["target"])
        for (payload,) in self._conn.execute(
            "SELECT payload_json FROM events WHERE accepted = 1 AND parcel_id = ? "
            "AND kind IN (?, ?, ?) AND json_extract(payload_json, '$.body.session_id') = ? "
            "ORDER BY sequence DESC",
            (
                parcel_id,
                EventKind.SESSION_CREATED.value,
                EventKind.ADOPTION_RESULT.value,
                EventKind.CREATE_REJECTED.value,
                session_id,
            ),
        ).fetchall():
            created = _create_resolution(codec.event_from_json(payload))
            if created is not None:
                with self._txn() as conn:
                    return self._close_unknown_create(conn, parcel_id, *created)
        return False

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
    # A routine sample, or an accepted event that moves nothing and writes nothing (no
    # effect beyond a read): DEBUG. Refusals and every real change stay INFO.
    quiet = (
        result.audit.accepted
        and before == after
        and (event.kind in _QUIET_KINDS or all(e.kind in READ_ONLY_KINDS for e in result.effects))
    )
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
