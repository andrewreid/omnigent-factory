"""SQLite store: migrations, reopen/rollback, inbox, outbox, leases, caps, crash windows."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event
from omnigent_factory.core.reducer import AuditRecord, TransitionResult, transition
from omnigent_factory.core.types import (
    MICROS_PER_HOUR,
    DecisionStatus,
    FenceKind,
    Lifecycle,
    QueueStatus,
    ReservationKind,
    State,
    Via,
)
from omnigent_factory.store.migrations import MIGRATIONS, V1_SQL, Migration
from omnigent_factory.store.sqlite import (
    DeliveryOutcome,
    DeliveryRecord,
    InvariantViolation,
    LeaseHeld,
    MigrationError,
    SqliteStore,
)
from omnigent_factory.testing.builders import REPO_ID, config
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
Q = "I_parcel_2"


class Crash(Exception):
    pass


def open_store(path: Path, **kw) -> SqliteStore:
    return SqliteStore.open(path, FakeClock(), **kw)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "state.sqlite3"


@dataclass
class StoreHarness(Harness):
    """Harness whose transitions go through the durable store."""

    store: SqliteStore | None = None

    def _apply_one(self, event: Event) -> TransitionResult:
        assert self.store is not None
        res = self.store.apply_event(event, self.cfg)
        if res.parcel is not None:
            self.parcels[res.parcel.parcel_id] = res.parcel
        self.admission = res.admission
        before = None
        result = TransitionResult(
            state=State(res.parcel, res.admission, self.cfg),
            effects=res.effects,
            audit=AuditRecord(
                event.event_id,
                event.kind.value,
                event.parcel_id,
                res.accepted,
                res.reason,
                before,
                res.parcel.version if res.parcel else None,
            ),
            duplicate=res.duplicate,
        )
        self.log.append((event, result))
        return result


def store_harness(store: SqliteStore, **cfg) -> StoreHarness:
    h = StoreHarness(cfg=config(**cfg), store=store)
    store.ensure_repository(h.cfg)
    h.apply(h.f(P).make(ev.Unpause(), parcel_id=None))
    return h


# ============================================================ migrations


def test_fresh_database_is_migrated_with_required_pragmas(db):
    store = open_store(db)
    assert store.schema_version() == 1
    assert store.query("PRAGMA journal_mode")[0][0] == "wal"
    assert store.query("PRAGMA foreign_keys")[0][0] == 1
    assert store.query("PRAGMA synchronous")[0][0] == 2  # FULL
    tables = {r[0] for r in store.query("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {
        "schema_migrations",
        "deliveries",
        "events",
        "repositories",
        "parcels",
        "stage_authorizations",
        "stage_sessions",
        "fences",
        "dispatch_intents",
        "contracts",
        "approvals",
        "decisions",
        "grants",
        "session_nodes",
        "activity_intervals",
        "effects",
        "own_items",
        "queue",
        "reservations",
        "pull_requests",
        "leases",
        "timers",
        "capabilities",
        "audit",
    } <= tables
    store.close()


def test_reopen_is_idempotent_and_preserves_data(db):
    store = open_store(db)
    store.ensure_repository(config())
    store.close()
    store = open_store(db)
    assert store.schema_version() == 1
    assert store.load_admission(REPO_ID).paused  # new repositories start paused
    assert len(store.query("SELECT * FROM schema_migrations")) == 1
    store.close()


def test_checksum_mismatch_refuses_to_open(db):
    open_store(db).close()
    tampered = (Migration(1, "initial-schema", V1_SQL + "\n-- edited"),)
    with pytest.raises(MigrationError, match="checksum"):
        open_store(db, migrations=tampered)


def test_database_newer_than_code_refuses(db):
    extra = (*MIGRATIONS, Migration(2, "future", "CREATE TABLE future_table (x INTEGER);"))
    open_store(db, migrations=extra).close()
    with pytest.raises(MigrationError, match="unknown migration"):
        open_store(db)


def test_failed_migration_rolls_back_atomically(db):
    open_store(db).close()
    bad = (
        *MIGRATIONS,
        Migration(2, "half", "CREATE TABLE half_table (x INTEGER);\nCREATE TABLE broken ("),
    )
    with pytest.raises(sqlite3.Error):
        open_store(db, migrations=bad)
    store = open_store(db)
    assert store.schema_version() == 1
    names = {r[0] for r in store.query("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "half_table" not in names
    store.close()


def test_migration_fault_before_commit_rolls_back(tmp_path):
    def hook(point: str) -> None:
        if point == "migration-1":
            raise Crash(point)

    with pytest.raises(Crash):
        open_store(tmp_path / "x.db", fault_hook=hook)
    store = open_store(tmp_path / "x.db")
    assert store.schema_version() == 1
    store.close()


# ================================================================= inbox


def delivery(guid="d-1", body=b'{"a":1}') -> DeliveryRecord:
    return DeliveryRecord(
        guid, "issue_comment", body, {"X-GitHub-Event": "issue_comment"}, action="created"
    )


def test_delivery_dedupe_and_quarantine(db):
    store = open_store(db)
    assert store.append_delivery(delivery()) == DeliveryOutcome.INSERTED
    assert store.append_delivery(delivery()) == DeliveryOutcome.DUPLICATE
    assert store.append_delivery(delivery(body=b"other")) == DeliveryOutcome.QUARANTINED
    rows = store.query("SELECT outcome FROM delivery_attempts ORDER BY id")
    assert [r[0] for r in rows] == ["inserted", "duplicate", "quarantined"]
    [pending] = store.pending_deliveries()
    assert pending.body == b'{"a":1}'  # the original bytes, never the conflicting copy
    store.close()


def test_delivery_marked_processed_in_event_transaction(db):
    store = open_store(db)
    h = store_harness(store)
    store.append_delivery(delivery("d-9"))
    event = replace(h.f(P).make(ev.GitHubSnapshot(), evidence=None), delivery_guid="d-9")
    h.apply(event)
    assert store.pending_deliveries() == []
    store.close()


def test_delivery_survives_reopen_until_processed(db):
    store = open_store(db)
    store.append_delivery(delivery("d-2"))
    store.close()
    store = open_store(db)
    assert [d.delivery_guid for d in store.pending_deliveries()] == ["d-2"]
    store.close()


# ======================================================= apply / outbox


def test_apply_commits_event_aggregate_projections_effects_and_audit(db):
    store = open_store(db)
    h = store_harness(store)
    h.eligible(P)
    r = h.send(P, ev.RequestTriage(via=Via.DRAG))
    create = [e for e in r.effects if e.kind == EffectKind.CREATE_SESSION]
    assert create
    assert store.load_parcel(P) == h.p(P)
    [intent] = store.query("SELECT * FROM dispatch_intents")
    assert intent["nonce"] == h.cur(P).nonce and intent["create_state"] == "pending"
    [session] = store.query("SELECT * FROM stage_sessions")
    assert session["lifecycle"] == "INTENT" and session["fence_mask"] == 0
    assert len(store.audit_entries(P)) == 2  # snapshot + triage (unpause is global)
    assert [e.kind for e in store.events_for(P)][-1] == ev.EventKind.REQUEST_TRIAGE
    store.close()


def test_double_ingest_is_a_durable_noop(db):
    store = open_store(db)
    h = store_harness(store)
    h.eligible(P)
    event = h.f(P).make(ev.RequestTriage(via=Via.DRAG))
    first = store.apply_event(event, h.cfg)
    second = store.apply_event(event, h.cfg)
    assert not first.duplicate and second.duplicate and second.effects == ()
    assert len(store.query("SELECT * FROM effects WHERE event_id = ?", (event.event_id,))) == len(
        first.effects
    )
    assert second.parcel == first.parcel
    store.close()


def test_reducer_exception_rolls_back_everything(db):
    store = open_store(db)
    h = store_harness(store)

    def boom(state: State, event: Event) -> TransitionResult:
        raise RuntimeError("reducer bug")

    before = len(store.query("SELECT * FROM events"))
    with pytest.raises(RuntimeError):
        store.apply_event(h.f(P).make(ev.RequestTriage(via=Via.DRAG)), h.cfg, reducer=boom)
    assert len(store.query("SELECT * FROM events")) == before
    assert store.load_parcel(P) is None
    store.close()


@pytest.mark.parametrize(
    "point", ["after-parcel-write", "after-event-insert", "after-effects", "before-commit"]
)
def test_crash_before_commit_loses_nothing_acknowledged(db, point):
    armed = {"on": False}

    def hook(p: str) -> None:
        if armed["on"] and p == point:
            raise Crash(p)

    store = open_store(db, fault_hook=hook)
    h = store_harness(store)
    h.eligible(P)
    event = h.f(P).make(ev.RequestTriage(via=Via.DRAG))
    armed["on"] = True
    with pytest.raises(Crash):
        store.apply_event(event, h.cfg)
    store.close()
    store = open_store(db)
    assert not store.has_event(event.event_id)
    assert store.query("SELECT * FROM effects WHERE event_id = ?", (event.event_id,)) == []
    result = store.apply_event(event, h.cfg)  # redelivery/replay succeeds exactly once
    assert result.accepted and not result.duplicate
    store.close()


def test_crash_after_commit_keeps_intents_and_dedupes_replay(db):
    store = open_store(db)
    h = store_harness(store)
    h.eligible(P)
    event = h.f(P).make(ev.RequestTriage(via=Via.DRAG))
    result = store.apply_event(event, h.cfg)
    store._conn.close()  # simulate process death right after commit
    store = open_store(db)
    pending = {s.effect.effect_id for s in store.pending_effects()}
    assert {e.effect_id for e in result.effects} <= pending
    assert store.apply_event(event, h.cfg).duplicate
    store.close()


def test_outbox_claim_complete_unknown_and_recovery(db):
    store = open_store(db)
    h = store_harness(store)
    h.eligible(P)
    r = h.send(P, ev.RequestTriage(via=Via.DRAG))
    create = next(e for e in r.effects if e.kind == EffectKind.CREATE_SESSION)
    lease = store.acquire_lease(P, "boot-1")
    assert store.claim_effect(create.effect_id, replace(lease, epoch=99)) is None  # stale lease
    claimed = store.claim_effect(create.effect_id, lease)
    assert claimed is not None and claimed.state == "claimed" and claimed.attempts == 1
    assert claimed.effect == create
    assert store.claim_effect(create.effect_id, lease) is None  # no double claim
    assert store.mark_effect_unknown(create.effect_id, "lost ack")
    assert store.get_effect(create.effect_id).state == "unknown"
    assert store.claim_effect(create.effect_id, lease) is None  # never back to pending
    assert store.query("SELECT create_state FROM dispatch_intents")[0][0] == "unknown"
    assert store.complete_effect(create.effect_id, remote_id="root-1")  # adopted later
    assert store.query("SELECT adopted_root_id FROM dispatch_intents")[0][0] == "root-1"
    store.close()


def test_restart_recovery_never_resends_non_idempotent_writes(db):
    store = open_store(db)
    h = store_harness(store)
    h.eligible(P)
    r = h.send(P, ev.RequestTriage(via=Via.DRAG))
    lease = store.acquire_lease(P, "boot-1")
    for e in r.effects:
        store.claim_effect(e.effect_id, lease)
    store.close()
    store = open_store(db)
    recovered = {s.effect.kind: s.state for s in store.recover_claimed()}
    assert recovered[EffectKind.CREATE_SESSION] == "unknown"
    # F5: the dispatch intent and its outbox row agree after recovery.
    rows = store.query(
        "SELECT e.state, d.create_state FROM dispatch_intents d "
        "JOIN effects e ON e.effect_id = d.effect_id"
    )
    assert [tuple(r) for r in rows] == [("unknown", "unknown")]
    assert all(state == "unknown" for state in recovered.values())  # no blind resend
    assert recovered.get(EffectKind.SET_BOT, "unknown") == "unknown"
    store.close()


def test_retryable_read_failure_is_rescheduled(db):
    store = open_store(db)
    h = store_harness(store)
    h.eligible(P)
    r = h.send(P, ev.ReconcileDue())
    [rec] = [e for e in r.effects if e.kind == EffectKind.RECONCILE_PARCEL]
    lease = store.acquire_lease(P, "b")
    store.claim_effect(rec.effect_id, lease)
    later = store._clock.now_utc_us() + 10
    assert store.fail_effect(rec.effect_id, "503", retry_at_us=later)
    assert rec.effect_id not in {s.effect.effect_id for s in store.pending_effects()}
    assert rec.effect_id in {s.effect.effect_id for s in store.pending_effects(now_us=later)}
    assert store.cancel_effect(rec.effect_id, "stale")
    store.close()


# ================================================================ leases


def test_leases_never_stolen_implicitly(db):
    store = open_store(db)
    a = store.acquire_lease(P, "boot-a")
    assert store.acquire_lease(P, "boot-a") == a
    with pytest.raises(LeaseHeld):
        store.acquire_lease(P, "boot-b")
    b = store.acquire_lease(P, "boot-b", takeover=True)
    assert b.epoch == a.epoch + 1
    assert not store.heartbeat(a) and store.heartbeat(b)
    assert not store.release_lease(a) and store.release_lease(b)
    assert store.acquire_lease(P, "boot-c").epoch == 1
    store.close()


# ============================================================= admission


def test_store_rejects_reservations_beyond_cap(db):
    store = open_store(db)
    h = store_harness(store, max_building=1)
    h.to_building(P)
    h.plan_published(Q)
    h.approve(Q)

    def cheating(state: State, event: Event) -> TransitionResult:
        result = transition(state, event)
        adm = result.state.admission
        from omnigent_factory.core.types import Reservation

        forced = replace(
            adm,
            reservations=(
                *adm.reservations,
                Reservation("rs-forced", Q, ReservationKind.BUILDING, "ep"),
            ),
        )
        return replace(result, state=replace(result.state, admission=forced))

    with pytest.raises(InvariantViolation):
        store.apply_event(h.f(Q).make(ev.ReconcileDue()), h.cfg, reducer=cheating)
    assert store.load_admission(REPO_ID).building_count == 1
    store.close()


def test_two_parcel_cap_race_admits_exactly_one(db):
    store = open_store(db)
    h = store_harness(store, max_building=1)
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(P)
    h.approve(Q)
    cfg = h.cfg
    store.close()

    def stale_head(state: State, event: Event) -> TransitionResult:
        # Simulate each worker believing it is at the queue head: only the serialized
        # admission ledger can then prevent over-admission.
        adm = state.admission
        own = tuple(q for q in adm.queue if q.parcel_id == event.parcel_id)
        return transition(replace(state, admission=replace(adm, queue=own)), event)

    barrier = threading.Barrier(2)
    results: dict[str, object] = {}

    def worker(pid: str) -> None:
        s = open_store(db)
        try:
            event = h.f(pid).make(ev.CapacityAvailable())
            barrier.wait()
            results[pid] = s.apply_event(event, cfg, reducer=stale_head)
        finally:
            s.close()

    threads = [threading.Thread(target=worker, args=(pid,)) for pid in (P, Q)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    accepted = [pid for pid, r in results.items() if r.accepted]  # type: ignore[attr-defined]
    assert len(accepted) == 1
    store = open_store(db)
    adm = store.load_admission(REPO_ID)
    assert adm.building_count == 1
    statuses = {q.parcel_id: q.status for q in adm.queue}
    assert sorted(statuses.values()) == [QueueStatus.QUEUED, QueueStatus.RESERVED]
    store.close()


# ======================================================= restart (inv 12)


def test_restart_preserves_time_decisions_fences_and_allowances(db):
    store = open_store(db)
    h = store_harness(store)
    b = h.to_building(P)
    h.send(
        P,
        ev.ActiveTimeSample(
            session_id=b.session_id, grant_id=b.grant.grant_id, consumed_us=MICROS_PER_HOUR
        ),
    )
    h.send(P, ev.ElicitationOpened(session_id=b.session_id, elicitation_id="q1"))
    h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    h.send(P, ev.GraceExpired(session_id=b.session_id, grant_id=b.grant.grant_id))
    before = h.p(P)
    admission = store.load_admission(REPO_ID)
    store.close()

    store = open_store(db)
    after = store.load_parcel(P)
    assert after == before
    s = after.session(b.session_id)
    assert s.grant.consumed_us == MICROS_PER_HOUR and s.restart_count == 0
    assert FenceKind.CHECKPOINT in s.fences and s.lifecycle == Lifecycle.DRAINING
    assert after.decisions[0].status == DecisionStatus.OPEN
    assert store.load_admission(REPO_ID) == admission
    [fence] = store.query("SELECT kind FROM fences WHERE cleared_at_us IS NULL")
    assert fence[0] == "checkpoint"
    store.close()


def test_projection_constraints_hold_through_full_flow(db):
    store = open_store(db)
    h = store_harness(store)
    h.to_building(P)
    h.build_ready(P)
    open_gates = store.query(
        "SELECT parcel_id, COUNT(*) c FROM stage_sessions WHERE execution_closed = 0 "
        "AND fence_mask = 0 GROUP BY parcel_id HAVING c > 1"
    )
    assert open_gates == []
    [contract] = store.query("SELECT * FROM contracts WHERE published = 1 AND superseded = 0")
    [approval] = store.query("SELECT * FROM approvals")
    assert approval["full_hash"] == contract["full_hash"]
    assert store.load_admission(REPO_ID).building_count == 0
    assert len(store.query("SELECT * FROM grants WHERE is_current = 1")) == 2  # plan + build
    store.backup(db.with_suffix(".bak"))
    copy = open_store(db.with_suffix(".bak"))
    assert copy.load_parcel(P) == h.p(P)
    copy.close()
    store.close()
