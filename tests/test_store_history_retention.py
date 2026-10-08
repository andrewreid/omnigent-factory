"""Migration 9: bounded aggregates, observation-event retention and file compaction."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from omnigent_factory.core import codec
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.store.migrations import MIGRATIONS
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock

from .test_store import P, Q, store_harness

HOUR_US = 3600 * 1_000_000


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "state.sqlite3"


def _reconcile(h, parcel_id: str, now: int) -> Event:
    return h.f(parcel_id).make(
        ev.ReconcileDue(),
        provenance=Provenance.SCHEDULER,
        event_id=f"reconcile:{parcel_id}:{now}",
        evidence=None,
    )


def _snapshot_ack(h, parcel_id: str, effect_id: str, now: int) -> Event:
    return h.f(parcel_id).make(
        ev.GitHubSnapshot(),
        provenance=Provenance.ADAPTER,
        event_id=f"effect:{effect_id}:ack",
        evidence=snapshot(read_at_us=now),
    )


def _cycle(store: SqliteStore, h, clock: FakeClock, parcel_id: str) -> tuple[str, str]:
    """One reconcile cycle as the daemon runs it: wake-up, read effect, settled ack."""
    now = clock.now_utc_us()
    due = store.apply_event(_reconcile(h, parcel_id, now), h.cfg)
    (read,) = [e for e in due.effects if e.kind == EffectKind.RECONCILE_PARCEL]
    store.apply_event(_snapshot_ack(h, parcel_id, read.effect_id, now), h.cfg)
    store.query("UPDATE effects SET state = 'done' WHERE effect_id = ?", (read.effect_id,))
    clock.advance(120 * 1_000_000)
    return f"reconcile:{parcel_id}:{now}", read.effect_id


def _event_ids(store: SqliteStore) -> set[str]:
    return {str(r[0]) for r in store.query("SELECT event_id FROM events")}


def _prune_all(store: SqliteStore, before_us: int, *, dry_run: bool = False):
    totals = [0, 0, 0]
    after = 0
    while True:
        batch = store.prune_observations(before_us, after_sequence=after, limit=3, dry_run=dry_run)
        totals = [totals[0] + batch.events, totals[1] + batch.effects, totals[2] + batch.audit]
        if batch.done:
            return tuple(totals)
        after = batch.last_sequence


# ============================================================ bounded aggregate


def test_stored_aggregate_omits_applied_event_ids_and_duplicates_stay_refused(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    first = h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1))
    store.apply_event(first, h.cfg)
    for _ in range(5):
        _cycle(store, h, clock, P)
    (text,) = [str(r[0]) for r in store.query("SELECT aggregate_json FROM parcels")]
    assert json.loads(text)["parcel"]["applied_event_ids"] == []
    store.close()

    reopened = SqliteStore.open(db, clock)
    again = reopened.apply_event(first, h.cfg)
    assert again.duplicate and again.effects == ()
    assert (
        reopened.query("SELECT COUNT(*) FROM events WHERE event_id = ?", (first.event_id,))[0][0]
        == 1
    )
    reopened.close()


def test_v8_aggregates_lose_their_event_id_sets_on_upgrade(db: Path):
    clock = FakeClock()
    old = SqliteStore.open(db, clock, migrations=MIGRATIONS[:8])
    h = store_harness(old)
    old.apply_event(h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1)), h.cfg)
    parcel = old.load_parcel(P)
    assert parcel is not None
    bloated = replace(parcel, applied_event_ids=frozenset(f"e-{i}" for i in range(50)))
    old.query(
        "UPDATE parcels SET aggregate_json = ? WHERE parcel_id = ?",
        (codec.parcel_to_json(bloated), P),
    )
    old.close()

    store = SqliteStore.open(db, clock)
    assert store.schema_version() == 10
    loaded = store.load_parcel(P)
    assert loaded is not None and loaded.applied_event_ids == frozenset()
    assert replace(loaded, applied_event_ids=bloated.applied_event_ids) == bloated
    names = {str(r[0]) for r in store.query("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"ix_effects_event", "ix_audit_event", "ix_events_parcel_kind"} <= names
    store.close()


# ============================================================ observation retention


def test_prune_removes_old_settled_reconcile_cycles_and_keeps_the_newest(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    cycles = [_cycle(store, h, clock, P) for _ in range(4)]
    cutoff = clock.now_utc_us()
    recent = _cycle(store, h, clock, P)
    before = _event_ids(store)

    # Dry run: same counts, nothing removed.
    assert _prune_all(store, cutoff, dry_run=True) == (8, 4, 8)
    assert _event_ids(store) == before

    assert _prune_all(store, cutoff) == (8, 4, 8)
    left = _event_ids(store)
    for due, read in cycles:
        assert due not in left and f"effect:{read}:ack" not in left
        assert not store.query("SELECT 1 FROM effects WHERE effect_id = ?", (read,))
        assert not store.query("SELECT 1 FROM audit WHERE event_id = ?", (due,))
    assert {recent[0], f"effect:{recent[1]}:ack"} <= left
    assert _prune_all(store, cutoff) == (0, 0, 0)  # idempotent

    # Everything older than now: the newest wake-up and snapshot of the parcel stay.
    _prune_all(store, clock.now_utc_us() + 1)
    assert {recent[0], f"effect:{recent[1]}:ack"} <= _event_ids(store)
    assert store.load_parcel(P) is not None
    store.close()


def test_prune_never_touches_unsettled_effects_or_non_observation_events(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    control = h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1))
    store.apply_event(control, h.cfg)  # a builder ID: not a never-recurring prefix
    pending = []
    for state in ("pending", "claimed", "unknown"):
        due, read = _cycle(store, h, clock, P)
        store.query("UPDATE effects SET state = ? WHERE effect_id = ?", (state, read))
        pending.append((due, read))
    # A wake-up whose spawned effect carries a semantic dedupe key stays (and its effect).
    semantic_due, semantic_read = _cycle(store, h, clock, Q)
    store.query(
        "UPDATE effects SET dedupe_key = 'cleanup:semantic' WHERE effect_id = ?",
        (semantic_read,),
    )
    _cycle(store, h, clock, Q)
    settled = _cycle(store, h, clock, P)
    _cycle(store, h, clock, P)

    _prune_all(store, clock.now_utc_us())
    left = _event_ids(store)
    assert settled[0] not in left  # the sweep ran: a settled cycle went
    assert control.event_id in left
    for due, read in pending:
        # The wake-up that spawned it and the ack startup recovery looks up both stay.
        assert due in left and f"effect:{read}:ack" in left
        assert store.query("SELECT 1 FROM effects WHERE effect_id = ?", (read,))
    assert semantic_due in left
    assert store.query("SELECT 1 FROM effects WHERE effect_id = ?", (semantic_read,))
    store.close()


def test_prune_keeps_events_other_tables_reference(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    referenced, _read = _cycle(store, h, clock, P)
    store.query("PRAGMA foreign_keys=OFF")
    store.query(
        "INSERT INTO fences (session_id, kind, cause_event_id, set_at_us) VALUES (?, ?, ?, 0)",
        ("s-1", "safety", referenced),
    )
    store.query("PRAGMA foreign_keys=ON")
    _cycle(store, h, clock, P)
    _cycle(store, h, clock, P)
    _prune_all(store, clock.now_utc_us())
    assert referenced in _event_ids(store)
    store.close()


def test_check_run_delivery_bodies_are_prunable_other_linked_bodies_are_not(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    for guid in ("checks", "snapshot"):
        store.append_delivery(DeliveryRecord(guid, "workflow_run", b'{"a":1}', {}))
    checks = h.f(P).make(ev.ChecksChanged(pr_number=1, head_sha="abc"), evidence=None)
    store.apply_event(replace(checks, delivery_guid="checks"), h.cfg)
    other = h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1))
    store.apply_event(replace(other, delivery_guid="snapshot"), h.cfg)
    clock.advance(10 * 24 * HOUR_US)

    assert store.prune_delivery_bodies(clock.now_utc_us(), dry_run=True) == 1
    assert store.prune_delivery_bodies(clock.now_utc_us()) == 1
    bodies = {
        str(r[0]): bytes(r[1]) for r in store.query("SELECT delivery_guid, body FROM deliveries")
    }
    assert bodies == {"checks": b"", "snapshot": b'{"a":1}'}
    store.close()


# ============================================================ compaction


def test_vacuum_switches_to_incremental_and_later_sweeps_return_pages(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    assert store.storage_stats()["auto_vacuum"] == 2  # a new file starts incremental
    store.close()

    legacy = db.with_name("legacy.sqlite3")
    raw = sqlite3.connect(legacy)
    raw.execute("CREATE TABLE seed (x)")  # an existing file keeps auto_vacuum=NONE
    raw.close()
    store = SqliteStore.open(legacy, clock)
    h = store_harness(store)
    assert store.storage_stats()["auto_vacuum"] == 0
    for _ in range(40):
        _cycle(store, h, clock, P)
    _prune_all(store, clock.now_utc_us())
    assert store.incremental_vacuum(1000) > 0  # NONE: freed pages stay in the file
    before = store.storage_stats()
    store.vacuum()
    after = store.storage_stats()
    assert after["auto_vacuum"] == 2 and after["free_bytes"] == 0
    assert after["bytes"] < before["bytes"]
    for _ in range(40):
        _cycle(store, h, clock, P)
    _prune_all(store, clock.now_utc_us())
    assert store.storage_stats()["free_bytes"] > 0
    assert store.incremental_vacuum(100_000) == 0
    store.close()


def test_delivery_keyed_check_events_prune_content_keyed_ids_never_do(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    checks = [f"github:workflow_run:ChecksChanged:g-{i}" for i in range(3)]
    for event_id in checks:
        store.apply_event(
            h.f(P).make(ev.ChecksChanged(pr_number=1), event_id=event_id, evidence=None), h.cfg
        )
    # Same kind, but an ID GitHub content could produce again (e.g. a recovery read).
    content = "github:comment:42:created"
    store.apply_event(
        h.f(P).make(ev.ChecksChanged(pr_number=1), event_id=content, evidence=None), h.cfg
    )
    store.apply_event(
        h.f(P).make(ev.ChecksChanged(pr_number=1), event_id=checks[0] + "-x", evidence=None),
        h.cfg,
    )
    clock.advance(HOUR_US)
    _prune_all(store, clock.now_utc_us())
    left = _event_ids(store)
    assert not set(checks) & left
    assert {content, checks[0] + "-x"} <= left  # content-keyed, and the newest check event
    store.close()
