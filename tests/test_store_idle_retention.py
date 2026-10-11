"""Migration 8: the indexed inbox lookup, delivery-body retention and the parcel cache."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from omnigent_factory.core import codec
from omnigent_factory.core import events as ev
from omnigent_factory.store.migrations import MIGRATIONS
from omnigent_factory.store.sqlite import DeliveryOutcome, DeliveryRecord, SqliteStore
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock

from .test_store import P, store_harness

DAY_US = 86_400 * 1_000_000


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "state.sqlite3"


def delivery(guid: str, body: bytes = b'{"a":1}') -> DeliveryRecord:
    return DeliveryRecord(guid, "issues", body, {"x-github-delivery": guid})


def plan(store: SqliteStore, sql: str) -> str:
    return " ".join(str(row[3]) for row in store.query("EXPLAIN QUERY PLAN " + sql))


def test_v7_database_with_inbox_rows_upgrades_and_uses_the_partial_index(db: Path):
    clock = FakeClock()
    old = SqliteStore.open(db, clock, migrations=MIGRATIONS[:7])
    for guid in ("d-1", "d-2", "d-3", "d-4"):
        old.append_delivery(delivery(guid))
        clock.advance(1)
    old.mark_delivery("d-2", "processed")
    old.mark_delivery("d-3", "rejected")
    old.defer_delivery_resolution("d-4", clock.now_utc_us() + 10, max_attempts=3)
    before = [d.delivery_guid for d in old.pending_deliveries()]
    old.close()

    store = SqliteStore.open(db, clock)
    assert store.schema_version() == 15
    assert [d.delivery_guid for d in store.pending_deliveries()] == before == ["d-1"]
    assert store.inbox_due() == [("d-1", None), ("d-4", clock.now_utc_us() + 10)]
    assert store.has_pending_delivery()
    rows = store.query("SELECT delivery_guid, body_pruned_at_us FROM deliveries ORDER BY 1")
    assert [tuple(r) for r in rows] == [("d-1", None), ("d-2", None), ("d-3", None), ("d-4", None)]
    assert bytes(store.query("SELECT body FROM deliveries WHERE delivery_guid = 'd-2'")[0][0])
    pending_sql = (
        "SELECT delivery_guid FROM deliveries WHERE status IN ('pending', 'unresolved') "
        "AND (resolution_retry_at_us IS NULL OR resolution_retry_at_us <= 0) "
        "ORDER BY received_at_us, delivery_guid"
    )
    assert "USING INDEX ix_deliveries_inbox" in plan(store, pending_sql)
    assert "TEMP B-TREE" not in plan(store, pending_sql)  # the index covers the ORDER BY
    assert "USING INDEX ix_deliveries_inbox" in plan(
        store,
        "SELECT 1 FROM deliveries WHERE status IN ('pending', 'unresolved') "
        "AND status = 'pending' LIMIT 1",
    )
    assert "ix_events_delivery" in plan(
        store, "SELECT 1 FROM events e WHERE e.delivery_guid = 'd-1'"
    )
    store.close()


def test_has_pending_delivery_ignores_unresolved_and_finished_rows(db: Path):
    store = SqliteStore.open(db, FakeClock())
    assert not store.has_pending_delivery()
    store.append_delivery(delivery("d-1"))
    assert store.has_pending_delivery()
    store.mark_delivery("d-1", "unresolved")
    assert not store.has_pending_delivery()
    store.mark_delivery("d-1", "processed")
    assert not store.has_pending_delivery()
    assert store.inbox_due() == []
    store.close()


def test_retention_empties_only_old_unreferenced_processed_bodies(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    for guid in ("old-ignored", "old-linked", "old-pending", "old-unresolved", "old-rejected"):
        store.append_delivery(delivery(guid))
    store.mark_delivery("old-ignored", "processed")
    linked = h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=clock.now_utc_us()))
    store.apply_event(replace(linked, delivery_guid="old-linked"), h.cfg)  # marks processed
    store.mark_delivery("old-unresolved", "unresolved")
    store.mark_delivery("old-rejected", "rejected")
    clock.advance(15 * DAY_US)
    store.append_delivery(delivery("recent"))
    store.mark_delivery("recent", "processed")
    sha = delivery("old-ignored").body_sha256

    assert store.prune_delivery_bodies(clock.now_utc_us() - 14 * DAY_US) == 1
    rows = {
        str(r[0]): (bytes(r[1]), r[2], r[3], r[4])
        for r in store.query(
            "SELECT delivery_guid, body, headers_json, body_sha256, body_pruned_at_us "
            "FROM deliveries"
        )
    }
    assert rows["old-ignored"] == (b"", "{}", sha, clock.now_utc_us())
    for kept in ("old-linked", "old-pending", "old-unresolved", "old-rejected", "recent"):
        assert rows[kept][0] == b'{"a":1}' and rows[kept][3] is None, kept
    # Idempotent, and the pruned GUID still dedupes a redelivery or recovered copy.
    assert store.prune_delivery_bodies(clock.now_utc_us() - 14 * DAY_US) == 0
    assert store.append_delivery(delivery("old-ignored")) == DeliveryOutcome.DUPLICATE
    recovered = replace(delivery("old-ignored", b"{ }"), provenance="recovery")
    assert store.append_delivery(recovered) == DeliveryOutcome.DUPLICATE
    assert store.pending_deliveries()[0].delivery_guid == "old-pending"
    store.close()


def test_retention_runs_in_bounded_batches(db: Path):
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    for i in range(7):
        store.append_delivery(delivery(f"d-{i}"))
        store.mark_delivery(f"d-{i}", "processed")
    clock.advance(DAY_US)
    assert [store.prune_delivery_bodies(clock.now_utc_us(), limit=3) for _ in range(4)] == [
        3,
        3,
        1,
        0,
    ]
    store.close()


def test_parcel_cache_skips_decoding_unchanged_aggregates(db: Path, monkeypatch):
    store = SqliteStore.open(db, FakeClock())
    h = store_harness(store)
    h.apply(h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1)))
    decodes = 0
    real = codec.parcel_from_json

    def counting(text: str):
        nonlocal decodes
        decodes += 1
        return real(text)

    monkeypatch.setattr(codec, "parcel_from_json", counting)
    first = store.load_parcel(P)
    assert first is not None and decodes == 1
    assert store.load_parcel(P) is first and decodes == 1
    store.ensure_repository(h.cfg)  # an unrelated write: re-read, same text, no decode
    assert store.load_parcel(P) is first and decodes == 1
    assert store.load_parcel("missing") is None
    store.close()


def test_parcel_cache_sees_raw_writes_on_this_and_other_connections(db: Path):
    store = SqliteStore.open(db, FakeClock())
    h = store_harness(store)
    h.apply(h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1)))
    first = store.load_parcel(P)
    assert first is not None
    changed = codec.parcel_to_json(replace(first, issue_number=99))
    store.query("UPDATE parcels SET aggregate_json = ? WHERE parcel_id = ?", (changed, P))
    same_conn = store.load_parcel(P)
    assert same_conn is not None and same_conn.issue_number == 99

    other = sqlite3.connect(db, isolation_level=None)
    restored = codec.parcel_to_json(replace(first, issue_number=7))
    other.execute("UPDATE parcels SET aggregate_json = ? WHERE parcel_id = ?", (restored, P))
    other.execute("DELETE FROM parcels WHERE parcel_id = 'nobody'")
    other.close()
    other_conn = store.load_parcel(P)
    assert other_conn is not None and other_conn.issue_number == 7
    store.close()
