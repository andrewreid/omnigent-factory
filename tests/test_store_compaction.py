"""Pruned delivery bodies give their page space back; the WAL file stays small."""

from __future__ import annotations

import random
from dataclasses import replace
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store.sqlite import DeliveryOutcome, SqliteStore
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock

from .test_store import P, store_harness
from .test_store_idle_retention import DAY_US, delivery

WAL_SIZE_LIMIT_BYTES = 4 * 1024 * 1024


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "state.sqlite3"


def _pages(store: SqliteStore, name: str) -> int:
    return int(store.query("SELECT COUNT(*) FROM dbstat WHERE name = ?", (name,))[0][0])


def test_pruned_bodies_free_their_leaf_pages(db: Path) -> None:
    """400 processed CI-sized deliveries (2-30 KB bodies), then the body prune.

    Before (base, emptied in place): ~400 pages stayed in the deliveries table, each
    holding one ~200-byte row (the file kept ~1.6 MB of dead space per 400 rows for the
    14-day row retention). Now the pruned rows pack into a few pages.
    """
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    rng = random.Random(7)  # noqa: S311 - reproducible sizes, not security
    for i in range(400):
        body = b'{"x":"' + b"a" * rng.randint(2_000, 30_000) + b'"}'
        store.append_delivery(delivery(f"d-{i:03d}", body))
        store.mark_delivery(f"d-{i:03d}", "processed")
    clock.advance(DAY_US)
    while store.prune_delivery_bodies(clock.now_utc_us(), limit=100) == 100:
        pass
    store.incremental_vacuum(100_000)
    pages = _pages(store, "deliveries")
    print(f"deliveries table pages after prune: {pages}")
    assert pages < 40
    rows = store.query(
        "SELECT COUNT(*), SUM(LENGTH(body)), COUNT(body_pruned_at_us) FROM deliveries"
    )[0]
    assert tuple(rows) == (400, 0, 400)
    store.close()


def test_a_rewritten_row_keeps_its_identity_and_references(db: Path) -> None:
    clock = FakeClock()
    store = SqliteStore.open(db, clock)
    h = store_harness(store)
    store.append_delivery(delivery("observed"))
    read = h.f(P).make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=clock.now_utc_us()))
    store.apply_event(replace(read, delivery_guid="observed"), h.cfg)
    # A check observation (its body is never read back) references the delivery.
    store.query("UPDATE events SET kind = 'ChecksChanged' WHERE delivery_guid = 'observed'")
    before = tuple(
        store.query(
            "SELECT delivery_guid, event_name, body_sha256, received_at_us, provenance, "
            "status, processed_at_us FROM deliveries"
        )[0]
    )
    clock.advance(DAY_US)
    assert store.prune_delivery_bodies(clock.now_utc_us()) == 1
    after = store.query(
        "SELECT delivery_guid, event_name, body_sha256, received_at_us, provenance, "
        "status, processed_at_us, body, headers_json, body_pruned_at_us FROM deliveries"
    )[0]
    assert tuple(after)[:7] == before
    assert (bytes(after["body"]), after["headers_json"]) == (b"", "{}")
    assert after["body_pruned_at_us"] == clock.now_utc_us()
    assert store.query("SELECT COUNT(*) FROM events WHERE delivery_guid = 'observed'")[0][0] == 1
    assert store.query("PRAGMA foreign_key_check") == []
    assert store.append_delivery(delivery("observed")) == DeliveryOutcome.DUPLICATE
    store.close()


def test_the_wal_is_truncated_after_checkpoints(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    assert int(store.query("PRAGMA journal_size_limit")[0][0]) == WAL_SIZE_LIMIT_BYTES
    for i in range(300):  # ~9 MB through the WAL in one transaction
        store.append_delivery(delivery(f"w-{i}", b"x" * 30_000))
    store.query("PRAGMA wal_checkpoint(PASSIVE)")
    store.append_delivery(delivery("after"))  # restarts the WAL: truncated to the limit
    wal = db.with_name(db.name + "-wal")
    assert wal.stat().st_size <= WAL_SIZE_LIMIT_BYTES
    store.close()


def test_unread_bodies_are_kept_two_hours_by_default() -> None:
    assert ServiceConfig.model_fields["delivery_body_retention_days"].default == 2 / 24
