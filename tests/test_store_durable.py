"""Task 5a store follow-ups: v2 migration, own-send ledger, capability records, worker
grants, parked deliveries, atomic effect outcomes and recovered-delivery duplicates.

Every durable piece is exercised across a close/reopen, and every multi-row write across
an injected crash before commit.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import FenceKind, InboxHoldReason, Lifecycle, Via
from omnigent_factory.store.migrations import MIGRATIONS
from omnigent_factory.store.sqlite import (
    CapabilityRow,
    DeliveryOutcome,
    DeliveryRecord,
    InvariantViolation,
    OwnSendRow,
    SqliteStore,
    StoreError,
    WorkerGrantRow,
)
from omnigent_factory.testing.builders import REPO_ID, config
from omnigent_factory.testing.fakes import FakeClock

from .test_store import P, StoreHarness, store_harness

LATEST = max(m.version for m in MIGRATIONS)


class Crash(Exception):
    pass


class Faults:
    def __init__(self) -> None:
        self.point: str | None = None

    def __call__(self, point: str) -> None:
        if point == self.point:
            self.point = None
            raise Crash(point)


def open_store(path: Path, faults: Faults | None = None, **kw: object) -> SqliteStore:
    return SqliteStore.open(path, FakeClock(), fault_hook=faults, **kw)  # type: ignore[arg-type]


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "state.sqlite3"


def delivery(guid: str = "d-1", body: bytes = b'{"a":1}', provenance: str = "webhook"):
    return DeliveryRecord(guid, "issues", body, {}, provenance=provenance)


# ============================================================ migration


def test_v1_database_upgrades_to_v2_preserving_data_and_reopens(db: Path):
    v1 = open_store(db, migrations=MIGRATIONS[:1])
    v1.ensure_repository(config())
    v1.append_delivery(delivery())
    assert v1.schema_version() == 1
    v1.close()

    upgraded = open_store(db)
    assert upgraded.schema_version() == LATEST == 8
    names = {r[0] for r in upgraded.query("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"own_sends", "capability_records", "worker_grants", "parked_deliveries"} <= names
    assert "mcp_feedback_reads" in names
    assert upgraded.pending_deliveries()[0].delivery_guid == "d-1"
    checksums = {
        r[0]: r[1] for r in upgraded.query("SELECT version, checksum FROM schema_migrations")
    }
    assert checksums == {m.version: m.checksum for m in MIGRATIONS}
    upgraded.close()
    again = open_store(db)
    assert again.schema_version() == LATEST
    again.close()


def test_v2_migration_crash_rolls_back_and_retries(db: Path):
    open_store(db, migrations=MIGRATIONS[:1]).close()
    faults = Faults()
    faults.point = "migration-2"
    with pytest.raises(Crash):
        open_store(db, faults)
    store = open_store(db)
    assert store.schema_version() == LATEST
    store.close()


# ============================================================ inbox: recovered duplicates


def test_recovered_copy_of_stored_guid_is_a_noop_even_if_bytes_differ(db: Path):
    store = open_store(db)
    assert store.append_delivery(delivery()) == DeliveryOutcome.INSERTED
    reencoded = delivery(body=b'{ "a": 1 }', provenance="recovery")
    assert store.append_delivery(reencoded) == DeliveryOutcome.DUPLICATE
    rows = store.query("SELECT body, provenance, status FROM deliveries")
    assert [(bytes(r[0]), r[1], r[2]) for r in rows] == [(b'{"a":1}', "webhook", "pending")]
    outcomes = [r[0] for r in store.query("SELECT outcome FROM delivery_attempts ORDER BY id")]
    assert outcomes == ["inserted", "duplicate"]
    store.close()


def test_original_webhook_after_recovered_copy_is_a_noop(db: Path):
    store = open_store(db)
    store.append_delivery(delivery(body=b'{ "a": 1 }', provenance="recovery"))
    assert store.append_delivery(delivery()) == DeliveryOutcome.DUPLICATE
    store.close()


def test_two_live_webhooks_with_different_bytes_still_quarantine(db: Path):
    store = open_store(db)
    store.append_delivery(delivery())
    assert store.append_delivery(delivery(body=b"other")) == DeliveryOutcome.QUARANTINED
    store.close()


def test_processed_delivery_is_not_reopened_by_a_recovered_copy(db: Path):
    store = open_store(db)
    store.append_delivery(delivery())
    store.mark_delivery("d-1", "processed")
    store.append_delivery(delivery(body=b"re-serialised", provenance="recovery"))
    assert store.pending_deliveries() == []
    store.close()


# ============================================================ own-send ledger


def send(effect_id: str = "e1", digest: str = "a" * 64, kind: str = "message") -> OwnSendRow:
    return OwnSendRow(effect_id, "S1", "root-1", kind, digest, "", None)


def test_own_send_intent_survives_crash_before_ack_and_first_record_wins(db: Path):
    store = open_store(db)
    store.record_own_send(send())
    store.close()  # daemon dies between the durable intent and the POST response

    store = open_store(db)
    persisted = store.own_send("e1")
    assert persisted == send()
    replay = store.record_own_send(send(digest="b" * 64))
    assert replay.text_sha256 == "a" * 64  # the text actually sent first is kept
    assert store.own_item_ids("S1") == frozenset()
    assert store.record_own_item("e1", "item-9")
    store.close()

    store = open_store(db)
    assert store.own_send("e1").item_id == "item-9"  # type: ignore[union-attr]
    assert store.own_item_ids("S1") == frozenset({"item-9"})
    assert store.record_own_item("e1", "item-9")  # idempotent
    with pytest.raises(InvariantViolation):
        store.record_own_item("e1", "item-other")
    assert not store.record_own_item("unknown", "item-1")
    store.close()


def test_own_send_rejects_unknown_kind_and_duplicate_item(db: Path):
    store = open_store(db)
    with pytest.raises(StoreError):
        store.record_own_send(send(kind="other"))
    store.record_own_send(send("e1"))
    store.record_own_send(send("e2", kind="resolve"))
    store.record_own_item("e1", "item-1")
    with pytest.raises(sqlite3.IntegrityError):
        store.record_own_item("e2", "item-1")
    store.close()


# ============================================================ capabilities / worker grants


def cap(key: str = "S1", generation: int = 1, **kw: object) -> CapabilityRow:
    fields: dict[str, object] = {
        "session_key": key,
        "stage_session_id": "S1",
        "worker_id": None,
        "worker_profile": None,
        "capability_id": f"cap-{key}-{generation}",
        "secret_sha256": "c" * 64,
        "generation": generation,
        "path": f"/caps/{key}.cap",
        "revoked": False,
    }
    fields.update(kw)
    return CapabilityRow(**fields)  # type: ignore[arg-type]


def test_capability_generations_are_monotonic_and_survive_revocation_and_reopen(db: Path):
    store = open_store(db)
    store.save_capability(cap(generation=1))
    store.save_capability(cap(generation=2))
    with pytest.raises(InvariantViolation):
        store.save_capability(cap(generation=2))
    store.revoke_capability("S1")
    store.close()

    store = open_store(db)
    [row] = store.capability_rows()
    assert (row.generation, row.revoked) == (2, True)
    with pytest.raises(InvariantViolation):
        store.save_capability(cap(generation=1))  # a revoked generation is never reused
    store.save_capability(cap(generation=3))
    assert [(r.generation, r.revoked) for r in store.capability_rows()] == [(3, False)]
    store.close()


def test_worker_capability_row_requires_its_profile(db: Path):
    store = open_store(db)
    with pytest.raises(sqlite3.IntegrityError):
        store.save_capability(cap("S1~worker~w", worker_id="w"))
    store.save_capability(cap("S1~worker~w", worker_id="w", worker_profile="read_only"))
    assert store.capability_rows()[0].worker_profile == "read_only"
    store.close()


def test_worker_grants_persist_and_delete_per_stage(db: Path):
    store = open_store(db)
    store.save_worker_grant(WorkerGrantRow("S1", "w1", "/wt/a", "factory/a", "read_only"))
    store.save_worker_grant(WorkerGrantRow("S1", "w2", "/wt/b", "factory/b", "build"))
    store.save_worker_grant(WorkerGrantRow("S2", "w1", "/wt/c", "factory/c", "build"))
    store.close()
    store = open_store(db)
    assert [(r.stage_session_id, r.worker_id) for r in store.worker_grant_rows()] == [
        ("S1", "w1"),
        ("S1", "w2"),
        ("S2", "w1"),
    ]
    store.delete_worker_grants("S1")
    assert [r.stage_session_id for r in store.worker_grant_rows()] == ["S2"]
    with pytest.raises(sqlite3.IntegrityError):
        store.save_worker_grant(WorkerGrantRow("S3", "w", "/wt", "b", "admin"))
    store.close()


# ============================================================ parked deliveries


def hold_event(guid: str, parcel: str = P) -> Event:
    return Event(
        event_id=f"inbox-hold:{guid}",
        repo_id=REPO_ID,
        parcel_id=parcel,
        source_time_us=10_000_000,
        provenance=Provenance.INBOX,
        body=ev.InboxHoldSet(delivery_guid=guid, reason=InboxHoldReason.PARKED),
    )


def test_park_is_atomic_with_status_and_parcel_hold(db: Path):
    faults = Faults()
    store = open_store(db, faults)
    cfg = config()
    store.ensure_repository(cfg)
    store.append_delivery(delivery("g1"))
    for point in ("after-park-row", "after-event-insert", "before-commit"):
        faults.point = point
        with pytest.raises(Crash):
            store.park_delivery("g1", P, hold=hold_event("g1"), config=cfg)
        assert store.parked_delivery_rows() == ()
        assert store.query("SELECT status FROM deliveries")[0][0] == "pending"
        assert store.load_parcel(P) is None
    store.park_delivery("g1", P, hold=hold_event("g1"), config=cfg)
    store.close()

    store = open_store(db)
    assert store.parked_delivery_rows() == (("g1", P),)
    assert store.query("SELECT status FROM deliveries")[0][0] == "rejected"
    held = store.load_parcel(P)
    assert held is not None and [h.delivery_guid for h in held.inbox_holds] == ["g1"]
    store.close()


def test_repark_never_narrows_scope(db: Path):
    store = open_store(db)
    store.append_delivery(delivery("g1"))
    store.append_delivery(delivery("g2"))
    store.park_delivery("g1", P)
    store.park_delivery("g1", None)
    store.park_delivery("g1", P)
    store.park_delivery("g2", P)
    store.park_delivery("g2", "I_other")
    assert store.parked_delivery_rows() == (("g1", None), ("g2", None))
    store.close()


def test_release_is_atomic_and_requeues(db: Path):
    faults = Faults()
    store = open_store(db, faults)
    cfg = config()
    store.ensure_repository(cfg)
    store.append_delivery(delivery("g1"))
    store.park_delivery("g1", P, hold=hold_event("g1"), config=cfg)
    release = Event(
        event_id="inbox-release:g1",
        repo_id=REPO_ID,
        parcel_id=P,
        source_time_us=20_000_000,
        provenance=Provenance.OPERATOR,
        body=ev.InboxHoldReleased(delivery_guid="g1"),
    )
    faults.point = "before-commit"
    with pytest.raises(Crash):
        store.release_parked_delivery("g1", release=release, config=cfg)
    assert store.parked_delivery_rows() == (("g1", P),)
    assert store.query("SELECT status FROM deliveries")[0][0] == "rejected"
    assert store.release_parked_delivery("g1", release=release, config=cfg)
    assert store.parked_delivery_rows() == ()
    assert store.query("SELECT status FROM deliveries")[0][0] == "pending"
    assert store.load_parcel(P).inbox_holds == ()  # type: ignore[union-attr]
    assert not store.release_parked_delivery("g1")
    store.close()


def test_sync_imports_legacy_scopes_and_fails_closed(db: Path):
    store = open_store(db)
    cfg = config()
    store.ensure_repository(cfg)
    for guid in ("legacy-scoped", "legacy-global", "orphan-rejected", "pending-parked"):
        store.append_delivery(delivery(guid))
    store.mark_delivery("orphan-rejected", "rejected")
    store.sync_parked_deliveries(
        (("legacy-scoped", P), ("legacy-global", None), ("pending-parked", None), ("gone", "X")),
        holds=(hold_event("legacy-scoped"),),
        config=cfg,
    )
    rows = dict(store.parked_delivery_rows())
    assert rows == {
        "legacy-scoped": P,
        "legacy-global": None,
        "pending-parked": None,
        "gone": "X",  # entry without a delivery row still gates (no FK)
        "orphan-rejected": None,  # rejected without a scope row: repository-wide
    }
    statuses = dict(store.query("SELECT delivery_guid, status FROM deliveries"))
    assert set(statuses.values()) == {"rejected"}
    assert store.load_parcel(P).inbox_holds[0].delivery_guid == "legacy-scoped"  # type: ignore[union-attr]
    # idempotent re-run (e.g. crash before the legacy file was renamed)
    store.sync_parked_deliveries(
        (("legacy-scoped", P),), holds=(hold_event("legacy-scoped"),), config=cfg
    )
    assert dict(store.parked_delivery_rows()) == rows
    store.close()


# ============================================================ atomic effect outcome


def send_effect(h: StoreHarness):
    h.eligible(P)
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok(P)
    assert s.lifecycle == Lifecycle.ACTIVE
    assert h.store is not None
    [pending] = [
        e for e in h.store.pending_effects(now_us=2**62) if e.effect.kind == EffectKind.SEND_MESSAGE
    ]
    return s, pending.effect


def claim(store: SqliteStore, effect_id: str, parcel_id: str = P) -> None:
    lease = store.acquire_lease(parcel_id, "boot")
    assert store.claim_effect(effect_id, lease) is not None


def ack_event(effect_id: str, session_id: str) -> Event:
    return Event(
        event_id=f"effect:{effect_id}:ack",
        repo_id=REPO_ID,
        parcel_id=P,
        source_time_us=99_000_000_000,
        provenance=Provenance.ADAPTER,
        body=ev.MessageAck(session_id=session_id, effect_id=effect_id, item_id="item-1"),
    )


@pytest.mark.parametrize("point", ["after-outcome-event", "after-event-insert", "before-commit"])
def test_outcome_and_event_commit_together_or_not_at_all(db: Path, point: str):
    faults = Faults()
    store = open_store(db, faults)
    h = store_harness(store)
    s, effect = send_effect(h)
    claim(store, effect.effect_id)
    faults.point = point
    with pytest.raises(Crash):
        store.record_effect_outcome(
            effect.effect_id,
            "done",
            from_states=("claimed",),
            event=ack_event(effect.effect_id, s.session_id),
            config=h.cfg,
            remote_id="item-1",
        )
    store.close()

    store = open_store(db)
    assert store.get_effect(effect.effect_id).state == "claimed"  # type: ignore[union-attr]
    assert not store.has_event(f"effect:{effect.effect_id}:ack")
    recovered = store.record_effect_outcome(
        effect.effect_id,
        "done",
        from_states=("claimed",),
        event=ack_event(effect.effect_id, s.session_id),
        config=h.cfg,
        remote_id="item-1",
    )
    assert recovered.recorded and recovered.applied is not None
    assert store.get_effect(effect.effect_id).state == "done"  # type: ignore[union-attr]
    assert store.has_event(f"effect:{effect.effect_id}:ack")
    store.close()


def test_outcome_not_in_expected_state_writes_nothing(db: Path):
    store = open_store(db)
    h = store_harness(store)
    s, effect = send_effect(h)
    before = store.load_parcel(P)
    result = store.record_effect_outcome(
        effect.effect_id,
        "done",
        from_states=("claimed",),  # still pending: never claimed
        event=ack_event(effect.effect_id, s.session_id),
        config=h.cfg,
    )
    assert not result.recorded and result.applied is None
    assert store.get_effect(effect.effect_id).state == "pending"  # type: ignore[union-attr]
    assert not store.has_event(f"effect:{effect.effect_id}:ack")
    assert store.load_parcel(P) == before
    with pytest.raises(StoreError):
        store.record_effect_outcome(
            effect.effect_id, "pending", from_states=("claimed",), event=None, config=h.cfg
        )
    store.close()


def test_unknown_outcome_records_state_and_reducer_ambiguity_together(db: Path):
    store = open_store(db)
    h = store_harness(store)
    s, effect = send_effect(h)
    claim(store, effect.effect_id)
    unknown = Event(
        event_id=f"effect:{effect.effect_id}:unknown",
        repo_id=REPO_ID,
        parcel_id=P,
        source_time_us=99_000_000_000,
        provenance=Provenance.ADAPTER,
        body=ev.EffectUnknown(
            effect_id=effect.effect_id, effect_kind=effect.kind.value, session_id=s.session_id
        ),
    )
    result = store.record_effect_outcome(
        effect.effect_id,
        "unknown",
        from_states=("claimed",),
        event=unknown,
        config=h.cfg,
        reason="timeout",
    )
    assert result.recorded
    assert store.get_effect(effect.effect_id).state == "unknown"  # type: ignore[union-attr]
    parcel = store.load_parcel(P)
    assert parcel is not None and [u.effect_id for u in parcel.unknown_effects] == [
        effect.effect_id
    ]
    # a second outcome for the same effect is refused
    again = store.record_effect_outcome(
        effect.effect_id, "done", from_states=("claimed",), event=None, config=h.cfg
    )
    assert not again.recorded
    store.close()


def test_hold_via_store_fences_the_current_session(db: Path):
    store = open_store(db)
    h = store_harness(store)
    s, _ = send_effect(h)
    store.append_delivery(delivery("g1"))
    store.park_delivery("g1", P, hold=hold_event("g1"), config=h.cfg)
    parcel = store.load_parcel(P)
    assert parcel is not None
    fenced = parcel.session(s.session_id)
    assert fenced is not None and FenceKind.SAFETY in fenced.fences
    kinds = {e.effect.kind for e in store.pending_effects(now_us=2**62)}
    assert {EffectKind.INTERRUPT_TREE, EffectKind.DISABLE_ISSUANCE} <= kinds
    mask = store.query(
        "SELECT fence_mask FROM stage_sessions WHERE session_id = ?", (s.session_id,)
    )
    assert mask[0][0] != 0
    store.close()
