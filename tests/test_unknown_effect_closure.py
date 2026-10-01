"""An ``unknown`` effect row closes when its ambiguity is later resolved (#694 follow-up).

A ``send_message`` whose write timed out stays ``unknown``; a later reconciliation (its own
``RECONCILE_SESSION`` effect) emits ``EffectReconciled`` for it. The reducer clears the
parcel's ``unknown_effects``; the original outbox row must close too, in the same
transaction, and must never return to ``pending`` (never resend).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import REPO_ID
from omnigent_factory.testing.fakes import FakeClock

from .test_store import P, StoreHarness, store_harness
from .test_store_durable import claim, send_effect


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "state.sqlite3"


def _event(event_id: str, body: ev.EventBody) -> Event:
    return Event(
        event_id=event_id,
        repo_id=REPO_ID,
        parcel_id=P,
        source_time_us=99_000_000_000,
        provenance=Provenance.ADAPTER,
        body=body,
    )


def unknown_send(store: SqliteStore) -> tuple[StoreHarness, str, str]:
    """A claimed send_message recorded ``unknown`` exactly as the executor does."""
    h = store_harness(store)
    s, effect = send_effect(h)
    claim(store, effect.effect_id)
    unknown = ev.EffectUnknown(
        effect_id=effect.effect_id, effect_kind="send_message", session_id=s.session_id
    )
    assert store.record_effect_outcome(
        effect.effect_id,
        "unknown",
        from_states=("claimed",),
        event=_event(f"effect:{effect.effect_id}:unknown", unknown),
        config=h.cfg,
        reason="message outcome unknown: ReadTimeout",
    ).recorded
    parcel = store.load_parcel(P)
    assert parcel is not None and [u.effect_id for u in parcel.unknown_effects] == [
        effect.effect_id
    ]
    return h, s.session_id, effect.effect_id


def _reconciled(effect_id: str, session_id: str, *, delivered: bool) -> Event:
    return _event(
        "effect:ef_reconcile:ack",
        ev.EffectReconciled(
            effect_id=effect_id,
            session_id=session_id,
            delivered=delivered,
            item_id="item-9" if delivered else "",
        ),
    )


def _never_resent(store: SqliteStore, effect_id: str) -> None:
    assert effect_id not in {e.effect.effect_id for e in store.pending_effects(now_us=2**62)}
    assert effect_id not in {e.effect.effect_id for e in store.effects_in_state("unknown")}


def test_delivered_reconciliation_closes_the_row_as_done(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, effect_id = unknown_send(store)
    applied = store.apply_event(_reconciled(effect_id, session_id, delivered=True), h.cfg)
    assert applied.accepted
    row = store.get_effect(effect_id)
    assert row is not None and row.state == "done"
    [remote] = store.query("SELECT remote_id FROM effects WHERE effect_id = ?", (effect_id,))
    assert tuple(remote) == ("item-9",)
    parcel = store.load_parcel(P)
    assert parcel is not None and parcel.unknown_effects == ()
    _never_resent(store, effect_id)


def test_proven_absence_closes_the_row_as_failed_not_pending(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, effect_id = unknown_send(store)
    assert store.apply_event(_reconciled(effect_id, session_id, delivered=False), h.cfg).accepted
    row = store.get_effect(effect_id)
    assert row is not None and row.state == "failed"
    _never_resent(store, effect_id)


def test_adopted_message_ack_closes_the_row(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, effect_id = unknown_send(store)
    ack = ev.MessageAck(session_id=session_id, effect_id=effect_id, item_id="item-3")
    assert store.apply_event(_event("adopt-1", ack), h.cfg).accepted
    row = store.get_effect(effect_id)
    assert row is not None and row.state == "done"
    _never_resent(store, effect_id)


def test_rejected_reconciliation_leaves_the_row_unknown(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, _, effect_id = unknown_send(store)
    wrong = store.apply_event(_reconciled(effect_id, "other-session", delivered=True), h.cfg)
    assert not wrong.accepted and wrong.reason == "ack-correlation-mismatch"
    row = store.get_effect(effect_id)
    assert row is not None and row.state == "unknown"


def test_startup_backfill_closes_rows_left_unknown_by_an_earlier_daemon(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, effect_id = unknown_send(store)
    assert not store.close_reconciled_unknown(effect_id)  # nothing resolved it yet
    assert store.apply_event(_reconciled(effect_id, session_id, delivered=True), h.cfg).accepted
    store.close()
    # The pre-fix daemon left the row ``unknown`` although the reconciliation persisted.
    with sqlite3.connect(db) as raw:
        raw.execute(
            "UPDATE effects SET state = 'unknown', remote_id = NULL WHERE effect_id = ?",
            (effect_id,),
        )
    store = SqliteStore.open(db, FakeClock())
    assert store.close_reconciled_unknown(effect_id)
    row = store.get_effect(effect_id)
    assert row is not None and row.state == "done"
    assert not store.close_reconciled_unknown(effect_id)  # idempotent: only unknown moves
    _never_resent(store, effect_id)
