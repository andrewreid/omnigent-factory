"""An ``unknown`` create closes once adoption settles whether its session exists.

A ``create_session`` whose POST timed out stays ``unknown``; the reducer marks the run
UNKNOWN and a ``RECONCILE_SESSION`` searches for its nonce. That search's
``AdoptionResult`` (a separate effect's ack) adopts the session, but before this fix the
original create row stayed ``unknown`` forever (live #822: ``status`` kept counting it).
The row must close in the adopting transaction, and a restart must repair rows an earlier
daemon left behind. It never returns to ``pending``: the create is never sent again.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import Lifecycle, Via
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import REPO_ID, result_candidate
from omnigent_factory.testing.fakes import FakeClock

from .test_store import P, StoreHarness, store_harness
from .test_store_durable import claim


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


def unknown_create(store: SqliteStore) -> tuple[StoreHarness, str, str, str]:
    """A claimed create_session recorded ``unknown`` exactly as the executor does."""
    h = store_harness(store)
    h.eligible(P)
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    [pending] = [
        e for e in store.pending_effects(now_us=2**62) if e.effect.kind == EffectKind.CREATE_SESSION
    ]
    effect = pending.effect
    session_id = effect.preconditions.session_id or ""
    claim(store, effect.effect_id)
    unknown = ev.EffectUnknown(
        effect_id=effect.effect_id, effect_kind="create_session", session_id=session_id
    )
    assert store.record_effect_outcome(
        effect.effect_id,
        "unknown",
        from_states=("claimed",),
        event=_event(f"effect:{effect.effect_id}:unknown", unknown),
        config=h.cfg,
        reason="create outcome unknown: ReadTimeout",
    ).recorded
    parcel = store.load_parcel(P)
    assert parcel is not None
    run = parcel.session(session_id)
    assert run is not None and run.lifecycle == Lifecycle.UNKNOWN
    return h, session_id, str(effect.args["nonce"]), effect.effect_id


def _adopted(session_id: str, nonce: str, *, matches: int = 1) -> Event:
    return _event(
        "effect:ef_adoption_search:ack",
        ev.AdoptionResult(
            session_id=session_id,
            matches=matches,
            root_id="root-adopted" if matches == 1 else None,
            nonce=nonce,
        ),
    )


def _state(store: SqliteStore, effect_id: str) -> tuple[str, str | None]:
    [row] = store.query("SELECT state, remote_id FROM effects WHERE effect_id = ?", (effect_id,))
    return str(row[0]), row[1]


def _never_recreated(store: SqliteStore, effect_id: str) -> None:
    assert effect_id not in {e.effect.effect_id for e in store.pending_effects(now_us=2**62)}
    assert effect_id not in {e.effect.effect_id for e in store.effects_in_state("unknown")}


def test_adoption_closes_the_unknown_create_as_done(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, nonce, effect_id = unknown_create(store)
    assert store.apply_event(_adopted(session_id, nonce), h.cfg).accepted
    parcel = store.load_parcel(P)
    run = parcel.session(session_id) if parcel is not None else None
    assert run is not None and run.root_id == "root-adopted"
    assert _state(store, effect_id) == ("done", "root-adopted")
    assert store.effects_in_state("unknown") == []
    _never_recreated(store, effect_id)


def test_a_later_session_created_closes_the_unknown_create(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, nonce, effect_id = unknown_create(store)
    created = ev.SessionCreated(session_id=session_id, root_id="root-late", nonce=nonce)
    assert store.apply_event(_event("adopt-late", created), h.cfg).accepted
    assert _state(store, effect_id) == ("done", "root-late")
    _never_recreated(store, effect_id)


def test_zero_or_several_matches_prove_nothing_and_leave_the_row_unknown(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, nonce, effect_id = unknown_create(store)
    assert store.apply_event(_adopted(session_id, nonce, matches=0), h.cfg).accepted
    assert _state(store, effect_id) == ("unknown", None)
    assert not store.close_reconciled_unknown(effect_id)
    several = _event(
        "effect:ef_adoption_search_2:ack",
        ev.AdoptionResult(session_id=session_id, matches=2, root_id=None, nonce=nonce),
    )
    store.apply_event(several, h.cfg)
    assert _state(store, effect_id) == ("unknown", None)


def test_rejected_adoption_leaves_the_row_unknown(db: Path) -> None:
    store = SqliteStore.open(db, FakeClock())
    h, session_id, _, effect_id = unknown_create(store)
    wrong = store.apply_event(_adopted(session_id, "not-the-nonce"), h.cfg)
    assert not wrong.accepted and wrong.reason == "nonce-mismatch"
    assert _state(store, effect_id) == ("unknown", None)


def test_startup_backfill_closes_a_create_left_unknown_after_adoption(db: Path) -> None:
    """The live #822 shape: adopted and WAITING, but the create row still ``unknown``."""
    store = SqliteStore.open(db, FakeClock())
    h, session_id, nonce, effect_id = unknown_create(store)
    assert not store.close_reconciled_unknown(effect_id)  # nothing resolved it yet
    assert store.apply_event(_adopted(session_id, nonce), h.cfg).accepted
    store.close()
    with sqlite3.connect(db) as raw:  # as the pre-fix daemon left it
        raw.execute(
            "UPDATE effects SET state = 'unknown', remote_id = NULL WHERE effect_id = ?",
            (effect_id,),
        )
    store = SqliteStore.open(db, FakeClock())
    assert [u.effect.effect_id for u in store.effects_in_state("unknown")] == [effect_id]
    assert store.close_reconciled_unknown(effect_id)
    assert _state(store, effect_id) == ("done", "root-adopted")
    assert not store.close_reconciled_unknown(effect_id)  # idempotent: only unknown moves
    assert store.effects_in_state("unknown") == []
    _never_recreated(store, effect_id)


def test_publication_ack_closes_an_unknown_publication(db: Path) -> None:
    """Same gap for comment publications: an adopting ack for the effect closes it."""
    store = SqliteStore.open(db, FakeClock())
    h = store_harness(store)
    h.eligible(P)
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok(P)
    assert s.root_id is not None
    h.send(P, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE))
    [publish] = [
        e for e in store.pending_effects(now_us=2**62) if e.effect.kind == EffectKind.PUBLISH_TRIAGE
    ]
    effect_id = publish.effect.effect_id
    claim(store, effect_id)
    unknown = ev.EffectUnknown(effect_id=effect_id, effect_kind="publish_triage")
    assert store.record_effect_outcome(
        effect_id,
        "unknown",
        from_states=("claimed",),
        event=_event(f"effect:{effect_id}:unknown", unknown),
        config=h.cfg,
    ).recorded
    acked = ev.PublicationAcked(
        effect_id=effect_id, effect_kind="publish_triage", comment_id="c-77"
    )
    assert store.apply_event(_event("effect:ef_rerender:ack", acked), h.cfg).accepted
    assert _state(store, effect_id) == ("done", "c-77")
