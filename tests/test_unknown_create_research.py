"""Durable bounded searches for an empty ambiguous create (#1576)."""

from dataclasses import replace
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.predicates import awaiting_create
from omnigent_factory.core.projection import project_note
from omnigent_factory.core.types import Lifecycle, Stage, Via
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import REPO_ID
from omnigent_factory.testing.fakes import FakeClock
from tests.test_store import P, Q, StoreHarness, store_harness
from tests.test_store_durable import claim


def unknown_triage(store: SqliteStore, *, fence_inflight: bool = False) -> tuple[StoreHarness, str]:
    h = store_harness(store)
    h.eligible(P)
    result = h.send(P, ev.RequestTriage(via=Via.DRAG))
    [create] = h.of(result, EffectKind.CREATE_SESSION)
    sid = h.cur(P).session_id
    claim(store, create.effect_id)
    if fence_inflight:
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        assert h.cur(P).lifecycle == Lifecycle.DRAINING
    event = h.f(P).make(
        ev.EffectUnknown(create.effect_id, "create_session", sid),
        event_id=f"effect:{create.effect_id}:unknown",
    )
    assert store.record_effect_outcome(
        create.effect_id,
        "unknown",
        from_states=("claimed",),
        event=event,
        config=h.cfg,
        reason="create outcome unknown: 409",
    ).recorded
    refresh(h)
    return h, create.effect_id


def refresh(h: StoreHarness) -> None:
    assert h.store is not None
    parcel = h.store.load_parcel(P)
    assert parcel is not None
    h.parcels[P] = parcel
    h.admission = h.store.load_admission(REPO_ID)


def complete_search(h: StoreHarness, *, matches: int = 0, root: str | None = None):
    assert h.store is not None
    s = next(s for s in h.p(P).sessions if awaiting_create(s) or s.create_unconfirmed)
    [pending] = [
        e
        for e in h.store.pending_effects(now_us=2**62)
        if e.effect.kind == EffectKind.RECONCILE_SESSION
        and e.effect.preconditions.session_id == s.session_id
        and e.effect.args.get("search") == s.create_searches + 1
    ]
    effect = pending.effect
    h.f(P).tick(max(s.create_search_not_before_us - h.f(P).now, 0))
    event = h.f(P).make(
        ev.AdoptionResult(s.session_id, matches, root, s.nonce, search=s.create_searches + 1),
        event_id=f"effect:{effect.effect_id}:ack",
        entropy="",  # real executor acknowledgements have no dispatch entropy
    )
    claim(h.store, effect.effect_id)
    assert h.store.record_effect_outcome(
        effect.effect_id,
        "done",
        from_states=("claimed",),
        event=event,
        config=h.cfg,
    ).recorded
    refresh(h)
    return effect


def test_first_empty_search_schedules_another(tmp_path: Path):
    with_store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(with_store)
        complete_search(h)
        s = h.cur(P)
        assert s.lifecycle == Lifecycle.UNKNOWN
        assert s.create_searches == 1 and s.create_search_pending
        assert s.create_search_not_before_us - h.f(P).now == 60_000_000
    finally:
        with_store.close()


@pytest.mark.parametrize("pending_owner", [False, True])
def test_conclusion_releases_triage_and_pending_authority_can_start(tmp_path, pending_owner):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, create_id = unknown_triage(store)
        old = h.cur(P)
        assert project_note(h.p(P), h.p(P).bot) == (
            "Blocked: session create unconfirmed (checking Omnigent)"
        )
        if pending_owner:
            h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
            h.send(P, ev.RequestTriage(via=Via.DRAG))
            assert h.cur(P).session_id == old.session_id
        h.eligible(Q)
        h.send(Q, ev.RequestTriage(via=Via.DRAG))
        assert h.p(Q).current_session is None
        began = h.f(P).now
        for _ in range(4):
            complete_search(h)
        s = h.p(P).session(old.session_id)
        assert s is not None
        assert s.lifecycle == Lifecycle.RETIRED  # a pending successor retires settled history
        assert s.execution_closed and s.create_unconfirmed and not s.issuance_enabled
        assert h.f(P).now - began >= 600_000_000
        assert (P in h.admission.triage_runs) == pending_owner
        assert (
            store.query("SELECT state FROM effects WHERE effect_id = ?", (create_id,))[0][0]
            == "failed"
        )
        if pending_owner:
            assert h.cur(P).nonce != old.nonce
            assert h.cur(P).session_id != old.session_id
            assert project_note(h.p(P), h.p(P).bot) == "Starting: retrying session start"
        else:
            assert "did not confirm the session" in project_note(h.p(P), h.p(P).bot)
            r = h.send(Q, ev.ReconcileDue())
            assert h.of(r, EffectKind.CREATE_SESSION)
    finally:
        store.close()


def test_late_match_during_search_is_adopted(tmp_path):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, create_id = unknown_triage(store)
        complete_search(h)
        complete_search(h, matches=1, root="late-root")
        assert h.cur(P).root_id == "late-root"
        assert not h.cur(P).create_search_pending
        assert not h.cur(P).create_unconfirmed
        assert (
            store.query("SELECT state FROM effects WHERE effect_id = ?", (create_id,))[0][0]
            == "done"
        )
    finally:
        store.close()


@pytest.mark.parametrize("observation", [False, True])
def test_late_root_after_conclusion_is_archived_never_prepared(tmp_path, observation):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store)
        old = h.cur(P)
        for _ in range(4):
            complete_search(h)
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        h.send(P, ev.RequestTriage(via=Via.DRAG))
        fresh = h.cur(P)
        assert fresh.nonce != old.nonce
        assert h.f(P).now < h.p(P).session(old.session_id).create_watch_until_us
        if observation:
            h.send(P, ev.SessionCreated(old.session_id, "late-root", old.nonce))
        else:
            complete_search(h, matches=1, root="late-root")
        assert h.cur(P).session_id == fresh.session_id and h.cur(P).root_id is None
        assert h.p(P).issue_session is None
        old = h.p(P).session(old.session_id)
        assert old is not None and old.execution_closed and old.root_id == "late-root"
        effects = [e.effect for e in store.pending_effects(now_us=2**62)]
        assert any(
            e.kind == EffectKind.CLOSE_SESSION and e.args["root_id"] == "late-root" for e in effects
        )
        assert not any(e.kind == EffectKind.PREPARE_SESSION for e in effects)
    finally:
        store.close()


def test_match_seen_or_stale_ack_cannot_conclude_absence(tmp_path):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store)
        complete_search(h, matches=1)  # an unverified root is still evidence of existence
        s = h.cur(P)
        stale = h.send(P, ev.AdoptionResult(s.session_id, 0, None, s.nonce, search=1))
        assert not stale.audit.accepted and stale.audit.reason == "stale-create-search"
        for _ in range(4):
            complete_search(h)
        assert h.cur(P).lifecycle == Lifecycle.UNKNOWN
        assert not h.cur(P).create_unconfirmed
    finally:
        store.close()


def test_restart_preserves_budget_deadline_and_single_pending_search(tmp_path):
    path = tmp_path / "state.db"
    store = SqliteStore.open(path, FakeClock())
    h, _ = unknown_triage(store)
    complete_search(h)
    before = h.cur(P)
    store.close()
    store = SqliteStore.open(path, FakeClock())
    try:
        h.store = store
        refresh(h)
        h.send(P, ev.ReconcileDue())
        assert h.cur(P) == before
        for _ in range(3):
            complete_search(h)
        assert h.cur(P).create_unconfirmed
    finally:
        store.close()


def test_unknown_projection_does_not_use_stale_failure_note(tmp_path):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store)
        parcel = replace(h.p(P), note="Blocked: session failed")
        assert (
            project_note(parcel, parcel.bot)
            == "Blocked: session create unconfirmed (checking Omnigent)"
        )
    finally:
        store.close()


def test_conclusion_releases_build_reservations(tmp_path):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h = store_harness(store)
        h.eligible(P)
        assert h.send(P, ev.WaivePlan(via=Via.DRAG)).audit.accepted
        admitted = h.send(P, ev.CapacityAvailable())
        [create] = h.of(admitted, EffectKind.CREATE_SESSION)
        s = h.cur(P)
        claim(store, create.effect_id)
        assert store.record_effect_outcome(
            create.effect_id,
            "unknown",
            from_states=("claimed",),
            event=h.f(P).make(ev.EffectUnknown(create.effect_id, "create_session", s.session_id)),
            config=h.cfg,
        ).recorded
        refresh(h)
        assert h.admission.building_count == 1
        for _ in range(4):
            complete_search(h)
        assert h.admission.building_count == 0
        assert not any(r.live and r.parcel_id == P for r in h.admission.reservations)
        assert h.cur(P).execution_closed
    finally:
        store.close()


def test_empty_adoption_emits_followup_regression(tmp_path):
    """Uses only the pre-fix API: base fails because zero matches emitted no search."""
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store)
        s = h.cur(P)
        result = h.send(P, ev.AdoptionResult(session_id=s.session_id, matches=0, nonce=s.nonce))
        assert h.of(result, EffectKind.RECONCILE_SESSION), (
            "empty adoption must schedule another nonce search"
        )
    finally:
        store.close()


def test_stale_positive_search_still_prevents_absence_conclusion(tmp_path):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store)
        complete_search(h)
        s = h.cur(P)
        result = h.send(P, ev.AdoptionResult(s.session_id, matches=2, nonce=s.nonce, search=1))
        assert result.audit.accepted and h.cur(P).create_match_seen
        for _ in range(4):
            complete_search(h)
        assert h.cur(P).lifecycle == Lifecycle.UNKNOWN
    finally:
        store.close()


@pytest.mark.parametrize("fence_inflight", [False, True])
def test_fenced_inflight_create_searches_to_fenced_and_releases_slot(tmp_path, fence_inflight):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store, fence_inflight=fence_inflight)
        if not fence_inflight:
            h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        assert h.cur(P).lifecycle == (Lifecycle.DRAINING if fence_inflight else Lifecycle.UNKNOWN)
        assert (
            project_note(h.p(P), h.p(P).bot)
            == "Blocked: session create unconfirmed (checking Omnigent)"
        )
        for _ in range(4):
            complete_search(h)
        s = h.cur(P)
        assert s.lifecycle == Lifecycle.FENCED and s.execution_closed and s.create_unconfirmed
        assert P not in h.admission.triage_runs and not s.issuance_enabled
        h.eligible(Q)
        result = h.send(Q, ev.RequestTriage(via=Via.DRAG))
        assert h.of(result, EffectKind.CREATE_SESSION)
    finally:
        store.close()


@pytest.mark.parametrize("rejection", ["early", "stale", "nonce"])
def test_stranded_search_flag_recovers_from_rejected_ack_on_reconcile(tmp_path, rejection):
    path = tmp_path / "state.db"
    store = SqliteStore.open(path, FakeClock())
    h, _ = unknown_triage(store)
    complete_search(h)
    s = h.cur(P)
    [pending] = [
        e
        for e in store.pending_effects(now_us=2**62)
        if e.effect.kind == EffectKind.RECONCILE_SESSION
    ]
    effect = pending.effect
    body = ev.AdoptionResult(
        s.session_id,
        nonce="wrong" if rejection == "nonce" else s.nonce,
        search=1 if rejection == "stale" else 2,
    )
    claim(store, effect.effect_id)
    event = h.f(P).make(body, event_id=f"effect:{effect.effect_id}:ack")
    assert store.record_effect_outcome(
        effect.effect_id,
        "done",
        from_states=("claimed",),
        event=event,
        config=h.cfg,
    ).recorded
    refresh(h)
    assert h.cur(P).create_search_pending and h.cur(P).create_searches == 1
    assert not [
        e
        for e in store.pending_effects(now_us=2**62)
        if e.effect.kind == EffectKind.RECONCILE_SESSION
    ]
    store.close()
    store = SqliteStore.open(path, FakeClock())
    try:
        h.store = store
        result = h.send(P, ev.ReconcileDue())
        [requeued] = h.of(result, EffectKind.RECONCILE_SESSION)
        assert requeued.args["search"] == 2
        assert requeued.args["not_before_us"] == s.create_search_not_before_us
        assert not h.of(h.send(P, ev.ReconcileDue()), EffectKind.RECONCILE_SESSION)
        for _ in range(3):
            complete_search(h)
        assert h.cur(P).create_unconfirmed and P not in h.admission.triage_runs
    finally:
        store.close()


@pytest.mark.parametrize("claimed", [False, True])
def test_reconcile_does_not_duplicate_queued_or_inflight_search(tmp_path, claimed):
    store = SqliteStore.open(tmp_path / "state.db", FakeClock())
    try:
        h, _ = unknown_triage(store)
        [pending] = [
            e
            for e in store.pending_effects(now_us=2**62)
            if e.effect.kind == EffectKind.RECONCILE_SESSION
        ]
        if claimed:
            claim(store, pending.effect.effect_id)
        result = h.send(P, ev.ReconcileDue())
        assert not h.of(result, EffectKind.RECONCILE_SESSION)
        assert h.cur(P).create_search_pending
    finally:
        store.close()


@pytest.mark.parametrize("stop", ["closed", "done", "deadline"])
def test_retired_watch_stops_durably_and_queued_read_is_invalid(tmp_path, stop):
    path = tmp_path / "state.db"
    store = SqliteStore.open(path, FakeClock())
    h, _ = unknown_triage(store)
    for _ in range(4):
        complete_search(h)
    retired = h.cur(P)
    assert retired.create_watch_until_us == h.f(P).now + 7 * 24 * 3600_000_000
    [watch] = [
        e.effect
        for e in store.pending_effects(now_us=2**62)
        if e.effect.kind == EffectKind.RECONCILE_SESSION
    ]
    if stop == "closed":
        h.send(P, ev.Closed())
    elif stop == "done":
        h.send(P, ev.ColumnObserved(stage=Stage.DONE))
    else:
        # Preflight must end even a read retry without waiting for ReconcileDue.
        assert effect_still_valid(h.p(P), watch, now_us=retired.create_watch_until_us - 1) is None
        assert (
            effect_still_valid(h.p(P), watch, now_us=retired.create_watch_until_us)
            == "create-watch-ended"
        )
        h.f(P).tick(retired.create_watch_until_us - h.f(P).now)
        h.send(P, ev.ReconcileDue())
    assert h.cur(P).create_watch_stopped and not h.cur(P).create_search_pending
    assert effect_still_valid(h.p(P), watch, now_us=h.f(P).now) == "create-watch-ended"
    store.close()
    store = SqliteStore.open(path, FakeClock())
    try:
        h.store = store
        refresh(h)
        assert h.cur(P).create_watch_until_us == retired.create_watch_until_us
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        h.eligible(P)
        assert h.cur(P).create_watch_stopped
        assert not h.of(h.send(P, ev.ReconcileDue()), EffectKind.RECONCILE_SESSION)
    finally:
        store.close()
