"""Boot repairs pre-budget UNKNOWN creates without editing a live database."""

import json
import sqlite3

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import FakeClock, FakeOmnigent
from tests.test_store import P
from tests.test_unknown_create_research import complete_search, unknown_triage


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_search_state", ["done", "pending", "claimed"])
async def test_legacy_completed_empty_search_starts_budget_on_boot(
    service_config, legacy_search_state
):
    path = service_config.database_path
    store = SqliteStore.open(path, FakeClock())
    h, _ = unknown_triage(store)
    complete_search(h)
    store.close()
    # Exact old JSON shape: no search fields, original create UNKNOWN, search done.
    with sqlite3.connect(path) as raw:
        data = json.loads(
            raw.execute("SELECT aggregate_json FROM parcels WHERE parcel_id = ?", (P,)).fetchone()[
                0
            ]
        )
        for session in data["parcel"]["sessions"]:
            for key in list(session):
                if key.startswith("create_"):
                    del session[key]
        raw.execute(
            "UPDATE parcels SET aggregate_json = ? WHERE parcel_id = ?", (json.dumps(data), P)
        )
        raw.execute(
            "UPDATE effects SET state = ? WHERE kind = 'reconcile_session'", (legacy_search_state,)
        )
    service = FactoryService(service_config, clock=FakeClock(start_us=h.f(P).now))
    await service.open()
    try:
        parcel = await service.db.call(lambda s: s.load_parcel(P))
        assert parcel.current_session.create_search_pending
        searches = await service.db.call(
            lambda s: [
                e
                for e in s.pending_effects(now_us=2**62)
                if e.effect.kind == EffectKind.RECONCILE_SESSION
            ]
        )
        assert len(searches) == 1 and searches[0].effect.args["search"] == 1
        # Boot is idempotent with respect to the new persisted pending search.
        await service._reconcile(P)
        again = await service.db.call(
            lambda s: [
                e
                for e in s.pending_effects(now_us=2**62)
                if e.effect.kind == EffectKind.RECONCILE_SESSION
            ]
        )
        assert [e.effect.effect_id for e in again] == [e.effect.effect_id for e in searches]
    finally:
        await service.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["closed", "deadline"])
async def test_executor_cancels_queued_retired_watch_before_remote_call(service_config, stop):
    store = SqliteStore.open(service_config.database_path, FakeClock())
    h, _ = unknown_triage(store)
    for _ in range(4):
        complete_search(h)
    deadline = h.cur(P).create_watch_until_us
    [watch] = [
        e.effect
        for e in store.pending_effects(now_us=2**62)
        if e.effect.kind == EffectKind.RECONCILE_SESSION
    ]
    if stop == "closed":
        h.send(P, ev.Closed())
    now = deadline if stop == "deadline" else h.f(P).now
    store.close()
    omnigent = FakeOmnigent()
    service = FactoryService(service_config, clock=FakeClock(start_us=now), adapters=(omnigent,))
    await service.open()
    try:
        # Foreground execution of just this queued read, after reopening the database.
        await service.executor._execute(watch.effect_id)
        effect = await service.db.call(lambda s: s.get_effect(watch.effect_id))
        assert effect.state == "cancelled"
        assert not omnigent.executed(EffectKind.RECONCILE_SESSION)
        parcel = await service.db.call(lambda s: s.load_parcel(P))
        assert parcel.current_session.create_watch_until_us == deadline
        assert parcel.current_session.create_watch_stopped
        assert not parcel.current_session.create_search_pending
    finally:
        await service.stop()
