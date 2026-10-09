"""The factory re-reads only what can have changed (owner-approved items A-H).

A: per-issue reads only for live parcels; a board diff catches missed webhooks.
B: an unchanged periodic issue read is not stored; superseded issue bodies become hashes.
C: owner-parked trees on the settled cadence; one inventory refresh per observer pass.
D: boot reads only live parcels; periodic reads are spread, never one burst.
E: the clock loop loads only parcels with a clocked run.
F: bodies of deliveries that produced no event go at once; old pruned rows are deleted.
G: routine lines are DEBUG.
H: the GitHub client keeps its connections.
Plus: a Done parcel's hard-fenced run is closed (never rescanned forever).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import event_from_json
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.predicates import run_closed
from omnigent_factory.core.types import (
    Hold,
    Lifecycle,
    Parcel,
    Via,
    WaitReason,
)
from omnigent_factory.omnigent.inventory import DEFAULT_RESYNC_S, SessionIndex
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.service.board_diff import BoardDiff
from omnigent_factory.service.composition import (
    GITHUB_HTTP_LIMITS,
    ProductionRuntime,
    github_http_client,
)
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.redaction import demote_routine_library_logs
from omnigent_factory.service.runtime import (
    RECONCILE_TICKS_PER_INTERVAL,
    FactoryService,
    prune_history,
    reconcile_live,
)
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.fakes import FakeClock, FakeCredentialBroker, FakeOmnigent
from omnigent_factory.testing.harness import Harness

from .quiet_support import (
    CountingGitHub,
    CountingOmnigent,
    LineCounter,
    Population,
    build_population,
    card_for,
    populate,
)

DAY_US = 86_400 * 1_000_000


async def eventually(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < end, "condition was not reached"
        await asyncio.sleep(0.01)


@dataclass
class Rig:
    service: FactoryService
    github: CountingGitHub
    population: Population

    async def parcel(self, parcel_id: str) -> Parcel:
        found = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        assert found is not None
        return found

    def idle(self) -> list[str]:
        return sorted(set(self.population.parcels) - self.population.live)


def _seed(config: ServiceConfig, *, digests: bool) -> tuple[Population, CountingGitHub]:
    harness, population = build_population()
    store = SqliteStore.open(config.database_path, SystemClock())
    try:
        store.ensure_repository(config.trusted)
        populate(store, harness)
        # The history's effects all ran long ago.
        store.query("UPDATE effects SET state = 'done'")
        github = CountingGitHub(cards=[card_for(p) for p in population.parcels.values()])
        if digests:  # the daemon compared the board before (steady state)
            store.save_board_digests({card.node_id: card.digest for card in github.cards})
    finally:
        store.close()
    return population, github


@asynccontextmanager
async def started(
    config: ServiceConfig, *, digests: bool = True, board_diff: bool = True
) -> AsyncIterator[Rig]:
    population, github = _seed(config, digests=digests)
    service = FactoryService(
        config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=SystemClock(),
    )
    if board_diff:
        service.board_diff = BoardDiff(service, github.board_cards)
    await service.start()
    try:
        yield Rig(service, github, population)
    finally:
        await service.stop()


# ============================================================== A: live-only reads


@pytest.mark.asyncio
async def test_reconcile_reads_only_live_parcels(service_config: ServiceConfig) -> None:
    config = service_config.model_copy(
        update={"reconcile_interval_seconds": 0.08, "board_diff_interval_minutes": 60}
    )
    async with started(config) as rig:
        live = rig.population.live
        assert len(live) == 6 and len(rig.population.parcels) == 140
        await eventually(lambda: all(rig.github.reads[pid] >= 2 for pid in live))
        # Retired, Inbox and Done parcels: never read per issue (the board shows no change).
        assert set(rig.github.reads) == set(live)
        assert rig.github.board_reads == 1


@pytest.mark.asyncio
async def test_board_diff_applies_a_missed_close_and_a_missed_label_within_one_interval(
    service_config: ServiceConfig,
) -> None:
    interval_s = 0.3
    config = service_config.model_copy(
        update={
            "reconcile_interval_seconds": 0.08,
            "board_diff_interval_minutes": interval_s / 60,
        }
    )
    async with started(config) as rig:
        closed, labelled = rig.idle()[0], rig.idle()[1]
        await eventually(lambda: rig.github.board_reads >= 1)
        assert not rig.github.reads[closed] and not rig.github.reads[labelled]
        # Webhooks lost: the issue was closed, another one labelled.
        cards = {card.node_id: card for card in rig.github.cards}
        cards[closed] = replace(cards[closed], open=False, updated_at="2026-10-09T01:00:00Z")
        cards[labelled] = replace(cards[labelled], labels=("factory:plan",))
        rig.github.cards = list(cards.values())
        rig.github.snapshots[closed] = snapshot(open=False, read_at_us=1)
        changed_at = asyncio.get_running_loop().time()
        await eventually(lambda: rig.github.reads[closed] >= 1 and rig.github.reads[labelled] >= 1)
        assert asyncio.get_running_loop().time() - changed_at <= interval_s + 1.0
        parcel = await rig.parcel(closed)
        end = asyncio.get_running_loop().time() + 3
        while Hold.COMPLETED not in parcel.holds:  # the missed close is applied
            assert asyncio.get_running_loop().time() < end
            await asyncio.sleep(0.02)
            parcel = await rig.parcel(closed)
        others = set(rig.github.reads) - rig.population.live - {closed, labelled}
        assert others == set()


@pytest.mark.asyncio
async def test_first_board_diff_reads_each_unknown_parcel_once(
    service_config: ServiceConfig,
) -> None:
    config = service_config.model_copy(
        update={"reconcile_interval_seconds": 0.4, "board_diff_interval_minutes": 0.25 / 60}
    )
    async with started(config, digests=False) as rig:
        idle = rig.idle()
        await eventually(lambda: all(rig.github.reads[pid] >= 1 for pid in idle))
        await asyncio.sleep(0.8)  # more diffs: nothing changed, nothing more is read
        assert all(rig.github.reads[pid] == 1 for pid in idle)
        stored = await rig.service.db.call(lambda store: store.board_digests())
        assert set(stored) == set(rig.population.parcels)


def test_reconcile_live_covers_unverified_readiness() -> None:
    h = Harness()
    h.to_building()
    h.build_ready()
    p = h.p()
    assert p.readiness is not None
    kwargs: dict[str, Any] = {"unsettled_effects": frozenset(), "now_us": 0}
    assert not reconcile_live(p, admission=h.admission, **kwargs)  # Ready, verified: idle
    unverified = replace(p, readiness=replace(p.readiness, verified=False))
    assert reconcile_live(unverified, admission=h.admission, **kwargs)
    merge_unknown = replace(p, readiness=replace(p.readiness, merge_unknown=True))
    assert reconcile_live(merge_unknown, admission=h.admission, **kwargs)
    idle = Harness()
    idle.eligible()
    assert not reconcile_live(idle.p(), admission=idle.admission, **kwargs)
    assert reconcile_live(
        idle.p(),
        admission=idle.admission,
        unsettled_effects=frozenset({idle.p().parcel_id}),
        now_us=0,
    )


# ============================================================ B: unchanged reads


def _read_ack(h: Harness, store: SqliteStore, *, body: str) -> tuple[str, Any]:
    pid = "I_parcel_1"
    f = h.f(pid)
    due = store.apply_event(
        f.make(ev.ReconcileDue(), provenance=Provenance.SCHEDULER, evidence=None), h.cfg
    )
    (read,) = [e for e in due.effects if e.kind == EffectKind.RECONCILE_PARCEL]
    store.query("UPDATE effects SET state = 'claimed' WHERE effect_id = ?", (read.effect_id,))
    ack = Event(
        event_id=f"effect:{read.effect_id}:ack",
        repo_id=h.cfg.repo_id,
        parcel_id=pid,
        source_time_us=f.now,
        provenance=Provenance.ADAPTER,
        body=ev.GitHubSnapshot(),
        evidence=snapshot(read_at_us=f.now, body=body),
    )
    outcome = store.record_effect_outcome(
        read.effect_id, "done", from_states=("claimed",), event=ack, config=h.cfg
    )
    return read.effect_id, outcome


def _snapshot_events(store: SqliteStore) -> list[Event]:
    return [
        event_from_json(str(row[0]))
        for row in store.query(
            "SELECT payload_json FROM events WHERE kind = ? ORDER BY sequence",
            (ev.EventKind.GITHUB_SNAPSHOT.value,),
        )
    ]


def test_unchanged_issue_read_is_not_stored_and_superseded_bodies_become_hashes(
    tmp_path: Path,
) -> None:
    store = SqliteStore.open(tmp_path / "s.db", FakeClock())
    h = Harness()
    store.ensure_repository(h.cfg)
    eligible = h.f().make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1))
    store.apply_event(eligible, h.cfg)
    audit = len(store.audit_entries("I_parcel_1"))

    effect_id, outcome = _read_ack(h, store, body="Body text")  # same issue as stored
    assert outcome.applied is not None and not outcome.applied.persisted
    assert len(_snapshot_events(store)) == 1  # not stored
    assert not store.query("SELECT 1 FROM effects WHERE effect_id = ?", (effect_id,))
    assert len(store.audit_entries("I_parcel_1")) == audit + 1  # the wake-up only

    effect_id, outcome = _read_ack(h, store, body="Edited body")
    assert outcome.applied is not None and outcome.applied.persisted
    first, newest = _snapshot_events(store)
    assert newest.evidence is not None and newest.evidence.body == "Edited body"
    # The older read keeps its values and a body hash; only the newest text is read back.
    assert first.evidence is not None and first.evidence.body is None
    assert first.evidence.body_sha256 is not None
    assert store.query("SELECT state FROM effects WHERE effect_id = ?", (effect_id,))[0][0] == (
        "done"
    )
    # Unchanged against the newest stored read (its body is still the edited one).
    _effect, outcome = _read_ack(h, store, body="Edited body")
    assert outcome.applied is not None and not outcome.applied.persisted
    store.close()


# ============================================================== C: observer


def _waiting_on_owner() -> Parcel:
    h = Harness()
    h.to_building()
    p = h.p()
    cur = p.current_session
    assert cur is not None
    return replace(
        p,
        sessions=tuple(
            replace(s, lifecycle=Lifecycle.WAITING, wait_reason=WaitReason.DECISION)
            if s.session_id == cur.session_id
            else s
            for s in p.sessions
        ),
    )


def _observer(adapter: CountingOmnigent, clock: FakeClock, parcels: list[Parcel]) -> Any:
    observer = OmnigentObserver(
        None,  # type: ignore[arg-type]
        adapter,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        clock,
        interval_seconds=5,
        settled_interval_seconds=60,
    )

    async def load() -> list[Parcel]:
        return parcels

    async def observe(parcel: Parcel, *_: Any) -> None:
        cur = parcel.current_session
        assert cur is not None
        observer._last_tree[cur.session_id] = (True, False, False)  # complete, idle

    async def nothing(*_: Any) -> None:
        return None

    observer._parcels = load
    observer._observe = observe
    observer._activity = nothing
    return observer


@pytest.mark.asyncio
async def test_a_waiting_tree_parked_on_the_owner_is_read_on_the_parked_cadence() -> None:
    parked = _waiting_on_owner()
    clock = FakeClock()
    adapter = CountingOmnigent()
    observer = _observer(adapter, clock, [parked])
    for _ in range(60):  # 300 s of 5 s passes
        await observer.observe_once()
        clock.advance(5_000_000)
    assert sum(adapter.scans.values()) == 2  # first read, then once every five minutes
    # Not parked any more (working): every pass again.
    working = replace(
        parked,
        sessions=tuple(replace(s, lifecycle=Lifecycle.ACTIVE) for s in parked.sessions),
    )
    observer._parcels = _observer(adapter, clock, [working])._parcels
    for _ in range(3):
        await observer.observe_once()
    assert sum(adapter.scans.values()) == 5


@pytest.mark.asyncio
async def test_one_inventory_refresh_per_observer_pass() -> None:
    parcels = []
    for i in range(3):
        h = Harness()
        h.f(f"I_tree_{i}")
        h.eligible(f"I_tree_{i}")
        h.send(f"I_tree_{i}", ev.RequestPlan(via=Via.DRAG))
        h.create_ok(f"I_tree_{i}")
        parcels.append(h.p(f"I_tree_{i}"))
    adapter = CountingOmnigent()
    observer = _observer(adapter, FakeClock(), parcels)
    await observer.observe_once()
    assert sum(adapter.scans.values()) == 3
    assert adapter.inventory_reads == 1


def test_inventory_full_resync_defaults_to_six_hours() -> None:
    index = SessionIndex(None)  # type: ignore[arg-type]
    assert DEFAULT_RESYNC_S == 6 * 3600 and index._resync_us == 6 * 3600 * 1_000_000
    index.set_resync(60)
    assert index._resync_us == 60_000_000
    assert ServiceConfig.model_fields["session_inventory_resync_hours"].default == 6.0
    assert {
        "session_inventory_resync_hours",
        "board_diff_interval_minutes",
        "delivery_row_retention_days",
    } <= HOT_RELOAD_KEYS


# ======================================================================= D: boot


@pytest.mark.asyncio
async def test_boot_reads_only_live_parcels_and_defers_their_next_read(
    service_config: ServiceConfig,
) -> None:
    config = service_config.model_copy(update={"reconcile_interval_seconds": 3600})
    async with started(config) as rig:
        runtime = ProductionRuntime(
            service=rig.service,
            broker=None,  # type: ignore[arg-type]
            broker_server=None,  # type: ignore[arg-type]
            observer=None,  # type: ignore[arg-type]
            github=rig.github,  # type: ignore[arg-type]
            workspaces=None,  # type: ignore[arg-type]
            config=config,
            github_http=None,  # type: ignore[arg-type]
            omnigent=None,  # type: ignore[arg-type]
            omnigent_adapter=None,  # type: ignore[arg-type]
        )
        rig.github.reads.clear()
        observed = await runtime._github_reconcile()
        assert observed == rig.population.live
        assert set(rig.github.reads) == set(rig.population.live)
        loop_now = asyncio.get_running_loop().time()
        for pid in rig.population.live:  # boot read them: next read a whole interval away
            due, period = rig.service._reconcile_due[pid]
            assert period == 3600 and due > loop_now + 3000


@pytest.mark.asyncio
async def test_periodic_reads_are_spread_over_the_interval(service_config: ServiceConfig) -> None:
    interval = 0.4
    config = service_config.model_copy(update={"reconcile_interval_seconds": interval})
    service = FactoryService(config, clock=SystemClock())
    ids = frozenset(f"I_many_{i:03d}" for i in range(40))
    ticks: list[list[str]] = []

    async def live() -> tuple[frozenset[str], frozenset[str]]:
        return ids, frozenset()

    async def reconcile(parcel_id: str, **_: Any) -> None:
        ticks[-1].append(parcel_id)

    service.live_parcel_ids = live  # type: ignore[method-assign]
    service._reconcile = reconcile  # type: ignore[method-assign]
    for _ in range(RECONCILE_TICKS_PER_INTERVAL + 1):
        ticks.append([])
        await service._reconcile_tick()
        await asyncio.sleep(interval / RECONCILE_TICKS_PER_INTERVAL)
    first_round = [pid for tick in ticks for pid in tick]
    assert sorted(first_round) == sorted(ids)  # each once within the interval
    assert max(len(tick) for tick in ticks) < len(ids) // 2  # never one burst


# ======================================================================= E: clock


@pytest.mark.asyncio
async def test_clock_loop_loads_only_parcels_with_a_clocked_run(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = service_config.model_copy(
        update={"reconcile_interval_seconds": 3600, "clock_interval_seconds": 0.01}
    )
    loads: list[str] = []
    original = SqliteStore.load_parcel

    def counting(self: SqliteStore, parcel_id: str) -> Parcel | None:
        loads.append(parcel_id)
        return original(self, parcel_id)

    async with started(config, board_diff=False) as rig:
        await asyncio.sleep(0.2)  # boot and the first reconcile tick are done
        monkeypatch.setattr(SqliteStore, "load_parcel", counting)
        await asyncio.sleep(0.3)  # ~30 clock ticks
        clocked = {
            pid
            for pid, p in rig.population.parcels.items()
            if any(s.lifecycle in (Lifecycle.ACTIVE, Lifecycle.WAITING) for s in p.sessions)
        }
        assert clocked and set(loads) <= clocked | rig.population.live
        assert len(loads) < 30 * 20  # not 140 parcels a tick


# =================================================================== F: deliveries


@pytest.mark.asyncio
async def test_ignored_delivery_body_is_cleared_when_processed(
    service_config: ServiceConfig,
) -> None:
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    try:
        record = DeliveryRecord("d-ignored", "issues", b'{"big":1}', {"x-github-event": "x"})
        await service.persist_delivery(record)
        await service.ignore_delivery("d-ignored")
        row = (
            await service.db.call(
                lambda store: store.query(
                    "SELECT status, body, headers_json, body_sha256 FROM deliveries"
                )
            )
        )[0]
        assert (row[0], bytes(row[1]), row[2]) == ("processed", b"", "{}")
        assert row[3] == record.body_sha256  # duplicate/recovery matching still works
    finally:
        await service.stop()


def test_delivery_row_retention_keeps_referenced_parked_and_held_rows(tmp_path: Path) -> None:
    clock = FakeClock()
    store = SqliteStore.open(tmp_path / "s.db", clock)
    h = Harness()
    store.ensure_repository(h.cfg)
    for guid in ("gone", "event", "parked", "held", "unpruned", "pending"):
        store.append_delivery(DeliveryRecord(guid, "issues", b"{}", {}))
        store.append_delivery(DeliveryRecord(guid, "issues", b"{}", {}))  # a duplicate
        if guid != "pending":
            store.mark_delivery(guid, "processed")
    store.append_delivery(DeliveryRecord("event", "issues", b"{1}", {}))  # quarantined copy
    event = h.f().make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1))
    store.apply_event(replace(event, delivery_guid="event"), h.cfg)
    store.query(
        "INSERT INTO parked_deliveries (delivery_guid, parcel_id, parked_at_us) "
        "VALUES ('parked', NULL, 0)"
    )
    store.query(
        "UPDATE deliveries SET body = X'', body_pruned_at_us = 1 WHERE delivery_guid != 'unpruned'"
    )
    clock.advance(15 * DAY_US)
    store.append_delivery(DeliveryRecord("recent", "issues", b"{}", {}))
    store.mark_delivery("recent", "processed")
    store.query("UPDATE deliveries SET body_pruned_at_us = 1 WHERE delivery_guid = 'recent'")
    before = clock.now_utc_us() - 14 * DAY_US
    # Rows: only "gone". Attempts: "gone" and "event" (row kept for its event; the
    # attempts of a fully pruned delivery are not needed), never a quarantined copy.
    assert store.prune_delivery_rows(before, ["held"], dry_run=True) == (1, 4)
    assert store.prune_delivery_rows(before, ["held"]) == (1, 4)
    rows = {str(r[0]) for r in store.query("SELECT delivery_guid FROM deliveries")}
    assert rows == {"event", "parked", "held", "unpruned", "pending", "recent"}
    attempts = {
        (str(r[0]), str(r[1]))
        for r in store.query("SELECT delivery_guid, outcome FROM delivery_attempts")
    }
    assert ("gone", "inserted") not in attempts and ("event", "inserted") not in attempts
    assert ("event", "quarantined") in attempts
    for kept in ("parked", "held", "unpruned", "pending"):
        assert (kept, "duplicate") in attempts
    store.close()


@pytest.mark.asyncio
async def test_history_sweep_reports_delivery_rows(service_config: ServiceConfig) -> None:
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(service_config.trusted)
    store.append_delivery(DeliveryRecord("old", "issues", b"{}", {}))
    store.retire_delivery("old")
    store.close()
    clock.advance(15 * DAY_US)
    service = FactoryService(service_config, clock=clock)
    await service.start()
    try:
        result = await prune_history(service.db.call, service_config, clock.now_utc_us())
        assert result["delivery_rows"] == 1 and result["delivery_attempts"] == 1
    finally:
        await service.stop()


# ====================================================================== G: logging


def test_transitions_that_change_nothing_are_debug(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = SqliteStore.open(tmp_path / "s.db", FakeClock())
    h = Harness()
    store.ensure_repository(h.cfg)
    store.apply_event(h.f().make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=1)), h.cfg)
    store.apply_event(h.f().make(ev.Unpause(), parcel_id=None), h.cfg)
    caplog.set_level(logging.DEBUG, logger="omnigent_factory.store.sqlite")
    store.apply_event(
        h.f().make(ev.ReconcileDue(), provenance=Provenance.SCHEDULER, evidence=None), h.cfg
    )
    store.apply_event(h.f().make(ev.RequestTriage(via=Via.DRAG)), h.cfg)  # a real change
    lines = {
        r.getMessage().split(" ")[2]: r.levelno
        for r in caplog.records
        if r.getMessage().startswith("parcel transition")
    }
    assert lines == {"kind=ReconcileDue": logging.DEBUG, "kind=RequestTriage": logging.INFO}
    store.close()


@pytest.mark.asyncio
async def test_read_effects_log_at_debug_and_writes_at_info(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
) -> None:
    config = service_config.model_copy(
        update={"reconcile_interval_seconds": 0.05, "board_diff_interval_minutes": 60}
    )
    caplog.set_level(logging.DEBUG, logger="omnigent_factory.service.executor")
    async with started(config) as rig:
        await eventually(lambda: sum(rig.github.reads.values()) >= 3)
        reads = [r for r in caplog.records if "kind=reconcile_parcel" in r.getMessage()]
        assert reads and {r.levelno for r in reads} == {logging.DEBUG}


def test_mcp_terminating_session_lines_are_debug(caplog: pytest.LogCaptureFixture) -> None:
    demote_routine_library_logs()
    logger = logging.getLogger("mcp.server.streamable_http")
    caplog.set_level(logging.INFO)
    logger.info("Terminating session: %s", "abc")
    logger.info("Something else")
    assert [r.getMessage() for r in caplog.records] == ["Something else"]
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="mcp.server.streamable_http")
    logger.info("Terminating session: %s", "abc")
    assert [r.levelno for r in caplog.records] == [logging.DEBUG]


# ==================================================================== H: GitHub HTTP


@pytest.mark.asyncio
async def test_github_client_keeps_idle_connections_and_bounds_the_pool() -> None:
    client = github_http_client()
    try:
        pool: Any = client._transport._pool  # type: ignore[attr-defined]
        # httpx's default (5 s) dropped every connection between spaced-out reads.
        assert pool._keepalive_expiry == 120.0
        # Four is plenty for one repository (idle GitHub usage is a few reads an hour).
        assert pool._max_connections == 4 == GITHUB_HTTP_LIMITS.max_keepalive_connections
        assert pool._max_keepalive_connections == 4
        assert client.timeout.pool == 60.0
    finally:
        await client.aclose()


# ============================================================ FENCED Done parcels


def test_a_closed_issue_closes_its_fenced_run() -> None:
    h = Harness()
    h.eligible()
    h.send("I_parcel_1", ev.RequestPlan(via=Via.DRAG))
    run = h.create_ok()
    f = h.f()
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(open=False, read_at_us=f.now)))
    h.quiesce("I_parcel_1", run.session_id)
    s = h.cur()
    assert Hold.COMPLETED in h.p().holds
    assert s.lifecycle == Lifecycle.FENCED and s.execution_closed


def test_a_stored_fenced_done_run_is_neither_scanned_nor_reconciled() -> None:
    h = Harness()
    h.eligible()
    h.send("I_parcel_1", ev.RequestPlan(via=Via.DRAG))
    run = h.create_ok()
    f = h.f()
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(open=False, read_at_us=f.now)))
    h.quiesce("I_parcel_1", run.session_id)
    issue = h.p().issue_session
    assert issue is not None
    h.send("I_parcel_1", ev.IssueSessionClosed(root_id=issue.root_id))
    # Stored before the reducer closed such runs.
    legacy = replace(
        h.p(), sessions=tuple(replace(s, execution_closed=False) for s in h.p().sessions)
    )
    cur = legacy.current_session
    assert cur is not None and cur.lifecycle == Lifecycle.FENCED and not cur.execution_closed
    assert run_closed(legacy, cur)
    assert not reconcile_live(
        legacy, admission=h.admission, unsettled_effects=frozenset(), now_us=0
    )
    # A checkpoint-only FENCED run can resume: still watched.
    resumable = replace(
        legacy,
        holds=legacy.holds - {Hold.COMPLETED},
        sessions=tuple(replace(s, fences=frozenset()) for s in legacy.sessions),
    )
    resumable_cur = resumable.current_session
    assert resumable_cur is not None and not run_closed(resumable, resumable_cur)


# ================================================================ measured load


@pytest.mark.asyncio
async def test_ten_minutes_of_load_scale_with_live_work_not_parcels(
    service_config: ServiceConfig,
) -> None:
    """10 simulated minutes at 1:100 (6 s) with 140 parcels, 6 of them live.

    Before (base, same simulation at 1:20): ~460 per-issue GitHub reads (~440 of them
    for parcels with nothing live), ~420 tree scans each with its own inventory read,
    ~1,650 INFO lines. Now: per-issue reads only for live parcels, one inventory read
    per observer pass, and almost no INFO lines.
    """
    scale = 100.0
    config = service_config.model_copy(
        update={
            "reconcile_interval_seconds": 120 / scale,
            "board_diff_interval_minutes": 5 / scale,
            "clock_interval_seconds": 1 / scale,
            "observation_interval_seconds": 5 / scale,
            "settled_observation_interval_seconds": 60 / scale,
        }
    )
    lines = LineCounter()
    root = logging.getLogger()
    level = root.level
    try:
        async with started(config) as rig:
            root.addHandler(lines)  # the daemon's lines, not the seeded history's
            root.setLevel(logging.DEBUG)
            omnigent = CountingOmnigent()
            observer = OmnigentObserver(
                rig.service,
                omnigent,  # type: ignore[arg-type]
                None,  # type: ignore[arg-type]
                SystemClock(),
                interval_seconds=config.observation_interval_seconds,
                settled_interval_seconds=config.settled_observation_interval_seconds,
            )

            async def no_activity(*_: Any) -> None:
                return None

            observer._activity = no_activity  # type: ignore[method-assign]
            await observer.start()
            await asyncio.sleep(600 / scale)
            await observer.close()
    finally:
        root.removeHandler(lines)
        root.setLevel(level)
    live = rig.population.live
    reads = rig.github.reads
    assert set(reads) <= live  # nothing idle is read per issue
    assert sum(reads.values()) <= len(live) * (600 // 120 + 2)
    # Each pass refreshes the inventory at most once, whatever the number of trees.
    assert omnigent.inventory_reads <= 600 / 5 + 5
    assert set(omnigent.scans) <= {
        p.current_session.root_id
        for pid, p in rig.population.parcels.items()
        if pid in live and p.current_session is not None
    }
    assert lines.info < 60, lines.messages.most_common(5)
