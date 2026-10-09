"""Auto-build in the service: the owner's field webhook, start order by Rank, the switch,
budget and status, restart safety."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.types import AutoBuildStatus, Parcel, QueueStatus, Stage
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.ports.github import RankingCard
from omnigent_factory.service.auto_build import AutoBuilder, order_candidates
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from omnigent_factory.testing.harness import Harness

BOT = 334_191_208
AUTO_FIELD = "PVTSSF_auto"
QUEUED_OPTION, STARTED_OPTION = "opt_queued", "opt_started"


def rcard(pid: str, number: int, *, rank: float | None, created: int = 1) -> RankingCard:
    return RankingCard(
        node_id=pid,
        item_id=f"PVTI_{number}",
        number=number,
        title=f"Issue {number}",
        stage=Stage.SCOPED,
        created_at_us=created,
        rank=rank,
        priority=None,
    )


# ------------------------------------------------------------------ order


def test_order_is_rank_ascending_then_oldest_with_unranked_last():
    parcels = [Parcel(parcel_id=f"I_{n}", repo_id="R", issue_number=n) for n in range(1, 6)]
    cards = {
        "I_1": rcard("I_1", 1, rank=None, created=10),
        "I_2": rcard("I_2", 2, rank=3.0, created=50),
        "I_3": rcard("I_3", 3, rank=1.0, created=90),
        "I_4": rcard("I_4", 4, rank=None, created=5),
        # I_5 has no card (board unread): last, by issue number
    }
    assert [c.number for c in order_candidates(parcels, cards)] == [3, 2, 4, 1, 5]


# ------------------------------------------------------------------ webhook


def _identity() -> DeliveryIdentity:
    return DeliveryIdentity(
        app_id=5085812,
        installation_id=165144097,
        organization_id=296340858,
        project_node_id="PVT_kwDOEanNes4BkJhb",
        status_field_node_id="PVTSSF_lADOEanNes4BkJhbzhi7I9w",
        repository_id=1278047766,
        repository_node_id="R_kgDOTC12Fg",
        repository_full_name="SA-Ambulance/timesheets",
        owner_ids=frozenset({OWNER_ID}),
        bot_user_id=BOT,
        auto_build_field_node_id=AUTO_FIELD,
        auto_build_option_ids={"Queued": QUEUED_OPTION, "Started": STARTED_OPTION},
    )


def field_payload(sender: int, change: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "action": "edited",
            "installation": {"id": 165144097},
            "organization": {"id": 296340858},
            "repository": {
                "id": 1278047766,
                "node_id": "R_kgDOTC12Fg",
                "full_name": "SA-Ambulance/timesheets",
            },
            "sender": {"id": sender, "type": "Bot" if sender == BOT else "User"},
            "projects_v2_item": {
                "node_id": "PVTI_1",
                "project_node_id": "PVT_kwDOEanNes4BkJhb",
                "content_node_id": "I_issue_1",
                "updated_at": "2026-10-09T00:00:00Z",
            },
            "changes": {"field_value": {"field_node_id": AUTO_FIELD, **change}},
        }
    ).encode()


def normalized(sender: int, change: dict[str, Any]) -> list[ev.EventBody]:
    result = DeliveryNormalizer(_identity()).normalize(
        raw_body=field_payload(sender, change),
        event_name="projects_v2_item",
        delivery_guid="guid-1",
        delivery_time_us=1_800_000_000_000_000,
    )
    return [e.body for e in result.events]


def test_only_an_owners_own_field_edit_is_a_mark_by_option_id():
    queued = {"from": None, "to": {"id": QUEUED_OPTION, "name": "Renamed"}}
    [body] = normalized(OWNER_ID, queued)
    assert body == ev.AutoBuildMarked(option="Queued")  # by option ID, never by name
    assert normalized(OTHER_USER_ID, queued) == []  # a non-owner approves nothing
    assert normalized(BOT, {"to": {"id": STARTED_OPTION}}) == []  # the factory's own write
    [cleared] = normalized(OWNER_ID, {"from": {"id": QUEUED_OPTION}, "to": None})
    assert cleared == ev.AutoBuildMarked(option="")
    [unknown] = normalized(OWNER_ID, {"to": {"id": "opt_other"}})
    assert unknown == ev.AutoBuildMarked(option="?")
    # A payload without the new value proves nothing: a later read sees it (unconfirmed).
    assert normalized(OWNER_ID, {}) == []


# ------------------------------------------------------------------ service rig


@dataclass
class Rig:
    service: FactoryService
    github: FakeGitHub
    clock: FakeClock
    cards: dict[str, RankingCard] = field(default_factory=dict)
    builder: AutoBuilder | None = None

    def new_builder(self) -> AutoBuilder:
        async def read() -> list[RankingCard]:
            return list(self.cards.values())

        return AutoBuilder(self.service, self.github.issue_snapshot, read)

    async def parcel(self, parcel_id: str) -> Parcel:
        found = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        assert found is not None
        return found

    async def accepted_starts(self) -> int:
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT COUNT(*) FROM events WHERE kind = ? AND accepted = 1",
                (EventKind.AUTO_BUILD.value,),
            )
        )
        return int(rows[0][0])


def history(*pids: str, question: str | None = None) -> Harness:
    """Planned cards the owner marked Queued (``question``: that one has an open one)."""
    h = Harness()
    for pid in pids:
        h.plan_published(pid)
        if pid == question:
            s = h.cur(pid)
            h.send(pid, ev.OwnerQuestion(session_id=s.session_id, question_key="q", summary="?"))
        r = h.send(pid, ev.AutoBuildMarked(option="Queued"))
        assert r.audit.accepted and h.p(pid).auto_build is not None
    return h


@asynccontextmanager
async def rig(
    config: ServiceConfig, h: Harness, *, ranks: dict[str, float | None] | None = None
) -> AsyncIterator[Rig]:
    store = SqliteStore.open(config.database_path, FakeClock())
    try:
        store.ensure_repository(config.trusted)
        for event, _ in h.log:
            store.apply_event(event, h.cfg)
        store.query("UPDATE effects SET state = 'done'")  # that history ran long ago
    finally:
        store.close()
    clock = FakeClock()
    clock.advance(10**9)  # after the seeded history
    github = FakeGitHub()
    for pid, parcel in h.parcels.items():
        github.snapshots[pid] = snapshot(
            stage=parcel.stage, read_at_us=clock.now_utc_us(), auto_build="Queued"
        )
    service = FactoryService(
        config, adapters=(github, FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        r = Rig(service, github, clock)
        for pid, rank in (ranks or {}).items():
            parcel = h.p(pid)
            r.cards[pid] = rcard(pid, parcel.issue_number or 0, rank=rank)
        r.builder = r.new_builder()
        service.auto_builder = r.builder
        yield r
    finally:
        await service.stop()


def on(config: ServiceConfig, **over: Any) -> ServiceConfig:
    return config.model_copy(update={"auto_build": True, **over})


# ------------------------------------------------------------------ start


@pytest.mark.asyncio
async def test_starts_the_best_ranked_mark_as_the_queues_auto_build(service_config: ServiceConfig):
    h = history("I_a", "I_b", "I_c")
    ranks = {"I_a": 2.0, "I_b": 1.0, "I_c": None}
    async with rig(on(service_config), h, ranks=ranks) as r:
        assert r.builder is not None
        assert await r.builder.run_once() == "started #2"
        b = await r.parcel("I_b")
        assert b.auto_build is not None and b.auto_build.status == AutoBuildStatus.STARTED
        assert b.stage == Stage.BUILDING
        assert b.current_approval is not None and b.current_approval.owner_id == OWNER_ID
        admission = await r.service.db.call(
            lambda store: store.load_admission(service_config.repo_id)
        )
        entry = admission.queue_entry("I_b")
        assert entry is not None and entry.auto  # derived from the stored mark on load
        assert entry.status == QueueStatus.QUEUED  # admitted once the plan run drains
        # One at a time: the next waits for the queued build (manual ones go first too).
        assert await r.builder.run_once() == "waiting: a queued build goes first"
        assert (await r.parcel("I_a")).auto_build.status == AutoBuildStatus.QUEUED  # type: ignore[union-attr]
        status = await r.builder.status()
        assert [e["issue"] for e in status["queue"]] == [1, 3]  # type: ignore[union-attr]
        assert status["auto_builds_waiting_for_slot"] == 1
        assert status["waiting_on"] == "a queued build goes first"


@pytest.mark.asyncio
async def test_disabled_by_default_and_switched_at_runtime(service_config: ServiceConfig):
    h = history("I_a")
    async with rig(service_config, h) as r:
        assert r.builder is not None
        assert await r.builder.run_once() == "disabled"
        status = await r.service.operator_command("auto-build", {"action": "on"})
        assert status["enabled"] is True and status["enabled_source"] == "operator"
        assert status["remaining_today"] is None  # 0 = unlimited
        assert status["queue"] == [{"issue": 1, "rank": None, "eligible": True, "blocker": None}]
        assert await r.builder.run_once() == "started #1"
        status = await r.service.operator_command("auto-build", {"action": "off"})
        assert status["enabled"] is False and status["waiting_on"] == "auto-build is off"
        with pytest.raises(ValueError, match="grant needs a whole number"):
            await r.service.operator_command("auto-build", {"action": "grant", "count": 0})


@pytest.mark.asyncio
async def test_status_shows_each_marks_blocker_and_an_open_question_waits(
    service_config: ServiceConfig,
):
    h = history("I_a", "I_b", question="I_a")
    async with rig(on(service_config), h, ranks={"I_a": 1.0, "I_b": 2.0}) as r:
        assert r.builder is not None
        status = await r.builder.status()
        assert status["queue"] == [
            {
                "issue": 1,
                "rank": 1.0,
                "eligible": False,
                "blocker": "waiting for the answer to an open question",
            },
            {"issue": 2, "rank": 2.0, "eligible": True, "blocker": None},
        ]
        assert await r.builder.run_once() == "started #2"  # #1 is skipped, not lost
        assert (await r.parcel("I_a")).auto_build is not None


@pytest.mark.asyncio
async def test_the_daily_limit_and_grant(service_config: ServiceConfig):
    h = history("I_a", "I_b")
    async with rig(on(service_config, auto_build_daily_limit=1), h) as r:
        assert r.builder is not None
        assert await r.builder.run_once() == "started #1"
        assert await r.builder.run_once() == "daily limit used (1/1)"
        status = await r.service.operator_command("auto-build", {"action": "grant", "count": 2})
        assert status["granted_today"] == 2 and status["remaining_today"] == 2


@pytest.mark.asyncio
async def test_a_read_showing_the_field_cleared_drops_the_mark_instead_of_starting(
    service_config: ServiceConfig,
):
    h = history("I_a")
    async with rig(on(service_config), h) as r:
        assert r.builder is not None
        r.github.snapshots["I_a"] = snapshot(
            stage=Stage.SCOPED, read_at_us=r.clock.now_utc_us(), auto_build=""
        )
        assert await r.builder.run_once() == "no startable auto-build"
        parcel = await r.parcel("I_a")
        assert parcel.auto_build is None and parcel.stage == Stage.SCOPED
        assert parcel.approvals == ()
        assert await r.builder.run_once() == "no auto-build queued"


@pytest.mark.asyncio
async def test_restart_never_starts_a_mark_twice(service_config: ServiceConfig):
    h = history("I_a")
    async with rig(on(service_config), h) as r:
        assert r.builder is not None
        assert await r.builder.run_once() == "started #1"
    # A daemon restart over the same state: the mark is started, the approval persisted.
    service = FactoryService(
        on(service_config),
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    try:
        github = FakeGitHub()
        github.snapshots["I_a"] = snapshot(stage=Stage.BUILDING, auto_build="Started")

        async def read() -> list[RankingCard]:
            return []

        builder = AutoBuilder(service, github.issue_snapshot, read)
        assert await builder.run_once() == "no auto-build queued"
        parcel = await service.db.call(lambda store: store.load_parcel("I_a"))
        assert parcel is not None and len(parcel.approvals) == 1
        rows = await service.db.call(
            lambda store: store.query(
                "SELECT COUNT(*) FROM events WHERE kind = ? AND accepted = 1",
                (EventKind.AUTO_BUILD.value,),
            )
        )
        assert int(rows[0][0]) == 1
        admission = await service.db.call(
            lambda store: store.load_admission(service_config.repo_id)
        )
        entry = admission.queue_entry("I_a")
        assert entry is not None and entry.auto
    finally:
        await service.stop()


# ------------------------------------------------------------------ setup and doctor


def test_setup_render_creates_the_field_and_doctor_reports_it_missing_or_wrong(
    service_config: ServiceConfig,
):
    from omnigent_factory.github.setup import render_project_migration
    from omnigent_factory.service.doctor import DoctorReport, _check_auto_build_field

    fields = {f["input"]["name"]: f["input"] for f in render_project_migration()["create_fields"]}
    auto = fields["Auto-build"]
    assert auto["dataType"] == "SINGLE_SELECT"
    assert [o["name"] for o in auto["singleSelectOptions"]] == ["Queued", "Started"]

    live = {
        AUTO_FIELD: {
            "id": AUTO_FIELD,
            "name": "Auto-build",
            "options": [{"id": QUEUED_OPTION, "name": "Queued"}, {"id": "x", "name": "Started"}],
        }
    }
    report = DoctorReport()
    _check_auto_build_field(on(service_config), live, report)  # switched on, no field set
    assert not report.ok and "Auto-build field is missing" in report.errors[0]
    assert "the board has one: id PVTSSF_auto" in report.errors[0]
    report = DoctorReport()
    _check_auto_build_field(on(service_config), {}, report)
    assert not report.ok and "see `setup render`" in report.errors[0]
    configured = on(
        service_config,
        auto_build_field_node_id=AUTO_FIELD,
        auto_build_options={"Queued": QUEUED_OPTION, "Started": STARTED_OPTION},
    )
    report = DoctorReport()
    _check_auto_build_field(configured, live, report)
    assert not report.ok and "option IDs differ" in report.errors[0]
    live[AUTO_FIELD]["options"][1]["id"] = STARTED_OPTION  # type: ignore[index]
    report = DoctorReport()
    _check_auto_build_field(configured, live, report)
    assert report.ok and report.checks["github_auto_build_field"].endswith("match")
    with pytest.raises(ValueError, match="auto_build_options must map Queued and Started"):
        service_config.model_validate(
            {**service_config.model_dump(), "auto_build_field_node_id": AUTO_FIELD}
        )
