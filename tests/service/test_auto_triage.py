"""Idle-time auto-triage in the service: eligibility, idle gating, budget, operator CLI."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from omnigent_factory import cli
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.projection import project_bot
from omnigent_factory.core.types import (
    MICROS_PER_HOUR,
    BotState,
    Lifecycle,
    Parcel,
    ReservationKind,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.ports.github import BoardIssue
from omnigent_factory.service.auto_triage import (
    AutoTriager,
    busy_reason,
    eligible_candidates,
    local_day,
)
from omnigent_factory.service.board_index import BoardIndex
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, result_candidate, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_pilot_677 import eventually

NOW = FakeClock().now_utc_us()
DAY = 24 * MICROS_PER_HOUR


def issue(number: int, **over: Any) -> BoardIssue:
    values: dict[str, Any] = {
        "node_id": f"I_auto_{number}",
        "number": number,
        "title": f"Issue {number}",
        "stage": Stage.INBOX,
        "labels": (),
        "created_at_us": NOW - 2 * DAY + number,  # higher numbers are newer
        "assigned": False,
    }
    values.update(over)
    return BoardIssue(**values)


# ------------------------------------------------------------------ eligibility


def test_eligible_candidates_excludes_ineligible_inbox_issues_oldest_first():
    known = Parcel(parcel_id="I_auto_8", repo_id="R", issue_number=8)
    from omnigent_factory.testing.harness import Harness

    h = Harness()
    h.triage("I_auto_8")
    worked = h.p("I_auto_8")
    issues = [
        issue(1, assigned=True),
        issue(2, labels=("bug", "factory:skip")),
        issue(3, labels=("Factory:Skip",)),
        issue(4, created_at_us=NOW - DAY + 60_000_000),  # 23h59m old: inside the grace
        issue(5, created_at_us=0),  # unknown age: never taken
        issue(6, stage=Stage.TRIAGED),
        issue(7, stage=None),
        issue(8),  # the factory already worked on it
        issue(9, created_at_us=NOW - 3 * DAY),
        issue(10),
        issue(11),
    ]
    chosen = eligible_candidates(
        issues,
        {"I_auto_8": worked, "I_auto_10": known},  # a bare parcel (e.g. a comment) is fine
        now_us=NOW,
        min_age_us=DAY,
        blocked=lambda node_id: node_id == "I_auto_11",  # parked delivery
    )
    assert [i.number for i in chosen] == [9, 10]


def test_board_read_lists_only_open_issues_of_the_repository():
    """Pull requests, draft items, other repositories and closed issues never list."""
    repo, project = "R_kgDOTC12Fg", "PVT_kwDOEanNes4BkJhb"

    def item(typename: str, number: int, **content: Any) -> dict[str, Any]:
        base = {
            "__typename": typename,
            "id": f"N{number}",
            "number": number,
            "title": f"t{number}",
            "state": "OPEN",
            "createdAt": "2026-10-01T00:00:00Z",
            "repository": {"id": repo},
            "assignees": {"totalCount": 0},
            "labels": {"nodes": [{"name": "bug"}]},
        }
        base.update(content)
        return {"status": {"optionId": "915abb46"}, "content": base}

    nodes = [
        item("Issue", 1),
        item("PullRequest", 2),
        {"status": {"optionId": "915abb46"}, "content": {"__typename": "DraftIssue"}},
        item("Issue", 4, repository={"id": "R_other"}),
        item("Issue", 5, state="CLOSED"),
        item("Issue", 6, assignees={"totalCount": 1}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        page = {"nodes": nodes, "pageInfo": {"hasNextPage": False, "endCursor": None}}
        return httpx.Response(200, json={"data": {"node": {"items": page}}})

    async def run() -> list[BoardIssue] | RetryableReadFailure:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            adapter = GitHubAPIAdapter(
                GitHubClient(http, "token"),
                repository="SA-Ambulance/timesheets",
                repository_node_id=repo,
                project_node_id=project,
                status_field_node_id="PVTSSF_x",
                bot_user_id=777,
                required_checks=frozenset(),
            )
            return await adapter.board_issues()

    import asyncio

    found = asyncio.run(run())
    assert isinstance(found, list)
    assert [(i.number, i.stage, i.labels, i.assigned) for i in found] == [
        (1, Stage.INBOX, ("bug",), False),
        (6, Stage.INBOX, ("bug",), True),
    ]
    assert found[0].created_at_us > 0


def test_busy_reason_names_running_plans_and_waiting_requests():
    from omnigent_factory.testing.harness import Harness

    h = Harness()
    h.eligible("P")
    h.send("P", ev.RequestPlan(via=Via.DRAG))
    assert busy_reason([h.p("P")]) == "plan running on #1"  # recorded, being created
    h.create_ok("P")
    assert busy_reason([h.p("P")]) == "plan running on #1"
    h.publish_plan("P")  # done: waiting for the owner's approval is not work
    assert busy_reason([h.p("P")]) is None
    # An owner triage waiting for a slot goes before any auto-triage.
    h.eligible("T")
    h.send("T", ev.RequestTriage(via=Via.DRAG))
    h.create_ok("T")
    h.eligible("W")
    h.send("W", ev.RequestTriage(via=Via.DRAG))
    assert busy_reason([h.p("W")]) == "triage requested on #3"
    # A running triage is not "busy": the triage slot limit governs it.
    assert busy_reason([h.p("T")]) is None


def test_busy_reason_names_working_and_draining_builds_and_reworks():
    from omnigent_factory.testing.harness import Harness

    h = Harness()
    h.to_building("B")
    assert busy_reason([h.p("B")]) == "build running on #1"
    h.send("B", ev.Stop())  # draining: the tree still runs until observed quiet
    assert h.cur("B").lifecycle == Lifecycle.DRAINING
    assert busy_reason([h.p("B")]) == "build draining on #1"
    h.quiesce("B", h.cur("B").session_id)
    assert busy_reason([h.p("B")]) is None
    h3 = Harness()
    h3.to_building("G")
    s = h3.cur("G")
    h3.send("G", ev.ActiveLimitReached(session_id=s.session_id, grant_id=s.grant.grant_id))
    g = h3.p("G")  # checkpoint grace: the card shows Checkpoint, the agent still winds down
    assert h3.cur("G").lifecycle == Lifecycle.CHECKPOINT_GRACE
    assert project_bot(g) == BotState.CHECKPOINT
    assert busy_reason([g]) == "build winding down on #1"
    h2 = Harness()
    h2.to_building("R")
    h2.build_ready("R")
    h2.send("R", ev.RequestRework())
    h2.send("R", ev.CapacityAvailable())
    assert busy_reason([h2.p("R")]) == "rework running on #1"


def _blocked(h: Any, pid: str) -> None:
    s = h.cur(pid)
    h.send(pid, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))


def _asks_owner(h: Any, pid: str) -> None:
    s = h.cur(pid)
    h.send(pid, ev.OwnerQuestion(session_id=s.session_id, question_key="q-1", summary="?"))


def _waits_on_checks(h: Any, pid: str) -> None:
    s = h.cur(pid)
    h.send(
        pid,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=7,
            head_sha="a" * 40,
        ),
    )


@pytest.mark.parametrize(
    ("settle", "bot"),
    [(_blocked, BotState.BLOCKED), (_asks_owner, BotState.NEEDS_YOU), (_waits_on_checks, None)],
)
def test_a_build_holding_its_slot_without_working_is_not_busy(settle: Any, bot: Any):
    """Blocked, Needs you or waiting on CI: the run holds its build slot but does no work."""
    from omnigent_factory.testing.harness import Harness

    h = Harness()
    h.to_building("B")
    settle(h, "B")
    p = h.p("B")
    assert p.stage == Stage.BUILDING and h.admission.building_count == 1
    if bot is not None:
        assert project_bot(p) == bot
    else:
        assert h.cur("B").wait_reason is not None
    assert busy_reason([p]) is None


# ------------------------------------------------------------------ service rig


@dataclass
class Rig:
    service: FactoryService
    github: FakeGitHub
    clock: FakeClock
    board: list[BoardIssue]
    triager: AutoTriager

    async def parcel(self, parcel_id: str) -> Parcel | None:
        return await self.service.db.call(lambda store: store.load_parcel(parcel_id))

    def inbox(self, *numbers: int) -> None:
        for number in numbers:
            self.board.append(issue(number))
            self.github.snapshots[f"I_auto_{number}"] = snapshot(
                stage=Stage.INBOX, read_at_us=self.clock.now_utc_us()
            )


def auto_config(config: ServiceConfig, **over: Any) -> ServiceConfig:
    return config.model_copy(update={"auto_triage": True, **over})


@asynccontextmanager
async def rig(config: ServiceConfig, clock: FakeClock | None = None) -> AsyncIterator[Rig]:
    clock = clock or FakeClock()
    github = FakeGitHub()
    service = FactoryService(
        config, adapters=(github, FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        board: list[BoardIssue] = []

        async def read() -> list[BoardIssue]:
            return list(board)

        triager = AutoTriager(service, BoardIndex(read, clock, ttl_us=0), github.issue_snapshot)
        service.auto_triager = triager
        yield Rig(service, github, clock, board, triager)
    finally:
        await service.stop()


async def triage_running(r: Rig, parcel_id: str) -> None:
    async def running() -> bool:
        parcel = await r.parcel(parcel_id)
        run = parcel.current_session if parcel is not None else None
        return run is not None and run.kind == SessionKind.TRIAGE

    await eventually(running)


async def finish(r: Rig, parcel_id: str) -> None:
    """The triage run submits its result and its tree goes quiet: the slot is free."""

    async def active() -> bool:
        p = await r.parcel(parcel_id)
        run = p.current_session if p is not None else None
        return run is not None and run.lifecycle == Lifecycle.ACTIVE

    await eventually(active)
    parcel = await r.parcel(parcel_id)
    assert parcel is not None and parcel.current_session is not None
    run = parcel.current_session
    assert run.root_id is not None
    factory = EventFactory(parcel_id, start_us=r.clock.now_utc_us())
    result = await r.service.apply_event(
        factory.make(
            result_candidate(run.session_id, run.root_id, run.revision, ev.ResultKind.TRIAGE)
        )
    )
    assert result.accepted, result.reason
    await r.service.apply_event(
        factory.make(ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=False))
    )
    after = await r.parcel(parcel_id)
    assert after is not None and after.session(run.session_id).lifecycle == Lifecycle.RETIRED  # type: ignore[union-attr]


# ------------------------------------------------------------------ start + gating


@pytest.mark.asyncio
async def test_idle_factory_auto_triages_the_oldest_eligible_inbox_issue(
    service_config: ServiceConfig,
):
    async with rig(auto_config(service_config)) as r:
        r.inbox(5, 3)
        r.board.append(issue(1, labels=("factory:skip",)))
        assert await r.triager.run_once() == "started #3"
        await triage_running(r, "I_auto_3")
        parcel = await r.parcel("I_auto_3")
        assert parcel is not None and parcel.stage == Stage.TRIAGED
        moves = r.github.executed(EffectKind.MOVE_CARD)
        assert [m.args["to"] for m in moves] == [Stage.TRIAGED.value]
        # Attributed to the clock's standing authorisation, never an owner.
        rows = await r.service.db.call(
            lambda store: store.query(
                "SELECT provenance, actor_id, accepted FROM events WHERE kind = ?",
                (EventKind.AUTO_TRIAGE.value,),
            )
        )
        assert [tuple(row) for row in rows] == [("scheduler", None, 1)]
        # The running triage holds the one slot: nothing more starts.
        assert await r.triager.run_once() == "not idle: every triage slot is taken"


@pytest.mark.asyncio
async def test_disabled_by_default(service_config: ServiceConfig):
    async with rig(service_config) as r:
        r.inbox(3)
        assert await r.triager.run_once() == "disabled"
        assert await r.parcel("I_auto_3") is None


@pytest.mark.asyncio
async def test_a_plan_in_flight_blocks_auto_triage(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        factory = EventFactory("I_plan", issue_number=90, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await r.service.apply_event(factory.make(ev.RequestPlan(via=Via.DRAG)))
        outcome = await r.triager.run_once()
        assert outcome.startswith("not idle: plan "), outcome
        assert await r.parcel("I_auto_3") is None


@pytest.mark.asyncio
async def test_an_active_build_blocks_auto_triage(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        factory = EventFactory("I_build", issue_number=91, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await r.service.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)))

        async def admitted() -> bool:
            admission = await r.service.db.call(
                lambda store: store.load_admission(service_config.repo_id)
            )
            return admission.building_count == 1

        await eventually(admitted)
        assert await r.triager.run_once() == "not idle: build running on #91"


@pytest.mark.asyncio
async def test_a_queued_build_that_can_start_blocks_auto_triage(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        r.service.accepting_admission = False  # hold the queue: nothing is admitted yet
        for number in (91, 92):
            factory = EventFactory(f"I_q{number}", issue_number=number)
            await r.service.apply_event(
                factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
            )
            await r.service.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)))
        assert await r.triager.run_once() == "not idle: a queued build on #91 can start"
        status = await r.triager.status()
        assert status["idle"] is False
        assert status["busy"] == "a queued build on #91 can start"
        await r.service.operator_command("pause", {})  # paused: no queued build can start
        assert await r.triager.run_once() == "not idle: paused"
        await r.service.operator_command("unpause", {})
        r.service.accepting_admission = True

        async def admitted() -> bool:
            # #91 runs; #92 stays queued with no free build slot, which alone is no work.
            return await r.triager.run_once() == "not idle: build running on #91"

        await eventually(admitted)
        assert await r.parcel("I_auto_3") is None


@pytest.mark.asyncio
async def test_a_card_holding_a_build_slot_with_no_run_working_leaves_the_factory_idle(
    service_config: ServiceConfig,
):
    """#729: a card in Building, Bot Idle, holding a build slot, no session running."""
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        pid = "I_729"
        factory = EventFactory(pid, issue_number=729, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await r.service.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)))

        async def active() -> bool:
            p = await r.parcel(pid)
            run = p.current_session if p is not None else None
            return run is not None and run.lifecycle == Lifecycle.ACTIVE

        await eventually(active)
        parcel = await r.parcel(pid)
        assert parcel is not None and parcel.current_session is not None
        run = parcel.current_session
        await r.service.apply_event(factory.make(ev.Stop()))
        await r.service.apply_event(
            factory.make(ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=False))
        )
        parcel = await r.parcel(pid)
        assert parcel is not None
        assert parcel.session(run.session_id).lifecycle == Lifecycle.FENCED  # type: ignore[union-attr]
        # The live #729 state: the card still holds its build slot, with no run behind it.

        def hold_slot(store: Any) -> None:
            with store._txn() as conn:
                conn.execute(
                    "INSERT INTO reservations (reservation_id, repo_id, parcel_id, kind, "
                    "episode_id, pr_number, live) VALUES (?, ?, ?, ?, ?, NULL, 1)",
                    ("rs_729", service_config.repo_id, pid, ReservationKind.BUILDING.value, "ep"),
                )

        await r.service.db.call(hold_slot)
        admission = await r.service.db.call(
            lambda store: store.load_admission(service_config.repo_id)
        )
        parcel = await r.parcel(pid)
        assert parcel is not None and admission.building_count == 1
        assert parcel.stage == Stage.BUILDING and project_bot(parcel) == BotState.IDLE
        status = await r.triager.status()
        assert status["idle"] is True and status["busy"] is None
        assert await r.triager.run_once() == "started #3"
        await triage_running(r, "I_auto_3")


@pytest.mark.asyncio
async def test_pause_blocks_auto_triage(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        await r.service.operator_command("pause", {})
        assert await r.triager.run_once() == "not idle: paused"
        assert await r.parcel("I_auto_3") is None


@pytest.mark.asyncio
async def test_a_fresh_read_showing_the_issue_assigned_is_skipped(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3, 4)
        r.github.snapshots["I_auto_3"] = snapshot(stage=Stage.INBOX, human_assigned=True)
        assert await r.triager.run_once() == "started #4"


@pytest.mark.asyncio
async def test_the_store_derives_running_triages_for_the_slot_limit(
    service_config: ServiceConfig,
):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        await r.triager.run_once()
        await triage_running(r, "I_auto_3")
        admission = await r.service.db.call(
            lambda store: store.load_admission(service_config.repo_id)
        )
        assert admission.triage_runs == frozenset({"I_auto_3"})
        # A manual triage now waits for the slot instead of starting (or being dropped).
        factory = EventFactory("I_manual", issue_number=77, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        result = await r.service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))
        assert result.accepted
        manual = await r.parcel("I_manual")
        assert manual is not None and manual.pending_authorization_id is not None
        assert manual.current_session is None
        # Waiting owner triages go before any further auto-triage.
        await finish(r, "I_auto_3")
        assert (await r.triager.run_once()).startswith("not idle: triage requested")
        await r.service.apply_event(factory.make(ev.ReconcileDue()))
        await triage_running(r, "I_manual")


# ------------------------------------------------------------------ budget


@pytest.mark.asyncio
async def test_daily_budget_grant_and_restart_persistence(service_config: ServiceConfig):
    config = auto_config(service_config, auto_triage_daily_limit=1)
    clock = FakeClock()
    async with rig(config, clock) as r:
        r.inbox(3, 4, 5)
        assert await r.triager.run_once() == "started #3"
        await triage_running(r, "I_auto_3")
        await finish(r, "I_auto_3")
        assert await r.triager.run_once() == "daily budget used (1/1)"
        status = await r.service.operator_command("auto-triage", {"action": "grant", "count": 2})
        assert status["granted_today"] == 2 and status["remaining_today"] == 2
        assert status["next_candidate"] == {"issue": 4, "title": "Issue 4"}
        assert await r.triager.run_once() == "started #4"
        await triage_running(r, "I_auto_4")
        await finish(r, "I_auto_4")
    # A restart neither resets today's count nor loses the grant.
    async with rig(config, clock) as r:
        status = await r.triager.status()
        assert (status["used_today"], status["granted_today"], status["remaining_today"]) == (
            2,
            2,
            1,
        )
        clock.advance(DAY)  # the next local day: a fresh budget, the grant was for one day
        status = await r.triager.status()
        assert (status["used_today"], status["granted_today"], status["remaining_today"]) == (
            0,
            0,
            1,
        )


@pytest.mark.asyncio
async def test_grant_needs_a_whole_positive_number(service_config: ServiceConfig):
    async with rig(service_config) as r:
        for bad in (0, -1, "3", True, 1001):
            with pytest.raises(ValueError, match="whole number"):
                await r.service.operator_command("auto-triage", {"action": "grant", "count": bad})
        with pytest.raises(ValueError, match="status, on, off or grant"):
            await r.service.operator_command("auto-triage", {"action": "reset"})


def test_local_day_bounds_contain_the_instant():
    day, start, end = local_day(NOW)
    assert start <= NOW < end and len(day) == 10
    assert local_day(end)[0] != day and local_day(end - 1)[0] == day


# ------------------------------------------------------------------ on / off


@pytest.mark.asyncio
async def test_operator_toggle_survives_restart_until_the_config_changes(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        status = await r.service.operator_command("auto-triage", {"action": "on"})
        assert (status["enabled"], status["enabled_source"]) == (True, "operator")
    async with rig(service_config) as r:
        assert await r.triager.enabled() == (True, "operator")
        # The config file changes after the toggle: the config is the newer word again.
        r.service.config = auto_config(service_config)
        assert await r.triager.enabled() == (True, "config")
        await r.service.operator_command("auto-triage", {"action": "off"})
        assert await r.triager.enabled() == (False, "operator")
        r.service.config = service_config
        assert await r.triager.enabled() == (False, "config")


@pytest.mark.asyncio
async def test_status_reports_idle_budget_and_next_candidate(service_config: ServiceConfig):
    async with rig(auto_config(service_config)) as r:
        r.inbox(3)
        status = await r.service.operator_command("auto-triage", {"action": "status"})
        assert status["enabled"] is True and status["idle"] is True and status["busy"] is None
        assert status["daily_limit"] == 20 and status["used_today"] == 0
        assert status["triage_concurrency"] == 1 and status["triage_running"] == 0
        assert status["next_candidate"] == {"issue": 3, "title": "Issue 3"}
        assert status["eligible_inbox"] == 1


@pytest.mark.asyncio
async def test_auto_triage_operator_command_is_refused_when_not_wired(
    service_config: ServiceConfig,
):
    service = FactoryService(service_config, clock=FakeClock())
    await service.start()
    try:
        with pytest.raises(ValueError, match="not wired"):
            await service.operator_command("auto-triage", {"action": "status"})
    finally:
        await service.stop()


def test_cli_auto_triage_subcommands(monkeypatch: pytest.MonkeyPatch, tmp_path: Any):
    calls: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(cli, "_load", lambda args: (tmp_path, object()))
    monkeypatch.setattr(
        cli, "_operator", lambda config, command, args=None, **kw: calls.append((command, args))
    )
    for argv in (["status"], ["on"], ["off"], ["grant", "20"]):
        cli.main(["auto-triage", *argv])
    assert calls == [
        ("auto-triage", {"action": "status"}),
        ("auto-triage", {"action": "on"}),
        ("auto-triage", {"action": "off"}),
        ("auto-triage", {"action": "grant", "count": 20}),
    ]


def test_auto_triage_config_keys_reload_hot():
    from omnigent_factory.service.config import HOT_RELOAD_KEYS

    assert {
        "auto_triage",
        "auto_triage_daily_limit",
        "auto_triage_min_age_hours",
        "triage_concurrency",
    } <= HOT_RELOAD_KEYS
