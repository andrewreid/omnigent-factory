"""Epic autopilot in the service: the switch and CLI, next-sub-issue selection, concurrency
and Parallel, pauses, Full/Delayed starts through the auto-build queue, restart safety,
withdrawal, human gates, questions, setup and doctor."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    AutoBuildStatus,
    EpicAutopilot,
    IssueLinks,
    LinkedIssue,
    Parcel,
    SessionKind,
    Stage,
    Via,
)
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.ports.github import RankingCard
from omnigent_factory.service.auto_build import AutoBuilder
from omnigent_factory.service.autopilot import EpicAutopilotService
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
from tests.service.test_pilot_677 import eventually
from tests.test_epic_autopilot_reducer import (
    EPIC,
    S1,
    S2,
    claimed,
    epic_links,
    on_autopilot,
    send_epic,
    sub_links,
)

pytestmark = pytest.mark.asyncio

S3 = "I_sub_3"
DELAY_MIN = 60
BOT = 334_191_208


def plan_dict(
    order: Sequence[int] = (2, 3), gates: Sequence[dict[str, Any]] = ()
) -> dict[str, Any]:
    return {
        "kind": "epic_triage",
        "summary": "x",
        "coverage": {"gaps": [], "overlaps": []},
        "build_order": [{"issue": n, "reason": "r"} for n in order],
        "plan": {
            "parts": [{"issue": n, "scope": "s"} for n in order],
            "coordination": [],
            "human_gates": list(gates),
            "external_blockers": [],
        },
    }


@dataclass
class FakeGates:
    """GitHub as the gate writer sees it: issues created stay findable by marker."""

    existing: dict[int, dict[str, int]] = field(default_factory=dict)
    created: list[tuple[int, str]] = field(default_factory=list)
    links: list[tuple[int, int]] = field(default_factory=list)
    next_number: int = 40

    async def find(self, epic: int) -> dict[str, int]:
        return dict(self.existing.get(epic, {}))

    async def create(
        self,
        epic: int,
        *,
        key: str,
        title: str,
        steps: str,
        blocks: Sequence[int],
        owner_id: int,
    ) -> int:
        assert owner_id == OWNER_ID and title and steps and blocks
        number = self.next_number
        self.next_number += 1
        self.created.append((epic, key))
        self.existing.setdefault(epic, {})[key] = number
        return number

    async def link(self, blocked: int, blocking: int, *, source: int) -> bool:
        if (blocked, blocking) in self.links:
            return False
        self.links.append((blocked, blocking))
        return True


def card(number: int, *, rank: float | None = None, created: int = 1, parallel: int | None = None):
    return RankingCard(
        node_id=f"I_{number}",
        item_id=f"PVTI_{number}",
        number=number,
        title=f"Issue {number}",
        stage=Stage.INBOX,
        created_at_us=created,
        rank=rank,
        priority=None,
        parallel=parallel,
    )


@dataclass
class Rig:
    config: ServiceConfig
    service: FactoryService
    github: FakeGitHub
    clock: FakeClock
    autopilot: EpicAutopilotService
    builder: AutoBuilder
    cards: dict[int, RankingCard]
    labels: dict[str, list[str]]
    plan: dict[str, Any]
    gates: FakeGates

    async def parcel(self, pid: str) -> Parcel:
        found = await self.service.db.call(lambda store: store.load_parcel(pid))
        assert found is not None
        return found

    async def count(self, kind: EventKind) -> int:
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT COUNT(*) FROM events WHERE kind = ? AND accepted = 1", (kind.value,)
            )
        )
        return int(rows[0][0])

    async def effects(self, kind: EffectKind) -> list[dict[str, Any]]:
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT payload_json FROM effects WHERE kind = ?", (kind.value,)
            )
        )
        return [json.loads(str(row[0])).get("args", {}) for row in rows]


def seed(config: ServiceConfig, h: Harness) -> None:
    store = SqliteStore.open(config.database_path, FakeClock())
    try:
        store.ensure_repository(config.trusted)
        store.query("UPDATE repositories SET paused = 0")  # as the harness ran it
        for event, _ in h.log:
            store.apply_event(event, h.cfg)
        store.query("UPDATE effects SET state = 'done'")  # that history ran long ago
    finally:
        store.close()


@asynccontextmanager
async def rig(
    config: ServiceConfig,
    h: Harness | None,
    *,
    plan: dict[str, Any] | None = None,
    cards: dict[int, RankingCard] | None = None,
    gates: FakeGates | None = None,
    clock: FakeClock | None = None,
    sub_reads: dict[str, IssueLinks] | None = None,
) -> AsyncIterator[Rig]:
    if h is not None:
        seed(config, h)
    if clock is None:
        clock = FakeClock()
        clock.advance(10**9)  # after the seeded history
    github = FakeGitHub()
    reads = {S1: sub_links(), S2: sub_links(), S3: sub_links(), **(sub_reads or {})}
    for pid, links in reads.items():
        github.snapshots[pid] = snapshot(read_at_us=clock.now_utc_us(), links=links)
    service = FactoryService(
        config, adapters=(github, FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        state_cards = cards if cards is not None else {n: card(n, created=n) for n in (1, 2, 3, 4)}
        labels: dict[str, list[str]] = {}
        the_plan = plan if plan is not None else plan_dict()
        fake_gates = gates if gates is not None else FakeGates()

        async def read_cards() -> list[RankingCard]:
            return list(state_cards.values())

        async def read_labels(parcel: Parcel) -> list[str] | None:
            return labels.get(parcel.parcel_id)

        def read_plan(ap: EpicAutopilot) -> dict[str, Any] | None:
            return the_plan if ap.approved else None

        autopilot = EpicAutopilotService(
            service,
            github.issue_snapshot,
            read_cards,
            labels=read_labels,
            plans=read_plan,
            gates=fake_gates,
        )
        builder = AutoBuilder(service, github.issue_snapshot, read_cards, autopilot=autopilot)
        service.epic_autopilot = autopilot
        service.auto_builder = builder
        yield Rig(
            config,
            service,
            github,
            clock,
            autopilot,
            builder,
            state_cards,
            labels,
            the_plan,
            fake_gates,
        )
    finally:
        await service.stop()


def on(config: ServiceConfig, **over: Any) -> ServiceConfig:
    return config.model_copy(update={"epic_autopilot": True, **over})


def fresh(config: ServiceConfig, tmp_path: Path, name: str) -> ServiceConfig:
    """The same config over a new, empty state directory (a second, unrelated factory)."""
    state = tmp_path / name
    state.mkdir(mode=0o700)
    os.chmod(state, 0o700)
    return config.model_copy(update={"state_dir": state})


def history(
    level: str = "Full", *, approve: bool = True, subs: Sequence[str] = (S1, S2)
) -> Harness:
    """An epic (#1) on autopilot with sub-issues #2 (S1) and #3 (S2) seen by the factory."""
    h = Harness()
    on_autopilot(h, level, approve=approve)
    for pid in subs:
        f = h.f(pid)
        h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=sub_links())))
    return h


# ------------------------------------------------------------------ switch (1) and CLI


async def test_off_by_default_and_switched_by_the_operator(service_config: ServiceConfig):
    async with rig(service_config, history()) as r:
        await r.autopilot.run_once()
        assert (await r.parcel(S1)).autopilot_claim is None
        epic = await r.parcel(EPIC)
        assert epic.epic_note == "Epic · 0/2 done · autopilot off (factory switch)"
        status = await r.service.operator_command("epic-autopilot", {"action": "on"})
        assert status["enabled"] is True and status["enabled_source"] == "operator"
        assert status["delay_minutes"] == DELAY_MIN and status["concurrency"] == 1
        [entry] = status["epics"]  # type: ignore[misc]
        assert entry["issue"] == 1 and entry["plan_approved"] is True
        assert await r.autopilot.run_once() == "#1: started #2"
        status = await r.service.operator_command("epic-autopilot", {"action": "off"})
        assert status["enabled"] is False
        with pytest.raises(ValueError, match="status, on or off"):
            await r.service.operator_command("epic-autopilot", {"action": "grant"})


async def test_cli_mirrors_auto_build():
    from omnigent_factory.cli import build_parser

    for action in ("status", "on", "off"):
        args = build_parser().parse_args(["epic-autopilot", action])
        assert args.command == "epic-autopilot" and args.autopilot_command == action


async def test_the_autopilot_field_is_an_owner_control_by_option_id():
    field_id = "PVTSSF_autopilot"
    identity = DeliveryIdentity(
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
        autopilot_field_node_id=field_id,
        autopilot_option_ids={"Full": "o_full", "Delayed": "o_delay", "Plan only": "o_plan"},
    )

    def bodies(sender: int, change: dict[str, Any]) -> list[ev.EventBody]:
        payload = {
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
                "updated_at": "2026-10-11T00:00:00Z",
            },
            "changes": {"field_value": {"field_node_id": field_id, **change}},
        }
        result = DeliveryNormalizer(identity).normalize(
            raw_body=json.dumps(payload).encode(),
            event_name="projects_v2_item",
            delivery_guid="guid-ap",
            delivery_time_us=1_800_000_000_000_000,
        )
        return [e.body for e in result.events]

    delayed = {"to": {"id": "o_delay", "name": "Renamed"}}
    assert bodies(OWNER_ID, delayed) == [ev.AutopilotMarked(option="Delayed")]
    assert bodies(OTHER_USER_ID, delayed) == []
    assert bodies(BOT, {"to": None}) == []
    assert bodies(OWNER_ID, {"to": None}) == [ev.AutopilotMarked(option="")]
    assert bodies(OWNER_ID, {"to": {"id": "o_x"}}) == [ev.AutopilotMarked(option="?")]
    assert bodies(OWNER_ID, {}) == []


# ------------------------------------------------------------------ the plan gate (4)


async def test_nothing_advances_until_the_epic_plan_is_approved(service_config: ServiceConfig):
    async with rig(on(service_config), history(approve=False)) as r:
        assert await r.autopilot.run_once() == (
            "#1: Epic · 0/2 done · autopilot: waiting for your approval of the epic plan"
        )
        assert (await r.parcel(S1)).autopilot_claim is None
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 0


async def test_a_revision_pending_epic_starts_nothing_new_but_keeps_running_builds(
    service_config: ServiceConfig,
):
    h = history()
    claimed(h)
    send_epic(h, ev.PlanFeedback(text_digest="change the order"))
    async with rig(on(service_config), h) as r:
        outcome = await r.autopilot.run_once()
        assert "revising the epic plan" in outcome
        assert (await r.parcel(S2)).autopilot_claim is None
        assert (await r.parcel(S1)).auto_build is None  # nothing queued either


# ------------------------------------------------------------------ next selection (3, 7)


async def test_next_follows_the_epic_plan_order(service_config: ServiceConfig):
    async with rig(on(service_config), history(), plan=plan_dict(order=(3, 2))) as r:
        assert await r.autopilot.run_once() == "#1: started #3"
        assert (await r.parcel(S2)).autopilot_claim is not None
        assert (await r.parcel(S2)).stage == Stage.SCOPED  # straight to planning
        note = (await r.parcel(EPIC)).epic_note
        assert note == "Epic · 0/2 done · autopilot: planning #3, next #2"


async def test_rank_breaks_ties_outside_the_plan_order(service_config: ServiceConfig):
    cards = {1: card(1), 2: card(2, rank=5.0), 3: card(3, rank=1.0)}
    async with rig(on(service_config), history(), plan=plan_dict(order=(9,)), cards=cards) as r:
        assert await r.autopilot.run_once() == "#1: started #3"


async def test_open_blockers_inside_or_outside_the_epic_are_waited_on(
    service_config: ServiceConfig,
):
    h = history()
    f = h.f(S1)
    outside = sub_links(blockers=(LinkedIssue(500, True),))
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=outside)))
    async with rig(on(service_config), h) as r:
        assert await r.autopilot.run_once() == "#1: started #3"
        # The outside blocker is never started by autopilot.
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 1


async def test_a_nested_epic_skipped_and_human_assigned_or_skip_labelled_left_alone(
    service_config: ServiceConfig, tmp_path: Path
):
    h = history(subs=(S1, S2))
    nested = sub_links(nested=True)
    f = h.f(S1)
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=nested)))
    async with rig(on(service_config), h) as r:
        r.labels[S2] = ["factory:skip"]
        assert await r.autopilot.run_once() == "#1: nothing to start"
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 0
    h2 = history()
    f = h2.f(S1)
    h2.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(read_at_us=f.now, human_assigned=True, links=sub_links()),
        )
    )
    async with rig(on(fresh(service_config, tmp_path, "second")), h2) as r:
        assert await r.autopilot.run_once() == "#1: started #3"


async def test_no_order_and_no_links_stops_and_asks_once(service_config: ServiceConfig):
    h = history()
    async with rig(on(service_config), h, plan=plan_dict(order=())) as r:
        assert await r.autopilot.run_once() == "#1: no build order for its sub-issues"
        assert await r.autopilot.run_once() == "#1: no build order for its sub-issues"
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 0
        comments = await r.effects(EffectKind.POST_COMMENT)
        assert [c["template"] for c in comments] == ["autopilot-order"]
        assert (await r.parcel(EPIC)).epic_note == (
            "Autopilot paused: no build order for its sub-issues"
        )
    # Restart: still asked only once.
    async with rig(on(service_config), None, plan=plan_dict(order=())) as r:
        await r.autopilot.run_once()
        assert len(await r.effects(EffectKind.POST_COMMENT)) == 1


# ------------------------------------------------------------------ concurrency


async def test_one_at_a_time_by_default_and_parallel_overrides(service_config: ServiceConfig):
    async with rig(on(service_config), history()) as r:
        assert await r.autopilot.run_once() == "#1: started #2"
        assert await r.autopilot.run_once() == "#1: nothing to start"
        assert (await r.parcel(S2)).autopilot_claim is None
        r.cards[1] = card(1, parallel=2)
        assert await r.autopilot.run_once() == "#1: started #3"


async def test_the_concurrency_setting(service_config: ServiceConfig):
    async with rig(on(service_config, epic_autopilot_concurrency=2), history()) as r:
        assert await r.autopilot.run_once() == "#1: started #2, #3"


# ------------------------------------------------------------------ pauses (8)


async def test_a_sub_issue_needing_the_owner_pauses_the_epic(service_config: ServiceConfig):
    h = history()
    r1 = h.apply(
        h.f(S1).make(
            ev.AutopilotPlan(epic=1, epoch=h.p(EPIC).autopilot.source_event_id),  # type: ignore[union-attr]
            evidence=snapshot(read_at_us=h.f(S1).now + 1, links=sub_links()),
        )
    )
    assert r1.audit.accepted
    s = h.create_ok(S1)
    h.send(S1, ev.OwnerQuestion(session_id=s.session_id, question_key="q", summary="Which?"))
    async with rig(on(service_config, epic_autopilot_concurrency=2), h) as r:
        assert await r.autopilot.run_once() == "#1: #2 needs you"
        epic = await r.parcel(EPIC)
        assert epic.epic_note == "Autopilot paused: #2 needs you"
        assert epic.autopilot is not None and epic.autopilot.paused == "#2 needs you"
        assert (await r.parcel(S2)).autopilot_claim is None
        comments = await r.effects(EffectKind.POST_COMMENT)
        # The status is a note only (the one comment is the agent's own question).
        assert [c["template"] for c in comments] == ["decision"]


async def test_plan_drift_pauses_the_epic(service_config: ServiceConfig):
    h = history()
    claimed(h, fit="exceeds")
    async with rig(on(service_config, epic_autopilot_concurrency=2), h) as r:
        outcome = await r.autopilot.run_once()
        assert outcome == "#1: #2: its plan goes beyond its part of the epic plan"
        assert (await r.parcel(S1)).auto_build is None
        assert (await r.parcel(S2)).autopilot_claim is None


# ------------------------------------------------------------------ levels (2) and capacity (9)


async def test_full_queues_the_posted_plan_and_the_auto_build_queue_starts_it(
    service_config: ServiceConfig,
):
    h = history("Full")
    claimed(h)
    async with rig(on(service_config), h) as r:
        await r.autopilot.run_once()
        mark = (await r.parcel(S1)).auto_build
        assert mark is not None and mark.autopilot_epic == 1 and mark.not_before_us == 0
        assert await r.builder.run_once() == "started #2"  # auto_build itself is off
        s1 = await r.parcel(S1)
        assert s1.stage == Stage.BUILDING and s1.current_approval is not None
        assert s1.current_approval.owner_id == OWNER_ID
        note = (await r.parcel(EPIC)).epic_note
        assert note.startswith("Epic · 0/2 done · autopilot:")


async def test_plan_only_never_queues(service_config: ServiceConfig):
    h = history("Plan only")
    claimed(h)
    async with rig(on(service_config), h) as r:
        await r.autopilot.run_once()
        assert (await r.parcel(S1)).auto_build is None
        assert "waiting for your approval #2" in (await r.parcel(EPIC)).epic_note
        assert await r.builder.run_once() == "disabled" or (await r.parcel(S1)).approvals == ()


async def test_manual_builds_go_first_and_autopilot_shares_the_auto_build_cap(
    service_config: ServiceConfig,
):
    h = history("Full")
    claimed(h)
    h.plan_published("I_manual")
    h.send("I_manual", ev.ApprovePlan(via=Via.COMMAND))  # queued until its plan run drains
    async with rig(on(service_config, max_building=3), h) as r:
        await r.autopilot.run_once()
        assert await r.builder.run_once() == "waiting: a queued build goes first"
        status = await r.builder.status()
        assert [e["issue"] for e in status["queue"]] == [2]  # type: ignore[union-attr]


async def test_the_switch_off_holds_queued_autopilot_starts(service_config: ServiceConfig):
    h = history("Full")
    claimed(h)
    async with rig(on(service_config), h) as r:
        await r.autopilot.run_once()
        await r.service.operator_command("epic-autopilot", {"action": "off"})
        assert await r.builder.run_once() == "disabled"
        assert (await r.parcel(S1)).auto_build is not None  # waits, not dropped


async def test_delayed_timer_survives_a_restart_and_fires_exactly_once(
    service_config: ServiceConfig,
):
    h = history("Delayed")
    claimed(h)
    clock = FakeClock()
    clock.advance(10**9)
    config = on(service_config)
    async with rig(config, h, clock=clock) as r:
        await r.autopilot.run_once()
        mark = (await r.parcel(S1)).auto_build
        assert mark is not None and mark.not_before_us > clock.now_utc_us()
        assert await r.builder.run_once() == "no startable auto-build"
        status = await r.builder.status()
        assert "autopilot start at" in str(status["queue"])
        due = mark.not_before_us
    # A restart before the delay is up: nothing starts; after it, exactly once.
    async with rig(config, None, clock=clock) as r:
        assert await r.builder.run_once() == "no startable auto-build"
        clock.advance(due - clock.now_utc_us())
        r.github.snapshots[S1] = snapshot(read_at_us=clock.now_utc_us(), links=sub_links())
        assert await r.builder.run_once() == "started #2"
        assert await r.builder.run_once() == "no auto-build queued"
    async with rig(config, None, clock=clock) as r:
        assert await r.builder.run_once() == "no auto-build queued"
        assert await r.count(EventKind.AUTO_BUILD) == 1
        assert await r.count(EventKind.AUTOPILOT_QUEUE) == 1


async def test_clearing_the_epic_field_withdraws_queued_autopilot_starts(
    service_config: ServiceConfig,
):
    h = history("Delayed")
    claimed(h)
    async with rig(on(service_config), h) as r:
        await r.autopilot.run_once()
        assert (await r.parcel(S1)).auto_build is not None
    h2 = Harness(parcels=dict(h.parcels), factories=h.factories)
    send_epic(h2, ev.AutopilotMarked(option=""))
    async with rig(on(service_config), h2) as r:
        assert (await r.parcel(EPIC)).autopilot is None
        await r.autopilot.run_once()
        s1 = await r.parcel(S1)
        assert s1.auto_build is None and s1.autopilot_claim is not None
        assert s1.autopilot_claim.released.startswith("autopilot was turned off")


# ------------------------------------------------------------------ restart (no duplicates)


async def test_a_restart_never_plans_a_sub_issue_twice(service_config: ServiceConfig):
    config = on(service_config)

    async def plan_runs(r: Rig) -> int:
        return sum(1 for s in (await r.parcel(S1)).sessions if s.kind == SessionKind.PLAN)

    async with rig(config, history()) as r:
        assert await r.autopilot.run_once() == "#1: started #2"
        await eventually(lambda: plan_runs(r))  # once the card move has landed
    async with rig(config, None) as r:
        await r.autopilot.run_once()
        await r.autopilot.run_once()
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 1
        assert await plan_runs(r) == 1


# ------------------------------------------------------------------ human gates (5)


GATE = {
    "key": "dns-record",
    "title": "Add the DNS record",
    "steps": "Create the CNAME in the zone.",
    "blocks": [2],
}


def gated_history() -> Harness:
    h = history()
    f = h.f(EPIC)
    links = epic_links((2, True), (3, True), (40, True))
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now + 2, links=links)))
    return h


async def test_gates_become_owner_sub_issues_once_and_block_their_dependents(
    service_config: ServiceConfig,
):
    gates = FakeGates()
    config = on(service_config)
    plan = plan_dict(gates=[GATE])
    async with rig(config, gated_history(), plan=plan, gates=gates) as r:
        # #2 waits for the gate (#40, open): #3 goes first.
        assert await r.autopilot.run_once() == "#1: started #3"
        assert gates.created == [(1, "dns-record")] and gates.links == [(2, 40)]
        epic = await r.parcel(EPIC)
        assert [(g.key, g.number, g.linked) for g in epic.autopilot_gates] == [
            ("dns-record", 40, True)
        ]
    async with rig(config, None, plan=plan, gates=gates) as r:
        await r.autopilot.run_once()
        assert gates.created == [(1, "dns-record")]  # never twice


async def test_a_gate_created_this_pass_blocks_at_once_and_a_closed_one_no_longer(
    service_config: ServiceConfig, tmp_path: Path
):
    plan = plan_dict(gates=[GATE])
    gates = FakeGates()
    async with rig(on(service_config), history(), plan=plan, gates=gates) as r:
        # The new gate #40 is in no read yet: #2 still waits for it.
        assert await r.autopilot.run_once() == "#1: started #3"
        assert (await r.parcel(S1)).autopilot_claim is None
    h = gated_history()
    f = h.f(EPIC)
    closed = epic_links((2, True), (3, True), (40, False))
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now + 3, links=closed)))
    gates = FakeGates(existing={1: {"dns-record": 40}})
    async with rig(on(fresh(service_config, tmp_path, "closed")), h, plan=plan, gates=gates) as r:
        assert await r.autopilot.run_once() == "#1: started #2"


async def test_a_gate_created_before_a_crash_is_found_by_its_marker(
    service_config: ServiceConfig,
):
    gates = FakeGates(existing={1: {"dns-record": 40}})  # created, never recorded
    async with rig(
        on(service_config), gated_history(), plan=plan_dict(gates=[GATE]), gates=gates
    ) as r:
        await r.autopilot.run_once()
        assert gates.created == []
        epic = await r.parcel(EPIC)
        assert [(g.key, g.number) for g in epic.autopilot_gates] == [("dns-record", 40)]


async def test_a_failing_gate_write_pauses_instead_of_advancing(service_config: ServiceConfig):
    class Broken(FakeGates):
        async def find(self, epic: int) -> dict[str, int]:
            raise RuntimeError("GitHub is down")

    async with rig(
        on(service_config), gated_history(), plan=plan_dict(gates=[GATE]), gates=Broken()
    ) as r:
        assert await r.autopilot.run_once() == "#1: its human steps could not be created yet"
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 0


# ------------------------------------------------------------------ setup and doctor


async def test_setup_render_and_doctor_cover_the_autopilot_fields(service_config: ServiceConfig):
    from omnigent_factory.github.setup import render_project_migration
    from omnigent_factory.service.doctor import DoctorReport, _check_autopilot_fields

    fields = {f["input"]["name"]: f["input"] for f in render_project_migration()["create_fields"]}
    assert [o["name"] for o in fields["Autopilot"]["singleSelectOptions"]] == [
        "Full",
        "Delayed",
        "Plan only",
    ]
    assert fields["Parallel"]["dataType"] == "NUMBER"
    live = {
        "F_ap": {
            "id": "F_ap",
            "name": "Autopilot",
            "options": [
                {"id": "o1", "name": "Full"},
                {"id": "o2", "name": "Delayed"},
                {"id": "o3", "name": "Plan only"},
            ],
        },
        "F_par": {"id": "F_par", "name": "Parallel", "dataType": "NUMBER"},
    }
    report = DoctorReport()
    _check_autopilot_fields(on(service_config), live, report)
    assert not report.ok and "the board has one: id F_ap" in report.errors[0]
    configured = on(
        service_config,
        autopilot_field_node_id="F_ap",
        autopilot_options={"Full": "o1", "Delayed": "o2", "Plan only": "x"},
        parallel_field_node_id="F_par",
    )
    report = DoctorReport()
    _check_autopilot_fields(configured, live, report)
    assert not report.ok and "option IDs differ" in report.errors[0]
    fixed = configured.model_copy(
        update={"autopilot_options": {"Full": "o1", "Delayed": "o2", "Plan only": "o3"}}
    )
    report = DoctorReport()
    _check_autopilot_fields(fixed, live, report)
    assert report.ok, report.errors
    report = DoctorReport()
    _check_autopilot_fields(replace_parallel(fixed, "F_wrong"), live, report)
    assert not report.ok and "Parallel" in report.errors[0]
    with pytest.raises(ValueError, match="autopilot_options must map Full, Delayed"):
        service_config.model_validate(
            {**service_config.model_dump(), "autopilot_field_node_id": "F_ap"}
        )


def replace_parallel(config: ServiceConfig, field_id: str) -> ServiceConfig:
    return config.model_copy(update={"parallel_field_node_id": field_id})


async def test_autopilot_marks_wait_for_their_delay_in_the_queue_view():
    from omnigent_factory.core.types import AutoBuildMark
    from omnigent_factory.service.auto_build import mark_blocker

    mark = AutoBuildMark(
        AutoBuildStatus.QUEUED, "h", "c", OWNER_ID, "e", 1, autopilot_epic=1, not_before_us=10
    )
    parcel = replace(Parcel(parcel_id="I", repo_id="R"), auto_build=mark)
    assert mark_blocker(parcel, now_us=5) is not None
    assert "autopilot start at" in str(mark_blocker(parcel, now_us=5))
    _ = MICROS_PER_MINUTE


async def test_the_epic_pass_leaves_an_autopilot_epics_note_to_autopilot(
    service_config: ServiceConfig,
):
    from tests.service.test_native_links import FakeClient, issue, links
    from tests.service.test_native_links import card as board_card

    h = history()
    async with rig(on(service_config), h) as r:
        await r.autopilot.run_once()
        note = (await r.parcel(EPIC)).epic_note
        assert "autopilot" in note
        epic_card = replace(board_card(1, linked=epic_links()), node_id=EPIC)
        client = FakeClient({1: issue(1)}, types=["Task"])
        assert await links(r.service, client, [epic_card]).epic_pass() == 0
        assert (await r.parcel(EPIC)).epic_note == note


class StubClient:
    """Answers the gate writer's GitHub calls (reads, user lookup, createIssue)."""

    def __init__(self, sub_bodies: dict[int, str]) -> None:
        self.sub_bodies = sub_bodies
        self.created: list[dict[str, Any]] = []

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if "createIssue" in query:
            self.created.append(variables["input"])
            return {"createIssue": {"issue": {"number": 77}}}
        if "subIssues(first: 100)" in query:
            nodes = [{"number": n, "body": b} for n, b in self.sub_bodies.items()]
            return {"repository": {"issue": {"subIssues": {"nodes": nodes}}}}
        return {
            "repository": {
                "issue": {
                    "id": f"I_{variables['number']}",
                    "number": variables["number"],
                    "repository": {"id": "R_kgDOTC12Fg"},
                    "issueType": None,
                    "blockedBy": {"totalCount": 0, "nodes": []},
                }
            }
        }

    async def get_json(self, path: str) -> dict[str, Any]:
        assert path == f"/user/{OWNER_ID}"
        return {"node_id": "U_owner"}


async def test_github_gates_find_by_marker_and_create_one_assigned_sub_issue(
    service_config: ServiceConfig,
):
    from omnigent_factory.github.links import gate_marker
    from omnigent_factory.service.directory import safe_publication
    from omnigent_factory.service.links import GitHubGates, NativeLinks

    marker = gate_marker(1, "dns-record")
    client = StubClient({40: f"steps\n\n{marker}", 41: gate_marker(9, "dns-record"), 42: "x"})
    async with rig(service_config, history()) as r:

        async def no_cards() -> list[RankingCard]:
            return []

        native = NativeLinks(r.service, client, lambda _sid: None, no_cards)  # type: ignore[arg-type]
        gates = GitHubGates(native, lambda text: safe_publication(text, r.service.config))
        assert await gates.find(1) == {"dns-record": 40}  # another epic's marker is not ours
        number = await gates.create(
            1,
            key="tenant",
            title="Grant the app consent",
            steps="Ask @someone to grant consent <!-- x -->",
            blocks=[2, 3],
            owner_id=OWNER_ID,
        )
        assert number == 77
        [created] = client.created
        assert created["parentIssueId"] == "I_1" and created["assigneeIds"] == ["U_owner"]
        assert created["body"].endswith(gate_marker(1, "tenant"))
        assert "@someone" not in created["body"] and "<!-- x" not in created["body"]
        assert "#2, #3" in created["body"]
