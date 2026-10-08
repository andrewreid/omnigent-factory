"""Triage ranking: validation, owner pins and sticky priorities, no-churn writes,
triggering and mutual exclusion with auto-triage, restart/duplicate safety, archiving."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import MICROS_PER_HOUR, MICROS_PER_MINUTE, Stage, Via
from omnigent_factory.github.ranking import RankingWriteError
from omnigent_factory.omnigent.ranking import RankingSessionError, RootState
from omnigent_factory.ports.github import BoardIssue, RankingCard
from omnigent_factory.service.auto_triage import AutoTriager
from omnigent_factory.service.board_index import BoardIndex
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.mcp import FactoryToolError, FactoryTools
from omnigent_factory.service.ranking import (
    Ranker,
    RankingToolError,
    assign_ranks,
    validate_submission,
)
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import ranking as rs
from omnigent_factory.testing.builders import OWNER_ID, EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)

RANK_FIELD = "PVTF_rank"
PRIORITY_FIELD = "PVTSSF_priority"
BOT_ID = 334_191_208


def card(number: int, *, stage: Stage = Stage.TRIAGED, **over: Any) -> RankingCard:
    values: dict[str, Any] = {
        "node_id": f"I_{number}",
        "item_id": f"PVTI_{number}",
        "number": number,
        "title": f"Issue {number}",
        "stage": stage,
        "created_at_us": 1,
        "rank": None,
        "priority": None,
    }
    values.update(over)
    return RankingCard(**values)


def item(issue: int, reason: str = "matters") -> dict[str, Any]:
    return {"issue": issue, "reason": reason}


# ------------------------------------------------------------------ pure validation


def test_assign_ranks_orders_around_owner_pins():
    assert assign_ranks([5, 6, 7], {9: 2.0}) == {9: 2.0, 5: 1.0, 6: 3.0, 7: 4.0}
    assert assign_ranks([5], {9: 1.5}) == {9: 1.5, 5: 1.0}


def test_validation_lists_every_problem():
    triage = {n: card(n) for n in (1, 2, 3, 4)}
    with pytest.raises(RankingToolError) as caught:
        validate_submission(
            {
                "ranking": [item(1), item(1), item(99), {"issue": 2, "reason": " "}],
                "summary": "x" * 601,
                "priority_changes": [
                    {"issue": 3, "new_priority": "P9", "reason": "r"},
                    {"issue": 4, "new_priority": "P1", "reason": "owner chose"},
                ],
            },
            triage,
            pinned={},
            owner_priority={4},
        )
    text = str(caught.value)
    for expected in (
        "#1 is listed twice",
        "#99 is not in the Triage column",
        "#2: reason is required",
        "missing Triage issues: #3, #4",
        "summary: 601 characters",
        "#3 new_priority must be one of P0-P3",
        "the owner set the Priority of #4",
    ):
        assert expected in text, text


def test_pinned_issues_may_be_left_out_or_kept_in_place_only():
    triage = {n: card(n) for n in (1, 2, 3)}
    pinned = {2: 1.0}
    ok = validate_submission({"ranking": [item(3), item(1)], "summary": "s"}, triage, pinned, set())
    assert ok.ranks == {2: 1.0, 3: 2.0, 1: 3.0}
    in_place = validate_submission(
        {"ranking": [item(2), item(3), item(1)], "summary": "s"}, triage, pinned, set()
    )
    assert in_place.ranks == ok.ranks
    with pytest.raises(RankingToolError, match="owner-pinned at rank 1"):
        validate_submission(
            {"ranking": [item(3), item(2), item(1)], "summary": "s"}, triage, pinned, set()
        )


def test_a_priority_change_must_be_new_and_reasoned():
    triage = {1: card(1, priority="P2")}
    with pytest.raises(RankingToolError, match="#1 already has P2"):
        validate_submission(
            {
                "ranking": [item(1)],
                "summary": "s",
                "priority_changes": [{"issue": 1, "new_priority": "P2", "reason": "r"}],
            },
            triage,
            {},
            set(),
        )
    with pytest.raises(RankingToolError, match="needs a reason"):
        validate_submission(
            {
                "ranking": [item(1)],
                "summary": "s",
                "priority_changes": [{"issue": 1, "new_priority": "P0", "reason": ""}],
            },
            triage,
            {},
            set(),
        )


# ------------------------------------------------------------------ fakes


@dataclass
class FakeBoardAdapter:
    async def _single_select_fields(self, names: tuple[str, ...]) -> dict[str, Any]:
        return {"Priority": (PRIORITY_FIELD, {p: f"opt_{p}" for p in ("P0", "P1", "P2", "P3")})}


@dataclass
class FakeBoard:
    cards_: list[RankingCard] = field(default_factory=list)
    ranks: list[tuple[str, int]] = field(default_factory=list)
    priorities: list[tuple[str, str]] = field(default_factory=list)
    #: write_id -> comment text (one per marker: adoption, never a second post).
    comments: dict[str, str] = field(default_factory=dict)
    comment_posts: int = 0
    updates: dict[str, str] = field(default_factory=dict)
    update_posts: int = 0
    #: Failures to raise before the next write of a kind: (kind, retry, after_effect).
    failures: list[tuple[str, bool, bool]] = field(default_factory=list)
    adapter: FakeBoardAdapter = field(default_factory=FakeBoardAdapter)

    async def cards(self, rank_field_id: str) -> list[RankingCard]:
        assert rank_field_id == RANK_FIELD
        return list(self.cards_)

    def _set(self, number: str, **values: Any) -> None:
        self.cards_ = [replace(c, **values) if c.item_id == number else c for c in self.cards_]

    def _fail(self, kind: str, *, after: bool) -> None:
        for index, (k, retry, after_effect) in enumerate(self.failures):
            if k == kind and after_effect == after:
                del self.failures[index]
                raise RankingWriteError(f"{kind} failed", retry=retry)

    async def set_rank(self, item_id: str, rank_field_id: str, rank: int) -> None:
        self._fail("rank", after=False)
        self.ranks.append((item_id, rank))
        self._set(item_id, rank=float(rank))

    async def set_priority(self, item_id: str, priority: str) -> None:
        self._fail("priority", after=False)
        self.priorities.append((item_id, priority))
        self._set(item_id, priority=priority)

    async def post_comment(self, issue_number: int, node: str, write_id: str, text: str) -> str:
        self._fail("comment", after=False)
        if write_id not in self.comments:  # adopted by its marker
            self.comment_posts += 1
            self.comments[write_id] = text
        self._fail("comment", after=True)  # a lost response after the post landed
        return f"c-{write_id}"

    async def post_status_update(self, write_id: str, text: str) -> str:
        self._fail("status_update", after=False)
        if write_id not in self.updates:
            self.update_posts += 1
            self.updates[write_id] = text
        return f"su-{write_id}"


@dataclass
class FakeSessions:
    #: nonce -> root (a create with a known nonce adopts it).
    roots: dict[str, str] = field(default_factory=dict)
    titles: dict[str, str] = field(default_factory=dict)
    creates: int = 0
    sent: dict[str, list[str]] = field(default_factory=dict)
    archived: list[str] = field(default_factory=list)
    interrupted: list[str] = field(default_factory=list)
    idle: bool = False
    gone: bool = False
    fail_create: RankingSessionError | None = None

    async def create(self, *, run_id: str, nonce: str, title: str) -> str:
        if nonce in self.roots:
            return self.roots[nonce]
        if self.fail_create is not None:
            raise self.fail_create
        self.creates += 1
        root = f"conv_rank_{self.creates}"
        self.roots[nonce] = root
        self.titles[root] = title
        return root

    async def find(self, nonce: str) -> str | None:
        return self.roots.get(nonce)

    async def prepare(self, root_id: str, nonce: str) -> None:
        assert self.roots.get(nonce) == root_id

    async def verify(self, root_id: str) -> None:
        return None

    async def send_once(self, root_id: str, key: str, text: str) -> None:
        sent = self.sent.setdefault(root_id, [])
        if key not in sent:
            sent.append(key)

    async def state(self, root_id: str) -> RootState | None:
        if self.gone:
            return None
        return RootState(archived=root_id in self.archived, idle=self.idle)

    async def interrupt(self, root_id: str) -> None:
        self.interrupted.append(root_id)

    async def archive(self, root_id: str) -> None:
        if root_id not in self.archived:
            self.archived.append(root_id)


@dataclass
class Rig:
    service: FactoryService
    clock: FakeClock
    board: FakeBoard
    sessions: FakeSessions
    ranker: Ranker

    def new_ranker(self) -> Ranker:
        """A fresh instance over the same store and fakes (a daemon restart)."""
        return Ranker(
            self.service,
            board=self.board,  # type: ignore[arg-type]
            sessions=self.sessions,  # type: ignore[arg-type]
            policy_barrier_us=0,
        )

    async def run(self) -> rs.RankingRun | None:
        return await self.service.db.call(lambda s: rs.open_run(s, self.service.config.repo_id))

    async def runs(self) -> list[rs.RankingRun]:
        return await self.service.db.call(
            lambda s: rs.recent_runs(s, self.service.config.repo_id, 20)
        )

    async def field(self, node: str, name: str) -> rs.BoardField | None:
        fields = await self.service.db.call(lambda s: rs.board_fields(s, [node]))
        return fields.get((node, name))

    async def add_triage_results(self, count: int) -> None:
        now = self.clock.now_utc_us()

        def insert(store: Any) -> None:
            with store._txn() as conn:
                (existing,) = conn.execute("SELECT COUNT(*) FROM mcp_receipts").fetchone()
                for i in range(count):
                    key = f"tr-{existing + i}"
                    conn.execute(
                        "INSERT INTO mcp_receipts (receipt_key, parcel_id, run_id, tool, "
                        "event_id, request_sha256, receipt_json, created_at_us) "
                        "VALUES (?, 'P', ?, 'submit', 'e', ?, ?, ?)",
                        (key, key, "0" * 64, json.dumps({"kind": "triage"}), now),
                    )

        await self.service.db.call(insert)

    async def submit(self, raw: Mapping[str, Any], ranker: Ranker | None = None) -> dict[str, Any]:
        run = await self.run()
        assert run is not None and run.root_id is not None
        return await (ranker or self.ranker).submit(run.root_id, run.run_id, dict(raw))


def ranking_config(config: ServiceConfig, **over: Any) -> ServiceConfig:
    return config.model_copy(update={"ranking": True, "rank_field_node_id": RANK_FIELD, **over})


@asynccontextmanager
async def rig(config: ServiceConfig, clock: FakeClock | None = None) -> AsyncIterator[Rig]:
    clock = clock or FakeClock()
    service = FactoryService(
        config, adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        r = Rig(service, clock, FakeBoard(), FakeSessions(), None)  # type: ignore[arg-type]
        r.ranker = r.new_ranker()
        service.ranker = r.ranker
        yield r
    finally:
        await service.stop()


async def started(r: Rig, *cards: RankingCard) -> rs.RankingRun:
    """A ranking run waiting for its submission."""
    r.board.cards_ = list(cards)
    await r.ranker.command({"action": "now"})
    assert await r.ranker.run_once() == "started"
    run = await r.run()
    assert run is not None and run.state == "running"
    return run


# ------------------------------------------------------------------ triggering


@pytest.mark.asyncio
async def test_disabled_by_default_and_not_started_without_the_rank_field(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        r.board.cards_ = [card(1)]
        await r.add_triage_results(10)
        assert await r.ranker.run_once() == "disabled"
    async with rig(service_config.model_copy(update={"ranking": True})) as r:
        assert await r.ranker.run_once() == ("not configured: rank_field_node_id is not configured")


@pytest.mark.asyncio
async def test_threshold_of_new_triage_results_or_a_day_with_a_change(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        r.board.cards_ = [card(1), card(2)]
        await r.ranker.command({"action": "now"})
        assert await r.ranker.run_once() == "started"
        await r.submit({"ranking": [item(1), item(2)], "summary": "first"})
        assert await r.ranker.run_once() == "completed"
        # Nothing new: no new run.
        r.clock.advance(MICROS_PER_HOUR)
        await r.add_triage_results(4)
        assert await r.ranker.run_once() == "nothing new to rank"
        await r.add_triage_results(1)  # 5 new triage results
        assert await r.ranker.run_once() == "started"
        await r.submit({"ranking": [item(1), item(2)], "summary": "second"})
        assert await r.ranker.run_once() == "completed"
        # A day later with no change: still nothing; a changed Triage column: a run.
        r.clock.advance(25 * MICROS_PER_HOUR)
        assert await r.ranker.run_once() == "nothing new to rank"
        r.board.cards_.append(card(3))
        assert await r.ranker.run_once() == "started"


@pytest.mark.asyncio
async def test_now_ignores_the_threshold_and_the_operator_override_wins(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config, ranking=False)) as r:
        r.board.cards_ = [card(1)]
        status = await r.ranker.command({"action": "on"})
        assert status["enabled"] is True and status["enabled_source"] == "operator"
        status = await r.ranker.command({"action": "off"})
        assert status["enabled"] is False and status["enabled_source"] == "operator"
        assert await r.ranker.run_once() == "disabled"
        status = await r.ranker.command({"action": "now"})
        assert status["now_requested"] is True
        assert await r.ranker.run_once() == "started"
        assert (await r.ranker.status())["now_requested"] is False


@pytest.mark.asyncio
async def test_not_idle_while_a_plan_runs_or_a_triage_runs(service_config: ServiceConfig):
    async with rig(ranking_config(service_config)) as r:
        r.board.cards_ = [card(1)]
        await r.add_triage_results(5)
        factory = EventFactory("I_plan", issue_number=90, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await r.service.apply_event(factory.make(ev.RequestPlan(via=Via.DRAG)))
        outcome = await r.ranker.run_once()
        assert outcome.startswith("not idle: plan "), outcome
    async with rig(ranking_config(service_config)) as r:
        r.board.cards_ = [card(1)]
        await r.add_triage_results(5)
        factory = EventFactory("I_tri", issue_number=91, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await r.service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))

        async def triage_running() -> bool:
            admission = await r.service.db.call(lambda s: s.load_admission(service_config.repo_id))
            return bool(admission.triage_runs)

        from tests.service.test_pilot_677 import eventually

        await eventually(triage_running)
        assert await r.ranker.run_once() == "not idle: a triage is running"
        # ``now`` skips the idle rule, never the triage slot.
        status = await r.ranker.command({"action": "now"})
        assert status["busy"] == "a triage is running" and status["idle"] is False
        assert await r.ranker.run_once() == "not idle: a triage is running"
        assert await r.run() is None


async def build_running(r: Rig) -> None:
    factory = EventFactory("I_build", issue_number=91, start_us=r.clock.now_utc_us())
    await r.service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    await r.service.apply_event(factory.make(ev.WaivePlan(via=Via.DRAG)))

    async def admitted() -> bool:
        admission = await r.service.db.call(lambda s: s.load_admission(r.service.config.repo_id))
        return admission.building_count == 1

    from tests.service.test_pilot_677 import eventually

    await eventually(admitted)


@pytest.mark.asyncio
async def test_now_starts_while_a_build_runs_and_an_automatic_run_waits(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        r.board.cards_ = [card(1)]
        await r.add_triage_results(5)
        await build_running(r)
        assert await r.ranker.run_once() == "not idle: build running on #91"
        status = await r.ranker.status()
        assert status["busy"] == "build running on #91" and status["idle"] is False
        status = await r.ranker.command({"action": "now"})
        assert status["now_requested"] is True
        assert status["busy"] is None and status["idle"] is True
        assert await r.ranker.run_once() == "started"
        # A second ``now`` during the run waits for it, and says so.
        status = await r.ranker.command({"action": "now"})
        assert status["busy"] == "a ranking run is in flight" and status["idle"] is False
        assert await r.ranker.run_once() == "running"
        assert len(await r.runs()) == 1
        await r.submit({"ranking": [item(1)], "summary": "s"})
        assert await r.ranker.run_once() == "completed"
        assert await r.ranker.run_once() == "started"  # the second request, build still on
        await r.submit({"ranking": [item(1)], "summary": "s"})
        assert await r.ranker.run_once() == "completed"
        # Automatic runs still wait for idle.
        await r.add_triage_results(5)
        assert await r.ranker.run_once() == "not idle: build running on #91"
        assert (await r.ranker.status())["busy"] == "build running on #91"


@pytest.mark.asyncio
async def test_a_ranking_run_blocks_auto_triage(service_config: ServiceConfig):
    config = ranking_config(service_config, auto_triage=True)
    async with rig(config) as r:
        await started(r, card(1))
        board: list[BoardIssue] = []

        async def read() -> list[BoardIssue]:
            return board

        triager = AutoTriager(
            r.service, BoardIndex(read, r.clock, ttl_us=0), FakeGitHub().issue_snapshot
        )
        assert await triager.run_once() == "not idle: a triage ranking is running"
        assert (await triager.status())["busy"] == "a triage ranking is running"
        await r.submit({"ranking": [item(1)], "summary": "s"})
        assert await r.ranker.run_once() == "completed"
        assert await triager.busy() is None


# ------------------------------------------------------------------ the run


@pytest.mark.asyncio
async def test_a_completed_run_writes_ranks_comment_and_status_once_and_archives(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        run = await started(
            r, card(1, priority="P2"), card(2, priority="P3"), card(7, stage=Stage.BUILDING)
        )
        root = run.root_id
        assert root is not None and r.sessions.titles[root].startswith("Factory ranking · ")
        assert r.sessions.sent[root] == [f"ranking:{run.run_id}:start"]
        receipt = await r.submit(
            {
                "ranking": [item(2, "unblocks #1"), item(1, "needs #2 first")],
                "summary": "Two issues; #2 first.",
                "priority_changes": [{"issue": 2, "new_priority": "P1", "reason": "it blocks #1"}],
            }
        )
        assert receipt["accepted"] and receipt["rank_changes"] == 2
        assert await r.ranker.run_once() == "completed"
        assert sorted(r.board.ranks) == [("PVTI_1", 2), ("PVTI_2", 1)]
        assert r.board.priorities == [("PVTI_2", "P1")]
        [comment] = r.board.comments.values()
        assert comment == (
            "Priority changed P3 -> P1 by the factory's triage ranking: it blocks #1"
        )
        [update] = r.board.updates.values()
        assert update.splitlines()[2:4] == [
            "1. #2 Issue 2: unblocks #1",
            "2. #1 Issue 1: needs #2 first",
        ]
        assert "- #2 P3 -> P1: it blocks #1" in update
        assert r.sessions.archived == [root]
        [done] = await r.runs()
        assert done.state == "done" and done.archived_at_us is not None
        # The exact retry replays the receipt; another submission is refused.
        again = await r.ranker.submit(
            root,
            run.run_id,
            {
                "ranking": [item(2, "unblocks #1"), item(1, "needs #2 first")],
                "summary": "Two issues; #2 first.",
                "priority_changes": [{"issue": 2, "new_priority": "P1", "reason": "it blocks #1"}],
            },
        )
        assert again["replayed"] is True
        with pytest.raises(RankingToolError, match="already accepted"):
            await r.ranker.submit(root, run.run_id, {"ranking": [], "summary": "x"})


@pytest.mark.asyncio
async def test_rank_writes_only_when_the_value_changes(service_config: ServiceConfig):
    async with rig(ranking_config(service_config, ranking_status_update=False)) as r:
        await started(r, card(1), card(2))
        await r.submit({"ranking": [item(1), item(2)], "summary": "s"})
        assert await r.ranker.run_once() == "completed"
        assert sorted(r.board.ranks) == [("PVTI_1", 1), ("PVTI_2", 2)]
        r.board.ranks.clear()
        await r.ranker.command({"action": "now"})
        assert await r.ranker.run_once() == "started"
        receipt = await r.submit({"ranking": [item(1), item(2)], "summary": "same"})
        assert receipt["rank_changes"] == 0
        assert await r.ranker.run_once() == "completed"
        assert r.board.ranks == [] and r.board.update_posts == 0
        # The factory's own values are no owner pins.
        assert not (await r.field("I_1", "rank")).owner_set  # type: ignore[union-attr]


def owner_edit(
    config: ServiceConfig, node: str, field_value: dict[str, Any] | None, sender: int = OWNER_ID
) -> bytes:
    """A ``projects_v2_item`` edited delivery body (no ``changes`` when ``field_value``
    is None)."""
    payload: dict[str, Any] = {
        "action": "edited",
        "sender": {"id": sender},
        "projects_v2_item": {"project_node_id": config.project_node_id, "content_node_id": node},
    }
    if field_value is not None:
        payload["changes"] = {"field_value": field_value}
    return json.dumps(payload).encode()


@pytest.mark.asyncio
async def test_a_value_nobody_recorded_is_a_baseline_not_an_owner_choice(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    """The #735 case: an earlier triage (or pre-ranking tooling) left P2 and a Rank on the
    board, unrecorded, and the owner never touched them."""
    caplog.set_level(logging.INFO, "omnigent_factory.service.ranking")
    async with rig(ranking_config(service_config)) as r:
        await started(r, card(735, priority="P2"), card(341, rank=1.0, priority="P1"), card(31))
        for node, name in (
            ("I_735", "priority"),
            ("I_341", "rank"),
            ("I_341", "priority"),
            ("I_735", "rank"),
        ):
            row = await r.field(node, name) or rs.BoardField(node, name)
            assert not row.owner_set, (node, name)
        assert "owner choice detected" not in caplog.text
        # Ranking may change both under the normal rules (with the priority comment).
        await r.submit(
            {
                "ranking": [item(31), item(735), item(341)],
                "summary": "s",
                "priority_changes": [{"issue": 735, "new_priority": "P1", "reason": "blocks"}],
            }
        )
        assert await r.ranker.run_once() == "completed"
        assert r.board.priorities == [("PVTI_735", "P1")]
        assert sorted(r.board.ranks) == [("PVTI_31", 1), ("PVTI_341", 3), ("PVTI_735", 2)]
        assert r.board.comment_posts == 1
        assert "P2 -> P1" in next(iter(r.board.comments.values()))


@pytest.mark.asyncio
async def test_an_owner_rank_webhook_pins_until_the_owner_clears_it(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config, ranking_status_update=False)) as r:
        r.board.cards_ = [card(1), card(2), card(3, rank=1.0)]
        # The owner sets #3's Rank (the bot's own edit of the same field is ignored).
        await r.ranker.note_owner_field(
            owner_edit(service_config, "I_3", {"field_node_id": RANK_FIELD, "to": 1}, BOT_ID)
        )
        assert await r.field("I_3", "rank") is None
        await r.ranker.note_owner_field(
            owner_edit(service_config, "I_3", {"field_node_id": RANK_FIELD, "to": 1})
        )
        await started(r, *r.board.cards_)
        row = await r.field("I_3", "rank")
        assert row is not None and row.owner_choice and row.owner_value == "1"
        with pytest.raises(RankingToolError, match="#3 is owner-pinned at rank 1"):
            await r.submit({"ranking": [item(1), item(3), item(2)], "summary": "s"})
        await r.submit({"ranking": [item(2), item(1)], "summary": "s"})
        assert await r.ranker.run_once() == "completed"
        assert sorted(r.board.ranks) == [("PVTI_1", 3), ("PVTI_2", 2)]
        # The owner clears the field: the pin ends, the card is ranked again.
        r.board._set("PVTI_3", rank=None)
        await r.ranker.command({"action": "now"})
        assert await r.ranker.run_once() == "started"
        assert not (await r.field("I_3", "rank")).owner_choice  # type: ignore[union-attr]
        with pytest.raises(RankingToolError, match="missing Triage issues: #3"):
            await r.submit({"ranking": [item(2), item(1)], "summary": "s"})


@pytest.mark.asyncio
async def test_an_owner_webhook_pin_mid_run_is_never_overwritten(service_config: ServiceConfig):
    async with rig(ranking_config(service_config, ranking_status_update=False)) as r:
        await started(r, card(1), card(2))
        await r.submit({"ranking": [item(1), item(2)], "summary": "s"})
        # Before the writes land, the owner sets #2's Rank (and the bot's own edits are
        # ignored).
        for sender in (BOT_ID, OWNER_ID):
            await r.ranker.note_owner_field(
                json.dumps(
                    {
                        "action": "edited",
                        "sender": {"id": sender},
                        "projects_v2_item": {
                            "project_node_id": service_config.project_node_id,
                            "content_node_id": "I_2",
                        },
                        "changes": {"field_value": {"field_node_id": RANK_FIELD, "to": 7}},
                    }
                ).encode()
            )
        assert await r.ranker.run_once() == "completed"
        assert r.board.ranks == [("PVTI_1", 1)]
        [done] = await r.runs()
        writes = await r.service.db.call(lambda s: rs.run_writes(s, done.run_id))
        assert [(w.issue_number, w.detail) for w in writes] == [
            (1, "rank 1"),
            (2, "skipped: owner-pinned"),
        ]
        pinned = await r.field("I_2", "rank")
        assert pinned is not None and pinned.owner_set and pinned.owner_value == "7"


@pytest.mark.asyncio
async def test_owner_priority_is_sticky_only_by_an_owner_webhook(service_config: ServiceConfig):
    async with rig(ranking_config(service_config, ranking_status_update=False)) as r:
        # #1 and #2 carry priorities nobody recorded writing: baselines, not the owner's.
        await started(r, card(1, priority="P2"), card(2, priority="P1"), card(3, priority="P3"))
        for node in ("I_1", "I_2"):
            assert not (await r.field(node, "priority")).owner_choice  # type: ignore[union-attr]
        # #3: the owner changes Priority by hand (webhook).
        await r.ranker.note_owner_field(
            owner_edit(
                service_config,
                "I_3",
                {"field_node_id": PRIORITY_FIELD, "to": {"id": "opt_P3", "name": "P3"}},
            )
        )
        with pytest.raises(RankingToolError, match="the owner set the Priority of #3"):
            await r.submit(
                {
                    "ranking": [item(1), item(2), item(3)],
                    "summary": "s",
                    "priority_changes": [{"issue": 3, "new_priority": "P0", "reason": "new"}],
                }
            )
        await r.submit(
            {
                "ranking": [item(1), item(2), item(3)],
                "summary": "s",
                "priority_changes": [
                    {"issue": 1, "new_priority": "P0", "reason": "outage"},
                    {"issue": 2, "new_priority": "P3", "reason": "workaround"},
                ],
            }
        )
        assert await r.ranker.run_once() == "completed"
        assert r.board.priorities == [("PVTI_1", "P0"), ("PVTI_2", "P3")]
        # The factory's own change is not the owner's: a later run may change it again.
        assert not (await r.field("I_1", "priority")).owner_choice  # type: ignore[union-attr]
        row = await r.field("I_3", "priority")
        assert row is not None and row.owner_choice and row.owner_value == "P3"


@pytest.mark.asyncio
async def test_an_owner_edit_without_a_field_id_is_attributed_only_when_unambiguous(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config, ranking_status_update=False)) as r:
        await started(r, card(1, priority="P2"), card(2, priority="P2"), card(3, priority="P2"))
        # #1: only Priority changed after the owner's edit -> the owner's.
        # #2: Priority and Rank changed -> ambiguous, nobody's.
        # #3: nothing it could be attributed to changed -> nobody's.
        r.board._set("PVTI_1", priority="P0")
        r.board._set("PVTI_2", priority="P0", rank=4.0)
        for node in ("I_1", "I_2", "I_3"):
            await r.ranker.note_owner_field(owner_edit(service_config, node, None))
        cards = await r.ranker.cards()
        _pinned, sticky = await r.ranker.owner_choices(cards)
        assert sticky == frozenset({"I_1"})
        row = await r.field("I_1", "priority")
        assert row is not None and row.owner_choice and row.owner_value == "P0"
        for node in ("I_2", "I_3"):
            for name in ("rank", "priority"):
                other = await r.field(node, name)
                assert other is not None and not other.owner_choice and not other.owner_probe


@pytest.mark.asyncio
async def test_repair_clears_inferred_owner_choices_and_keeps_webhook_backed_ones(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    """Rows the removed inference wrote (owner_set, no owner_webhook) are cleared unless a
    stored owner delivery backs them; the repair runs once, on the first pass."""
    caplog.set_level(logging.INFO, "omnigent_factory.service.ranking")
    async with rig(ranking_config(service_config, ranking=False)) as r:
        now = r.clock.now_utc_us()
        legacy = [
            ("I_735", "priority", "P2"),
            ("I_341", "rank", "2"),
            ("I_9", "rank", "1"),
            ("I_10", "priority", "P0"),
        ]
        deliveries = [
            owner_edit(service_config, "I_9", {"field_node_id": RANK_FIELD, "to": 1}),
            owner_edit(
                service_config, "I_10", {"field_node_id": PRIORITY_FIELD, "to": {"name": "P0"}}
            ),
            # Not evidence: the bot's own write, and an owner Status move.
            owner_edit(service_config, "I_341", {"field_node_id": RANK_FIELD, "to": 2}, BOT_ID),
            owner_edit(
                service_config,
                "I_735",
                {"field_node_id": "PVTSSF_status", "to": {"name": "Triage"}},
            ),
        ]

        def seed(store: Any) -> None:
            with store._txn() as conn:
                for node, name, value in legacy:
                    conn.execute(
                        "INSERT INTO board_fields (issue_node_id, field, owner_value, owner_set, "
                        "updated_at_us) VALUES (?, ?, ?, 1, ?)",
                        (node, name, value, now),
                    )
                for index, body in enumerate(deliveries):
                    conn.execute(
                        "INSERT INTO deliveries (delivery_guid, event_name, action, "
                        "headers_json, body, body_sha256, received_at_us, provenance, status) "
                        "VALUES (?, 'projects_v2_item', 'edited', '{}', ?, 'x', ?, 'webhook', "
                        "'processed')",
                        (f"g{index}", body, now),
                    )

        await r.service.db.call(seed)
        await r.ranker.run_once()
        for node, name, _value in legacy[:2]:
            row = await r.field(node, name)
            assert row is not None and not row.owner_set and row.owner_value is None
        for node, name, value in legacy[2:]:
            row = await r.field(node, name)
            assert row is not None and row.owner_choice and row.owner_value == value
        assert "cleared (no owner webhook backs it) node=I_735 field=priority value=P2" in (
            caplog.text
        )
        assert "node=I_341 field=rank value=2" in caplog.text
        assert "ranking owner choice repair cleared=2 kept=2" in caplog.text
        # Once per process: a second pass does not repeat it.
        caplog.clear()
        await r.ranker.run_once()
        assert "repair" not in caplog.text


@pytest.mark.asyncio
async def test_restart_mid_run_adopts_the_session_and_never_duplicates_writes(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        r.sessions.fail_create = RankingSessionError("create outcome unknown", retry=True)
        r.board.cards_ = [card(1, priority="P2"), card(2)]
        await r.ranker.command({"action": "now"})
        assert (await r.ranker.run_once()).startswith("retrying")
        r.sessions.fail_create = None
        # A restart: a new ranker resumes the same run (one session, one start message).
        ranker = r.new_ranker()
        assert await ranker.run_once() == "started"
        assert await r.new_ranker().run_once() == "running"
        run = await r.run()
        assert run is not None and r.sessions.creates == 1
        assert r.sessions.sent[run.root_id or ""] == [f"ranking:{run.run_id}:start"]
        await r.submit(
            {
                "ranking": [item(1), item(2)],
                "summary": "s",
                "priority_changes": [{"issue": 1, "new_priority": "P1", "reason": "why"}],
            },
            ranker,
        )
        # The comment lands but its response is lost; the status update fails once.
        r.board.failures = [("comment", True, True), ("status_update", True, False)]
        assert await ranker.run_once() == "applying"
        assert await r.new_ranker().run_once() == "applying"
        assert await r.new_ranker().run_once() == "completed"
        assert r.board.comment_posts == 1 and r.board.update_posts == 1
        assert r.board.priorities == [("PVTI_1", "P1")]
        assert sorted(r.board.ranks) == [("PVTI_1", 1), ("PVTI_2", 2)]


@pytest.mark.asyncio
async def test_a_definitive_failure_is_recorded_and_the_status_update_degrades(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        await started(r, card(1, priority="P2"))
        await r.submit(
            {
                "ranking": [item(1)],
                "summary": "s",
                "priority_changes": [{"issue": 1, "new_priority": "P1", "reason": "why"}],
            }
        )
        r.board.failures = [("priority", False, False)] + [("status_update", True, False)] * 5
        for _ in range(5):
            outcome = await r.ranker.run_once()
        assert outcome == "completed"
        assert r.board.priorities == [] and r.board.comment_posts == 0
        assert r.board.update_posts == 0
        [done] = await r.runs()
        assert done.state == "done" and "2 failed" in done.outcome
        # The failed write leaves no factory value behind (no false owner detection).
        prio = await r.field("I_1", "priority")
        assert prio is not None and prio.factory_value == "P2" and not prio.owner_set


@pytest.mark.asyncio
async def test_a_run_that_submits_nothing_fails_backs_off_and_is_archived(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        run = await started(r, card(1))
        r.sessions.idle = True
        assert await r.ranker.run_once() == "running"  # inside the grace
        r.clock.advance(3 * MICROS_PER_MINUTE)
        assert (await r.ranker.run_once()).startswith("failed: the session ended its turn")
        assert r.sessions.archived == [run.root_id] and r.sessions.interrupted == [run.root_id]
        [failed] = await r.runs()
        assert failed.state == "failed" and failed.archived_at_us is not None
        await r.add_triage_results(10)
        assert await r.ranker.run_once() == "backing off after a failed run"
        state = await r.ranker.state()
        assert state.failures == 1 and state.retry_after_us == r.clock.now_utc_us() + (
            MICROS_PER_HOUR
        )
        with pytest.raises(RankingToolError, match="closed"):
            await r.ranker.submit(run.root_id, run.run_id, {"ranking": [item(1)], "summary": "s"})
        # ``now`` overrides the backoff once; automatic runs back off again after.
        await r.ranker.command({"action": "now"})
        assert await r.ranker.run_once() == "started"
        r.clock.advance(3 * MICROS_PER_MINUTE)
        assert (await r.ranker.run_once()).startswith("failed: ")
        assert await r.ranker.run_once() == "backing off after a failed run"


@pytest.mark.asyncio
async def test_a_run_abandoned_across_a_restart_times_out_and_is_archived(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        run = await started(r, card(1))
        r.clock.advance(31 * MICROS_PER_MINUTE)
        assert (await r.new_ranker().run_once()).startswith("failed: no ranking was submitted")
        assert r.sessions.archived == [run.root_id]
        # An archive that failed earlier is retried by the next pass.
    async with rig(ranking_config(service_config)) as r:
        run = await started(r, card(1))
        r.sessions.gone = True
        assert (await r.ranker.run_once()).startswith("failed: the ranking session is gone")


@pytest.mark.asyncio
async def test_old_finished_runs_are_pruned_only_after_their_session_is_deleted(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config, ranking_status_update=False)) as r:
        for _ in range(12):
            await started(r, card(1))
            await r.submit({"ranking": [item(1)], "summary": "s"})
            assert await r.ranker.run_once() == "completed"
        repo = service_config.repo_id
        assert await r.service.db.call(lambda s: rs.prune_runs(s, repo)) == 0
        assert await r.service.db.call(lambda s: rs.prune_runs(s, repo, require_deleted=False)) == 2
        assert len(await r.runs()) == 10


# ------------------------------------------------------------------ MCP


@pytest.mark.asyncio
async def test_mcp_ranking_tools_guard_session_and_run(service_config: ServiceConfig):
    from omnigent_factory.service.directory import ServiceDispatchDirectory

    async with rig(ranking_config(service_config)) as r:
        run = await started(r, card(1))
        tools = FactoryTools(
            r.service, ServiceDispatchDirectory(r.service.db, r.service.config), r.service.config
        )
        tools.ranker = r.ranker
        with pytest.raises(FactoryToolError, match="not a ranking session"):
            await tools.submit_ranking("conv_other", run.run_id, [item(1)], "s")
        with pytest.raises(FactoryToolError, match="is not this session's ranking run"):
            await tools.submit_ranking(run.root_id or "", "rk_stale", [item(1)], "s")
        with pytest.raises(FactoryToolError, match="this is a ranking session"):
            await tools.get_status(run.root_id or "")
        with pytest.raises(FactoryToolError, match="issue: required"):
            await tools.get_issue(run.root_id or "")
        with pytest.raises(FactoryToolError, match="invalid ranking"):
            await tools.submit_ranking(run.root_id or "", run.run_id, [], "s")
        receipt = await tools.submit_ranking(run.root_id or "", run.run_id, [item(1)], "s")
        assert receipt["accepted"] is True
        replay = await tools.submit_ranking(run.root_id or "", run.run_id, [item(1)], "s")
        assert replay["replayed"] is True
        with pytest.raises(FactoryToolError, match="ranking run is closed"):
            await tools.get_issue(run.root_id or "", 1)


# ------------------------------------------------------------------ operator and config


def test_cli_ranking_and_sessions_prune_subcommands(monkeypatch: pytest.MonkeyPatch, tmp_path: Any):
    from omnigent_factory import cli

    calls: list[tuple[str, dict[str, object] | None]] = []
    monkeypatch.setattr(cli, "_load", lambda args: (tmp_path, object()))
    monkeypatch.setattr(
        cli, "_operator", lambda config, command, args=None, **kw: calls.append((command, args))
    )
    for argv in (["ranking", "status"], ["ranking", "on"], ["ranking", "off"], ["ranking", "now"]):
        cli.main(argv)
    cli.main(["sessions", "prune", "--dry-run"])
    cli.main(["sessions", "prune"])
    assert calls == [
        ("ranking", {"action": "status"}),
        ("ranking", {"action": "on"}),
        ("ranking", {"action": "off"}),
        ("ranking", {"action": "now"}),
        ("sessions-prune", {"dry_run": True}),
        ("sessions-prune", {"dry_run": False}),
    ]


def test_ranking_and_retention_keys_reload_hot_with_safe_defaults():
    from omnigent_factory.service.config import HOT_RELOAD_KEYS

    assert {
        "ranking",
        "ranking_min_new_triages",
        "ranking_status_update",
        "rank_field_node_id",
        "session_retention_days",
    } <= HOT_RELOAD_KEYS
    config = ServiceConfig(repo_id="R", owners=frozenset({1}))
    assert (config.ranking, config.ranking_min_new_triages, config.ranking_status_update) == (
        False,
        5,
        True,
    )
    assert config.session_retention_days == 30


@pytest.mark.asyncio
async def test_operator_ranking_command(service_config: ServiceConfig):
    async with rig(ranking_config(service_config)) as r:
        status = await r.service.operator_command("ranking", {"action": "status"})
        assert status["enabled"] is True and status["configured"] is True
        with pytest.raises(ValueError, match="status, on, off or now"):
            await r.service.operator_command("ranking", {"action": "bogus"})


@pytest.mark.asyncio
async def test_a_run_failing_before_its_create_is_known_still_archives_its_session(
    service_config: ServiceConfig,
):
    async with rig(ranking_config(service_config)) as r:
        r.board.cards_ = [card(1)]
        await r.ranker.command({"action": "now"})
        # The create landed but its answer was lost, and the setup then timed out.
        r.sessions.fail_create = RankingSessionError("create outcome unknown", retry=True)
        assert (await r.ranker.run_once()).startswith("retrying")
        run = await r.run()
        assert run is not None
        r.sessions.roots[run.nonce] = "conv_lost"
        r.clock.advance(16 * MICROS_PER_MINUTE)
        assert (await r.ranker.run_once()).startswith("failed: the session could not be created")
        assert r.sessions.archived == ["conv_lost"]
        [failed] = await r.runs()
        assert failed.root_id == "conv_lost" and failed.archived_at_us is not None
        # A run whose create never landed has nothing to archive and is settled too.
        r.clock.advance(2 * MICROS_PER_HOUR)
        await r.ranker.command({"action": "now"})
        assert (await r.ranker.run_once()).startswith("retrying")
        r.clock.advance(16 * MICROS_PER_MINUTE)
        assert (await r.ranker.run_once()).startswith("failed")
        newest = (await r.runs())[0]
        assert newest.root_id is None and newest.archived_at_us is not None
