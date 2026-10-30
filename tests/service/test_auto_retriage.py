"""Stale triage and idle-time auto-retriage in the service: signals, flags, selection,
budget, refusals, the operator command and the status comment header."""

from __future__ import annotations

import asyncio
import subprocess
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory import cli
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.types import MICROS_PER_HOUR, Hold, Parcel, SessionKind, Stage, Via
from omnigent_factory.credentials.worktree import Workspaces, WorktreeError
from omnigent_factory.ports.github import BoardIssue, RankingCard
from omnigent_factory.service.auto_retriage import (
    BASE_MOVED,
    DUPLICATE_CLOSED,
    MAX_AGE,
    PATHS_CHANGED,
    RELATED_CLOSED,
    AutoRetriager,
    GitFacts,
    Signal,
    Stale,
    order,
    refusal,
    signals,
)
from omnigent_factory.service.board_index import BoardIndex
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.directory import TriageFreshness
from omnigent_factory.testing.builders import EventFactory, snapshot
from tests.service.test_auto_triage import Rig, finish, issue, rig, triage_running
from tests.service.test_pilot_677 import eventually

DAY = 24 * MICROS_PER_HOUR
OID = "a" * 40
HEAD = "b" * 40


def fresh(**over: Any) -> TriageFreshness:
    values: dict[str, Any] = {
        "session_id": "ss_1",
        "accepted_at_us": 1_000 * DAY,
        "base_oid": OID,
        "recommendation": "fix",
    }
    values.update(over)
    return TriageFreshness(**values)


def found(f: TriageFreshness, *, age_days: float, closed=frozenset(), git=None):
    return signals(
        f,
        now_us=f.accepted_at_us + int(age_days * DAY),
        min_age_us=7 * DAY,
        max_age_us=30 * DAY,
        min_commits=50,
        closed=frozenset(closed),
        git=git or GitFacts(),
    )


# ------------------------------------------------------------------ signals


def test_signals_count_only_once_the_triage_is_old_enough():
    f = fresh(paths=("src/a.py",), duplicate_issue=5, related=(6,))
    git = GitFacts(commits=500, changed=("src/a.py",))
    shut = {5, 6}
    assert found(f, age_days=6.9, closed=shut, git=git) == []  # young: nothing at all
    assert [s.kind for s in found(f, age_days=7, closed=shut, git=git)] == [
        DUPLICATE_CLOSED,
        RELATED_CLOSED,
        PATHS_CHANGED,
        BASE_MOVED,
    ]
    assert found(fresh(), age_days=29) == []
    [old] = found(fresh(), age_days=30)
    assert old == Signal(MAX_AGE, "triaged 30 days ago")  # max age: regardless
    assert found(fresh(accepted_at_us=0), age_days=99) == []  # unknown age: never


def test_signal_texts_name_what_changed():
    f = fresh(paths=("a", "b", "c", "d"), duplicate_issue=5, related=(5, 6, 7))
    texts = [
        s.text
        for s in found(
            f,
            age_days=8,
            closed={5, 6},
            git=GitFacts(commits=49, changed=("a", "b", "c", "d")),
        )
    ]
    assert texts == [
        "duplicate #5 closed",
        "related #6 closed",
        "cited paths changed: a, b, c (+1)",
    ]  # 49 commits stay under retriage_min_commits
    assert found(fresh(), age_days=8, git=GitFacts(commits=50))[0].text == (
        "50 commits on the base since"
    )
    # Unknown issue states (None) or unknown git: those signals are simply absent; an
    # issue GitHub does not report closed (open, or unknown) is never "closed".
    assert found(fresh(related=(9,), duplicate_issue=4), age_days=8, closed={10}) == []
    assert (
        signals(
            fresh(related=(9,)),
            now_us=fresh().accepted_at_us + 8 * DAY,
            min_age_us=7 * DAY,
            max_age_us=30 * DAY,
            min_commits=50,
            closed=None,
            git=GitFacts(),
        )
        == []
    )


def _stale(number: int, kind: int, *, close: bool = False, rank: float | None = None) -> Stale:
    board = issue(number, stage=Stage.TRIAGED)
    parcel = Parcel(parcel_id=board.node_id, repo_id="R", issue_number=number)
    return Stale(board, parcel, (Signal(kind, "x"),), close_candidate=close, rank=rank)


def test_order_close_candidates_first_then_strongest_signal_then_rank():
    picked = order(
        [
            _stale(1, BASE_MOVED, rank=1.0),
            _stale(2, PATHS_CHANGED, rank=5.0),
            _stale(3, PATHS_CHANGED, rank=2.0),
            _stale(4, MAX_AGE, close=True),
            _stale(5, PATHS_CHANGED),  # no Rank: after ranked ones
            _stale(6, DUPLICATE_CLOSED),
        ]
    )
    assert [s.issue.number for s in picked] == [4, 6, 3, 2, 5, 1]


def test_refusal_rules():
    board = issue(1, stage=Stage.TRIAGED)
    quiet = Parcel(
        parcel_id=board.node_id,
        repo_id="R",
        issue_number=1,
        eligible=True,
        holds=frozenset({Hold.AWAITING_OWNER}),
    )
    assert refusal(board, quiet) is None  # a posted triage's resting hold is fine
    assert refusal(replace(board, assigned=True), quiet) == "assigned"
    assert refusal(replace(board, labels=("Factory:Skip",)), quiet) == "skip_label"
    assert refusal(board, replace(quiet, holds=frozenset({Hold.BUNDLED}))) == "bundled"
    assert refusal(board, replace(quiet, holds=frozenset({Hold.STOPPED}))) == "held"
    assert refusal(board, replace(quiet, pending_authorization_id="au_1")) == "run_in_flight"
    assert refusal(board, quiet, blocked=lambda _n: True) == "parked"


# ------------------------------------------------------------------ git (source clone)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603 - test-controlled argv
        ["git", *args],  # noqa: S607
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_workspaces_answer_commits_and_changed_paths_from_the_fetched_base(tmp_path: Path):
    repo = tmp_path / "clone"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    (repo / "src").mkdir()
    (repo / "src" / "a.py").write_text("a\n")
    (repo / "b.py").write_text("b\n")
    (repo / "*.py").write_text("literal\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "one")
    triaged = _git(repo, "rev-parse", "HEAD")
    (repo / "src" / "a.py").write_text("a2\n")
    _git(repo, "commit", "-qam", "two")
    (repo / "README").write_text("r\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "three")
    head = _git(repo, "rev-parse", "HEAD")
    _git(repo, "update-ref", "refs/remotes/origin/main", head)
    ws = Workspaces(repo, [tmp_path], "o/r")
    assert ws.base_head("main") == head
    assert ws.commits_since(triaged, head) == 2
    assert ws.changed_since(triaged, head, ["src", "b.py", "missing.txt"]) == ("src/a.py",)
    # Pathspecs are literal: "*.py" names that one file, never a glob.
    assert ws.changed_since(triaged, head, ["*.py"]) == ()
    assert ws.changed_since(triaged, head, []) == ()
    # The captured OID, never the moving ref: a later fetch does not change the answer.
    (repo / "b.py").write_text("b2\n")
    _git(repo, "commit", "-qam", "four")
    _git(repo, "update-ref", "refs/remotes/origin/main", _git(repo, "rev-parse", "HEAD"))
    assert ws.commits_since(triaged, head) == 2
    assert ws.changed_since(triaged, head, ["b.py"]) == ()
    for bad in ("HEAD", "--all", "abc", "refs/remotes/origin/main"):
        with pytest.raises(WorktreeError, match="full object id"):
            ws.commits_since(bad, head)
        with pytest.raises(WorktreeError, match="full object id"):
            ws.commits_since(triaged, bad)


# ------------------------------------------------------------------ service rig


@dataclass
class _Workspaces:
    head: str = HEAD
    commits: int = 0
    changed: tuple[str, ...] = ()
    calls: list[str] = field(default_factory=list)
    delay: float = 0.0

    def base_head(self, base_branch: str = "main") -> str:
        self.calls.append("head")
        return self.head

    def commits_since(self, oid: str, head: str) -> int:
        self.calls.append(f"commits {oid[:4]}..{head[:4]}")
        if self.delay:
            import time

            time.sleep(self.delay)
        return self.commits

    def changed_since(self, oid: str, head: str, paths: Any) -> tuple[str, ...]:
        self.calls.append(f"changed {oid[:4]}..{head[:4]}")
        return tuple(p for p in self.changed if p in paths)


class _Directory:
    def __init__(self) -> None:
        self.fresh: dict[str, TriageFreshness] = {}

    async def triage_freshness(self, parcel: Parcel) -> TriageFreshness | None:
        return self.fresh.get(parcel.parcel_id)


@dataclass
class Retriage:
    r: Rig
    retriager: AutoRetriager
    directory: _Directory
    workspaces: _Workspaces
    ranks: dict[str, float | None]
    states: dict[int, bool]
    reads: list[tuple[int, ...]]

    async def parcel(self, pid: str) -> Parcel:
        p = await self.r.parcel(pid)
        assert p is not None
        return p

    async def triaged(self, number: int, **fresh_over: Any) -> str:
        """A real triage run on #number, finished and posted (a quiet Triaged card)."""
        r = self.r
        pid = f"I_auto_{number}"
        r.board.append(issue(number, stage=Stage.TRIAGED))
        r.github.snapshots[pid] = snapshot(stage=Stage.TRIAGED, read_at_us=r.clock.now_utc_us())
        factory = EventFactory(pid, issue_number=number, start_us=r.clock.now_utc_us())
        await r.service.apply_event(
            factory.make(
                ev.GitHubSnapshot(),
                evidence=snapshot(read_at_us=factory.now),
                event_id=f"{pid}:setup-read",
            )
        )
        await r.service.apply_event(
            factory.make(ev.RequestTriage(via=Via.DRAG), event_id=f"{pid}:setup-triage")
        )
        await triage_running(r, pid)
        await finish(r, pid)

        async def posted() -> bool:
            return Hold.AWAITING_OWNER in (await self.parcel(pid)).holds

        await eventually(posted)
        newest = [s for s in (await self.parcel(pid)).sessions if s.kind == SessionKind.TRIAGE]
        fresh_over.setdefault("session_id", newest[-1].session_id)
        self.directory.fresh[pid] = fresh(
            accepted_at_us=r.clock.now_utc_us() - 40 * DAY, **fresh_over
        )
        return pid

    async def count(self) -> int:
        rows = await self.r.service.db.call(
            lambda store: store.query(
                "SELECT COUNT(*) FROM events WHERE kind = ? AND accepted = 1",
                (EventKind.AUTO_RETRIAGE.value,),
            )
        )
        return int(rows[0][0])


async def finish_again(r: Rig, pid: str, tag: str) -> None:
    """``finish`` for a later run of the same card (its own event ids)."""
    from omnigent_factory.core.types import Lifecycle
    from omnigent_factory.testing.builders import result_candidate

    async def active() -> bool:
        p = await r.parcel(pid)
        run = p.current_session if p is not None else None
        return run is not None and run.lifecycle == Lifecycle.ACTIVE

    await eventually(active)
    parcel = await r.parcel(pid)
    assert parcel is not None and parcel.current_session is not None
    run = parcel.current_session
    assert run.root_id is not None
    factory = EventFactory(pid, start_us=r.clock.now_utc_us())
    result = await r.service.apply_event(
        factory.make(
            result_candidate(run.session_id, run.root_id, run.revision, ev.ResultKind.TRIAGE),
            event_id=f"{pid}:{tag}-result",
        )
    )
    assert result.accepted, result.reason
    await r.service.apply_event(
        factory.make(
            ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=False),
            event_id=f"{pid}:{tag}-quiet",
        )
    )


@asynccontextmanager
async def retriage_rig(config: ServiceConfig, **over: Any) -> AsyncIterator[Retriage]:
    async with rig(config.model_copy(update=over)) as r:

        async def read() -> list[BoardIssue]:
            return list(r.board)

        ranks: dict[str, float | None] = {}

        async def cards() -> list[RankingCard] | RetryableReadFailure:
            return [
                RankingCard(
                    i.node_id, "", i.number, i.title, i.stage, 0, ranks.get(i.node_id), None
                )
                for i in r.board
            ]

        directory = _Directory()
        workspaces = _Workspaces()
        states: dict[int, bool] = {}

        async def issue_states(numbers: Any) -> dict[int, bool]:
            reads.append(tuple(numbers))
            return {n: states[n] for n in numbers if n in states}

        reads: list[tuple[int, ...]] = []
        retriager = AutoRetriager(
            r.service,
            BoardIndex(read, r.clock, ttl_us=0),
            r.github.issue_snapshot,
            directory=directory,  # type: ignore[arg-type]
            workspaces=workspaces,  # type: ignore[arg-type]
            triager=r.triager,
            cards=cards,
            issue_states=issue_states,
        )
        r.service.auto_retriager = retriager
        yield Retriage(r, retriager, directory, workspaces, ranks, states, reads)


@pytest.mark.asyncio
async def test_stale_cards_are_flagged_even_while_auto_retriage_is_off(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config) as t:
        pid = await t.triaged(3)
        young = await t.triaged(4)
        t.directory.fresh[young] = fresh(accepted_at_us=t.r.clock.now_utc_us() - DAY)
        assert await t.retriager.run_once() == "disabled"
        p = await t.parcel(pid)
        assert p.triage_stale == "triaged 40 days ago"
        assert p.board_note == "Triage may be stale: triaged 40 days ago"
        assert (await t.parcel(young)).triage_stale == ""
        assert await t.count() == 0
        # Signals gone (a fresh triage): the flag clears.
        t.directory.fresh[pid] = fresh(accepted_at_us=t.r.clock.now_utc_us())
        await t.retriager.scan(force=True)
        assert (await t.parcel(pid)).triage_stale == ""


@pytest.mark.asyncio
async def test_on_and_idle_it_retriages_one_stale_card_from_the_clock(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config, auto_retriage=True) as t:
        t.workspaces.changed = ("src/a.py",)
        pid = await t.triaged(3, paths=("src/a.py",))
        assert await t.retriager.run_once() == "started #3"
        await triage_running(t.r, pid)
        p = await t.parcel(pid)
        assert p.stage == Stage.TRIAGED and p.triage_stale == ""
        rows = await t.r.service.db.call(
            lambda store: store.query(
                "SELECT provenance, actor_id, accepted FROM events WHERE kind = ?",
                (EventKind.AUTO_RETRIAGE.value,),
            )
        )
        assert [tuple(row) for row in rows] == [("scheduler", None, 1)]
        # The run holds the one triage slot: nothing more starts.
        assert await t.retriager.run_once() == "not idle: every triage slot is taken"


@pytest.mark.asyncio
async def test_daily_budget_and_grant(service_config: ServiceConfig):
    async with retriage_rig(service_config, auto_retriage=True, auto_retriage_daily_limit=1) as t:
        first = await t.triaged(3)
        await t.triaged(4)
        assert await t.retriager.run_once() == "started #3"
        await finish_again(t.r, first, "retriage")
        assert await t.retriager.run_once() == "daily budget used (1/1)"
        status = await t.r.service.operator_command(
            "auto-retriage", {"action": "grant", "count": 1}
        )
        assert status["granted_today"] == 1 and status["remaining_today"] == 1
        assert await t.retriager.run_once() == "started #4"
        assert await t.count() == 2
        t.r.clock.advance(DAY)  # a new local day: a fresh budget, the grant is gone
        budget = await t.retriager.budget()
        assert (budget.used, budget.granted, budget.remaining) == (0, 0, 1)


@pytest.mark.asyncio
async def test_close_candidates_go_first_then_the_strongest_signal_then_rank(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config, auto_retriage=True) as t:
        t.workspaces.changed = ("src/a.py",)
        old = await t.triaged(3)  # only old
        changed = await t.triaged(4, paths=("src/a.py",))
        await t.triaged(5, paths=("src/a.py",))
        close = await t.triaged(6, recommendation="wont_fix")  # only old, but a candidate
        t.ranks.update({changed: 9.0, "I_auto_5": 1.0})
        status = await t.retriager.status()
        assert [c["issue"] for c in status["candidates"]] == [6, 5, 4, 3]  # type: ignore[union-attr,index]
        assert status["next_candidate"]["close_candidate"] is True  # type: ignore[index]
        assert await t.retriager.run_once() == "started #6"
        await finish_again(t.r, close, "retriage")
        _ = old


@pytest.mark.asyncio
async def test_a_held_or_skipped_card_is_never_taken_and_keeps_its_hold(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config, auto_retriage=True) as t:
        stopped = await t.triaged(3)
        factory = EventFactory(stopped, issue_number=3, start_us=t.r.clock.now_utc_us())
        await t.r.service.apply_event(factory.make(ev.Stop(), event_id=f"{stopped}:stop"))
        assert Hold.STOPPED in (await t.parcel(stopped)).holds
        await t.triaged(4)
        t.r.board[-1] = replace(t.r.board[-1], labels=("factory:skip",))
        status = await t.retriager.status()
        refused = {c["issue"]: c["refused"] for c in status["candidates"]}  # type: ignore[union-attr]
        assert refused == {3: "held", 4: "skip_label"}
        assert status["next_candidate"] is None
        assert await t.retriager.run_once() == "no eligible stale triage"
        assert Hold.STOPPED in (await t.parcel(stopped)).holds  # never cleared
        assert await t.count() == 0


@pytest.mark.asyncio
async def test_inbox_auto_triage_goes_first(service_config: ServiceConfig):
    async with retriage_rig(service_config, auto_retriage=True, auto_triage=True) as t:
        await t.triaged(3)
        t.r.inbox(9)
        outcome = await t.retriager.run_once()
        assert outcome == "not idle: Inbox auto-triage has an issue to start"
        # Its budget used up: Inbox work no longer waits ahead of re-triage.
        await t.r.service.operator_command("auto-triage", {"action": "off"})
        assert await t.retriager.run_once() == "started #3"


@pytest.mark.asyncio
async def test_operator_toggle_wins_until_the_config_changes(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        status = await t.r.service.operator_command("auto-retriage", {"action": "on"})
        assert (status["enabled"], status["enabled_source"]) == (True, "operator")
        await t.r.service.operator_command("auto-retriage", {"action": "off"})
        assert await t.retriager.enabled() == (False, "operator")
        # The config changing (to on) is the newer intent: it wins over the override.
        t.r.service.config = t.r.service.config.model_copy(update={"auto_retriage": True})
        assert await t.retriager.enabled() == (True, "config")
        with pytest.raises(ValueError, match="1-1000"):
            await t.r.service.operator_command("auto-retriage", {"action": "grant", "count": 0})
        with pytest.raises(ValueError, match="status, on, off or grant"):
            await t.r.service.operator_command("auto-retriage", {"action": "reset"})


@pytest.mark.asyncio
async def test_auto_retriage_command_is_refused_when_not_wired(service_config: ServiceConfig):
    async with rig(service_config) as r:
        with pytest.raises(ValueError, match="auto-retriage is not wired"):
            await r.service.operator_command("auto-retriage", {"action": "status"})


def test_cli_auto_retriage_subcommands(monkeypatch: pytest.MonkeyPatch, tmp_path: Any):
    calls: list[tuple[str, Mapping[str, object] | None]] = []
    monkeypatch.setattr(cli, "_load", lambda args: (tmp_path, object()))
    monkeypatch.setattr(
        cli, "_operator", lambda config, command, args=None, **kw: calls.append((command, args))
    )
    for argv in (["status"], ["on"], ["off"], ["grant", "3"]):
        cli.main(["auto-retriage", *argv])
    assert calls == [
        ("auto-retriage", {"action": "status"}),
        ("auto-retriage", {"action": "on"}),
        ("auto-retriage", {"action": "off"}),
        ("auto-retriage", {"action": "grant", "count": 3}),
    ]


# ------------------------------------------------------------------ config


def test_config_defaults_validation_and_hot_reload(service_config: ServiceConfig):
    c = service_config
    assert (c.auto_retriage, c.auto_retriage_daily_limit) == (False, 5)
    assert (c.retriage_min_age_days, c.retriage_min_commits, c.retriage_max_age_days) == (
        7.0,
        50,
        30.0,
    )
    assert {
        "auto_retriage",
        "auto_retriage_daily_limit",
        "retriage_min_age_days",
        "retriage_min_commits",
        "retriage_max_age_days",
    } <= HOT_RELOAD_KEYS
    data = c.model_dump()
    for bad in (
        {"retriage_max_age_days": 3, "retriage_min_age_days": 7},
        {"auto_retriage_daily_limit": -1},
        {"retriage_min_commits": 0},
        {"retriage_max_age_days": 0},
    ):
        with pytest.raises(ValueError):
            ServiceConfig.model_validate({**data, **bad})


def test_setup_template_and_readme_document_the_keys():
    from omnigent_factory.service import setup

    template = Path(setup.__file__).read_text("utf-8")
    readme = (Path(__file__).parents[2] / "README.md").read_text("utf-8")
    for key in (
        "auto_retriage = false",
        "auto_retriage_daily_limit = 5",
        "retriage_min_age_days = 7",
        "retriage_min_commits = 50",
        "retriage_max_age_days = 30",
    ):
        assert key in template and key in readme
    assert "`auto-retriage status`" in readme and "factory:close-candidate" in readme


# ------------------------------------------------------------------ status header


@pytest.mark.asyncio
async def test_status_header_text(service_config: ServiceConfig):
    async with retriage_rig(service_config) as t:
        pid = await t.triaged(3)
        p = await t.parcel(pid)
        accepted = t.directory.fresh[pid].accepted_at_us
        from datetime import UTC, datetime

        day = datetime.fromtimestamp(accepted / 1e6, UTC).strftime("%Y-%m-%d")
        t.workspaces.commits = 52
        assert (
            await t.retriager.header(p) == f"Triaged {day} against main@aaaaaaa · 52 commits behind"
        )
        t.workspaces.commits = 1
        # Cached per (triage OID, base OID): the count is not asked again.
        assert (await t.retriager.header(p)).endswith("52 commits behind")
        t.workspaces.head = "c" * 40
        t.retriager._head = None  # the base moved (cache of the base OID expired)
        assert (await t.retriager.header(p)).endswith("· 1 commit behind")
        t.directory.fresh[pid] = replace(t.directory.fresh[pid], base_oid="")
        assert await t.retriager.header(p) == f"Triaged {day}"
        t.directory.fresh.pop(pid)
        assert await t.retriager.header(p) == ""


@pytest.mark.asyncio
async def test_status_header_never_waits_on_a_slow_count(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    from omnigent_factory.service import auto_retriage as module

    monkeypatch.setattr(module, "HEADER_GIT_TIMEOUT_S", 0.05)
    async with retriage_rig(service_config) as t:
        pid = await t.triaged(3)
        p = await t.parcel(pid)
        t.workspaces.commits, t.workspaces.delay = 7, 0.5
        text = await t.retriager.header(p)
        assert text.endswith("against main@aaaaaaa")  # no count: never waited on
        await asyncio.sleep(0.6)
        t.workspaces.delay = 0.0
        assert (await t.retriager.header(p)).endswith("· 7 commits behind")  # cached since


# ------------------------------------------------------------------ doctor


@pytest.mark.asyncio
async def test_doctor_line_shows_switch_settings_and_flagged_cards(service_config: ServiceConfig):
    from omnigent_factory.service.doctor import DoctorReport, _check_auto_retriage

    report = DoctorReport()
    _check_auto_retriage(service_config, report)  # no database yet
    assert report.checks["auto_retriage"].startswith("off (config); at most 5 per day")
    async with retriage_rig(service_config) as t:
        await t.triaged(3)
        await t.retriager.run_once()  # flags #3 (off: nothing starts)
        await t.r.service.operator_command("auto-retriage", {"action": "on"})
        report = DoctorReport()
        _check_auto_retriage(service_config, report)
    line = report.checks["auto_retriage"]
    assert report.ok and line.startswith("on (operator)")
    assert "stale after 7 days with a signal (50 commits" in line
    assert line.endswith("1 triaged cards flagged stale")


# ------------------------------------------------------------------ review fixes


@pytest.mark.asyncio
async def test_a_triage_published_after_the_scan_is_never_refreshed(
    service_config: ServiceConfig,
):
    """Scan, then the owner's comment re-triages the card: the cached candidate is
    re-checked before the start and skipped, without spending budget."""
    async with retriage_rig(service_config, auto_retriage=True) as t:
        pid = await t.triaged(3)
        [item] = await t.retriager.scan()
        factory = EventFactory(pid, issue_number=3, start_us=t.r.clock.now_utc_us())
        await t.r.service.apply_event(
            factory.make(
                ev.PlanFeedback(text_digest="d" * 64),
                event_id=f"{pid}:owner-comment",
                evidence=snapshot(stage=Stage.TRIAGED, read_at_us=factory.now),
            )
        )
        await finish_again(t.r, pid, "owner")

        async def posted() -> bool:
            return Hold.AWAITING_OWNER in (await t.parcel(pid)).holds

        await eventually(posted)
        newest = [s for s in (await t.parcel(pid)).sessions if s.kind == SessionKind.TRIAGE]
        assert newest[-1].session_id != item.generation
        # Its freshness still reads stale (e.g. old paths changed): only the generation moved.
        t.directory.fresh[pid] = replace(t.directory.fresh[pid], session_id=newest[-1].session_id)
        assert t.retriager._scan is not None  # the cached scan still lists the old judgement
        assert await t.retriager.run_once() == "no eligible stale triage"
        assert await t.count() == 0
        assert (await t.retriager.budget()).used == 0
        assert t.retriager._scan is None  # dropped: the next pass scans again
        assert await t.retriager.run_once() == "started #3"  # judged on the new triage


@pytest.mark.asyncio
async def test_closed_signals_use_github_issue_state_not_the_board(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config) as t:
        pid = await t.triaged(3, duplicate_issue=12, related=(13, 14, 15))
        t.r.board.append(issue(15, stage=Stage.TRIAGED))  # on the board: open, not read
        t.directory.fresh[pid] = replace(
            t.directory.fresh[pid], accepted_at_us=t.r.clock.now_utc_us() - 8 * DAY
        )
        # #12 open but removed from the project, #13 unknown (not reported): no signal.
        t.states.update({12: True, 14: True})
        assert await t.retriager.scan(force=True) == []
        assert t.reads == [(12, 13, 14)]
        t.states.update({12: False, 14: False})
        [item] = await t.retriager.scan(force=True)
        assert [s.text for s in item.signals] == ["duplicate #12 closed", "related #14 closed"]

        async def unreadable(numbers: Any) -> dict[int, bool]:
            raise RuntimeError("GitHub down")

        t.retriager.issue_states = unreadable  # fail closed: unknown is never closed
        assert await t.retriager.scan(force=True) == []


@pytest.mark.asyncio
async def test_no_git_work_before_the_minimum_age_and_ranges_use_the_captured_oid(
    service_config: ServiceConfig,
):
    async with retriage_rig(service_config) as t:
        young = await t.triaged(3, paths=("src/a.py",))
        t.directory.fresh[young] = replace(
            t.directory.fresh[young], accepted_at_us=t.r.clock.now_utc_us() - 6 * DAY
        )
        await t.retriager.scan(force=True)
        assert t.workspaces.calls == ["head"]  # the base OID only, once per pass
        old = await t.triaged(4, paths=("src/a.py",))
        assert old
        t.workspaces.calls.clear()
        await t.retriager.scan(force=True)
        assert t.workspaces.calls == ["head", "commits aaaa..bbbb", "changed aaaa..bbbb"]
        t.workspaces.calls.clear()
        await t.retriager.scan(force=True)
        assert t.workspaces.calls == ["head"]  # cached per (triage OID, base OID)


@pytest.mark.asyncio
async def test_git_work_per_pass_is_bounded_and_resumes_next_pass(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    from omnigent_factory.service import auto_retriage as module

    monkeypatch.setattr(module, "MAX_GIT_CARDS_PER_PASS", 1)
    async with retriage_rig(service_config) as t:
        t.workspaces.commits = 60  # every old card is stale by commits
        for n in (3, 4, 5):
            await t.triaged(n, base_oid=f"{n}" * 40)
        first = await t.retriager.scan(force=True)
        assert len(first) == 1 and len([c for c in t.workspaces.calls if "commits" in c]) == 1
        second = await t.retriager.scan(force=True)
        assert len(second) == 2  # the cached card plus one more
        third = await t.retriager.scan(force=True)
        assert len(third) == 3
        flagged = [(await t.parcel(f"I_auto_{n}")).triage_stale for n in (3, 4, 5)]
        assert all("60 commits on the base since" in text for text in flagged)
