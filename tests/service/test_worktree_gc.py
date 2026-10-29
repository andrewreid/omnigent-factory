"""Worktree housekeeping: extra ``factory-issue-<N>-*`` worktrees go at Done, the sweep
cleans up closed issues and idle early-stage worktrees, and the clone is pruned and gc'd
under the clone lock. Real Git.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core.effects import EffectIntent, EffectKind, RetryableReadFailure
from omnigent_factory.core.types import Hold, Lifecycle, Parcel, SessionKind, Stage
from omnigent_factory.credentials.worktree import CLONE_LOCK
from omnigent_factory.service import worktree_gc as gc_mod
from omnigent_factory.service.cleanup import CleanupAdapter, WorkspaceCleaner, issue_of
from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig
from omnigent_factory.service.doctor import DoctorReport, _check_worktrees
from omnigent_factory.service.worktree_gc import (
    WorktreeGC,
    github_issue_states,
    read_state,
    state_path,
)
from omnigent_factory.testing.harness import Harness
from tests.credentials.repos import GitEnv, git
from tests.service.conftest import OWNER_ID, REPO_ID
from tests.service.test_devenv_lifecycle import CTX, TEMPLATES, _effect, _runs, _script
from tests.service.test_followups import _Directory, _finished_parcel, _worktree, git_env

__all__ = ["git_env"]

NOW_S = 2_000_000_000.0
DAY = 86400


def _add(git_env: GitEnv, name: str, branch: str | None, *, root: Path | None = None) -> Path:
    """A worktree of the clone at ``<root>/<name>`` (new ``branch``, or detached)."""
    path = (root or git_env.worktrees) / name
    args = ["-b", branch] if branch else ["--detach"]
    git("worktree", "add", *args, str(path), "origin/main", cwd=git_env.source)
    return path.resolve()


def _branches(git_env: GitEnv) -> set[str]:
    return set(git("branch", "--format=%(refname:short)", cwd=git_env.source).split())


def _registered(git_env: GitEnv) -> set[Path]:
    out = git("worktree", "list", "--porcelain", cwd=git_env.source)
    return {Path(line[9:]).resolve() for line in out.splitlines() if line.startswith("worktree ")}


def _age(ws: Any, path: Path, days: float) -> None:
    """Make real reflog activity and relevant mtimes ``days`` old."""
    then = NOW_S - days * DAY
    admin = ws.admin_dir(path)
    reflog = admin / "logs" / "HEAD"
    text = reflog.read_text(encoding="utf-8")
    reflog.write_text(
        re.sub(r" \d+ [+-]\d{4}\t", f" {int(then)} +0000\t", text),
        encoding="utf-8",
    )
    for item in (path, admin / "index", admin / "HEAD", admin / "logs" / "HEAD"):
        if item.exists():
            os.utime(item, (then, then))


def _age_entry(path: Path, days: float) -> None:
    then = NOW_S - days * DAY
    paths = (path, *path.rglob("*")) if path.is_dir() else (path,)
    for item in reversed(paths):
        os.utime(item, (then, then), follow_symlinks=False)


# ------------------------------------------------------------------ naming


def test_issue_number_comes_from_the_directory_name():
    assert issue_of("factory-issue-9") == 9
    assert issue_of("factory-issue-627-p0") == 627
    assert issue_of("factory-issue-1-2") == 1
    assert issue_of("factory-issue-884-blob-a") == 884
    for other in (
        "674-a-docs",
        "884-blob",
        "factory-issue-",
        "factory-issue-x",
        "factory-issue-9x",
    ):
        assert issue_of(other) is None


def test_build_templates_tell_the_agent_where_extra_worktrees_go():
    from omnigent_factory.service import directory as d

    names = {
        d._FIRST_TEMPLATES[SessionKind.BUILD],
        d._REWORK_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
        d._CONFLICT_TEMPLATE,
    }
    for name in names:
        text = " ".join((TEMPLATES / name).read_text("utf-8").split())
        assert "factory-issue-{issue_number}-<suffix>" in text, name
        assert "never in /tmp or under other names" in text, name
    # In-flight runs stay pinned to the bytes they were dispatched with.
    assert "factory-issue-" not in (TEMPLATES / "build-v15.txt").read_text("utf-8")


# ------------------------------------------------------------------ Done removes extras


@pytest.mark.asyncio
async def test_done_removes_the_issue_extra_worktrees_and_their_branches(
    git_env: GitEnv, tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    ws, wt, head = _worktree(git_env)
    docs = _add(git_env, "factory-issue-9-docs", "rosie/9-docs")
    detached = _add(git_env, "factory-issue-9-p0", None)
    other_issue = _add(git_env, "factory-issue-90", "factory/issue-90")
    not_ours = _add(git_env, "factory-issue-9x", "scratch-9x")
    outside_root = tmp_path / "elsewhere"
    outside_root.mkdir()
    outside = _add(git_env, "factory-issue-9-out", "rosie/9-out", root=outside_root)
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    script, log, _ = _script(tmp_path)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=(str(script),),
    )
    with caplog.at_level(logging.INFO, logger="omnigent_factory.service.cleanup"):
        detail = await cleaner.cleanup(parcel.parcel_id, merged=False)
    assert set(detail["removed"]) == {str(wt.resolve()), str(docs), str(detached)}
    assert not wt.exists() and not docs.exists() and not detached.exists()
    assert other_issue.exists() and not_ours.exists() and outside.exists()
    # Teardown ran in each removed worktree first, never elsewhere.
    ran = {line.split()[0] for line in _runs(log)}
    assert ran == {str(wt.resolve()), str(docs), str(detached)}
    branches = _branches(git_env)
    assert "rosie/9-docs" not in branches and "factory/issue-9" not in branches
    assert {"factory/issue-90", "scratch-9x", "rosie/9-out"} <= branches
    removed_logs = [r.getMessage() for r in caplog.records if "worktree removed" in r.getMessage()]
    assert any(str(docs) in m and "reason=done" in m for m in removed_logs)
    assert all(r.levelno == logging.INFO for r in caplog.records if "removed" in r.getMessage())


@pytest.mark.asyncio
async def test_extra_branch_checked_out_elsewhere_or_dirty_extra_is_kept(git_env: GitEnv):
    ws, wt, head = _worktree(git_env)
    dirty = _add(git_env, "factory-issue-9-wip", "rosie/9-wip")
    (dirty / "wip.txt").write_text("unsaved\n")
    shared = _add(git_env, "factory-issue-9-a", "rosie/9-shared")
    # The same branch cannot be checked out twice; a second worktree on it is forced.
    git(
        "worktree",
        "add",
        "-f",
        str(git_env.worktrees / "keep-me"),
        "rosie/9-shared",
        cwd=git_env.source,
    )
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    cleaner = WorkspaceCleaner(_Directory(parcel, snaps), ws, git_env.worktrees)  # type: ignore[arg-type]
    detail = await cleaner.cleanup(parcel.parcel_id, merged=False)
    assert dirty.exists() and any("uncommitted" in s for s in detail["skipped"])
    assert not shared.exists()
    assert "rosie/9-shared" in _branches(git_env)  # still checked out in keep-me
    assert "rosie/9-wip" in _branches(git_env)


@pytest.mark.asyncio
async def test_extra_teardown_failure_keeps_that_worktree_and_retries(
    git_env: GitEnv, tmp_path: Path
):
    ws, wt, head = _worktree(git_env)
    docs = _add(git_env, "factory-issue-9-docs", "rosie/9-docs")
    log = tmp_path / "teardown.log"
    script = tmp_path / "teardown.sh"
    flag = tmp_path / "fail-docs"
    script.write_text(
        "#!/bin/sh\n"
        f'echo "$(pwd)" >> "{log}"\n'
        f'case "$(pwd)" in *-docs) [ -f "{flag}" ] && exit 1;; esac\n'
        "exit 0\n"
    )
    script.chmod(0o755)
    flag.write_text("")
    parcel, snaps = _finished_parcel(wt, "factory/issue-9", head)
    cleaner = WorkspaceCleaner(
        _Directory(parcel, snaps),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=(str(script),),
    )
    adapter = CleanupAdapter(cleaner, max_teardown_attempts=3)
    effect: EffectIntent = replace(_effect(parcel.parcel_id), args={"merged": True})
    outcome = await adapter.execute(effect, CTX)
    assert isinstance(outcome, RetryableReadFailure)
    assert not wt.exists() and docs.exists() and "rosie/9-docs" in _branches(git_env)
    flag.unlink()
    await adapter.execute(effect, CTX)
    assert not docs.exists() and "rosie/9-docs" not in _branches(git_env)


# ------------------------------------------------------------------ the sweep


@pytest.fixture
def gc_config() -> ServiceConfig:
    # Not the ``service_config`` fixture: its state directory collides with ``git_env``'s.
    return ServiceConfig(repo_id=REPO_ID, owners=frozenset({OWNER_ID}))


class _Store:
    def __init__(self, parcels: dict[int, Parcel], merged: set[str], unknown: set[str]) -> None:
        self.parcels = parcels
        self.merged = merged
        self.unknown = unknown

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
        if "FROM parcels" in sql:
            return [(n, p.parcel_id) for n, p in self.parcels.items()]
        key = str(params[1])
        return [(1,)] if key.split(":")[1] in self.merged else []

    def load_parcel(self, parcel_id: str) -> Parcel | None:
        return next((p for p in self.parcels.values() if p.parcel_id == parcel_id), None)

    def effects_in_state(self, state: str) -> list[Any]:
        assert state == "unknown"
        return [SimpleNamespace(effect=SimpleNamespace(parcel_id=p)) for p in self.unknown]


def _service(config: ServiceConfig, store: _Store, now_s: float = NOW_S) -> Any:
    async def call(operation: Any) -> Any:
        return operation(store)

    clock = SimpleNamespace(now_utc_us=lambda: int(now_s * 1_000_000))
    return SimpleNamespace(config=config, clock=clock, db=SimpleNamespace(call=call))


def _parcel(n: int, stage: Stage, *, live: bool = False, completed: bool = False) -> Parcel:
    h = Harness()
    h.to_building()
    base = h.p()
    lifecycle = Lifecycle.ACTIVE if live else Lifecycle.RETIRED
    return replace(
        base,
        parcel_id=f"I_{n}",
        issue_number=n,
        stage=stage,
        sessions=tuple(replace(s, lifecycle=lifecycle) for s in base.sessions),
        holds=(base.holds | {Hold.COMPLETED}) if completed else base.holds - {Hold.COMPLETED},
        pending_authorization_id=None,
        readiness=None,
    )


def _gc(
    git_env: GitEnv,
    gc_config: ServiceConfig,
    store: _Store,
    states: dict[int, bool],
    *,
    teardown: tuple[str, ...] = (),
    **config: Any,
) -> tuple[WorktreeGC, list[list[int]]]:
    ws = git_env.workspaces()
    cfg = gc_config.model_copy(update=config)
    service = _service(cfg, store)
    cleaner = WorkspaceCleaner(
        _Directory(None, {}),  # type: ignore[arg-type]
        ws,
        git_env.worktrees,
        teardown=teardown,
    )
    reads: list[list[int]] = []

    async def issue_states(numbers: Any) -> dict[int, bool]:
        reads.append(list(numbers))
        return {n: s for n, s in states.items() if n in numbers}

    return WorktreeGC(service, cleaner, ws, issue_states), reads


@pytest.mark.asyncio
async def test_sweep_cleans_up_closed_issues_but_never_live_or_unsettled_ones(
    git_env: GitEnv, gc_config: ServiceConfig, tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    closed_old = _add(git_env, "factory-issue-5", "factory/issue-5")  # pre-cleanup era
    closed_extra = _add(git_env, "factory-issue-5-docs", "rosie/5-docs")
    closed_dirty = _add(git_env, "factory-issue-6", "factory/issue-6")
    (closed_dirty / "wip.txt").write_text("x\n")
    open_issue = _add(git_env, "factory-issue-7", "factory/issue-7")
    live = _add(git_env, "factory-issue-11", "factory/issue-11")
    unknown = _add(git_env, "factory-issue-12", "factory/issue-12")
    ready = _add(git_env, "factory-issue-13", "factory/issue-13")
    merged_dirty = _add(git_env, "factory-issue-14", "factory/issue-14")
    (merged_dirty / "wip.txt").write_text("x\n")
    unnamed = _add(git_env, "674-a-docs", "rosie/674-docs")
    store = _Store(
        {
            11: _parcel(11, Stage.BUILDING, live=True, completed=True),
            12: _parcel(12, Stage.TRIAGED),
            13: _parcel(13, Stage.READY),
            14: _parcel(14, Stage.READY, completed=True),
        },
        merged={"I_14"},
        unknown={"I_12"},
    )
    script, log, _ = _script(tmp_path)
    gc, reads = _gc(
        git_env,
        gc_config,
        store,
        {5: False, 6: False, 7: True, 11: False, 12: False, 13: False, 14: False},
        teardown=(str(script),),
    )
    with caplog.at_level(logging.INFO):
        result = await gc.sweep_once()
    assert reads == [[5, 6, 7, 11, 12, 13, 14]]  # one batched read
    assert not closed_old.exists() and not closed_extra.exists()
    # Done's branch rule: the extra's branch goes; the unmerged issue branch is kept.
    assert "rosie/5-docs" not in _branches(git_env) and "factory/issue-5" in _branches(git_env)
    assert "factory/issue-14" not in _branches(git_env)  # merged
    assert closed_dirty.exists()  # closed, not merged: uncommitted work kept
    assert not merged_dirty.exists()  # merged per the factory's record: forced
    assert open_issue.exists() and live.exists() and unknown.exists() and ready.exists()
    assert unnamed.exists() and "rosie/674-docs" in _branches(git_env)
    assert set(result["skipped"]) == {"#11", "#12", "#13"}  # type: ignore[arg-type]
    assert result["unnamed"] == [str(unnamed)]
    assert {line.split()[0] for line in _runs(log)} >= {str(closed_old), str(closed_extra)}
    messages = [r.getMessage() for r in caplog.records]
    assert any("no issue number" in m and str(unnamed) in m for m in messages)
    assert any(
        "worktree removed" in m and "issue closed" in m and str(closed_old) in m for m in messages
    )
    # Idempotent: a second sweep finds nothing more to do and fails nothing.
    again = await gc.sweep_once()
    assert set(again["closed"]) == {"#6"}  # type: ignore[arg-type]
    assert closed_dirty.exists()


@pytest.mark.asyncio
async def test_unreadable_issue_states_skip_closed_cleanup_only(
    git_env: GitEnv, gc_config: ServiceConfig
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    closed = _add(git_env, "factory-issue-5", "factory/issue-5")
    gc, _ = _gc(git_env, gc_config, _Store({}, set(), set()), {})

    async def broken(numbers: Any) -> dict[int, bool]:
        raise gc_mod.GitHubAPIError("boom")

    gc.issue_states = broken
    result = await gc.sweep_once()
    assert closed.exists() and "boom" in str(result["issue_state_error"])


@pytest.mark.asyncio
async def test_idle_early_stage_worktree_without_own_work_is_removed(
    git_env: GitEnv, gc_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    idle = _add(git_env, "factory-issue-20", "factory/issue-20")
    dirty = _add(git_env, "factory-issue-21", "factory/issue-21")
    (dirty / "notes.txt").write_text("x\n")
    commits = _add(git_env, "factory-issue-22", "factory/issue-22")
    (commits / "c.txt").write_text("x\n")
    git("add", "c.txt", cwd=commits)
    git("commit", "-q", "-m", "own", cwd=commits)
    recent = _add(git_env, "factory-issue-23", "factory/issue-23")
    building = _add(git_env, "factory-issue-24", "factory/issue-24")
    no_record = _add(git_env, "factory-issue-25", "factory/issue-25")
    for path in (idle, dirty, commits, building, no_record):
        _age(ws, path, 10)
    _age(ws, recent, 2)
    store = _Store(
        {
            20: _parcel(20, Stage.SCOPED),
            21: _parcel(21, Stage.TRIAGED),
            22: _parcel(22, Stage.INBOX),
            23: _parcel(23, Stage.SCOPED),
            24: _parcel(24, Stage.BUILDING),
        },
        set(),
        set(),
    )
    states = dict.fromkeys(range(20, 26), True)
    gc, _ = _gc(git_env, gc_config, store, states)
    with caplog.at_level(logging.INFO):
        result = await gc.sweep_once()
    assert not idle.exists() and "factory/issue-20" not in _branches(git_env)
    for kept in (dirty, commits, recent, building, no_record):
        assert kept.exists(), kept
    idle_result: dict[str, str] = result["idle"]  # type: ignore[assignment]
    assert idle_result[str(idle)] == "removed"
    assert idle_result[str(dirty)] == "kept: uncommitted changes"
    assert idle_result[str(commits)].startswith("kept: 1 commits beyond")
    assert any(
        "reason=idle" in r.getMessage() and str(idle) in r.getMessage() for r in caplog.records
    )
    # 0 disables idle removal.
    off, _ = _gc(git_env, gc_config, store, states, idle_worktree_days=0)
    _age(ws, recent, 30)
    await off.sweep_once()
    assert recent.exists()


@pytest.mark.asyncio
async def test_housekeeping_reflog_mtime_does_not_make_an_idle_worktree_recent(
    git_env: GitEnv, gc_config: ServiceConfig
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    idle = _add(git_env, "factory-issue-26", "factory/issue-26")
    _age(ws, idle, 10)
    # Fetch-triggered maintenance / gc expires and replaces this file without appending
    # a reflog entry.  The content still says the worktree was last used ten days ago.
    reflog = ws.admin_dir(idle) / "logs" / "HEAD"
    os.utime(reflog, (NOW_S, NOW_S))
    assert ws.last_touched(idle) == NOW_S - 10 * DAY
    gc, _ = _gc(
        git_env,
        gc_config,
        _Store({26: _parcel(26, Stage.TRIAGED)}, set(), set()),
        {26: True},
    )
    result = await gc.sweep_once()
    assert result["idle"][str(idle)] == "removed"  # type: ignore[index]
    assert not idle.exists()


@pytest.mark.asyncio
async def test_sweep_removes_only_eligible_factory_named_strays(
    git_env: GitEnv, gc_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    root = git_env.worktrees
    closed_log = root / "factory-issue-30-devenv-start.log"
    closed_log.write_text("finished\n")
    closed_dir = root / "factory-issue-30-leftover"
    closed_dir.mkdir()
    (closed_dir / "output.log").write_text("finished\n")
    idle_log = root / "factory-issue-31-review.md"
    idle_log.write_text("old\n")
    live_log = root / "factory-issue-32-review.md"
    live_log.write_text("old\n")
    recent_log = root / "factory-issue-33-review.md"
    recent_log.write_text("recent\n")
    unrelated = root / "mutation.diff"
    unrelated.write_text("never factory-owned by name\n")
    for path in (closed_log, closed_dir, idle_log, live_log, unrelated):
        _age_entry(path, 10)
    _age_entry(recent_log, 2)
    store = _Store(
        {
            31: _parcel(31, Stage.SCOPED),
            32: _parcel(32, Stage.TRIAGED, live=True),
            33: _parcel(33, Stage.INBOX),
        },
        set(),
        set(),
    )
    gc, reads = _gc(git_env, gc_config, store, {30: False, 31: True, 32: True, 33: True})
    with caplog.at_level(logging.INFO):
        result = await gc.sweep_once()
    assert reads == [[30, 31, 32, 33]]
    assert not closed_log.exists() and not closed_dir.exists() and not idle_log.exists()
    assert live_log.exists() and recent_log.exists() and unrelated.exists()
    assert result["strays"] == {  # type: ignore[comparison-overlap]
        str(closed_log): "removed",
        str(closed_dir): "removed",
        str(idle_log): "removed",
    }
    assert result["skipped"] == {  # type: ignore[comparison-overlap]
        "#32": "a stage session is live (starting, running or draining)"
    }
    removed = [
        r.getMessage() for r in caplog.records if "stray worktree entry removed" in r.getMessage()
    ]
    assert {str(closed_log), str(closed_dir), str(idle_log)} <= {
        next(path for path in (str(closed_log), str(closed_dir), str(idle_log)) if path in line)
        for line in removed
    }


@pytest.mark.asyncio
async def test_stray_directory_waits_for_dev_environment_teardown(
    git_env: GitEnv, gc_config: ServiceConfig, tmp_path: Path
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    stray = git_env.worktrees / "factory-issue-34-devenv"
    stray.mkdir()
    (stray / "state").write_text("running\n")
    _age_entry(stray, 10)
    script = tmp_path / "teardown.sh"
    script.write_text("#!/bin/sh\nexit 1\n")
    script.chmod(0o755)
    gc, _ = _gc(
        git_env,
        gc_config,
        _Store({34: _parcel(34, Stage.TRIAGED)}, set(), set()),
        {34: True},
        teardown=(str(script),),
    )
    result = await gc.sweep_once()
    assert stray.exists()
    assert "dev environment teardown failed" in result["strays"][str(stray)]  # type: ignore[index]


@pytest.mark.asyncio
async def test_a_run_starting_during_teardown_keeps_the_worktree(
    git_env: GitEnv, gc_config: ServiceConfig, tmp_path: Path
):
    """The re-check under the clone lock sees the run that started meanwhile."""
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    idle = _add(git_env, "factory-issue-20", "factory/issue-20")
    _age(ws, idle, 10)
    store = _Store({20: _parcel(20, Stage.SCOPED)}, set(), set())
    started = tmp_path / "started"
    script = tmp_path / "teardown.sh"
    script.write_text(f'#!/bin/sh\ntouch "{started}"\n')
    script.chmod(0o755)
    gc, _ = _gc(git_env, gc_config, store, {20: True}, teardown=(str(script),))
    real_load = store.load_parcel

    def load(parcel_id: str) -> Parcel | None:
        parcel = real_load(parcel_id)
        if parcel is not None and started.exists():  # a plan run was admitted meanwhile
            return _parcel(20, Stage.SCOPED, live=True)
        return parcel

    store.load_parcel = load  # type: ignore[method-assign]
    result = await gc.sweep_once()
    assert idle.exists() and "factory/issue-20" in _branches(git_env)
    assert "live" in result["idle"][str(idle)]  # type: ignore[index]


@pytest.mark.asyncio
async def test_idle_removed_worktree_is_recreated_by_the_next_stage(git_env: GitEnv) -> None:
    from tests.omnigent.support import CTX as RIG_CTX
    from tests.omnigent.support import intent, make_rig, spec

    rig = make_rig(git_env)
    branch = "factory/issue-42"
    rig.directory.specs["T1"] = spec("T1", kind=SessionKind.TRIAGE, branch=branch)
    out = await rig.adapter.execute(
        intent(EffectKind.CREATE_SESSION, "T1", nonce="nonce-abc"), RIG_CTX
    )
    worktree = Path(str(out.detail["workspace"]))  # type: ignore[union-attr]
    ws = git_env.workspaces()
    cleaner = WorkspaceCleaner(SimpleNamespace(), ws, git_env.worktrees)  # type: ignore[arg-type]
    git("checkout", "--", ".", cwd=worktree)
    git("clean", "-fdq", cwd=worktree)
    assert cleaner.remove_idle(worktree.resolve(), branch) == "removed"
    assert not worktree.exists() and branch not in _branches(git_env)
    rig.directory.specs["P2"] = spec(
        "P2", kind=SessionKind.PLAN, branch=branch, nonce="nonce-2", bind_worktree=worktree
    )
    created = await rig.adapter.execute(
        intent(EffectKind.CREATE_SESSION, "P2", nonce="nonce-2"), RIG_CTX
    )
    assert worktree.is_dir(), created
    assert git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree) == branch


# ------------------------------------------------------------------ schedule and gc


@pytest.mark.asyncio
async def test_last_runs_persist_so_a_restart_does_not_rerun_and_reload_applies(
    git_env: GitEnv, gc_config: ServiceConfig
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    gc, _ = _gc(git_env, gc_config, _Store({}, set(), set()), {})
    await gc.sweep_once()
    await gc.gc_once()
    persisted = read_state(git_env.source)
    assert persisted == {"sweep_at_us": int(NOW_S * 1e6), "gc_at_us": int(NOW_S * 1e6)}
    # A restart an hour later: nothing is due yet.
    later = _service(gc_config, _Store({}, set(), set()), NOW_S + 3600)
    restarted, _ = _gc(git_env, gc_config, _Store({}, set(), set()), {})
    restarted.service = later
    calls: list[str] = []

    async def sweep() -> dict[str, object]:
        calls.append("sweep")
        return {}

    restarted.sweep_once = sweep  # type: ignore[method-assign]
    await restarted.start()
    await asyncio.sleep(0.1)
    assert calls == []
    # Reload with a 30-minute interval: the sweep is due now and runs without a restart.
    later.config = gc_config.model_copy(update={"worktree_sweep_interval_hours": 0.5})
    restarted.reloaded(later.config)
    for _ in range(50):
        if calls:
            break
        await asyncio.sleep(0.02)
    await restarted.close()
    assert calls == ["sweep"]
    assert {"worktree_sweep_interval_hours", "idle_worktree_days", "clone_gc_interval_days"} <= (
        HOT_RELOAD_KEYS
    )
    assert gc_config.worktree_sweep_interval_hours == 24
    assert gc_config.idle_worktree_days == 7
    assert gc_config.clone_gc_interval_days == 7


@pytest.mark.asyncio
async def test_gc_prunes_stale_registrations_under_the_clone_lock(
    git_env: GitEnv, gc_config: ServiceConfig
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    gone = _add(git_env, "factory-issue-30", "factory/issue-30")
    shutil.rmtree(gone)
    assert gone in _registered(git_env)
    gc, _ = _gc(git_env, gc_config, _Store({}, set(), set()), {})
    with CLONE_LOCK:
        task = asyncio.create_task(gc.gc_once())
        await asyncio.sleep(0.3)
        assert not task.done()  # waits for the lock worktree creation holds
        assert gone in _registered(git_env)
    # Release from this thread; the gc thread now proceeds.
    result = await task
    assert result == {"ok": True, "error": ""}
    assert gone not in _registered(git_env)
    assert state_path(git_env.source).is_file()


def test_clone_lock_is_released_after_gc(git_env: GitEnv):
    ws = git_env.workspaces()
    ws.maintain_clone()
    acquired = threading.Event()

    def other() -> None:
        with CLONE_LOCK:
            acquired.set()

    thread = threading.Thread(target=other)
    thread.start()
    thread.join(5)
    assert acquired.is_set()


# ------------------------------------------------------------------ status and doctor


@pytest.mark.asyncio
async def test_status_and_doctor_show_worktree_count_and_last_runs(
    git_env: GitEnv, gc_config: ServiceConfig
):
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    _add(git_env, "factory-issue-40", "factory/issue-40")
    _add(git_env, "unnamed", "rosie/unnamed")
    gc, _ = _gc(git_env, gc_config, _Store({}, set(), set()), {40: True})
    await gc.sweep_once()
    status = await gc.status()
    assert status["worktrees"] == 2
    assert status["last_sweep"] == "2033-05-18T03:33:20+00:00" and status["last_gc"] is None
    cfg = gc_config.model_copy(
        update={"source_clone": git_env.source, "worktree_root": git_env.worktrees}
    )
    report = DoctorReport()
    _check_worktrees(cfg, report)
    detail = report.checks["worktrees"]
    assert detail.startswith("2 under ") and "last sweep 2033-05-18" in detail
    assert "last gc never" in detail


# ------------------------------------------------------------------ the batched read


@pytest.mark.asyncio
async def test_issue_states_read_is_batched_and_tolerates_missing_issues():
    bodies: list[dict[str, Any]] = []

    class Client:
        async def request(self, method: str, path: str, *, json_body: dict[str, Any]) -> Any:
            bodies.append(json_body)
            query = json_body["query"]
            numbers = [int(n) for n in re.findall(r"\bi(\d+):", query)]
            repo = {
                f"i{n}": None if n == 7 else {"state": "CLOSED" if n % 2 else "OPEN"}
                for n in numbers
            }
            body = {"data": {"repository": repo}, "errors": [{"type": "NOT_FOUND"}]}
            return SimpleNamespace(json=lambda: body)

    states = await github_issue_states(Client(), "SA-Ambulance/timesheets", list(range(1, 151)))  # type: ignore[arg-type]
    assert len(bodies) == 2  # 100 per read
    assert bodies[0]["variables"] == {"o": "SA-Ambulance", "n": "timesheets"}
    assert 7 not in states and states[1] is False and states[2] is True and len(states) == 149


@pytest.mark.asyncio
async def test_a_failing_sweep_backs_off_instead_of_spinning(
    git_env: GitEnv, gc_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    gc, _ = _gc(git_env, gc_config, _Store({}, set(), set()), {})
    calls: list[str] = []
    waits: list[float] = []

    async def sweep() -> dict[str, object]:
        calls.append("sweep")
        raise RuntimeError("boom")

    async def gc_once() -> dict[str, object]:
        calls.append("gc")
        return {}

    real_wait_for = asyncio.wait_for

    async def wait_for(aw: Any, timeout: float) -> Any:
        waits.append(timeout)
        gc._stop.set()
        return await real_wait_for(aw, 1)

    gc.sweep_once = sweep  # type: ignore[method-assign]
    gc.gc_once = gc_once  # type: ignore[method-assign]
    monkeypatch.setattr(gc_mod.asyncio, "wait_for", wait_for)
    await gc._run()
    assert calls == ["sweep", "gc"]  # a failed sweep does not skip the gc
    assert waits == [gc_mod.RETRY_S]
