"""A stage run's start syncs the issue worktree with main (factory-side, real Git).

Before the first message of a new stage run, preparation fetches ``origin/main`` and
fast-forwards an issue branch with no commits of its own and a clean worktree. A branch
with its own commits is never merged or rebased (the message says how far main moved); a
dirty, detached or mid-operation worktree is untouched; a failed fetch never blocks the
stage. Never mid-stage, and once per run (restart-safe).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.types import SessionKind
from omnigent_factory.credentials.worktree import WorktreeError
from omnigent_factory.omnigent.adapter import effect_marker
from tests.credentials.repos import GitEnv, LocalWorkspaces, git
from tests.omnigent.support import CTX, Directory, Rig, intent, make_rig
from tests.omnigent.test_issue_session_adapter import _reuse_spec, _root_with_triage_run

pytestmark = pytest.mark.asyncio

BRANCH = "factory/issue-42-g1"


@dataclass
class SyncDirectory(Directory):
    """The test directory plus the per-run base-sync record (the dispatch snapshot)."""

    syncs: dict[str, dict[str, Any]] = field(default_factory=dict)
    workspaces: dict[str, str] = field(default_factory=dict)

    async def base_sync(self, session_id: str) -> dict[str, Any] | None:
        return self.syncs.get(session_id)

    async def record_base_sync(self, session_id: str, record: Mapping[str, Any]) -> None:
        self.syncs.setdefault(session_id, dict(record))

    async def stage_workspace(self, session_id: str) -> str | None:
        return self.workspaces.get(session_id)


async def _issue_worktree(git_env: GitEnv) -> tuple[Rig, str, Path]:
    """An issue session whose triage run created the worktree at the then-current main."""
    rig = make_rig(git_env)
    directory = SyncDirectory()
    rig.directory = directory
    rig.adapter.directory = directory
    root = await _root_with_triage_run(rig)
    worktree = Path(str(rig.server.sessions[root].workspace))
    _reuse_spec(rig, "B1", root, SessionKind.BUILD)
    directory.workspaces["B1"] = str(worktree)
    return rig, root, worktree


def _advance_main(git_env: GitEnv, commits: int) -> str:
    """``commits`` new commits on the remote's main (another PR merged)."""
    seed = git_env.remote.parent / "seed"
    git("pull", "-q", "origin", "main", cwd=seed)
    for i in range(commits):
        path = seed / f"merged-{i}-{git('rev-parse', 'HEAD', cwd=seed)[:7]}.txt"
        path.write_text(f"{i}\n", encoding="utf-8")
        git("add", path.name, cwd=seed)
        git("commit", "-q", "-m", f"merged {i}", cwd=seed)
    git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return git("rev-parse", "HEAD", cwd=seed)


async def _prepare(rig: Rig, root: str, sid: str = "B1") -> Ack:
    prep = intent(EffectKind.PREPARE_SESSION, sid, root_id=root, reuse=True)
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, Ack), outcome
    return outcome


def base_note(record: Mapping[str, Any], kind: SessionKind) -> str:
    from omnigent_factory.service.directory import base_note as render

    return render(record, kind)


def _head(worktree: Path) -> str:
    return git("rev-parse", "HEAD", cwd=worktree)


def _syncs(rig: Rig) -> dict[str, dict[str, Any]]:
    assert isinstance(rig.directory, SyncDirectory)
    return rig.directory.syncs


async def test_a_clean_branch_without_own_commits_is_fast_forwarded(
    git_env: GitEnv, caplog: pytest.LogCaptureFixture
) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    old = _head(worktree)
    main = _advance_main(git_env, 2)
    with caplog.at_level(logging.INFO, logger="omnigent_factory.omnigent.adapter"):
        outcome = await _prepare(rig, root)
    assert outcome.detail["ok"] is True, outcome.detail
    assert _head(worktree) == main == outcome.detail["head_oid"]
    assert git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree) == BRANCH
    record = _syncs(rig)["B1"]
    assert record == {
        "status": "fast_forwarded",
        "base": "main",
        "old_oid": old,
        "new_oid": main,
        "behind": 2,
        "reason": "",
    }
    [line] = [r.getMessage() for r in caplog.records if "fast-forwarded" in r.getMessage()]
    assert f"{old}->{main}" in line and "commits=2" in line
    note = base_note(record, SessionKind.BUILD)
    assert note.startswith("The factory fast-forwarded this issue worktree to origin/main")
    assert f"{old[:12]} -> {main[:12]}, 2 new commits" in note


async def test_a_branch_with_its_own_commits_is_never_merged(git_env: GitEnv) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    (worktree / "feature.txt").write_text("work\n", encoding="utf-8")
    git("add", "feature.txt", cwd=worktree)
    git("commit", "-q", "-m", "own work", cwd=worktree)
    own = _head(worktree)
    _advance_main(git_env, 3)
    outcome = await _prepare(rig, root)
    assert outcome.detail["ok"] is True, outcome.detail
    assert _head(worktree) == own  # no merge, no rebase
    assert git("rev-list", "--count", "HEAD", cwd=worktree) == "2"
    record = _syncs(rig)["B1"]
    assert (record["status"], record["behind"]) == ("own_commits", 3)
    assert base_note(record, SessionKind.BUILD) == (
        "main has moved 3 commits since your branch base; merge origin/main first if relevant."
    )


async def test_a_dirty_worktree_is_left_untouched(git_env: GitEnv) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    before = _head(worktree)
    (worktree / "README.md").write_text("local edit\n", encoding="utf-8")
    _advance_main(git_env, 1)
    outcome = await _prepare(rig, root)
    assert outcome.detail["ok"] is True, outcome.detail
    assert _head(worktree) == before
    assert (worktree / "README.md").read_text(encoding="utf-8") == "local edit\n"
    record = _syncs(rig)["B1"]
    assert (record["status"], record["reason"], record["behind"]) == (
        "not_synced",
        "uncommitted changes",
        1,
    )
    assert "may be stale" in base_note(record, SessionKind.BUILD)


async def test_a_worktree_mid_merge_is_left_untouched(git_env: GitEnv) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    before = _head(worktree)
    marker = Path(
        git("rev-parse", "--path-format=absolute", "--git-path", "MERGE_HEAD", cwd=worktree)
    )
    marker.write_text(before + "\n", encoding="utf-8")
    _advance_main(git_env, 1)
    await _prepare(rig, root)
    assert _head(worktree) == before
    assert _syncs(rig)["B1"]["reason"] == "merge in progress"


class FailingFetch(LocalWorkspaces):
    def fetch_base(self, base_branch: str = "main") -> str:
        raise WorktreeError("git fetch --no-tags origin failed: network is unreachable")


async def test_a_failed_fetch_never_blocks_the_stage(git_env: GitEnv) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    before = _head(worktree)
    failing = FailingFetch(
        git_env.source,
        [git_env.worktrees],
        rig.adapter.workspaces.repository,
        remote=git_env.remote,
        runner_config_env=rig.adapter.workspaces.runner_config_env,
    )
    rig.adapter.workspaces = failing
    _advance_main(git_env, 1)
    outcome = await _prepare(rig, root)
    assert outcome.detail["ok"] is True, outcome.detail  # the stage starts anyway
    assert _head(worktree) == before
    record = _syncs(rig)["B1"]
    assert record["status"] == "fetch_failed"
    assert base_note(record, SessionKind.BUILD) == (
        "The factory could not fetch origin/main: the worktree's base may be stale."
    )


async def test_the_sync_happens_once_per_run_even_after_a_restart(git_env: GitEnv) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    first = _advance_main(git_env, 1)
    await _prepare(rig, root)
    assert _head(worktree) == first
    recorded = dict(_syncs(rig)["B1"])
    _advance_main(git_env, 2)
    # A retried preparation of the same run: the adapter keeps no sync state in memory,
    # so this is also what a restarted daemon does with the persisted record (the
    # dispatch snapshot; see tests/service/test_base_sync_messages.py).
    await _prepare(rig, root)
    assert _head(worktree) == first
    assert _syncs(rig)["B1"] == recorded
    # The next stage run syncs again.
    _reuse_spec(rig, "B2", root, SessionKind.BUILD, generation=3)
    await _prepare(rig, root, "B2")
    assert _syncs(rig)["B2"]["status"] == "fast_forwarded"


async def test_mid_stage_messages_never_sync_and_say_how_far_main_moved(
    git_env: GitEnv,
) -> None:
    rig, root, worktree = await _issue_worktree(git_env)
    await _prepare(rig, root)
    before = _head(worktree)
    _advance_main(git_env, 2)
    rig.adapter.workspaces.fetch_base("main")  # e.g. the agent's own fetch
    for purpose, effect_id in (("feedback", "ef_fb"), ("readiness_wake", "ef_wake")):
        rig.directory.texts[effect_id] = "Owner feedback"
        send = intent(EffectKind.SEND_MESSAGE, "B1", id=effect_id, purpose=purpose)
        outcome = await rig.adapter.execute(send, CTX)
        assert isinstance(outcome, Ack), outcome
        text = rig.server.sessions[root].items[-1]["content"][0]["text"]
        assert text == (
            "Owner feedback\n\nmain has moved 2 commits since your branch base; merge "
            f"origin/main first if relevant.\n\n{effect_marker(effect_id)}"
        )
    assert _head(worktree) == before  # never synced mid-stage
    rig.directory.texts["ef_conflict"] = "Conflict"
    conflict = intent(
        EffectKind.SEND_MESSAGE, "B1", id="ef_conflict", purpose="readiness_wake", wake="conflict"
    )
    await rig.adapter.execute(conflict, CTX)
    text = rig.server.sessions[root].items[-1]["content"][0]["text"]
    assert text == f"Conflict\n\n{effect_marker('ef_conflict')}"
