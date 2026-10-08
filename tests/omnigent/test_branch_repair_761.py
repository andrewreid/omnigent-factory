"""#761: a read-only stage ran ``git checkout origin/main`` in the issue worktree, so the
build's preparation was refused ("workspace is not on the recorded branch") and the card
went Blocked with no hint why. Preparation now switches the worktree back when nothing can
be lost, and otherwise refuses with an owner-facing note naming what to fix. Real Git.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.types import SessionKind
from omnigent_factory.omnigent.outcomes import observations
from tests.credentials.repos import GitEnv, git
from tests.omnigent.support import CTX, Rig, intent, make_rig
from tests.omnigent.test_issue_session_adapter import _reuse_spec, _root_with_triage_run

pytestmark = pytest.mark.asyncio

BRANCH = "factory/issue-42-g1"


async def _off_branch(git_env: GitEnv) -> tuple[Rig, str, Path]:
    """A triage-prepared issue session whose worktree a stage left on ``origin/main``."""
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    worktree = Path(str(rig.server.sessions[root].workspace))
    git("checkout", "-q", "origin/main", cwd=worktree)  # the #761 reflog entry
    _reuse_spec(rig, "B1", root, SessionKind.BUILD)
    return rig, root, worktree


async def _prepare_build(rig: Rig, root: str) -> Ack:
    prep = intent(EffectKind.PREPARE_SESSION, "B1", root_id=root, reuse=True)
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, Ack), outcome
    return outcome


def _head(worktree: Path) -> str:
    return git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree)


def _refused(outcome: Ack, note: str) -> None:
    assert outcome.detail["ok"] is False
    assert outcome.detail["note"] == note
    assert outcome.detail["reason"] == f"workspace: {note}"
    [prepared] = observations(intent(EffectKind.PREPARE_SESSION, "B1"), outcome)
    assert isinstance(prepared, ev.Prepared) and prepared.note == note and not prepared.ok


async def test_detached_clean_worktree_is_switched_back_and_prepare_proceeds(
    git_env: GitEnv, caplog: pytest.LogCaptureFixture
) -> None:
    rig, root, worktree = await _off_branch(git_env)
    detached_at = git("rev-parse", "HEAD", cwd=worktree)
    with caplog.at_level(logging.WARNING, logger="omnigent_factory.omnigent.adapter"):
        outcome = await _prepare_build(rig, root)
    assert outcome.detail["ok"] is True, outcome.detail
    assert _head(worktree) == BRANCH
    assert outcome.detail["head_oid"] == git("rev-parse", BRANCH, cwd=worktree)
    assert git("config", "--get", "factory.stageSession", cwd=worktree) == "B1"  # wired
    [record] = [r for r in caplog.records if "off its branch" in r.getMessage()]
    assert detached_at in record.getMessage() and BRANCH in record.getMessage()


async def test_detached_worktree_with_changes_is_refused_with_a_specific_note(
    git_env: GitEnv,
) -> None:
    rig, root, worktree = await _off_branch(git_env)
    (worktree / "README.md").write_text("edited\n", encoding="utf-8")
    short = git("rev-parse", "--short=7", "HEAD", cwd=worktree)
    outcome = await _prepare_build(rig, root)
    _refused(outcome, f"worktree off branch {BRANCH} (detached at {short}, uncommitted changes)")
    assert _head(worktree) == "HEAD"  # left exactly where it was
    assert (worktree / "README.md").read_text(encoding="utf-8") == "edited\n"
    assert rig.broker.capabilities.get("B1") is None  # nothing provisioned


async def test_untracked_file_counts_as_a_change(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    (worktree / "notes.txt").write_text("scratch\n", encoding="utf-8")
    outcome = await _prepare_build(rig, root)
    assert outcome.detail["ok"] is False
    assert "uncommitted changes" in str(outcome.detail["note"])


async def test_another_branch_clean_is_switched_back(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    git("switch", "-q", "-c", "scratch", cwd=worktree)
    outcome = await _prepare_build(rig, root)
    assert outcome.detail["ok"] is True, outcome.detail
    assert _head(worktree) == BRANCH
    assert git("rev-parse", "--verify", "-q", "refs/heads/scratch", cwd=worktree)  # kept


async def test_another_branch_dirty_is_refused(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    git("switch", "-q", "-c", "scratch", cwd=worktree)
    (worktree / "README.md").write_text("edited\n", encoding="utf-8")
    outcome = await _prepare_build(rig, root)
    _refused(outcome, f"worktree off branch {BRANCH} (on scratch, uncommitted changes)")
    assert _head(worktree) == "scratch"


async def test_missing_branch_is_refused(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    git("branch", "-D", BRANCH, cwd=git_env.source)
    short = git("rev-parse", "--short=7", "HEAD", cwd=worktree)
    outcome = await _prepare_build(rig, root)
    _refused(outcome, f"worktree off branch {BRANCH} (detached at {short}, branch missing)")
    assert _head(worktree) == "HEAD"


async def test_operation_in_progress_is_refused(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    marker = Path(
        git("rev-parse", "--path-format=absolute", "--git-path", "MERGE_HEAD", cwd=worktree)
    )
    marker.write_text(git("rev-parse", "HEAD", cwd=worktree) + "\n", encoding="utf-8")
    assert git("status", "--porcelain", cwd=worktree) == ""  # clean, yet mid-merge
    outcome = await _prepare_build(rig, root)
    assert outcome.detail["ok"] is False
    assert str(outcome.detail["note"]).endswith(", merge in progress)")
    assert _head(worktree) == "HEAD"


async def test_detached_commits_on_no_branch_are_not_abandoned(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    git(
        "-c", "user.name=t", "-c", "user.email=t@example.com",
        "commit", "-q", "--allow-empty", "-m", "detached work",
        cwd=worktree,
    )  # fmt: skip
    outcome = await _prepare_build(rig, root)
    assert outcome.detail["ok"] is False
    assert str(outcome.detail["note"]).endswith(", commits on no branch)")
    assert _head(worktree) == "HEAD"


async def test_on_branch_prepares_without_any_switch(git_env: GitEnv) -> None:
    rig, root, worktree = await _off_branch(git_env)
    git("switch", "-q", BRANCH, cwd=worktree)
    reflog = git("reflog", "-n", "1", cwd=worktree)
    outcome = await _prepare_build(rig, root)
    assert outcome.detail["ok"] is True and "note" not in outcome.detail
    assert git("reflog", "-n", "1", cwd=worktree) == reflog  # HEAD not touched
