"""#821: Done cleanup removed the issue worktree but kept the unmerged ``factory/issue-N``
branch. The owner dragged the card back out of Done; the next run's create bound the
recorded worktree path and was refused ("workspace-rejected: workspace does not exist"),
twice. The recorded worktree is now re-added before it is bound (create and prepare), from
the kept branch, without ever discarding its commits. Real Git.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, DefinitiveFailure, EffectKind
from omnigent_factory.core.types import Parcel, SessionKind
from omnigent_factory.omnigent.outcomes import observations
from omnigent_factory.service.cleanup import WorkspaceCleaner
from tests.credentials.repos import GitEnv, git
from tests.omnigent.support import CTX, Rig, intent, make_rig, spec

pytestmark = pytest.mark.asyncio

BRANCH = "factory/issue-42"


class _Snapshots:
    """Just what :class:`WorkspaceCleaner` reads besides the parcel."""

    def dispatch_snapshot(self, session_id: str) -> None:
        del session_id


async def _done_issue(git_env: GitEnv) -> tuple[Rig, str, Path]:
    """Triage created the issue worktree; the issue then went Done and was cleaned up."""
    rig = make_rig(git_env)
    rig.directory.specs["T1"] = spec("T1", kind=SessionKind.TRIAGE, branch=BRANCH)
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "T1", nonce="nonce-abc"), CTX)
    assert isinstance(out, Ack) and out.remote_id is not None
    worktree = Path(str(out.detail["workspace"]))
    assert worktree.is_dir()
    cleaner = WorkspaceCleaner(_Snapshots(), git_env.workspaces(), git_env.worktrees)  # type: ignore[arg-type]
    detail = cleaner._cleanup(Parcel("I_kwIssue42", "repo", issue_number=42), merged=False)
    assert detail["removed"] == [str(worktree.resolve())]
    assert not worktree.exists()
    assert git("branch", "--list", BRANCH, cwd=git_env.source)  # unmerged: kept
    return rig, out.remote_id, worktree


def _advance_main(git_env: GitEnv, tmp_name: str = "more") -> str:
    seed = git_env.remote.parent / "seed"
    (seed / f"{tmp_name}.txt").write_text("x\n", encoding="utf-8")
    git("add", ".", cwd=seed)
    git("commit", "-q", "-m", tmp_name, cwd=seed)
    git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return git("rev-parse", "HEAD", cwd=seed)


async def _reopen_create(rig: Rig, worktree: Path) -> object:
    """The reopened issue's next run binds the recorded worktree (a fresh issue session)."""
    rig.directory.specs["P2"] = spec(
        "P2", kind=SessionKind.PLAN, branch=BRANCH, nonce="nonce-2", bind_worktree=worktree
    )
    return await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "P2", nonce="nonce-2"), CTX)


def _upstream(git_env: GitEnv) -> str:
    return git("for-each-ref", "--format=%(upstream)", f"refs/heads/{BRANCH}", cwd=git_env.source)


async def test_reopened_done_issue_recreates_worktree_and_creates_the_session(
    git_env: GitEnv, caplog: pytest.LogCaptureFixture
) -> None:
    rig, _, worktree = await _done_issue(git_env)
    main = _advance_main(git_env)  # main moved on while the issue was Done
    with caplog.at_level(logging.WARNING, logger="omnigent_factory.omnigent.adapter"):
        outcome = await _reopen_create(rig, worktree)
    assert isinstance(outcome, Ack), outcome
    body = rig.server.requests[-1][2]
    assert body["git"] == {"branch_name": BRANCH, "existing_worktree": True}
    assert body["workspace"] == str(worktree.resolve())
    assert git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree) == BRANCH
    assert git("rev-parse", "HEAD", cwd=worktree) == main  # fast-forwarded, nothing of its own
    assert _upstream(git_env) == ""  # never tracks origin/main
    [record] = [r for r in caplog.records if "worktree was missing" in r.getMessage()]
    assert str(worktree) in record.getMessage() and "fast-forwarded" in record.getMessage()
    # The session prepares in the recreated worktree.
    assert outcome.remote_id is not None
    rig.directory.set_root("P2", outcome.remote_id)
    prep = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "P2", root_id=outcome.remote_id), CTX
    )
    assert isinstance(prep, Ack) and prep.detail["ok"] is True, prep.detail


async def test_branch_with_its_own_commits_is_recreated_as_is(git_env: GitEnv) -> None:
    rig, _, worktree = await _done_issue(git_env)
    # Commit on the kept branch (no worktree) while main also moves on.
    tree = git("rev-parse", f"{BRANCH}^{{tree}}", cwd=git_env.source)
    own = git("commit-tree", tree, "-p", BRANCH, "-m", "own work", cwd=git_env.source)
    git("update-ref", f"refs/heads/{BRANCH}", own, cwd=git_env.source)
    _advance_main(git_env)
    outcome = await _reopen_create(rig, worktree)
    assert isinstance(outcome, Ack), outcome
    assert git("rev-parse", "HEAD", cwd=worktree) == own  # never rewritten or merged
    assert _upstream(git_env) == ""


async def test_deleted_branch_is_created_fresh_from_origin_main(git_env: GitEnv) -> None:
    rig, _, worktree = await _done_issue(git_env)
    git("branch", "-D", BRANCH, cwd=git_env.source)  # cleanup deleted the merged branch
    main = _advance_main(git_env)
    outcome = await _reopen_create(rig, worktree)
    assert isinstance(outcome, Ack), outcome
    assert git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree) == BRANCH
    assert git("rev-parse", "HEAD", cwd=worktree) == main
    assert _upstream(git_env) == ""


async def test_stale_worktree_registration_is_pruned_first(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["T1"] = spec("T1", kind=SessionKind.TRIAGE, branch=BRANCH)
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "T1", nonce="nonce-abc"), CTX)
    assert isinstance(out, Ack)
    worktree = Path(str(out.detail["workspace"]))
    shutil.rmtree(worktree)  # gone without ``git worktree remove``: still registered
    outcome = await _reopen_create(rig, worktree)
    assert isinstance(outcome, Ack), outcome
    assert git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree) == BRANCH


async def test_branch_checked_out_elsewhere_is_still_a_definitive_rejection(
    git_env: GitEnv,
) -> None:
    rig, _, worktree = await _done_issue(git_env)
    other = git_env.worktrees / "elsewhere"
    git("worktree", "add", "-q", str(other), BRANCH, cwd=git_env.source)
    create = intent(EffectKind.CREATE_SESSION, "P2", nonce="nonce-2")
    outcome = await _reopen_create(rig, worktree)
    assert isinstance(outcome, DefinitiveFailure)
    assert "checked out in another worktree" in outcome.reason
    assert not worktree.exists()
    (rejected,) = observations(create, outcome)
    assert isinstance(rejected, ev.CreateRejected)
    assert rig.server.count("POST", "/v1/sessions") == 1  # only the triage create


async def test_path_outside_owned_roots_is_never_created(git_env: GitEnv, tmp_path: Path) -> None:
    rig, _, _ = await _done_issue(git_env)
    outside = tmp_path / "outside" / "factory-issue-42"
    outside.parent.mkdir()
    outcome = await _reopen_create(rig, outside)
    assert isinstance(outcome, DefinitiveFailure)
    assert "outside the approved owned roots" in outcome.reason
    assert not outside.exists()


async def test_reused_issue_session_prepare_recreates_a_missing_worktree(git_env: GitEnv) -> None:
    rig, root, worktree = await _done_issue(git_env)
    rig.directory.set_root("T1", root)
    rig.directory.specs["B1"] = replace(
        spec("B1", kind=SessionKind.BUILD, branch=BRANCH, nonce="nonce-B1", policy_generation=2),
        root_id=root,
        root_nonce="nonce-abc",
    )
    prep = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "B1", root_id=root, reuse=True), CTX
    )
    assert isinstance(prep, Ack) and prep.detail["ok"] is True, prep.detail
    assert git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree) == BRANCH
    assert git("config", "--get", "factory.stageSession", cwd=worktree) == "B1"  # wired
