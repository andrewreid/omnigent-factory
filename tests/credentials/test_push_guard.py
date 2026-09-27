"""Real-git proof of the worktree ``pre-push`` guard and the never-committed harness settings."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from omnigent_factory.credentials.push_guard import install_push_guard
from omnigent_factory.credentials.worktree import HARNESS_ALLOW_RULES, StageWiring
from tests.credentials.repos import BOT, REPO, GitEnv, LocalWorkspaces, git

BRANCH = "factory/issue-9"


def _wired(git_env: GitEnv) -> tuple[LocalWorkspaces, Path]:
    hooks = install_push_guard(git_env.runtime / "hooks")
    ws = LocalWorkspaces(
        git_env.source,
        [git_env.worktrees],
        REPO,
        remote=git_env.remote,
        runner_config_env={"GIT_CONFIG_NOSYSTEM": "1"},
        push_guard_dir=hooks,
        harness_settings=True,
    )
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / "factory-issue-9"
    git("worktree", "add", "-b", BRANCH, str(wt), "origin/main", cwd=git_env.source)
    ws.configure(
        ws.verify_worktree(wt, BRANCH),
        StageWiring("S", git_env.runtime / "c", git_env.runtime / "s", REPO),
        BOT,
    )
    (wt / "f.txt").write_text("x\n", encoding="utf-8")
    git("add", "f.txt", cwd=wt)
    git("commit", "-m", "work", cwd=wt)
    return ws, wt


def _push(wt: Path, remote: Path, *refspec: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - test-controlled argv
        ["git", "push", str(remote), *refspec],  # noqa: S607
        cwd=wt,
        capture_output=True,
        text=True,
        check=False,
    )


def test_guard_allows_only_the_parcel_branch_including_force(git_env: GitEnv) -> None:
    _, wt = _wired(git_env)
    remote = git_env.remote
    assert _push(wt, remote, f"HEAD:refs/heads/{BRANCH}").returncode == 0
    git("commit", "--amend", "-m", "rewritten", cwd=wt)
    assert _push(wt, remote, "--force", f"HEAD:{BRANCH}").returncode == 0  # own: fine
    for refspec in ("HEAD:main", "HEAD:refs/heads/other", "HEAD:refs/tags/v1"):
        denied = _push(wt, remote, refspec)
        assert denied.returncode != 0, refspec
        assert f"only refs/heads/{BRANCH} may be pushed" in denied.stderr
    git("tag", "v2", cwd=wt)
    assert _push(wt, remote, "--tags").returncode != 0
    assert "other" not in git("branch", "-a", cwd=wt)
    assert git("rev-parse", "main", cwd=remote) != git("rev-parse", "HEAD", cwd=wt)


def test_harness_settings_are_written_ignored_and_never_committed(git_env: GitEnv) -> None:
    ws, wt = _wired(git_env)
    settings = json.loads((wt / ".claude" / "settings.local.json").read_text())
    assert set(HARNESS_ALLOW_RULES) <= set(settings["permissions"]["allow"])
    assert git("status", "--porcelain", cwd=wt) == ""  # ignored: cannot be committed
    assert (
        "/.claude/settings.local.json" in (git_env.source / ".git" / "info" / "exclude").read_text()
    )
    # Idempotent re-wire merges instead of duplicating rules.
    ws.write_harness_settings(wt)
    again = json.loads((wt / ".claude" / "settings.local.json").read_text())
    assert again["permissions"]["allow"].count("Bash") == 1
