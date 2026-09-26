"""Regressions for review t3/recheck-1 (sha256 53ae4fb1...): Git config-source
environment. Each test fails against candidate tree e631cba4 and passes after."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from omnigent_factory.credentials.worktree import StageWiring, WorktreeError
from tests.credentials.repos import BOT, REPO, GitEnv, LocalWorkspaces, git

REWRITE = ("url.ssh://git@evil.invalid/.insteadOf", "https://")


def _worktree(git_env: GitEnv, ws: LocalWorkspaces, name: str = "t") -> Path:
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / name
    git("worktree", "add", "-b", f"factory/{name}", str(wt), "origin/main", cwd=git_env.source)
    return wt


def _configure(git_env: GitEnv, ws: LocalWorkspaces, wt: Path) -> None:
    ws.configure(
        ws.verify_worktree(wt, f"factory/{wt.name}"),
        StageWiring("S", git_env.runtime / "c", git_env.runtime / "s", REPO),
        BOT,
    )


def _effective_origin(wt: Path) -> str:
    return subprocess.run(
        ["git", "remote", "get-url", "origin"],  # noqa: S607
        cwd=wt,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def test_hidden_global_rewrite_is_rejected(
    git_env: GitEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reviewer's repro: the owner-global rewrite is hidden only around configure()."""
    ws = git_env.workspaces()
    wt = _worktree(git_env, ws)
    git("config", "--global", *REWRITE, cwd=git_env.source)
    with monkeypatch.context() as m:
        m.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
        with pytest.raises(WorktreeError, match="config-source"):
            _configure(git_env, ws, wt)
    # What the separately launched session would really use:
    assert _effective_origin(wt).startswith("ssh://git@evil.invalid/")
    with pytest.raises(WorktreeError, match="rewrite"):  # normal environment: still refused
        _configure(git_env, ws, wt)


def test_hidden_system_rewrite_is_rejected(
    git_env: GitEnv, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    system = tmp_path / "system-gitconfig"
    system.write_text('[url "ssh://git@evil.invalid/"]\n\tinsteadOf = https://\n', "utf-8")
    runner = {"GIT_CONFIG_SYSTEM": str(system)}  # the runner reads this system config
    ws = LocalWorkspaces(
        git_env.source, [git_env.worktrees], REPO, remote=git_env.remote, runner_config_env=runner
    )
    monkeypatch.delenv("GIT_CONFIG_NOSYSTEM")
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", "/dev/null")  # daemon hides it
    wt = _worktree(git_env, ws)
    with pytest.raises(WorktreeError, match="config-source"):
        _configure(git_env, ws, wt)
    # Same selectors as the runner: the inspection sees the rewrite and refuses.
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", str(system))
    with pytest.raises(WorktreeError, match="rewrite"):
        _configure(git_env, ws, wt)


@pytest.mark.parametrize(
    "selectors",
    [
        {"GIT_CONFIG_GLOBAL": "/dev/null"},
        {"GIT_CONFIG_SYSTEM": "/dev/null"},
        {"GIT_CONFIG_NOSYSTEM": None},  # daemon reads a system config the runner ignores
        {
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "url.https://github.com/.insteadOf",
            "GIT_CONFIG_VALUE_0": "https://example.invalid/",
        },
        {"GIT_CONFIG_PARAMETERS": "'core.askpass'='true'"},
        {"GIT_CONFIG": "/dev/null"},
    ],
)
def test_any_differing_config_selector_fails_closed(
    git_env: GitEnv, monkeypatch: pytest.MonkeyPatch, selectors: dict[str, str | None]
) -> None:
    ws = git_env.workspaces()
    wt = _worktree(git_env, ws)
    for key, value in selectors.items():
        if value is None:
            monkeypatch.delenv(key)
        else:
            monkeypatch.setenv(key, value)
    with pytest.raises(WorktreeError, match="config-source"):
        _configure(git_env, ws, wt)
