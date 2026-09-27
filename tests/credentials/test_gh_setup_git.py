"""Live pilot bug: the owner's standard ``gh auth setup-git`` global config refused every
stage prepare ("URL-specific credential helper is configured"). The worktree's generic
reset already clears it; the check now judges the *effective* helper chain."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from omnigent_factory.core.effects import CredentialProfile
from omnigent_factory.credentials.worktree import (
    StageWiring,
    WorktreeError,
    default_helper_command,
)
from tests.credentials.repos import BOT, OWNER_TOKEN, REPO, GitEnv, git
from tests.credentials.test_identity_e2e import Stack, credential_fill, password


def _gh_setup_git(env: GitEnv) -> Path:
    """Append exactly what ``gh auth setup-git`` writes, with a recording fake ``gh``."""
    marker = env.home / "gh-helper-called"
    fake_gh = env.home / "gh"
    fake_gh.write_text(
        f"#!/bin/sh\ntouch {marker}\necho username=owner\necho password={OWNER_TOKEN}\n",
        encoding="utf-8",
    )
    fake_gh.chmod(0o700)
    with (env.home / ".gitconfig").open("a", encoding="utf-8") as config:
        for host in ("https://github.com", "https://gist.github.com"):
            config.write(
                f'[credential "{host}"]\n\thelper =\n\thelper = !{fake_gh} auth git-credential\n'
            )
    return marker


@pytest.mark.asyncio
async def test_standard_gh_setup_git_is_overridden_and_bot_helper_answers(
    git_env: GitEnv,
) -> None:
    marker = _gh_setup_git(git_env)
    stack = Stack(git_env)
    await stack.server.start()
    try:
        wt = stack.worktree("factory/issue-677")
        # Fails closed before the fix: WorktreeError("URL-specific credential helper ...").
        await stack.wire(wt, "factory/issue-677", "PLAN1", CredentialProfile.READ_ONLY)
        assert stack.ws.effective_helpers(wt) == [default_helper_command()]

        code, out, err = await credential_fill(wt)
        assert code == 0, err
        assert "username=x-access-token" in out
        token = password(out)
        assert token is not None and token.startswith("ghs_bot_")
        assert OWNER_TOKEN not in out
        assert not marker.exists(), "the owner's gh helper must never run in the worktree"
        # The owner's global config is untouched (no requirement to change it).
        assert "gh auth git-credential" in (git_env.home / ".gitconfig").read_text()
    finally:
        await stack.server.close()


def test_owner_checkout_outside_the_worktree_still_uses_gh(git_env: GitEnv) -> None:
    _gh_setup_git(git_env)
    ws = git_env.workspaces()
    helpers = ws.effective_helpers(git_env.source)
    assert helpers[-1].endswith("gh auth git-credential")
    assert default_helper_command() not in helpers


def test_residual_helper_after_the_worktree_reset_is_refused(git_env: GitEnv) -> None:
    _gh_setup_git(git_env)
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    ws.fetch_base()
    wt = git_env.worktrees / "factory-issue-9"
    git("worktree", "add", "-b", "factory/issue-9", str(wt), "origin/main", cwd=git_env.source)
    # A path-specific helper later in the worktree's own config than the generic reset
    # survives it: Git would run it after the factory helper.
    for args in (
        ("credential.useHttpPath", "true"),
        (f"credential.https://github.com/{REPO}.git.helper", "!echo password=RESIDUAL"),
    ):
        subprocess.run(["git", "config", "--worktree", *args], cwd=wt, check=True)  # noqa: S603, S607
    with pytest.raises(WorktreeError, match="effective credential helpers"):
        ws.configure(
            ws.verify_worktree(wt, "factory/issue-9"),
            StageWiring("S", git_env.runtime / "c", git_env.runtime / "s", REPO),
            BOT,
        )


def test_non_matching_url_helper_is_ignored(git_env: GitEnv) -> None:
    ws = git_env.workspaces()
    subprocess.run(
        ["git", "config", "--global", "credential.https://example.com.helper", "!other"],  # noqa: S607
        check=True,
    )
    assert "!other" not in ws.effective_helpers(git_env.source)
