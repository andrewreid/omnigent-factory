"""``/.molly/`` is excluded through the factory clone's effective info/exclude only."""

from __future__ import annotations

from tests.credentials.repos import GitEnv, git


def test_molly_scratch_is_excluded_idempotently_for_clone_and_linked_worktrees(
    git_env: GitEnv,
) -> None:
    ws = git_env.workspaces()
    gitignore = git_env.source / ".gitignore"
    before = gitignore.read_bytes() if gitignore.exists() else None
    assert ws.ensure_excluded("/.molly/") is True
    assert ws.ensure_excluded("/.molly/") is False  # idempotent
    exclude = git_env.source / ".git" / "info" / "exclude"
    assert exclude.read_text().splitlines().count("/.molly/") == 1
    assert (gitignore.read_bytes() if gitignore.exists() else None) == before

    # A linked worktree (how Omnigent runs a stage) shares the same exclude file.
    linked = git_env.worktrees / "factory-issue-1"
    git("worktree", "add", "-b", "factory/issue-1", str(linked), cwd=git_env.source)
    (linked / ".molly").mkdir()
    (linked / ".molly" / "notes.md").write_text("scratch")
    (git_env.source / ".molly").mkdir()
    (git_env.source / ".molly" / "x").write_text("scratch")
    assert git("status", "--porcelain", "--untracked-files=all", cwd=linked) == ""
    assert git("status", "--porcelain", "--untracked-files=all", cwd=git_env.source) == ""
