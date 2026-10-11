"""#888: two create_session effects ran in the shared source clone at once; one hit
``git config --local extensions.worktreeConfig`` while the other held ``.git/config.lock``
("could not lock config file .git/config: File exists") and the card ended Blocked
("workspace-rejected"). Writes to the clone's shared ``.git`` are now serialized behind
one process-wide lock, a transient Git lock is retried briefly, a lock that outlasts the
retries is a retryable failure (never a definitive rejection), and the per-worktree
config extension is written only when it is not already on. Real Git.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from omnigent_factory.core.effects import DefinitiveFailure, EffectKind, RetryableReadFailure
from omnigent_factory.core.types import SessionKind
from omnigent_factory.credentials import worktree
from tests.credentials.repos import GitEnv, git
from tests.omnigent.support import CTX, intent, make_rig, spec


@pytest.fixture(autouse=True)
def _fast_lock_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worktree, "LOCK_RETRY_DELAYS_S", (0.05, 0.1, 0.2, 0.4), raising=False)


def _unset_extension(git_env: GitEnv) -> Path:
    """A fresh clone: the extension is not on yet, so ensuring it must write the config."""
    config = (git_env.source / ".git" / "config").read_text()
    assert "worktreeconfig" not in config.lower()
    return git_env.source / ".git" / "config.lock"


def test_a_transient_config_lock_is_retried(git_env: GitEnv) -> None:
    lock = _unset_extension(git_env)
    lock.write_text("held by another git\n")
    release = threading.Timer(0.15, lock.unlink)
    release.start()
    try:
        git_env.workspaces().ensure_source_clone()
    finally:
        release.join()
    assert git("config", "--local", "extensions.worktreeConfig", cwd=git_env.source) == "true"


def test_an_enabled_extension_is_not_rewritten(git_env: GitEnv) -> None:
    ws = git_env.workspaces()
    ws.ensure_source_clone()
    lock = git_env.source / ".git" / "config.lock"
    lock.write_text("held by another git\n")  # e.g. Omnigent adding a worktree
    ws.ensure_source_clone()  # nothing to write: no lock needed
    assert lock.exists()


def test_clone_writes_wait_for_the_process_wide_lock(git_env: GitEnv) -> None:
    _unset_extension(git_env)
    ws = git_env.workspaces()
    done = threading.Event()

    def run() -> None:
        ws.ensure_source_clone()
        done.set()

    with worktree.CLONE_LOCK:
        thread = threading.Thread(target=run)
        thread.start()
        assert not done.wait(0.3)  # blocked behind the holder
    thread.join(10)
    assert done.is_set()
    assert git("config", "--local", "extensions.worktreeConfig", cwd=git_env.source) == "true"


@pytest.mark.asyncio
async def test_a_lock_outlasting_the_retries_is_retryable_not_a_rejection(
    git_env: GitEnv,
) -> None:
    lock = _unset_extension(git_env)
    lock.write_text("held by another git\n")
    rig = make_rig(git_env)
    rig.directory.specs["P1"] = spec(
        "P1", kind=SessionKind.PLAN, branch="factory/issue-888", nonce="nonce-1"
    )
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "P1", nonce="nonce-1"), CTX)
    assert not isinstance(out, DefinitiveFailure), out
    assert isinstance(out, RetryableReadFailure), out
    assert "could not lock" in out.reason
    assert not rig.server.requests  # no session was asked for
    lock.unlink()
    again = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "P1", nonce="nonce-1"), CTX)
    assert not isinstance(again, DefinitiveFailure | RetryableReadFailure), again
