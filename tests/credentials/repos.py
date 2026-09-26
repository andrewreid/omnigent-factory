"""Disposable local Git fixtures: fake owner HOME, bare "remote", dedicated source clone.

The fake HOME carries an *owner* global Git credential helper and an owner ``gh``
hosts file holding ``OWNER-TOKEN`` sentinels, so tests can prove the factory path never
uses or edits them. No network: the "remote" is a local bare repository and base fetches
read from it directly, while the clone's configured origin stays the exact GitHub HTTPS URL.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from omnigent_factory.credentials.worktree import BotIdentity, Workspaces

REPO = "SA-Ambulance/timesheets"
OWNER_TOKEN = "OWNER-TOKEN-must-never-appear"  # noqa: S105 - sentinel, not a secret
RUNNER_CONFIG_ENV = {"GIT_CONFIG_NOSYSTEM": "1"}
BOT = BotIdentity("reid-factory[bot]", "123456+reid-factory[bot]@users.noreply.github.com")


def git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(  # noqa: S603 - test-controlled argv
        ["git", *args],  # noqa: S607
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    return proc.stdout.strip()


class LocalWorkspaces(Workspaces):
    """Fetches the base from the local bare remote instead of github.com."""

    def __init__(self, *args: object, remote: Path, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.local_remote = remote

    def fetch_base(self, base_branch: str = "main") -> str:
        ref = f"+refs/heads/{base_branch}:refs/remotes/origin/{base_branch}"
        git("fetch", "--no-tags", str(self.local_remote), ref, cwd=self.source_clone)
        return git("rev-parse", f"refs/remotes/origin/{base_branch}", cwd=self.source_clone)


@dataclass
class GitEnv:
    home: Path
    remote: Path
    source: Path
    worktrees: Path
    runtime: Path

    def workspaces(self) -> LocalWorkspaces:
        # The modelled runner environment ignores the host's /etc/gitconfig.
        return LocalWorkspaces(
            self.source,
            [self.worktrees],
            REPO,
            remote=self.remote,
            runner_config_env=RUNNER_CONFIG_ENV,
        )

    def home_digest(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for p in sorted(self.home.rglob("*")):
            if p.is_file():
                out[str(p.relative_to(self.home))] = hashlib.sha256(p.read_bytes()).hexdigest()
        return out


def make_git_env(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> GitEnv:
    home = tmp / "home"
    (home / ".config" / "gh").mkdir(parents=True)
    (home / ".gitconfig").write_text(
        "[user]\n\tname = Owner Human\n\temail = owner@example.com\n"
        "[credential]\n"
        f'\thelper = "!f() {{ echo username=owner; echo password={OWNER_TOKEN}; }}; f"\n'
        "[init]\n\tdefaultBranch = main\n",
        encoding="utf-8",
    )
    (home / ".config" / "gh" / "hosts.yml").write_text(
        f"github.com:\n    oauth_token: {OWNER_TOKEN}\n    user: owner\n", encoding="utf-8"
    )
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home / ".config"))
    for var in list(os.environ):
        if var.startswith("GIT_CONFIG") or var in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR"):
            monkeypatch.delenv(var, raising=False)
    for var in ("GIT_ASKPASS", "SSH_ASKPASS"):
        monkeypatch.delenv(var, raising=False)
    for key, value in RUNNER_CONFIG_ENV.items():
        monkeypatch.setenv(key, value)
    remote = tmp / "remote.git"
    git("init", "--bare", "-b", "main", str(remote), cwd=tmp)
    seed = tmp / "seed"
    git("clone", str(remote), str(seed), cwd=tmp)
    (seed / "README.md").write_text("seed\n", encoding="utf-8")
    git("add", "README.md", cwd=seed)
    git("commit", "-m", "seed", cwd=seed)
    git("push", "origin", "HEAD:main", cwd=seed)
    state = tmp / "state"
    source = state / "repos" / "timesheets"
    source.parent.mkdir(parents=True)
    git("clone", str(remote), str(source), cwd=tmp)
    git("remote", "set-url", "origin", f"https://github.com/{REPO}.git", cwd=source)
    worktrees = state / "worktrees"
    worktrees.mkdir()
    # Unix socket paths are length-limited; keep the runtime dir short.
    runtime = Path(tempfile.mkdtemp(prefix="of-", dir="/tmp"))
    os.chmod(runtime, 0o700)
    return GitEnv(home, remote, source, worktrees, runtime)
