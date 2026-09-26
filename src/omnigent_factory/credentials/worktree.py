"""Daemon-owned source clone and worktree-only Git wiring (architecture §5.1, §6.2).

Only the dedicated source clone's own configuration (``extensions.worktreeConfig``) and a
verified worktree's ``config.worktree`` are ever written. Nothing here runs
``git config --global`` / ``--system`` or touches ``gh`` configuration.

Wiring a worktree sets, in that worktree's config only:

* an empty ``credential.helper`` (resets inherited helpers) followed by the factory helper;
  ``credential.useHttpPath=true``; ``credential.interactive=false``;
* an empty generic ``http.extraHeader`` (a helper cannot override an inherited
  Authorization header, so inherited generic extra headers are reset);
* ``user.name`` / ``user.email`` from the App bot identity;
* nonsecret ``factory.*`` handles: stage session, capability-file path, socket, repository.

What cannot be reset per worktree fails closed (:meth:`Workspaces.check_transport`,
before and after wiring): any ``insteadOf``/``pushInsteadOf`` rewrite matching the origin
URL, effective fetch/push URLs other than the exact credential-free HTTPS URL, any
non-exact push URL, URL-specific extra headers and URL-specific credential helpers.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

GITHUB_HTTPS = "https://github.com/"


class WorktreeError(RuntimeError):
    """A workspace failed verification or could not be wired. Fail closed."""


@dataclass(frozen=True, slots=True)
class BotIdentity:
    """Commit identity of the GitHub App bot user (``<id>+<slug>[bot]@users.noreply...``)."""

    name: str
    email: str


@dataclass(frozen=True, slots=True)
class StageWiring:
    """Nonsecret handles written into a worktree's config for one stage session."""

    stage_session_id: str
    capability_file: Path
    socket_path: Path
    repository: str


@dataclass(frozen=True, slots=True)
class VerifiedWorktree:
    path: Path
    branch: str
    head_oid: str


def default_helper_command() -> str:
    """Absolute helper command (git appends the operation name)."""
    return f"!{shlex.quote(sys.executable)} -m omnigent_factory.credentials.git_helper"


def git_env(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Environment for daemon Git subprocesses: never prompt."""
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env.pop("GIT_DIR", None)
    env.pop("GIT_WORK_TREE", None)
    if extra:
        env.update(extra)
    return env


#: Variables that select or inject Git configuration sources ("config-source
#: selectors"): ``GIT_CONFIG``, ``GIT_CONFIG_GLOBAL``, ``GIT_CONFIG_SYSTEM``,
#: ``GIT_CONFIG_NOSYSTEM``, ``GIT_CONFIG_COUNT``/``GIT_CONFIG_KEY_n``/``GIT_CONFIG_VALUE_n``,
#: ``GIT_CONFIG_PARAMETERS`` - everything with this prefix - plus repository selectors.
CONFIG_SELECTOR_PREFIX = "GIT_CONFIG"
REPOSITORY_SELECTORS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR")


def config_selectors(env: Mapping[str, str]) -> dict[str, str]:
    return {
        k: v
        for k, v in env.items()
        if k.startswith(CONFIG_SELECTOR_PREFIX) or k in REPOSITORY_SELECTORS
    }


def inspection_env(runner_config_env: Mapping[str, str]) -> dict[str, str]:
    """Deterministic environment for inspecting/wiring what the *runner* will see:
    inherited selectors removed, exactly the declared runner selectors applied."""
    env = {k: v for k, v in git_env().items() if k not in config_selectors(os.environ)}
    env.update(runner_config_env)
    return env


def _run(
    args: Sequence[str],
    cwd: Path,
    *,
    check: bool = True,
    git: str = "git",
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [git, *args],
        cwd=cwd,
        env=dict(env) if env is not None else git_env(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if check and proc.returncode != 0:
        raise WorktreeError(f"git {' '.join(args[:3])} failed: {proc.stderr.strip()[:500]}")
    return proc


def _within(path: Path, roots: Sequence[Path]) -> bool:
    return any(path == root or root in path.parents for root in roots)


class Workspaces:
    """Operations on the dedicated source clone and its worktrees.

    ``repository`` is ``owner/name``; ``owned_roots`` bound every accepted worktree path
    (after resolving symlinks).

    ``runner_config_env`` declares the Git config-source selectors of the supported
    Omnigent runner environment (normally none). Every Git call here runs with exactly
    those selectors, and wiring fails closed when the daemon's own environment carries
    different ones, because the configuration inspected could then differ from the
    configuration the session uses (review recheck-1).
    """

    def __init__(
        self,
        source_clone: Path,
        owned_roots: Sequence[Path],
        repository: str,
        *,
        git: str = "git",
        runner_config_env: Mapping[str, str] | None = None,
    ) -> None:
        declared = dict(runner_config_env or {})
        if config_selectors(declared) != declared:
            raise WorktreeError("runner_config_env may only declare Git config-source selectors")
        self.runner_config_env = declared
        self.inspection_env = inspection_env(declared)
        self.source_clone = source_clone.resolve()
        self.owned_roots = tuple(r.resolve() for r in owned_roots)
        self.repository = repository
        self.git = git

    @property
    def remote_url(self) -> str:
        return f"{GITHUB_HTTPS}{self.repository}.git"

    def _git(self, args: Sequence[str], cwd: Path, *, check: bool = True) -> str:
        return _run(args, cwd, check=check, git=self.git, env=self.inspection_env).stdout.strip()

    # ------------------------------------------------------------ source clone

    def common_dir(self, path: Path) -> Path:
        out = self._git(["rev-parse", "--path-format=absolute", "--git-common-dir"], path)
        return Path(out).resolve()

    def ensure_source_clone(self) -> None:
        """Verify the dedicated clone and enable per-worktree config in it only."""
        if not (self.source_clone / ".git").is_dir():
            raise WorktreeError("source clone is not an ordinary non-bare clone")
        urls = self._git(["config", "--get-all", "remote.origin.url"], self.source_clone)
        if urls.splitlines() != [self.remote_url]:
            raise WorktreeError("source clone origin is not exactly the expected HTTPS URL")
        push = _run(
            ["config", "--get-all", "remote.origin.pushurl"],
            self.source_clone,
            check=False,
            git=self.git,
            env=self.inspection_env,
        ).stdout.split()
        if push and push != [self.remote_url]:
            raise WorktreeError("source clone push URL differs from the expected HTTPS URL")
        self._git(["config", "--local", "extensions.worktreeConfig", "true"], self.source_clone)

    def fetch_base(self, base_branch: str = "main") -> str:
        """Fetch ``origin/<base>`` explicitly and return its OID (§5.1)."""
        ref = f"+refs/heads/{base_branch}:refs/remotes/origin/{base_branch}"
        self._git(["fetch", "--no-tags", "origin", ref], self.source_clone)
        return self._git(["rev-parse", f"refs/remotes/origin/{base_branch}"], self.source_clone)

    def find_branch_worktree(self, branch: str) -> Path | None:
        """The worktree that has ``branch`` checked out, if any (orphan inspection)."""
        out = self._git(["worktree", "list", "--porcelain"], self.source_clone)
        current: Path | None = None
        for line in out.splitlines():
            if line.startswith("worktree "):
                current = Path(line[len("worktree ") :])
            elif line == f"branch refs/heads/{branch}" and current is not None:
                return current
        return None

    def branch_exists(self, branch: str) -> bool:
        proc = _run(
            ["rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
            self.source_clone,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        return proc.returncode == 0

    # ------------------------------------------------------------ worktrees

    def verify_worktree(self, workspace: str | Path, branch: str) -> VerifiedWorktree:
        """Verify ``workspace`` is our clone's worktree on ``branch`` inside owned roots."""
        raw = Path(workspace)
        if not raw.is_absolute():
            raise WorktreeError("workspace path is not absolute")
        if not raw.is_dir():
            raise WorktreeError("workspace does not exist")
        resolved = raw.resolve()
        if resolved != raw.absolute():
            raise WorktreeError("workspace path traverses a symlink")
        if not _within(resolved, self.owned_roots):
            raise WorktreeError("workspace is outside the approved owned roots")
        top = Path(self._git(["rev-parse", "--show-toplevel"], resolved)).resolve()
        if top != resolved:
            raise WorktreeError("workspace is not a worktree root")
        if self.common_dir(resolved) != self.common_dir(self.source_clone):
            raise WorktreeError("workspace does not belong to the dedicated source clone")
        head = _run(
            ["symbolic-ref", "--short", "-q", "HEAD"],
            resolved,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        if head.returncode != 0 or head.stdout.strip() != branch:
            raise WorktreeError("workspace is not on the recorded branch")
        oid = self._git(["rev-parse", "HEAD"], resolved)
        return VerifiedWorktree(resolved, branch, oid)

    def _config_entries(self, path: Path, pattern: str) -> list[tuple[str, str]]:
        proc = _run(
            ["config", "--get-regexp", pattern],
            path,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        out = []
        for line in proc.stdout.splitlines():
            key, _, value = line.partition(" ")
            out.append((key.lower(), value))
        return out

    def check_config_environment(self) -> None:
        """The daemon's config-source selectors must equal the declared runner ones."""
        inherited = config_selectors(os.environ)
        if inherited != self.runner_config_env:
            names = sorted(set(inherited) ^ set(self.runner_config_env)) or sorted(inherited)
            raise WorktreeError(
                "Git config-source environment differs from the runner's; "
                f"equivalence unprovable: {', '.join(names)}"
            )

    def check_transport(self, path: Path) -> None:
        """Fail closed unless Git's *effective* transport for origin is the exact HTTPS URL
        through the factory helper (review r1 B2).

        * no ``url.<base>.insteadOf`` / ``pushInsteadOf`` whose prefix matches the intended
          URL (generic ``https://`` rewrites included);
        * effective fetch/push URLs after all rewriting, and every configured push URL,
          equal exactly the credential-free HTTPS URL;
        * no URL-specific ``http.<url>.extraHeader`` with a value (a helper cannot
          override an inherited Authorization header), and the generic list ends reset;
        * no URL-specific ``credential.<url>.helper`` that could answer instead of ours.
        """
        self.check_config_environment()
        want = self.remote_url
        for key, value in self._config_entries(path, r"^url\..*\.(insteadof|pushinsteadof)$"):
            if value and want.lower().startswith(value.lower()):
                raise WorktreeError(f"URL rewrite transforms the origin URL: {key}")
        fetch = self._git(["remote", "get-url", "--all", "origin"], path).splitlines()
        push = self._git(["remote", "get-url", "--push", "--all", "origin"], path).splitlines()
        resolved = self._git(["ls-remote", "--get-url", "origin"], path)
        if fetch != [want] or push != [want] or resolved != want:
            raise WorktreeError("effective origin fetch/push URL is not exactly the expected URL")
        for key, value in self._config_entries(path, r"^remote\.origin\.pushurl$"):
            if value != want:
                raise WorktreeError(f"non-exact push URL configured: {key}")
        for key, value in self._config_entries(path, r"^http\..+\.extraheader$"):
            if value:
                raise WorktreeError(f"URL-specific extra header would bypass the helper: {key}")
        for key, value in self._config_entries(path, r"^credential\..+\.helper$"):
            if value:
                raise WorktreeError(f"URL-specific credential helper is configured: {key}")

    def _generic_resets_hold(self, path: Path) -> None:
        for key in ("http.extraHeader", "credential.helper"):
            values = _run(
                ["config", "--get-all", key],
                path,
                check=False,
                git=self.git,
                env=self.inspection_env,
            ).stdout.split("\n")
            values = values[:-1] if values and values[-1] == "" else values
            reset = [i for i, v in enumerate(values) if v == ""]
            if not reset:
                raise WorktreeError(f"{key} is not reset in the worktree")
            if key == "http.extraHeader" and any(values[reset[-1] + 1 :]):
                raise WorktreeError("an extra header is configured after the reset")
            if key == "credential.helper" and len(values[reset[-1] + 1 :]) != 1:
                raise WorktreeError("credential helpers follow the factory helper")

    def configure(
        self,
        verified: VerifiedWorktree,
        wiring: StageWiring,
        identity: BotIdentity,
        *,
        helper_command: str | None = None,
    ) -> None:
        """Write the stage's wiring into the worktree's own config (idempotent)."""
        if wiring.repository != self.repository:
            raise WorktreeError("wiring names a different repository")
        path = verified.path
        self.check_config_environment()
        self.check_transport(path)
        helper = helper_command or default_helper_command()

        def cfg(*args: str) -> None:
            self._git(["config", "--worktree", *args], path)

        for key in ("credential.helper", "http.extraHeader"):
            _run(
                ["config", "--worktree", "--unset-all", key],
                path,
                check=False,
                git=self.git,
                env=self.inspection_env,
            )
        cfg("--add", "credential.helper", "")
        cfg("--add", "credential.helper", helper)
        cfg("credential.useHttpPath", "true")
        cfg("credential.interactive", "false")
        cfg("--add", "http.extraHeader", "")
        cfg("user.name", identity.name)
        cfg("user.email", identity.email)
        cfg("factory.stageSession", wiring.stage_session_id)
        cfg("factory.capabilityFile", str(wiring.capability_file))
        cfg("factory.socket", str(wiring.socket_path))
        cfg("factory.repository", wiring.repository)
        self.check_transport(path)
        self._generic_resets_hold(path)

    def wiring_of(self, path: Path) -> StageWiring | None:
        def get(key: str) -> str | None:
            proc = _run(
                ["config", "--get", key], path, check=False, git=self.git, env=self.inspection_env
            )
            return proc.stdout.strip() or None

        sid, cap, sock, repo = (
            get("factory.stageSession"),
            get("factory.capabilityFile"),
            get("factory.socket"),
            get("factory.repository"),
        )
        if sid is None or cap is None or sock is None or repo is None:
            return None
        return StageWiring(sid, Path(cap), Path(sock), repo)
