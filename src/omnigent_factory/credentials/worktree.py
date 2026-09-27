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
non-exact push URL and URL-specific extra headers.

Credential helpers are judged by their *effective* chain, not by presence: Git applies
every ``credential.helper`` and matching ``credential.<url>.helper`` in configuration
order (system, global, local, worktree) and an empty value resets the list. The owner's
standard ``gh auth setup-git`` entries (``credential.https://github.com.helper``) come
before the worktree's reset and are cleared by it. After wiring, the chain Git would use
for the origin URL must be exactly ``[factory helper]`` (:meth:`Workspaces.effective_helpers`).
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

GITHUB_HTTPS = "https://github.com/"

#: Claude Code tools pre-approved in factory worktrees (bare names match every use).
HARNESS_ALLOW_RULES = (
    "Bash",
    "Read",
    "Edit",
    "MultiEdit",
    "Write",
    "Glob",
    "Grep",
    "NotebookEdit",
    "WebFetch",
    "WebSearch",
    "Skill",
    "ToolSearch",
    "TodoWrite",
    "Task",
    "mcp__omnigent",
)


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
        push_guard_dir: Path | None = None,
        harness_settings: bool = False,
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
        #: ``core.hooksPath`` for wired worktrees (the parcel-branch ``pre-push`` guard).
        self.push_guard_dir = push_guard_dir
        #: Write worktree-scoped Claude Code allow rules (never committed; see below).
        self.harness_settings = harness_settings

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
          override an inherited Authorization header), and the generic list ends reset.

        Credential helpers are checked after wiring as an effective chain (see
        :meth:`effective_helpers`); an inherited URL-specific helper is not a bypass
        when the worktree's reset clears it.
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

    def effective_helpers(self, path: Path) -> list[str]:
        """The credential helpers Git would run for the origin URL inside ``path``.

        Mirrors Git's credential config application (every matching entry in config
        order; an empty value resets the list). URL matching is delegated to Git's own
        ``--get-urlmatch`` against a one-entry probe file, so wildcards, paths and
        normalisation follow Git exactly.
        """
        proc = _run(
            ["config", "-z", "--get-regexp", r"^credential\.(.+\.)?helper$"],
            path,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        if proc.returncode not in (0, 1):
            raise WorktreeError("credential helper configuration is unreadable")
        chain: list[str] = []
        for record in proc.stdout.split("\0"):
            if not record:
                continue
            key, separator, value = record.partition("\n")
            if not separator:
                raise WorktreeError(f"credential helper without a value: {key}")
            if key.lower() != "credential.helper" and not self._helper_url_matches(
                path, key[len("credential.") : -len(".helper")]
            ):
                continue
            chain = [] if value == "" else [*chain, value]
        return chain

    def _helper_url_matches(self, path: Path, pattern: str) -> bool:
        if any(char in pattern for char in '"\\\n'):
            raise WorktreeError(f"unverifiable credential URL pattern: {pattern!r}")
        with tempfile.TemporaryDirectory(prefix="factory-urlmatch-") as tmp:
            probe = Path(tmp) / "probe"
            probe.write_text(f'[credential "{pattern}"]\n\thelper = probe\n', encoding="utf-8")
            proc = _run(
                [
                    "config",
                    "--file",
                    str(probe),
                    "--get-urlmatch",
                    "credential.helper",
                    self.remote_url,
                ],
                path,
                check=False,
                git=self.git,
                env=self.inspection_env,
            )
        if proc.returncode not in (0, 1):
            raise WorktreeError(f"cannot evaluate credential URL pattern: {pattern!r}")
        return proc.returncode == 0 and proc.stdout.strip() == "probe"

    def _check_helper_chain(self, path: Path, helper: str) -> None:
        chain = self.effective_helpers(path)
        if chain != [helper]:
            residual = len([h for h in chain if h != helper])
            raise WorktreeError(
                "effective credential helpers for origin are not exactly the factory helper "
                f"({len(chain)} configured, {residual} other)"
            )

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
        cfg("factory.branch", verified.branch)
        if self.push_guard_dir is not None:
            cfg("core.hooksPath", str(self.push_guard_dir))
        if self.harness_settings:
            self.write_harness_settings(path)
        self.check_transport(path)
        self._generic_resets_hold(path)
        self._check_helper_chain(path, helper)

    def write_harness_settings(self, path: Path) -> bool:
        """Worktree-scoped Claude Code allow rules so factory sessions never stop on a
        harness permission prompt for normal work (owner direction 2026-09-27).

        Written to ``<worktree>/.claude/settings.local.json`` only after Git confirms the
        file is ignored (it is added to the dedicated clone's ``info/exclude`` if needed),
        so it can never be committed. Omnigent's session policies still apply: allow rules
        skip Claude's own prompt, not Omnigent's PreToolUse/TOOL_CALL policy hooks.
        Returns whether the file was written.
        """
        rel = ".claude/settings.local.json"
        if not self._ignored(path, rel):
            exclude = self.common_dir(path) / "info" / "exclude"
            exclude.parent.mkdir(parents=True, exist_ok=True)
            existing = exclude.read_text(encoding="utf-8") if exclude.exists() else ""
            if f"/{rel}" not in existing.splitlines():
                with exclude.open("a", encoding="utf-8") as handle:
                    handle.write(
                        ("" if existing.endswith("\n") or not existing else "\n") + f"/{rel}\n"
                    )
            if not self._ignored(path, rel):
                return False
        target = path / rel
        current: dict[str, object] = {}
        if target.is_file():
            try:
                loaded = json.loads(target.read_text(encoding="utf-8"))
                current = loaded if isinstance(loaded, dict) else {}
            except ValueError:
                current = {}
        permissions = current.get("permissions")
        permissions = dict(permissions) if isinstance(permissions, dict) else {}
        allow = [a for a in permissions.get("allow", []) if isinstance(a, str)]
        dirs = [d for d in permissions.get("additionalDirectories", []) if isinstance(d, str)]
        for rule in HARNESS_ALLOW_RULES:
            if rule not in allow:
                allow.append(rule)
        for extra in (str(self.common_dir(path)), tempfile.gettempdir()):
            if extra not in dirs:
                dirs.append(extra)
        permissions.update(allow=allow, additionalDirectories=dirs)
        current["permissions"] = permissions
        target.parent.mkdir(exist_ok=True)
        tmp = target.with_suffix(".tmp")
        tmp.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, target)
        return True

    def _ignored(self, path: Path, rel: str) -> bool:
        proc = _run(
            ["check-ignore", "-q", rel], path, check=False, git=self.git, env=self.inspection_env
        )
        return proc.returncode == 0

    # ------------------------------------------------------------ cleanup

    def worktrees(self) -> list[tuple[Path, str | None]]:
        """(path, branch) of every linked worktree of the dedicated clone."""
        out = self._git(["worktree", "list", "--porcelain"], self.source_clone)
        rows: list[tuple[Path, str | None]] = []
        current: Path | None = None
        branch: str | None = None
        for line in [*out.splitlines(), ""]:
            if line.startswith("worktree "):
                current, branch = Path(line[len("worktree ") :]).resolve(), None
            elif line.startswith("branch refs/heads/"):
                branch = line[len("branch refs/heads/") :]
            elif not line and current is not None:
                if current != self.source_clone:
                    rows.append((current, branch))
                current, branch = None, None
        return rows

    def is_clean(self, path: Path) -> bool:
        """No modified tracked files and no untracked (non-ignored) files."""
        return self._git(["status", "--porcelain"], path) == ""

    def branch_tip(self, branch: str) -> str | None:
        proc = _run(
            ["rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
            self.source_clone,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        return proc.stdout.strip() or None

    def remove_worktree(self, path: Path, *, force: bool) -> None:
        """Remove one linked worktree of the dedicated clone (never the clone itself)."""
        resolved = path.resolve()
        if resolved == self.source_clone or all(resolved != p for p, _ in self.worktrees()):
            raise WorktreeError("not a linked worktree of the dedicated clone")
        args = ["worktree", "remove", *(["--force"] if force else []), str(resolved)]
        self._git(args, self.source_clone)

    def delete_branch(self, branch: str) -> None:
        """Delete a local factory branch of the dedicated clone that no worktree uses."""
        if not branch.startswith("factory/"):
            raise WorktreeError("only factory branches may be deleted")
        if any(b == branch for _, b in self.worktrees()):
            raise WorktreeError("branch is checked out in a worktree")
        self._git(["branch", "-D", branch], self.source_clone)

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
