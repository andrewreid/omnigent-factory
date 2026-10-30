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
import math
import os
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

REFERENCE_NAME = re.compile(r"factory-ref-(?P<oid>[0-9a-f]{12})(?:-[1-9][0-9]*)?\Z")
REFERENCE_SETTINGS = '{"autoMemoryEnabled": false}\n'
REFERENCE_META = "factory-reference.json"

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


class ReferenceFetchError(WorktreeError):
    """A reference snapshot could not refresh the base; retry before creating a session."""


class GitLockBusy(WorktreeError):
    """Another Git process held a lock of the clone (``index.lock``, ``config.lock``, a ref
    lock) for longer than the short retry. Transient: retry later, never a rejection."""


class OffBranchError(WorktreeError):
    """A worktree is off its recorded branch and could not be switched back safely.

    The message is owner-facing (the card's "Factory note") and names what to fix.
    """


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


@dataclass(frozen=True, slots=True)
class BaseSync:
    """How a stage run's start brought the issue worktree up to ``origin/<base>``.

    ``status``: ``fast_forwarded`` (no commits of its own, clean: moved ``old_oid`` ->
    ``new_oid``, ``behind`` commits), ``up_to_date``, ``own_commits`` (never merged or
    rebased by the factory; ``behind`` commits on the base since), ``not_synced`` (dirty,
    detached, off its branch or mid-operation: untouched, ``reason`` says why) or
    ``fetch_failed`` (the base could not be fetched: the worktree may be stale).
    """

    status: str
    base: str
    old_oid: str = ""
    new_oid: str = ""
    behind: int = 0
    reason: str = ""

    def to_json(self) -> dict[str, str | int]:
        return {
            "status": self.status,
            "base": self.base,
            "old_oid": self.old_oid,
            "new_oid": self.new_oid,
            "behind": self.behind,
            "reason": self.reason,
        }


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


#: Every write to the dedicated clone's shared ``.git`` (config, refs, worktree
#: registrations, ``info/exclude``) in this process holds this lock (#888: two sessions
#: created at once collided on ``.git/config.lock``). Re-entrant: writers nest.
CLONE_LOCK = threading.RLock()
#: Waits before each retry of a Git command that found a lock held (bounded, ~3 s).
LOCK_RETRY_DELAYS_S: tuple[float, ...] = (0.1, 0.2, 0.4, 0.8, 1.6)


def _clone_write[**P, R](method: Callable[P, R]) -> Callable[P, R]:
    """Run a method that writes the clone's shared ``.git`` under :data:`CLONE_LOCK`."""

    @wraps(method)
    def locked(*args: P.args, **kwargs: P.kwargs) -> R:
        with CLONE_LOCK:
            return method(*args, **kwargs)

    return locked


def git_lock_busy(stderr: str) -> bool:
    """Git refused because another process holds one of its lock files."""
    text = stderr.lower()
    return "could not lock" in text or (".lock" in text and "file exists" in text)


def _run(
    args: Sequence[str],
    cwd: Path,
    *,
    check: bool = True,
    git: str = "git",
    env: Mapping[str, str] | None = None,
    timeout: float = 120,
) -> subprocess.CompletedProcess[str]:
    for delay in (*LOCK_RETRY_DELAYS_S, None):
        proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
            [git, *args],
            cwd=cwd,
            env=dict(env) if env is not None else git_env(),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode == 0 or delay is None or not git_lock_busy(proc.stderr):
            break
        time.sleep(delay)  # another Git process (Omnigent, a stage) holds a lock
    if check and proc.returncode != 0:
        error = GitLockBusy if git_lock_busy(proc.stderr) else WorktreeError
        raise error(f"git {' '.join(args[:3])} failed: {proc.stderr.strip()[:500]}")
    return proc


_OID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")


def _check_oid(oid: str) -> None:
    if not _OID.fullmatch(oid):
        raise WorktreeError("not a full object id")


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

    @_clone_write
    def ensure_source_clone(self) -> None:
        """Verify the dedicated clone and enable per-worktree config in it only (written
        only when not already on, so a ready clone needs no config lock)."""
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
        enabled = _run(
            ["config", "--local", "--get", "extensions.worktreeConfig"],
            self.source_clone,
            check=False,
            git=self.git,
            env=self.inspection_env,
        ).stdout.strip()
        if enabled != "true":
            self._git(["config", "--local", "extensions.worktreeConfig", "true"], self.source_clone)

    @_clone_write
    def ensure_excluded(self, pattern: str) -> bool:
        """Add ``pattern`` to the clone's effective ``info/exclude`` once (idempotent).

        The path comes from Git (``--git-path``), so every linked worktree of the clone
        shares it; the repository's tracked ``.gitignore`` is never touched. Returns
        whether the file changed.
        """
        out = self._git(
            ["rev-parse", "--path-format=absolute", "--git-path", "info/exclude"],
            (self.source_clone),
        )
        path = Path(out)
        existing = path.read_text(encoding="utf-8") if path.is_file() else ""
        if pattern in (line.strip() for line in existing.splitlines()):
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        prefix = "" if not existing or existing.endswith("\n") else "\n"
        with path.open("a", encoding="utf-8") as stream:
            stream.write(f"{prefix}{pattern}\n")
        return True

    @_clone_write
    def fetch_base(self, base_branch: str = "main") -> str:
        """Fetch ``origin/<base>`` explicitly and return its OID (§5.1).

        The daemon schedules clone maintenance itself.  Suppress fetch's implicit
        ``maintenance run --auto``: reflog expiry rewrites every linked worktree's HEAD
        reflog even when that worktree had no activity.
        """
        ref = f"+refs/heads/{base_branch}:refs/remotes/origin/{base_branch}"
        self._git(
            ["-c", "maintenance.auto=false", "fetch", "--no-tags", "origin", ref],
            self.source_clone,
        )
        return self._git(["rev-parse", f"refs/remotes/origin/{base_branch}"], self.source_clone)

    # Read-only questions about the fetched base (no fetch, no lock). The caller reads
    # the base OID once (``base_head``) and passes it to every range, so a concurrent
    # fetch moving the ref cannot mix two bases into one answer. OIDs must be full ids.

    def base_head(self, base_branch: str = "main") -> str:
        """The last fetched ``origin/<base>`` OID (no fetch)."""
        oid = self._git(
            ["rev-parse", "--verify", f"refs/remotes/origin/{base_branch}^{{commit}}"],
            self.source_clone,
        )
        _check_oid(oid)
        return oid

    def commits_since(self, oid: str, head: str) -> int:
        """Commits in ``head`` (a captured base OID, e.g. ``base_head``) since ``oid``."""
        _check_oid(oid)
        _check_oid(head)
        out = self._git(["rev-list", "--count", f"{oid}..{head}"], self.source_clone)
        return int(out or 0)

    def changed_since(self, oid: str, head: str, paths: Sequence[str]) -> tuple[str, ...]:
        """Which of ``paths`` (literal pathspecs; a directory covers its files) changed
        between ``oid`` and ``head`` (a captured base OID)."""
        _check_oid(oid)
        _check_oid(head)
        if not paths:
            return ()
        out = self._git(
            [
                "--literal-pathspecs",
                "diff",
                "--name-only",
                "--no-renames",
                oid,
                head,
                "--",
                *paths,
            ],
            self.source_clone,
        )
        return tuple(line for line in out.splitlines() if line)

    def commits_behind(self, workspace: str | Path, base_branch: str = "main") -> int:
        """Commits on the last fetched ``origin/<base>`` that HEAD lacks (no fetch)."""
        path = self._owned_worktree(workspace)
        out = self._git(["rev-list", "--count", f"HEAD..refs/remotes/origin/{base_branch}"], path)
        return int(out or 0)

    def sync_with_base(
        self, workspace: str | Path, branch: str, base_branch: str = "main"
    ) -> BaseSync:
        """Fetch ``origin/<base>`` and fast-forward an issue worktree with no commits of
        its own to it (at a stage run's start only, never mid-stage).

        Only ``git merge --ff-only`` on a clean worktree on ``branch`` whose HEAD is an
        ancestor of the base; a branch with its own commits is never merged or rebased,
        and a dirty, detached, off-branch or mid-operation worktree is left untouched.
        Never raises: a failure is reported in the result (the stage starts anyway).
        """
        try:
            target = self.fetch_base(base_branch)
        except WorktreeError as exc:
            return BaseSync("fetch_failed", base_branch, reason=str(exc)[:300])
        try:
            path = self._owned_worktree(workspace)
            head = self._git(["rev-parse", "HEAD"], path)
            behind = self.commits_behind(path, base_branch)
            ahead = int(self._git(["rev-list", "--count", f"{target}..HEAD"], path) or 0)
            problems: list[str] = []
            current = self._head_branch(path)
            if current != branch:
                problems.append("detached HEAD" if current is None else f"on {current}")
            problems += self._operations_in_progress(path)
            if not self.is_clean(path):
                problems.append("uncommitted changes")
            if problems:
                return BaseSync("not_synced", base_branch, head, head, behind, ", ".join(problems))
            if ahead:
                return BaseSync("own_commits", base_branch, head, head, behind)
            if not behind:
                return BaseSync("up_to_date", base_branch, head, head)
            proc = _run(
                ["merge", "--ff-only", "--no-edit", target],
                path,
                check=False,
                git=self.git,
                env=self.inspection_env,
            )
            moved = self._git(["rev-parse", "HEAD"], path)
            if proc.returncode != 0 or moved != target:
                return BaseSync(
                    "not_synced", base_branch, head, moved, behind, "fast-forward failed"
                )
            return BaseSync("fast_forwarded", base_branch, head, moved, behind)
        except WorktreeError as exc:
            return BaseSync("not_synced", base_branch, reason=str(exc)[:300])

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

    @_clone_write
    def recreate_worktree(
        self, workspace: str | Path, branch: str, base_branch: str = "main"
    ) -> str | None:
        """Re-add a recorded issue worktree that is gone (#821); None when it exists.

        Finished-parcel cleanup removes the worktree (and keeps an unmerged branch); a
        reopened issue's next run binds the recorded path again. Stale registrations are
        pruned first. An existing ``branch`` with no commits of its own is fast-forwarded
        to the last fetched ``origin/<base>``; one with its own commits is checked out as
        it is (never rewritten); a missing branch is created from ``origin/<base>``. The
        branch never tracks the base. Raises :class:`WorktreeError` when the path is not
        a safe owned location or the branch is checked out elsewhere. Callers fetch the
        base first and verify the result with :meth:`verify_worktree`.
        """
        raw = Path(workspace)
        if not raw.is_absolute():
            raise WorktreeError("workspace path is not absolute")
        if raw.exists() or raw.is_symlink():
            return None
        parent = raw.parent
        if not parent.is_dir() or parent.resolve() != parent.absolute():
            raise WorktreeError("workspace parent is missing or traverses a symlink")
        if not _within(raw.absolute(), self.owned_roots):
            raise WorktreeError("workspace is outside the approved owned roots")
        self._git(["worktree", "prune"], self.source_clone)
        other = self.find_branch_worktree(branch)
        if other is not None:
            raise WorktreeError(f"branch {branch} is checked out in another worktree ({other})")
        base = f"refs/remotes/origin/{base_branch}"
        if self.branch_exists(branch):
            ahead = int(
                self._git(
                    ["rev-list", "--count", f"{base}..refs/heads/{branch}"], self.source_clone
                )
                or 0
            )
            if ahead:
                how = f"from {branch} as is ({ahead} commits of its own)"
            else:
                # Nothing of its own: the move to the base is a fast-forward (no loss).
                self._git(["branch", "--force", "--no-track", branch, base], self.source_clone)
                how = f"from {branch} fast-forwarded to origin/{base_branch}"
            self._git(["worktree", "add", str(raw), branch], self.source_clone)
        else:
            self._git(
                ["worktree", "add", "--no-track", "-b", branch, str(raw), base],
                self.source_clone,
            )
            how = f"with new branch {branch} from origin/{base_branch}"
        _run(
            ["branch", "--unset-upstream", branch],
            self.source_clone,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        return how

    def reference_metadata(self, path: Path) -> dict[str, object]:
        """Creation time and permanent poison marker, outside the working tree."""
        try:
            data = json.loads((self.admin_dir(path) / REFERENCE_META).read_text())
            if (
                isinstance(data, dict)
                and type(data.get("created_at")) in (int, float)
                and math.isfinite(data["created_at"])
                and data["created_at"] >= 0
                and isinstance(data.get("oid"), str)
                and re.fullmatch(r"[0-9a-f]{40,64}", data["oid"])
                and isinstance(data.get("poisoned"), bool)
            ):
                return data
        except (OSError, ValueError, OverflowError):
            pass
        raise WorktreeError("reference snapshot has no valid creation record")

    def _write_reference_metadata(self, path: Path, data: dict[str, object]) -> None:
        target = self.admin_dir(path) / REFERENCE_META
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(data))
        os.replace(temporary, target)

    def references(self, root: Path) -> list[Path]:
        return [
            path
            for path, _ in self.worktrees()
            if path.parent == root.resolve()
            and REFERENCE_NAME.fullmatch(path.name)
            and path.is_dir()
        ]

    def reference_clean(self, path: Path) -> bool:
        settings = path / ".claude/settings.json"
        if (path / ".claude").is_symlink() or settings.is_symlink() or not settings.is_file():
            return False
        try:
            if settings.read_text() != REFERENCE_SETTINGS or (path / "node_modules").exists():
                return False
        except (OSError, UnicodeError):
            return False
        # Only the exact factory-written, untracked settings file is exempt.
        if self._git(["ls-files", "--", ".claude/settings.json"], path):
            return False
        return not self._git(
            [
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
                ".",
                ":(exclude).claude/settings.json",
            ],
            path,
        )

    @_clone_write
    def verify_reference(self, workspace: str | Path) -> VerifiedWorktree:
        path = self._owned_worktree(workspace)
        name = REFERENCE_NAME.fullmatch(path.name)
        if name is None:
            raise WorktreeError("workspace is not a reference snapshot")
        oid = self._git(["rev-parse", "HEAD"], path)
        metadata = self.reference_metadata(path)
        if (
            self._head_branch(path) is not None
            or name["oid"] != oid[:12]
            or metadata.get("oid") != oid
            or not self.reference_clean(path)
        ):
            metadata["poisoned"] = True
            self._write_reference_metadata(path, metadata)
        if metadata.get("poisoned"):
            raise WorktreeError("reference snapshot is poisoned")
        self.check_transport(path)
        if self.effective_helpers(path):
            raise WorktreeError("reference snapshot credential helper chain is not empty")
        if self._config_entries(path, r"^factory\.(capabilityfile|stagesession)$"):
            raise WorktreeError("reference snapshot has per-run Git wiring")
        for key in ("credential.helper", "http.extraHeader"):
            entries = self._config_entries(path, "^" + re.escape(key.lower()) + "$")
            if not entries or entries[-1][1] != "":
                raise WorktreeError("reference snapshot lacks reset-only Git wiring")
        guard = self._git(["config", "--get", "core.hooksPath"], path, check=False)
        expected = self.push_guard_dir or (self.common_dir(path) / "factory-reference-hooks")
        if guard != str(expected):
            raise WorktreeError("reference snapshot push guard differs")
        return VerifiedWorktree(path, "", oid)

    @_clone_write
    def select_reference(
        self, root: Path, max_age_hours: float, now: float, base_branch: str = "main"
    ) -> VerifiedWorktree:
        """Fetch and select under the clone lock; never move an existing snapshot."""
        self.ensure_source_clone()
        try:
            oid = self.fetch_base(base_branch)
        except GitLockBusy:
            raise
        except WorktreeError as exc:
            raise ReferenceFetchError(f"source fetch failed: {exc}") from exc
        # Incomplete registrations and poisoned snapshots remain at their pinned
        # paths. They cannot participate in selection or block a new generation.
        usable: list[tuple[float, VerifiedWorktree]] = []
        for candidate in self.references(root):
            try:
                verified = self.verify_reference(candidate)
                created = float(str(self.reference_metadata(candidate)["created_at"]))
                usable.append((created, verified))
            except (WorktreeError, OSError):
                continue
        usable.sort(key=lambda row: row[0], reverse=True)
        if usable and now - usable[0][0] < max_age_hours * 3600:
            return usable[0][1]
        for _, verified in usable:
            if verified.head_oid == oid:
                return verified  # an unchanged base needs no duplicate clean snapshot
        path = root / f"factory-ref-{oid[:12]}"
        generation = 0
        while path.exists() or path.is_symlink():
            generation += 1
            path = root / f"factory-ref-{oid[:12]}-{generation}"
        if root.resolve() not in self.owned_roots or root.is_symlink():
            raise WorktreeError("reference root is not an approved owned root")
        self._git(["worktree", "prune"], self.source_clone)
        self._git(["worktree", "add", "--detach", str(path), oid], self.source_clone)
        settings = path / ".claude/settings.json"
        if (path / ".claude").is_symlink() or settings.exists() or settings.is_symlink():
            raise WorktreeError("reference snapshot already contains Claude settings")
        settings.parent.mkdir(exist_ok=True)
        settings.write_text(REFERENCE_SETTINGS)
        guard = self.push_guard_dir
        if guard is None:
            guard = self.common_dir(path) / "factory-reference-hooks"
            guard.mkdir(exist_ok=True)
            hook = guard / "pre-push"
            if not hook.exists():
                hook.write_text("#!/bin/sh\nexit 1\n")
                hook.chmod(0o700)
        for key, value in (
            ("credential.helper", ""),
            ("http.extraHeader", ""),
            ("credential.useHttpPath", "true"),
            ("credential.interactive", "false"),
            ("core.hooksPath", str(guard)),
        ):
            self._git(["config", "--worktree", "--add", key, value], path)
        # Commit the creation record last. A crash at any earlier step leaves an
        # unusable registration that selection skips and GC can safely collect.
        self._write_reference_metadata(path, {"created_at": now, "oid": oid, "poisoned": False})
        return self.verify_reference(path)

    @_clone_write
    def collect_references(
        self, root: Path, pins: set[Path], now: float, retention_hours: float
    ) -> dict[str, str]:
        """Retain every pin and the newest usable snapshot; never reset or clean."""
        paths = self.references(root)
        usable: dict[Path, float] = {}
        for path in paths:
            try:
                self.verify_reference(path)
                usable[path] = float(str(self.reference_metadata(path)["created_at"]))
            except (WorktreeError, OSError):
                pass  # includes incomplete creation and malformed metadata
        newest = max(usable, key=lambda p: usable[p]) if usable else None
        outcomes: dict[str, str] = {}
        for path in paths:
            if path in pins:
                outcomes[str(path)] = "pinned"
            elif path == newest:
                outcomes[str(path)] = "newest"
            elif path not in usable or now - usable[path] >= retention_hours * 3600:
                try:
                    self.remove_worktree(path, force=True)
                    outcomes[str(path)] = (
                        "removed poisoned" if path not in usable else "removed expired"
                    )
                except (WorktreeError, OSError) as exc:
                    outcomes[str(path)] = f"removal failed: {exc}"
            else:
                outcomes[str(path)] = "retained"
        return outcomes

    def verify_worktree(self, workspace: str | Path, branch: str) -> VerifiedWorktree:
        """Verify ``workspace`` is our clone's worktree on ``branch`` inside owned roots."""
        resolved = self._owned_worktree(workspace)
        if self._head_branch(resolved) != branch:
            raise WorktreeError("workspace is not on the recorded branch")
        oid = self._git(["rev-parse", "HEAD"], resolved)
        return VerifiedWorktree(resolved, branch, oid)

    def _owned_worktree(self, workspace: str | Path) -> Path:
        """``workspace`` resolved, if it is a worktree root of our clone inside owned roots."""
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
        return resolved

    def _head_branch(self, path: Path) -> str | None:
        """The branch HEAD points at, None when detached."""
        head = _run(
            ["symbolic-ref", "--short", "-q", "HEAD"],
            path,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        if head.returncode != 0:
            return None
        return head.stdout.strip() or None

    def restore_branch(self, workspace: str | Path, branch: str) -> str | None:
        """Put a worktree that left its recorded branch back on it (#761).

        A read-only stage once ran ``git checkout origin/main`` in the issue worktree, so
        the next preparation was refused. Switches back only when nothing can be lost:
        tracked and untracked (non-ignored) files clean, no merge/rebase/cherry-pick/
        revert/bisect in progress, the recorded branch exists locally and a detached HEAD
        is contained in some branch or remote-tracking ref. Then ``git switch <branch>``
        (no fetch, no force, nothing else moved). Returns the OID HEAD was at when it
        switched, None when already on ``branch``. Otherwise raises
        :class:`OffBranchError` naming where HEAD is and why it stays there.
        """
        path = self._owned_worktree(workspace)
        current = self._head_branch(path)
        if current == branch:
            return None
        oid = self._git(["rev-parse", "HEAD"], path)
        where = f"detached at {oid[:7]}" if current is None else f"on {current}"
        problems: list[str] = []
        if not self.branch_exists(branch):
            problems.append("branch missing")
        if not self.is_clean(path):
            problems.append("uncommitted changes")
        problems += self._operations_in_progress(path)
        if current is None and not self._git(
            ["for-each-ref", "--count=1", "--contains", oid, "refs/heads", "refs/remotes"], path
        ):
            problems.append("commits on no branch")
        if problems:
            raise OffBranchError(f"worktree off branch {branch} ({where}, {', '.join(problems)})")
        proc = _run(
            ["switch", "--no-guess", branch],
            path,
            check=False,
            git=self.git,
            env=self.inspection_env,
        )
        if proc.returncode != 0 or self._head_branch(path) != branch:
            raise OffBranchError(f"worktree off branch {branch} ({where}, switch failed)")
        return oid

    _OPERATIONS = (
        ("MERGE_HEAD", "merge"),
        ("rebase-merge", "rebase"),
        ("rebase-apply", "rebase"),
        ("CHERRY_PICK_HEAD", "cherry-pick"),
        ("REVERT_HEAD", "revert"),
        ("BISECT_LOG", "bisect"),
    )

    def _operations_in_progress(self, path: Path) -> list[str]:
        args = ["rev-parse", "--path-format=absolute"]
        for name, _ in self._OPERATIONS:
            args += ["--git-path", name]
        found = self._git(args, path).splitlines()
        names = [
            f"{op} in progress"
            for (_, op), marker in zip(self._OPERATIONS, found, strict=True)
            if Path(marker).exists()
        ]
        return list(dict.fromkeys(names))

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
            with CLONE_LOCK:  # the clone's shared exclude file
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

    def admin_dir(self, path: Path) -> Path:
        """The worktree's own Git directory (``.git/worktrees/<name>`` of the clone): its
        files are removed with the worktree."""
        out = self._git(["rev-parse", "--absolute-git-dir"], path)
        return Path(out).resolve()

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

    @_clone_write
    def remove_worktree(self, path: Path, *, force: bool) -> None:
        """Remove one linked worktree of the dedicated clone (never the clone itself)."""
        resolved = path.resolve()
        if resolved == self.source_clone or all(resolved != p for p, _ in self.worktrees()):
            raise WorktreeError("not a linked worktree of the dedicated clone")
        args = ["worktree", "remove", *(["--force"] if force else []), str(resolved)]
        self._git(args, self.source_clone)

    def commits_ahead(self, path: Path, base_branch: str = "main") -> int:
        """Commits on HEAD that the last fetched ``origin/<base>`` lacks (no fetch)."""
        out = self._git(["rev-list", "--count", f"refs/remotes/origin/{base_branch}..HEAD"], path)
        return int(out or 0)

    def last_touched(self, path: Path) -> float:
        """Epoch seconds of the worktree's newest real HEAD activity.

        Reflog *file mtimes* are not activity: fetch's automatic maintenance and explicit
        ``git gc`` may expire/rewrite every linked worktree reflog.  Directory mtimes are
        likewise changed by factory-owned ignored harness files, and ``git status`` may
        refresh an index.  Reflog entry timestamps instead record creation, checkout,
        reset, fast-forward and commit events without being changed by housekeeping.

        Dirty worktrees and commits ahead of the base are independently protected by
        :meth:`WorkspaceCleaner.remove_idle`; this timestamp only decides whether a clean,
        no-ahead worktree is old enough to consider.
        """
        admin = self.admin_dir(path)
        reflog = admin / "logs" / "HEAD"
        try:
            lines = reflog.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError as exc:
            raise WorktreeError(f"worktree HEAD reflog is unreadable: {exc}") from exc
        timestamps: list[int] = []
        for line in lines:
            metadata = line.partition("\t")[0].rsplit(" ", 2)
            if len(metadata) == 3 and metadata[-2].isdigit():
                timestamps.append(int(metadata[-2]))
        if not timestamps:
            raise WorktreeError("worktree HEAD reflog has no activity timestamps")
        return float(max(timestamps))

    @_clone_write
    def maintain_clone(self, *, timeout: float = 1800) -> None:
        """``git worktree prune`` and ``git gc --auto`` on the dedicated clone (under
        :data:`CLONE_LOCK`, so no worktree is created meanwhile)."""
        self._git(["worktree", "prune"], self.source_clone)
        _run(
            ["gc", "--auto", "--quiet"],
            self.source_clone,
            git=self.git,
            env=self.inspection_env,
            timeout=timeout,
        )

    @_clone_write
    def delete_branch(self, branch: str, *, any_branch: bool = False) -> None:
        """Delete a local factory branch of the dedicated clone that no worktree uses
        (``any_branch``: any local branch, for one a removed extra worktree used)."""
        if not any_branch and not branch.startswith("factory/"):
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
