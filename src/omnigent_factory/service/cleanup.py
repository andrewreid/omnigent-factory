"""Finished-parcel cleanup of the factory-owned clone (never the owner's checkouts).

When a parcel is finished (its PR merged or its issue closed: never merely Ready, so the
owner can review the worktree and any dev environment the agent left running), and every
stage session has settled, its factory worktree(s) under the configured
``worktree_root`` are removed and its local ``factory/`` branch deleted from the
dedicated clone. A worktree is removed only when clean, or forcibly when the PR is merged
(the work is on the default branch). The local branch is deleted only when the PR is
merged or its tip is the verified, pushed Ready head. Anything else is skipped and
reported.

Before a worktree is removed (or kept as dirty), the repository's dev-environment
teardown command (``devenv_teardown``) runs in it, if configured. A failed teardown
keeps the worktree and its branch; the cleanup effect is retried (it is a persisted
outbox effect, so a restart keeps it) until ``devenv_teardown_max_attempts`` failures,
counted in the worktree's own Git directory. A marker there records a teardown that
succeeded, so a restart between the teardown and the removal does not run it again;
both files go with the worktree.

The issue's worktrees are its recorded one and every registered worktree under
``worktree_root`` named ``factory-issue-<N>`` or ``factory-issue-<N>-<suffix>`` (extra
worktrees the agent made for parallel subtasks); an extra one's local branch is deleted
with it unless another worktree has it checked out. The housekeeping sweep
(``service.worktree_gc``) runs the same cleanup for closed issues and removes idle
early-stage worktrees through :meth:`WorkspaceCleaner.remove_idle`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    JsonValue,
    RetryableReadFailure,
)
from omnigent_factory.core.types import Hold, Lifecycle, Parcel, Stage
from omnigent_factory.credentials.worktree import CLONE_LOCK, Workspaces, WorktreeError
from omnigent_factory.ports.workspace import WORKSPACE_EFFECT_KINDS
from omnigent_factory.service.directory import ServiceDispatchDirectory

LOG = logging.getLogger(__name__)

_SETTLED = frozenset({Lifecycle.RETIRED, Lifecycle.FENCED})

#: Files in a worktree's own Git directory (removed with the worktree).
TEARDOWN_DONE = "omnigent-factory-devenv-torn-down"
TEARDOWN_FAILURES = "omnigent-factory-devenv-teardown-failures"
#: A failed teardown's cleanup is retried after this long.
TEARDOWN_RETRY_US = 300_000_000
_REASON_CHARS = 200
#: The directory name of an issue's own worktree or one of its extra worktrees.
_ISSUE_NAME = re.compile(r"factory-issue-(\d+)(?:-.+)?")

#: Called under the clone lock right before a removal: None to go ahead, else why not.
Guard = Callable[[], str | None]


def issue_of(name: str) -> int | None:
    """The issue number in a factory worktree directory name (None: not one)."""
    match = _ISSUE_NAME.fullmatch(name)
    return int(match.group(1)) if match else None


class NotSettled(RuntimeError):
    """A session of the parcel is still live; cleanup waits."""


class NotFinished(ValueError):
    """The parcel is not merged, closed or Done (e.g. only Ready): nothing is removed."""


def finished(parcel: Parcel) -> bool:
    """Merged or closed (the terminal barrier), or on the Done column."""
    return Hold.COMPLETED in parcel.holds or parcel.stage == Stage.DONE


class WorkspaceCleaner:
    def __init__(
        self,
        directory: ServiceDispatchDirectory,
        workspaces: Workspaces,
        worktree_root: Path,
        *,
        teardown: Sequence[str] = (),
        teardown_timeout_seconds: float = 600.0,
        base_branch: str = "main",
    ) -> None:
        self.directory = directory
        self.workspaces = workspaces
        self.worktree_root = worktree_root.resolve()
        self.teardown = tuple(teardown)
        self.teardown_timeout_seconds = teardown_timeout_seconds
        self.base_branch = base_branch
        # The effect and the operator's ``cleanup`` never tear one worktree down at once.
        self._lock = threading.Lock()

    async def cleanup(self, parcel_id: str, *, merged: bool) -> dict[str, JsonValue]:
        parcel = await self.directory.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None:
            raise ValueError("unknown parcel")
        if not finished(parcel):
            raise NotFinished("parcel is not finished (merged, closed or Done)")
        if any(s.lifecycle not in _SETTLED for s in parcel.sessions):
            raise NotSettled("a stage session is still live")
        return await asyncio.to_thread(self._locked_cleanup, parcel, merged)

    def _locked_cleanup(self, parcel: Parcel, merged: bool) -> dict[str, JsonValue]:
        with self._lock:
            return self._cleanup(parcel, merged)

    def under_root(self, path: Path) -> bool:
        return self.worktree_root in path.parents

    def _candidates(self, parcel: Parcel) -> tuple[set[Path], set[str]]:
        paths: set[Path] = set()
        branches: set[str] = set()
        if parcel.issue_number is not None:
            branches.add(f"factory/issue-{parcel.issue_number}")
        for session in parcel.sessions:
            data: Any = self.directory.dispatch_snapshot(session.session_id)
            if not isinstance(data, dict) or data.get("workspace_kind") == "reference":
                continue
            if isinstance(data.get("workspace"), str):
                paths.add(Path(data["workspace"]).resolve())
            if isinstance(data.get("branch"), str):
                branches.add(data["branch"])
        return paths, branches

    def _cleanup(self, parcel: Parcel, merged: bool) -> dict[str, JsonValue]:
        paths, branches = self._candidates(parcel)
        ready_head = parcel.readiness.head_sha if parcel.readiness is not None else None
        return self._clean(
            parcel.issue_number,
            paths,
            branches,
            merged=merged,
            ready_head=ready_head,
            label=f"parcel={parcel.parcel_id}",
            reason="done",
        )

    def clean_closed(
        self, issue: int, parcel: Parcel | None, *, merged: bool, guard: Guard | None = None
    ) -> dict[str, JsonValue]:
        """The Done cleanup for a closed issue the sweep found (with or without a record)."""
        with self._lock:
            if parcel is not None:
                paths, branches = self._candidates(parcel)
                ready = parcel.readiness.head_sha if parcel.readiness is not None else None
            else:
                paths, branches, ready = set(), {f"factory/issue-{issue}"}, None
            return self._clean(
                issue,
                paths,
                branches,
                merged=merged,
                ready_head=ready,
                label=f"issue=#{issue}",
                reason="issue closed",
                guard=guard,
            )

    def _clean(
        self,
        issue: int | None,
        paths: set[Path],
        branches: set[str],
        *,
        merged: bool,
        ready_head: str | None,
        label: str,
        reason: str,
        guard: Guard | None = None,
    ) -> dict[str, JsonValue]:
        removed: list[JsonValue] = []
        skipped: list[JsonValue] = []
        deleted: list[JsonValue] = []
        pending: list[JsonValue] = []
        extra_branches: set[str] = set()
        failures = 0
        for path, branch in self.workspaces.worktrees():
            if path.name.startswith("factory-ref-"):
                continue  # reference snapshots belong exclusively to snapshot GC
            named = issue is not None and self.under_root(path) and issue_of(path.name) == issue
            if path not in paths and branch not in branches and not named:
                continue
            if not self.under_root(path):
                skipped.append(f"{path}: outside worktree_root")
                continue
            failed = self._teardown(path)
            if failed is not None:
                count, why = failed
                failures = max(failures, count)
                pending.append(str(path))
                skipped.append(f"{path}: dev environment teardown failed: {why}")
                if branch is not None:
                    branches.discard(branch)  # still checked out; the retry deletes it
                continue
            clean = self.workspaces.is_clean(path)
            if not clean and not merged:
                self._forget_teardown(path)  # kept: a later cleanup tears down again
                skipped.append(f"{path}: uncommitted changes")
                continue
            with CLONE_LOCK:
                why_not = guard() if guard is not None else None
                if why_not is not None:
                    self._forget_teardown(path)
                    skipped.append(f"{path}: {why_not}")
                    continue
                try:
                    self.workspaces.remove_worktree(path, force=merged or not clean)
                except WorktreeError as exc:
                    self._forget_teardown(path)
                    skipped.append(f"{path}: {exc}")
                    continue
            LOG.info("worktree removed path=%s reason=%s %s", path, reason, label)
            removed.append(str(path))
            if branch is not None and branch not in branches:
                extra_branches.add(branch)
        for branch in sorted(branches):
            tip = self.workspaces.branch_tip(branch)
            if tip is None:
                continue
            if not (merged or (ready_head is not None and tip == ready_head)):
                skipped.append(f"{branch}: not merged and not the verified Ready head")
                continue
            if self._delete_branch(branch, reason, label, skipped, any_branch=False):
                deleted.append(branch)
        for branch in sorted(extra_branches - {self.base_branch}):
            if self._delete_branch(branch, f"{reason} (extra worktree)", label, skipped):
                deleted.append(branch)
        LOG.info(
            "parcel cleanup %s merged=%s removed=%s branches=%s skipped=%s",
            label,
            merged,
            removed,
            deleted,
            skipped,
        )
        detail: dict[str, JsonValue] = {
            "removed": removed,
            "branches_deleted": deleted,
            "skipped": skipped,
        }
        if pending:
            detail["teardown_pending"] = pending
            detail["teardown_failures"] = failures
        return detail

    def _delete_branch(
        self,
        branch: str,
        reason: str,
        label: str,
        skipped: list[JsonValue],
        *,
        any_branch: bool = True,
    ) -> bool:
        try:
            self.workspaces.delete_branch(branch, any_branch=any_branch)
        except WorktreeError as exc:
            skipped.append(f"{branch}: {exc}")
            return False
        LOG.info("branch deleted branch=%s reason=%s %s", branch, reason, label)
        return True

    def remove_idle(self, path: Path, branch: str | None, *, guard: Guard | None = None) -> str:
        """Remove an idle early-stage worktree (and its branch) that holds no work of its
        own: clean, and no commits beyond the last fetched base. Returns what happened;
        anything else keeps it (logged). The next stage run recreates it."""
        with self._lock:
            if not self.under_root(path):
                return "kept: outside worktree_root"
            kept = self._own_work(path)
            if kept is not None:
                LOG.info("idle worktree kept path=%s reason=%s", path, kept)
                return f"kept: {kept}"
            failed = self._teardown(path)
            if failed is not None:
                return f"kept: dev environment teardown failed: {failed[1]}"
            with CLONE_LOCK:
                why_not = (guard() if guard is not None else None) or self._own_work(path)
                if why_not is not None:
                    self._forget_teardown(path)
                    LOG.info("idle worktree kept path=%s reason=%s", path, why_not)
                    return f"kept: {why_not}"
                try:
                    self.workspaces.remove_worktree(path, force=False)
                except WorktreeError as exc:
                    self._forget_teardown(path)
                    return f"kept: {exc}"
                LOG.info("worktree removed path=%s reason=idle", path)
                if branch is not None and branch != self.base_branch:
                    skipped: list[JsonValue] = []
                    if not self._delete_branch(branch, "idle", f"path={path}", skipped):
                        return f"removed; branch kept: {skipped[0]}"
            return "removed"

    def remove_stray(self, path: Path, *, reason: str, guard: Guard | None = None) -> str:
        """Remove one factory-named root entry that is not a registered worktree.

        Plain files/directories are factory-owned leftovers by their strict name.  A
        directory carrying Git metadata is removed only when it can still be verified as
        part of the dedicated clone, is clean and has no commits beyond the fetched base.
        Symlinks and special files are always kept.  A configured dev-environment teardown
        must also succeed before a directory is removed.
        """
        with self._lock:
            kept = self._stray_problem(path)
            if kept is not None:
                LOG.info("stray worktree entry kept path=%s reason=%s", path, kept)
                return f"kept: {kept}"
            if path.is_dir() and self.teardown:
                failed = self._run_teardown(path)
                if failed is not None:
                    LOG.warning(
                        "stray dev environment teardown failed path=%s reason=%s", path, failed
                    )
                    return f"kept: dev environment teardown failed: {failed}"
            with CLONE_LOCK:
                why_not = (guard() if guard is not None else None) or self._stray_problem(path)
                if why_not is None and any(
                    path.resolve() == p for p, _ in self.workspaces.worktrees()
                ):
                    why_not = "became a registered worktree"
                if why_not is not None:
                    LOG.info("stray worktree entry kept path=%s reason=%s", path, why_not)
                    return f"kept: {why_not}"
                try:
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                except OSError as exc:
                    return f"kept: removal failed: {str(exc)[:_REASON_CHARS]}"
            LOG.info("stray worktree entry removed path=%s reason=%s", path, reason)
            return "removed"

    def _stray_problem(self, path: Path) -> str | None:
        """Why an unregistered root entry cannot safely be removed (None: it can)."""
        if path.parent.resolve() != self.worktree_root or issue_of(path.name) is None:
            return "outside factory-named worktree_root entries"
        if path.is_symlink():
            return "symlink"
        if not path.exists():
            return "already gone"
        if path.is_file():
            return None
        if not path.is_dir():
            return "special file"
        git_marker = path / ".git"
        if not os.path.lexists(git_marker):
            return None
        try:
            clone_git = (self.workspaces.source_clone / ".git").resolve()
            if self.workspaces.common_dir(path) != clone_git:
                return "Git directory does not belong to the dedicated clone"
            return self._own_work(path)
        except WorktreeError as exc:
            return f"Git state is unreadable: {str(exc)[:_REASON_CHARS]}"

    def _own_work(self, path: Path) -> str | None:
        """Why the worktree holds work of its own (None: it holds none)."""
        try:
            if not self.workspaces.is_clean(path):
                return "uncommitted changes"
            ahead = self.workspaces.commits_ahead(path, self.base_branch)
        except WorktreeError as exc:
            return str(exc)[:_REASON_CHARS]
        if ahead:
            return f"{ahead} commits beyond origin/{self.base_branch}"
        return None

    def _teardown(self, path: Path) -> tuple[int, str] | None:
        """Run the dev-environment teardown in ``path``: None when done (or none is
        configured), else (failures so far, reason). Only worktrees under
        ``worktree_root`` reach here."""
        if not self.teardown:
            return None
        try:
            admin = self.workspaces.admin_dir(path)
        except WorktreeError as exc:
            return 1, str(exc)[:_REASON_CHARS]
        if (admin / TEARDOWN_DONE).exists():
            return None  # torn down before a restart; only the removal was left
        reason = self._run_teardown(path)
        if reason is None:
            (admin / TEARDOWN_DONE).write_text("ok\n", encoding="utf-8")
            (admin / TEARDOWN_FAILURES).unlink(missing_ok=True)
            return None
        counter = admin / TEARDOWN_FAILURES
        try:
            count = int(counter.read_text(encoding="utf-8").strip() or 0) + 1
        except (OSError, ValueError):
            count = 1
        counter.write_text(f"{count}\n", encoding="utf-8")
        LOG.warning(
            "dev environment teardown failed path=%s failures=%s reason=%s", path, count, reason
        )
        return count, reason

    def _forget_teardown(self, path: Path) -> None:
        """The worktree stays: a later cleanup must tear down again (a dev environment may
        have been started in it since)."""
        with contextlib.suppress(OSError, WorktreeError):
            (self.workspaces.admin_dir(path) / TEARDOWN_DONE).unlink(missing_ok=True)

    def _run_teardown(self, path: Path) -> str | None:
        """None on exit 0; otherwise why it failed (exit code or timeout, last stderr line)."""
        argv = list(self.teardown)
        if "/" in argv[0] and not Path(argv[0]).is_absolute():
            argv[0] = str(path / argv[0])  # a repository script, from this worktree
        try:
            proc = subprocess.Popen(  # noqa: S603 - argv from the host config, no shell
                argv,
                cwd=path,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                start_new_session=True,
            )
        except OSError as exc:
            return f"could not start: {exc.strerror or type(exc).__name__}"
        try:
            _, err = proc.communicate(timeout=self.teardown_timeout_seconds)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate()
            return f"timed out after {self.teardown_timeout_seconds:g}s"
        if proc.returncode == 0:
            return None
        lines = [line.strip() for line in (err or "").splitlines() if line.strip()]
        last = f": {lines[-1]}" if lines else ""
        return f"exit {proc.returncode}{last}"[:_REASON_CHARS]


class CleanupAdapter:
    """Executes ``CLEANUP_WORKSPACE``; waits (retries) until the parcel's sessions settle,
    and retries a failed dev-environment teardown up to ``max_teardown_attempts``. It
    never fails the effect: the card's Done state does not depend on it."""

    def __init__(self, cleaner: WorkspaceCleaner, *, max_teardown_attempts: int = 5) -> None:
        self.cleaner = cleaner
        self.max_teardown_attempts = max_teardown_attempts

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return WORKSPACE_EFFECT_KINDS

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        del ctx
        if effect.parcel_id is None:
            return Ack(detail={"skipped": ["no parcel"]})
        try:
            detail = await self.cleaner.cleanup(
                effect.parcel_id, merged=effect.args.get("merged") is True
            )
        except NotSettled:
            return RetryableReadFailure("sessions-not-settled", 60_000_000)
        except NotFinished:
            return Ack(effect.parcel_id, {"skipped": ["parcel not finished"]})
        failures = detail.get("teardown_failures")
        if isinstance(failures, int) and failures:
            if failures < self.max_teardown_attempts:
                return RetryableReadFailure("devenv-teardown-failed", TEARDOWN_RETRY_US)
            LOG.warning(
                "dev environment teardown gave up parcel=%s failures=%s worktrees=%s",
                effect.parcel_id,
                failures,
                detail.get("teardown_pending"),
            )
        return Ack(effect.parcel_id, detail)
