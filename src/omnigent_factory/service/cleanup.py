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
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import subprocess
import threading
from collections.abc import Sequence
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
from omnigent_factory.credentials.worktree import Workspaces, WorktreeError
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
    ) -> None:
        self.directory = directory
        self.workspaces = workspaces
        self.worktree_root = worktree_root.resolve()
        self.teardown = tuple(teardown)
        self.teardown_timeout_seconds = teardown_timeout_seconds
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

    def _candidates(self, parcel: Parcel) -> tuple[set[Path], set[str]]:
        paths: set[Path] = set()
        branches: set[str] = set()
        if parcel.issue_number is not None:
            branches.add(f"factory/issue-{parcel.issue_number}")
        for session in parcel.sessions:
            data: Any = self.directory.dispatch_snapshot(session.session_id)
            if not isinstance(data, dict):
                continue
            if isinstance(data.get("workspace"), str):
                paths.add(Path(data["workspace"]).resolve())
            if isinstance(data.get("branch"), str):
                branches.add(data["branch"])
        return paths, branches

    def _cleanup(self, parcel: Parcel, merged: bool) -> dict[str, JsonValue]:
        paths, branches = self._candidates(parcel)
        removed: list[JsonValue] = []
        skipped: list[JsonValue] = []
        deleted: list[JsonValue] = []
        pending: list[JsonValue] = []
        failures = 0
        for path, branch in self.workspaces.worktrees():
            if path not in paths and branch not in branches:
                continue
            if self.worktree_root not in path.parents:
                skipped.append(f"{path}: outside worktree_root")
                continue
            failed = self._teardown(path)
            if failed is not None:
                count, reason = failed
                failures = max(failures, count)
                pending.append(str(path))
                skipped.append(f"{path}: dev environment teardown failed: {reason}")
                if branch is not None:
                    branches.discard(branch)  # still checked out; the retry deletes it
                continue
            clean = self.workspaces.is_clean(path)
            if not clean and not merged:
                self._forget_teardown(path)  # kept: a later cleanup tears down again
                skipped.append(f"{path}: uncommitted changes")
                continue
            try:
                self.workspaces.remove_worktree(path, force=merged or not clean)
            except WorktreeError as exc:
                self._forget_teardown(path)
                skipped.append(f"{path}: {exc}")
                continue
            removed.append(str(path))
        ready_head = parcel.readiness.head_sha if parcel.readiness is not None else None
        for branch in sorted(branches):
            tip = self.workspaces.branch_tip(branch)
            if tip is None:
                continue
            if not (merged or (ready_head is not None and tip == ready_head)):
                skipped.append(f"{branch}: not merged and not the verified Ready head")
                continue
            try:
                self.workspaces.delete_branch(branch)
            except WorktreeError as exc:
                skipped.append(f"{branch}: {exc}")
                continue
            deleted.append(branch)
        LOG.info(
            "parcel cleanup parcel=%s merged=%s removed=%s branches=%s skipped=%s",
            parcel.parcel_id,
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
