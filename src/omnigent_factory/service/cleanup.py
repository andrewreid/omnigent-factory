"""Finished-parcel cleanup of the factory-owned clone (never the owner's checkouts).

When a parcel's PR is merged or its issue is closed, and every stage session has
settled, its factory worktree(s) under the configured ``worktree_root`` are removed and
its local ``factory/`` branch deleted from the dedicated clone. A worktree is removed
only when clean, or forcibly when the PR is merged (the work is on the default branch).
The local branch is deleted only when the PR is merged or its tip is the verified,
pushed Ready head. Anything else is skipped and reported.
"""

from __future__ import annotations

import asyncio
import logging
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
from omnigent_factory.core.types import Lifecycle, Parcel
from omnigent_factory.credentials.worktree import Workspaces, WorktreeError
from omnigent_factory.ports.workspace import WORKSPACE_EFFECT_KINDS
from omnigent_factory.service.directory import ServiceDispatchDirectory

LOG = logging.getLogger(__name__)

_SETTLED = frozenset({Lifecycle.RETIRED, Lifecycle.FENCED})


class NotSettled(RuntimeError):
    """A session of the parcel is still live; cleanup waits."""


class WorkspaceCleaner:
    def __init__(
        self,
        directory: ServiceDispatchDirectory,
        workspaces: Workspaces,
        worktree_root: Path,
    ) -> None:
        self.directory = directory
        self.workspaces = workspaces
        self.worktree_root = worktree_root.resolve()

    async def cleanup(self, parcel_id: str, *, merged: bool) -> dict[str, JsonValue]:
        parcel = await self.directory.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None:
            raise ValueError("unknown parcel")
        if any(s.lifecycle not in _SETTLED for s in parcel.sessions):
            raise NotSettled("a stage session is still live")
        return await asyncio.to_thread(self._cleanup, parcel, merged)

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
        for path, branch in self.workspaces.worktrees():
            if path not in paths and branch not in branches:
                continue
            if self.worktree_root not in path.parents:
                skipped.append(f"{path}: outside worktree_root")
                continue
            clean = self.workspaces.is_clean(path)
            if not clean and not merged:
                skipped.append(f"{path}: uncommitted changes")
                continue
            try:
                self.workspaces.remove_worktree(path, force=merged or not clean)
            except WorktreeError as exc:
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
        return {"removed": removed, "branches_deleted": deleted, "skipped": skipped}


class CleanupAdapter:
    """Executes ``CLEANUP_WORKSPACE``; waits (retries) until the parcel's sessions settle."""

    def __init__(self, cleaner: WorkspaceCleaner) -> None:
        self.cleaner = cleaner

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
        return Ack(effect.parcel_id, detail)
