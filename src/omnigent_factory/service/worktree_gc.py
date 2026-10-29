"""Worktree housekeeping of the factory-owned clone (never the owner's checkouts).

Every ``worktree_sweep_interval_hours`` (default 24) the sweep lists the clone's
registered worktrees and factory-named unregistered leftovers under ``worktree_root``
and, by the issue number in the name (``factory-issue-<N>`` or
``factory-issue-<N>-<suffix>``):

* an issue GitHub reports closed (one batched read) gets the Done cleanup
  (:meth:`WorkspaceCleaner.clean_closed`: dev-environment teardown, then removal; a dirty
  worktree is force-removed only when the factory recorded the PR as merged);
* an open issue whose card is in Inbox, Triage or Planning, untouched for
  ``idle_worktree_days`` (default 7; 0 never), loses a worktree holding no work of its own
  (:meth:`WorkspaceCleaner.remove_idle`); the next stage run recreates it.
* an eligible issue also loses exact factory-named stray files/directories left directly
  in ``worktree_root``; Git directories retain the same clean/no-ahead protection and a
  configured dev-environment teardown must succeed.

An issue with a live (starting, running, draining) stage session, a pending stage start
or an effect of unknown outcome is never touched, nor a Ready card the factory has not
yet seen finished. Each removal re-checks that under the clone lock, so no stage run can
bind the worktree in between. A name without an issue number is logged, never removed.

Every ``clone_gc_interval_days`` (default 7) ``git worktree prune`` and ``git gc --auto``
run on the clone under the same lock. Fetch's redundant automatic maintenance is
disabled. Both times persist in the clone's Git directory (:data:`STATE_FILE`), so a
restart does not run them again early; the intervals are reload-applied.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import SETTLED_LIFECYCLES, Hold, Parcel, Stage
from omnigent_factory.credentials.worktree import Workspaces, WorktreeError
from omnigent_factory.github.client import GitHubAPIError, GitHubClient
from omnigent_factory.service.cleanup import WorkspaceCleaner, issue_of
from omnigent_factory.service.config import ServiceConfig

if TYPE_CHECKING:
    from omnigent_factory.service.runtime import FactoryService
    from omnigent_factory.store.sqlite import SqliteStore

LOG = logging.getLogger(__name__)

#: Last sweep and gc times, in the clone's own ``.git`` (survives restarts).
STATE_FILE = "omnigent-factory-maintenance.json"
#: Stages whose worktree may be removed when idle (the next stage recreates it).
IDLE_STAGES = frozenset({Stage.INBOX, Stage.TRIAGED, Stage.SCOPED})
#: A removal's re-check (on the event loop) waits at most this long, then skips.
GUARD_TIMEOUT_S = 30.0
#: A failed sweep or gc is retried after this long (or on reload).
RETRY_S = 3600.0
#: Issue aliases per GraphQL read.
STATE_BATCH = 100
_US = 1_000_000

#: issue number -> open (True) / closed (False); a number GitHub did not answer is absent.
IssueStates = Callable[[Sequence[int]], Awaitable[dict[int, bool]]]


def state_path(source_clone: Path) -> Path:
    return source_clone / ".git" / STATE_FILE


def read_state(source_clone: Path) -> dict[str, int]:
    """Persisted ``sweep_at_us`` / ``gc_at_us`` (missing or unreadable: none)."""
    try:
        raw = json.loads(state_path(source_clone).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if isinstance(v, int) and not isinstance(v, bool)}


def _write_state(source_clone: Path, state: dict[str, int]) -> None:
    path = state_path(source_clone)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def iso(us: int | None) -> str | None:
    return datetime.fromtimestamp(us / _US, UTC).isoformat() if us else None


def _entry_last_touched(path: Path) -> float:
    """Newest lstat mtime in an unregistered factory-named root entry.

    Unlike registered worktrees these leftovers have no reliable reflog.  Never follow a
    symlink; :meth:`WorkspaceCleaner.remove_stray` refuses symlinks in all cases.
    """
    try:
        times = [path.lstat().st_mtime]
        if path.is_dir() and not path.is_symlink():
            times.extend(item.lstat().st_mtime for item in path.rglob("*"))
    except OSError as exc:
        raise WorktreeError(f"stray entry activity is unreadable: {exc}") from exc
    return max(times)


async def github_issue_states(
    client: GitHubClient, repository: str, numbers: Sequence[int]
) -> dict[int, bool]:
    """Open/closed of each issue, ``STATE_BATCH`` per GraphQL read. A number GitHub does
    not resolve (deleted, transferred, a PR) is left out, not failed."""
    owner, name = repository.split("/", 1)
    states: dict[int, bool] = {}
    for start in range(0, len(numbers), STATE_BATCH):
        chunk = numbers[start : start + STATE_BATCH]
        fields = " ".join(f"i{n}: issue(number: {int(n)}) {{ state }}" for n in chunk)
        query = (
            f"query($o: String!, $n: String!) {{ repository(owner: $o, name: $n) {{ {fields} }} }}"
        )
        response = await client.request(
            "POST", "/graphql", json_body={"query": query, "variables": {"o": owner, "n": name}}
        )
        body: Any = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        repo = data.get("repository") if isinstance(data, dict) else None
        if not isinstance(repo, dict):
            raise GitHubAPIError("issue state read returned no repository")
        for n in chunk:
            issue = repo.get(f"i{n}")
            state = issue.get("state") if isinstance(issue, dict) else None
            if state in ("OPEN", "CLOSED"):
                states[n] = state == "OPEN"
    return states


@dataclass(slots=True)
class GCState:
    last_sweep_us: int | None = None
    last_gc_us: int | None = None
    last_sweep: dict[str, object] = field(default_factory=dict)
    last_gc_error: str = ""


class WorktreeGC:
    """The periodic housekeeping (see the module docstring). A managed runtime."""

    def __init__(
        self,
        service: FactoryService,
        cleaner: WorkspaceCleaner,
        workspaces: Workspaces,
        issue_states: IssueStates,
    ) -> None:
        self.service = service
        self.cleaner = cleaner
        self.workspaces = workspaces
        self.issue_states = issue_states
        self.state = GCState()
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def config(self) -> ServiceConfig:
        return self.service.config

    @property
    def source_clone(self) -> Path:
        return self.workspaces.source_clone

    def reloaded(self, _config: ServiceConfig) -> None:
        """Config listener: re-plan the next run with the new intervals."""
        self._wake.set()

    async def start(self) -> None:
        persisted = read_state(self.source_clone)
        self.state.last_sweep_us = persisted.get("sweep_at_us")
        self.state.last_gc_us = persisted.get("gc_at_us")
        self._task = asyncio.create_task(self._run(), name="worktree-gc")

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def healthy(self) -> bool:
        return self._task is None or not self._task.done()

    def _due_us(self) -> tuple[int | None, int | None]:
        """When the next sweep / gc is due (None: off)."""
        cfg = self.config
        sweep = gc = None
        if cfg.worktree_sweep_interval_hours > 0:
            sweep = (self.state.last_sweep_us or 0) + int(
                cfg.worktree_sweep_interval_hours * 3600 * _US
            )
        if cfg.clone_gc_interval_days > 0:
            gc = (self.state.last_gc_us or 0) + int(cfg.clone_gc_interval_days * 86400 * _US)
        return sweep, gc

    async def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            now = self.service.clock.now_utc_us()
            failed = False
            for due, job in zip(self._due_us(), (self.sweep_once, self.gc_once), strict=True):
                if due is None or now < due:
                    continue
                try:
                    await job()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    failed = True
                    LOG.warning("worktree housekeeping failed", exc_info=True)
            dues = [d for d in self._due_us() if d is not None]
            wait = RETRY_S
            if dues and not failed:
                wait = max(60.0, (min(dues) - self.service.clock.now_utc_us()) / _US)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wait(), wait)

    async def _wait(self) -> None:
        stop = asyncio.ensure_future(self._stop.wait())
        wake = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({stop, wake}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop.cancel()
            wake.cancel()

    def _persist(self) -> None:
        state = {}
        if self.state.last_sweep_us is not None:
            state["sweep_at_us"] = self.state.last_sweep_us
        if self.state.last_gc_us is not None:
            state["gc_at_us"] = self.state.last_gc_us
        try:
            _write_state(self.source_clone, state)
        except OSError:
            LOG.warning("worktree housekeeping state not saved", exc_info=True)

    # ------------------------------------------------------------------ gc

    async def gc_once(self) -> dict[str, object]:
        try:
            await asyncio.to_thread(self.workspaces.maintain_clone)
            self.state.last_gc_error = ""
            LOG.info("clone maintenance done (prune, gc --auto) clone=%s", self.source_clone)
        except (WorktreeError, OSError) as exc:  # includes a gc timeout
            self.state.last_gc_error = str(exc)[:300]
            LOG.warning("clone maintenance failed: %s", exc)
        self.state.last_gc_us = self.service.clock.now_utc_us()
        self._persist()
        return {"ok": not self.state.last_gc_error, "error": self.state.last_gc_error}

    # ------------------------------------------------------------------ sweep

    async def sweep_once(self) -> dict[str, object]:
        self._loop = asyncio.get_running_loop()
        root = self.cleaner.worktree_root
        rows = await asyncio.to_thread(self.workspaces.worktrees)
        by_issue: dict[int, list[tuple[Path, str | None]]] = defaultdict(list)
        registered = {path for path, _ in rows}
        strays_by_issue: dict[int, list[Path]] = defaultdict(list)
        unnamed: list[str] = []
        for path, branch in rows:
            if root not in path.parents:
                continue
            number = issue_of(path.name)
            if number is None:
                LOG.info("worktree sweep: no issue number in the name, kept path=%s", path)
                unnamed.append(str(path))
                continue
            by_issue[number].append((path, branch))
        try:
            root_entries = tuple(root.iterdir())
        except OSError as exc:
            raise WorktreeError(f"worktree_root is unreadable: {exc}") from exc
        for path in root_entries:
            if path.resolve() in registered:
                continue
            number = issue_of(path.name)
            if number is None:
                LOG.info("worktree sweep: non-factory root entry kept path=%s", path)
                continue
            strays_by_issue[number].append(path)
        issue_numbers = sorted(set(by_issue) | set(strays_by_issue))
        parcels = await self._parcels(issue_numbers)
        unknown = await self._unknown_effect_parcels()
        states: dict[int, bool] = {}
        state_error = ""
        if issue_numbers:
            try:
                states = await self.issue_states(issue_numbers)
            except (GitHubAPIError, OSError, ValueError) as exc:
                # Closed issues wait for the next sweep; idle removal needs no GitHub read.
                state_error = str(exc)[:300]
                LOG.warning("worktree sweep: issue states unreadable: %s", exc)
        closed: dict[str, object] = {}
        idle: dict[str, str] = {}
        strays: dict[str, str] = {}
        skipped: dict[str, str] = {}
        cutoff = self.service.clock.now_utc_us() / _US - self.config.idle_worktree_days * 86400
        for number in issue_numbers:
            worktrees = by_issue.get(number, [])
            issue_strays = strays_by_issue.get(number, [])
            parcel = parcels.get(number)
            why = _blocker(parcel, unknown)
            if why is not None:
                LOG.info("worktree sweep: issue #%s kept: %s", number, why)
                skipped[f"#{number}"] = why
                continue
            if states.get(number) is False:
                if parcel is not None and parcel.stage == Stage.READY and not _finished(parcel):
                    why = "Ready card the factory has not seen finished"
                    LOG.info("worktree sweep: issue #%s kept: %s", number, why)
                    skipped[f"#{number}"] = why
                    continue
                merged = parcel is not None and await self._merged(parcel)
                for path in issue_strays:
                    strays[str(path)] = await asyncio.to_thread(
                        self.cleaner.remove_stray,
                        path,
                        reason="issue closed",
                        guard=self._guard(number, idle=False),
                    )
                closed[f"#{number}"] = await asyncio.to_thread(
                    self.cleaner.clean_closed,
                    number,
                    parcel,
                    merged=merged,
                    guard=self._guard(number, idle=False),
                )
                continue
            if self.config.idle_worktree_days <= 0:
                continue
            if parcel is None or parcel.stage not in IDLE_STAGES or _finished(parcel):
                continue
            for path, branch in worktrees:
                outcome = await asyncio.to_thread(self._idle_one, number, path, branch, cutoff)
                if outcome is not None:
                    idle[str(path)] = outcome
            for path in issue_strays:
                outcome = await asyncio.to_thread(self._idle_stray, number, path, cutoff)
                if outcome is not None:
                    strays[str(path)] = outcome
        self.state.last_sweep_us = self.service.clock.now_utc_us()
        self.state.last_sweep = {
            "worktrees": sum(len(w) for w in by_issue.values()) + len(unnamed),
            "stray_entries": sum(len(paths) for paths in strays_by_issue.values()),
            "closed": closed,
            "idle": idle,
            "strays": strays,
            "skipped": skipped,
            "unnamed": unnamed,
            "issue_state_error": state_error,
        }
        self._persist()
        LOG.info(
            "worktree sweep done closed=%s idle=%s strays=%s skipped=%s unnamed=%s",
            len(closed),
            sum(v == "removed" or v.startswith("removed;") for v in idle.values()),
            sum(v == "removed" for v in strays.values()),
            len(skipped),
            len(unnamed),
        )
        return self.state.last_sweep

    def _idle_one(self, number: int, path: Path, branch: str | None, cutoff_s: float) -> str | None:
        try:
            touched = self.workspaces.last_touched(path)
        except WorktreeError as exc:
            return f"kept: {exc}"
        if touched > cutoff_s:
            return None  # touched recently: nothing to report
        return self.cleaner.remove_idle(path, branch, guard=self._guard(number, idle=True))

    def _idle_stray(self, number: int, path: Path, cutoff_s: float) -> str | None:
        try:
            touched = _entry_last_touched(path)
        except WorktreeError as exc:
            return f"kept: {exc}"
        if touched > cutoff_s:
            return None
        return self.cleaner.remove_stray(path, reason="idle", guard=self._guard(number, idle=True))

    def _guard(self, number: int, *, idle: bool) -> Callable[[], str | None]:
        """Re-checks the issue's record on the event loop (from a worker thread holding the
        clone lock) right before a removal."""
        loop = self._loop

        def guard() -> str | None:
            if loop is None:
                return "no event loop"
            future = asyncio.run_coroutine_threadsafe(self._recheck(number, idle=idle), loop)
            try:
                return future.result(timeout=GUARD_TIMEOUT_S)
            except TimeoutError:
                future.cancel()
                return "re-check timed out"

        return guard

    async def _recheck(self, number: int, *, idle: bool) -> str | None:
        parcel = (await self._parcels([number])).get(number)
        why = _blocker(parcel, await self._unknown_effect_parcels())
        if why is not None or not idle:
            return why
        if parcel is None or parcel.stage not in IDLE_STAGES or _finished(parcel):
            return "the card left Inbox/Triage/Planning"
        return None

    # ------------------------------------------------------------------ records

    async def _parcels(self, numbers: Sequence[int]) -> dict[int, Parcel]:
        if not numbers:
            return {}
        repo_id = self.config.repo_id
        wanted = set(numbers)

        def load(store: SqliteStore) -> dict[int, Parcel]:
            rows = store.query(
                "SELECT issue_number, parcel_id FROM parcels WHERE repo_id = ? AND "
                "issue_number IS NOT NULL",
                (repo_id,),
            )
            found: dict[int, Parcel] = {}
            for row in rows:
                if row[0] not in wanted:
                    continue
                parcel = store.load_parcel(row[1])
                if parcel is not None and parcel.issue_number is not None:
                    found[parcel.issue_number] = parcel
            return found

        result: dict[int, Parcel] = await self.service.db.call(load)
        return result

    async def _unknown_effect_parcels(self) -> frozenset[str]:
        effects = await self.service.db.call(lambda store: store.effects_in_state("unknown"))
        return frozenset(e.effect.parcel_id for e in effects if e.effect.parcel_id is not None)

    async def _merged(self, parcel: Parcel) -> bool:
        """The factory recorded the parcel's PR as merged (its merged cleanup effect)."""
        key = f"cleanup:{parcel.parcel_id}:merged"
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT 1 FROM effects WHERE kind = ? AND dedupe_key = ?",
                (EffectKind.CLEANUP_WORKSPACE.value, key),
            )
        )
        return bool(rows)

    # ------------------------------------------------------------------ reporting

    async def status(self) -> dict[str, object]:
        try:
            rows = await asyncio.to_thread(self.workspaces.worktrees)
            root = self.cleaner.worktree_root
            count: int | None = sum(root in path.parents for path, _ in rows)
        except WorktreeError:
            count = None
        cfg = self.config
        return {
            "worktrees": count,
            "sweep_interval_hours": cfg.worktree_sweep_interval_hours,
            "idle_worktree_days": cfg.idle_worktree_days,
            "gc_interval_days": cfg.clone_gc_interval_days,
            "last_sweep": iso(self.state.last_sweep_us),
            "last_gc": iso(self.state.last_gc_us),
            "last_gc_error": self.state.last_gc_error,
            "last_sweep_result": self.state.last_sweep,
        }


def _finished(parcel: Parcel) -> bool:
    return Hold.COMPLETED in parcel.holds or parcel.stage == Stage.DONE


def _blocker(parcel: Parcel | None, unknown: frozenset[str]) -> str | None:
    """Why the sweep must not touch this issue's worktrees (None: it may)."""
    if parcel is None:
        return None
    if any(s.lifecycle not in SETTLED_LIFECYCLES for s in parcel.sessions):
        return "a stage session is live (starting, running or draining)"
    if parcel.pending_authorization_id is not None:
        return "a stage run is about to start"
    if parcel.parcel_id in unknown:
        return "an effect has an unknown outcome"
    return None


__all__ = [
    "IDLE_STAGES",
    "STATE_FILE",
    "WorktreeGC",
    "github_issue_states",
    "read_state",
    "state_path",
]
