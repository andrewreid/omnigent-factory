"""Polling Omnigent observer and conservative active-time sampler.

Stage results are not read from the transcript: they arrive through the factory MCP tools
(:mod:`omnigent_factory.service.mcp`).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.predicates import run_closed, settled
from omnigent_factory.core.types import (
    BotState,
    DecisionSource,
    DecisionStatus,
    Hold,
    Lifecycle,
    Parcel,
    StageSession,
)
from omnigent_factory.omnigent.activity import ActivityTracker
from omnigent_factory.omnigent.adapter import OmnigentExecutionAdapter
from omnigent_factory.omnigent.feed import SSE, TIMER, WS, ChangeFeed, DirtyMark
from omnigent_factory.omnigent.observe import StreamNormalizer
from omnigent_factory.omnigent.rest import OmnigentReadError
from omnigent_factory.omnigent.tree import TreeObservation, epoch_us
from omnigent_factory.ports.clock import Clock
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import InvariantViolation

LOG = logging.getLogger(__name__)

#: Lifecycles a run may be parked in (:meth:`OmnigentObserver._parked`).
_PARKABLE_LIFECYCLES = frozenset({Lifecycle.ACTIVE, Lifecycle.WAITING, Lifecycle.FENCED})

#: Active time is persisted rounded *up* to this step (never past the grant's last
#: microsecond until it is really spent): one event per step instead of one per pass,
#: and a crash between steps can only over-count (see :func:`persisted_active_us`).
ACTIVE_TIME_STEP_US = 5 * 60 * 1_000_000
#: A changed cost total is persisted at most this often per run (audit only: the cost
#: backstop is the Omnigent policy, not these samples).
COST_SAMPLE_INTERVAL_US = 5 * 60 * 1_000_000
#: Between full passes, each tree on the active cadence has its root snapshot read this
#: often for new prompts (Omnigent mirrors every descendant's pending prompt into it):
#: one request per active tree, so a slow pass never hides a prompt for minutes.
PROMPT_PROBE_INTERVAL_SECONDS = 15.0
#: With a change feed, a tree is reconciled with a full scan at least this often (see
#: ``ServiceConfig.observer_reconcile_*_seconds`` for the defaults' rationale).
RECONCILE_ACTIVE_SECONDS = 600.0
RECONCILE_SETTLED_SECONDS = 900.0
RECONCILE_PARKED_SECONDS = 1800.0
#: After a feed event wakes the observer it waits this long, so a burst of events (a
#: turn ending in several nodes) costs one targeted pass.
FEED_DEBOUNCE_SECONDS = 0.25
#: A reading is never treated as stale before this (the health sweep's floor).
_MIN_FRESH_US = 600 * 1_000_000


def persisted_active_us(
    actual_us: int, duration_us: int, step_us: int = ACTIVE_TIME_STEP_US
) -> int:
    """The active time to store for an upper-bound estimate ``actual_us``.

    Rounded up to the next ``step_us`` so the stored value always covers the estimate
    (a crash loses nothing: the next tracker starts from it), but capped one microsecond
    short of the grant until the estimate reaches it, so the limit (``remaining_us ==
    0``) is reached on exactly the pass it was before, never early.
    """
    rounded = -(-actual_us // step_us) * step_us if actual_us > 0 else 0
    if actual_us >= duration_us:
        return rounded  # spent: >= duration, so the limit is reached on this pass
    return min(rounded, duration_us - 1)


@dataclass(frozen=True, slots=True)
class TreeReading:
    """The latest tree scan of a run, kept for the health sweep (in memory only)."""

    observation: TreeObservation
    #: When it was read (UTC us).
    read_at_us: int
    #: When this process first saw the tree as it is now (node ids, statuses, items,
    #: ``updated_at``): the fallback activity time when the server sends no
    #: ``updated_at`` (a restart starts it afresh: later, never earlier).
    unchanged_since_us: int
    #: How long this reading stays usable when nothing changes (0: the sweep's default).
    #: A tree on a change feed is only re-read on a change or at its reconcile, so its
    #: reading is current for up to twice that interval.
    fresh_for_us: int = 0

    @property
    def last_activity_us(self) -> int:
        return self.observation.last_activity_us or self.unchanged_since_us


def _fingerprint(observation: TreeObservation) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (n.node_id, n.status, n.task_status, n.items_hint, n.updated_at_us, n.busy)
        for _nid, n in sorted(observation.nodes.items())
    )


@dataclass(slots=True)
class _FeedTree:
    """The cached tree of a run on a change feed (in memory only)."""

    root_id: str
    observation: TreeObservation
    #: The feed coverage ``(channel, epoch)`` the last full scan started under: a change
    #: (a (re)connect, a channel switch) means events may have been missed.
    key: tuple[str, int]
    #: When the last full scan started (monotonic us).
    full_at_us: int
    #: When each node was last read (the reading pass's start, UTC us and monotonic us).
    read_at_us: dict[str, int] = field(default_factory=dict)
    read_mono_us: dict[str, int] = field(default_factory=dict)
    #: When its topology (root, child list, inventory) was last read (monotonic us): a
    #: tree only the WebSocket watches is told about no spawn, so it is re-read on the
    #: tree's old cadence.
    topology_at_us: int = 0


class OmnigentObserver:
    """Feed tree, prompt, result, crash, cost and time observations through the service.

    Without a :class:`~omnigent_factory.omnigent.feed.ChangeFeed` every due tree is fully
    re-scanned on the timer cadence (5 s active, settled 60 s, parked 300 s). With one,
    a tree the feed covers (``sse`` or ``ws``) is fully scanned only after a (re)connect
    or channel switch and at its slow reconcile interval; in between only nodes the feed
    marked dirty are re-read (lite), and a tree whose last emitted state was busy is
    declared idle only after a confirming pass. A tree the feed does not cover (``timer``)
    keeps the old cadence, so a dead feed degrades to polling, never to blindness.
    """

    def __init__(
        self,
        service: FactoryService,
        adapter: OmnigentExecutionAdapter,
        directory: ServiceDispatchDirectory,
        clock: Clock,
        *,
        interval_seconds: float,
        settled_interval_seconds: float | None = None,
        parked_interval_seconds: float = 300.0,
        prompt_probe_interval_seconds: float = PROMPT_PROBE_INTERVAL_SECONDS,
        feed: ChangeFeed | None = None,
        reconcile_active_seconds: float = RECONCILE_ACTIVE_SECONDS,
        reconcile_settled_seconds: float = RECONCILE_SETTLED_SECONDS,
        reconcile_parked_seconds: float = RECONCILE_PARKED_SECONDS,
        feed_debounce_seconds: float = FEED_DEBOUNCE_SECONDS,
    ) -> None:
        self.service = service
        self.adapter = adapter
        self.directory = directory
        self.clock = clock
        self.interval_seconds = interval_seconds
        #: A settled tree (observed quiescent, nothing of ours running) is only watched for
        #: external activity, so it is read at this slower cadence; any other is every pass.
        self.settled_interval_seconds = settled_interval_seconds or interval_seconds
        #: A parked run, or one waiting on the owner, CI or the review bot with its tree
        #: last seen idle (:meth:`_parked`), is read at this cadence (hot-reloadable).
        self.parked_interval_seconds = parked_interval_seconds
        self.prompt_probe_interval_seconds = prompt_probe_interval_seconds
        self._probe_task: asyncio.Task[None] | None = None
        #: When the current full pass started reading (UTC us; None: no pass yet).
        self._pass_started_us: int | None = None
        self._settled_read_at: dict[str, int] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._trackers: dict[str, ActivityTracker] = {}
        self._normalizers: dict[str, StreamNormalizer] = {}
        self._last_runtime: dict[str, bool] = {}
        self._last_tree: dict[str, tuple[bool, bool, bool]] = {}
        self._last_activity: dict[str, int] = {}
        self._last_cost: dict[str, int | None] = {}
        self._last_cost_at: dict[str, int] = {}
        self._crashed: set[str] = set()
        self._readings: dict[str, TreeReading] = {}
        self._fingerprints: dict[str, tuple[tuple[tuple[object, ...], ...], int]] = {}
        #: The change feed (None: timer cadence only, the pre-feed behaviour).
        self.feed = feed
        self.reconcile_active_seconds = reconcile_active_seconds
        self.reconcile_settled_seconds = reconcile_settled_seconds
        self.reconcile_parked_seconds = reconcile_parked_seconds
        self.feed_debounce_seconds = feed_debounce_seconds
        self._feed_trees: dict[str, _FeedTree] = {}
        #: Each run's last known owner node of every prompt it saw (for "gone" checks).
        self._prompt_owners: dict[str, dict[str, str]] = {}
        #: Full scans, targeted passes and confirming passes, by reason (``status``).
        self.passes: Counter[str] = Counter()
        self._mode: str | None = None
        self._coverage: dict[str, str] = {}

    def reading(self, session_id: str) -> TreeReading | None:
        """The latest tree scan of run ``session_id`` this process made (None: none)."""
        return self._readings.get(session_id)

    def _remember(
        self, session_id: str, observation: TreeObservation, now: int, fresh_for_us: int = 0
    ) -> None:
        fingerprint = _fingerprint(observation)
        previous = self._fingerprints.get(session_id)
        since = previous[1] if previous is not None and previous[0] == fingerprint else now
        self._fingerprints[session_id] = (fingerprint, since)
        self._readings[session_id] = TreeReading(observation, now, since, fresh_for_us)

    def status(self) -> dict[str, object]:
        """The observer's feed mode, per-tree coverage and request counters (``status``)."""
        coverage = Counter(self._coverage.values())
        rest = getattr(self.adapter, "rest", None)
        requests = getattr(rest, "requests", None)
        return {
            "mode": self._mode or (TIMER if self.feed is None else "idle"),
            "trees": {mode: coverage.get(mode, 0) for mode in (SSE, WS, TIMER)},
            "feed": self.feed.status() if self.feed is not None else None,
            "passes": dict(sorted(self.passes.items())),
            "omnigent_requests": dict(sorted(requests.items())) if requests else {},
            "reconcile_seconds": {
                "active": self.reconcile_active_seconds,
                "settled": self.reconcile_settled_seconds,
                "parked": self.reconcile_parked_seconds,
            },
        }

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="omnigent-observer")
        self._probe_task = asyncio.create_task(self._probe(), name="omnigent-prompt-probe")

    async def close(self) -> None:
        self._stop.set()
        if self.feed is not None:
            await self.feed.close()
        was_running = self._task is not None
        for task in (self._task, self._probe_task):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self._task = self._probe_task = None
        if not was_running:
            return
        for parcel in await self._parcels():
            session = parcel.current_session
            tracker = self._trackers.get(session.session_id) if session is not None else None
            if tracker is not None:
                await self._activity(parcel, tracker)  # the stored value covers the estimate

    def healthy(self) -> bool:
        return self._task is None or not self._task.done()

    async def _probe(self) -> None:
        """Best effort: a failed probe waits for the next one (the full pass is the truth)."""
        while not self._stop.is_set():
            try:
                await self.probe_prompts_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.debug("prompt probe failed", exc_info=True)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self.prompt_probe_interval_seconds)

    async def probe_prompts_once(self) -> frozenset[str]:
        """Read the root snapshot of every tree on the active cadence (or holding a
        permission prompt inside its grace) for open prompts.

        A full pass scans every due tree, node by node, before it applies anything, so
        one slow tree (a 15 s read timeout per stale node) delays every prompt behind it.
        The probe opens new prompts and posts due permission prompts in between; only a
        full scan may decide that a prompt is gone. Returns the runs it read.
        """
        probed: set[str] = set()
        for parcel in await self._parcels():
            session = parcel.current_session
            if session is None or session.root_id is None or run_closed(parcel, session):
                continue
            deferred = any(
                d.comment_deferred and d.session_id == session.session_id
                for d in parcel.open_decisions
            )
            if not deferred and (self._parked(parcel, session) or settled(session)):
                continue  # a deferred permission prompt is watched for its grace
            if not deferred and self._feed_covers(session.root_id):
                continue  # prompt edges reach the change feed; its passes open them
            try:
                prompts = await self.adapter.pending_prompts(session.root_id)
            except (OmnigentReadError, OSError, ValueError):
                continue
            fresh = await self._load_parcel(parcel.parcel_id)
            current = fresh.current_session if fresh is not None else None
            if (
                fresh is None
                or current is None
                or current.session_id != session.session_id
                or current.root_id != session.root_id
                or current.execution_closed
            ):
                continue
            owners = self._prompt_owners.setdefault(session.session_id, {})
            owners.update({eid: owner for eid, (owner, _raw) in prompts.items()})
            await self._prompts(fresh, prompts, complete=False)
            probed.add(session.session_id)
        return frozenset(probed)

    def _feed_covers(self, root_id: str) -> bool:
        return self.feed is not None and self.feed.coverage(root_id)[0] != TIMER

    async def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                await self.observe_once()
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                failures += 1
                if failures >= self.service.config.background_failure_limit:
                    self.service.managed_task_failed("observer")
                    return
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._stop.wait(),
                        self.service.config.background_error_backoff_seconds * failures,
                    )
                continue
            await self._idle_wait()

    async def _idle_wait(self) -> None:
        """Until the next pass: the timer interval, or (with a feed) the first feed event
        or (re)connect, then a short debounce so a burst costs one pass."""
        if self.feed is None:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self.interval_seconds)
            return
        wake = self.feed.wake
        stop = asyncio.ensure_future(self._stop.wait())
        woke = asyncio.ensure_future(wake.wait())
        try:
            done, _ = await asyncio.wait(
                {stop, woke}, timeout=self.interval_seconds, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for task in (stop, woke):
                task.cancel()
        if woke in done and not self._stop.is_set():
            wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self.feed_debounce_seconds)

    async def observe_once(self) -> frozenset[str]:
        """Take one complete pass; successful roots are safe for boot re-enable checks.

        Every tree due for a full scan in the pass is scanned with one shared inventory
        refresh (``observe_trees``); a failed refresh makes each scan incomplete, never
        idle. With a change feed, trees it covers are scanned only when reconciling, and
        otherwise re-read node by node where it marked them dirty (:meth:`_refresh`).
        """
        due: list[tuple[Parcel, StageSession, str]] = []
        keys: dict[str, tuple[str, int]] = {}
        targeted: list[
            tuple[Parcel, StageSession, _FeedTree, dict[str, DirtyMark], bool, bool]
        ] = []
        parcels = await self._parcels()
        open_runs = {p.current_session_id for p in parcels}
        for stale in set(self._readings) - open_runs:  # closed runs: nothing watches them
            self._readings.pop(stale, None)
            self._fingerprints.pop(stale, None)
        for stale in set(self._feed_trees) - open_runs:
            self._feed_trees.pop(stale, None)
            self._prompt_owners.pop(stale, None)
        watched: dict[str, Mapping[str, str | None] | None] = {}
        coverage: dict[str, str] = {}
        for parcel in parcels:
            session = parcel.current_session
            if session is None or session.root_id is None or run_closed(parcel, session):
                continue
            root = session.root_id
            parked = self._parked(parcel, session)
            if parked:
                interval: float | None = max(
                    self.parked_interval_seconds, self.settled_interval_seconds
                )
            elif settled(session):
                interval = self.settled_interval_seconds
            else:
                interval = None
            if self.feed is None:
                if self._due(session.session_id, interval):
                    due.append((parcel, session, root))
                continue
            cached = self._feed_trees.get(session.session_id)
            if cached is not None and cached.root_id != root:
                cached = self._feed_trees.pop(session.session_id)  # a new root: start over
                cached = None
            watched[root] = (
                {nid: n.parent_id for nid, n in cached.observation.nodes.items()}
                if cached is not None
                else None
            )
            key = self.feed.coverage(root)
            coverage[session.session_id] = key[0]
            reason = self._full_reason(session, cached, key, interval, parked)
            resume = reason == "resume"
            if resume:
                reason = None
            if reason is not None:
                if reason != "timer-skip":
                    due.append((parcel, session, root))
                    keys[session.session_id] = key
                    self.passes[f"full:{reason}"] += 1
                continue
            assert cached is not None  # noqa: S101 - no full scan means a cached tree
            marks = self.feed.take_dirty(root)
            # Progress alone (a pushed row whose ``updated_at`` moved, nothing else) is
            # never read: the row's time is the node's activity (stall detection).
            progress = {
                n: m
                for n, m in marks.items()
                if not m.edge and not m.spawn and n in cached.observation.nodes
            }
            if progress:
                self._progress(parcel, session, cached, progress)
                marks = {n: m for n, m in marks.items() if n not in progress}
            # Only a root stream announces spawns; on the WebSocket the tree's topology is
            # unknown until read, so it is polled on the tree's old cadence.
            spacing = interval if interval is not None else self.interval_seconds
            topology = (
                key[0] != SSE and self.clock.monotonic_us() - cached.topology_at_us >= spacing * 1e6
            )
            if marks or topology or resume:
                targeted.append((parcel, session, cached, marks, topology, resume))
        if self.feed is not None:
            # Every tree keeps its root stream where the server has them (an idle stream
            # costs the server nothing); the feed watches the rest on the WebSocket.
            self.feed.set_targets(set(watched), watched)
            self._note_mode(coverage)
        observed: set[str] = set()
        if due:
            observed |= await self._full_pass(due, keys)
        inventory_error: str | None = None
        if any(t[4] for t in targeted):
            inventory_error = await self.adapter.refresh_inventory()  # one per pass
        for parcel, session, cached, marks, topology, resume in targeted:
            if resume:
                cached.key = self.feed.coverage(cached.root_id) if self.feed else cached.key
            if await self._refresh(
                parcel,
                session,
                cached,
                marks,
                topology=topology,
                inventory_error=inventory_error,
                resume=resume,
            ):
                observed.add(session.session_id)
        return frozenset(observed)

    def _full_reason(
        self,
        session: StageSession,
        cached: _FeedTree | None,
        key: tuple[str, int],
        interval: float | None,
        parked: bool,
    ) -> str | None:
        """Why this feed-mode tree needs a full scan now (``"timer-skip"``: it is on timer
        coverage and not due; None: no full scan, only dirty nodes are re-read)."""
        # Feed events wake passes often; a polled tree keeps at least the old spacing.
        spacing = interval if interval is not None else self.interval_seconds
        if (
            cached is not None
            and cached.observation.complete
            and cached.key[0] == SSE
            and cached.key != key
            and self.feed is not None
        ):
            if key[0] == SSE:
                # The same root stream reconnected (the live proxy ends every stream after
                # ~120 s): its connect snapshot re-lists the children; a resume re-reads
                # the rest of what the gap made unknown, not the whole tree.
                return "resume"
            if self.feed.resuming(cached.root_id):
                return "timer-skip"  # moments into a reconnect: wait for the resume
        if key[0] == TIMER:
            return "timer" if self._due(session.session_id, spacing) else "timer-skip"
        if cached is None:
            return "initial"
        if cached.key != key:
            return "reconnect"
        if not cached.observation.complete:
            # A failed read: retried by a full scan at the old cadence (unknown meanwhile).
            return "retry" if self._due(session.session_id, spacing) else "timer-skip"
        if parked:
            every = self.reconcile_parked_seconds
        elif settled(session):
            every = self.reconcile_settled_seconds
        else:
            every = self.reconcile_active_seconds
        if self.clock.monotonic_us() - cached.full_at_us >= every * 1e6:
            return "safety"
        return None

    def _reconcile_us(self, session: StageSession, parcel: Parcel) -> int:
        if self._parked(parcel, session):
            every = self.reconcile_parked_seconds
        elif settled(session):
            every = self.reconcile_settled_seconds
        else:
            every = self.reconcile_active_seconds
        return int(every * 1_000_000)

    def _progress(
        self,
        parcel: Parcel,
        session: StageSession,
        cached: _FeedTree,
        marks: Mapping[str, DirtyMark],
    ) -> None:
        """Advance the cached nodes' activity time to their pushed rows' ``updated_at``
        (no read: busy, idle and prompts change only on edges, which are read). The
        reading the health sweep judges stalls by moves with it."""
        nodes = dict(cached.observation.nodes)
        moved = False
        for node_id, mark in marks.items():
            node = nodes.get(node_id)
            updated = epoch_us(mark.updated_at)
            if node is not None and updated > node.updated_at_us:
                nodes[node_id] = replace(node, updated_at_us=updated)
                moved = True
        if not moved:
            return
        self.passes["progress"] += 1
        cached.observation = replace(cached.observation, nodes=nodes)
        if self._readings.get(session.session_id) is not None:
            self._remember(
                session.session_id,
                cached.observation,
                self.clock.now_utc_us(),
                self._fresh_for(parcel, session),
            )

    def _fresh_for(self, parcel: Parcel, session: StageSession) -> int:
        """How long a reading of this run stays current with no change (0: the sweep's
        default). Only a root stream pushes both state edges and spawns: such a tree may
        go unread until its reconcile. A WebSocket tree is re-read on its topology cadence
        (at most the old one), so the sweep's own gate applies to it."""
        if (
            self.feed is not None
            and session.root_id is not None
            and self.feed.coverage(session.root_id)[0] == SSE
        ):
            return max(_MIN_FRESH_US, 2 * self._reconcile_us(session, parcel))
        return 0

    def _note_mode(self, coverage: Mapping[str, str]) -> None:
        """The feed mode is the weakest coverage of any tree (logged when it changes)."""
        self._coverage = dict(coverage)
        modes = set(coverage.values())
        mode = TIMER if TIMER in modes else WS if WS in modes else SSE if modes else "idle"
        if mode != self._mode:
            counts = Counter(coverage.values())
            LOG.info(
                "observer feed mode %s -> %s (trees: sse=%d ws=%d timer=%d)",
                self._mode or "start",
                mode,
                counts.get(SSE, 0),
                counts.get(WS, 0),
                counts.get(TIMER, 0),
            )
            self._mode = mode

    async def _full_pass(
        self, due: list[tuple[Parcel, StageSession, str]], keys: Mapping[str, tuple[str, int]]
    ) -> set[str]:
        started_utc = self._pass_started_us = self.clock.now_utc_us()
        started_mono = self.clock.monotonic_us()
        results = await self.adapter.observe_trees([root for _, _, root in due])
        observed: set[str] = set()
        for (parcel, session, root), result in zip(due, results, strict=True):
            try:
                try:
                    if not isinstance(result, TreeObservation):
                        raise result
                    if self.feed is not None:
                        key = keys.get(session.session_id, (TIMER, 0))
                        self._feed_trees[session.session_id] = _FeedTree(
                            root,
                            result,
                            key,
                            started_mono,
                            dict.fromkeys(result.nodes, started_utc),
                            dict.fromkeys(result.nodes, started_mono),
                            started_mono,
                        )
                        self.feed.clear_before(root, started_mono)
                    await self._observe(parcel, result)
                except (OmnigentReadError, OSError, ValueError):
                    tracker = self._tracker(
                        session.session_id, session.grant.grant_id, session.grant.consumed_us
                    )
                    tracker.gap_started()
                    await self._activity(parcel, tracker)
                else:
                    observed.add(session.session_id)
            except InvariantViolation:
                await self.service.hold_observation_failure(parcel.parcel_id)
        return observed

    async def _refresh(
        self,
        parcel: Parcel,
        session: StageSession,
        cached: _FeedTree,
        marks: Mapping[str, DirtyMark],
        *,
        topology: bool = False,
        inventory_error: str | None = None,
        resume: bool = False,
    ) -> bool:
        """Re-read what is not proven fresh in a cached tree; confirm before idle.

        Lite reads of the marked nodes (children listed for spawns and unknown nodes);
        ``topology`` (a tree no root stream covers, on its old cadence) also re-reads the
        root, its child list and the shared inventory refreshed for this pass, so a
        spawned child is found. If the result is complete and idle but the last emitted
        state was not, a confirming pass re-reads every previously busy node and the
        root, lists their children and their parents' (task statuses) and refreshes the
        inventory before idle is reported. Any failed read leaves the tree incomplete
        (unknown, never idle) until a full scan.
        """
        started_utc = self._pass_started_us = self.clock.now_utc_us()
        started_mono = self.clock.monotonic_us()
        dirty = sorted(
            n for n, m in marks.items() if m.marked_at_us >= cached.read_mono_us.get(n, -1)
        )
        spawned = sorted(n for n, m in marks.items() if m.spawn)
        if resume and self.feed is not None:
            # What the gap made unknown: the root (its own status and the prompts it
            # mirrors), and the children of every inner node; the root's own children
            # only when the connect snapshot did not re-list them.
            self.passes["resume"] += 1
            root = cached.root_id
            inner = {n.parent_id for n in cached.observation.nodes.values() if n.parent_id}
            if self.feed.snapshot_complete(root):
                inner.discard(root)
            else:
                inner.add(root)
            dirty = sorted({*dirty, root})
            spawned = sorted({*spawned, *inner})
        if not dirty and not spawned and not topology:
            return True  # every mark predates its node's latest read
        self.passes["topology" if topology else "targeted"] += 1
        read: set[str] = set()
        try:
            if topology:
                cached.topology_at_us = started_mono
                first = await self.adapter.refresh_tree(
                    cached.observation,
                    dirty,
                    spawned=spawned,
                    topology=True,
                    inventory_ready=inventory_error is None,
                    inventory_error=inventory_error,
                )
            else:
                first = await self.adapter.refresh_tree(cached.observation, dirty, spawned=spawned)
            observation = first.observation
            read |= first.read
            last = self._last_tree.get(session.session_id)
            was_quiet = last is not None and last[0] and not last[1]
            if observation.complete and not observation.busy and not was_quiet:
                self.passes["confirm"] += 1
                busy_before = sorted(n.node_id for n in cached.observation.nodes.values() if n.busy)
                second = await self.adapter.refresh_tree(
                    observation, (), confirm=True, confirm_busy=busy_before
                )
                observation = second.observation
                read |= second.read
        except (OmnigentReadError, OSError, ValueError):
            observation = replace(cached.observation, complete=False, errors=("refresh failed",))
        cached.observation = observation
        for node in read:
            cached.read_at_us[node] = started_utc
            cached.read_mono_us[node] = started_mono
        if not observation.complete:
            self.passes["targeted:incomplete"] += 1
        try:
            await self._observe(parcel, observation, read_at=cached.read_at_us)
        except InvariantViolation:
            await self.service.hold_observation_failure(parcel.parcel_id)
            return False
        return observation.complete

    async def _observe(
        self,
        parcel: Parcel,
        observation: TreeObservation,
        *,
        read_at: Mapping[str, int] | None = None,
    ) -> None:
        """Apply one tree observation. ``read_at`` (a targeted feed pass: node -> when it
        was last read, UTC us) limits "prompt gone" to prompts whose owner was re-read
        after the prompt opened; a full scan (None) re-read every node."""
        session = parcel.current_session
        if session is None or session.root_id is None:
            return
        # The tree read is slow and successive runs share the root: a run admitted
        # meanwhile owns what was observed. Crediting its activity to the stale run
        # (settled, so "external") would hold the parcel; the next pass reads it fresh.
        fresh = await self._load_parcel(parcel.parcel_id)
        current = fresh.current_session if fresh is not None else None
        if (
            fresh is None
            or current is None
            or current.session_id != session.session_id
            or current.root_id != session.root_id
            or current.execution_closed
        ):
            return
        parcel, session = fresh, current
        tracker = self._tracker(
            session.session_id, session.grant.grant_id, session.grant.consumed_us
        )
        tracker.gap_ended()
        tracker.observe_tree(observation)
        now = self.clock.now_utc_us()
        self._remember(session.session_id, observation, now, self._fresh_for(parcel, session))
        if self._last_runtime.get(session.session_id) != observation.busy:
            await self._apply(
                parcel,
                ev.RuntimeActivity(session_id=session.session_id, busy=observation.busy),
                f"runtime:{session.session_id}:{now}",
            )
            self._last_runtime[session.session_id] = observation.busy
        tree = (observation.complete, observation.busy, observation.pending_waiter)
        if self._last_tree.get(session.session_id) != tree:
            await self._apply(
                parcel,
                ev.TreeQuiescent(
                    session_id=session.session_id,
                    complete=observation.complete,
                    busy=observation.busy,
                    pending_waiter=observation.pending_waiter,
                ),
                f"tree:{session.session_id}:{now}",
            )
            self._last_tree[session.session_id] = tree
        await self._activity(parcel, tracker)
        cost = observation.root.total_cost_usd if observation.root is not None else None
        microdollars = None if cost is None else int(cost * 1_000_000)
        last_cost_at = self._last_cost_at.get(session.session_id)
        mono = self.clock.monotonic_us()
        if (
            session.session_id not in self._last_cost
            or self._last_cost[session.session_id] != microdollars
        ) and (last_cost_at is None or mono - last_cost_at >= COST_SAMPLE_INTERVAL_US):
            await self._apply(
                parcel,
                ev.CostSample(session_id=session.session_id, microdollars=microdollars),
                f"cost:{session.session_id}:{now}",
            )
            self._last_cost[session.session_id] = microdollars
            self._last_cost_at[session.session_id] = mono
        owned = observation.owned_elicitations()
        owners = self._prompt_owners.setdefault(session.session_id, {})
        gone_ok = None
        if read_at is not None:

            def gone_ok(eid: str, opened_at_us: int) -> bool:
                owner = owners.get(eid)
                return owner is not None and read_at.get(owner, -1) >= opened_at_us

        await self._prompts(parcel, owned, scanned_at_us=self._pass_started_us, gone_ok=gone_ok)
        owners.update({eid: owner for eid, (owner, _raw) in owned.items()})
        if (
            observation.root is not None
            and observation.root.status == "failed"
            and session.session_id not in self._crashed
        ):
            await self._apply(
                parcel,
                ev.SessionCrashed(session_id=session.session_id),
                f"crash:{session.session_id}",
            )
            self._crashed.add(session.session_id)

    def _parked(self, parcel: Parcel, session: StageSession) -> bool:
        """The run holds no turn and waits on someone else: only watched for external
        activity until the factory relays a move (a state change, which ends this).

        A build the reducer parked (``slot_parked``: Blocked, Needs you, idle on checks
        or the review bot, a settled checkpoint), or an ACTIVE/WAITING/FENCED run whose
        tree this process last saw complete and idle that is WAITING (decision, plan
        approval, checks) or whose card is not Working (Needs you, Blocked, Checkpoint,
        or Idle with its stage done, e.g. triage). Starting, running, grace and draining
        runs keep the active cadence; a run never seen idle is never parked here.
        """
        if session.lifecycle not in _PARKABLE_LIFECYCLES:
            return False
        if parcel.slot_parked:
            return True
        tree = self._last_tree.get(session.session_id)
        if tree is None or not tree[0] or tree[1]:
            return False  # not yet observed complete and idle
        return (
            session.lifecycle == Lifecycle.WAITING
            or bool(parcel.open_decisions)
            or Hold.AWAITING_OWNER in parcel.holds
            or parcel.bot not in (BotState.WORKING, BotState.QUEUED)
        )

    def _due(self, session_id: str, interval_seconds: float | None) -> bool:
        if interval_seconds is None:
            self._settled_read_at.pop(session_id, None)
            return True
        now = self.clock.monotonic_us()
        last = self._settled_read_at.get(session_id)
        if last is not None and now - last < interval_seconds * 1e6:
            return False
        self._settled_read_at[session_id] = now
        return True

    def _tracker(self, session_id: str, grant_id: str, baseline_us: int) -> ActivityTracker:
        tracker = self._trackers.get(session_id)
        if tracker is None or tracker.grant_id != grant_id:
            tracker = ActivityTracker(self.clock, session_id, grant_id, baseline_us=baseline_us)
            self._trackers[session_id] = tracker
        return tracker

    async def _activity(self, parcel: Parcel, tracker: ActivityTracker) -> None:
        """Persist the grant's active time only when the estimate passes what is stored
        (stored values are rounded up, see :func:`persisted_active_us`)."""
        sample = tracker.sample()
        session = parcel.current_session
        stored = self._last_activity.get(sample.grant_id, 0)
        if session is not None and session.grant.grant_id == sample.grant_id:
            stored = max(stored, session.grant.consumed_us)
            duration = session.grant.duration_us
        else:
            duration = sample.consumed_us  # unknown grant: store the estimate as is
        if sample.consumed_us <= stored:
            return
        value = persisted_active_us(sample.consumed_us, duration)
        await self._apply(
            parcel,
            replace(sample, consumed_us=value),
            f"activity:{sample.grant_id}:{value}",
        )
        self._last_activity[sample.grant_id] = value

    async def _prompts(
        self,
        parcel: Parcel,
        prompts: Mapping[str, tuple[str, Mapping[str, Any]]],
        *,
        complete: bool = True,
        scanned_at_us: int | None = None,
        gone_ok: Callable[[str, int], bool] | None = None,
    ) -> None:
        """Open new prompts, post due permission prompts and (``complete``: a full tree
        scan) close the decisions whose prompt is gone. A prompt first seen (by the probe)
        after the scan started is newer than the scan: never gone by it."""
        session = parcel.current_session
        if session is None or session.root_id is None:
            return
        normalizer = self._normalizers.setdefault(
            session.session_id,
            StreamNormalizer(
                session.session_id,
                session.root_id,
                seen_elicitations={d.elicitation_id for d in parcel.decisions},
                own_item_ids=set(session.own_items),
            ),
        )
        present = set(prompts)
        opened = False
        for eid, (_owner, raw) in prompts.items():
            for body in normalizer.on_event(session.root_id, raw):
                await self._apply(parcel, body, f"elicitation:{session.session_id}:{eid}:open")
                opened = True
        if opened or any(d.comment_deferred for d in parcel.open_decisions):
            await self._due_permission_prompts(parcel.parcel_id, session.session_id, present)
        if not complete:
            return
        for decision in parcel.decisions:
            if decision.source != DecisionSource.ELICITATION:
                continue  # factory_ask_owner questions have no native prompt to vanish
            if scanned_at_us is not None and decision.opened_at_us > scanned_at_us:
                continue
            if gone_ok is not None and not gone_ok(decision.elicitation_id, decision.opened_at_us):
                continue  # its owner was not re-read since it opened: not proof it is gone
            if decision.session_id == session.session_id and decision.elicitation_id not in present:
                await self._apply(
                    parcel,
                    ev.ElicitationGone(session.session_id, decision.elicitation_id),
                    f"elicitation:{session.session_id}:{decision.elicitation_id}:gone",
                )

    async def _due_permission_prompts(
        self, parcel_id: str, session_id: str, present: set[str]
    ) -> None:
        """Comment on each permission prompt still open after the grace (read live, so
        ``reload`` applies it). Restart-safe: the decision records whether it was posted
        and when it was first seen, and the event ID is stable."""
        parcel = await self._load_parcel(parcel_id)
        if parcel is None:
            return
        grace_us = int(self.service.config.permission_prompt_grace_seconds * 1_000_000)
        now = self.clock.now_utc_us()
        for d in parcel.decisions:
            if (
                d.session_id == session_id
                and d.comment_deferred
                and d.status == DecisionStatus.OPEN
                and d.elicitation_id in present
                and now - d.opened_at_us >= grace_us
            ):
                await self._apply(
                    parcel,
                    ev.PermissionPromptDue(session_id, d.elicitation_id),
                    f"elicitation:{session_id}:{d.elicitation_id}:due",
                )

    async def _apply(self, parcel: Parcel, body: ev.EventBody, event_id: str) -> None:
        await self.service.apply_event(
            Event(
                event_id=event_id,
                repo_id=self.service.config.repo_id,
                parcel_id=parcel.parcel_id,
                source_time_us=self.clock.now_utc_us(),
                provenance=Provenance.ADAPTER,
                body=body,
            )
        )

    async def _parcels(self) -> list[Parcel]:
        """Parcels whose current run is open (a closed run is never scanned, so idle
        parcels are not loaded every pass)."""
        ids = await self.service.open_run_parcel_ids()
        values = await asyncio.gather(*(self._load_parcel(parcel_id) for parcel_id in ids))
        return [parcel for parcel in values if parcel is not None]

    async def _load_parcel(self, parcel_id: str) -> Parcel | None:
        return await self.service.db.call(lambda store: store.load_parcel(parcel_id))
