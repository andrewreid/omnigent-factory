"""Polling Omnigent observer and conservative active-time sampler.

Stage results are not read from the transcript: they arrive through the factory MCP tools
(:mod:`omnigent_factory.service.mcp`).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.predicates import run_closed, settled
from omnigent_factory.core.types import (
    BotState,
    DecisionSource,
    Hold,
    Lifecycle,
    Parcel,
    StageSession,
    WaitReason,
)
from omnigent_factory.omnigent.activity import ActivityTracker
from omnigent_factory.omnigent.adapter import OmnigentExecutionAdapter
from omnigent_factory.omnigent.observe import StreamNormalizer
from omnigent_factory.omnigent.rest import OmnigentReadError
from omnigent_factory.omnigent.tree import TreeObservation
from omnigent_factory.ports.clock import Clock
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.runtime import FactoryService

LOG = logging.getLogger(__name__)


class OmnigentObserver:
    """Feed tree, prompt, result, crash, cost and time observations through the service."""

    def __init__(
        self,
        service: FactoryService,
        adapter: OmnigentExecutionAdapter,
        directory: ServiceDispatchDirectory,
        clock: Clock,
        *,
        interval_seconds: float,
        settled_interval_seconds: float | None = None,
    ) -> None:
        self.service = service
        self.adapter = adapter
        self.directory = directory
        self.clock = clock
        self.interval_seconds = interval_seconds
        #: A settled tree (observed quiescent, nothing of ours running) is only watched for
        #: external activity, so it is read at this slower cadence; any other is every pass.
        self.settled_interval_seconds = settled_interval_seconds or interval_seconds
        self._settled_read_at: dict[str, int] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._trackers: dict[str, ActivityTracker] = {}
        self._normalizers: dict[str, StreamNormalizer] = {}
        self._last_runtime: dict[str, bool] = {}
        self._last_tree: dict[str, tuple[bool, bool, bool]] = {}
        self._last_activity: dict[str, int] = {}
        self._last_cost: dict[str, int | None] = {}
        self._crashed: set[str] = set()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="omnigent-observer")

    async def close(self) -> None:
        self._stop.set()
        was_running = self._task is not None
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if not was_running:
            return
        for parcel in await self._parcels():
            session = parcel.current_session
            tracker = self._trackers.get(session.session_id) if session is not None else None
            if tracker is not None:
                sample = tracker.sample()
                await self._apply(
                    parcel,
                    sample,
                    f"shutdown-activity:{sample.grant_id}:{sample.consumed_us}",
                )

    def healthy(self) -> bool:
        return self._task is None or not self._task.done()

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
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self.interval_seconds)

    async def observe_once(self) -> frozenset[str]:
        """Take one complete pass; successful roots are safe for boot re-enable checks.

        Every tree due in the pass is scanned with one shared inventory refresh
        (``observe_trees``); a failed refresh makes each scan incomplete, never idle.
        """
        due: list[tuple[Parcel, StageSession, str]] = []
        for parcel in await self._parcels():
            session = parcel.current_session
            if session is None or session.root_id is None or run_closed(parcel, session):
                continue
            slow = settled(session) or self._parked_on_owner(parcel, session)
            if self._due(session.session_id, settled_now=slow):
                due.append((parcel, session, session.root_id))
        if not due:
            return frozenset()
        results = await self.adapter.observe_trees([root for _, _, root in due])
        observed: set[str] = set()
        for (parcel, session, _root), result in zip(due, results, strict=True):
            try:
                if not isinstance(result, TreeObservation):
                    raise result
                await self._observe(parcel, result)
            except (OmnigentReadError, OSError, ValueError):
                tracker = self._tracker(
                    session.session_id, session.grant.grant_id, session.grant.consumed_us
                )
                tracker.gap_started()
                await self._activity(parcel, tracker)
            else:
                observed.add(session.session_id)
        return frozenset(observed)

    async def _observe(self, parcel: Parcel, observation: TreeObservation) -> None:
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
        if (
            session.session_id not in self._last_cost
            or self._last_cost[session.session_id] != microdollars
        ):
            await self._apply(
                parcel,
                ev.CostSample(session_id=session.session_id, microdollars=microdollars),
                f"cost:{session.session_id}:{now}",
            )
            self._last_cost[session.session_id] = microdollars
        await self._prompts(parcel, observation.owned_elicitations())
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

    def _parked_on_owner(self, parcel: Parcel, session: StageSession) -> bool:
        """A WAITING run last observed idle, waiting on the owner (an open decision, a
        plan approval, a Needs-you card): only watched for external activity, like a
        settled tree, until the factory relays the owner's move (which wakes it)."""
        if session.lifecycle != Lifecycle.WAITING:
            return False
        tree = self._last_tree.get(session.session_id)
        if tree is None or not tree[0] or tree[1]:
            return False  # not yet observed complete and idle
        return (
            bool(parcel.open_decisions)
            or session.wait_reason in (WaitReason.DECISION, WaitReason.PLAN_APPROVAL)
            or parcel.bot == BotState.NEEDS_YOU
            or Hold.AWAITING_OWNER in parcel.holds
        )

    def _due(self, session_id: str, *, settled_now: bool) -> bool:
        if not settled_now:
            self._settled_read_at.pop(session_id, None)
            return True
        now = self.clock.monotonic_us()
        last = self._settled_read_at.get(session_id)
        if last is not None and now - last < self.settled_interval_seconds * 1e6:
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
        sample = tracker.sample()
        if self._last_activity.get(sample.grant_id) == sample.consumed_us:
            return
        await self._apply(parcel, sample, f"activity:{sample.grant_id}:{sample.consumed_us}")
        self._last_activity[sample.grant_id] = sample.consumed_us

    async def _prompts(
        self, parcel: Parcel, prompts: Mapping[str, tuple[str, Mapping[str, Any]]]
    ) -> None:
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
        for eid, (_owner, raw) in prompts.items():
            for body in normalizer.on_event(session.root_id, raw):
                await self._apply(parcel, body, f"elicitation:{session.session_id}:{eid}:open")
        for decision in parcel.decisions:
            if decision.source != DecisionSource.ELICITATION:
                continue  # factory_ask_owner questions have no native prompt to vanish
            if decision.session_id == session.session_id and decision.elicitation_id not in present:
                await self._apply(
                    parcel,
                    ev.ElicitationGone(session.session_id, decision.elicitation_id),
                    f"elicitation:{session.session_id}:{decision.elicitation_id}:gone",
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
