"""Polling Omnigent observer and conservative active-time sampler."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping
from typing import Any, cast

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance, PublicationKind, ResultKind
from omnigent_factory.core.protocol import (
    BuildResult,
    CheckpointResult,
    Correlation,
    ParsedResult,
    PlanResult,
    ResultError,
    TriageResult,
    parse_factory_result,
)
from omnigent_factory.core.types import Lifecycle, Parcel, Size
from omnigent_factory.omnigent.activity import ActivityTracker
from omnigent_factory.omnigent.adapter import OmnigentExecutionAdapter
from omnigent_factory.omnigent.observe import StreamNormalizer
from omnigent_factory.omnigent.rest import OmnigentReadError
from omnigent_factory.ports.clock import Clock
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.runtime import FactoryService


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
    ) -> None:
        self.service = service
        self.adapter = adapter
        self.directory = directory
        self.clock = clock
        self.interval_seconds = interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._trackers: dict[str, ActivityTracker] = {}
        self._seen_items: set[str] = set()
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
        """Take one complete pass; successful roots are safe for boot re-enable checks."""
        observed: set[str] = set()
        for parcel in await self._parcels():
            session = parcel.current_session
            if session is None or session.root_id is None or session.execution_closed:
                continue
            try:
                await self._observe(parcel)
            except (OmnigentReadError, OSError, ValueError):
                tracker = self._tracker(
                    session.session_id, session.grant.grant_id, session.grant.consumed_us
                )
                tracker.gap_started()
                await self._activity(parcel, tracker)
            else:
                observed.add(session.session_id)
        return frozenset(observed)

    async def _observe(self, parcel: Parcel) -> None:
        session = parcel.current_session
        if session is None or session.root_id is None:
            return
        observation = await self.adapter.observe_tree(session.root_id)
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
        await self._results(parcel)

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
            if decision.session_id == session.session_id and decision.elicitation_id not in present:
                await self._apply(
                    parcel,
                    ev.ElicitationGone(session.session_id, decision.elicitation_id),
                    f"elicitation:{session.session_id}:{decision.elicitation_id}:gone",
                )

    async def _results(self, parcel: Parcel) -> None:
        session = parcel.current_session
        if session is None or session.root_id is None:
            return
        items = await self.adapter.rest.paginate(
            f"/v1/sessions/{session.root_id}/items", {"order": "asc"}
        )
        for item in items:
            item_id = item.get("id")
            if not isinstance(item_id, str) or item_id in self._seen_items:
                continue
            text = _assistant_text(item)
            if text is None or "FACTORY_RESULT_V1" not in text:
                continue
            expected = Correlation(
                parcel.parcel_id,
                session.session_id,
                session.nonce,
                session.revision,
                cast(Any, session.kind.value),
                waiver_build=(
                    parcel.current_approval is not None
                    and parcel.current_approval.kind.value == "skip"
                ),
                in_checkpoint=session.lifecycle
                in (Lifecycle.CHECKPOINT_GRACE, Lifecycle.CHECKPOINT_WAIT),
            )
            try:
                parsed = parse_factory_result(text, expected)
            except ResultError:
                result_kind = (
                    ResultKind.CHECKPOINT
                    if expected.in_checkpoint
                    else _default_result_kind(session.kind.value)
                )
                body = ev.ResultCandidate(
                    session_id=session.session_id,
                    root_id=session.root_id,
                    revision=session.revision,
                    valid=False,
                    result_kind=result_kind,
                )
            else:
                await self.directory.save_result(session.session_id, item_id, parsed)
                body = _candidate(session.root_id, parsed)
            await self._apply(parcel, body, f"result:{session.root_id}:{item_id}")
            self._seen_items.add(item_id)

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
        ids = await self.service._parcel_ids()
        values = await asyncio.gather(*(self._load_parcel(parcel_id) for parcel_id in ids))
        return [parcel for parcel in values if parcel is not None]

    async def _load_parcel(self, parcel_id: str) -> Parcel | None:
        return await self.service.db.call(lambda store: store.load_parcel(parcel_id))


def _assistant_text(item: Mapping[str, Any]) -> str | None:
    data = item.get("data") if isinstance(item.get("data"), dict) else item
    if not isinstance(data, Mapping) or data.get("role") != "assistant":
        return None
    if item.get("status") not in (None, "completed") and data.get("status") not in (
        None,
        "completed",
    ):
        return None
    content = data.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        texts: list[str] = [
            str(part["text"])
            for part in content
            if isinstance(part, dict) and isinstance(part.get("text"), str)
        ]
        return "".join(texts) if texts else None
    return None


def _candidate(root_id: str, parsed: ParsedResult) -> ev.ResultCandidate:
    envelope = parsed.result
    result = envelope.result
    if isinstance(result, TriageResult):
        return ev.ResultCandidate(
            session_id=envelope.stage_session_id,
            root_id=root_id,
            revision=envelope.revision,
            valid=True,
            result_kind=ResultKind.TRIAGE,
            size=Size(result.size),
        )
    if isinstance(result, PlanResult):
        return ev.ResultCandidate(
            session_id=envelope.stage_session_id,
            root_id=root_id,
            revision=envelope.revision,
            valid=True,
            result_kind=ResultKind.PLAN,
            publication_kind=PublicationKind(result.publication_kind),
            contract_canonical=(
                parsed.contract_canonical.decode()
                if parsed.contract_canonical is not None
                else None
            ),
            size=Size(result.contract.size),
            open_decision_ids=tuple(result.open_decision_ids),
        )
    if isinstance(result, BuildResult):
        return ev.ResultCandidate(
            session_id=envelope.stage_session_id,
            root_id=root_id,
            revision=envelope.revision,
            valid=True,
            result_kind=ResultKind.BUILD_READY,
            pr_number=result.pr_number,
            head_sha=result.head_sha,
        )
    if isinstance(result, CheckpointResult):
        return ev.ResultCandidate(
            session_id=envelope.stage_session_id,
            root_id=root_id,
            revision=envelope.revision,
            valid=True,
            result_kind=ResultKind.CHECKPOINT,
        )
    raise AssertionError("unreachable result model")


def _default_result_kind(stage: str) -> ResultKind:
    return {
        "triage": ResultKind.TRIAGE,
        "plan": ResultKind.PLAN,
        "build": ResultKind.BUILD_READY,
    }[stage]
