"""Auto-build: start the builds an owner approved with the board's "Auto-build" field.

The approval is the owner's: their own ``projects_v2_item`` edit setting a card's
"Auto-build" field to Queued (``AutoBuildMarked``) records a mark bound to the plan posted
then. The standing authorisation is the operator's (host config ``auto_build``, or the
``auto-build on|off`` CLI override, with an optional per-day budget) and decides only
when: whenever a build slot is free (not only when the factory is idle), no manually
approved build is waiting (those always go first), and fewer than
``auto_build_concurrency`` auto-builds hold a slot, the first eligible mark by Rank
(then oldest issue) gets an ``AutoBuild`` event from the trusted clock with a fresh read.
The reducer re-checks the mark against the posted plan and starts it like an approval.

Started auto-builds are counted from their accepted ``AutoBuild`` events (durable, never
pruned) per local calendar day of the host.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, EventKind, Provenance
from omnigent_factory.core.predicates import board_pending, dispatchable
from omnigent_factory.core.projection import (
    auto_build_capacity_available,
    building_capacity_available,
    pr_capacity_available,
    queue_head,
    running_text,
)
from omnigent_factory.core.types import (
    AdmissionSnapshot,
    AutoBuildStatus,
    IssueSnapshot,
    Parcel,
    QueueStatus,
    Stage,
    blockers_text,
)
from omnigent_factory.ports.github import IssueRef, RankingCard
from omnigent_factory.service.auto_triage import MAX_GRANT, local_day
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import AutoTriageState, SqliteStore

LOG = logging.getLogger(__name__)

#: Marks one pass tries (a fresh read can show one is no longer startable).
MAX_ATTEMPTS_PER_PASS = 3

SnapshotReader = Callable[[IssueRef], Awaitable[IssueSnapshot | RetryableReadFailure]]
#: Open board issues with their Rank (``RankingBoard.cards``; Rank None when unset).
CardReader = Callable[[], Awaitable[list[RankingCard] | RetryableReadFailure]]


@dataclass(frozen=True, slots=True)
class AutoBuildBudget:
    day: str
    used: int
    #: 0 = unlimited.
    daily_limit: int
    granted: int

    @property
    def remaining(self) -> int | None:
        """Auto-builds left today (None: unlimited)."""
        if self.daily_limit == 0:
            return None
        return max(0, self.daily_limit + self.granted - self.used)


@dataclass(frozen=True, slots=True)
class Candidate:
    parcel: Parcel
    rank: float | None
    created_at_us: int

    @property
    def number(self) -> int:
        return self.parcel.issue_number or 0


def order_candidates(
    parcels: Sequence[Parcel], cards: Mapping[str, RankingCard]
) -> list[Candidate]:
    """Marked parcels in start order: Rank ascending (unranked last), then the oldest
    issue, then the lowest issue number."""
    found = [
        Candidate(
            parcel,
            cards[parcel.parcel_id].rank if parcel.parcel_id in cards else None,
            cards[parcel.parcel_id].created_at_us if parcel.parcel_id in cards else 0,
        )
        for parcel in parcels
    ]
    return sorted(
        found,
        key=lambda c: (
            c.rank is None,
            c.rank if c.rank is not None else 0.0,
            c.created_at_us <= 0,
            c.created_at_us,
            c.number,
        ),
    )


def mark_blocker(parcel: Parcel, *, parked: bool = False) -> str | None:
    """Why this queued mark cannot start now, from the parcel alone (None: it can)."""
    if parcel.open_decisions:
        return "waiting for the answer to an open question"
    if parcel.revision_pending:
        return "a plan revision is in progress"
    if parcel.stage != Stage.SCOPED:
        return "not in Planning"
    if not dispatchable(parcel):
        return "the issue is closed, assigned or not on the board"
    if parked:
        return "a webhook delivery for it is parked"
    if board_pending(parcel):
        return "a card move is in flight"
    if parcel.unknown_effects:
        return "an unconfirmed write is outstanding"
    links = parcel.links
    if links is not None and links.epic:
        return "an epic: build its sub-issues"
    if links is not None and links.open_blockers:
        # Becomes eligible by itself once a read shows every blocker closed. Links never
        # read are not a blocker here: the start's fresh read decides (fails closed).
        return f"waiting on {blockers_text(links.open_blockers)}"
    return None


def admission_blocker(admission: AdmissionSnapshot, service: FactoryService) -> str | None:
    """Why no auto-build may start now, repository-wide (None: one may)."""
    trusted = service.config.trusted
    if admission.paused:
        return "paused"
    head = queue_head(admission)
    if head is not None and (head.resume or not head.auto):
        return "a queued build goes first"
    if not building_capacity_available(admission, trusted):
        running = admission.running()
        return (
            f"waiting for a build slot ({admission.building_count}/{trusted.max_building} "
            f"running: {running_text(running) or '-'})"
        )
    if not auto_build_capacity_available(admission, trusted):
        running = admission.running(auto=True)
        return (
            f"waiting for an auto-build slot ({admission.auto_build_count}/"
            f"{trusted.auto_build_concurrency} in use: {running_text(running)})"
        )
    if not pr_capacity_available(admission, trusted):
        return "every open-PR slot is taken"
    if head is not None:
        return "a queued build goes first"
    return None


class AutoBuilder:
    """Starts owner-marked auto-builds; also the ``auto-build`` operator command."""

    def __init__(
        self, service: FactoryService, snapshot: SnapshotReader, cards: CardReader
    ) -> None:
        self.service = service
        self.snapshot = snapshot
        self.cards = cards

    # --------------------------------------------------------------- state

    async def _state(self) -> AutoTriageState:
        repo_id = self.service.config.repo_id
        state: AutoTriageState = await self.service.db.call(
            lambda store: store.auto_build_state(repo_id)
        )
        return state

    async def enabled(self) -> tuple[bool, str]:
        """Whether auto-build is on, and where that comes from (config or operator)."""
        configured = self.service.config.auto_build
        state = await self._state()
        source = (
            "operator"
            if state.enabled_override is not None and state.override_config == configured
            else "config"
        )
        return state.enabled(configured), source

    async def budget(self) -> AutoBuildBudget:
        config = self.service.config
        day, start, end = local_day(self.service.clock.now_utc_us())
        state = await self._state()
        used = await self.service.db.call(
            partial(
                SqliteStore.count_accepted_events,
                repo_id=config.repo_id,
                kind=EventKind.AUTO_BUILD,
                since_us=start,
                until_us=end,
            )
        )
        return AutoBuildBudget(day, int(used), config.auto_build_daily_limit, state.granted_on(day))

    async def marked(self) -> list[Parcel]:
        """Parcels whose owner auto-build mark waits to be started."""
        repo_id = self.service.config.repo_id
        parcels: list[Parcel] = await self.service.db.call(partial(_marked, repo_id=repo_id))
        return parcels

    async def queue(self) -> list[Candidate]:
        """Waiting marks in start order (unranked by issue age when the board is unread)."""
        parcels = await self.marked()
        if not parcels:
            return []
        found = await self.cards()
        cards = (
            {}
            if isinstance(found, RetryableReadFailure)
            else {card.node_id: card for card in found}
        )
        if isinstance(found, RetryableReadFailure):
            LOG.info("auto-build board read failed reason=%s (ordering by issue)", found.reason)
        return order_candidates(parcels, cards)

    async def _admission(self) -> AdmissionSnapshot:
        repo_id = self.service.config.repo_id
        admission: AdmissionSnapshot = await self.service.db.call(
            lambda store: store.load_admission(repo_id)
        )
        return admission

    # --------------------------------------------------------------- pass

    async def run_once(self) -> str:
        """One decision: start at most one auto-build. Returns what happened (for logs)."""
        enabled, _source = await self.enabled()
        if not enabled:
            return "disabled"
        if not await self.marked():
            return "no auto-build queued"
        budget = await self.budget()
        if budget.remaining == 0:
            return f"daily limit used ({budget.used}/{budget.daily_limit + budget.granted})"
        if await self.service.db.call(lambda store: store.has_pending_delivery()):
            return "waiting: webhook deliveries are pending"
        blocker = admission_blocker(await self._admission(), self.service)
        if blocker is not None:
            return f"waiting: {blocker}"
        attempts = 0
        for candidate in await self.queue():
            parked = self.service.parked.blocks(candidate.parcel.parcel_id)
            if mark_blocker(candidate.parcel, parked=parked) is not None:
                continue
            outcome = await self._start(candidate.parcel)
            if outcome is not None:
                return outcome
            attempts += 1
            if attempts >= MAX_ATTEMPTS_PER_PASS:
                break
        return "no startable auto-build"

    async def _start(self, parcel: Parcel) -> str | None:
        config = self.service.config
        number = parcel.issue_number
        if number is None:
            return None
        snapshot = await self.snapshot(IssueRef(config.repo_id, number, parcel.parcel_id))
        if not isinstance(snapshot, IssueSnapshot):
            LOG.info("auto-build read failed issue=#%s reason=%s", number, snapshot.reason)
            return None
        now = self.service.clock.now_utc_us()
        event = Event(
            event_id=f"auto-build:{parcel.parcel_id}:{now}",
            repo_id=config.repo_id,
            parcel_id=parcel.parcel_id,
            source_time_us=now,
            provenance=Provenance.SCHEDULER,
            body=ev.AutoBuild(),
            issue_number=number,
            evidence=snapshot,
        )
        # The fresh read is applied even when the start is refused: a field the owner
        # cleared (its webhook lost) drops the mark here.
        result = await self.service.apply_event(event)
        if not result.accepted:
            LOG.info(
                "auto-build refused issue=#%s parcel=%s reason=%s",
                number,
                parcel.parcel_id,
                result.reason,
            )
            return None
        mark = parcel.auto_build
        LOG.info(
            "auto-build started issue=#%s parcel=%s source=scheduler "
            "authority=owner-auto-build-mark owner=%s mark_event=%s",
            number,
            parcel.parcel_id,
            mark.owner_id if mark is not None else "?",
            mark.source_event_id if mark is not None else "?",
        )
        return f"started #{number}"

    # --------------------------------------------------------------- operator

    async def command(self, args: Mapping[str, object]) -> dict[str, object]:
        """``auto-build status|on|off|grant <n>`` over the operator socket."""
        action = str(args.get("action") or "status")
        config = self.service.config
        repo_id = config.repo_id
        if action in ("on", "off"):
            enabled, configured = action == "on", config.auto_build
            await self.service.db.call(
                lambda store: store.set_auto_build_override(repo_id, enabled, configured)
            )
            LOG.info("operator auto-build %s (config auto_build=%s)", action, configured)
        elif action == "grant":
            count = args.get("count")
            if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= MAX_GRANT:
                raise ValueError(f"grant needs a whole number 1-{MAX_GRANT}")
            day = local_day(self.service.clock.now_utc_us())[0]
            total = await self.service.db.call(
                lambda store: store.grant_auto_build(repo_id, day, count)
            )
            LOG.info("operator auto-build grant count=%s day=%s granted=%s", count, day, total)
        elif action != "status":
            raise ValueError("auto-build action must be status, on, off or grant")
        return await self.status()

    async def status(self) -> dict[str, object]:
        config = self.service.config
        enabled, source = await self.enabled()
        budget = await self.budget()
        admission = await self._admission()
        blocker = admission_blocker(admission, self.service)
        if not enabled:
            blocker = "auto-build is off"
        elif budget.remaining == 0:
            blocker = "today's auto-build limit is used"
        queue = await self.queue()
        entries: list[dict[str, object]] = []
        for candidate in queue:
            parked = self.service.parked.blocks(candidate.parcel.parcel_id)
            why = mark_blocker(candidate.parcel, parked=parked)
            entries.append(
                {
                    "issue": candidate.number,
                    "rank": candidate.rank,
                    "eligible": why is None,
                    "blocker": why,
                }
            )
        return {
            "enabled": enabled,
            "enabled_source": source,
            "day": budget.day,
            "used_today": budget.used,
            "daily_limit": budget.daily_limit,
            "granted_today": budget.granted,
            "remaining_today": budget.remaining,
            "auto_build_concurrency": config.auto_build_concurrency,
            "auto_builds_running": admission.auto_build_count,
            "auto_builds_running_issues": [q.issue_number for q in admission.running(auto=True)],
            "auto_builds_parked": [q.issue_number for q in admission.parked() if q.auto],
            "auto_builds_waiting_for_slot": sum(
                1 for q in admission.queue if q.auto and q.status == QueueStatus.QUEUED
            ),
            "building": admission.building_count,
            "max_building": config.max_building,
            "can_start": blocker is None,
            "waiting_on": blocker,
            "queue": entries,
        }


def _marked(store: SqliteStore, *, repo_id: str) -> list[Parcel]:
    rows = store.query(
        "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND "
        "json_extract(aggregate_json, '$.parcel.auto_build.status') = ?",
        (repo_id, AutoBuildStatus.QUEUED.value),
    )
    return [parcel_from_json(str(row[0])) for row in rows]


__all__ = [
    "AutoBuildBudget",
    "AutoBuilder",
    "Candidate",
    "admission_blocker",
    "mark_blocker",
    "order_candidates",
]
