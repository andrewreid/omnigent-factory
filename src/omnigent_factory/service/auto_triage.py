"""Idle-time auto-triage: work through the Inbox while the factory has nothing else to do.

The standing authorisation is the operator's (host config ``auto_triage``, or the
``auto-triage on|off`` CLI override, with a per-day budget). When it is on and the
factory is idle (no plan, build or rework run working or draining, no queued build that
could start now, no stage request waiting, a triage slot free, not paused), the oldest
eligible Inbox issue gets an
``AutoTriage`` event from the trusted clock. The reducer starts triage exactly as an
owner Inbox -> Triage drag would; nothing past triage is ever started automatically.

Auto-started triages are counted from their accepted ``AutoTriage`` events (durable, never
pruned) per local calendar day of the host.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import partial

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, EventKind, Provenance
from omnigent_factory.core.projection import project_bot, startable_build
from omnigent_factory.core.types import (
    MICROS_PER_HOUR,
    BotState,
    IssueSnapshot,
    Lifecycle,
    Parcel,
    SessionKind,
    Stage,
    StageSession,
)
from omnigent_factory.ports.github import BoardIssue, IssueRef
from omnigent_factory.service.board_index import BoardIndex, BoardUnavailable
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import AutoTriageState, SqliteStore

LOG = logging.getLogger(__name__)

#: An owner label that keeps an issue out of auto-triage (it is otherwise ignored).
SKIP_LABEL = "factory:skip"
#: Candidates one pass tries (a fresh read can show one is no longer eligible).
MAX_ATTEMPTS_PER_PASS = 3
#: Most auto-triage budget one ``grant`` adds.
MAX_GRANT = 1000

SnapshotReader = Callable[[IssueRef], Awaitable[IssueSnapshot | RetryableReadFailure]]


@dataclass(frozen=True, slots=True)
class Budget:
    day: str
    used: int
    daily_limit: int
    granted: int

    @property
    def limit(self) -> int:
        return self.daily_limit + self.granted

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)


def local_day(now_us: int) -> tuple[str, int, int]:
    """The host's local calendar day containing ``now_us``: (ISO date, start, end) in us."""
    today = datetime.fromtimestamp(now_us / 1_000_000).astimezone().date()

    def midnight(day: date) -> int:
        return int(datetime.combine(day, time()).astimezone().timestamp() * 1_000_000)

    return today.isoformat(), midnight(today), midnight(today + timedelta(days=1))


def known_to_factory(parcel: Parcel | None) -> bool:
    """The factory has worked on (or holds) this issue: it is no auto-triage candidate."""
    return parcel is not None and bool(
        parcel.authorizations
        or parcel.sessions
        or parcel.holds
        or parcel.decisions
        or parcel.pending_authorization_id
    )


def eligible_candidates(
    issues: Iterable[BoardIssue],
    parcels: Mapping[str, Parcel],
    *,
    now_us: int,
    min_age_us: int,
    blocked: Callable[[str], bool] = lambda _node_id: False,
) -> list[BoardIssue]:
    """Inbox issues auto-triage may take, oldest first.

    Open repository issues only (the board read lists no pull requests or drafts), in the
    Inbox column, nobody assigned, no ``factory:skip`` label, older than the grace period,
    never worked on by the factory and not held by a parked delivery.
    """
    chosen = [
        issue
        for issue in issues
        if issue.stage == Stage.INBOX
        and not issue.assigned
        and all(label.casefold() != SKIP_LABEL for label in issue.labels)
        and issue.created_at_us > 0
        and now_us - issue.created_at_us >= min_age_us
        and not known_to_factory(parcels.get(issue.node_id))
        and not blocked(issue.node_id)
    ]
    return sorted(chosen, key=lambda issue: (issue.created_at_us, issue.number))


#: A plan or build run in one of these lifecycles is starting or working, if its card
#: shows ``Working``: Blocked, Needs you and Checkpoint mean it waits, whatever the
#: lifecycle says (an agent that reported blocked stays ``ACTIVE``).
_WORKING_LIFECYCLES = frozenset(
    {Lifecycle.INTENT, Lifecycle.CREATING, Lifecycle.PREPARING, Lifecycle.ACTIVE}
)


def _run_name(parcel: Parcel, session: StageSession) -> str:
    auth = parcel.authorization(session.authorization_id)
    return "rework" if auth is not None and auth.rework else session.kind.value


def busy_reason(parcels: Iterable[Parcel]) -> str | None:
    """Why stage work keeps the factory from idle (None: none does).

    A stage request not yet started (owner triage requests waiting for a slot go first),
    or a plan, build or rework run that is working or draining. Not stage work: a run
    waiting on the owner or on checks, a blocked one, and a card that only holds a build
    slot with no run working (triage runs are governed by the triage slots).
    """
    for parcel in parcels:
        pending = parcel.authorization(parcel.pending_authorization_id)
        if pending is not None and not pending.cancelled:
            return f"{pending.kind.value} requested on #{parcel.issue_number}"
        working = project_bot(parcel) == BotState.WORKING
        for session in parcel.sessions:
            if session.kind == SessionKind.TRIAGE:
                continue
            # A stop, revoke or checkpoint drain: the tree still runs, whatever the card shows.
            if session.lifecycle == Lifecycle.DRAINING:
                return f"{_run_name(parcel, session)} draining on #{parcel.issue_number}"
            if working and session.lifecycle in _WORKING_LIFECYCLES:
                return f"{_run_name(parcel, session)} running on #{parcel.issue_number}"
    return None


class AutoTriager:
    """Decides and starts idle-time auto-triage; also the ``auto-triage`` operator command."""

    def __init__(
        self, service: FactoryService, board: BoardIndex, snapshot: SnapshotReader
    ) -> None:
        self.service = service
        self.board = board
        self.snapshot = snapshot
        #: Issues whose start the reducer refused today: not retried until tomorrow.
        self._refused: tuple[str, set[str]] = ("", set())

    # --------------------------------------------------------------- state

    async def _state(self) -> AutoTriageState:
        repo_id = self.service.config.repo_id
        state: AutoTriageState = await self.service.db.call(
            lambda store: store.auto_triage_state(repo_id)
        )
        return state

    async def enabled(self) -> tuple[bool, str]:
        """Whether auto-triage is on, and where that comes from (config or operator)."""
        configured = self.service.config.auto_triage
        state = await self._state()
        enabled = state.enabled(configured)
        source = (
            "operator"
            if state.enabled_override is not None and state.override_config == configured
            else "config"
        )
        return enabled, source

    async def budget(self) -> Budget:
        config = self.service.config
        day, start, end = local_day(self.service.clock.now_utc_us())
        state = await self._state()
        used = await self.service.db.call(
            partial(
                SqliteStore.count_accepted_events,
                repo_id=config.repo_id,
                kind=EventKind.AUTO_TRIAGE,
                since_us=start,
                until_us=end,
            )
        )
        return Budget(day, int(used), config.auto_triage_daily_limit, state.granted_on(day))

    async def busy(self) -> str | None:
        """Why the factory is not idle for auto-triage (None: idle)."""
        config = self.service.config
        admission = await self.service.db.call(lambda store: store.load_admission(config.repo_id))
        if admission.paused:
            return "paused"
        if len(admission.triage_runs) >= config.triage_concurrency:
            return "every triage slot is taken"
        if await self.service.db.call(lambda store: store.has_pending_delivery()):
            return "webhook deliveries are pending"
        reason = busy_reason(
            await self.service.db.call(partial(_active_parcels, repo_id=config.repo_id))
        )
        if reason is not None:
            return reason
        head = startable_build(admission, config.trusted)
        if head is None:
            return None
        parcel = await self.service.db.call(lambda store: store.load_parcel(head.parcel_id))
        number = f" on #{parcel.issue_number}" if parcel is not None else ""
        return f"a queued build{number} can start"

    async def candidates(self) -> list[BoardIssue]:
        """Eligible Inbox issues, oldest first (raises ``BoardUnavailable``)."""
        issues = [i for i in await self.board.issues() if i.stage == Stage.INBOX]
        ids = [i.node_id for i in issues]
        parcels = await self.service.db.call(partial(_parcels_by_id, parcel_ids=ids))
        config = self.service.config
        day, refused = self._refused
        today = local_day(self.service.clock.now_utc_us())[0]
        skip = refused if day == today else set()
        return eligible_candidates(
            issues,
            parcels,
            now_us=self.service.clock.now_utc_us(),
            min_age_us=int(config.auto_triage_min_age_hours * MICROS_PER_HOUR),
            blocked=lambda node_id: node_id in skip or self.service.parked.blocks(node_id),
        )

    # --------------------------------------------------------------- pass

    async def run_once(self) -> str:
        """One decision: start at most one triage. Returns what happened (for logs)."""
        enabled, _source = await self.enabled()
        if not enabled:
            return "disabled"
        budget = await self.budget()
        if budget.remaining <= 0:
            return f"daily budget used ({budget.used}/{budget.limit})"
        reason = await self.busy()
        if reason is not None:
            return f"not idle: {reason}"
        try:
            candidates = await self.candidates()
        except BoardUnavailable as exc:
            LOG.warning("auto-triage board read failed reason=%s", exc)
            return "board unavailable"
        for issue in candidates[:MAX_ATTEMPTS_PER_PASS]:
            outcome = await self._start(issue, budget)
            if outcome is not None:
                return outcome
        return "no eligible Inbox issue"

    async def _start(self, issue: BoardIssue, budget: Budget) -> str | None:
        config = self.service.config
        snapshot = await self.snapshot(IssueRef(config.repo_id, issue.number, issue.node_id))
        if not isinstance(snapshot, IssueSnapshot):
            LOG.info("auto-triage read failed issue=#%s reason=%s", issue.number, snapshot.reason)
            return None
        if not (snapshot.eligible and snapshot.in_project and snapshot.stage == Stage.INBOX):
            return None  # assigned, closed or moved since the board read
        now = self.service.clock.now_utc_us()
        event = Event(
            event_id=f"auto-triage:{issue.node_id}:{now}",
            repo_id=config.repo_id,
            parcel_id=issue.node_id,
            source_time_us=now,
            provenance=Provenance.SCHEDULER,
            body=ev.AutoTriage(),
            issue_number=issue.number,
            evidence=snapshot,
        )
        result = await self.service.apply_event(event)
        if not result.accepted:
            LOG.info(
                "auto-triage refused issue=#%s parcel=%s reason=%s",
                issue.number,
                issue.node_id,
                result.reason,
            )
            self._remember_refused(issue.node_id)
            return None
        LOG.info(
            "auto-triage started issue=#%s parcel=%s source=scheduler "
            "authority=operator-standing-authorisation used=%s/%s",
            issue.number,
            issue.node_id,
            budget.used + 1,
            budget.limit,
        )
        return f"started #{issue.number}"

    def _remember_refused(self, node_id: str) -> None:
        today = local_day(self.service.clock.now_utc_us())[0]
        day, refused = self._refused
        if day != today:
            refused = set()
        refused.add(node_id)
        self._refused = (today, refused)

    # --------------------------------------------------------------- operator

    async def command(self, args: Mapping[str, object]) -> dict[str, object]:
        """``auto-triage status|on|off|grant <n>`` over the operator socket."""
        action = str(args.get("action") or "status")
        config = self.service.config
        repo_id = config.repo_id
        if action in ("on", "off"):
            enabled, configured = action == "on", config.auto_triage
            await self.service.db.call(
                lambda store: store.set_auto_triage_override(repo_id, enabled, configured)
            )
            LOG.info("operator auto-triage %s (config auto_triage=%s)", action, configured)
        elif action == "grant":
            count = args.get("count")
            if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= MAX_GRANT:
                raise ValueError(f"grant needs a whole number 1-{MAX_GRANT}")
            day = local_day(self.service.clock.now_utc_us())[0]
            total = await self.service.db.call(
                lambda store: store.grant_auto_triage(repo_id, day, count)
            )
            LOG.info("operator auto-triage grant count=%s day=%s granted=%s", count, day, total)
        elif action != "status":
            raise ValueError("auto-triage action must be status, on, off or grant")
        return await self.status()

    async def status(self) -> dict[str, object]:
        config = self.service.config
        enabled, source = await self.enabled()
        budget = await self.budget()
        admission = await self.service.db.call(lambda store: store.load_admission(config.repo_id))
        busy = await self.busy()
        out: dict[str, object] = {
            "enabled": enabled,
            "enabled_source": source,
            "day": budget.day,
            "used_today": budget.used,
            "daily_limit": budget.daily_limit,
            "granted_today": budget.granted,
            "remaining_today": budget.remaining,
            "min_age_hours": config.auto_triage_min_age_hours,
            "triage_concurrency": config.triage_concurrency,
            "triage_running": len(admission.triage_runs),
            "idle": busy is None,
            "busy": busy,
        }
        try:
            candidates = await self.candidates()
        except BoardUnavailable as exc:
            out["next_candidate"] = None
            out["next_candidate_error"] = f"board unavailable: {exc}"
        else:
            out["eligible_inbox"] = len(candidates)
            out["next_candidate"] = (
                {"issue": candidates[0].number, "title": candidates[0].title[:120]}
                if candidates
                else None
            )
        return out


def _parcels_by_id(store: SqliteStore, *, parcel_ids: list[str]) -> dict[str, Parcel]:
    found: dict[str, Parcel] = {}
    for parcel_id in parcel_ids:
        parcel = store.load_parcel(parcel_id)
        if parcel is not None:
            found[parcel_id] = parcel
    return found


def _active_parcels(store: SqliteStore, *, repo_id: str) -> list[Parcel]:
    """Parcels with a stage request recorded or a stage run that may be running."""
    rows = store.query(
        "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND ("
        "pending_authorization_id IS NOT NULL OR parcel_id IN ("
        "SELECT parcel_id FROM stage_sessions WHERE lifecycle NOT IN (?, ?)))",
        (repo_id, Lifecycle.RETIRED.value, Lifecycle.FENCED.value),
    )
    return [parcel_from_json(str(row[0])) for row in rows]


__all__ = [
    "SKIP_LABEL",
    "AutoTriager",
    "Budget",
    "busy_reason",
    "eligible_candidates",
    "known_to_factory",
    "local_day",
]
