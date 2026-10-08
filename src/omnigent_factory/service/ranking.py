"""Triage ranking: an idle-time pass that orders the Triage column (the board's Rank field).

The standing authorisation is the operator's (host config ``ranking``, or the
``ranking on|off`` CLI override; ``ranking now`` asks for one run). A run starts when the
factory is idle by the auto-triage rule, no triage is running, and enough changed since
the last completed run (``ranking_min_new_triages`` new triage results, or 24 hours and
any change in the Triage column). It never moves cards, starts stages or closes issues.

One run = one read-only Omnigent session (``RankingSessions``) that reads through
``factory_list_issues``/``factory_get_issue`` and submits once with
``factory_submit_ranking``. The submission is validated against a fresh board read and
committed together with its outbox (``ranking_writes``): Rank values that change, Priority
changes (each with one comment on the issue) and one project status update. Each write is
idempotent or adopted by its marker, so a restart never duplicates one. The session is
archived when the run completes, fails or is abandoned; a failure backs off.

Owner choices win: a Rank the owner sets (owner webhook, or a value the factory did not
write) pins the card at that rank until the owner clears it; a Priority the owner sets is
never changed by the factory again. Triage-set priorities are not owner-set.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from functools import partial
from typing import Any

from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.types import MICROS_PER_HOUR, MICROS_PER_MINUTE, SessionKind, Stage
from omnigent_factory.github.ranking import PRIORITIES, RankingBoard, RankingWriteError
from omnigent_factory.omnigent.ranking import RankingSessionError, RankingSessions
from omnigent_factory.ports.github import RankingCard
from omnigent_factory.service.auto_triage import stage_busy
from omnigent_factory.service.directory import (
    ServiceDispatchDirectory,
    _safe_publication,
    _template,
)
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import ranking as rs

LOG = logging.getLogger(__name__)

#: A run with no submission this long after its start message is abandoned.
RUN_TIMEOUT_US = 30 * MICROS_PER_MINUTE
#: A run whose session went idle without a submission is failed after this grace.
IDLE_GRACE_US = 2 * MICROS_PER_MINUTE
#: A run stuck before its start message (create/prepare retries) is abandoned after this.
SETUP_TIMEOUT_US = 15 * MICROS_PER_MINUTE
#: The time-based trigger: this long since the last completed run, plus any change.
MAX_AGE_US = 24 * MICROS_PER_HOUR
#: Failure backoff: doubles per consecutive failure, capped.
BASE_BACKOFF_US = MICROS_PER_HOUR
MAX_BACKOFF_US = 24 * MICROS_PER_HOUR
#: A write still failing after this many attempts is recorded as failed (never spun on).
MAX_WRITE_ATTEMPTS = 5
#: Loop cadence while a run is open (seconds).
ACTIVE_POLL_SECONDS = 10.0
SUMMARY_MAX = 600
REASON_MAX = 200
PRIORITY_REASON_MAX = 300
MAX_PRIORITY_CHANGES = 20
#: Issues listed in the project status update.
STATUS_TOP = 5


class RankingToolError(RuntimeError):
    """A precise, bounded error for the agent (the MCP tool returns it in-turn)."""


@dataclass(frozen=True, slots=True)
class PriorityChange:
    issue: int
    old: str | None
    new: str
    reason: str


@dataclass(frozen=True, slots=True)
class Submission:
    #: (issue, one-line reason) in the agent's order (pinned issues left out).
    order: tuple[tuple[int, str], ...]
    priority_changes: tuple[PriorityChange, ...]
    summary: str
    #: Final rank of every Triage issue (pinned ones at their pin).
    ranks: Mapping[int, float]


def one_line(text: object, limit: int) -> str:
    value = " ".join(str(text or "").split())
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def rank_text(value: float | None) -> str | None:
    if value is None:
        return None
    return str(int(value)) if float(value).is_integer() else repr(float(value))


def triage_digest(cards: Iterable[RankingCard]) -> str:
    """The Triage column's membership (a change starts the time-based trigger)."""
    nodes = sorted(c.node_id for c in cards if c.stage == Stage.TRIAGED)
    return hashlib.sha256("\n".join(nodes).encode()).hexdigest()


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def assign_ranks(order: Iterable[int], pinned: Mapping[int, float]) -> dict[int, float]:
    """Pinned issues keep their rank; the others take 1, 2, ... in order around them."""
    taken = {int(v) for v in pinned.values() if float(v).is_integer()}
    ranks: dict[int, float] = dict(pinned)
    next_rank = 1
    for issue in order:
        while next_rank in taken:
            next_rank += 1
        ranks[issue] = float(next_rank)
        next_rank += 1
    return ranks


def validate_submission(
    raw: Mapping[str, Any],
    triage: Mapping[int, RankingCard],
    pinned: Mapping[int, float],
    owner_priority: Iterable[int],
) -> Submission:
    """Check a submission against the current Triage column; raises with every problem.

    Every unpinned Triage issue exactly once; a pinned issue may be left out or listed at
    its pinned position; only Triage issues; priorities from P0-P3, never on an issue
    whose Priority the owner chose.
    """
    errors: list[str] = []
    sticky = set(owner_priority)
    items = raw.get("ranking")
    if not isinstance(items, list):
        errors.append("ranking: required - a list of {issue, reason} in order")
        items = []
    order: list[tuple[int, str]] = []
    seen: set[int] = set()
    for position, item in enumerate(items, start=1):
        issue = _int(item.get("issue")) if isinstance(item, Mapping) else None
        if issue is None:
            errors.append(f"ranking[{position}]: needs issue (a number) and reason")
            continue
        if issue in seen:
            errors.append(f"#{issue} is listed twice")
            continue
        seen.add(issue)
        if issue not in triage:
            errors.append(f"#{issue} is not in the Triage column")
            continue
        if issue in pinned:
            if float(position) != pinned[issue]:
                errors.append(
                    f"#{issue} is owner-pinned at rank {rank_text(pinned[issue])}: leave it "
                    "out or list it at that position"
                )
            continue
        reason = one_line(item.get("reason") if isinstance(item, Mapping) else "", REASON_MAX)
        if not reason:
            errors.append(f"#{issue}: reason is required (one line)")
        order.append((issue, reason))
    missing = sorted(set(triage) - set(pinned) - seen)
    if missing:
        errors.append("missing Triage issues: " + ", ".join(f"#{n}" for n in missing))
    summary = str(raw.get("summary") or "").strip()
    if not summary:
        errors.append("summary: required (at most 600 characters)")
    elif len(summary) > SUMMARY_MAX:
        errors.append(f"summary: {len(summary)} characters; at most {SUMMARY_MAX}")
    changes_raw = raw.get("priority_changes") or []
    changes: list[PriorityChange] = []
    if not isinstance(changes_raw, list) or len(changes_raw) > MAX_PRIORITY_CHANGES:
        errors.append(f"priority_changes: a list of at most {MAX_PRIORITY_CHANGES}")
        changes_raw = []
    changed: set[int] = set()
    for item in changes_raw:
        issue = _int(item.get("issue")) if isinstance(item, Mapping) else None
        if issue is None or not isinstance(item, Mapping):
            errors.append("priority_changes: each needs issue, new_priority and reason")
            continue
        new = item.get("new_priority")
        reason = one_line(item.get("reason"), PRIORITY_REASON_MAX)
        card = triage.get(issue)
        if card is None:
            errors.append(f"priority_changes: #{issue} is not in the Triage column")
        elif issue in changed:
            errors.append(f"priority_changes: #{issue} is listed twice")
        elif new not in PRIORITIES:
            errors.append(f"priority_changes: #{issue} new_priority must be one of P0-P3")
        elif issue in sticky:
            errors.append(
                f"priority_changes: the owner set the Priority of #{issue}; it is not changed"
            )
        elif new == card.priority:
            errors.append(f"priority_changes: #{issue} already has {new}")
        elif not reason:
            errors.append(f"priority_changes: #{issue} needs a reason (the new information)")
        else:
            changes.append(PriorityChange(issue, card.priority, str(new), reason))
        changed.add(issue)
    if errors:
        raise RankingToolError(
            "invalid ranking (fix these and resend):\n" + "\n".join(f"- {e}" for e in errors)
        )
    return Submission(
        order=tuple(order),
        priority_changes=tuple(changes),
        summary=summary,
        ranks=assign_ranks((n for n, _ in order), pinned),
    )


def _backoff(failures: int) -> int:
    return int(min(MAX_BACKOFF_US, BASE_BACKOFF_US * 2 ** max(0, failures)))


TriagePriorities = Callable[[list[str]], Awaitable[Mapping[str, frozenset[str]]]]


class Ranker:
    """Decides, runs and applies triage rankings; also the ``ranking`` operator command."""

    def __init__(
        self,
        service: FactoryService,
        *,
        board: RankingBoard | None,
        sessions: RankingSessions | None,
        triage_priorities: TriagePriorities,
        policy_barrier_us: int = 35_000_000,
    ) -> None:
        self.service = service
        self.board = board
        self.sessions = sessions
        self.triage_priorities = triage_priorities
        self.policy_barrier_us = policy_barrier_us
        self._lock = asyncio.Lock()
        #: The Priority field's node ID (resolved from the first board read).
        self.priority_field_id: str | None = None

    @property
    def repo_id(self) -> str:
        return self.service.config.repo_id

    def _now(self) -> int:
        return self.service.clock.now_utc_us()

    async def _db[T](self, operation: Callable[..., T], **kwargs: object) -> T:
        result: T = await self.service.db.call(partial(operation, **kwargs))
        return result

    # ------------------------------------------------------------------ state

    async def state(self) -> rs.RankingState:
        return await self._db(rs.ranking_state, repo_id=self.repo_id)

    async def enabled(self) -> tuple[bool, str]:
        configured = self.service.config.ranking
        state = await self.state()
        return state.enabled(configured), state.source(configured)

    async def busy(self) -> str | None:
        """Why the factory is not idle for a ranking (None: idle). The auto-triage rule,
        and no triage of any kind running (a ranking shares the triage slot)."""
        config = self.service.config
        admission = await self.service.db.call(lambda store: store.load_admission(config.repo_id))
        if admission.paused:
            return "paused"
        if admission.triage_runs:
            return "a triage is running"
        return await stage_busy(self.service, admission)

    def _not_configured(self) -> str | None:
        if self.board is None or self.sessions is None:
            return "ranking is not wired"
        if not self.service.config.rank_field_node_id:
            return "rank_field_node_id is not configured"
        return None

    async def cards(self) -> list[RankingCard]:
        """A fresh board read with Rank and Priority (raises ``RankingToolError``)."""
        assert self.board is not None  # noqa: S101 - _not_configured() checked first
        found = await self.board.cards(self.service.config.rank_field_node_id)
        if isinstance(found, RetryableReadFailure):
            raise RankingToolError(f"GitHub could not be read just now: {found.reason}")
        return found

    # ------------------------------------------------------------------ owner choices

    async def owner_choices(
        self, cards: list[RankingCard]
    ) -> tuple[dict[str, float], frozenset[str]]:
        """Reconcile owner choices from a fresh read: (pinned rank per node, nodes whose
        Priority the owner chose). A value the factory did not write is the owner's."""
        triage = [c for c in cards if c.stage == Stage.TRIAGED]
        nodes = [c.node_id for c in cards]
        fields = await self._db(rs.board_fields, node_ids=nodes)
        priorities = await self.triage_priorities([c.node_id for c in triage])
        now = self._now()
        pinned: dict[str, float] = {}
        sticky: set[str] = set()
        for card in cards:
            rank = self._reconcile_rank(card, fields.get((card.node_id, "rank")))
            prio = self._reconcile_priority(
                card,
                fields.get((card.node_id, "priority")),
                priorities.get(card.node_id, frozenset()),
            )
            for before, after in (
                (fields.get((card.node_id, "rank")), rank),
                (fields.get((card.node_id, "priority")), prio),
            ):
                if after is not None and after != before:
                    await self._db(rs.save_board_field, field=after, now_us=now)
                    if after.owner_set and (before is None or not before.owner_set):
                        LOG.info(
                            "ranking owner choice detected issue=#%s field=%s value=%s",
                            card.number,
                            after.field,
                            after.owner_value,
                        )
            if rank is not None and rank.owner_set and card.rank is not None:
                pinned[card.node_id] = card.rank
            if prio is not None and prio.owner_set:
                sticky.add(card.node_id)
        return pinned, frozenset(sticky)

    @staticmethod
    def _reconcile_rank(card: RankingCard, row: rs.BoardField | None) -> rs.BoardField | None:
        current = rank_text(card.rank)
        base = row or rs.BoardField(card.node_id, "rank")
        if row is not None and row.owner_refresh:
            return replace(
                row, owner_set=current is not None, owner_value=current, owner_refresh=False
            )
        if current is None:
            # Cleared on the board: an owner's pin ends (the factory never clears a Rank).
            return replace(base, owner_set=False, owner_value=None) if row is not None else None
        if row is not None and row.owner_set:
            return replace(row, owner_value=current)
        if current != base.factory_value:
            return replace(base, owner_set=True, owner_value=current)
        return row

    @staticmethod
    def _reconcile_priority(
        card: RankingCard, row: rs.BoardField | None, triaged: frozenset[str]
    ) -> rs.BoardField | None:
        current = card.priority
        if row is not None and (row.owner_refresh or row.owner_set):
            return replace(row, owner_set=True, owner_value=current, owner_refresh=False)
        if current is None:
            return row
        factory = row.factory_value if row is not None else None
        if current != factory and current not in triaged:
            base = row or rs.BoardField(card.node_id, "priority")
            return replace(base, owner_set=True, owner_value=current)
        return row

    async def note_owner_field(self, body: bytes) -> None:
        """A ``projects_v2_item`` delivery: record an owner's Rank or Priority change.

        Only the owner's own edits count (the factory bot's writes are ignored), on the
        configured project. A cleared Rank unpins; a value whose payload omits it is read
        on the next board read.
        """
        try:
            payload: Any = json.loads(body)
        except ValueError:
            return
        if not isinstance(payload, dict) or payload.get("action") != "edited":
            return
        config = self.service.config
        sender = payload.get("sender")
        item = payload.get("projects_v2_item")
        changes = payload.get("changes")
        change = changes.get("field_value") if isinstance(changes, dict) else None
        if (
            not isinstance(sender, dict)
            or sender.get("id") not in config.owners
            or not isinstance(item, dict)
            or item.get("project_node_id") != config.project_node_id
            or not isinstance(item.get("content_node_id"), str)
            or not isinstance(change, dict)
        ):
            return
        field_id = change.get("field_node_id")
        node_id = str(item["content_node_id"])
        now = self._now()
        if config.rank_field_node_id and field_id == config.rank_field_node_id:
            await self._owner_rank(node_id, change, now)
        elif field_id is not None and field_id == await self._priority_field():
            to = change.get("to")
            name = to.get("name") if isinstance(to, dict) else None
            value = str(name) if name in PRIORITIES else None
            await self._db(
                rs.mark_owner_change, node_id=node_id, field="priority", value=value, now_us=now
            )
            LOG.info("ranking owner priority recorded node=%s value=%s", node_id, value or "?")

    async def _owner_rank(self, node_id: str, change: Mapping[str, Any], now: int) -> None:
        if "to" not in change:
            await self._db(
                rs.mark_owner_change, node_id=node_id, field="rank", value=None, now_us=now
            )
            return
        to = change.get("to")
        number = to if isinstance(to, int | float) and not isinstance(to, bool) else None
        if number is None and isinstance(to, str):
            try:
                number = float(to)
            except ValueError:
                number = None
        if to is None:
            fields = await self._db(rs.board_fields, node_ids=[node_id])
            row = fields.get((node_id, "rank")) or rs.BoardField(node_id, "rank")
            await self._db(
                rs.save_board_field,
                field=replace(row, owner_set=False, owner_value=None, owner_refresh=False),
                now_us=now,
            )
            LOG.info("ranking owner pin cleared node=%s", node_id)
            return
        value = rank_text(number) if number is not None else None
        await self._db(rs.mark_owner_change, node_id=node_id, field="rank", value=value, now_us=now)
        LOG.info("ranking owner pin recorded node=%s rank=%s", node_id, value or "?")

    async def _priority_field(self) -> str | None:
        if self.priority_field_id is None and self.board is not None:
            try:
                fields = await self.board.adapter._single_select_fields(("Priority",))
            except Exception:
                return None
            field = fields.get("Priority")
            self.priority_field_id = field[0] if field is not None else None
        return self.priority_field_id

    # ------------------------------------------------------------------ the pass

    def next_wait(self, open_run: bool) -> float:
        return ACTIVE_POLL_SECONDS if open_run else self.service.config.reconcile_interval_seconds

    async def run_once(self) -> str:
        """One pass: archive finished sessions, advance the open run or maybe start one."""
        async with self._lock:
            await self._archive_finished()
            run = await self._db(rs.open_run, repo_id=self.repo_id)
            if run is not None:
                return await self._advance(run)
            return await self._maybe_start()

    async def _maybe_start(self) -> str:
        state = await self.state()
        enabled, _source = await self.enabled()
        forced = state.now_requested_at_us is not None
        if not enabled and not forced:
            return "disabled"
        problem = self._not_configured()
        if problem is not None:
            return f"not configured: {problem}"
        now = self._now()
        if not forced and state.retry_after_us is not None and now < state.retry_after_us:
            return "backing off after a failed run"
        reason = await self.busy()
        if reason is not None:
            return f"not idle: {reason}"
        try:
            cards = await self.cards()
        except RankingToolError as exc:
            LOG.warning("ranking board read failed reason=%s", exc)
            return "board unavailable"
        triage = [c for c in cards if c.stage == Stage.TRIAGED]
        if not triage:
            return "no issue in Triage"
        pinned, _sticky = await self.owner_choices(cards)
        due = await self._due(state, triage_digest(cards), forced)
        if due is None:
            return "nothing new to rank"
        if all(c.node_id in pinned for c in triage):
            return "every Triage issue is owner-pinned"
        return await self._start(cards, forced=forced, why=due)

    async def _due(self, state: rs.RankingState, digest: str, forced: bool) -> str | None:
        if forced:
            return "requested"
        last = state.last_completed_at_us or 0
        new = await self._db(rs.count_triage_results, since_us=last)
        if new >= self.service.config.ranking_min_new_triages:
            return f"{new} new triage results"
        changed = new > 0 or digest != state.last_triage_digest
        if changed and self._now() - last >= MAX_AGE_US:
            return "24 hours and the Triage column changed"
        return None

    async def _start(self, cards: list[RankingCard], *, forced: bool, why: str) -> str:
        now = self._now()
        run_id = f"rk_{uuid.uuid4().hex[:16]}"
        day = datetime.fromtimestamp(now / 1_000_000).astimezone().strftime("%Y-%m-%d %H:%M")
        run = rs.RankingRun(
            run_id=run_id,
            nonce=f"ranking-{uuid.uuid4().hex}",
            title=f"Factory ranking · {day}",
            state="creating",
            triage_digest=triage_digest(cards),
            created_at_us=now,
            forced=forced,
        )
        if not await self._db(rs.insert_run, repo_id=self.repo_id, run=run):
            return "a ranking run is already open"
        LOG.info(
            "ranking run started run=%s reason=%s source=scheduler "
            "authority=operator-standing-authorisation",
            run_id,
            why,
        )
        return await self._advance(run)

    async def _advance(self, run: rs.RankingRun) -> str:
        assert self.sessions is not None  # noqa: S101 - a run exists only when wired
        now = self._now()
        try:
            if run.state == "creating":
                if now - run.created_at_us > SETUP_TIMEOUT_US:
                    return await self._fail(run, "the session could not be created")
                root = await self.sessions.create(
                    run_id=run.run_id, nonce=run.nonce, title=run.title
                )
                await self._update(run, state="preparing", root_id=root)
                run = replace(run, state="preparing", root_id=root)
            if run.state == "preparing":
                assert run.root_id is not None  # noqa: S101
                if now - run.created_at_us > SETUP_TIMEOUT_US:
                    return await self._fail(run, "the session could not be prepared")
                await self.sessions.prepare(run.root_id, run.nonce)
                ready = self._now() + self.policy_barrier_us
                await self._update(run, state="sending", policy_ready_at_us=ready)
                run = replace(run, state="sending", policy_ready_at_us=ready)
            if run.state == "sending":
                assert run.root_id is not None  # noqa: S101
                if now - run.created_at_us > SETUP_TIMEOUT_US:
                    return await self._fail(run, "the start message could not be sent")
                if run.policy_ready_at_us is not None and self._now() < run.policy_ready_at_us:
                    return "waiting for the policy propagation barrier"
                await self.sessions.verify(run.root_id)
                text = await self._start_message(run)
                await self.sessions.send_once(run.root_id, f"ranking:{run.run_id}:start", text)
                await self._update(run, state="running", started_at_us=self._now())
                LOG.info("ranking session ready run=%s root=%s", run.run_id, run.root_id)
                return "started"
            if run.state == "running":
                return await self._watch(run)
            if run.state == "applying":
                return await self._apply(run)
        except RankingSessionError as exc:
            if exc.retry:
                LOG.info("ranking step retried run=%s reason=%s", run.run_id, exc.reason)
                return f"retrying: {exc.reason}"
            return await self._fail(run, exc.reason)
        except RankingToolError as exc:
            LOG.info("ranking step retried run=%s reason=%s", run.run_id, exc)
            return f"retrying: {exc}"
        return run.state

    async def _watch(self, run: rs.RankingRun) -> str:
        assert self.sessions is not None and run.root_id is not None  # noqa: S101
        started = run.started_at_us or run.created_at_us
        now = self._now()
        if now - started > RUN_TIMEOUT_US:
            return await self._fail(run, "no ranking was submitted within 30 minutes")
        root = await self.sessions.state(run.root_id)
        if root is None:
            return await self._fail(run, "the ranking session is gone")
        if root.archived:
            return await self._fail(run, "the ranking session was archived")
        if root.idle and now - started > IDLE_GRACE_US:
            return await self._fail(run, "the session ended its turn without a ranking")
        return "running"

    async def _start_message(self, run: rs.RankingRun) -> str:
        config = self.service.config
        cards = await self.cards()
        pinned, _sticky = await self.owner_choices(cards)
        triage = sorted((c for c in cards if c.stage == Stage.TRIAGED), key=lambda c: c.number)
        pins = ", ".join(
            f"#{c.number} at {rank_text(pinned[c.node_id])}" for c in triage if c.node_id in pinned
        )
        return (
            _template("ranking-v1.txt")
            .format(
                run_id=run.run_id,
                repository=config.repository,
                session_id=run.root_id,
                triage_column=config.status_names.get(Stage.TRIAGED.value, Stage.TRIAGED.value),
                count=len(triage),
                pinned=f"pinned: {pins}" if pins else "none are pinned now",
            )
            .strip()
        )

    async def _update(self, run: rs.RankingRun, **fields: object) -> None:
        await self._db(rs.update_run, run_id=run.run_id, now_us=self._now(), **fields)

    async def _fail(self, run: rs.RankingRun, reason: str) -> str:
        state = await self.state()
        backoff = _backoff(state.failures)
        await self._db(
            rs.finish_run,
            repo_id=self.repo_id,
            run_id=run.run_id,
            ok=False,
            outcome=reason,
            now_us=self._now(),
            backoff_us=backoff,
        )
        LOG.warning(
            "ranking run failed run=%s reason=%s retry_in_minutes=%s",
            run.run_id,
            reason,
            backoff // MICROS_PER_MINUTE,
        )
        if run.root_id is not None and self.sessions is not None:
            try:
                await self.sessions.interrupt(run.root_id)
            except Exception:
                LOG.info("ranking interrupt failed run=%s", run.run_id)
        await self._archive_finished()
        return f"failed: {reason}"

    async def _archive_finished(self) -> None:
        """Archive the session of every finished run (completed, failed or abandoned)."""
        if self.sessions is None:
            return
        for run in await self._db(rs.unresolved_runs, repo_id=self.repo_id):
            # The create's outcome was never learned: find the session by its nonce.
            try:
                root = await self.sessions.find(run.nonce)
            except RankingSessionError as exc:
                LOG.info("ranking session lookup retried run=%s reason=%s", run.run_id, exc)
                continue
            if root is None:
                await self._update(run, archived_at_us=self._now())  # nothing was created
            else:
                await self._update(run, root_id=root)
        for run in await self._db(rs.unarchived_runs, repo_id=self.repo_id):
            assert run.root_id is not None  # noqa: S101 - selected with a root
            try:
                await self.sessions.archive(run.root_id)
            except RankingSessionError as exc:
                LOG.info("ranking session archive retried run=%s reason=%s", run.run_id, exc)
                continue
            await self._update(run, archived_at_us=self._now())
            LOG.info("ranking session archived run=%s root=%s", run.run_id, run.root_id)

    # ------------------------------------------------------------------ submission

    async def submit(self, session_id: object, run_id: object, raw: object) -> dict[str, Any]:
        """``factory_submit_ranking``: validate and record the run's one submission."""
        if not isinstance(run_id, str) or not run_id:
            raise RankingToolError("run_id: required - the run id from your start message")
        if not isinstance(raw, Mapping):
            raise RankingToolError("ranking, summary: required")
        request = json.dumps(raw, sort_keys=True, default=str)
        async with self._lock:
            run = await self._db(rs.run_by_root, root_id=str(session_id))
            if run is None:
                raise RankingToolError("session_id is not a ranking session")
            if run.run_id != run_id:
                raise RankingToolError(
                    f"run_id {run_id} is not this session's ranking run ({run.run_id})"
                )
            if run.submission_json is not None:
                stored = json.loads(run.submission_json)
                if stored.get("request") == request:
                    return {**stored["receipt"], "replayed": True}
                raise RankingToolError("a ranking was already accepted for this run")
            if run.state != "running":
                raise RankingToolError(f"this ranking run is closed ({run.state})")
            problem = self._not_configured()
            if problem is not None:
                raise RankingToolError(problem)
            cards = await self.cards()
            pinned_nodes, sticky_nodes = await self.owner_choices(cards)
            triage = {c.number: c for c in cards if c.stage == Stage.TRIAGED}
            by_node = {c.node_id: c for c in cards}
            pinned = {by_node[n].number: v for n, v in pinned_nodes.items() if n in by_node}
            sticky = {by_node[n].number for n in sticky_nodes if n in by_node}
            submission = validate_submission(
                raw, triage, {n: v for n, v in pinned.items() if n in triage}, sticky
            )
            writes = self._writes(run, submission, triage)
            receipt = {
                "accepted": True,
                "run_id": run.run_id,
                "ranked": len(submission.ranks),
                "rank_changes": sum(w.kind == "rank" for w in writes),
                "priority_changes": len(submission.priority_changes),
                "next": "End your turn; the factory writes the ranking to the board.",
            }
            stored_submission = {
                "request": request,
                "receipt": receipt,
                "order": [list(item) for item in submission.order],
                "summary": submission.summary,
            }
            accepted = await self._db(
                rs.accept_submission,
                run_id=run.run_id,
                submission=stored_submission,
                writes=writes,
                now_us=self._now(),
            )
            if not accepted:
                raise RankingToolError("this ranking run is closed")
            LOG.info(
                "ranking submitted run=%s ranked=%s rank_changes=%s priority_changes=%s",
                run.run_id,
                receipt["ranked"],
                receipt["rank_changes"],
                receipt["priority_changes"],
            )
        # Apply in the background of the next pass; the agent's reply does not wait for it.
        return receipt

    def _writes(
        self, run: rs.RankingRun, submission: Submission, triage: Mapping[int, RankingCard]
    ) -> list[rs.RankingWrite]:
        writes: list[rs.RankingWrite] = []

        def add(kind: str, key: str, payload: dict[str, Any], card: RankingCard | None) -> str:
            write_id = f"{run.run_id}:{kind}:{key}"
            writes.append(
                rs.RankingWrite(
                    write_id=write_id,
                    run_id=run.run_id,
                    seq=len(writes),
                    kind=kind,
                    payload=payload,
                    issue_node_id=card.node_id if card is not None else None,
                    issue_number=card.number if card is not None else None,
                )
            )
            return write_id

        for change in submission.priority_changes:
            card = triage[change.issue]
            prio_id = add(
                "priority",
                str(change.issue),
                {"item_id": card.item_id, "new": change.new, "old": change.old},
                card,
            )
            add(
                "comment",
                str(change.issue),
                {"text": _priority_comment(change), "after": prio_id},
                card,
            )
        for number, rank in sorted(submission.ranks.items(), key=lambda kv: kv[1]):
            card = triage[number]
            if card.rank is not None and float(card.rank) == rank:
                continue  # no churn: the board already shows this rank
            add(
                "rank",
                str(number),
                {"item_id": card.item_id, "rank": int(rank), "old": rank_text(card.rank)},
                card,
            )
        if self.service.config.ranking_status_update:
            add(
                "status_update",
                "project",
                {"text": _status_text(submission, triage, self._now())},
                None,
            )
        return writes

    # ------------------------------------------------------------------ apply

    async def _apply(self, run: rs.RankingRun) -> str:
        assert self.board is not None  # noqa: S101 - a run exists only when wired
        writes = await self._db(rs.run_writes, run_id=run.run_id)
        states = {w.write_id: w for w in writes}
        for write in writes:
            if write.state != "pending":
                continue
            applied = await self._apply_one(write, states)
            if applied is None:
                return "applying"  # retried on the next pass, in order
            state, detail = applied
            await self._db(
                rs.update_write,
                write_id=write.write_id,
                state=state,
                detail=detail,
                now_us=self._now(),
                attempt=True,
            )
            states[write.write_id] = replace(write, state=state, detail=detail)
        done = sum(
            w.state == "done" and not w.detail.startswith("skipped") for w in states.values()
        )
        failed = [w for w in states.values() if w.state == "failed"]
        outcome = f"{done} writes applied" + (f", {len(failed)} failed" if failed else "")
        await self._db(
            rs.finish_run,
            repo_id=self.repo_id,
            run_id=run.run_id,
            ok=True,
            outcome=outcome,
            now_us=self._now(),
        )
        LOG.info("ranking run completed run=%s %s", run.run_id, outcome)
        await self._archive_finished()
        return "completed"

    async def _apply_one(
        self, write: rs.RankingWrite, states: Mapping[str, rs.RankingWrite]
    ) -> tuple[str, str] | None:
        assert self.board is not None  # noqa: S101
        node = write.issue_node_id or ""
        payload = write.payload
        try:
            if write.kind == "rank":
                return await self._write_field(
                    write,
                    "rank",
                    str(payload["rank"]),
                    lambda: self.board.set_rank(  # type: ignore[union-attr]
                        str(payload["item_id"]),
                        self.service.config.rank_field_node_id,
                        int(payload["rank"]),
                    ),
                )
            if write.kind == "priority":
                return await self._write_field(
                    write,
                    "priority",
                    str(payload["new"]),
                    lambda: self.board.set_priority(  # type: ignore[union-attr]
                        str(payload["item_id"]), str(payload["new"])
                    ),
                )
            if write.kind == "comment":
                before = states.get(str(payload.get("after")))
                if before is None or before.state != "done" or before.detail.startswith("skip"):
                    return "done", "skipped: the priority was not changed"
                comment = _safe_publication(str(payload["text"]), self.service.config)
                comment_id = await self.board.post_comment(
                    write.issue_number or 0, node, write.write_id, comment
                )
                return "done", f"comment {comment_id}"
            if write.kind == "status_update":
                if not self.service.config.ranking_status_update:
                    return "done", "skipped: status updates are off"
                text = _safe_publication(str(payload["text"]), self.service.config)
                update_id = await self.board.post_status_update(write.write_id, text)
                return "done", f"status update {update_id}"
        except RankingWriteError as exc:
            if exc.retry and write.attempts + 1 < MAX_WRITE_ATTEMPTS:
                await self._db(
                    rs.update_write,
                    write_id=write.write_id,
                    state="pending",
                    detail=exc.reason,
                    now_us=self._now(),
                    attempt=True,
                )
                LOG.info("ranking write retried write=%s reason=%s", write.write_id, exc.reason)
                return None
            await self._revert(write)
            if write.kind == "status_update":
                LOG.warning(
                    "ranking status update unavailable write=%s reason=%s (no summary posted)",
                    write.write_id,
                    exc.reason,
                )
            else:
                LOG.warning("ranking write failed write=%s reason=%s", write.write_id, exc.reason)
            return "failed", exc.reason
        return "failed", f"unknown write kind {write.kind}"

    async def _write_field(
        self,
        write: rs.RankingWrite,
        field: str,
        value: str,
        perform: Callable[[], Awaitable[None]],
    ) -> tuple[str, str]:
        """Write Rank/Priority unless the owner holds it; the intent is recorded first, so
        a crash after the write never reads the factory's own value as the owner's."""
        node = write.issue_node_id or ""
        fields = await self._db(rs.board_fields, node_ids=[node])
        row = fields.get((node, field)) or rs.BoardField(node, field)
        if row.owner_set:
            return "done", f"skipped: owner-{'pinned' if field == 'rank' else 'set'}"
        await self._db(
            rs.save_board_field, field=replace(row, factory_value=value), now_us=self._now()
        )
        await perform()
        return "done", f"{field} {value}"

    async def _revert(self, write: rs.RankingWrite) -> None:
        """A field write that failed for good: the board keeps its old value."""
        if write.kind not in ("rank", "priority"):
            return
        node = write.issue_node_id or ""
        fields = await self._db(rs.board_fields, node_ids=[node])
        row = fields.get((node, write.kind))
        if row is None:
            return
        old = write.payload.get("old")
        await self._db(
            rs.save_board_field,
            field=replace(row, factory_value=None if old is None else str(old)),
            now_us=self._now(),
        )

    # ------------------------------------------------------------------ operator

    async def command(self, args: Mapping[str, object]) -> dict[str, object]:
        """``ranking status|on|off|now`` over the operator socket."""
        action = str(args.get("action") or "status")
        config = self.service.config
        now = self._now()
        if action in ("on", "off"):
            await self._db(
                rs.set_ranking_override,
                repo_id=self.repo_id,
                enabled=action == "on",
                config_value=config.ranking,
                now_us=now,
            )
            LOG.info("operator ranking %s (config ranking=%s)", action, config.ranking)
        elif action == "now":
            await self._db(rs.request_ranking_now, repo_id=self.repo_id, now_us=now)
            LOG.info("operator ranking now requested")
        elif action != "status":
            raise ValueError("ranking action must be status, on, off or now")
        return await self.status()

    async def status(self) -> dict[str, object]:
        state = await self.state()
        enabled, source = await self.enabled()
        busy = await self.busy()
        runs = await self._db(rs.recent_runs, repo_id=self.repo_id)
        new = await self._db(rs.count_triage_results, since_us=state.last_completed_at_us or 0)
        return {
            "enabled": enabled,
            "enabled_source": source,
            "configured": self._not_configured() is None,
            "not_configured": self._not_configured(),
            "now_requested": state.now_requested_at_us is not None,
            "idle": busy is None,
            "busy": busy,
            "new_triage_results": new,
            "min_new_triages": self.service.config.ranking_min_new_triages,
            "last_completed_at_us": state.last_completed_at_us,
            "consecutive_failures": state.failures,
            "retry_after_us": state.retry_after_us,
            "status_update": self.service.config.ranking_status_update,
            "runs": [
                {
                    "run_id": r.run_id,
                    "state": r.state,
                    "title": r.title,
                    "outcome": r.outcome,
                    "archived": r.archived_at_us is not None or r.root_id is None,
                }
                for r in runs
            ],
        }


def directory_triage_priorities(
    service: FactoryService, directory: ServiceDispatchDirectory
) -> TriagePriorities:
    """Every priority the parcels' accepted triage results set (a revised triage that
    found the field already filled leaves the earlier triage's value on the board)."""

    async def read(node_ids: list[str]) -> Mapping[str, frozenset[str]]:
        rows = await service.db.call(
            lambda store: store.query(
                "SELECT parcel_id, aggregate_json FROM parcels "
                "WHERE parcel_id IN (SELECT value FROM json_each(?))",
                (json.dumps(node_ids),),
            )
        )
        found: dict[str, frozenset[str]] = {}
        for row in rows:
            parcel = parcel_from_json(str(row[1]))
            values: set[str] = set()
            for session in parcel.sessions:
                if session.kind != SessionKind.TRIAGE:
                    continue
                stored = directory.latest_result(session.session_id)
                record = stored.get("factory_result") if stored is not None else None
                body = record.get("result") if isinstance(record, dict) else None
                if isinstance(body, dict) and body.get("priority") in PRIORITIES:
                    values.add(str(body["priority"]))
            found[str(row[0])] = frozenset(values)
        return found

    return read


def _priority_comment(change: PriorityChange) -> str:
    old = change.old or "unset"
    return (
        f"Priority changed {old} -> {change.new} by the factory's triage ranking: {change.reason}"
    )


def _status_text(submission: Submission, triage: Mapping[int, RankingCard], now_us: int) -> str:
    day = datetime.fromtimestamp(now_us / 1_000_000).astimezone().strftime("%Y-%m-%d")
    ordered = sorted(submission.ranks.items(), key=lambda kv: (kv[1], kv[0]))
    reasons = dict(submission.order)
    lines = [f"**Triage ranking** · {day}", ""]
    for number, rank in ordered[:STATUS_TOP]:
        title = one_line(triage[number].title, 80)
        reason = reasons.get(number) or "owner-pinned"
        lines.append(f"{rank_text(rank)}. #{number} {title}: {reason}")
    if len(ordered) > STATUS_TOP:
        lines.append(f"…and {len(ordered) - STATUS_TOP} more in Triage.")
    if submission.priority_changes:
        lines += ["", "**Priority changes:**"]
        lines += [
            f"- #{c.issue} {c.old or 'unset'} -> {c.new}: {c.reason}"
            for c in submission.priority_changes
        ]
    lines += ["", one_line(submission.summary, SUMMARY_MAX)]
    return "\n".join(lines)


__all__ = [
    "PriorityChange",
    "Ranker",
    "RankingToolError",
    "Submission",
    "assign_ranks",
    "directory_triage_priorities",
    "rank_text",
    "triage_digest",
    "validate_submission",
]
