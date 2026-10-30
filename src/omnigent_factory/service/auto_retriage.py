"""Stale triage: flag Triaged cards whose triage may be out of date, and re-triage them
while the factory has nothing else to do.

* **Signals** (deterministic, no tokens), from the newest accepted triage
  (``ServiceDispatchDirectory.triage_freshness``: the base OID its run read, when it was
  accepted, its ``paths``, ``duplicate_issue`` and ``related``): a cited path changed
  between that OID and the last fetched ``origin/<base>`` OID read once per pass (local
  Git in the source clone, no fetch), GitHub reports the duplicate or a related issue
  closed (one batched read; an unreadable state is unknown, never closed), or the base
  moved ``retriage_min_commits`` commits. They count only once the triage is
  ``retriage_min_age_days`` old (no Git work before that); ``retriage_max_age_days``
  flags it whatever changed. Git answers are cached per (triage OID, base OID) and at
  most ``MAX_GIT_CARDS_PER_PASS`` cards need new ones per pass (the rest wait for the
  next pass, flags unchanged). A merged linked PR is not a signal: no stored board or
  link data carries it cheaply.
* **Flag.** Each flagged card's note says "Triage may be stale: <why>" (``TriageStale``,
  display only); a new triage, or the card leaving Triaged, clears it.
* **Re-triage.** The standing authorisation is the operator's (host config
  ``auto_retriage``, or the ``auto-retriage on|off`` CLI override, with a per-day budget
  ``auto_retriage_daily_limit``). When on, idle by the auto-triage rule and Inbox
  auto-triage has nothing to start, the best quiet flagged card gets an ``AutoRetriage``
  event from the trusted clock: close candidates first, then the strongest signal, then
  Rank. Each candidate is bound to its triage generation (the triage run whose result
  it judged) and re-evaluated just before the start: a newer triage, or signals gone,
  skips it without spending budget, and the reducer refuses a start whose generation is
  no longer the newest. It shares the triage slots and never clears a hold; the run's
  brief lists the signals (``triage-refresh`` template).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import partial

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, EventKind, Provenance
from omnigent_factory.core.predicates import settled
from omnigent_factory.core.protocol import CLOSURE_RECOMMENDATIONS
from omnigent_factory.core.types import (
    BotState,
    Hold,
    IssueSnapshot,
    Parcel,
    SessionKind,
    Stage,
)
from omnigent_factory.credentials.worktree import Workspaces, WorktreeError
from omnigent_factory.ports.github import BoardIssue, IssueRef, RankingCard
from omnigent_factory.service.auto_triage import (
    MAX_GRANT,
    SKIP_LABEL,
    AutoTriager,
    Budget,
    local_day,
)
from omnigent_factory.service.board_index import BoardIndex, BoardUnavailable
from omnigent_factory.service.directory import ServiceDispatchDirectory, TriageFreshness
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import AutoTriageState, SqliteStore

LOG = logging.getLogger(__name__)

MICROS_PER_DAY = 86_400 * 1_000_000
#: Candidates one pass tries (a fresh read can show one is no longer eligible).
MAX_ATTEMPTS_PER_PASS = 3
#: Signals shown per card (note, status, the run's brief).
MAX_SIGNALS = 5
#: Changed paths named in one signal.
MAX_PATHS_SHOWN = 3
#: How often the board is scanned for stale triages (each pass reuses a recent scan).
SCAN_INTERVAL_US = 10 * 60 * 1_000_000
#: The status header's commit count waits at most this long (never blocks rendering).
HEADER_GIT_TIMEOUT_S = 2.0
#: The last fetched base OID is re-read at most this often for the status header.
BASE_HEAD_TTL_S = 60.0
#: Cards whose Git facts are not cached yet that one pass asks Git about (at most two
#: subprocesses each); the rest are evaluated on a later pass.
MAX_GIT_CARDS_PER_PASS = 20

SnapshotReader = Callable[[IssueRef], Awaitable[IssueSnapshot | RetryableReadFailure]]
CardReader = Callable[[], Awaitable[list[RankingCard] | RetryableReadFailure]]
#: Open (True) or closed (False) per issue number, as GitHub reports it (``worktree_gc.
#: github_issue_states``); a number left out is unknown.
IssueStates = Callable[[Sequence[int]], Awaitable[dict[int, bool]]]

# Signal kinds, strongest first (the order candidates are taken in).
DUPLICATE_CLOSED = 0
RELATED_CLOSED = 1
PATHS_CHANGED = 2
MAX_AGE = 3
BASE_MOVED = 4

#: Why a stale Triaged card is not re-triaged automatically, in the order checked.
REFUSALS = (
    "assigned",
    "skip_label",
    "bundled",
    "held",
    "needs_you",
    "run_in_flight",
    "parked",
    "refused_today",
)


@dataclass(frozen=True, slots=True)
class Signal:
    kind: int
    text: str


@dataclass(frozen=True, slots=True)
class GitFacts:
    """What local Git says about a triage's base (None: unknown or not asked)."""

    commits: int | None = None
    changed: tuple[str, ...] | None = None


def signals(
    fresh: TriageFreshness,
    *,
    now_us: int,
    min_age_us: int,
    max_age_us: int,
    min_commits: int,
    closed: frozenset[int] | None,
    git: GitFacts,
) -> list[Signal]:
    """Why the triage may be stale, strongest first (empty: it is fresh)."""
    if fresh.accepted_at_us <= 0:
        return []  # an unknown age is never flagged
    age_us = now_us - fresh.accepted_at_us
    found: list[Signal] = []
    if age_us >= max_age_us:
        found.append(Signal(MAX_AGE, f"triaged {age_us // MICROS_PER_DAY} days ago"))
    if age_us < min_age_us:
        return found
    if closed is not None:  # issues GitHub reports closed (unknown is never closed)
        if fresh.duplicate_issue is not None and fresh.duplicate_issue in closed:
            found.append(Signal(DUPLICATE_CLOSED, f"duplicate #{fresh.duplicate_issue} closed"))
        gone = [n for n in fresh.related if n in closed and n != fresh.duplicate_issue]
        if gone:
            refs = ", ".join(f"#{n}" for n in gone[:MAX_PATHS_SHOWN])
            found.append(Signal(RELATED_CLOSED, f"related {refs} closed"))
    if git.changed:
        shown = ", ".join(git.changed[:MAX_PATHS_SHOWN])
        more = len(git.changed) - MAX_PATHS_SHOWN
        extra = f" (+{more})" if more > 0 else ""
        found.append(Signal(PATHS_CHANGED, f"cited paths changed: {shown}{extra}"))
    if git.commits is not None and git.commits >= min_commits:
        found.append(Signal(BASE_MOVED, f"{git.commits} commits on the base since"))
    return sorted(found, key=lambda s: s.kind)[:MAX_SIGNALS]


def refusal(
    issue: BoardIssue,
    parcel: Parcel,
    *,
    blocked: Callable[[str], bool] = lambda _node_id: False,
) -> str | None:
    """Why auto-retriage may not take this stale Triaged card (one of ``REFUSALS``)."""
    if issue.assigned or not parcel.eligible:
        return "assigned"
    if any(label.casefold() == SKIP_LABEL for label in issue.labels):
        return "skip_label"
    if Hold.BUNDLED in parcel.holds:
        return "bundled"
    if parcel.holds - {Hold.AWAITING_OWNER}:
        return "held"  # a posted triage's resting AWAITING_OWNER is no obstacle
    if parcel.open_decisions or parcel.bot == BotState.NEEDS_YOU:
        return "needs_you"
    cur = parcel.current_session
    if parcel.pending_authorization_id is not None or (cur is not None and not settled(cur)):
        return "run_in_flight"
    if blocked(issue.node_id):
        return "parked"
    return None


@dataclass(frozen=True, slots=True)
class Stale:
    """A Triaged card whose triage may be stale."""

    issue: BoardIssue
    parcel: Parcel
    signals: tuple[Signal, ...]
    close_candidate: bool
    rank: float | None = None
    refusal: str | None = None
    #: The triage run whose result was judged stale (its generation).
    generation: str = ""

    @property
    def text(self) -> str:
        return "; ".join(s.text for s in self.signals)

    def key(self) -> tuple[bool, int, bool, float, int]:
        """Close candidates first, then the strongest signal, then Rank."""
        return (
            not self.close_candidate,
            min(s.kind for s in self.signals),
            self.rank is None,
            self.rank if self.rank is not None else 0.0,
            self.issue.number,
        )


def order(stale: Iterable[Stale]) -> list[Stale]:
    return sorted(stale, key=Stale.key)


@dataclass(slots=True)
class _Pass:
    """One evaluation pass: its time, the base OID read once, the closed issues read
    once (None: unknown) and how many cards may still ask Git."""

    now_us: int
    head: str | None
    closed: frozenset[int] | None
    git_cards: int


class AutoRetriager:
    """Flags stale triages, starts idle-time re-triage; the ``auto-retriage`` command."""

    def __init__(
        self,
        service: FactoryService,
        board: BoardIndex,
        snapshot: SnapshotReader,
        *,
        directory: ServiceDispatchDirectory,
        workspaces: Workspaces,
        triager: AutoTriager,
        cards: CardReader | None = None,
        issue_states: IssueStates | None = None,
    ) -> None:
        self.service = service
        self.board = board
        self.snapshot = snapshot
        self.directory = directory
        self.workspaces = workspaces
        self.triager = triager
        self.cards = cards
        self.issue_states = issue_states
        #: Issues whose start the reducer refused today: not retried until tomorrow.
        self._refused: tuple[str, set[str]] = ("", set())
        #: The last scan: (when, stale cards) (``SCAN_INTERVAL_US``) and its pass facts
        #: (base OID, closed issues), which a start's re-evaluation reuses.
        self._scan: tuple[int, list[Stale]] | None = None
        self._last_pass: _Pass | None = None
        #: Git answers per (triage OID, base OID[, paths]): they never change.
        self._commits: dict[tuple[str, str], int] = {}
        self._changed: dict[tuple[str, str, tuple[str, ...]], tuple[str, ...]] = {}
        #: The last fetched base OID and when it was read (status header).
        self._head: tuple[float, str] | None = None

    # --------------------------------------------------------------- state

    async def _state(self) -> AutoTriageState:
        repo_id = self.service.config.repo_id
        state: AutoTriageState = await self.service.db.call(
            lambda store: store.auto_retriage_state(repo_id)
        )
        return state

    async def enabled(self) -> tuple[bool, str]:
        """Whether auto-retriage is on, and where that comes from (config or operator)."""
        configured = self.service.config.auto_retriage
        state = await self._state()
        source = (
            "operator"
            if state.enabled_override is not None and state.override_config == configured
            else "config"
        )
        return state.enabled(configured), source

    async def budget(self) -> Budget:
        config = self.service.config
        day, start, end = local_day(self.service.clock.now_utc_us())
        state = await self._state()
        used = await self.service.db.call(
            partial(
                SqliteStore.count_accepted_events,
                repo_id=config.repo_id,
                kind=EventKind.AUTO_RETRIAGE,
                since_us=start,
                until_us=end,
            )
        )
        return Budget(day, int(used), config.auto_retriage_daily_limit, state.granted_on(day))

    async def busy(self) -> str | None:
        """Why the factory is not idle for auto-retriage (None: idle): the auto-triage
        idle rule, and Inbox auto-triage goes first."""
        reason = await self.triager.busy()
        if reason is not None:
            return reason
        enabled, _source = await self.triager.enabled()
        if enabled and (await self.triager.budget()).remaining > 0:
            try:
                if await self.triager.candidates():
                    return "Inbox auto-triage has an issue to start"
            except BoardUnavailable:
                return "board unavailable"
        return None

    # --------------------------------------------------------------- git

    async def _git_facts(
        self, fresh: TriageFreshness, head: str | None, budget: _Pass | None = None
    ) -> GitFacts | None:
        """Commits and changed cited paths between the triage's OID and the captured base
        ``head`` OID (both ranges use that OID, never the moving ref). Cached; None when
        ``budget`` has no Git work left for this pass."""
        if not fresh.base_oid or head is None:
            return GitFacts()
        commits = self._commits.get((fresh.base_oid, head))
        key = (fresh.base_oid, head, fresh.paths)
        changed = self._changed.get(key) if fresh.paths else None
        missing = commits is None or (bool(fresh.paths) and changed is None)
        if missing and budget is not None:
            if budget.git_cards <= 0:
                return None
            budget.git_cards -= 1
        try:
            if commits is None:
                commits = await asyncio.to_thread(
                    self.workspaces.commits_since, fresh.base_oid, head
                )
                self._commits[(fresh.base_oid, head)] = commits
            if fresh.paths and changed is None:
                changed = await asyncio.to_thread(
                    self.workspaces.changed_since, fresh.base_oid, head, fresh.paths
                )
                self._changed[key] = changed
        except WorktreeError as exc:  # e.g. an OID the clone no longer has
            LOG.info("stale-triage git read failed oid=%s reason=%s", fresh.base_oid[:12], exc)
        return GitFacts(commits=commits, changed=changed)

    async def _base_head(self, *, cached: bool = False) -> str | None:
        now = time.monotonic()
        if cached and self._head is not None and now - self._head[0] < BASE_HEAD_TTL_S:
            return self._head[1]
        try:
            head = await asyncio.to_thread(
                self.workspaces.base_head, self.service.config.default_branch
            )
        except WorktreeError as exc:
            LOG.info("stale-triage base read failed reason=%s", exc)
            return None
        self._head = (now, head)
        return head

    # --------------------------------------------------------------- scan

    async def scan(self, *, force: bool = False) -> list[Stale]:
        """Every stale Triaged card, best first (raises ``BoardUnavailable``); flags each
        card's note to match. Reuses a scan younger than ``SCAN_INTERVAL_US``."""
        now = self.service.clock.now_utc_us()
        if not force and self._scan is not None and now - self._scan[0] < SCAN_INTERVAL_US:
            return self._scan[1]
        board = await self.board.issues()
        open_on_board = frozenset(i.number for i in board)
        triaged = [i for i in board if i.stage == Stage.TRIAGED]
        parcels = await self.service.db.call(
            partial(_parcels_by_id, parcel_ids=[i.node_id for i in triaged])
        )
        judged: list[tuple[BoardIssue, Parcel, TriageFreshness | None]] = []
        for issue in triaged:
            parcel = parcels.get(issue.node_id)
            if parcel is None or (parcel.links is not None and parcel.links.epic):
                continue  # never triaged here, or an epic (its triage is out of scope)
            judged.append((issue, parcel, await self.directory.triage_freshness(parcel)))
        # Issues on the board are open; only the other referenced ones are read.
        referenced = {
            n
            for _issue, _parcel, fresh in judged
            if fresh is not None and self._old_enough(fresh, now)
            for n in (*fresh.related, *([fresh.duplicate_issue] if fresh.duplicate_issue else []))
            if n not in open_on_board
        }
        current = _Pass(
            now,
            await self._base_head(),
            await self._closed(sorted(referenced)),
            git_cards=MAX_GIT_CARDS_PER_PASS,
        )
        ranks = await self._ranks()
        day, refused = self._refused
        skip = refused if day == local_day(now)[0] else set()
        found: list[Stale] = []
        for issue, parcel, fresh in judged:
            found_signals = await self._evaluate(fresh, current)
            if found_signals is None:
                continue  # its Git facts wait for a later pass: flag left as it is
            await self._flag(parcel, "; ".join(s.text for s in found_signals))
            if not found_signals or fresh is None:
                continue
            found.append(
                Stale(
                    issue=issue,
                    parcel=parcel,
                    signals=tuple(found_signals),
                    close_candidate=fresh.recommendation in CLOSURE_RECOMMENDATIONS,
                    rank=ranks.get(issue.node_id),
                    refusal=refusal(
                        issue,
                        parcel,
                        blocked=lambda node_id: (
                            node_id in skip or self.service.parked.blocks(node_id)
                        ),
                    ),
                    generation=fresh.session_id,
                )
            )
        stale = order(found)
        self._scan = (now, stale)
        self._last_pass = current
        return stale

    def _old_enough(self, fresh: TriageFreshness, now_us: int) -> bool:
        """Signals other than the maximum age can count (``retriage_min_age_days``)."""
        min_age_us = int(self.service.config.retriage_min_age_days * MICROS_PER_DAY)
        return fresh.accepted_at_us > 0 and now_us - fresh.accepted_at_us >= min_age_us

    async def _closed(self, numbers: Sequence[int]) -> frozenset[int] | None:
        """The issues GitHub reports closed (None: unknown, so no closed signal)."""
        if not numbers:
            return frozenset()
        if self.issue_states is None:
            return None
        try:
            states = await self.issue_states(numbers)
        except Exception as exc:  # unreadable: never treated as closed
            LOG.info("stale-triage issue state read failed reason=%s", exc)
            return None
        return frozenset(n for n, is_open in states.items() if not is_open)

    async def _evaluate(self, fresh: TriageFreshness | None, current: _Pass) -> list[Signal] | None:
        """The signals of one triage (None: its Git facts wait for a later pass). No Git
        work before the minimum age, nor without a known triage time or base OID."""
        if fresh is None:
            return []
        git = GitFacts()
        if self._old_enough(fresh, current.now_us):
            facts = await self._git_facts(fresh, current.head, current)
            if facts is None:
                return None
            git = facts
        config = self.service.config
        return signals(
            fresh,
            now_us=current.now_us,
            min_age_us=int(config.retriage_min_age_days * MICROS_PER_DAY),
            max_age_us=int(config.retriage_max_age_days * MICROS_PER_DAY),
            min_commits=config.retriage_min_commits,
            closed=current.closed,
            git=git,
        )

    async def _revalidate(self, item: Stale) -> Stale | None:
        """Just before a start: the card's newest triage is still the judged generation
        and still stale, and nothing refuses it (None: skip, no budget spent)."""
        parcel_id = item.parcel.parcel_id
        parcel = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None:
            return None
        fresh = await self.directory.triage_freshness(parcel)
        if fresh is None or fresh.session_id != item.generation:
            return None  # a newer triage was published since the scan
        if refusal(item.issue, parcel, blocked=self.service.parked.blocks) is not None:
            return None
        previous = self._last_pass
        current = _Pass(
            self.service.clock.now_utc_us(),
            previous.head if previous is not None else None,
            previous.closed if previous is not None else None,
            git_cards=1,
        )
        found = await self._evaluate(fresh, current)
        if not found:
            return None
        return replace(item, parcel=parcel, signals=tuple(found))

    async def _ranks(self) -> dict[str, float | None]:
        if self.cards is None:
            return {}
        cards = await self.cards()
        if isinstance(cards, RetryableReadFailure):
            return {}  # Rank only orders: an unreadable Rank is not a stop
        return {c.node_id: c.rank for c in cards}

    async def _flag(self, parcel: Parcel, text: str) -> None:
        """Keep the card's "Triage may be stale" note in step with the signals."""
        if " ".join(text.split()) == parcel.triage_stale:
            return
        now = self.service.clock.now_utc_us()
        result = await self.service.apply_event(
            Event(
                event_id=f"triage-stale:{parcel.parcel_id}:{now}:{uuid.uuid4().hex[:12]}",
                repo_id=self.service.config.repo_id,
                parcel_id=parcel.parcel_id,
                source_time_us=now,
                provenance=Provenance.ADAPTER,
                body=ev.TriageStale(text=text),
                issue_number=parcel.issue_number,
            )
        )
        if result.accepted:
            LOG.info("triage stale flag issue=#%s text=%s", parcel.issue_number, text or "-")

    # --------------------------------------------------------------- pass

    async def run_once(self) -> str:
        """Flag stale triages, then start at most one re-triage. Returns what happened."""
        try:
            stale = await self.scan()
        except BoardUnavailable as exc:
            LOG.warning("auto-retriage board read failed reason=%s", exc)
            return "board unavailable"
        enabled, _source = await self.enabled()
        if not enabled:
            return "disabled"
        budget = await self.budget()
        if budget.remaining <= 0:
            return f"daily budget used ({budget.used}/{budget.limit})"
        reason = await self.busy()
        if reason is not None:
            return f"not idle: {reason}"
        for item in [s for s in stale if s.refusal is None][:MAX_ATTEMPTS_PER_PASS]:
            current = await self._revalidate(item)
            if current is None:
                self._scan = None  # out of date: the next pass scans again
                LOG.info("auto-retriage skipped issue=#%s: no longer stale", item.issue.number)
                continue
            outcome = await self._start(current, budget)
            if outcome is not None:
                self._scan = None  # the started card is no longer a candidate
                return outcome
        return "no eligible stale triage"

    async def _start(self, item: Stale, budget: Budget) -> str | None:
        config = self.service.config
        issue = item.issue
        snapshot = await self.snapshot(IssueRef(config.repo_id, issue.number, issue.node_id))
        if not isinstance(snapshot, IssueSnapshot):
            LOG.info("auto-retriage read failed issue=#%s reason=%s", issue.number, snapshot.reason)
            return None
        if not (snapshot.eligible and snapshot.in_project and snapshot.stage == Stage.TRIAGED):
            return None  # assigned, closed or moved since the board read
        now = self.service.clock.now_utc_us()
        result = await self.service.apply_event(
            Event(
                event_id=f"auto-retriage:{issue.node_id}:{now}",
                repo_id=config.repo_id,
                parcel_id=issue.node_id,
                source_time_us=now,
                provenance=Provenance.SCHEDULER,
                body=ev.AutoRetriage(
                    signals=tuple(s.text for s in item.signals),
                    triage_session=item.generation,
                ),
                issue_number=issue.number,
                evidence=snapshot,
            )
        )
        if not result.accepted:
            LOG.info(
                "auto-retriage refused issue=#%s parcel=%s reason=%s",
                issue.number,
                issue.node_id,
                result.reason,
            )
            self._remember_refused(issue.node_id)
            return None
        LOG.info(
            "auto-retriage started issue=#%s parcel=%s source=scheduler "
            "authority=operator-standing-authorisation signals=%s used=%s/%s",
            issue.number,
            issue.node_id,
            item.text,
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

    # --------------------------------------------------------------- status header

    async def header(self, parcel: Parcel) -> str:
        """``Triaged 2026-10-01 against main@abc1234 · 52 commits behind`` for the status
        comment ("" without a known triage). Local Git only, cached; a slow or failed
        count is left out, never waited on."""
        if any(s.kind == SessionKind.TRIAGE and not settled(s) for s in parcel.sessions):
            return ""  # a triage is running: its result replaces this one
        fresh = await self.directory.triage_freshness(parcel)
        if fresh is None or fresh.accepted_at_us <= 0:
            return ""
        day = datetime.fromtimestamp(fresh.accepted_at_us / 1_000_000, UTC).strftime("%Y-%m-%d")
        if not fresh.base_oid:
            return f"Triaged {day}"
        base = self.service.config.default_branch
        text = f"Triaged {day} against {base}@{fresh.base_oid[:7]}"
        bare = TriageFreshness(fresh.session_id, fresh.accepted_at_us, fresh.base_oid, "")
        # Shielded: a count that misses this render still lands in the cache for the next.
        lookup = asyncio.ensure_future(self._header_facts(bare))
        try:
            facts = await asyncio.wait_for(asyncio.shield(lookup), HEADER_GIT_TIMEOUT_S)
        except TimeoutError:
            return text
        if facts.commits is None:
            return text
        return f"{text} · {facts.commits} commit{'s' if facts.commits != 1 else ''} behind"

    async def _header_facts(self, fresh: TriageFreshness) -> GitFacts:
        return await self._git_facts(fresh, await self._base_head(cached=True)) or GitFacts()

    # --------------------------------------------------------------- operator

    async def command(self, args: Mapping[str, object]) -> dict[str, object]:
        """``auto-retriage status|on|off|grant <n>`` over the operator socket."""
        action = str(args.get("action") or "status")
        config = self.service.config
        repo_id = config.repo_id
        if action in ("on", "off"):
            enabled, configured = action == "on", config.auto_retriage
            await self.service.db.call(
                lambda store: store.set_auto_retriage_override(repo_id, enabled, configured)
            )
            LOG.info("operator auto-retriage %s (config auto_retriage=%s)", action, configured)
        elif action == "grant":
            count = args.get("count")
            if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= MAX_GRANT:
                raise ValueError(f"grant needs a whole number 1-{MAX_GRANT}")
            day = local_day(self.service.clock.now_utc_us())[0]
            total = await self.service.db.call(
                lambda store: store.grant_auto_retriage(repo_id, day, count)
            )
            LOG.info("operator auto-retriage grant count=%s day=%s granted=%s", count, day, total)
        elif action != "status":
            raise ValueError("auto-retriage action must be status, on, off or grant")
        return await self.status()

    async def status(self) -> dict[str, object]:
        config = self.service.config
        enabled, source = await self.enabled()
        budget = await self.budget()
        busy = await self.busy()
        out: dict[str, object] = {
            "enabled": enabled,
            "enabled_source": source,
            "day": budget.day,
            "used_today": budget.used,
            "daily_limit": budget.daily_limit,
            "granted_today": budget.granted,
            "remaining_today": budget.remaining,
            "min_age_days": config.retriage_min_age_days,
            "min_commits": config.retriage_min_commits,
            "max_age_days": config.retriage_max_age_days,
            "idle": busy is None,
            "busy": busy,
        }
        try:
            stale = await self.scan(force=True)
        except BoardUnavailable as exc:
            out["candidates"] = None
            out["candidates_error"] = f"board unavailable: {exc}"
            return out
        out["candidates"] = [_candidate(s) for s in stale[:20]]
        out["stale_count"] = len(stale)
        eligible = [s for s in stale if s.refusal is None]
        out["next_candidate"] = _candidate(eligible[0]) if eligible else None
        return out


def _candidate(s: Stale) -> dict[str, object]:
    return {
        "issue": s.issue.number,
        "title": s.issue.title[:120],
        "reasons": [x.text for x in s.signals],
        "close_candidate": s.close_candidate,
        "rank": s.rank,
        "refused": s.refusal,
    }


def _parcels_by_id(store: SqliteStore, *, parcel_ids: Sequence[str]) -> dict[str, Parcel]:
    found: dict[str, Parcel] = {}
    for parcel_id in parcel_ids:
        parcel = store.load_parcel(parcel_id)
        if parcel is not None:
            found[parcel_id] = parcel
    return found


__all__ = [
    "REFUSALS",
    "AutoRetriager",
    "GitFacts",
    "Signal",
    "Stale",
    "order",
    "refusal",
    "signals",
]
