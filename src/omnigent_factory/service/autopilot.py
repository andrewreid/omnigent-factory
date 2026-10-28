"""Epic autopilot: advance an epic's sub-issues through plan and build in dependency order.

The control is the owner's: their own ``projects_v2_item`` edit setting the epic card's
"Autopilot" field (``AutopilotMarked``: Full, Delayed or Plan only) starts the epic
planning pass (the epic triage, asked for the epic plan), and their ``/approve`` of the
posted epic plan (hash-bound) is the approval every later step carries. The standing
authorisation is the operator's (host config ``epic_autopilot`` or the ``epic-autopilot
on|off`` CLI override) and decides only whether passes run.

Each pass, per approved epic (one decision per sub-issue; the reducer re-checks each):

* human gates of the plan become sub-issues of the epic assigned to the owner (found by
  their marker after a restart, so never created twice), with blocked-by links from the
  sub-issues they come before;
* it pauses (no new starts) while a sub-issue it drives is Needs you or Blocked, or its
  plan goes beyond its part of the epic, and asks once when no build order exists;
* a claimed sub-issue with a posted plan that fits its part gets an auto-build mark
  (Full: at once; Delayed: ``epic_autopilot_delay_minutes`` after the plan was posted;
  Plan only: none, the owner approves as usual); the auto-build queue starts it like any
  auto-build (manual builds first, ``auto_build_concurrency``, ``max_building``);
* while fewer than the epic's concurrency (its "Parallel" field, else
  ``epic_autopilot_concurrency``) are in progress, the next open sub-issue with every
  blocker closed (epic plan order, then Rank, then oldest) gets ``AutopilotPlan``.
  Blockers come from the pass's board read when it saw every one (else the stored read);
  a waiting sub-issue whose stored blockers that read contradicts, or cannot confirm, is
  re-read (bounded per epic and pass, at most every ``REREAD_INTERVAL_US`` per issue), and the
  reducer starts nothing without a fresh per-issue read showing every blocker closed.
* an epic whose stored sub-issue list is shorter than GitHub's sub-issue total (a read
  from before the per-issue read paged the list, e.g. one cut at 50) is re-read, at most
  every ``REREAD_INTERVAL_US``; the pass goes on with the list it has meanwhile.
* a sub-issue labelled ``follow-up`` (``service.follow_ups``) is held, not started, unless
  it natively blocks another open sub-issue of the epic, the owner removed its label, or
  the owner put ``factory:build-follow-ups`` on the epic; the note counts held ones.
* bundles (``service.bundles``): a bundle issue that joined the epic is never one of its
  parts (never started, counted or asked about, whatever the bundle's state), and a
  sub-issue held in a bundle is never started, claimed, counted in progress or paused on:
  the bundle builds it. A part natively blocked by such a member waits like on any
  blocker, until the bundle PR closes the member; the note says ``bundle #X builds #N``.
  A member released or ejected from its bundle is an ordinary sub-issue again.

The epic card's note (``Epic · 3/9 done · autopilot: planning #882, next #883``) is the
only status output: no comments except the epic plan and genuine questions.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.predicates import settled
from omnigent_factory.core.types import (
    AUTOPILOT_DELAYED,
    AUTOPILOT_FULL,
    AUTOPILOT_PLAN_ONLY,
    MICROS_PER_MINUTE,
    AutoBuildStatus,
    BotState,
    EpicAutopilot,
    Hold,
    IssueLinks,
    IssueSnapshot,
    Parcel,
    Stage,
)
from omnigent_factory.ports.github import IssueRef, RankingCard
from omnigent_factory.service.follow_ups import is_held
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import bundles as bs
from omnigent_factory.store.sqlite import AutoTriageState, FollowUpRow, SqliteStore

LOG = logging.getLogger(__name__)

#: Sub-issue plan starts one pass tries per epic (a fresh read can refuse one).
MAX_STARTS_PER_PASS = 3
#: Columns of a sub-issue autopilot counts as in progress (Ready waits only for merge).
_IN_PROGRESS = frozenset({Stage.TRIAGED, Stage.SCOPED, Stage.BUILDING})
SKIP_LABEL = "factory:skip"
#: Stale waiting sub-issues re-read per epic per pass, and how often one issue is re-read.
MAX_REREADS_PER_PASS = 5
REREAD_INTERVAL_US = 5 * MICROS_PER_MINUTE

SnapshotReader = Callable[[IssueRef], Awaitable[IssueSnapshot | RetryableReadFailure]]
#: Open board issues with Rank and the epic's Parallel value.
CardReader = Callable[[], Awaitable[list[RankingCard] | RetryableReadFailure]]
#: The issue's labels from its latest webhook (None: unknown).
LabelReader = Callable[[Parcel], Awaitable[list[str] | None]]
#: The stored epic plan the owner approved (None: unreadable or not the approved hash).
PlanReader = Callable[[EpicAutopilot], dict[str, Any] | None]


class GateWriter(Protocol):
    """GitHub writes for human gates (``github.links``); idempotent by marker and link."""

    async def find(self, epic: int) -> dict[str, int]: ...

    async def create(
        self, epic: int, *, key: str, title: str, steps: str, blocks: Sequence[int], owner_id: int
    ) -> int: ...

    async def link(self, blocked: int, blocking: int, *, source: int) -> bool: ...


@dataclass(frozen=True, slots=True)
class Sub:
    """One open sub-issue of the epic as the pass sees it."""

    number: int
    parcel: Parcel | None
    card: RankingCard | None
    position: int

    def key(self) -> tuple[int, bool, float, bool, int, int]:
        rank = self.card.rank if self.card is not None else None
        created = self.card.created_at_us if self.card is not None else 0
        return (
            self.position,
            rank is None,
            rank if rank is not None else 0.0,
            created <= 0,
            created,
            self.number,
        )


class EpicAutopilotService:
    """The epic autopilot pass and the ``epic-autopilot`` operator command."""

    def __init__(
        self,
        service: FactoryService,
        snapshot: SnapshotReader,
        cards: CardReader,
        *,
        labels: LabelReader,
        plans: PlanReader,
        gates: GateWriter | None = None,
    ) -> None:
        self.service = service
        self.snapshot = snapshot
        self.cards = cards
        self.labels = labels
        self.plans = plans
        self.gates = gates
        #: When each parcel was last re-read for stale blockers (this process only).
        self._reread_at: dict[str, int] = {}

    # --------------------------------------------------------------- state

    async def _state(self) -> AutoTriageState:
        repo_id = self.service.config.repo_id
        state: AutoTriageState = await self.service.db.call(
            lambda store: store.epic_autopilot_state(repo_id)
        )
        return state

    async def enabled(self) -> tuple[bool, str]:
        """Whether epic autopilot is on, and where that comes from (config or operator)."""
        configured = self.service.config.epic_autopilot
        state = await self._state()
        source = (
            "operator"
            if state.enabled_override is not None and state.override_config == configured
            else "config"
        )
        return state.enabled(configured), source

    async def _parcels(self, sql: str, args: tuple[object, ...]) -> list[Parcel]:
        rows = await self.service.db.call(lambda store: store.query(sql, args))
        return [parcel_from_json(str(row[0])) for row in rows]

    async def epics(self) -> list[Parcel]:
        repo_id = self.service.config.repo_id
        found = await self._parcels(
            "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND "
            "json_extract(aggregate_json, '$.parcel.autopilot') IS NOT NULL",
            (repo_id,),
        )
        return sorted(found, key=lambda p: p.issue_number or 0)

    async def _bundle_issues(self) -> set[int]:
        """Every bundle issue of the repository (in any state): never a part of an epic."""
        repo_id = self.service.config.repo_id
        found = await self.service.db.call(partial(bs.bundles_of, repo_id=repo_id))
        return {b.issue_number for b in found if b.issue_number is not None}

    async def _by_number(self, number: int) -> Parcel | None:
        repo_id = self.service.config.repo_id
        found = await self._parcels(
            "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND issue_number = ?",
            (repo_id, number),
        )
        return found[0] if found else None

    # --------------------------------------------------------------- authority

    async def blocker(self, parcel: Parcel) -> str | None:
        """Why an autopilot auto-build mark may not start now (None: it may; not an
        autopilot mark: None). The auto-build queue asks just before each start."""
        mark = parcel.auto_build
        if mark is None or not mark.autopilot_epic:
            return None
        if not (await self.enabled())[0]:
            return "epic autopilot is off"
        epic = await self._by_number(mark.autopilot_epic)
        ap = epic.autopilot if epic is not None else None
        claim = parcel.autopilot_claim
        if ap is None:
            return f"autopilot is off on #{mark.autopilot_epic}"
        if claim is None or claim.epoch != ap.source_event_id or not claim.active:
            return f"autopilot on #{mark.autopilot_epic} no longer drives it"
        if not ap.approved:
            return f"the epic plan of #{mark.autopilot_epic} awaits approval"
        if ap.level == AUTOPILOT_PLAN_ONLY:
            return f"#{mark.autopilot_epic} is on Plan only"
        if ap.paused:
            return f"autopilot paused on #{mark.autopilot_epic}: {ap.paused}"
        return None

    # --------------------------------------------------------------- pass

    async def run_once(self) -> str:
        """One pass over every epic on autopilot. Returns what happened (for logs)."""
        enabled, _source = await self.enabled()
        withdrawn = await self._withdraw_orphans()
        epics = await self.epics()
        if not epics:
            return f"no epic on autopilot{f' (withdrew {withdrawn})' if withdrawn else ''}"
        found = await self.cards()
        if isinstance(found, RetryableReadFailure):
            LOG.info("epic autopilot: board unreadable reason=%s", found.reason)
            return "waiting: the board could not be read"
        cards = {card.number: card for card in found}
        outcomes = []
        for epic in epics:
            outcome = await self._advance(epic, cards, enabled=enabled)
            outcomes.append(f"#{epic.issue_number}: {outcome}")
        return "; ".join(outcomes)

    async def _withdraw_orphans(self) -> int:
        """Queued autopilot marks whose epic no longer authorises them are dropped (the
        epic's Autopilot cleared or re-set, or lowered to Plan only)."""
        repo_id = self.service.config.repo_id
        marked = await self._parcels(
            "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND "
            "json_extract(aggregate_json, '$.parcel.auto_build.status') = ? AND "
            "COALESCE(json_extract(aggregate_json, '$.parcel.auto_build.autopilot_epic'), 0) > 0",
            (repo_id, AutoBuildStatus.QUEUED.value),
        )
        count = 0
        for parcel in marked:
            mark = parcel.auto_build
            assert mark is not None  # noqa: S101 - selected above
            epic = await self._by_number(mark.autopilot_epic)
            ap = epic.autopilot if epic is not None else None
            claim = parcel.autopilot_claim
            if ap is None:
                reason = f"autopilot was turned off on #{mark.autopilot_epic}"
            elif claim is None or claim.epoch != ap.source_event_id:
                reason = f"autopilot on #{mark.autopilot_epic} was set again"
            elif ap.level == AUTOPILOT_PLAN_ONLY:
                reason = f"#{mark.autopilot_epic} was set to Plan only"
            else:
                continue
            count += int(
                await self._apply(
                    parcel, ev.AutopilotWithdraw(epic=mark.autopilot_epic, reason=reason)
                )
            )
        return count

    async def _advance(
        self, epic: Parcel, cards: Mapping[int, RankingCard], *, enabled: bool
    ) -> str:
        ap = epic.autopilot
        assert ap is not None  # noqa: S101 - epics() selects them
        number = epic.issue_number or 0
        links = epic.links
        if links is None or not links.epic:
            return await self._status(epic, "Epic · autopilot: waiting for its links", "")
        progress = f"Epic · {links.sub_completed}/{links.sub_total} done"
        if len(links.sub_issues) < links.sub_total:
            await self._reread_short_epic(epic, links)
        if not enabled:
            return await self._status(
                epic, f"{progress} · autopilot off (factory switch)", "the factory switch is off"
            )
        if ap.plan is None or ap.plan.posted_at_us is None:
            return await self._status(epic, f"{progress} · autopilot: writing the epic plan", "")
        if ap.revision_pending:
            return await self._status(epic, f"{progress} · autopilot: revising the epic plan", "")
        if not ap.approved:
            return await self._status(
                epic, f"{progress} · autopilot: waiting for your approval of the epic plan", ""
            )
        plan = self.plans(ap)
        if plan is None:
            return await self._pause(epic, "the approved epic plan cannot be read")
        section = plan.get("plan") if isinstance(plan.get("plan"), dict) else {}
        assert isinstance(section, dict)  # noqa: S101 - narrowed above
        gates, problem = await self._ensure_gates(epic, section)
        if problem:
            return await self._pause(epic, problem)
        epic = await self._by_number(number) or epic  # the gate events moved it on
        order = [
            item["issue"]
            for item in plan.get("build_order") or []
            if isinstance(item, dict) and isinstance(item.get("issue"), int)
        ]
        # A gate blocks until a read shows it closed (one created this pass is not yet in
        # the epic's links, nor its blocked-by links in the sub-issues' reads).
        open_gates = {
            n
            for n in gates.values()
            if not any(s.number == n and not s.open and not s.repo for s in links.sub_issues)
        }
        gate_blocks: dict[int, set[int]] = {}
        for gate in section.get("human_gates") or []:
            if not isinstance(gate, dict) or gate.get("key") not in gates:
                continue
            for dep in gate.get("blocks") or []:
                if isinstance(dep, int):
                    gate_blocks.setdefault(dep, set()).add(gates[str(gate["key"])])
        excluded = set(gates.values()) | await self._bundle_issues()
        subs = await self._subs(epic, cards, order, excluded)
        epoch = ap.source_event_id
        claimed = [
            s
            for s in subs
            if s.parcel is not None
            and s.parcel.autopilot_claim is not None
            and s.parcel.autopilot_claim.epoch == epoch
            and s.parcel.autopilot_claim.epic == number
            and not _bundled(s)  # the bundle builds it (a claim it keeps is released)
        ]
        pause = _pause_reason(claimed)
        if pause:
            return await self._pause(epic, pause)
        if ap.level in (AUTOPILOT_FULL, AUTOPILOT_DELAYED):
            await self._queue_builds(epic, ap, claimed)
        in_flight = [s for s in claimed if s.parcel is not None and s.parcel.stage in _IN_PROGRESS]
        claimed_numbers = {s.number for s in claimed}
        unstarted = [s for s in subs if s.number not in claimed_numbers and not _bundled(s)]
        if not order and len(unstarted) > 1 and not _linked_among(subs):
            await self._apply(epic, ev.AutopilotQuestion(key="order"))
            return await self._pause(epic, "no build order for its sub-issues")
        candidates: list[Sub] = []
        waiting: list[str] = []
        stale: list[Sub] = []
        held: list[int] = []
        build_all, _changed = await self.service.db.call(
            lambda store: store.epic_follow_up_builds(self.service.config.repo_id, number)
        )
        open_subs = {s.number for s in links.sub_issues if s.open and not s.repo}
        for sub in unstarted:
            why = await self._candidate_blocker(
                sub, open_gates & gate_blocks.get(sub.number, set())
            )
            if why != "" and not build_all and await self._held_follow_up(sub, open_subs):
                held.append(sub.number)
                continue
            if why is None:
                candidates.append(sub)
            elif why:
                waiting.append(why)
            if why != "" and _stale_blockers(sub):  # waiting (or startable) on blockers
                stale.append(sub)
        await self._reread_stale(stale)
        card = cards.get(number)
        limit = (
            card.parallel
            if card is not None and card.parallel is not None and card.parallel > 0
            else self.service.config.epic_autopilot_concurrency
        )
        started: list[int] = []
        for attempt, sub in enumerate(candidates):
            if len(in_flight) + len(started) >= limit or attempt >= MAX_STARTS_PER_PASS:
                break
            if await self._start(sub, number, epoch):
                started.append(sub.number)
        epic = await self._by_number(number) or epic
        subs = await self._subs(epic, cards, order, excluded)
        text = _status_text(
            progress,
            subs,
            epic=number,
            epoch=epoch,
            started=started,
            candidates=candidates,
            waiting=waiting,
            held=len(held),
        )
        await self._status(epic, text, "")
        return f"started {', '.join(f'#{n}' for n in started)}" if started else "nothing to start"

    async def _subs(
        self,
        epic: Parcel,
        cards: Mapping[int, RankingCard],
        order: Sequence[int],
        excluded: set[int],
    ) -> list[Sub]:
        """The epic's open sub-issues (gates and bundle issues excluded) in start order."""
        links = epic.links
        assert links is not None  # noqa: S101 - checked by the caller
        numbers = [s.number for s in links.sub_issues if s.open and not s.repo]
        subs = []
        for n in numbers:
            if n in excluded:
                continue
            position = order.index(n) if n in order else len(order)
            subs.append(Sub(n, await self._by_number(n), cards.get(n), position))
        return sorted(subs, key=Sub.key)

    async def _candidate_blocker(self, sub: Sub, gates: set[int]) -> str | None:
        """Why this sub-issue cannot be started now ("" = never by autopilot; None: it can)."""
        p = sub.parcel
        if p is None or p.links is None:
            return ""  # never read by the factory: its blockers are unknown (fail closed)
        if not p.eligible:
            return ""  # assigned to a person, closed or unverified: left alone
        if p.links.epic:
            return ""  # a nested epic only blocks; autopilot never recurses into it
        if Hold.BUNDLED in p.holds:
            return ""  # built as part of its bundle issue, never on its own
        if SKIP_LABEL in (await self.labels(p) or []):
            return ""
        blockers = [b.ref for b in _links(sub).open_blockers] + [f"#{n}" for n in sorted(gates)]
        if blockers:
            return f"#{sub.number} waits on {', '.join(dict.fromkeys(blockers))}"
        if p.stage in (Stage.BUILDING, Stage.READY, Stage.DONE):
            return ""  # the owner is building it already
        cur = p.current_session
        if p.pending_authorization_id is not None or (cur is not None and not settled(cur)):
            return ""  # a run the owner started
        return None

    async def _held_follow_up(self, sub: Sub, open_subs: set[int]) -> bool:
        """A follow-up autopilot leaves for the owner: labelled ``follow-up`` (and not
        released by the owner) and blocking no other open sub-issue of the epic."""
        p = sub.parcel
        if p is None:
            return False
        repo_id = self.service.config.repo_id
        row: FollowUpRow | None = await self.service.db.call(
            lambda store: store.follow_up(repo_id, sub.number)
        )
        if not is_held(row, await self.labels(p) if row is None else None):
            return False
        links = sub.card.links if sub.card is not None and sub.card.links is not None else p.links
        blocks = {
            b.number
            for b in (links.blocking if links is not None else ())
            if b.open and not b.repo and b.number != sub.number
        }
        return not blocks & open_subs

    async def _reread_stale(self, stale: Sequence[Sub]) -> None:
        """Re-read sub-issues whose stored blockers the board read contradicts or cannot
        confirm (a closed blocker whose close never reached them), so their stored state
        catches up; bounded per pass and per issue."""
        now = self.service.clock.now_utc_us()
        done = 0
        for sub in stale:
            if done >= MAX_REREADS_PER_PASS:
                break
            assert sub.parcel is not None  # noqa: S101 - _stale_blockers checked
            pid = sub.parcel.parcel_id
            if now - self._reread_at.get(pid, -REREAD_INTERVAL_US) < REREAD_INTERVAL_US:
                continue
            self._reread_at[pid] = now
            done += 1
            LOG.info("epic autopilot re-reading blockers issue=#%s", sub.number)
            await self.service.reread(pid)

    async def _reread_short_epic(self, epic: Parcel, links: IssueLinks) -> None:
        """Re-read an epic whose stored sub-issues are fewer than its total, so the next
        pass sees every one (at most every ``REREAD_INTERVAL_US``)."""
        now = self.service.clock.now_utc_us()
        pid = epic.parcel_id
        if now - self._reread_at.get(pid, -REREAD_INTERVAL_US) < REREAD_INTERVAL_US:
            return
        self._reread_at[pid] = now
        LOG.info(
            "epic autopilot re-reading sub-issues issue=#%s stored=%s total=%s",
            epic.issue_number,
            len(links.sub_issues),
            links.sub_total,
        )
        await self.service.reread(pid)

    async def _start(self, sub: Sub, epic: int, epoch: str) -> bool:
        assert sub.parcel is not None  # noqa: S101 - candidates have one
        parcel = sub.parcel
        snapshot = await self.snapshot(
            IssueRef(self.service.config.repo_id, sub.number, parcel.parcel_id)
        )
        if not isinstance(snapshot, IssueSnapshot):
            LOG.info("epic autopilot read failed issue=#%s reason=%s", sub.number, snapshot.reason)
            return False
        accepted = await self._apply(
            parcel, ev.AutopilotPlan(epic=epic, epoch=epoch), evidence=snapshot
        )
        if accepted:
            LOG.info(
                "epic autopilot planning issue=#%s epic=#%s source=scheduler "
                "authority=owner-epic-plan-approval",
                sub.number,
                epic,
            )
        return accepted

    async def _queue_builds(self, epic: Parcel, ap: EpicAutopilot, claimed: list[Sub]) -> None:
        """Full/Delayed: queue the build of each claimed sub-issue whose posted plan fits
        its part (each plan once; the auto-build queue starts it)."""
        delay = (
            self.service.config.epic_autopilot_delay_minutes * MICROS_PER_MINUTE
            if ap.level == AUTOPILOT_DELAYED
            else 0
        )
        for sub in claimed:
            p = sub.parcel
            claim = p.autopilot_claim if p is not None else None
            c = p.current_contract if p is not None else None
            if (
                p is None
                or claim is None
                or not claim.active
                or c is None
                or not c.published
                or p.stage != Stage.SCOPED
                or p.auto_build is not None
                or claim.marked_hash == c.full_hash
                or claim.drift_hash != c.full_hash
                or claim.drift
            ):
                continue
            snapshot = await self.snapshot(
                IssueRef(self.service.config.repo_id, sub.number, p.parcel_id)
            )
            if not isinstance(snapshot, IssueSnapshot):
                continue
            await self._apply(
                p,
                ev.AutopilotQueue(
                    epic=epic.issue_number or 0,
                    epoch=ap.source_event_id,
                    owner_id=ap.approved_by,
                    approval_event_id=ap.approval_event_id,
                    delay_us=delay,
                    show_auto_build=bool(self.service.config.auto_build_field_node_id),
                ),
                evidence=snapshot,
            )

    async def _ensure_gates(
        self, epic: Parcel, section: Mapping[str, Any]
    ) -> tuple[dict[str, int], str]:
        """Every human gate of the approved plan as a sub-issue of the epic with its
        blocked-by links (gate key -> issue number), or why that is not possible yet."""
        wanted = [
            g
            for g in section.get("human_gates") or []
            if isinstance(g, dict) and isinstance(g.get("key"), str)
        ]
        known = {g.key: g for g in epic.autopilot_gates}
        numbers = {key: gate.number for key, gate in known.items()}
        if not wanted:
            return numbers, ""
        number = epic.issue_number or 0
        ap = epic.autopilot
        assert ap is not None  # noqa: S101 - callers pass autopilot epics
        pending = [g for g in wanted if g["key"] not in known or not known[g["key"]].linked]
        if not pending:
            return numbers, ""
        if self.gates is None:
            return numbers, "its human steps cannot be created (no GitHub access)"
        try:
            found = await self.gates.find(number)
            for gate in pending:
                key = str(gate["key"])
                blocks = [n for n in gate.get("blocks") or [] if isinstance(n, int)]
                if key not in known:
                    issue = found.get(key)
                    if issue is None:
                        issue = await self.gates.create(
                            number,
                            key=key,
                            title=str(gate.get("title") or key),
                            steps=str(gate.get("steps") or ""),
                            blocks=blocks,
                            owner_id=ap.approved_by,
                        )
                        LOG.info(
                            "epic autopilot gate created epic=#%s key=%s issue=#%s",
                            number,
                            key,
                            issue,
                        )
                    await self._apply(epic, ev.AutopilotGateCreated(key=key, number=issue))
                    numbers[key] = issue
                for dep in blocks:
                    await self.gates.link(dep, numbers[key], source=number)
                await self._apply(
                    epic, ev.AutopilotGateCreated(key=key, number=numbers[key], linked=True)
                )
        except Exception as exc:  # GitHub may fail any way: pause, retry next pass
            LOG.warning("epic autopilot gates failed epic=#%s reason=%s", number, exc)
            return numbers, "its human steps could not be created yet"
        return numbers, ""

    # --------------------------------------------------------------- events

    async def _apply(
        self, parcel: Parcel, body: ev.EventBody, *, evidence: IssueSnapshot | None = None
    ) -> bool:
        now = self.service.clock.now_utc_us()
        result = await self.service.apply_event(
            Event(
                event_id=f"autopilot:{body.KIND.value}:{parcel.parcel_id}:{now}:{uuid.uuid4().hex[:12]}",
                repo_id=self.service.config.repo_id,
                parcel_id=parcel.parcel_id,
                source_time_us=now,
                provenance=(
                    Provenance.SCHEDULER
                    if isinstance(body, ev.AutopilotPlan | ev.AutopilotQueue | ev.AutopilotWithdraw)
                    else Provenance.ADAPTER
                ),
                body=body,
                issue_number=parcel.issue_number,
                evidence=evidence,
            )
        )
        if not result.accepted and not result.reason.endswith("unchanged"):
            LOG.debug(
                "epic autopilot %s refused issue=#%s reason=%s",
                body.KIND.value,
                parcel.issue_number,
                result.reason,
            )
        return bool(result.accepted)

    async def _status(self, epic: Parcel, text: str, paused: str) -> str:
        ap = epic.autopilot
        if ap is not None and (text != epic.epic_note or paused != ap.paused):
            await self._apply(epic, ev.AutopilotStatus(text=text, paused=paused))
        return paused or text

    async def _pause(self, epic: Parcel, why: str) -> str:
        return await self._status(epic, f"Autopilot paused: {why}", why)

    # --------------------------------------------------------------- operator

    async def command(self, args: Mapping[str, object]) -> dict[str, object]:
        """``epic-autopilot status|on|off`` over the operator socket."""
        action = str(args.get("action") or "status")
        config = self.service.config
        repo_id = config.repo_id
        if action in ("on", "off"):
            enabled, configured = action == "on", config.epic_autopilot
            await self.service.db.call(
                partial(
                    SqliteStore.set_epic_autopilot_override,
                    repo_id=repo_id,
                    enabled=enabled,
                    config_value=configured,
                )
            )
            LOG.info("operator epic-autopilot %s (config epic_autopilot=%s)", action, configured)
        elif action != "status":
            raise ValueError("epic-autopilot action must be status, on or off")
        return await self.status()

    async def status(self) -> dict[str, object]:
        config = self.service.config
        enabled, source = await self.enabled()
        epics: list[dict[str, object]] = []
        for epic in await self.epics():
            ap = epic.autopilot
            assert ap is not None  # noqa: S101 - selected
            epics.append(
                {
                    "issue": epic.issue_number,
                    "level": ap.level,
                    "plan_posted": ap.plan is not None and ap.plan.posted_at_us is not None,
                    "plan_approved": ap.approved,
                    "revision_pending": ap.revision_pending,
                    "paused": ap.paused or None,
                    "note": epic.epic_note,
                    "gates": {g.key: g.number for g in epic.autopilot_gates},
                }
            )
        return {
            "enabled": enabled,
            "enabled_source": source,
            "delay_minutes": config.epic_autopilot_delay_minutes,
            "concurrency": config.epic_autopilot_concurrency,
            "epics": epics,
        }


def _pause_reason(claimed: Sequence[Sub]) -> str:
    """Why autopilot must not advance (a sub-issue it drives needs the owner)."""
    for sub in claimed:
        p = sub.parcel
        claim = p.autopilot_claim if p is not None else None
        if p is None or claim is None or not claim.active:
            continue
        if p.bot == BotState.NEEDS_YOU:
            return f"#{sub.number} needs you"
        if p.bot == BotState.BLOCKED:
            return f"#{sub.number} is blocked"
        c = p.current_contract
        if claim.drift and c is not None and claim.drift_hash == c.full_hash:
            return f"#{sub.number}: {claim.drift}"
    return ""


def _bundled(sub: Sub) -> bool:
    """Held in a bundle: built by its bundle issue, never by autopilot."""
    return sub.parcel is not None and Hold.BUNDLED in sub.parcel.holds


def _links(sub: Sub) -> IssueLinks:
    """The sub-issue's links from this pass's board read when it saw every blocker,
    else its stored read (the caller checked that one exists)."""
    fresh = sub.card.links if sub.card is not None else None
    if fresh is not None:
        return fresh
    assert sub.parcel is not None and sub.parcel.links is not None  # noqa: S101
    return sub.parcel.links


def _blocker_state(links: IssueLinks) -> frozenset[tuple[str, int, bool]]:
    return frozenset((b.repo, b.number, b.open) for b in links.blocked_by)


def _stale_blockers(sub: Sub) -> bool:
    """The stored read shows an open blocker that the board read contradicts, or the
    board read could not see the blockers to confirm it."""
    stored = sub.parcel.links if sub.parcel is not None else None
    if stored is None or not stored.open_blockers:
        return False
    fresh = sub.card.links if sub.card is not None else None
    return fresh is None or _blocker_state(fresh) != _blocker_state(stored)


def _linked_among(subs: Sequence[Sub]) -> bool:
    """A native blocked-by link between two of the epic's sub-issues orders them."""
    numbers = {s.number for s in subs}
    return any(
        s.parcel is not None
        and s.parcel.links is not None
        and any(not b.repo and b.number in numbers for b in s.parcel.links.blocked_by)
        for s in subs
    )


def _status_text(
    progress: str,
    subs: Sequence[Sub],
    *,
    epic: int,
    epoch: str,
    started: Sequence[int],
    candidates: Sequence[Sub],
    waiting: Sequence[str],
    held: int = 0,
) -> str:
    """``Epic · 3/9 done · autopilot: planning #882, next #883``."""
    planning: list[int] = []
    queued: list[int] = []
    building: list[int] = []
    yours: list[int] = []
    bundled: dict[int, list[int]] = {}
    for sub in subs:
        p = sub.parcel
        if p is not None and _bundled(sub):
            bundled.setdefault(p.bundled_into, []).append(sub.number)
            continue
        claim = p.autopilot_claim if p is not None else None
        if p is None or claim is None or claim.epoch != epoch or claim.epic != epic:
            continue
        if p.stage == Stage.BUILDING:
            building.append(sub.number)
        elif p.stage in (Stage.TRIAGED, Stage.SCOPED):
            mark = p.auto_build
            if mark is not None and mark.status == AutoBuildStatus.QUEUED:
                queued.append(sub.number)
            elif not claim.active or (
                p.current_contract is not None and p.current_contract.published
            ):
                yours.append(sub.number)
            else:
                planning.append(sub.number)
    parts: list[str] = []
    for label, numbers in (
        ("planning", planning),
        ("build queued", queued),
        ("building", building),
        ("waiting for your approval", yours),
    ):
        if numbers:
            parts.append(f"{label} {', '.join(f'#{n}' for n in numbers)}")
    nxt = [s.number for s in candidates if s.number not in started]
    if nxt:
        parts.append(f"next #{nxt[0]}")
    elif not parts and waiting:
        parts.append(waiting[0])
    for bundle, numbers in sorted(bundled.items()):
        parts.append(f"bundle #{bundle} builds {', '.join(f'#{n}' for n in numbers)}")
    if held:
        parts.append(f"{held} follow-up{'s' if held != 1 else ''} held")
    if not subs:
        parts.append("every sub-issue done")
    return f"{progress} · autopilot: {', '.join(parts) or 'nothing to start'}"


__all__ = ["EpicAutopilotService", "GateWriter", "Sub"]
