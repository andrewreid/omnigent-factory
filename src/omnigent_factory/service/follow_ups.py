"""Follow-ups: issues the agent files during a run join that issue's epic, carry the
dependencies it names, and wait for the owner before epic autopilot builds them.

* **Reported.** A stage result (``build_ready`` or ``blocked``) lists every issue the run
  filed in ``follow_ups`` (``[{issue, blocks}]``). Each is recorded (``follow_ups`` table)
  and linked in the background, idempotently, adding only: every ``blocks`` issue M gets
  the native link "M blocked by F"; F gets the repository label ``follow-up`` (created when
  absent); and when the source issue S has a parent epic and F has none, F becomes a
  sub-issue of that epic. Each action is done once and logged; one that failed is retried
  after board diffs and at start, backing off per issue (1 minute doubling to 1 hour). A
  parent GitHub refuses for good (a permanent GraphQL error, e.g. an epic already at its
  sub-issue limit) is given up: logged once with GitHub's message and recorded
  (``gave-up:parent``), never retried. An owner's later undo (label removed, parent or
  link changed) is never redone.
* **Detected (backstop).** An issue opened by the factory bot (the agent's gh wrapper runs
  as the bot) while some issue's run is open, whose body names that issue, is recorded as
  its follow-up when not already reported: label and parent as above, never ``blocks``.
  Excluded: an issue whose body carries a factory marker (``<!-- factory-``: epic
  autopilot's human gates, and anything else the factory writes itself), a known human
  gate of an epic, an issue opened by anyone else, and a body naming more than one issue
  with an open run (ambiguous: left alone, logged).
* **Holds.** Epic autopilot leaves a sub-issue labelled ``follow-up`` alone unless it
  natively blocks another open sub-issue of the same epic (``service.autopilot``). The
  owner releases one issue by removing its ``follow-up`` label (a removal by anyone else
  changes nothing), or all follow-ups of an epic by adding ``factory:build-follow-ups`` to
  the epic (removing it restores the default; only owner changes count). Label webhooks
  apply these at once; a lost one is recovered from the issue's label timeline when the
  board diff sees the card change (as ``service.label_recovery`` does for ``factory:*``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from datetime import datetime
from functools import partial
from typing import Any, TypeVar

from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.predicates import run_closed
from omnigent_factory.core.types import MICROS_PER_SECOND, Parcel
from omnigent_factory.github.client import GitHubAPIError, GraphQLErrors
from omnigent_factory.github.links import (
    LinkTarget,
    add_label,
    add_sub_issue,
    ensure_label,
    read_target,
)
from omnigent_factory.github.webhook import label_name
from omnigent_factory.ports.github import IssueRef, LabelEvent
from omnigent_factory.service.links import NativeLinks
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import FollowUpRow, SqliteStore

LOG = logging.getLogger(__name__)
T = TypeVar("T")

FOLLOW_UP_LABEL = "follow-up"
BUILD_FOLLOW_UPS_LABEL = "factory:build-follow-ups"
_LABEL_DESCRIPTION = "Filed by the factory during another issue's run"
#: Retry delay after a failed link (doubling per failure up to the cap), in seconds.
RETRY_BASE_SECONDS = 60.0
RETRY_MAX_SECONDS = 3600.0
#: Done-set marker of an action given up for good (its action is marked done too).
GAVE_UP = "gave-up:"
#: Hidden markers the factory writes into issues it creates itself (human gates, ...).
FACTORY_MARKER = "<!-- factory-"

ReadLabelEvents = Callable[[IssueRef], Awaitable[tuple[LabelEvent, ...] | RetryableReadFailure]]


def reported_follow_ups(result: Mapping[str, Any] | None) -> list[tuple[int, tuple[int, ...]]]:
    """(issue, blocks) per ``follow_ups`` entry of a stage result body."""
    items = result.get("follow_ups") if isinstance(result, Mapping) else None
    found: list[tuple[int, tuple[int, ...]]] = []
    for item in items if isinstance(items, list) else []:
        number = item.get("issue") if isinstance(item, dict) else None
        if not isinstance(number, int) or isinstance(number, bool) or number < 1:
            continue
        blocks = item.get("blocks") or []
        found.append(
            (
                number,
                tuple(
                    n
                    for n in blocks
                    if isinstance(n, int) and not isinstance(n, bool) and n >= 1 and n != number
                ),
            )
        )
    return found


def referenced_issues(body: str, repository: str) -> set[int]:
    """Issue numbers a body names: ``#123`` or a link to the repository's issue 123."""
    numbers = {int(n) for n in re.findall(r"(?<![\w/&#])#(\d+)\b", body)}
    pattern = rf"github\.com/{re.escape(repository)}/(?:issues|pull)/(\d+)\b"
    numbers |= {int(n) for n in re.findall(pattern, body, flags=re.IGNORECASE)}
    return numbers


def follow_up_hold(events: Iterable[LabelEvent], owners: frozenset[int]) -> LabelEvent | None:
    """The label event that decides the ``follow-up`` hold: the newest ``labeled`` event
    (anyone) or owner's ``unlabeled`` event; another actor's removal never counts."""
    found: LabelEvent | None = None
    for event in sorted(events, key=lambda e: e.created_at_us):
        if event.label != FOLLOW_UP_LABEL:
            continue
        if event.labeled or event.actor_id in owners:
            found = event
    return found


def epic_override(events: Iterable[LabelEvent], owners: frozenset[int]) -> LabelEvent | None:
    """The owner's newest ``factory:build-follow-ups`` event (anyone else's is ignored)."""
    found: LabelEvent | None = None
    for event in sorted(events, key=lambda e: e.created_at_us):
        if event.label == BUILD_FOLLOW_UPS_LABEL and event.actor_id in owners:
            found = event
    return found


def is_held(row: FollowUpRow | None, labels: Sequence[str] | None) -> bool:
    """Whether autopilot holds the issue: its stored hold, else its current label."""
    if row is not None:
        return row.hold == "held"
    return FOLLOW_UP_LABEL in (labels or ())


def _truncated(labels: Sequence[str]) -> bool:
    """A board card listing fewer labels than it has (``#<total>`` marker)."""
    read = [name for name in labels if not name.startswith("#")]
    totals = [name[1:] for name in labels if name.startswith("#") and name[1:].isdigit()]
    return bool(totals) and int(totals[-1]) > len(read)


class FollowUps:
    def __init__(
        self,
        service: FactoryService,
        links: NativeLinks,
        read_events: ReadLabelEvents,
    ) -> None:
        self.service = service
        #: Its GitHub client and blocked-by writes.
        self.links = links
        self.read_events = read_events
        self._tasks: set[asyncio.Task[object]] = set()
        self._locks: dict[int, asyncio.Lock] = {}
        #: Card labels last checked per parcel (the same labels need no second read).
        self._settled: dict[str, tuple[str, ...]] = {}
        #: Issue -> (loop time of its next retry, failures in a row) after a failed link.
        self._backoff: dict[int, tuple[float, int]] = {}

    @property
    def _repo(self) -> str:
        return self.service.config.repository

    async def _db(self, fn: Callable[[SqliteStore], T]) -> T:
        return await self.service.db.call(fn)

    def _spawn(self, coro: Awaitable[object], name: str) -> None:
        task: asyncio.Task[object] = asyncio.ensure_future(coro)
        task.set_name(name)
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            LOG.warning("follow-up linking failed", exc_info=task.exception())

    async def drain(self) -> None:
        """Wait for the background work started so far (tests, shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ------------------------------------------------------------------ reported

    def schedule_result(self, issue: int, result: Mapping[str, Any]) -> None:
        """Called once a stage result is accepted: record and link its ``follow_ups``."""
        if reported_follow_ups(result):
            self._spawn(self.record_reported(issue, result), "follow-ups")

    async def record_reported(self, source: int, result: Mapping[str, Any]) -> list[int]:
        """Record the result's follow-ups of ``source`` and link them; returns their numbers."""
        repo_id = self.service.config.repo_id
        numbers: list[int] = []
        for number, blocks in reported_follow_ups(result):
            if number == source:
                continue
            await self._db(
                partial(
                    SqliteStore.record_follow_up,
                    repo_id=repo_id,
                    issue_number=number,
                    source_issue=source,
                    origin="reported",
                    blocks=blocks,
                )
            )
            LOG.info(
                "follow-up reported issue=#%s source=#%s blocks=%s",
                number,
                source,
                ",".join(f"#{n}" for n in blocks) or "-",
            )
            numbers.append(number)
        for number in numbers:
            await self.link(number)
        return numbers

    async def retry_pending(self) -> int:
        """Link every follow-up with an action still to do; returns how many are complete."""
        repo_id = self.service.config.repo_id
        rows: list[FollowUpRow] = await self._db(lambda store: store.follow_ups(repo_id))
        complete = 0
        now = asyncio.get_running_loop().time()
        for row in rows:
            due = self._backoff.get(row.issue_number)
            if row.pending() and (due is None or due[0] <= now):
                complete += int(await self.link(row.issue_number))
        return complete

    def _after(self, number: int, ok: bool) -> bool:
        """Note a link attempt's outcome for :meth:`retry_pending`'s backoff."""
        if ok:
            self._backoff.pop(number, None)
            return True
        failures = self._backoff.get(number, (0.0, 0))[1] + 1
        delay = min(RETRY_BASE_SECONDS * 2 ** (failures - 1), RETRY_MAX_SECONDS)
        self._backoff[number] = (asyncio.get_running_loop().time() + delay, failures)
        return False

    async def link(self, number: int) -> bool:
        """Do the follow-up's pending link actions (blocks, label, parent), each once.
        True when none is left."""
        lock = self._locks.setdefault(number, asyncio.Lock())
        async with lock:
            repo_id = self.service.config.repo_id
            row: FollowUpRow | None = await self._db(lambda store: store.follow_up(repo_id, number))
            if row is None or not row.pending() or row.source_issue is None:
                return True
            source = row.source_issue
            owner, name = self._repo.partition("/")[::2]
            client = self.links.client
            try:
                target = await read_target(client, owner, name, number, repo_id)
            except GitHubAPIError as exc:
                LOG.warning("follow-up read failed issue=#%s reason=%s", number, exc)
                return self._after(number, False)
            left = 0
            for action in row.pending():
                if target is None:
                    LOG.info(
                        "follow-up skipped (not an issue of this repository) issue=#%s "
                        "source=#%s action=%s",
                        number,
                        source,
                        action,
                    )
                    ok = True
                elif action == "label":
                    ok = await self._label(row, target)
                elif action == "parent":
                    ok = await self._parent(row, target, source)
                else:
                    blocked = int(action.partition(":")[2])
                    status = await self.links.link_status(blocked, number, source=source)
                    ok = status != "failed"
                if ok:
                    await self._db(
                        partial(
                            SqliteStore.mark_follow_up_done,
                            repo_id=repo_id,
                            issue_number=number,
                            action=action,
                        )
                    )
                else:
                    left += 1
            return self._after(number, left == 0)

    async def _label(self, row: FollowUpRow, target: LinkTarget) -> bool:
        number = row.issue_number
        if FOLLOW_UP_LABEL in target.labels:
            LOG.info("follow-up label exists issue=#%s", number)
            return True
        if row.hold == "released":
            LOG.info("follow-up label not added: an owner removed it issue=#%s", number)
            return True
        try:
            if await ensure_label(
                self.links.client, self._repo, FOLLOW_UP_LABEL, _LABEL_DESCRIPTION
            ):
                LOG.info("repository label created label=%s", FOLLOW_UP_LABEL)
            await add_label(self.links.client, self._repo, number, FOLLOW_UP_LABEL)
        except GitHubAPIError as exc:
            LOG.warning("follow-up label failed issue=#%s reason=%s", number, exc)
            return False
        LOG.info("follow-up labelled issue=#%s source=#%s", number, row.source_issue)
        return True

    async def _parent(self, row: FollowUpRow, target: LinkTarget, source: int) -> bool:
        number = row.issue_number
        if target.has_parent:
            LOG.info("follow-up parent kept issue=#%s parent=#%s", number, target.parent or "?")
            return True
        owner, name = self._repo.partition("/")[::2]
        client = self.links.client
        repo_id = self.service.config.repo_id
        epic_number: int | None = None
        try:
            origin = await read_target(client, owner, name, source, repo_id)
            if origin is None or origin.parent is None:
                LOG.info(
                    "follow-up parent: source has no epic issue=#%s source=#%s", number, source
                )
                return True
            epic_number = origin.parent
            epic = await read_target(client, owner, name, epic_number, repo_id)
            if epic is None:
                return True
            await add_sub_issue(client, epic.node_id, target.node_id)
        except GraphQLErrors as exc:
            if not exc.permanent:
                LOG.warning("follow-up parent failed issue=#%s reason=%s", number, exc)
                return False
            LOG.warning(
                "follow-up parent given up issue=#%s epic=#%s source=#%s reason=%s",
                number,
                epic_number or "?",
                source,
                exc,
            )
            await self._db(
                partial(
                    SqliteStore.mark_follow_up_done,
                    repo_id=repo_id,
                    issue_number=number,
                    action=f"{GAVE_UP}parent",
                )
            )
            return True
        except GitHubAPIError as exc:
            LOG.warning("follow-up parent failed issue=#%s reason=%s", number, exc)
            return False
        LOG.info(
            "follow-up added to epic issue=#%s epic=#%s source=#%s", number, origin.parent, source
        )
        return True

    # ------------------------------------------------------------------ webhooks

    async def on_issues_delivery(self, body: bytes | str) -> None:
        """An authenticated ``issues`` delivery: an opened issue (backstop) or a change of
        ``follow-up`` / ``factory:build-follow-ups``."""
        try:
            payload = json.loads(body)
        except ValueError:
            return
        if not isinstance(payload, dict):
            return
        action = payload.get("action")
        if action == "opened":
            await self.detect(payload)
        elif action in ("labeled", "unlabeled"):
            await self.on_label(payload)

    async def on_label(self, payload: Mapping[str, Any]) -> None:
        label = label_name(payload)
        if label not in (FOLLOW_UP_LABEL, BUILD_FOLLOW_UPS_LABEL):
            return
        issue = payload.get("issue")
        number = issue.get("number") if isinstance(issue, dict) else None
        sender = payload.get("sender")
        actor = sender.get("id") if isinstance(sender, dict) else None
        if not isinstance(number, int) or not isinstance(issue, dict):
            return
        labeled = payload.get("action") == "labeled"
        at_us = _time_us(issue.get("updated_at")) or self.service.clock.now_utc_us()
        owner = isinstance(actor, int) and actor in self.service.config.owners
        await self._apply_label(
            number, label, labeled=labeled, owner=owner, at_us=at_us, actor=actor
        )

    async def _apply_label(
        self, number: int, label: str, *, labeled: bool, owner: bool, at_us: int, actor: object
    ) -> None:
        repo_id = self.service.config.repo_id
        verb = "added" if labeled else "removed"
        if label == FOLLOW_UP_LABEL:
            if not labeled and not owner:
                LOG.info(
                    "follow-up label removal ignored: not an owner issue=#%s actor=%s",
                    number,
                    actor,
                )
                return
            hold = "held" if labeled else "released"
            changed = await self._db(
                partial(
                    SqliteStore.set_follow_up_hold,
                    repo_id=repo_id,
                    issue_number=number,
                    hold=hold,
                    at_us=at_us,
                )
            )
            if changed:
                LOG.info("follow-up %s issue=#%s (label %s by %s)", hold, number, verb, actor)
            return
        if not owner:
            LOG.info("%s change ignored: not an owner issue=#%s actor=%s", label, number, actor)
            return
        changed = await self._db(
            partial(
                SqliteStore.set_epic_follow_up_builds,
                repo_id=repo_id,
                epic_number=number,
                enabled=labeled,
                at_us=at_us,
            )
        )
        if changed:
            LOG.info(
                "epic follow-ups %s epic=#%s (owner %s)",
                "built by autopilot" if labeled else "held again",
                number,
                actor,
            )

    # ------------------------------------------------------------------ backstop

    async def detect(self, payload: Mapping[str, Any]) -> int | None:
        """An opened issue as a follow-up of the one issue with an open run its body names
        (None: not one). Records it and links label and parent in the background."""
        issue = payload.get("issue")
        sender = payload.get("sender")
        if not isinstance(issue, dict) or "pull_request" in issue:
            return None
        number = issue.get("number")
        if not isinstance(number, int):
            return None
        config = self.service.config
        login = sender.get("login") if isinstance(sender, dict) else None
        sender_id = sender.get("id") if isinstance(sender, dict) else None
        if login != config.github_bot_login and sender_id != config.github_bot_user_id:
            return None
        body = str(issue.get("body") or "")
        if FACTORY_MARKER in body:
            LOG.info("opened issue not a follow-up: factory marker issue=#%s", number)
            return None
        repo_id = config.repo_id
        row: FollowUpRow | None = await self._db(lambda store: store.follow_up(repo_id, number))
        if row is not None and row.origin in ("reported", "detected"):
            return None
        parcels = await self._parcels()
        gates = {g.number for p in parcels for g in p.autopilot_gates}
        if number in gates:
            LOG.info("opened issue not a follow-up: human gate issue=#%s", number)
            return None
        named = referenced_issues(body, config.repository) - {number}
        running = sorted(
            p.issue_number
            for p in parcels
            if p.issue_number in named and _run_open(p) and p.issue_number is not None
        )
        if len(running) != 1:
            if len(running) > 1:
                LOG.info(
                    "opened issue not a follow-up: names several running issues issue=#%s "
                    "running=%s",
                    number,
                    ",".join(f"#{n}" for n in running),
                )
            return None
        source = running[0]
        await self._db(
            partial(
                SqliteStore.record_follow_up,
                repo_id=repo_id,
                issue_number=number,
                source_issue=source,
                origin="detected",
            )
        )
        LOG.info("follow-up detected issue=#%s source=#%s (opened by the bot)", number, source)
        self._spawn(self.link(number), "follow-ups")
        return source

    async def _parcels(self) -> list[Parcel]:
        repo_id = self.service.config.repo_id
        rows = await self._db(
            lambda store: store.query(
                "SELECT aggregate_json FROM parcels WHERE repo_id = ?", (repo_id,)
            )
        )
        return [parcel_from_json(str(r[0])) for r in rows]

    # ------------------------------------------------------------------ recovery

    async def check_card(self, parcel_id: str, labels: Sequence[str]) -> bool:
        """A board card changed: when its labels disagree with the stored hold or epic
        override, recover the owner's label changes from the issue's timeline. False only
        when that read failed (the board diff then checks the card again)."""
        names = tuple(labels)
        if self._settled.get(parcel_id) == names:
            return True
        parcel = await self._db(lambda store: store.load_parcel(parcel_id))
        if parcel is None or parcel.issue_number is None:
            return True
        number = parcel.issue_number
        repo_id = self.service.config.repo_id
        row: FollowUpRow | None = await self._db(lambda store: store.follow_up(repo_id, number))
        enabled, _at = await self._db(lambda store: store.epic_follow_up_builds(repo_id, number))
        unsure = _truncated(names)
        has_label = FOLLOW_UP_LABEL in names
        issue_check = (row is None and has_label) or (
            row is not None and (unsure or has_label != (row.hold == "held"))
        )
        epic_check = (BUILD_FOLLOW_UPS_LABEL in names) != enabled or (unsure and enabled)
        if issue_check or epic_check:
            ok = await self.refresh(IssueRef(repo_id, number, parcel_id))
            if not ok:
                return False
        self._settled[parcel_id] = names
        return True

    async def refresh(self, ref: IssueRef) -> bool:
        """Apply the hold and epic override the issue's label timeline shows."""
        events = await self.read_events(ref)
        if isinstance(events, RetryableReadFailure):
            LOG.warning(
                "follow-up label read failed issue=#%s reason=%s", ref.issue_number, events.reason
            )
            return False
        owners = self.service.config.owners
        hold = follow_up_hold(events, owners)
        if hold is not None:
            await self._apply_label(
                ref.issue_number,
                FOLLOW_UP_LABEL,
                labeled=hold.labeled,
                owner=hold.actor_id in owners,
                at_us=hold.created_at_us,
                actor=hold.actor_id,
            )
        override = epic_override(events, owners)
        if override is not None:
            await self._apply_label(
                ref.issue_number,
                BUILD_FOLLOW_UPS_LABEL,
                labeled=override.labeled,
                owner=True,
                at_us=override.created_at_us,
                actor=override.actor_id,
            )
        return True


def _run_open(parcel: Parcel) -> bool:
    run = parcel.current_session
    return run is not None and not run_closed(parcel, run)


def _time_us(value: object) -> int:
    if not isinstance(value, str):
        return 0
    try:
        return int(
            datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * MICROS_PER_SECOND
        )
    except ValueError:
        return 0


__all__ = [
    "BUILD_FOLLOW_UPS_LABEL",
    "FOLLOW_UP_LABEL",
    "GAVE_UP",
    "FollowUps",
    "epic_override",
    "follow_up_hold",
    "is_held",
    "referenced_issues",
    "reported_follow_ups",
]
