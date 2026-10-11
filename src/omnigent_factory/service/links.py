"""GitHub-native issue links: write the ones triage reports, keep epics' cards current,
and re-read issues whose links changed.

* **Writes.** Once a triage comment exists, every ``depends_on``/``blocks`` entry of the
  triage (and every ``links`` entry of an epic triage) becomes a native blocked-by link
  (``addBlockedBy``): only between two issues of the configured repository, skipped when
  it already exists, never removing one. Each is logged. Best effort, in the background
  like related-issue marking; a failed write only logs.
* **Epics.** An issue with at least one sub-issue is an epic. Each board diff pass
  computes every epic's card note, ``Epic · 1/8 done · next: #823`` (next: the
  best-ranked open sub-issue on the board with no open blocker), applied as an
  ``EpicProgress`` event only when it changes. An epic also gets GitHub's "Epic" issue
  type when the repository has one and the issue has no type (a type the owner set is
  never changed; that is logged once).
* **Re-reads.** A ``sub_issues`` or ``issue_dependencies`` webhook, or a closed/reopened
  issue that blocks (or is a sub-issue of) a known issue, re-reads each affected issue of
  the repository the factory knows, so its links (and an auto-build waiting on a blocker)
  update without waiting for the board diff.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from functools import partial
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import EffectIntent, RetryableReadFailure
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import Parcel
from omnigent_factory.github.client import GitHubAPIError, GitHubClient
from omnigent_factory.github.links import (
    add_blocked_by,
    create_sub_issue,
    epic_gate_issues,
    epic_issue_type,
    gate_marker,
    read_target,
    set_issue_type,
    user_node_id,
)
from omnigent_factory.ports.github import RankingCard
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore

LOG = logging.getLogger(__name__)

#: Webhook events that report a native link change (GitHub docs, "Webhook events and
#: payloads": ``sub_issues`` and ``issue_dependencies``, Issues read permission).
LINK_EVENTS = frozenset({"sub_issues", "issue_dependencies"})
#: The payload members of each that name an affected issue.
_LINK_EVENT_ISSUES: Mapping[str, tuple[str, ...]] = {
    "sub_issues": ("parent_issue", "sub_issue"),
    "issue_dependencies": ("blocked_issue", "blocking_issue"),
}

ResultSource = Callable[[str], dict[str, Any] | None]
CardReader = Callable[[], Awaitable[list[RankingCard] | RetryableReadFailure]]


def requested_links(result: Mapping[str, Any] | None, issue: int) -> list[tuple[int, int]]:
    """(blocked, blocking) pairs a stored stage result asks for: the ``depends_on`` and
    ``blocks`` entries of any result's ``related`` (triage, plan, build, blocked, epic
    triage), and an epic triage's ``links``."""
    body = result.get("result") if result is not None else None
    if not isinstance(body, dict):
        return []
    pairs: list[tuple[int, int]] = []
    if isinstance(body.get("related"), list):
        for item in body.get("related") or []:
            other = item.get("issue") if isinstance(item, dict) else None
            if not isinstance(other, int) or isinstance(other, bool) or other == issue:
                continue
            if item.get("relation") == "depends_on":
                pairs.append((issue, other))
            elif item.get("relation") == "blocks":
                pairs.append((other, issue))
    if body.get("kind") == "epic_triage":
        for item in body.get("links") or []:
            if not isinstance(item, dict):
                continue
            blocked, blocking = item.get("issue"), item.get("blocked_by")
            if (
                isinstance(blocked, int)
                and isinstance(blocking, int)
                and not isinstance(blocked, bool)
                and not isinstance(blocking, bool)
                and blocked != blocking
            ):
                pairs.append((blocked, blocking))
    return list(dict.fromkeys(pairs))


def epic_note(epic: RankingCard, cards: Iterable[RankingCard]) -> str:
    """``Epic · 1/8 done · next: #823`` for an epic card ("" when not an epic)."""
    links = epic.links
    if links is None or not links.epic:
        return ""
    text = f"Epic · {links.sub_completed}/{links.sub_total} done"
    children = [
        c
        for c in cards
        if c.links is not None
        and c.links.parent is not None
        and not c.links.parent.repo
        and c.links.parent.number == epic.number
        and not c.links.open_blockers
    ]
    if children:
        best = min(
            children,
            key=lambda c: (c.rank is None, c.rank if c.rank is not None else 0.0, c.number),
        )
        text += f" · next: #{best.number}"
    return text


class NativeLinks:
    def __init__(
        self,
        service: FactoryService,
        client: GitHubClient,
        results: ResultSource,
        cards: CardReader,
    ) -> None:
        self.service = service
        self.client = client
        self.results = results
        self.cards = cards
        self._tasks: set[asyncio.Task[int]] = set()
        #: The repository's "Epic" issue type: unknown (not looked up), None or its ID.
        self._epic_type: str | None = None
        self._epic_type_known = False
        #: Epics whose issue type was set or found set (once per process).
        self._typed: set[int] = set()

    @property
    def _owner_name(self) -> tuple[str, str]:
        owner, _, name = self.service.config.repository.partition("/")
        return owner, name

    # ------------------------------------------------------------------ writes

    def schedule(self, effect: EffectIntent) -> None:
        """Called by the GitHub adapter once a triage comment exists (created or adopted)."""
        task = asyncio.create_task(self.write(effect), name="native-links")
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[int]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            LOG.warning("native link writing failed")

    def schedule_result(self, issue: int, result: Mapping[str, Any]) -> None:
        """Called once a stage result is accepted (any kind): create the native links its
        ``related`` (and an epic triage's ``links``) report, in the background."""
        pairs = requested_links({"result": dict(result)}, issue)
        if not pairs:
            return
        task = asyncio.create_task(self._write_pairs(pairs, issue), name="native-links")
        self._tasks.add(task)
        task.add_done_callback(self._done)

    async def _write_pairs(self, pairs: list[tuple[int, int]], source: int) -> int:
        created = 0
        for blocked, blocking in pairs:
            created += int(await self.link(blocked, blocking, source=source))
        return created

    async def write(self, effect: EffectIntent) -> int:
        """Create the blocked-by links the published triage reports; returns how many."""
        session_id = str(effect.args.get("session_id") or effect.preconditions.session_id or "")
        if effect.parcel_id is None or not session_id:
            return 0
        parcel_id = effect.parcel_id
        source = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        if source is None or source.issue_number is None:
            return 0
        stored = self.results(session_id)
        record = stored.get("factory_result") if stored is not None else None
        pairs = requested_links(record if isinstance(record, dict) else None, source.issue_number)
        created = 0
        for blocked, blocking in pairs:
            created += int(await self.link(blocked, blocking, source=source.issue_number))
        return created

    async def link(self, blocked: int, blocking: int, *, source: int) -> bool:
        """``blocked`` is blocked by ``blocking`` (both in the configured repository)."""
        owner, name = self._owner_name
        repo_node = self.service.config.repo_id
        try:
            target = await read_target(self.client, owner, name, blocked, repo_node)
            other = await read_target(self.client, owner, name, blocking, repo_node)
            if target is None or other is None:
                LOG.info(
                    "native link skipped (not an issue of this repository) source=#%s "
                    "issue=#%s blocked_by=#%s",
                    source,
                    blocked,
                    blocking,
                )
                return False
            if blocking in target.blocked_by:
                LOG.info(
                    "native link exists source=#%s issue=#%s blocked_by=#%s",
                    source,
                    blocked,
                    blocking,
                )
                return False
            await add_blocked_by(self.client, target.node_id, other.node_id)
        except GitHubAPIError as exc:
            LOG.warning(
                "native link failed source=#%s issue=#%s blocked_by=#%s reason=%s",
                source,
                blocked,
                blocking,
                exc,
            )
            return False
        LOG.info(
            "native link created source=#%s issue=#%s blocked_by=#%s", source, blocked, blocking
        )
        return True

    # ------------------------------------------------------------------ epics

    async def epic_pass(self) -> int:
        """Bring every epic's card note (and issue type) up to date; returns notes applied."""
        found = await self.cards()
        if isinstance(found, RetryableReadFailure):
            LOG.info("epic pass skipped: board unreadable reason=%s", found.reason)
            return 0
        cards = list(found)
        repo_id = self.service.config.repo_id
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND "
                "(json_extract(aggregate_json, '$.parcel.epic_note') != '' OR "
                "parcel_id IN (SELECT value FROM json_each(?)))",
                (repo_id, json.dumps([c.node_id for c in cards])),
            )
        )
        parcels = {p.parcel_id: p for p in (parcel_from_json(str(r[0])) for r in rows)}
        by_node = {c.node_id: c for c in cards}
        applied = 0
        for parcel in parcels.values():
            if parcel.autopilot is not None:
                continue  # epic autopilot keeps this epic's note (``service.autopilot``)
            card = by_node.get(parcel.parcel_id)
            if card is None and parcel.epic_note and parcel.issue_number is not None:
                continue  # off the board or closed: the note goes with the card
            note = epic_note(card, cards) if card is not None else ""
            if note != parcel.epic_note:
                applied += int(await self._apply_note(parcel, note))
        for card in cards:
            if card.links is not None and card.links.epic:
                await self._type_epic(card.number)
        return applied

    async def _apply_note(self, parcel: Parcel, note: str) -> bool:
        now = self.service.clock.now_utc_us()
        result = await self.service.apply_event(
            Event(
                event_id=f"epic-progress:{parcel.parcel_id}:{now}",
                repo_id=self.service.config.repo_id,
                parcel_id=parcel.parcel_id,
                source_time_us=now,
                provenance=Provenance.ADAPTER,
                body=ev.EpicProgress(text=note),
                issue_number=parcel.issue_number,
            )
        )
        if result.accepted:
            LOG.info("epic note issue=#%s note=%r", parcel.issue_number, note)
        return bool(result.accepted)

    async def _type_epic(self, number: int) -> None:
        """Give an epic GitHub's "Epic" issue type, only when it has none (once)."""
        if number in self._typed:
            return
        owner, name = self._owner_name
        if not self._epic_type_known:
            self._epic_type = await epic_issue_type(self.client, owner, name)
            self._epic_type_known = True
            if self._epic_type is None:
                LOG.info("no 'Epic' issue type in %s: epics are not typed", owner)
        if self._epic_type is None:
            return
        self._typed.add(number)
        try:
            target = await read_target(
                self.client, owner, name, number, self.service.config.repo_id
            )
            if target is None:
                return
            if target.issue_type is not None:
                if target.issue_type.lower() != "epic":
                    LOG.info(
                        "epic issue type not set issue=#%s: the owner set type %r",
                        number,
                        target.issue_type,
                    )
                return
            await set_issue_type(self.client, target.node_id, self._epic_type)
        except GitHubAPIError as exc:
            LOG.warning("epic issue type failed issue=#%s reason=%s", number, exc)
            return
        LOG.info("epic issue type set issue=#%s", number)

    # ------------------------------------------------------------------ re-reads

    async def on_link_delivery(self, event_name: str, body: bytes | str) -> int:
        """A ``sub_issues``/``issue_dependencies`` webhook: re-read each known issue of
        this repository it names; returns how many."""
        try:
            payload = json.loads(body)
        except ValueError:
            return 0
        if not isinstance(payload, dict):
            return 0
        repo_name = self.service.config.repository.lower()
        node_ids: list[str] = []
        for key in _LINK_EVENT_ISSUES.get(event_name, ()):
            issue = payload.get(key)
            if not isinstance(issue, dict):
                continue
            url = str(issue.get("repository_url") or "").lower()
            node = issue.get("node_id")
            if url.endswith(f"/repos/{repo_name}") and isinstance(node, str):
                node_ids.append(node)
        return await self._reread(node_ids)

    async def on_issue_state(self, parcel_id: str) -> int:
        """An issue closed or reopened: re-read the known issues it blocks and its parent
        (their blockers or sub-issue progress changed)."""
        parcel = await self.service.db.call(lambda store: store.load_parcel(parcel_id))
        links = parcel.links if parcel is not None else None
        if links is None:
            return 0
        numbers = [b.number for b in links.blocking if not b.repo]
        if links.parent is not None and not links.parent.repo:
            numbers.append(links.parent.number)
        if not numbers:
            return 0
        repo_id = self.service.config.repo_id
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? AND "
                "issue_number IN (SELECT value FROM json_each(?))",
                (repo_id, json.dumps(numbers)),
            )
        )
        return await self._reread([str(r[0]) for r in rows])

    async def _reread(self, node_ids: Iterable[str]) -> int:
        count = 0
        for node_id in dict.fromkeys(node_ids):
            parcel = await self.service.db.call(partial(_load, parcel_id=node_id))
            if parcel is None:
                continue
            await self.service.reread(parcel.parcel_id)
            count += 1
        if count:
            LOG.info("native links changed: re-read issues=%s", count)
        return count


class GitHubGates:
    """Epic autopilot's human gates on GitHub (``service.autopilot.GateWriter``): a gate
    is a sub-issue of the epic assigned to the owner, created in one mutation and found
    again by its hidden marker, so a retry or restart never creates a second one; its
    blocked-by links go through :meth:`NativeLinks.link` (skipped when they exist)."""

    def __init__(self, links: NativeLinks, publish: Callable[[str], str]) -> None:
        self.links = links
        #: Makes agent text safe to post as the bot (mentions, HTML comments, credentials).
        self.publish = publish

    async def find(self, epic: int) -> dict[str, int]:
        owner, name = self.links._owner_name
        return await epic_gate_issues(self.links.client, owner, name, epic)

    async def create(
        self,
        epic: int,
        *,
        key: str,
        title: str,
        steps: str,
        blocks: Iterable[int],
        owner_id: int,
    ) -> int:
        owner, name = self.links._owner_name
        client = self.links.client
        repo_node = self.links.service.config.repo_id
        target = await read_target(client, owner, name, epic, repo_node)
        if target is None:
            raise GitHubAPIError(f"epic #{epic} is not an issue of this repository")
        before = ", ".join(f"#{n}" for n in blocks)
        body = (
            f"{self.publish(steps)}\n\n"
            f"This is a step of epic #{epic} the factory can't take itself"
            + (f"; autopilot goes on with {before} once this issue is closed." if before else ".")
            + f"\n\n{gate_marker(epic, key)}"
        )
        return await create_sub_issue(
            client,
            repository_node_id=repo_node,
            parent_node_id=target.node_id,
            title=" ".join(title.split())[:120] or key,
            body=body,
            assignee_node_id=await user_node_id(client, owner_id),
        )

    async def link(self, blocked: int, blocking: int, *, source: int) -> bool:
        return await self.links.link(blocked, blocking, source=source)


def _load(store: SqliteStore, *, parcel_id: str) -> Parcel | None:
    return store.load_parcel(parcel_id)


__all__ = ["LINK_EVENTS", "GitHubGates", "NativeLinks", "epic_note", "requested_links"]
