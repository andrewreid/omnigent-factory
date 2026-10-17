"""Owner-directed splits: work the owner said to file separately ("do these bits, file the
rest as an issue"), filed by the factory, one issue per item.

* **Reported.** A triage, epic triage, plan, build_ready or blocked result lists the
  pieces in ``split_out`` (title, body, reason, ``relation`` to the source issue: ``none``,
  ``blocks`` (the new issue blocks it) or ``blocked_by``, and ``blocks``). Agents never
  open issues themselves (the shell policy denies it); this field is the way.
* **Filed.** Each item becomes its own issue (separate work, never consolidated): the
  title as given, the body with a provenance line ("Split out of #N at the owner's
  direction") and a hidden marker. It lands in the Inbox like any new issue, with NO
  ``follow-up`` label and no follow-up record, so it is not held by epic autopilot and
  not in follow-up lineage: its own build may defer follow-ups. Items are keyed by source
  issue and title, so a re-submission files nothing twice; the row is ``creating`` before
  the create, and an unclear create is looked up by its marker before any retry.
* **Linked.** When the source issue is a sub-issue of an epic, the new issue joins that
  epic (unless it already has a parent); ``relation`` and ``blocks`` become native
  blocked-by links. Each link step is done once, logged and retried after a failure;
  nothing is ever removed.
* **Shown.** No comment of its own: the stage's normal comment (triage, plan, blocked or
  Ready report) lists the split issues with links (``section``).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from typing import Any, TypeVar

from omnigent_factory.core.types import Parcel
from omnigent_factory.github.client import GitHubAPIError
from omnigent_factory.github.links import add_sub_issue, read_target
from omnigent_factory.service.deferred import find_marked_issue
from omnigent_factory.service.links import NativeLinks
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import discipline as ds
from omnigent_factory.store.sqlite import SqliteStore

LOG = logging.getLogger(__name__)
T = TypeVar("T")


def split_key(title: str) -> str:
    """An item's key within its source issue: its title, case and spacing aside."""
    return hashlib.sha256(" ".join(title.lower().split()).encode("utf-8")).hexdigest()[:24]


def split_marker(source: int, key: str) -> str:
    return f"<!-- factory-split source={source} key={key} -->"


class Splits:
    def __init__(
        self, service: FactoryService, links: NativeLinks, publish: Callable[[str], str]
    ) -> None:
        self.service = service
        #: Its GitHub client and blocked-by writes.
        self.links = links
        #: Makes agent text safe to post as the bot (mentions, HTML comments, credentials).
        self.publish = publish
        self._tasks: set[asyncio.Task[object]] = set()
        self._locks: dict[int, asyncio.Lock] = {}

    @property
    def _repo_id(self) -> str:
        return self.service.config.repo_id

    @property
    def _repo(self) -> str:
        return self.service.config.repository

    async def _db(self, fn: Callable[[SqliteStore], T]) -> T:
        return await self.service.db.call(fn)

    def _spawn(self, coro: Awaitable[object]) -> None:
        task: asyncio.Task[object] = asyncio.ensure_future(coro)
        task.set_name("splits")
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            LOG.warning("split issues failed", exc_info=task.exception())

    async def drain(self) -> None:
        """Wait for the background work started so far (tests, shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ------------------------------------------------------------------ results

    def schedule_result(self, parcel: Parcel, session_id: str, result: Mapping[str, Any]) -> None:
        """Called once a stage result is accepted: file its ``split_out`` items."""
        if result.get("split_out"):
            self._spawn(self.record_result(parcel, session_id, result))

    async def record_result(
        self, parcel: Parcel, session_id: str, result: Mapping[str, Any]
    ) -> list[ds.SplitIssue]:
        source = parcel.issue_number
        raw = result.get("split_out")
        if source is None or not isinstance(raw, list) or not raw:
            return []
        items = [
            ds.NewSplit(
                item_key=split_key(str(item.get("title") or "")),
                title=str(item.get("title") or ""),
                body=str(item.get("body") or ""),
                reason=str(item.get("reason") or ""),
                relation=str(item.get("relation") or "none"),
                blocks=tuple(
                    n
                    for n in item.get("blocks") or []
                    if isinstance(n, int) and not isinstance(n, bool) and n != source
                ),
            )
            for item in raw
            if isinstance(item, dict) and item.get("title")
        ]
        added = await self._db(
            partial(
                ds.record_splits,
                repo_id=self._repo_id,
                source_issue=source,
                session_id=session_id,
                items=items,
            )
        )
        if added:
            LOG.info("owner-directed splits recorded issue=#%s items=%s", source, added)
        await self.sync(source)
        keys = {i.item_key for i in items}
        return [s for s in await self._splits(source) if s.item_key in keys]

    async def _splits(self, source: int | None = None) -> list[ds.SplitIssue]:
        return await self._db(partial(ds.splits, repo_id=self._repo_id, source_issue=source))

    async def retry_pending(self) -> None:
        """File and link what a failure or restart left undone (start, board diff)."""
        sources = {
            s.source_issue
            for s in await self._splits()
            if s.issue_number is None or s.pending_links()
        }
        for source in sorted(sources):
            await self.sync(source)

    # ------------------------------------------------------------------ filing

    async def sync(self, source: int) -> None:
        """Open an issue for each unfiled item of ``source`` and do its link actions."""
        async with self._locks.setdefault(source, asyncio.Lock()):
            for split in await self._splits(source):
                try:
                    number = split.issue_number or await self._file(split)
                    await self._link(split, number)
                except GitHubAPIError as exc:
                    LOG.warning(
                        "split issue failed source=#%s title=%r reason=%s",
                        source,
                        split.title[:80],
                        exc,
                    )

    async def _file(self, split: ds.SplitIssue) -> int:
        repo_id, source, key = self._repo_id, split.source_issue, split.item_key
        if split.state == "creating":
            found = await find_marked_issue(
                self.links.client,
                self._repo,
                split_marker(source, key),
                since_us=split.attempt_at_us,
                bot_id=self.service.config.github_bot_user_id,
            )
            if found is not None:
                LOG.info("split issue found by marker issue=#%s source=#%s", found[0], source)
                await self._db(
                    partial(
                        ds.set_split_state,
                        repo_id=repo_id,
                        source_issue=source,
                        item_key=key,
                        state="created",
                        issue_number=found[0],
                    )
                )
                return found[0]
        # Before the create: a restart (or an unclear outcome) searches for it first.
        await self._db(
            partial(
                ds.set_split_state,
                repo_id=repo_id,
                source_issue=source,
                item_key=key,
                state="creating",
            )
        )
        body = (
            f"{self.publish(split.body)}\n\n"
            f"Split out of #{source} at the owner's direction: "
            f"{' '.join(self.publish(split.reason).split())}\n\n{split_marker(source, key)}"
        )
        response = await self.links.client.request(
            "POST",
            f"/repos/{self._repo}/issues",
            json_body={"title": " ".join(split.title.split())[:200], "body": body},
            expected=frozenset({201}),
        )
        data = response.json()
        number = data.get("number") if isinstance(data, dict) else None
        if not isinstance(number, int):
            raise GitHubAPIError("issue create returned no number")
        await self._db(
            partial(
                ds.set_split_state,
                repo_id=repo_id,
                source_issue=source,
                item_key=key,
                state="created",
                issue_number=number,
            )
        )
        LOG.info("split issue created issue=#%s source=#%s", number, source)
        return number

    async def _link(self, split: ds.SplitIssue, number: int) -> None:
        source = split.source_issue
        for action in split.pending_links():
            if action == "parent":
                ok = await self._parent(number, source)
            elif action.startswith("blocks:"):
                blocked = int(action.partition(":")[2])
                ok = await self.links.link_status(blocked, number, source=source) != "failed"
            else:  # blocked_by:<S>
                blocking = int(action.partition(":")[2])
                ok = await self.links.link_status(number, blocking, source=source) != "failed"
            if ok:
                await self._db(
                    partial(
                        ds.mark_split_done,
                        repo_id=self._repo_id,
                        source_issue=source,
                        item_key=split.item_key,
                        action=action,
                    )
                )

    async def _parent(self, number: int, source: int) -> bool:
        """Join the source issue's epic (a parent already set is kept)."""
        owner, name = self._repo.partition("/")[::2]
        client = self.links.client
        repo_id = self._repo_id
        try:
            target = await read_target(client, owner, name, number, repo_id)
            if target is None or target.has_parent:
                return True
            origin = await read_target(client, owner, name, source, repo_id)
            if origin is None or origin.parent is None:
                return True
            epic = await read_target(client, owner, name, origin.parent, repo_id)
            if epic is None:
                return True
            await add_sub_issue(client, epic.node_id, target.node_id)
        except GitHubAPIError as exc:
            LOG.warning("split issue parent failed issue=#%s reason=%s", number, exc)
            return False
        LOG.info("split issue added to epic issue=#%s epic=#%s", number, origin.parent)
        return True

    # ------------------------------------------------------------------ the stage comment

    async def section(self, parcel: Parcel, session_id: str) -> str:
        """The stage comment's "Split out" lines for the items ``session_id`` reported
        ("" when none). Unfiled ones are filed first, so the links are there."""
        source = parcel.issue_number
        if source is None or not session_id:
            return ""
        mine = [s for s in await self._splits(source) if s.session_id == session_id]
        if not mine:
            return ""
        if any(s.issue_number is None for s in mine):
            await self.sync(source)
            mine = [s for s in await self._splits(source) if s.session_id == session_id]
        lines = []
        for s in mine:
            title = " ".join(self.publish(s.title).split())[:200]
            lines.append(
                f"- #{s.issue_number}: {title}"
                if s.issue_number is not None
                else f"- {title} (being filed)"
            )
        return "\n\n**Split out** (filed at the owner's direction):\n" + "\n".join(lines)


__all__ = ["Splits", "split_key", "split_marker"]
