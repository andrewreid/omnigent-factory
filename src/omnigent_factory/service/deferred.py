"""Deferred review findings: one consolidated follow-up issue per PR, filed by the factory,
and the Ready report's Deferred checkboxes the owner ticks to have items done now.

* **Recorded.** A ``build_ready`` or ``blocked`` result lists the findings the run did not
  fix in ``deferred`` (outcome ``follow_up`` or ``advisory``); a legacy ``follow_ups``
  entry (an issue the run opened itself) is recorded as a ``filed`` item. Items are kept
  per source issue under a stable key, so a re-submission or restart records nothing
  twice (``store.discipline``).
* **Filed.** The ``follow_up`` items of one PR go into ONE issue the factory opens itself,
  ``Follow-ups: <source title> (#<source>)``: a checklist, with a hidden marker per item
  and one for the issue (``<!-- factory-follow-ups source=S pr=P -->``). It is labelled
  ``follow-up``, joins the source issue's epic and gets the native blocked-by links the
  items name, through ``service.follow_ups`` (each once, never undone). The row is set to
  ``creating`` before the create: a restart (or an unclear create) first looks for the
  marker among the newest issues, so the issue is never opened twice. Items a later
  result adds are appended to the same issue (re-read first; existing lines and ticks kept).
* **Ticked.** The Ready report lists every item and advisory as ``- [ ] Fix now: ...``
  (and ``One more <bot> review round`` once the issue's review rounds are used up), each
  with a hidden ``factory-action`` marker. An ``issue_comment`` ``edited`` webhook of that
  report (the bot's own comment) from an owner (numeric id) is diffed (``changes.body.from``
  against the new body): each newly ticked box is recorded once. Anyone else's tick, an
  untick, or a box already recorded does nothing. Ticks of one card within
  ``BATCH_WINDOW_US`` of the first are one batch: one ``RequestRework`` (the owner's
  control, under the same approval; refused like any rework, e.g. once merged) whose brief
  is exactly the ticked items, and "one more round" raises the issue's review budget by one
  (once per box). The batch and its event id are stored before the event is applied, so a
  restart re-applies the same event (deduplicated), never a second rework.
* **Shown.** After acting, each line is rewritten in place (``→ fixing (rework started)``,
  ``→ fixed in <sha>`` once the rework's build_ready is accepted, or ``→ not started:
  <reason>``): the comment is re-read before the write and only lines with a recorded
  action change, so ticks not yet processed stay. A fixed follow-up item is ticked in the
  consolidated issue with the fixing PR and commit (the factory never closes issues; the
  owner closes the consolidated issue).
* **Recovered.** A lost webhook: on the board-diff catch-up of a Ready card, a box the
  report shows ticked with no recorded action is recovered only when the comment's
  ``userContentEdits`` show an owner as the editor of the edit that ticked it; otherwise it
  is ignored and logged.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from datetime import datetime
from functools import partial
from typing import Any, TypeVar

from omnigent_factory.core import events as ev
from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import MICROS_PER_SECOND, Parcel, Stage
from omnigent_factory.github.client import GitHubAPIError, GitHubClient
from omnigent_factory.service.follow_ups import FollowUps
from omnigent_factory.service.review_rounds import ReviewRounds
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import discipline as ds
from omnigent_factory.store.sqlite import SqliteStore

LOG = logging.getLogger(__name__)

#: Ticks of one card this close to the first are one rework.
BATCH_WINDOW_US = 120 * MICROS_PER_SECOND
#: Event ids of the reworks ticked boxes start (the rework's brief is found by it).
EVENT_PREFIX = "report-items:"
ROUND_PREFIX = "round-"
#: Newest issues searched for the consolidated issue's marker after an unclear create.
_FIND_PAGES = 3

T = TypeVar("T")


def item_key(severity: str, path: str, title: str, thread_url: str) -> str:
    """A deferred finding's stable key (the same finding re-reported is the same item)."""
    text = "\x1f".join(" ".join(part.split()) for part in (severity, path, title, thread_url))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def issue_marker(source: int, pr_number: int) -> str:
    return f"<!-- factory-follow-ups source={source} pr={pr_number} -->"


def _one_line(text: str, limit: int = 300) -> str:
    # "→" separates the factory's status from the text: never part of agent text.
    text = " ".join(str(text).split()).replace("→", "->")
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _time_us(value: object) -> int:
    if not isinstance(value, str):
        return 0
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return int(stamp.timestamp() * MICROS_PER_SECOND)


class Deferrals:
    def __init__(
        self,
        service: FactoryService,
        client: GitHubClient,
        follow_ups: FollowUps,
        rounds: ReviewRounds,
        publish: Callable[[str], str],
    ) -> None:
        self.service = service
        self.client = client
        self.follow_ups = follow_ups
        self.rounds = rounds
        #: Makes agent text safe to post as the bot (mentions, HTML comments, credentials).
        self.publish = publish
        self._tasks: set[asyncio.Task[object]] = set()
        self._locks: dict[str, asyncio.Lock] = {}

    @property
    def _repo_id(self) -> str:
        return self.service.config.repo_id

    @property
    def _repo(self) -> str:
        return self.service.config.repository

    async def _db(self, fn: Callable[[SqliteStore], T]) -> T:
        return await self.service.db.call(fn)

    def _lock(self, key: str) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def _spawn(self, coro: Awaitable[object], name: str) -> None:
        task: asyncio.Task[object] = asyncio.ensure_future(coro)
        task.set_name(name)
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            LOG.warning("deferred findings failed", exc_info=task.exception())

    async def drain(self) -> None:
        """Wait for the background work started so far (tests, shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    def _bot(self) -> tuple[str, str]:
        """(the review bot's display name, the mention that asks it for a review)."""
        config = self.service.config
        mention = config.review_bot_mention.strip()
        name = mention.lstrip("@") or config.review_bot_login.removesuffix("[bot]")
        name = " ".join(name.split())
        return (name[:1].upper() + name[1:] if name else "review bot"), mention

    # ------------------------------------------------------------------ results

    def schedule_result(self, parcel: Parcel, session_id: str, result: Mapping[str, Any]) -> None:
        """Called once a stage result is accepted: record its deferred findings, file
        them, and finish a rework started by ticked boxes."""
        if result.get("kind") in ("build_ready", "blocked"):
            self._spawn(self.record_result(parcel, session_id, result), "deferred")

    async def record_result(
        self, parcel: Parcel, session_id: str, result: Mapping[str, Any]
    ) -> list[ds.DeferredItem]:
        source = parcel.issue_number
        if source is None:
            return []
        pr = result.get("pr_number")
        pr_number = pr if isinstance(pr, int) else (parcel.pr_number or 0)
        items = [*_deferred(result.get("deferred")), *_legacy(result.get("follow_ups"))]
        recorded: list[ds.DeferredItem] = []
        if items:
            recorded = await self._db(
                partial(
                    ds.record_deferred,
                    repo_id=self._repo_id,
                    source_issue=source,
                    pr_number=pr_number,
                    session_id=session_id,
                    items=items,
                )
            )
            LOG.info(
                "deferred findings recorded issue=#%s pr=#%s items=%s",
                source,
                pr_number,
                len(recorded),
            )
        head = result.get("head_sha")
        if result.get("kind") == "build_ready" and isinstance(head, str):
            await self._finish_rework(parcel, session_id, head)
        await self.sync(source)
        return recorded

    async def _finish_rework(self, parcel: Parcel, session_id: str, head: str) -> None:
        """The build_ready of a rework that ticked boxes started: its items are fixed."""
        session = parcel.session(session_id)
        auth = parcel.authorization(session.authorization_id) if session is not None else None
        if auth is None or not auth.source_event_id.startswith(EVENT_PREFIX):
            return
        event_id = auth.source_event_id
        batch = await self._db(partial(ds.batch_by_event, repo_id=self._repo_id, event_id=event_id))
        if batch is None or batch.state != "started":
            return
        await self._db(
            partial(
                ds.finish_batch,
                repo_id=self._repo_id,
                batch_id=batch.batch_id,
                state="done",
                reason=head[:12],
            )
        )
        LOG.info(
            "ticked items fixed issue=#%s batch=%s head=%s",
            parcel.issue_number,
            batch.batch_id,
            head[:12],
        )
        await self._close_out(parcel, batch, head)
        await self.refresh_report_lines(parcel.parcel_id)

    # ------------------------------------------------------------------ consolidated issue

    async def sync(self, source: int) -> None:
        """File every ``follow_up`` item of ``source`` not yet in its PR's consolidated
        issue (create it, or append to it), then link that issue."""
        async with self._lock(f"issue:{source}"):
            items = await self._db(
                partial(ds.deferred_items, repo_id=self._repo_id, source_issue=source)
            )
            filed = [i for i in items if i.outcome == "follow_up"]
            for pr_number in sorted({i.pr_number for i in filed if not i.listed}):
                try:
                    await self._sync_pr(
                        source, pr_number, [i for i in filed if i.pr_number == pr_number]
                    )
                except GitHubAPIError as exc:
                    LOG.warning(
                        "consolidated follow-up issue failed source=#%s pr=#%s reason=%s",
                        source,
                        pr_number,
                        exc,
                    )

    async def retry_pending(self) -> None:
        """File items not yet in their consolidated issue, and tick fixed items a crash
        left unticked there (at start and after each board diff)."""
        sources = await self._db(partial(ds.unsettled_sources, repo_id=self._repo_id))
        for source in sources:
            await self.sync(source)
        for batch in await self._db(partial(ds.batches, repo_id=self._repo_id, state="done")):
            parcel = await self._db(partial(SqliteStore.load_parcel, parcel_id=batch.parcel_id))
            if parcel is not None and await self._close_out(parcel, batch, batch.reason):
                await self.refresh_report_lines(parcel.parcel_id)

    async def _sync_pr(self, source: int, pr_number: int, items: list[ds.DeferredItem]) -> None:
        repo_id = self._repo_id
        rows = await self._db(
            partial(ds.consolidated, repo_id=repo_id, source_issue=source, pr_number=pr_number)
        )
        row = rows[0] if rows else None
        number = row.issue_number if row is not None else None
        body: str | None = None
        if number is None and row is not None and row.state == "creating":
            found = await self._find(source, pr_number, row.updated_at_us)
            if found is not None:
                number, body = found
                LOG.info("consolidated follow-up issue found by marker issue=#%s", number)
        if number is None:
            await self._db(
                partial(
                    ds.set_consolidated,
                    repo_id=repo_id,
                    source_issue=source,
                    pr_number=pr_number,
                    state="creating",
                )
            )
            body = self._issue_body(source, pr_number, items)
            number = await self._create(source, pr_number, body)
            LOG.info(
                "consolidated follow-up issue created issue=#%s source=#%s pr=#%s items=%s",
                number,
                source,
                pr_number,
                len(items),
            )
        await self._db(
            partial(
                ds.set_consolidated,
                repo_id=repo_id,
                source_issue=source,
                pr_number=pr_number,
                state="created",
                issue_number=number,
            )
        )
        if body is None:
            body = await self._issue_text(number)
        present = rl.ids(body, rl.ITEM)
        missing = [i for i in items if i.item_key not in present]
        if missing:
            new = rl.append_lines(
                body, (self._issue_line(i) for i in missing), issue_marker(source, pr_number)
            )
            await self._write_issue(number, new)
            LOG.info(
                "consolidated follow-up issue extended issue=#%s items=%s", number, len(missing)
            )
        await self._db(
            partial(
                ds.mark_items,
                repo_id=repo_id,
                source_issue=source,
                keys=[i.item_key for i in items],
                column="listed",
            )
        )
        blocks = sorted({n for i in items for n in i.blocks} - {number, source})
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
        await self.follow_ups.link(number)

    def _issue_line(self, item: ds.DeferredItem) -> str:
        path = f" `{_one_line(item.path, 200).replace('`', chr(39))}`" if item.path else ""
        link = f" ([thread]({item.thread_url}))" if item.thread_url.startswith("https://") else ""
        text = self.publish(
            f"{item.severity}{path} {_one_line(item.title)}{link}: {_one_line(item.reason)}"
        )
        return rl.line(rl.ITEM, item.item_key, _one_line(text, 1000))

    def _issue_body(self, source: int, pr_number: int, items: Sequence[ds.DeferredItem]) -> str:
        lines = "\n".join(self._issue_line(i) for i in items)
        return (
            f"Review findings deferred from {f'PR #{pr_number}' if pr_number else 'the run'} "
            f"(issue #{source}): real defects "
            "outside that change, or too large for it. The factory ticks an item here when a "
            f"later rework of #{source} fixes it; close this issue when nothing is left.\n\n"
            f"{lines}\n\n{issue_marker(source, pr_number)}"
        )

    async def _create(self, source: int, pr_number: int, body: str) -> int:
        """Open the consolidated issue using the source title already stored locally."""
        rows = await self._db(
            lambda store: store.query(
                "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND issue_number = ?",
                (self._repo_id, source),
            )
        )
        source_title = parcel_from_json(str(rows[0][0])).issue_title if rows else ""
        source_title = " ".join(source_title.split())
        # Only strip familiar conventional prefixes with a nonempty description.
        source_title = re.sub(
            r"^(?:feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)"
            r"(?:\([\w./-]+\))?!?: (?=\S)",
            "",
            source_title,
        )
        title = f"Follow-ups from #{pr_number or source}"
        if source_title:
            prefix, suffix = "Follow-ups: ", f" (#{source})"
            limit = 120 - len(prefix) - len(suffix)
            if len(source_title) > limit:
                source_title = source_title[: limit - 1].rstrip() + "…"
            title = f"{prefix}{source_title}{suffix}"
        response = await self.client.request(
            "POST",
            f"/repos/{self._repo}/issues",
            json_body={"title": title, "body": body},
            expected=frozenset({201}),
        )
        data = response.json()
        number = data.get("number") if isinstance(data, dict) else None
        if not isinstance(number, int):
            raise GitHubAPIError("issue create returned no number")
        return number

    async def _find(self, source: int, pr_number: int, since_us: int) -> tuple[int, str] | None:
        """The consolidated issue an earlier (unclear) create opened."""
        return await find_marked_issue(
            self.client,
            self._repo,
            issue_marker(source, pr_number),
            since_us=since_us,
            bot_id=self.service.config.github_bot_user_id,
        )

    async def _issue_text(self, number: int) -> str:
        issue: Any = await self.client.get_json(f"/repos/{self._repo}/issues/{number}")
        return str(issue.get("body") or "") if isinstance(issue, dict) else ""

    async def _write_issue(self, number: int, body: str) -> None:
        await self.client.request(
            "PATCH",
            f"/repos/{self._repo}/issues/{number}",
            json_body={"body": body},
            expected=frozenset({200}),
        )

    async def _close_out(self, parcel: Parcel, batch: ds.ActionBatch, head: str) -> bool:
        """Tick the fixed follow-up items in their consolidated issue (re-read first).
        Returns whether any item was still to tick."""
        source = parcel.issue_number
        if source is None:
            return False
        repo_id = self._repo_id
        items = await self._db(partial(ds.deferred_items, repo_id=repo_id, source_issue=source))
        rows = await self._db(partial(ds.consolidated, repo_id=repo_id, source_issue=source))
        issues = {r.pr_number: r.issue_number for r in rows if r.issue_number is not None}
        fixed = [
            i
            for i in items
            if i.action_id in batch.actions
            and i.outcome == "follow_up"
            and not i.closed_out
            and i.pr_number in issues
        ]
        if not fixed:
            return False
        pr = parcel.pr_number
        status = f"fixed in #{pr} (`{head[:7]}`)" if pr else f"fixed in `{head[:7]}`"
        for number in sorted({issues[i.pr_number] for i in fixed}):
            keys = [i.item_key for i in fixed if issues.get(i.pr_number) == number]
            try:
                body = await self._issue_text(number)
                new = rl.with_status(body, dict.fromkeys(keys, status), rl.ITEM)
                if new != body:
                    await self._write_issue(number, new)
            except GitHubAPIError as exc:
                LOG.warning("consolidated issue update failed issue=#%s reason=%s", number, exc)
                continue
            await self._db(
                partial(
                    ds.mark_items,
                    repo_id=repo_id,
                    source_issue=source,
                    keys=keys,
                    column="closed_out",
                )
            )
            LOG.info("consolidated follow-up items ticked issue=#%s items=%s", number, len(keys))
        return True

    # ------------------------------------------------------------------ Ready report

    async def report_section(self, parcel: Parcel) -> str:
        """The Ready report's Deferred section ("" when there is nothing to list)."""
        source = parcel.issue_number
        if source is None:
            return ""
        repo_id = self._repo_id
        items = await self._db(partial(ds.deferred_items, repo_id=repo_id, source_issue=source))
        rows = await self._db(partial(ds.consolidated, repo_id=repo_id, source_issue=source))
        actions = await self._db(
            partial(ds.report_actions, repo_id=repo_id, parcel_id=parcel.parcel_id)
        )
        issues = {r.pr_number: r.issue_number for r in rows if r.issue_number is not None}
        lines = [
            rl.line(
                rl.ACTION,
                item.action_id,
                self._report_text(item, issues),
                ticked=item.action_id in actions,
                status=_status(actions.get(item.action_id), round_line=False),
            )
            for item in items
        ]
        r = parcel.readiness
        if (
            r is not None
            and r.review_rounds_budget
            and r.review_rounds_used >= r.review_rounds_budget
        ):
            ident = f"{ROUND_PREFIX}{r.review_rounds_budget}"
            name, _mention = self._bot()
            lines.append(
                rl.line(
                    rl.ACTION,
                    ident,
                    f"One more {name} review round",
                    ticked=ident in actions,
                    status=_status(actions.get(ident), round_line=True),
                )
            )
        if not lines:
            return ""
        return "\n\n**Deferred** (tick an item to have it done in this PR):\n" + "\n".join(lines)

    def _report_text(self, item: ds.DeferredItem, issues: Mapping[int, int | None]) -> str:
        if item.outcome == "filed":
            return f"Fix now: #{item.issue_number} (filed during the run)"
        path = f" `{_one_line(item.path, 200).replace('`', chr(39))}`" if item.path else ""
        text = f"Fix now: {item.severity}{path} {_one_line(item.title)}"
        if item.outcome == "advisory":
            where = f"(advisory: {_one_line(item.reason, 200)})"
        else:
            number = issues.get(item.pr_number)
            where = f"(in #{number})" if number is not None else "(follow-up issue pending)"
        return _one_line(self.publish(f"{text} {where}"), 1000)

    async def on_comment_delivery(self, body: bytes | str) -> int:
        """An authenticated ``issue_comment`` delivery: record the owner's newly ticked
        Ready report boxes. Returns how many ticks were new."""
        try:
            payload = json.loads(body)
        except ValueError:
            return 0
        if not isinstance(payload, dict) or payload.get("action") != "edited":
            return 0
        comment = payload.get("comment")
        issue = payload.get("issue")
        changes = payload.get("changes")
        if not isinstance(comment, dict) or not isinstance(issue, dict) or "pull_request" in issue:
            return 0
        config = self.service.config
        author = comment.get("user")
        if not isinstance(author, dict) or author.get("id") != config.github_bot_user_id:
            return 0
        after = str(comment.get("body") or "")
        if "<!-- factory-action:" not in after:
            return 0
        previous = changes.get("body") if isinstance(changes, dict) else None
        before = previous.get("from") if isinstance(previous, dict) else None
        if not isinstance(before, str):
            return 0  # no earlier body to diff: nothing is taken as newly ticked
        ticked = rl.newly_ticked(before, after)
        if not ticked:
            return 0
        sender = payload.get("sender")
        actor = sender.get("id") if isinstance(sender, dict) else None
        parcel_id = issue.get("node_id")
        if not isinstance(actor, int) or actor not in config.owners:
            LOG.info(
                "ready report ticks ignored: not an owner issue=#%s actor=%s items=%s",
                issue.get("number"),
                actor,
                ",".join(ticked),
            )
            return 0
        if not isinstance(parcel_id, str) or not isinstance(comment.get("id"), int):
            return 0
        at_us = _time_us(comment.get("updated_at")) or self.service.clock.now_utc_us()
        return await self._record(
            parcel_id, int(comment["id"]), ticked, at_us=at_us, actor=actor, via="webhook"
        )

    async def _record(
        self,
        parcel_id: str,
        comment_id: int,
        ticked: Iterable[str],
        *,
        at_us: int,
        actor: int,
        via: str,
    ) -> int:
        new = 0
        for action_id in ticked:
            added = await self._db(
                partial(
                    ds.record_tick,
                    repo_id=self._repo_id,
                    parcel_id=parcel_id,
                    action_id=action_id,
                    comment_id=comment_id,
                    ticked_at_us=at_us,
                    actor_id=actor,
                    via=via,
                )
            )
            if added:
                new += 1
                LOG.info(
                    "ready report item ticked parcel=%s item=%s via=%s", parcel_id, action_id, via
                )
        return new

    # ------------------------------------------------------------------ acting on ticks

    async def tick(self) -> int:
        """Start the rework of every batch of ticks whose window has passed (and finish a
        batch a restart interrupted). Returns how many batches were applied."""
        repo_id = self._repo_id
        applied = 0
        for batch in await self._db(partial(ds.batches, repo_id=repo_id, state="applying")):
            await self._apply(batch)
            applied += 1
        ticks = await self._db(partial(ds.ticked_actions, repo_id=repo_id))
        groups: dict[str, list[ds.ReportAction]] = {}
        for t in ticks:
            groups.setdefault(t.parcel_id, []).append(t)
        now = self.service.clock.now_utc_us()
        for parcel_id, group in sorted(groups.items()):
            first = min(t.ticked_at_us for t in group)
            if now < first + BATCH_WINDOW_US:
                continue
            action_ids = [t.action_id for t in group]
            digest = hashlib.sha256("\x1f".join([parcel_id, *action_ids]).encode()).hexdigest()
            batch_id = f"b{first}-{digest[:12]}"
            parcel = await self._db(partial(SqliteStore.load_parcel, parcel_id=parcel_id))
            if parcel is None:
                continue
            brief = await self._brief(parcel, action_ids)
            batch = await self._db(
                partial(
                    ds.start_batch,
                    repo_id=repo_id,
                    parcel_id=parcel_id,
                    batch_id=batch_id,
                    event_id=f"{EVENT_PREFIX}{batch_id}",
                    actions=action_ids,
                    brief=brief,
                )
            )
            await self._apply(batch)
            applied += 1
        return applied

    async def _brief(self, parcel: Parcel, action_ids: Sequence[str]) -> str:
        source = parcel.issue_number
        items = (
            await self._db(partial(ds.deferred_items, repo_id=self._repo_id, source_issue=source))
            if source is not None
            else []
        )
        rows = (
            await self._db(partial(ds.consolidated, repo_id=self._repo_id, source_issue=source))
            if source is not None
            else []
        )
        issues = {r.pr_number: r.issue_number for r in rows if r.issue_number is not None}
        by_action = {i.action_id: i for i in items}
        name, mention = self._bot()
        lines: list[str] = []
        for action_id in action_ids:
            item = by_action.get(action_id)
            if item is not None:
                lines.append(f"- {self._brief_line(item, issues)}")
            elif action_id.startswith(ROUND_PREFIX):
                ask = f"comment `{mention} review` on the PR" if mention else "ask it on the PR"
                lines.append(
                    f"- One more {name} review round: after pushing any fixes, {ask}, then "
                    "give its findings the outcome rule (fix in this PR, follow-up or "
                    "advisory, listed in deferred)."
                )
        return "\n".join(lines)

    def _brief_line(self, item: ds.DeferredItem, issues: Mapping[int, int | None]) -> str:
        if item.outcome == "filed":
            return f"Fix #{item.issue_number} (filed during an earlier run) in this PR."
        path = f" in `{_one_line(item.path, 200).replace('`', chr(39))}`" if item.path else ""
        where = ""
        if item.outcome == "follow_up" and issues.get(item.pr_number) is not None:
            where = f" (listed in follow-up issue #{issues[item.pr_number]})"
        thread = f" Thread: {item.thread_url}." if item.thread_url.startswith("https://") else ""
        return self.publish(
            f"Fix the {item.severity} finding{path}: {_one_line(item.title)}{where}.{thread} "
            f"It was deferred because: {_one_line(item.reason)}"
        )

    async def rework_brief(self, parcel: Parcel, source_event_id: str) -> str | None:
        """The brief of the rework ``source_event_id`` started (None: not a ticked batch)."""
        if not source_event_id.startswith(EVENT_PREFIX):
            return None
        batch = await self._db(
            partial(ds.batch_by_event, repo_id=self._repo_id, event_id=source_event_id)
        )
        return None if batch is None or batch.parcel_id != parcel.parcel_id else batch.brief

    async def _apply(self, batch: ds.ActionBatch) -> None:
        """Apply the batch's rework control (once: its event id is fixed) and record and
        show what came of it."""
        repo_id = self._repo_id
        actions = await self._db(
            partial(ds.report_actions, repo_id=repo_id, parcel_id=batch.parcel_id)
        )
        mine = [actions[a] for a in batch.actions if a in actions]
        parcel = await self._db(lambda store: store.load_parcel(batch.parcel_id))
        if not mine or parcel is None:
            await self._db(
                partial(
                    ds.finish_batch,
                    repo_id=repo_id,
                    batch_id=batch.batch_id,
                    state="refused",
                    reason="issue unknown",
                )
            )
            return
        latest = max(mine, key=lambda a: a.ticked_at_us)
        event = Event(
            event_id=batch.event_id,
            repo_id=repo_id,
            parcel_id=batch.parcel_id,
            source_time_us=latest.ticked_at_us,
            # The owner's own authenticated edit (its webhook, or the recovered edit the
            # comment's history shows them making): the same authority as a drag.
            provenance=(
                Provenance.WEBHOOK if all(a.via == "webhook" for a in mine) else Provenance.RECOVERY
            ),
            body=ev.RequestRework(batch_id=batch.batch_id),
            actor_id=latest.actor_id,
            issue_number=parcel.issue_number,
            entropy=hashlib.sha256(batch.event_id.encode()).hexdigest(),
        )
        result = await self.service.apply_event(event)
        after = result.parcel or await self._db(lambda store: store.load_parcel(batch.parcel_id))
        accepted = result.accepted or (
            result.duplicate
            and after is not None
            and any(a.source_event_id == batch.event_id for a in after.authorizations)
        )
        if accepted:
            for action_id in batch.actions:
                if action_id.startswith(ROUND_PREFIX):
                    await self.rounds.grant(batch.parcel_id, action_id)
            await self._db(
                partial(ds.finish_batch, repo_id=repo_id, batch_id=batch.batch_id, state="started")
            )
            LOG.info(
                "ready report rework started issue=#%s batch=%s items=%s",
                parcel.issue_number,
                batch.batch_id,
                ",".join(batch.actions),
            )
        else:
            reason = _refusal(result.reason, after)
            await self._db(
                partial(
                    ds.finish_batch,
                    repo_id=repo_id,
                    batch_id=batch.batch_id,
                    state="refused",
                    reason=reason,
                )
            )
            LOG.info(
                "ready report rework refused issue=#%s batch=%s reason=%s",
                parcel.issue_number,
                batch.batch_id,
                result.reason,
            )
        await self.refresh_report_lines(batch.parcel_id)

    async def refresh_report_lines(self, parcel_id: str) -> None:
        """Rewrite the status of every acted-on line in the reports it is in (re-read
        first: lines without a recorded action, and their ticks, stay as they are)."""
        actions = await self._db(
            partial(ds.report_actions, repo_id=self._repo_id, parcel_id=parcel_id)
        )
        by_comment: dict[int, dict[str, str]] = {}
        for action in actions.values():
            status = _status(action, round_line=action.action_id.startswith(ROUND_PREFIX))
            by_comment.setdefault(action.comment_id, {})[action.action_id] = status
        for comment_id, states in sorted(by_comment.items()):
            try:
                comment: Any = await self.client.get_json(
                    f"/repos/{self._repo}/issues/comments/{comment_id}"
                )
                body = str(comment.get("body") or "") if isinstance(comment, dict) else ""
                new = rl.with_status(body, states)
                if body and new != body:
                    await self.client.request(
                        "PATCH",
                        f"/repos/{self._repo}/issues/comments/{comment_id}",
                        json_body={"body": new},
                        expected=frozenset({200}),
                    )
            except GitHubAPIError as exc:
                LOG.warning("ready report line update failed comment=%s reason=%s", comment_id, exc)

    # ------------------------------------------------------------------ lost webhooks

    async def catch_up(self) -> int:
        """Ready cards whose report shows a box ticked that no webhook recorded: recover
        the tick when the comment's edit history shows an owner made it. Returns how many
        were recovered."""
        repo_id = self._repo_id
        rows = await self._db(
            lambda store: store.query(
                "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND "
                "json_extract(aggregate_json, '$.parcel.stage') = ?",
                (repo_id, Stage.READY.value),
            )
        )
        recovered = 0
        for row in rows:
            parcel = parcel_from_json(str(row[0]))
            r = parcel.readiness
            if parcel.issue_number is None or r is None or not r.report_effect_id:
                continue
            if not await self.report_section(parcel):
                continue  # no checkboxes in its report: nothing to read
            try:
                recovered += await self._catch_up_report(parcel, r.report_effect_id)
            except GitHubAPIError as exc:
                LOG.warning(
                    "ready report catch-up failed issue=#%s reason=%s", parcel.issue_number, exc
                )
        return recovered

    async def _catch_up_report(self, parcel: Parcel, effect_id: str) -> int:
        marker = f"<!-- omnigent-factory effect={effect_id} -->"
        bot = self.service.config.github_bot_user_id
        comments = await self.client.paginate(
            f"/repos/{self._repo}/issues/{parcel.issue_number}/comments?per_page=100"
        )
        found = [
            c
            for c in comments
            if isinstance(c, dict)
            and marker in str(c.get("body") or "")
            and isinstance(c.get("user"), dict)
            and c["user"].get("id") == bot
            and isinstance(c.get("id"), int)
        ]
        if len(found) != 1:
            return 0
        comment = found[0]
        ticked = {i for i, on in rl.checked(str(comment.get("body") or "")).items() if on}
        actions = await self._db(
            partial(ds.report_actions, repo_id=self._repo_id, parcel_id=parcel.parcel_id)
        )
        missing = ticked - set(actions)
        if not missing:
            return 0
        node_id = comment.get("node_id")
        edits = await self._edits(str(node_id)) if isinstance(node_id, str) else None
        owners = self.service.config.owners
        recovered = 0
        if edits is not None:
            previous: set[str] = set()
            for at_us, editor, text in edits:
                if text is None:
                    break  # a version without its text: later ticks cannot be attributed
                now_ticked = {i for i, on in rl.checked(text).items() if on}
                for action_id in sorted((now_ticked - previous) & missing):
                    if editor in owners:
                        recovered += await self._record(
                            parcel.parcel_id,
                            int(comment["id"]),
                            [action_id],
                            at_us=at_us,
                            actor=editor,
                            via="recovered",
                        )
                        missing.discard(action_id)
                    else:
                        LOG.info(
                            "ready report tick not recovered: edited by a non-owner "
                            "issue=#%s item=%s editor=%s",
                            parcel.issue_number,
                            action_id,
                            editor,
                        )
                        missing.discard(action_id)
                previous = now_ticked
        for action_id in sorted(missing):
            LOG.info(
                "ready report tick ignored: no owner edit shows it issue=#%s item=%s",
                parcel.issue_number,
                action_id,
            )
        return recovered

    async def _edits(self, node_id: str) -> list[tuple[int, int | None, str | None]] | None:
        """The comment's edit history, oldest first: (time, editor's numeric id, the body
        after that edit). None when it cannot be read."""
        query = """
        query($id: ID!) {
          node(id: $id) {
            ... on IssueComment {
              userContentEdits(first: 100) {
                nodes {
                  editedAt
                  diff
                  editor { __typename ... on User { databaseId } ... on Bot { databaseId } }
                }
              }
            }
          }
        }
        """
        data = await self.client.graphql(query, {"id": node_id})
        node = data.get("node")
        history = node.get("userContentEdits") if isinstance(node, dict) else None
        nodes = history.get("nodes") if isinstance(history, dict) else None
        if not isinstance(nodes, list):
            return None
        out: list[tuple[int, int | None, str | None]] = []
        for item in nodes:
            if not isinstance(item, dict):
                continue
            editor = item.get("editor")
            editor_id = editor.get("databaseId") if isinstance(editor, dict) else None
            text = item.get("diff")
            out.append(
                (
                    _time_us(item.get("editedAt")),
                    editor_id if isinstance(editor_id, int) else None,
                    text if isinstance(text, str) else None,
                )
            )
        return sorted(out, key=lambda e: e[0])


async def find_marked_issue(
    client: GitHubClient, repo: str, marker: str, *, since_us: int, bot_id: int
) -> tuple[int, str] | None:
    """(number, body) of the issue the factory bot opened carrying ``marker``: the newest
    issues, back to a minute before ``since_us`` (an unclear create's attempt), searched
    for it. None: not there."""
    floor = since_us - 60 * MICROS_PER_SECOND
    for page in range(1, _FIND_PAGES + 1):
        items: Any = await client.get_json(
            f"/repos/{repo}/issues?state=all&sort=created&direction=desc&per_page=100&page={page}"
        )
        if not isinstance(items, list) or not items:
            return None
        for issue in items:
            if not isinstance(issue, dict) or "pull_request" in issue:
                continue
            user = issue.get("user")
            body = str(issue.get("body") or "")
            if (
                marker in body
                and isinstance(user, dict)
                and user.get("id") == bot_id
                and isinstance(issue.get("number"), int)
            ):
                return int(issue["number"]), body
        last = items[-1]
        if isinstance(last, dict) and 0 < _time_us(last.get("created_at")) < floor:
            return None
    return None


def _status(action: ds.ReportAction | None, *, round_line: bool) -> str:
    """The status a line shows for its recorded action ("" before it is acted on)."""
    if action is None or action.state == "ticked":
        return ""
    if action.state == "refused":
        return f"not started: {action.detail or 'refused'}"
    if action.state == "done":
        return "done" if round_line else f"fixed in `{action.detail[:7]}`"
    return "one more round granted (rework started)" if round_line else "fixing (rework started)"


def _refusal(reason: str, parcel: Parcel | None) -> str:
    words = reason.replace("-", " ").strip() or "refused"
    if reason == "rework-only-from-ready" and parcel is not None and parcel.stage is not None:
        return f"the card is in {parcel.stage.value}, not Ready"
    return words[:120]


def _deferred(raw: object) -> list[ds.NewDeferred]:
    out: list[ds.NewDeferred] = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity") or "")
        path = str(item.get("path") or "")
        title = str(item.get("title") or "")
        url = str(item.get("thread_url") or "")
        out.append(
            ds.NewDeferred(
                item_key=item_key(severity, path, title, url),
                severity=severity,
                path=path,
                title=title,
                thread_url=url,
                reason=str(item.get("reason") or ""),
                outcome="advisory" if item.get("outcome") == "advisory" else "follow_up",
                blocks=tuple(
                    n
                    for n in item.get("blocks") or []
                    if isinstance(n, int) and not isinstance(n, bool)
                ),
            )
        )
    return out


def _legacy(raw: object) -> list[ds.NewDeferred]:
    """``follow_ups`` entries (issues the run opened itself): listed, never re-filed."""
    out: list[ds.NewDeferred] = []
    for item in raw if isinstance(raw, list) else []:
        number = item.get("issue") if isinstance(item, dict) else None
        if not isinstance(number, int) or isinstance(number, bool) or number < 1:
            continue
        out.append(
            ds.NewDeferred(
                item_key=f"issue-{number}",
                severity="",
                path="",
                title=f"#{number}",
                thread_url="",
                reason="",
                outcome="filed",
                issue_number=number,
            )
        )
    return out


__all__ = ["BATCH_WINDOW_US", "Deferrals", "find_marked_issue", "issue_marker", "item_key"]
