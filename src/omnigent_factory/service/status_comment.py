"""The status comment: one factory-owned comment per issue, the issue's dashboard.

* **Created** the first time a parcel the factory handles has something to show (its first
  stage run, or epic autopilot), or, for a parcel that existed before status comments, at
  its first change after that (never a bulk sweep: ``status_settings.baseline_us``),
  except an open epic, which gets one at once (its sub-issues' work never changes it). One
  per issue, found by its hidden marker (``status_marker``): the row is ``creating``
  before the create, and a retry or restart searches for the marker before creating
  again, so an unclear create is adopted, never duplicated.
* **Edited** in place only when the rendered text changes, at most once per
  ``status_comment_edit_seconds`` per issue (a burst is coalesced into the next edit; the
  reconcile loop writes what is pending). The comment is re-read before every write and
  the owner's checkbox ticks are kept (``core.report_lines.keep_ticks``). An epic is
  rendered again when one of its sub-issues changes (stored links), even when finished.
  A stage result
  that used to be its own comment (triage, the checkpoint report) and the Ready line are
  written at once (``publish``): the owner never gets a link to a stale dashboard.
* **Pinned** once after it is created (``pinIssueComment``, issues only, never a pull
  request). A refusal is logged once with GitHub's message and recorded for the
  installation: pinning is not tried again. ``doctor`` shows the state.
* **Shows** (built only from stored state, no extra GitHub reads): a headline (stage and
  state, or what the factory waits on you for), the triage result, the plan (a link, the
  approved hash, a newer revision), the PR (link, checks, review rounds, the agent's
  summary, findings, the Deferred checkboxes), the last checkpoint report, split-out
  issues and a collapsed History of stage events. An epic shows its sub-issues instead:
  who it waits on, what is in progress, blocked, next and done.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import partial
from typing import Any, TypeVar

from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import EffectIntent, EffectKind
from omnigent_factory.core.types import (
    ApprovalKind,
    BotState,
    DecisionStatus,
    Hold,
    LinkedIssue,
    Parcel,
    Stage,
)
from omnigent_factory.github.adapter import StatusWrite
from omnigent_factory.github.client import (
    AmbiguousRequest,
    GitHubAPIError,
    GitHubClient,
    GitHubRejected,
    RateLimited,
)
from omnigent_factory.service.directory import (
    ServiceDispatchDirectory,
    agent_display_name,
    approved_epic_plan,
    ready_text,
    review_bot_name,
    stage_result_text,
)
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import discipline as ds
from omnigent_factory.store import status as st
from omnigent_factory.store.sqlite import SqliteStore

LOG = logging.getLogger(__name__)
T = TypeVar("T")

#: GitHub's comment limit, with room for the marker.
_BODY_LIMIT = 60_000
#: Sub-issues an epic's "Up next" lists.
_UP_NEXT = 3
#: Characters of a question kept in a headline.
_QUESTION_HEADLINE = 100

_PIN = """
mutation($id: ID!) { pinIssueComment(input: {issueCommentId: $id}) { clientMutationId } }
"""


def status_marker(issue_number: int) -> str:
    return f"<!-- omnigent-factory status issue={issue_number} -->"


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _one_line(text: object, limit: int = 300) -> str:
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


def _comment_url(repository: str, issue: int, comment_id: int | None) -> str:
    base = f"https://github.com/{repository}/issues/{issue}"
    return f"{base}#issuecomment-{comment_id}" if comment_id else base


def _clock(at_us: int) -> str:
    return datetime.fromtimestamp(at_us / 1_000_000, UTC).strftime("%Y-%m-%d %H:%M UTC")


def _without_heading(text: str) -> str:
    """``text`` without its first line when that is a Markdown heading."""
    first, _, rest = text.partition("\n")
    return rest.lstrip("\n") if first.startswith("#") else text


# ------------------------------------------------------------------ what is rendered


@dataclass(frozen=True, slots=True)
class Question:
    """An open owner question: its text (as asked) and the comment that asks it."""

    summary: str
    url: str = ""


@dataclass(frozen=True, slots=True)
class PlanView:
    #: The plan comment of the newest published plan.
    url: str
    prefix: str
    #: The approved plan's hash prefix ("" = none approved).
    approved: str = ""
    #: A newer plan than the approved one is posted, or a revision is being written.
    newer: str = ""
    revising: bool = False
    #: Built from the issue as written (no plan).
    skip: bool = False


@dataclass(frozen=True, slots=True)
class IssueView:
    """Everything a normal issue's status comment shows (no internal ids)."""

    issue: int
    stage: str
    bot: BotState
    note: str = ""
    finished: bool = False
    question: Question | None = None
    pr_number: int | None = None
    checks: str = ""
    rounds: str = ""
    #: Rendered sections: triage, plan, PR, checkpoint (each "" when there is none).
    triage: str = ""
    plan: PlanView | None = None
    pr: str = ""
    deferred: str = ""
    checkpoint: str = ""
    splits: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SubIssue:
    number: int
    title: str
    open: bool
    stage: str = ""
    bot: BotState | None = None
    note: str = ""
    pr_number: int | None = None
    question: Question | None = None
    #: Open blockers, each with whether it is outside this epic.
    blockers: tuple[tuple[str, bool], ...] = ()
    #: A nested epic: (done, total).
    epic: tuple[int, int] | None = None
    building: bool = False
    #: The factory started work on it (a stage run) and it is not finished.
    started: bool = False
    gate: bool = False
    #: A follow-up epic autopilot holds.
    held: bool = False


@dataclass(frozen=True, slots=True)
class EpicView:
    issue: int
    done: int
    total: int
    autopilot: str
    subs: tuple[SubIssue, ...]
    #: Build order (issue numbers) from the epic plan or triage.
    order: tuple[int, ...] = ()
    next_issue: int | None = None
    plan_url: str = ""
    plan_label: str = ""
    #: Who human gates are assigned to, and who answers questions.
    gate_assignee: str = "You"
    question_assignee: str = "You"
    paused: str = ""


# ------------------------------------------------------------------ rendering


def issue_headline(view: IssueView) -> str:
    """E.g. "Building · PR #1148 · checks running · Codex round 1/2", or "Waiting on you:
    <the question, short>"."""
    if view.question is not None:
        return f"Waiting on you: {_one_line(view.question.summary, _QUESTION_HEADLINE)}"
    if view.bot == BotState.NEEDS_YOU:
        reason = view.note.removeprefix("Needs you:").strip()
        return f"Waiting on you: {_one_line(reason or 'a decision only you can make', 120)}"
    parts = [view.stage]
    if view.finished:
        return " · ".join([*parts, "finished"])
    if view.pr_number is not None:
        parts.append(f"PR #{view.pr_number}")
        if view.checks:
            parts.append(view.checks)
        if view.rounds:
            parts.append(view.rounds)
    parts.append(_STATE_TEXT[view.bot])
    return " · ".join(parts)


_STATE_TEXT = {
    BotState.WORKING: "working",
    BotState.QUEUED: "queued",
    BotState.CHECKPOINT: "at a checkpoint",
    BotState.BLOCKED: "blocked",
    BotState.IDLE: "idle",
    BotState.NEEDS_YOU: "waiting on you",
}


def render_issue(view: IssueView, history: Sequence[tuple[int, str]], headline: str) -> str:
    lines = [f"### {headline}"]
    if view.note and not headline.endswith(view.note):
        lines += ["", f"_{_one_line(view.note, 200)}_"]
    if view.question is not None:
        lines += ["", "**Question:**", "", view.question.summary.strip()]
        if view.question.url:
            lines += ["", f"([asked here]({view.question.url}))"]
    if view.triage:
        lines += ["", "#### Triage", "", view.triage]
    if view.plan is not None:
        lines += ["", "#### Plan", "", _plan_line(view.plan)]
    if view.pr or view.deferred:
        lines += ["", "#### Pull request", ""]
        if view.pr:
            lines.append(view.pr)
        if view.rounds:
            lines += ["", f"**Review rounds:** {view.rounds}"]
        if view.deferred:
            lines.append(view.deferred)
    if view.checkpoint:
        lines += ["", "#### Last checkpoint", "", view.checkpoint]
    if view.splits:
        lines += ["", "#### Split out", "", *view.splits]
    lines += _history(history)
    return "\n".join(lines)


def _plan_line(plan: PlanView) -> str:
    if plan.skip:
        return "Built from the issue as written (no plan)."
    link = f"[Plan `{plan.prefix}`]({plan.url})" if plan.url else f"Plan `{plan.prefix}`"
    parts = [link]
    if not plan.approved:
        parts.append("not approved yet")
    else:
        parts.append("approved" if plan.approved == plan.prefix else f"approved `{plan.approved}`")
    if plan.newer:
        parts.append(f"a newer revision `{plan.newer}` is waiting for approval")
    if plan.revising:
        parts.append("a revision is being written")
    return " · ".join(parts)


def _history(history: Sequence[tuple[int, str]]) -> list[str]:
    if not history:
        return []
    events = [f"- {_clock(at)} · {text}" for at, text in reversed(history)]
    return ["", "<details><summary>History</summary>", "", *events, "", "</details>"]


def epic_headline(view: EpicView) -> str:
    building = sum(1 for s in view.subs if s.building)
    parts = [f"Epic · {view.done}/{view.total} done", f"{building} building"]
    parts.append(f"autopilot {view.autopilot.lower()}" if view.autopilot else "autopilot off")
    if view.next_issue is not None:
        parts.append(f"next: #{view.next_issue}")
    return " · ".join(parts)


def render_epic(view: EpicView, history: Sequence[tuple[int, str]], headline: str) -> str:
    lines = [f"### {headline}"]
    if view.paused:
        lines += ["", f"_Autopilot paused: {_one_line(view.paused, 200)}_"]
    if view.plan_url:
        lines += ["", f"{view.plan_label or 'Epic plan'}: {view.plan_url}"]
    open_subs = [s for s in view.subs if s.open]
    waiting = _waiting(view, open_subs)
    if waiting:
        lines += ["", "#### Waiting on people", "", *waiting]
    progress = [
        _sub_line(s)
        for s in open_subs
        if not s.gate and s.epic is None and _in_progress(s) and not _blocked(s)
    ]
    if progress:
        lines += ["", "#### In progress", "", *progress]
    blocked = [_blocked_line(s) for s in open_subs if not s.gate and _blocked(s)]
    if blocked:
        lines += ["", "#### Blocked", "", *blocked]
    nested = [_nested_line(s) for s in view.subs if s.epic is not None]
    if nested:
        lines += ["", "#### Nested epics", "", *nested]
    nxt = _up_next(view, open_subs)
    if nxt:
        lines += ["", "#### Up next", "", *nxt]
    held = [s for s in open_subs if s.held]
    if held:
        refs = ", ".join(f"#{s.number}" for s in held)
        lines += ["", f"**Follow-ups held:** {len(held)} ({refs})"]
    done = [s for s in view.subs if not s.open]
    if done:
        items = [
            f"- #{s.number} {_one_line(s.title, 120)}"
            + (f" (PR #{s.pr_number})" if s.pr_number else "")
            for s in done
        ]
        lines += [
            "",
            f"<details><summary>Done ({len(done)})</summary>",
            "",
            *items,
            "",
            "</details>",
        ]
    lines += _history(history)
    return "\n".join(lines)


def _in_progress(s: SubIssue) -> bool:
    return s.started and s.question is None and s.bot != BotState.NEEDS_YOU


def _blocked(s: SubIssue) -> bool:
    return bool(s.blockers) or s.bot == BotState.BLOCKED


def _sub_line(s: SubIssue) -> str:
    parts = [s.stage] if s.stage else []
    if s.pr_number:
        parts.append(f"PR #{s.pr_number}")
    if s.bot is not None:
        parts.append(_STATE_TEXT[s.bot])
    return f"- #{s.number} {_one_line(s.title, 120)} — {' · '.join(parts)}"


def _blocked_line(s: SubIssue) -> str:
    if s.blockers:
        refs = ", ".join(
            ref + (" (outside this epic)" if outside else "") for ref, outside in s.blockers
        )
        return f"- #{s.number} {_one_line(s.title, 120)} — waits on {refs}"
    reason = s.note.removeprefix("Blocked:").strip()
    return f"- #{s.number} {_one_line(s.title, 120)} — blocked" + (
        f": {_one_line(reason, 160)}" if reason else ""
    )


def _nested_line(s: SubIssue) -> str:
    done, total = s.epic or (0, 0)
    state = "" if s.open else " (closed)"
    return f"- #{s.number} {_one_line(s.title, 120)} — epic · {done}/{total} done{state}"


def _waiting(view: EpicView, open_subs: Sequence[SubIssue]) -> list[str]:
    groups: dict[str, list[str]] = {}
    for s in open_subs:
        if s.gate:
            groups.setdefault(view.gate_assignee, []).append(
                f"- #{s.number} {_one_line(s.title, 120)} (a step only a person can do)"
            )
        elif s.question is not None:
            groups.setdefault(view.question_assignee, []).append(
                f"- #{s.number} {_one_line(s.title, 80)}: {_one_line(s.question.summary, 160)}"
            )
        elif s.bot == BotState.NEEDS_YOU:
            reason = s.note.removeprefix("Needs you:").strip() or "needs a decision"
            groups.setdefault(view.question_assignee, []).append(
                f"- #{s.number} {_one_line(s.title, 80)}: {_one_line(reason, 160)}"
            )
    lines: list[str] = []
    for who, items in groups.items():
        lines += [*([""] if lines else []), f"**{who}**", *items]
    return lines


def _up_next(view: EpicView, open_subs: Sequence[SubIssue]) -> list[str]:
    waiting = [
        s
        for s in open_subs
        if not s.gate
        and s.epic is None
        and not _blocked(s)
        and not _in_progress(s)
        and s.question is None
        and s.bot != BotState.NEEDS_YOU
    ]
    position = {n: i for i, n in enumerate(view.order)}
    waiting.sort(key=lambda s: (position.get(s.number, len(position)), s.number))
    return [f"- #{s.number} {_one_line(s.title, 120)}" for s in waiting[:_UP_NEXT]]


# ------------------------------------------------------------------ the service


@dataclass(slots=True)
class StatusComments:
    """Keeps every handled issue's status comment current (see the module docstring)."""

    service: FactoryService
    client: GitHubClient
    directory: ServiceDispatchDirectory
    #: Makes agent text safe to post as the bot (mentions, HTML comments, credentials).
    publish_text: Callable[[str], str]
    #: The Deferred checkboxes (``service.deferred.Deferrals.report_section``); None: none.
    deferred_section: Callable[[Parcel], Awaitable[str]] | None = None
    _locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    #: Per epic, the sub-issue stamp (``_epic_stamps``) last rendered by ``tick`` (in
    #: memory: after a restart each finished epic is rendered once more).
    _shown_stamps: dict[str, object] = field(default_factory=dict)

    @property
    def _repo_id(self) -> str:
        return self.service.config.repo_id

    @property
    def _repo(self) -> str:
        return self.service.config.repository

    async def _db(self, fn: Callable[[SqliteStore], T]) -> T:
        return await self.service.db.call(fn)

    def _lock(self, parcel_id: str) -> asyncio.Lock:
        return self._locks.setdefault(parcel_id, asyncio.Lock())

    # -------------------------------------------------------------- adapter hooks

    async def folded(self, effect: EffectIntent) -> bool:
        """The publication is shown in the status comment instead of a comment of its own:
        a triage result (an epic triage keeps its comment) or a checkpoint report."""
        if effect.kind == EffectKind.PUBLISH_REPORT:
            return effect.args.get("report") == "checkpoint"
        if effect.kind != EffectKind.PUBLISH_TRIAGE:
            return False
        sid = str(effect.args.get("session_id") or effect.preconditions.session_id or "")
        body = self._result(sid)
        return body is not None and body.get("kind") == "triage"

    async def publish(self, parcel_id: str) -> StatusWrite:
        """Bring the status comment up to date now (no rate limit): before a folded
        publication is acknowledged, or before the Ready line links to it."""
        return await self.refresh(parcel_id, urgent=True)

    async def owns(self, parcel_id: str, comment_id: int) -> bool:
        row = await self._db(partial(st.status_row, repo_id=self._repo_id, parcel_id=parcel_id))
        return row is not None and row.comment_id == comment_id

    # -------------------------------------------------------------- the loop

    async def tick(self) -> int:
        """Write every status comment whose text changed (rate limit permitting).
        Returns how many were written."""
        settings = await self._db(partial(st.status_settings, repo_id=self._repo_id))
        rows = {
            r.parcel_id: r for r in await self._db(partial(st.status_rows, repo_id=self._repo_id))
        }
        candidates = [
            _Candidate.of(r)
            for r in await self._db(
                lambda store: store.query(
                    # One extract: each further path parses the aggregate again.
                    "SELECT parcel_id, version, updated_at_us, stage, issue_number, holds_json, "
                    "json_extract(aggregate_json, '$.parcel.links') "
                    "FROM parcels WHERE repo_id = ? AND issue_number IS NOT NULL "
                    "ORDER BY issue_number",
                    (self._repo_id,),
                )
            )
        ]
        stamps = await self._epic_stamps(candidates)
        written = 0
        for c in candidates:
            row = rows.get(c.parcel_id)
            if row is None and c.updated_at_us <= settings.baseline_us and not c.open_epic:
                # Unchanged since status comments started: no backfill sweep. An open
                # epic is the exception (its sub-issues' work never changes it).
                continue
            if (
                row is not None
                and not row.pending
                and row.state == "posted"
                and row.seen_version == c.version
                and c.stage == Stage.DONE.value
                and self._shown_stamps.get(c.parcel_id) == stamps.get(c.parcel_id)
            ):
                continue  # finished and shown (an epic: its sub-issues unchanged too)
            try:
                outcome = await self.refresh(c.parcel_id)
            except Exception:
                LOG.warning("status comment failed parcel=%s", c.parcel_id, exc_info=True)
                continue
            if c.parcel_id in stamps:
                self._shown_stamps[c.parcel_id] = stamps[c.parcel_id]
            written += int(outcome.written)
        return written

    async def _epic_stamps(self, candidates: Sequence[_Candidate]) -> dict[str, object]:
        """Per epic: what of its sub-issues its status shows depends on (each sub-issue's
        parcel version and held follow-ups), from the epic's stored sub-issue list and the
        sub-issues' stored parent links. A change marks the epic for a re-render."""
        epics = [c for c in candidates if c.epic]
        if not epics:
            return {}
        by_number = {c.issue_number: c for c in candidates}
        children: dict[int, set[int]] = {}
        for c in candidates:
            if c.parent is not None:
                children.setdefault(c.parent, set()).add(c.issue_number)
        follow_ups = await self._db(lambda store: store.follow_ups(self._repo_id))
        held = {f.issue_number for f in follow_ups if f.hold == "held"}
        stamps: dict[str, object] = {}
        for e in epics:
            numbers = sorted(set(e.sub_numbers) | children.get(e.issue_number, set()))
            stamps[e.parcel_id] = tuple(
                (
                    n,
                    by_number[n].version if n in by_number else None,
                    n in held,
                )
                for n in numbers
            )
        return stamps

    async def refresh(self, parcel_id: str, *, urgent: bool = False) -> StatusWrite:
        """Render the parcel's status and write it when it changed (create, adopt by
        marker, or edit in place). ``urgent`` skips the per-issue edit rate limit."""
        async with self._lock(parcel_id):
            return await self._refresh(parcel_id, urgent)

    async def _refresh(self, parcel_id: str, urgent: bool) -> StatusWrite:
        parcel = await self._db(lambda store: store.load_parcel(parcel_id))
        if parcel is None or parcel.issue_number is None:
            return StatusWrite(ok=False, reason="no issue")
        row = await self._db(partial(st.status_row, repo_id=self._repo_id, parcel_id=parcel_id))
        if row is None:
            shows = _handled(parcel) or _open_epic(parcel)
            if not urgent and (not shows or Hold.COMPLETED in parcel.holds):
                return StatusWrite(ok=True, reason="nothing to show")
            row = st.StatusRow(parcel_id, parcel.issue_number, state="creating")
        headline, render = await self._render(parcel)
        now = self.service.clock.now_utc_us()
        row = st.with_event(row, now, headline)
        body = render(row.history)
        if len(body) > _BODY_LIMIT:
            body = body[: _BODY_LIMIT - 30].rstrip() + "\n\n[truncated]"
        digest = _digest(body)
        stale = row.seen_version != parcel.version
        row = replace(row, seen_version=parcel.version, issue_number=parcel.issue_number)
        if row.state == "posted" and row.comment_id is not None and row.digest == digest:
            if row.pending or stale:
                await self._put(replace(row, pending=False))
            return self._ok(row, written=False)
        interval = int(self.service.config.status_comment_edit_seconds * 1_000_000)
        if (
            not urgent
            and row.state == "posted"
            and row.written_at_us
            and now < row.written_at_us + interval
        ):
            await self._put(replace(row, pending=True))
            return self._ok(row, written=False)
        return await self._write(parcel, row, body, digest, now)

    def _ok(self, row: st.StatusRow, *, written: bool) -> StatusWrite:
        return StatusWrite(
            ok=True,
            comment_id=row.comment_id,
            url=row.url or _comment_url(self._repo, row.issue_number, row.comment_id),
            written=written,
        )

    async def _put(self, row: st.StatusRow) -> None:
        await self._db(partial(st.put_status, repo_id=self._repo_id, row=row))

    # -------------------------------------------------------------- GitHub writes

    async def _write(
        self, parcel: Parcel, row: st.StatusRow, body: str, digest: str, now: int
    ) -> StatusWrite:
        del parcel
        issue = row.issue_number
        try:
            current: str | None = None
            if row.state == "posted" and row.comment_id is not None:
                current = await self._read(row.comment_id)
                if current is None:
                    # Deleted on GitHub: found again by its marker, else created again.
                    LOG.info("status comment gone issue=#%s comment=%s", issue, row.comment_id)
                    row = replace(row, state="creating", comment_id=None, node_id="", url="")
                    row = replace(row, pin="")
            if row.state != "posted" or row.comment_id is None:
                # Never a second comment: a create that may have landed is found first.
                found = await self._find(issue)
                if found is None:
                    row = replace(row, state="creating", pending=True)
                    await self._put(row)
                    return await self._create(row, body, digest, now)
                comment_id, node_id, url, current = found
                row = replace(row, state="posted", comment_id=comment_id, node_id=node_id, url=url)
                LOG.info("status comment adopted issue=#%s comment=%s", issue, comment_id)
            assert row.comment_id is not None and current is not None  # noqa: S101
            # The owner's ticks not yet recorded stay ticked (recorded ones render ticked).
            new = f"{rl.keep_ticks(current, body)}\n\n{status_marker(issue)}"
            if new != current:
                await self.client.request(
                    "PATCH",
                    f"/repos/{self._repo}/issues/comments/{row.comment_id}",
                    json_body={"body": new},
                    expected=frozenset({200}),
                )
                LOG.info("status comment edited issue=#%s comment=%s", issue, row.comment_id)
            row = replace(row, digest=digest, written_at_us=now, pending=False)
            await self._put(row)
            if not row.pin:
                row = await self._pin(row)  # an adopted comment whose pin was never tried
            return self._ok(row, written=new != current)
        except AmbiguousRequest as exc:
            # Unclear: an edit is repeated, a create is looked up by its marker first.
            await self._put(replace(row, pending=True))
            return StatusWrite(ok=False, reason=str(exc))
        except RateLimited as exc:
            await self._put(replace(row, pending=True))
            return StatusWrite(ok=False, reason=str(exc), retry_after_us=exc.retry_after_us)
        except GitHubRejected as exc:
            await self._put(replace(row, pending=True))
            return StatusWrite(ok=False, reason=str(exc), definitive=exc.status_code < 500)
        except GitHubAPIError as exc:
            await self._put(replace(row, pending=True))
            return StatusWrite(ok=False, reason=str(exc))

    async def _create(self, row: st.StatusRow, body: str, digest: str, now: int) -> StatusWrite:
        issue = row.issue_number
        response = await self.client.request(
            "POST",
            f"/repos/{self._repo}/issues/{issue}/comments",
            json_body={"body": f"{body}\n\n{status_marker(issue)}"},
            expected=frozenset({201}),
        )
        try:
            data: Any = response.json()
        except ValueError:
            data = None
        comment_id = data.get("id") if isinstance(data, dict) else None
        if not isinstance(comment_id, int):
            # It may exist: the next attempt finds it by its marker.
            return StatusWrite(ok=False, reason="status comment response omitted its id")
        row = replace(
            row,
            state="posted",
            comment_id=comment_id,
            node_id=str(data.get("node_id") or ""),
            url=str(data.get("html_url") or ""),
            digest=digest,
            written_at_us=now,
            pending=False,
        )
        await self._put(row)
        LOG.info("status comment created issue=#%s comment=%s", issue, comment_id)
        row = await self._pin(row)
        return self._ok(row, written=True)

    async def _pin(self, row: st.StatusRow) -> st.StatusRow:
        """Pin a new status comment once; a refusal is recorded for the installation."""
        if row.pin:
            return row
        settings = await self._db(partial(st.status_settings, repo_id=self._repo_id))
        if settings.pin_state == "unsupported" or not row.node_id or "/issues/" not in row.url:
            # Never on a pull request (its comments are under /pull/), never retried.
            row = replace(row, pin="skipped")
            await self._put(row)
            return row
        try:
            await self.client.graphql(_PIN, {"id": row.node_id})
        except (GitHubAPIError, AmbiguousRequest) as exc:
            row = replace(row, pin="failed")
            await self._put(row)
            first = await self._db(
                partial(
                    st.set_pin_state, repo_id=self._repo_id, state="unsupported", message=str(exc)
                )
            )
            if first:
                LOG.warning(
                    "status comment pinning unsupported, not tried again issue=#%s reason=%s",
                    row.issue_number,
                    exc,
                )
            return row
        row = replace(row, pin="pinned")
        await self._put(row)
        await self._db(partial(st.set_pin_state, repo_id=self._repo_id, state="supported"))
        LOG.info("status comment pinned issue=#%s comment=%s", row.issue_number, row.comment_id)
        return row

    async def _find(self, issue: int) -> tuple[int, str, str, str] | None:
        """(id, node id, url, body) of the bot's status comment on ``issue`` (the oldest
        when there are several)."""
        marker = status_marker(issue)
        bot = self.service.config.github_bot_user_id
        comments = await self.client.paginate(
            f"/repos/{self._repo}/issues/{issue}/comments?per_page=100"
        )
        found = sorted(
            (
                c
                for c in comments
                if isinstance(c, dict)
                and isinstance(c.get("id"), int)
                and marker in str(c.get("body") or "")
                and isinstance(c.get("user"), dict)
                and c["user"].get("id") == bot
            ),
            key=lambda c: int(c["id"]),
        )
        if len(found) > 1:
            LOG.warning("several status comments issue=#%s: keeping the oldest", issue)
        if not found:
            return None
        c = found[0]
        return (
            int(c["id"]),
            str(c.get("node_id") or ""),
            str(c.get("html_url") or ""),
            str(c.get("body") or ""),
        )

    async def _read(self, comment_id: int) -> str | None:
        try:
            data: Any = await self.client.get_json(
                f"/repos/{self._repo}/issues/comments/{comment_id}"
            )
        except GitHubRejected as exc:
            if exc.status_code == 404:
                return None
            raise
        return str(data.get("body") or "") if isinstance(data, dict) else ""

    # -------------------------------------------------------------- reading state

    def _result(self, session_id: str) -> dict[str, Any] | None:
        if not session_id:
            return None
        stored = self.directory.latest_result(session_id)
        record = stored.get("factory_result") if stored is not None else None
        body = record.get("result") if isinstance(record, dict) else None
        return body if isinstance(body, dict) else None

    async def _render(
        self, parcel: Parcel
    ) -> tuple[str, Callable[[Sequence[tuple[int, str]]], str]]:
        """The headline, and the body for a given history (which ends with it)."""
        if parcel.links is not None and parcel.links.epic:
            epic = await self._epic_view(parcel)
            headline = epic_headline(epic)
            return headline, partial(_epic_body, epic, headline)
        view = await self._issue_view(parcel)
        headline = issue_headline(view)
        return headline, partial(_issue_body, view, headline)

    async def _issue_view(self, parcel: Parcel) -> IssueView:
        config = self.service.config
        assert parcel.issue_number is not None  # noqa: S101 - callers check
        stage = parcel.stage
        stage_name = config.status_names.get(stage.value, stage.value) if stage else "Inbox"
        triage = self.directory.latest_triage(parcel)
        triage_text = ""
        if triage is not None and triage.get("kind") == "triage":
            triage_text = self.publish_text(
                _without_heading(stage_result_text(triage, agent_display_name(config)))
            )
        r = parcel.readiness
        pr_text = ""
        checks = ""
        if r is not None:
            build = self._result(r.session_id)
            args: dict[str, Any] = {
                "pr_number": r.pr_number,
                "head_sha": r.head_sha,
                "checks_summary": r.checks_summary,
                "review_bot": r.review_bot,
            }
            if r.sync_red:
                args["red_checks"] = r.red_checks
            pr_text = self.publish_text(
                _without_heading(
                    ready_text(args, build, r.checks_summary, self._repo, review_bot_name(config))
                )
            )
            checks = _checks(parcel)
        deferred = ""
        if self.deferred_section is not None and (r is not None or parcel.pr_number):
            deferred = await self.deferred_section(parcel)
        checkpoint = ""
        session = parcel.current_session
        if session is not None:
            latest = self._result(session.session_id)
            if latest is not None and latest.get("kind") == "checkpoint":
                checkpoint = self.publish_text(
                    _without_heading(stage_result_text(latest, agent_display_name(config)))
                )
        return IssueView(
            issue=parcel.issue_number,
            stage=stage_name,
            bot=parcel.bot,
            note=parcel.note or parcel.board_note,
            finished=Hold.COMPLETED in parcel.holds,
            question=await self._question(parcel),
            pr_number=r.pr_number if r is not None else parcel.pr_number,
            checks=checks,
            rounds=r.review_rounds_note if r is not None else "",
            triage=triage_text,
            plan=self._plan(parcel),
            pr=pr_text,
            deferred=deferred,
            checkpoint=checkpoint,
            splits=await self._splits(parcel.issue_number),
        )

    def _plan(self, parcel: Parcel) -> PlanView | None:
        approval = parcel.current_approval
        valid = approval if approval is not None and approval.valid else None
        if valid is not None and valid.kind == ApprovalKind.SKIP:
            return PlanView(url="", prefix="", skip=True)
        published = [c for c in parcel.contracts if c.published]
        if not published:
            return None
        latest = published[-1]
        issue = parcel.issue_number or 0
        url = (
            _comment_url(self._repo, issue, int(latest.comment_id))
            if latest.comment_id and latest.comment_id.isdigit()
            else ""
        )
        approved = valid.full_hash if valid is not None else ""
        return PlanView(
            url=url,
            prefix=latest.prefix,
            approved=approved[:12],
            newer=latest.prefix if approved and latest.full_hash != approved else "",
            revising=parcel.revision_pending,
        )

    async def _question(self, parcel: Parcel) -> Question | None:
        open_ids = {d.decision_id for d in parcel.decisions if d.status == DecisionStatus.OPEN}
        if not open_ids:
            return None
        rows = await self._db(
            lambda store: store.query(
                "SELECT payload_json, remote_id FROM effects WHERE parcel_id = ? AND kind = ? "
                "ORDER BY created_at_us DESC",
                (parcel.parcel_id, EffectKind.POST_COMMENT.value),
            )
        )
        for payload_json, remote_id in rows:
            try:
                args = json.loads(str(payload_json)).get("args") or {}
            except ValueError:
                continue
            if args.get("template") != "decision" or args.get("decision_id") not in open_ids:
                continue
            summary = self.publish_text(str(args.get("summary") or "").strip())
            url = (
                _comment_url(self._repo, parcel.issue_number or 0, int(str(remote_id)))
                if remote_id is not None and str(remote_id).isdigit()
                else ""
            )
            return Question(summary or "I have a question before I can go on.", url)
        return Question("I have a question before I can go on.")

    async def _splits(self, source: int) -> tuple[str, ...]:
        items = await self._db(partial(ds.splits, repo_id=self._repo_id, source_issue=source))
        return tuple(
            f"- #{s.issue_number}: {_one_line(self.publish_text(s.title), 200)}"
            if s.issue_number is not None
            else f"- {_one_line(self.publish_text(s.title), 200)} (being filed)"
            for s in items
        )

    async def _epic_view(self, parcel: Parcel) -> EpicView:
        config = self.service.config
        links = parcel.links
        assert links is not None and parcel.issue_number is not None  # noqa: S101
        subs_links = links.sub_issues
        numbers = [s.number for s in subs_links if not s.repo]
        rows = await self._db(
            lambda store: store.query(
                "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND issue_number IN "
                "(SELECT value FROM json_each(?))",
                (self._repo_id, json.dumps(numbers)),
            )
        )
        by_number = {p.issue_number: p for p in (parcel_from_json(str(r[0])) for r in rows)}
        follow_ups = await self._db(lambda store: store.follow_ups(self._repo_id))
        held = {f.issue_number for f in follow_ups if f.hold == "held"}
        gates = {g.number for g in parcel.autopilot_gates}
        inside = set(numbers)
        subs: list[SubIssue] = []
        for link in subs_links:
            subs.append(await self._sub(link, by_number.get(link.number), gates, held, inside))
        ap = parcel.autopilot
        plan = approved_epic_plan(self.directory, ap) if ap is not None else None
        triage = plan or self.directory.latest_triage(parcel)
        order = tuple(
            int(item["issue"])
            for item in (triage.get("build_order") or [] if triage is not None else [])
            if isinstance(item, dict) and isinstance(item.get("issue"), int)
        )
        plan_url, plan_label = await self._epic_plan_link(parcel)
        single = len(config.owners) == 1
        return EpicView(
            issue=parcel.issue_number,
            done=links.sub_completed or sum(1 for s in subs_links if not s.open),
            total=links.sub_total or len(subs_links),
            autopilot=ap.level if ap is not None else "",
            subs=tuple(subs),
            order=order,
            next_issue=_next_issue(parcel.epic_note, subs, order),
            plan_url=plan_url,
            plan_label=plan_label,
            gate_assignee="You" if single or ap is None else f"Owner (GitHub user {ap.owner_id})",
            question_assignee="You" if single else "Owners",
            paused=ap.paused if ap is not None else "",
        )

    async def _sub(
        self,
        link: LinkedIssue,
        sub: Parcel | None,
        gates: set[int],
        held: set[int],
        inside: set[int],
    ) -> SubIssue:
        config = self.service.config
        base = SubIssue(
            number=link.number,
            title=self.publish_text(link.title),
            open=link.open,
            gate=link.number in gates,
            held=link.number in held,
        )
        if sub is None:
            return base
        stage = sub.stage
        blockers: tuple[tuple[str, bool], ...] = ()
        if sub.links is not None:
            blockers = tuple(
                (b.ref, bool(b.repo) or b.number not in inside) for b in sub.links.open_blockers
            )
        epic = (
            (sub.links.sub_completed, sub.links.sub_total)
            if sub.links is not None and sub.links.epic
            else None
        )
        return replace(
            base,
            stage=config.status_names.get(stage.value, stage.value) if stage else "",
            bot=sub.bot,
            note=sub.note or sub.board_note,
            pr_number=sub.readiness.pr_number if sub.readiness is not None else sub.pr_number,
            question=await self._question(sub),
            blockers=blockers,
            epic=epic,
            building=stage == Stage.BUILDING,
            started=_handled(sub) and Hold.COMPLETED not in sub.holds,
        )

    async def _epic_plan_link(self, parcel: Parcel) -> tuple[str, str]:
        """The epic plan comment (autopilot's, else the newest epic triage comment)."""
        ap = parcel.autopilot
        effect_ids = [ap.plan.effect_id] if ap is not None and ap.plan is not None else []
        rows = await self._db(
            lambda store: store.query(
                "SELECT effect_id, remote_id FROM effects WHERE parcel_id = ? AND kind = ? "
                "AND remote_id IS NOT NULL ORDER BY created_at_us DESC",
                (parcel.parcel_id, EffectKind.PUBLISH_TRIAGE.value),
            )
        )
        row = await self._db(
            partial(st.status_row, repo_id=self._repo_id, parcel_id=parcel.parcel_id)
        )
        own = str(row.comment_id) if row is not None and row.comment_id is not None else ""
        # A folded triage result's acknowledgement names the status comment itself.
        found = [(str(e), str(r)) for e, r in rows if str(r) != own]
        remote = dict(found)
        for effect_id in effect_ids:
            if remote.get(effect_id, "").isdigit():
                url = _comment_url(self._repo, parcel.issue_number or 0, int(remote[effect_id]))
                return url, f"Epic plan `{ap.plan.prefix}`" if ap and ap.plan else "Epic plan"
        for _effect_id, remote_id in found:
            if remote_id.isdigit():
                return _comment_url(self._repo, parcel.issue_number or 0, int(remote_id)), (
                    "Epic triage"
                )
        return "", ""


def _issue_body(view: IssueView, headline: str, history: Sequence[tuple[int, str]]) -> str:
    return render_issue(view, history, headline)


def _epic_body(view: EpicView, headline: str, history: Sequence[tuple[int, str]]) -> str:
    return render_epic(view, history, headline)


def _handled(parcel: Parcel) -> bool:
    """The factory has something to show: a stage run started, or epic autopilot."""
    return bool(parcel.sessions) or parcel.autopilot is not None


def _open_epic(parcel: Parcel) -> bool:
    """An epic (stored links) neither Done nor closed: its sub-issues are worth showing."""
    return (
        parcel.links is not None
        and parcel.links.epic
        and parcel.stage != Stage.DONE
        and Hold.COMPLETED not in parcel.holds
    )


@dataclass(frozen=True, slots=True)
class _Candidate:
    """One parcel as ``tick`` sees it (columns and stored links, no aggregate decode)."""

    parcel_id: str
    version: int
    updated_at_us: int
    stage: str | None
    issue_number: int
    epic: bool
    open_epic: bool
    #: The parent issue in this repository (stored links), if any.
    parent: int | None
    sub_numbers: tuple[int, ...]

    @classmethod
    def of(cls, r: Sequence[Any]) -> _Candidate:
        parcel_id, version, updated, stage, number, holds_json, links_json = r
        links = json.loads(str(links_json)) if links_json else None
        links = links if isinstance(links, dict) else {}
        subs = links.get("sub_issues") or []
        sub_numbers = tuple(
            int(s["number"])
            for s in subs
            if isinstance(s, dict) and not s.get("repo") and isinstance(s.get("number"), int)
        )
        epic = int(links.get("sub_total") or 0) > 0 or bool(subs)
        up = links.get("parent")
        parent = up.get("number") if isinstance(up, dict) and not up.get("repo") else None
        completed = Hold.COMPLETED.value in json.loads(str(holds_json or "[]"))
        return cls(
            parcel_id=str(parcel_id),
            version=int(version),
            updated_at_us=int(updated),
            stage=None if stage is None else str(stage),
            issue_number=int(number),
            epic=epic,
            open_epic=epic and stage != Stage.DONE.value and not completed,
            parent=parent if isinstance(parent, int) else None,
            sub_numbers=sub_numbers,
        )


def _checks(parcel: Parcel) -> str:
    r = parcel.readiness
    if r is None:
        return ""
    if r.red_checks and (r.sync_red or Hold.CHECKS_FAILED in parcel.holds):
        return "required check red"
    if r.ready:
        return "checks green"
    return "checks running"


_NEXT = re.compile(r"next: #(\d+)")


def _next_issue(note: str, subs: Iterable[SubIssue], order: Sequence[int]) -> int | None:
    match = _NEXT.search(note or "")
    if match:
        return int(match.group(1))
    candidates = [
        s.number
        for s in subs
        if s.open and not s.gate and s.epic is None and not s.started and not _blocked(s)
    ]
    for number in order:
        if number in candidates:
            return number
    return min(candidates) if candidates else None


def pin_status(settings: Mapping[str, object] | None) -> str:
    """``doctor``'s line for the stored pin state."""
    if not settings:
        return "not tried yet (no status comment created)"
    state = settings.get("pin_state")
    if state == "supported":
        return "status comments are pinned"
    if state == "unsupported":
        return f"pinning is not available to the App installation: {settings.get('pin_message')}"
    return "not tried yet (no status comment created)"


__all__ = [
    "EpicView",
    "IssueView",
    "PlanView",
    "Question",
    "StatusComments",
    "SubIssue",
    "epic_headline",
    "issue_headline",
    "pin_status",
    "render_epic",
    "render_issue",
    "status_marker",
]
