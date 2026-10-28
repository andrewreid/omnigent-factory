"""Issue bundling: related issues built as one PR, in one worktree and one Rosie session,
on the owner's confirmation. Switched like auto-triage (host config
``bundling``, ``omnigent-factory bundles on|off``).

* **Proposed.** While bundling is on, the triage ranking may propose ``bundles`` (members,
  theme, reason naming the shared area). Each is validated deterministically
  (:func:`validate_bundle`; a violation fails the ranking submission with a precise error)
  and recorded once per member set (``store.bundles``): the same set proposed again is the
  same proposal, never a second comment. One bot comment on the lowest-numbered member
  asks the owner with ``- [ ] Build these together as one PR``; the ranking's status
  update names it. A proposal (and its tick) keeps the suggestion rules: at most
  ``bundle_max_members`` small (S or M) issues within ``bundle_max_points``. A member that
  becomes ineligible before the tick makes the proposal stale (its comment says so);
  nothing else happens.
* **Confirmed.** Only an owner's tick counts (the comment's ``edited`` webhook from an
  owner's numeric id, diffed against the previous body; a lost webhook is recovered from
  the comment's ``userContentEdits`` when an owner made that edit), or an owner's
  ``/bundle #a #b ...`` on a member (validated by the same protective rules, but sized by
  the owner: up to ``bundle_command_max_members``, L or unsized members and any points).
  The factory then opens a synthetic bundle issue (``Bundle: <theme> (#a, #b)``, label
  ``factory:bundle``, a hidden marker: an unclear create is found by its marker first, so
  never twice), never linking the members as its sub-issues (that would make it an
  epic); it joins the members' shared epic (as a sub-issue of it), takes their highest
  priority and starts planning (the owner's confirmation is the plan control). Each member gets the
  ``bundled`` hold: it is never triaged, planned or built on its own, auto-build,
  autopilot, auto-triage and ranking leave it alone, its card notes ``In bundle #X`` and
  mirrors the bundle's Bot.
* **Built.** The bundle issue runs the normal plan -> approval -> build -> Ready flow;
  ``factory_get_issue``/``factory_get_plan`` list ``bundle_members`` (title, text,
  triage). Its plan names every member in its acceptance criteria, and it is Ready only
  when the PR closes the bundle issue and every member still in it. A run may eject a
  member (``ejected``): the hold is removed and the member goes back to Triage.
* **Finished.** The PR's merge closes the members (GitHub); a member whose close webhook
  was lost is re-read when the bundle completes and on the board diff. Closing the bundle
  issue unmerged, assigning it to a person or ``/unbundle`` on it releases every member;
  closing, assigning or ``factory:skip`` on one member ejects only that one.

Off: no proposal is made and no bundle is created (pending proposals stay inert);
confirmed bundles carry on.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any, TypeVar

from omnigent_factory.core import events as ev
from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.projection import bundle_member_refusal
from omnigent_factory.core.types import (
    MICROS_PER_SECOND,
    BotState,
    Hold,
    IssueSnapshot,
    Parcel,
    Stage,
    Via,
)
from omnigent_factory.github.client import GitHubAPIError, GitHubClient
from omnigent_factory.github.links import (
    LinkTarget,
    add_label,
    add_sub_issue,
    ensure_label,
    read_target,
)
from omnigent_factory.github.webhook import label_name
from omnigent_factory.ports.github import IssueRef, RankingCard
from omnigent_factory.service.deferred import comment_edits, find_marked_issue
from omnigent_factory.service.directory import untrusted_block
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import bundles as bs
from omnigent_factory.store.sqlite import ApplyResult, AutoTriageState, SqliteStore

LOG = logging.getLogger(__name__)
T = TypeVar("T")

BUNDLE_LABEL = "factory:bundle"
_LABEL_DESCRIPTION = "Several issues the factory builds as one pull request"
SKIP_LABEL = "factory:skip"
NEEDS_REVIEW_LABEL = "needs-review"
#: Size points of a member (the factory never suggests an L or unsized issue).
POINTS = {"S": 1, "M": 3}
#: Hard ceiling of members, whatever the config says.
MAX_MEMBERS = 12
#: Bundles one ranking may propose.
MAX_PROPOSALS = 5
THEME_MAX = 80
REASON_MAX = 600
AREA_MAX = 200
CONFIRM_TEXT = "Build these together as one PR"
_COMMAND = re.compile(r"^/(?P<name>bundle|unbundle)(?:\s+(?P<args>.*))?$", re.DOTALL)
_REF = re.compile(r"(?<![\w/&])#(\d+)\b")
#: ``bundle_member_refusal`` reasons the other rules do not already name.
_IN_FLIGHT = frozenset(
    {
        "work on it is running or queued",
        "it has an auto-build mark",
        "epic autopilot drives it",
    }
)

TargetReader = Callable[[int], Awaitable[LinkTarget | None]]
SnapshotReader = Callable[[IssueRef], Awaitable[IssueSnapshot | RetryableReadFailure]]
CardReader = Callable[[], Awaitable[list[RankingCard] | RetryableReadFailure]]


def proposal_marker(key: str) -> str:
    return f"<!-- factory-bundle-proposal key={key} -->"


def bundle_marker(key: str) -> str:
    return f"<!-- factory-bundle key={key} -->"


def _one_line(text: object, limit: int) -> str:
    value = " ".join(str(text or "").split()).replace("→", "->")
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _refs(numbers: Iterable[int]) -> str:
    items = [f"#{n}" for n in numbers]
    if len(items) <= 2:
        return " and ".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _time_us(value: object) -> int:
    if not isinstance(value, str):
        return 0
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return int(stamp.timestamp() * MICROS_PER_SECOND)


# ------------------------------------------------------------------ validation


@dataclass(frozen=True, slots=True)
class MemberFacts:
    """What the rules need to know about one proposed member (stored state plus a fresh
    label read)."""

    number: int
    parcel: Parcel | None
    #: The issue's labels (None: could not be read).
    labels: frozenset[str] | None
    triage: Mapping[str, Any] | None
    #: The board's Priority, else the triage's.
    priority: str | None
    #: Its epic (parent issue in this repository) and whether that epic is on autopilot.
    parent: int | None = None
    parent_autopilot: bool = False
    #: Its build is queued (an admission queue entry).
    queued: bool = False
    #: The active bundle issue it is held in (0: none; -1: a bundle being created).
    bundled: int = 0
    #: Open issues it depends on outside the proposed members (triage ``depends_on``).
    open_dependencies: tuple[int, ...] = ()


def member_points(facts: MemberFacts) -> int | None:
    """Size points (None: unsized, or L)."""
    size = facts.triage.get("size") if facts.triage is not None else None
    if not isinstance(size, str) and facts.parcel is not None and facts.parcel.size is not None:
        size = facts.parcel.size.value
    return POINTS.get(str(size)) if size is not None else None


def validate_bundle(
    members: Sequence[int],
    facts: Mapping[int, MemberFacts],
    *,
    max_members: int,
    max_points: int,
    explicit: bool = False,
) -> list[str]:
    """Every rule a bundle breaks (empty: it may be built as one PR). ``explicit``: an
    owner's ``/bundle``, which sizes the bundle itself (no L/unsized or points rule; the
    caller passes its own ``max_members``); every protective rule still applies."""
    errors: list[str] = []
    limit = min(max_members, MAX_MEMBERS)
    numbers = list(members)
    if len(set(numbers)) != len(numbers):
        errors.append("each member at most once")
    unique = sorted(set(numbers))
    if not 2 <= len(unique) <= limit:
        errors.append(f"a bundle has 2 to {limit} members ({len(unique)} given)")
    points = 0
    parents: set[int] = set()
    for n in unique:
        f = facts.get(n)
        p = f.parcel if f is not None else None
        if f is None or p is None:
            errors.append(f"#{n} is not known to the factory (never triaged)")
            continue
        if p.stage not in (Stage.TRIAGED, Stage.SCOPED) or f.triage is None:
            errors.append(f"#{n} is not in Triage or Planning with a triage result")
        if p.stage in (Stage.BUILDING, Stage.READY):
            errors.append(f"#{n} is already in {p.stage.value}")
        size = member_points(f)
        if size is not None:
            points += size
        elif not explicit:
            errors.append(f"#{n} is size L or unsized: only S and M issues are bundled")
        if p.links is not None and p.links.epic:
            errors.append(f"#{n} is an epic")
        if f.parent is not None:
            parents.add(f.parent)
            if f.parent_autopilot:
                errors.append(f"#{n} is a sub-issue of epic #{f.parent}, which is on autopilot")
        if not p.eligible:
            errors.append(f"#{n} is closed, assigned to a person or unverified")
        if f.labels is None:
            errors.append(f"#{n}: its labels could not be read")
        else:
            for label in (SKIP_LABEL, NEEDS_REVIEW_LABEL):
                if label in f.labels:
                    errors.append(f"#{n} is labelled {label}")
        blockers = [
            b.ref
            for b in (p.links.open_blockers if p.links is not None else ())
            if b.repo or b.number not in unique
        ]
        if blockers:
            errors.append(f"#{n} is blocked by open {', '.join(blockers)}")
        if f.open_dependencies:
            deps = ", ".join(f"#{d}" for d in f.open_dependencies)
            errors.append(f"#{n} depends on open {deps}")
        if f.priority == "P0":
            errors.append(f"#{n} is P0: build it on its own")
        if p.open_decisions or p.bot == BotState.NEEDS_YOU:
            errors.append(f"#{n} needs you (an open question or decision)")
        if f.bundled:
            where = f"bundle #{f.bundled}" if f.bundled > 0 else "a bundle being created"
            errors.append(f"#{n} is already in {where}")
        else:
            refusal = bundle_member_refusal(p, queued=f.queued)
            if refusal in _IN_FLIGHT:
                errors.append(f"#{n}: {refusal}")
    if points > max_points and not explicit:
        errors.append(f"{points} size points (S = 1, M = 3): at most {max_points}")
    if len(parents) > 1:
        errors.append(
            "members of different epics (" + ", ".join(f"#{e}" for e in sorted(parents)) + ")"
        )
    return list(dict.fromkeys(errors))


def parse_proposals(raw: object) -> tuple[list[bs.NewProposal], list[str]]:
    """The shape of a ranking's ``bundles`` (the rules are checked separately)."""
    errors: list[str] = []
    found: list[bs.NewProposal] = []
    if raw in (None, []):
        return found, errors
    if not isinstance(raw, list) or len(raw) > MAX_PROPOSALS:
        return found, [f"bundles: a list of at most {MAX_PROPOSALS}"]
    seen: dict[int, int] = {}
    for position, item in enumerate(raw, start=1):
        where = f"bundles[{position}]"
        if not isinstance(item, Mapping):
            errors.append(f"{where}: needs members, theme and reason")
            continue
        members = item.get("members")
        if not isinstance(members, list) or not all(
            isinstance(n, int) and not isinstance(n, bool) and n > 0 for n in members
        ):
            errors.append(f"{where}: members must be issue numbers")
            continue
        theme = _one_line(item.get("theme"), THEME_MAX + 1)
        reason = " ".join(str(item.get("reason") or "").split())
        area = _one_line(item.get("shared_area"), AREA_MAX + 1)
        if not theme or len(theme) > THEME_MAX:
            errors.append(f"{where}: theme is required (at most {THEME_MAX} characters)")
        if not reason or len(reason) > REASON_MAX:
            errors.append(
                f"{where}: reason is required (at most {REASON_MAX} characters), naming the "
                "area of code the members share"
            )
        if len(area) > AREA_MAX:
            errors.append(f"{where}: shared_area at most {AREA_MAX} characters")
        for n in set(members):
            if n in seen:
                errors.append(f"#{n} is in bundles[{seen[n]}] and {where}")
            seen[n] = position
        found.append(bs.NewProposal(tuple(members), theme, reason, area))
    return found, errors


# ------------------------------------------------------------------ the service


class Bundles:
    def __init__(
        self,
        service: FactoryService,
        client: GitHubClient,
        directory: Any,  # noqa: ANN401 - service.directory.ServiceDispatchDirectory
        publish: Callable[[str], str],
        *,
        snapshot: SnapshotReader | None = None,
        cards: CardReader | None = None,
        set_priority: Callable[[str, str], Awaitable[None]] | None = None,
    ) -> None:
        self.service = service
        self.client = client
        self.directory = directory
        #: Makes agent text safe to post as the bot (mentions, HTML comments, credentials).
        self.publish = publish
        #: A fresh issue read (evidence for the bundle issue's plan control).
        self.snapshot = snapshot
        #: The board's Rank/Priority cards (the bundle card's item and the members'
        #: Priority); None: not wired.
        self.cards = cards
        self.set_priority = set_priority
        self._lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[object]] = set()

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
        task.set_name("bundles")
        self._tasks.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            LOG.warning("bundle work failed", exc_info=task.exception())

    async def drain(self) -> None:
        """Wait for the background work started so far (tests, shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # -------------------------------------------------------------- switch

    async def _state(self) -> AutoTriageState:
        repo_id = self._repo_id
        state: AutoTriageState = await self._db(lambda store: store.bundling_state(repo_id))
        return state

    async def enabled(self) -> tuple[bool, str]:
        """Whether bundling is on, and where that comes from (config or operator)."""
        configured = self.service.config.bundling
        state = await self._state()
        source = (
            "operator"
            if state.enabled_override is not None and state.override_config == configured
            else "config"
        )
        return state.enabled(configured), source

    # -------------------------------------------------------------- reads

    async def _parcel(self, number: int) -> Parcel | None:
        repo_id = self._repo_id
        rows = await self._db(
            lambda store: store.query(
                "SELECT aggregate_json FROM parcels WHERE repo_id = ? AND issue_number = ?",
                (repo_id, number),
            )
        )
        return parcel_from_json(str(rows[0][0])) if rows else None

    async def _target(self, number: int) -> LinkTarget | None:
        """A fresh read of the issue's labels and parent (None: unreadable)."""
        owner, name = self._repo.partition("/")[::2]
        try:
            return await read_target(self.client, owner, name, number, self._repo_id)
        except GitHubAPIError as exc:
            LOG.info("bundle member unreadable issue=#%s reason=%s", number, exc)
            return None

    async def held_numbers(self) -> frozenset[int]:
        """Members held in an active bundle (excluded from ranking and auto-work)."""
        held = await self._db(partial(bs.held_members, repo_id=self._repo_id))
        return frozenset(held)

    async def facts(
        self,
        members: Iterable[int],
        priorities: Mapping[int, str | None] | None = None,
        *,
        ignore_key: str | None = None,
    ) -> dict[int, MemberFacts]:
        numbers = sorted(set(members))
        held = await self._db(partial(bs.held_members, repo_id=self._repo_id))
        admission = await self._db(lambda store: store.load_admission(self._repo_id))
        out: dict[int, MemberFacts] = {}
        for n in numbers:
            p = await self._parcel(n)
            triage = self.directory.latest_triage(p) if p is not None else None
            if triage is not None and triage.get("kind") != "triage":
                triage = None
            target = await self._target(n) if p is not None else None
            parent = p.links.parent if p is not None and p.links is not None else None
            parent_number = parent.number if parent is not None and not parent.repo else None
            if target is not None and target.parent is not None:
                parent_number = target.parent  # the fresh read wins over the stored links
            parent_parcel = await self._parcel(parent_number) if parent_number else None
            bundle = held.get(n)
            bundled = 0
            if bundle is not None and bundle.bundle_key != ignore_key:
                bundled = bundle.issue_number or -1
            deps: list[int] = []
            for item in (triage or {}).get("related") or []:
                if not isinstance(item, Mapping) or item.get("relation") != "depends_on":
                    continue
                dep = item.get("issue")
                if isinstance(dep, int) and dep not in numbers and await self._open(dep):
                    deps.append(dep)
            priority = (priorities or {}).get(n)
            if priority is None and triage is not None:
                priority = str(triage.get("priority") or "") or None
            out[n] = MemberFacts(
                number=n,
                parcel=p,
                labels=target.labels if target is not None else None,
                triage=triage,
                priority=priority,
                parent=parent_number,
                parent_autopilot=parent_parcel is not None and parent_parcel.autopilot is not None,
                queued=p is not None and admission.queue_entry(p.parcel_id) is not None,
                bundled=bundled,
                open_dependencies=tuple(sorted(set(deps))),
            )
        return out

    async def _open(self, number: int) -> bool:
        """An issue counts as open unless the factory saw it finished (fails closed)."""
        p = await self._parcel(number)
        if p is None:
            return True
        return Hold.COMPLETED not in p.holds and p.stage != Stage.DONE

    async def validate(
        self,
        members: Sequence[int],
        priorities: Mapping[int, str | None] | None = None,
        *,
        ignore_key: str | None = None,
        explicit: bool = False,
    ) -> list[str]:
        """Every rule ``members`` break. ``explicit``: an owner's ``/bundle`` (up to
        ``bundle_command_max_members``, no size rules); else a suggestion (a proposal or
        its tick: ``bundle_max_members`` and ``bundle_max_points``)."""
        config = self.service.config
        facts = await self.facts(members, priorities, ignore_key=ignore_key)
        return validate_bundle(
            members,
            facts,
            max_members=(
                config.bundle_command_max_members if explicit else config.bundle_max_members
            ),
            max_points=config.bundle_max_points,
            explicit=explicit,
        )

    # -------------------------------------------------------------- proposals

    async def check_proposals(
        self, raw: object, priorities: Mapping[int, str | None]
    ) -> tuple[list[bs.NewProposal], list[str]]:
        """A ranking's ``bundles``: (the proposals, every problem). Off: none allowed."""
        proposals, errors = parse_proposals(raw)
        if not proposals and not errors:
            return [], []
        if not (await self.enabled())[0]:
            return [], ["bundles: bundling is off; leave bundles out"]
        for position, item in enumerate(proposals, start=1):
            for problem in await self.validate(item.members, priorities):
                errors.append(f"bundles[{position}] ({_refs(sorted(item.members))}): {problem}")
        return proposals, errors

    async def ranking_text(self) -> tuple[str, str]:
        """The ranking start message's bundle paragraph and its submit field ("" while
        off: the ranking then proposes no bundles)."""
        if not (await self.enabled())[0]:
            return "", ""
        config = self.service.config
        members = min(config.bundle_max_members, MAX_MEMBERS)
        return (
            "Bundling is on: you may also propose bundles of 2 to "
            f"{members} related small issues to build as one PR (size S = 1 and M = 3 points, "
            f"at most {config.bundle_max_points} points; no L or unsized, epic, P0, assigned, "
            "skipped, needs-review or blocked issue, and none that needs the owner). Propose one "
            "only when the members change the same area of code; name that area in the reason. "
            "The owner confirms each; members keep their ranks here.\n",
            ", any bundles ({members, theme, reason, shared_area})",
        )

    async def record_proposals(self, run_id: str, proposals: Sequence[bs.NewProposal]) -> None:
        """Record a ranking's accepted proposals (a set already proposed is not again)."""
        if not proposals:
            return
        rows = await self._db(
            partial(bs.record_proposals, repo_id=self._repo_id, run_id=run_id, proposals=proposals)
        )
        for row in rows:
            LOG.info(
                "bundle proposed key=%s members=%s state=%s run=%s",
                row.proposal_key,
                ",".join(f"#{n}" for n in row.members),
                row.state,
                run_id,
            )

    def _proposal_body(self, p: bs.Proposal) -> str:
        lines = [
            f"**Bundle proposal:** build {_refs(p.members)} together as one pull request: "
            f"{self.publish(_one_line(p.theme, THEME_MAX))}",
            "",
            self.publish(_one_line(p.reason, REASON_MAX)),
        ]
        if p.shared_area:
            lines += ["", f"Shared area: {self.publish(_one_line(p.shared_area, AREA_MAX))}"]
        lines.append("")
        if p.open:
            lines.append(rl.line(rl.BUNDLE, p.action_id, CONFIRM_TEXT))
        else:
            lines.append(f"- {CONFIRM_TEXT} → {self._proposal_status(p)}")
        lines += [
            "",
            "Only the owner's tick counts. Until then each issue stays on its own card.",
            "",
            proposal_marker(p.proposal_key),
        ]
        return "\n".join(lines)

    @staticmethod
    def _proposal_status(p: bs.Proposal) -> str:
        if p.state == "stale":
            return f"stale: {p.detail or 'a member is no longer eligible'}"
        if p.state == "refused":
            return f"not started: {p.detail or 'refused'}"
        if p.state == "created":
            return f"bundled{f' into {p.detail}' if p.detail else ''}"
        return "confirmed: creating the bundle issue"

    async def post_pending(self) -> int:
        """Post the comment of every new proposal (or find one an unclear post made);
        a proposal whose members are no longer eligible goes stale instead."""
        if not (await self.enabled())[0]:
            return 0  # pending proposals stay inert while bundling is off
        posted = 0
        for p in await self._db(
            partial(bs.proposals_of, repo_id=self._repo_id, states=("pending", "posting"))
        ):
            problems = await self.validate(p.members)
            if problems:
                await self._stale(p, problems[0])
                continue
            try:
                posted += int(await self._post(p))
            except GitHubAPIError as exc:
                LOG.warning("bundle proposal comment failed key=%s reason=%s", p.proposal_key, exc)
        return posted

    async def _post(self, p: bs.Proposal) -> bool:
        if p.state == "posting":
            found = await self._find_comment(p.anchor_issue, proposal_marker(p.proposal_key))
            if found is not None:
                await self._db(
                    partial(
                        bs.set_proposal,
                        repo_id=self._repo_id,
                        proposal_key=p.proposal_key,
                        state="posted",
                        comment_id=found[0],
                        comment_node_id=found[1],
                    )
                )
                LOG.info("bundle proposal comment found by marker key=%s", p.proposal_key)
                return False
        await self._db(
            partial(
                bs.set_proposal,
                repo_id=self._repo_id,
                proposal_key=p.proposal_key,
                state="posting",
                attempt=True,
            )
        )
        response = await self.client.request(
            "POST",
            f"/repos/{self._repo}/issues/{p.anchor_issue}/comments",
            json_body={"body": self._proposal_body(p)},
            expected=frozenset({201}),
        )
        data: Any = response.json()
        comment_id = data.get("id") if isinstance(data, dict) else None
        if not isinstance(comment_id, int):
            raise GitHubAPIError("proposal comment response omitted its id")
        await self._db(
            partial(
                bs.set_proposal,
                repo_id=self._repo_id,
                proposal_key=p.proposal_key,
                state="posted",
                comment_id=comment_id,
                comment_node_id=str(data.get("node_id") or ""),
            )
        )
        LOG.info(
            "bundle proposal posted key=%s issue=#%s comment=%s",
            p.proposal_key,
            p.anchor_issue,
            comment_id,
        )
        return True

    async def _find_comment(self, issue: int, marker: str) -> tuple[int, str, str] | None:
        """(id, node id, body) of the bot's comment on ``issue`` carrying ``marker``."""
        bot = self.service.config.github_bot_user_id
        comments = await self.client.paginate(
            f"/repos/{self._repo}/issues/{issue}/comments?per_page=100"
        )
        for c in comments:
            if (
                isinstance(c, dict)
                and isinstance(c.get("id"), int)
                and marker in str(c.get("body") or "")
                and isinstance(c.get("user"), dict)
                and c["user"].get("id") == bot
            ):
                return int(c["id"]), str(c.get("node_id") or ""), str(c.get("body") or "")
        return None

    async def _stale(self, p: bs.Proposal, why: str) -> None:
        await self._db(
            partial(
                bs.set_proposal,
                repo_id=self._repo_id,
                proposal_key=p.proposal_key,
                state="stale",
                detail=_one_line(why, 200),
            )
        )
        LOG.info("bundle proposal stale key=%s reason=%s", p.proposal_key, why)
        await self._rewrite_proposal(p.proposal_key)

    async def _rewrite_proposal(self, key: str) -> None:
        """Show the proposal's state in its comment (no checkbox once it is decided)."""
        p = await self._db(partial(bs.proposal, repo_id=self._repo_id, proposal_key=key))
        if p is None or p.comment_id is None:
            return
        try:
            await self.client.request(
                "PATCH",
                f"/repos/{self._repo}/issues/comments/{p.comment_id}",
                json_body={"body": self._proposal_body(p)},
                expected=frozenset({200}),
            )
        except GitHubAPIError as exc:
            LOG.warning("bundle proposal comment update failed key=%s reason=%s", key, exc)

    # -------------------------------------------------------------- owner ticks

    async def on_comment_delivery(self, body: bytes | str) -> int:
        """An authenticated ``issue_comment`` delivery: an owner's ``/bundle`` or
        ``/unbundle`` (created), or a tick of a proposal's box (edited). Returns how many
        confirmations or commands it recorded."""
        try:
            payload = json.loads(body)
        except ValueError:
            return 0
        if not isinstance(payload, dict):
            return 0
        comment = payload.get("comment")
        issue = payload.get("issue")
        if not isinstance(comment, dict) or not isinstance(issue, dict) or "pull_request" in issue:
            return 0
        sender = payload.get("sender")
        actor = sender.get("id") if isinstance(sender, dict) else None
        owner = isinstance(actor, int) and actor in self.service.config.owners
        if payload.get("action") == "created":
            command = _COMMAND.match(str(comment.get("body") or "").strip())
            number = issue.get("number")
            if command is None or not owner or not isinstance(number, int):
                return 0
            assert isinstance(actor, int)  # noqa: S101 - owner checked
            return await self._command(
                command.group("name"), command.group("args") or "", number, comment, actor
            )
        if payload.get("action") != "edited":
            return 0
        author = comment.get("user")
        if (
            not isinstance(author, dict)
            or author.get("id") != self.service.config.github_bot_user_id
        ):
            return 0
        after = str(comment.get("body") or "")
        if "<!-- factory-bundle:" not in after:
            return 0
        changes = payload.get("changes")
        previous = changes.get("body") if isinstance(changes, dict) else None
        before = previous.get("from") if isinstance(previous, dict) else None
        if not isinstance(before, str):
            return 0
        ticked = rl.newly_ticked(before, after, rl.BUNDLE)
        if not ticked or not isinstance(comment.get("id"), int):
            return 0
        if not owner:
            LOG.info(
                "bundle tick ignored: not an owner issue=#%s actor=%s", issue.get("number"), actor
            )
            return 0
        assert isinstance(actor, int)  # noqa: S101 - owner checked
        at_us = _time_us(comment.get("updated_at")) or self.service.clock.now_utc_us()
        return await self._tick(int(comment["id"]), ticked, actor, at_us, "webhook")

    async def _tick(
        self, comment_id: int, ticked: Iterable[str], actor: int, at_us: int, via: str
    ) -> int:
        p = await self._db(
            partial(bs.proposal_by_comment, repo_id=self._repo_id, comment_id=comment_id)
        )
        if p is None or p.action_id not in set(ticked):
            return 0
        if not (await self.enabled())[0]:
            LOG.info("bundle tick ignored: bundling is off key=%s", p.proposal_key)
            return 0
        new = await self._db(
            partial(
                bs.confirm_proposal,
                repo_id=self._repo_id,
                proposal_key=p.proposal_key,
                actor_id=actor,
                via=via,
                at_us=at_us,
            )
        )
        if not new:
            return 0
        LOG.info("bundle confirmed key=%s owner=%s via=%s", p.proposal_key, actor, via)
        await self.sync()
        return 1

    # -------------------------------------------------------------- commands

    async def _react(self, comment: Mapping[str, Any], content: str) -> None:
        comment_id = comment.get("id")
        if not isinstance(comment_id, int):
            return
        try:
            await self.client.request(
                "POST",
                f"/repos/{self._repo}/issues/comments/{comment_id}/reactions",
                json_body={"content": content},
                expected=frozenset({200, 201}),
            )
        except GitHubAPIError as exc:
            LOG.info("bundle command reaction failed comment=%s reason=%s", comment_id, exc)

    async def _reply(self, issue: int, text: str) -> None:
        try:
            await self.client.request(
                "POST",
                f"/repos/{self._repo}/issues/{issue}/comments",
                json_body={"body": text},
                expected=frozenset({201}),
            )
        except GitHubAPIError as exc:
            LOG.warning("bundle command reply failed issue=#%s reason=%s", issue, exc)

    async def _command(
        self, name: str, args: str, issue: int, comment: Mapping[str, Any], actor: int
    ) -> int:
        comment_id = comment.get("id")
        at_us = _time_us(comment.get("created_at")) or self.service.clock.now_utc_us()
        if name == "unbundle":
            done = await self.unbundle(issue, reason="/unbundle")
            await self._react(comment, "+1" if done else "confused")
            return int(done)
        members = sorted({issue, *(int(n) for n in _REF.findall(args))})
        theme = _one_line(_REF.sub("", args), THEME_MAX)
        key = f"c{comment_id}" if isinstance(comment_id, int) else bs.member_key(members)
        if await self._db(partial(bs.bundle, repo_id=self._repo_id, bundle_key=key)) is not None:
            return 0  # the same command again (a redelivery): already acted on
        problems: list[str] = []
        if not (await self.enabled())[0]:
            problems.append("bundling is off (`omnigent-factory bundles on`)")
        else:
            problems = await self.validate(members, explicit=True)
        if problems:
            await self._react(comment, "confused")
            await self._reply(
                issue,
                f"Not bundled ({_refs(members)}):\n"
                + "\n".join(f"- {_one_line(p, 300)}" for p in problems[:10]),
            )
            LOG.info("bundle command refused issue=#%s problems=%s", issue, "; ".join(problems))
            return 0
        if not theme:
            first = await self._parcel(members[0])
            theme = _one_line(first.issue_title if first is not None else "", THEME_MAX)
        created = await self._db(
            partial(
                bs.create_bundle,
                repo_id=self._repo_id,
                bundle_key=key,
                members=members,
                theme=theme or f"issues {_refs(members)}",
                reason="",
                source="command",
                owner_id=actor,
                via="webhook",
                confirmed_at_us=at_us,
            )
        )
        await self._react(comment, "+1")
        if created:
            LOG.info("bundle confirmed by /bundle key=%s members=%s", key, members)
            # An open proposal of the same set is taken by this bundle.
            p = await self._db(
                partial(bs.proposal, repo_id=self._repo_id, proposal_key=bs.member_key(members))
            )
            if p is not None and p.open:
                await self._db(
                    partial(
                        bs.set_proposal,
                        repo_id=self._repo_id,
                        proposal_key=p.proposal_key,
                        state="created",
                        detail="by /bundle",
                    )
                )
                await self._rewrite_proposal(p.proposal_key)
        await self.sync()
        return int(created)

    # -------------------------------------------------------------- creating bundles

    async def sync(self) -> None:
        """Advance every confirmed proposal and active bundle (idempotent; at start, on
        ticks, commands and every reconcile tick)."""
        async with self._lock:
            await self._from_proposals()
            for b in await self._db(
                partial(bs.bundles_of, repo_id=self._repo_id, states=("creating", "open"))
            ):
                try:
                    await self._advance(b)
                except GitHubAPIError as exc:
                    LOG.warning("bundle sync failed key=%s reason=%s", b.bundle_key, exc)

    async def _from_proposals(self) -> None:
        for p in await self._db(
            partial(bs.proposals_of, repo_id=self._repo_id, states=("confirmed",))
        ):
            existing = await self._db(
                partial(bs.bundle, repo_id=self._repo_id, bundle_key=p.proposal_key)
            )
            if existing is None:
                problems = (
                    await self.validate(p.members)
                    if (await self.enabled())[0]
                    else ["bundling is off"]
                )
                if problems:
                    await self._db(
                        partial(
                            bs.set_proposal,
                            repo_id=self._repo_id,
                            proposal_key=p.proposal_key,
                            state="refused",
                            detail=_one_line(problems[0], 200),
                        )
                    )
                    LOG.info(
                        "bundle not created key=%s reason=%s", p.proposal_key, "; ".join(problems)
                    )
                    await self._rewrite_proposal(p.proposal_key)
                    continue
                assert p.confirmed_by is not None and p.confirmed_at_us is not None  # noqa: S101
                await self._db(
                    partial(
                        bs.create_bundle,
                        repo_id=self._repo_id,
                        bundle_key=p.proposal_key,
                        members=p.members,
                        theme=p.theme,
                        reason=p.reason,
                        source="proposal",
                        owner_id=p.confirmed_by,
                        via=p.confirmed_via or "webhook",
                        confirmed_at_us=p.confirmed_at_us,
                    )
                )
            b = await self._db(partial(bs.bundle, repo_id=self._repo_id, bundle_key=p.proposal_key))
            if b is not None and b.issue_number is not None:
                await self._db(
                    partial(
                        bs.set_proposal,
                        repo_id=self._repo_id,
                        proposal_key=p.proposal_key,
                        state="created",
                        detail=f"#{b.issue_number}",
                    )
                )
                await self._rewrite_proposal(p.proposal_key)

    async def _advance(self, b: bs.Bundle) -> None:
        if b.state == "creating":
            await self._ensure_issue(b)
            b = await self._reload(b)
            if b.state != "open":
                return
            # The proposal's comment now names the bundle issue.
            await self._from_proposals()
        assert b.issue_number is not None  # noqa: S101 - open bundles have an issue
        await self._apply_members(b)
        b = await self._reload(b)
        for action in ("label", "parent", "plan"):
            if action in b.done:
                continue
            if await self._do(b, action):
                await self._db(
                    partial(
                        bs.mark_bundle_done,
                        repo_id=self._repo_id,
                        bundle_key=b.bundle_key,
                        action=action,
                    )
                )

    async def _reload(self, b: bs.Bundle) -> bs.Bundle:
        fresh = await self._db(partial(bs.bundle, repo_id=self._repo_id, bundle_key=b.bundle_key))
        assert fresh is not None  # noqa: S101 - never deleted
        return fresh

    async def _member_titles(self, numbers: Iterable[int]) -> dict[int, str]:
        out: dict[int, str] = {}
        for n in numbers:
            p = await self._parcel(n)
            out[n] = _one_line(self.publish(p.issue_title if p is not None else ""), 120)
        return out

    async def _issue_body(self, b: bs.Bundle) -> str:
        titles = await self._member_titles(m.issue_number for m in b.members)
        lines = []
        for m in b.members:
            title = titles.get(m.issue_number, "")
            text = f"#{m.issue_number}" + (f" {title}" if title else "")
            if m.state == "ejected":
                text = f"~~#{m.issue_number}~~ ejected: {self.publish(_one_line(m.reason, 200))}"
            elif m.state == "released":
                text = f"~~#{m.issue_number}~~ released"
            lines.append(f"- {text}")
        held = [m.issue_number for m in b.members if m.state in ("held", "done")]
        closes = ", ".join(f"`Closes #{n}`" for n in held)
        reason = f"\n\n{self.publish(_one_line(b.reason, REASON_MAX))}" if b.reason else ""
        return (
            f"Bundle of {_refs(held) or 'no issues'}, built as one pull request: "
            f"{self.publish(_one_line(b.theme, THEME_MAX))}.{reason}\n\n"
            "**Members**\n\n" + "\n".join(lines) + "\n\n"
            "The pull request must include a `Closes` line for this bundle issue and for "
            f"each member ({closes}). The members are not sub-issues of this issue; each "
            "keeps its own card, held in this bundle.\n\n"
            f"{bundle_marker(b.bundle_key)}"
        )

    def _title(self, b: bs.Bundle) -> str:
        refs = ", ".join(f"#{m.issue_number}" for m in b.members)
        suffix = f" ({refs})"
        theme = _one_line(self.publish(b.theme), max(10, 200 - len("Bundle: ") - len(suffix)))
        return f"Bundle: {theme}{suffix}"

    async def _ensure_issue(self, b: bs.Bundle) -> None:
        """Open the bundle issue once: an unclear create is found by its marker first."""
        if b.attempt_at_us:
            found = await find_marked_issue(
                self.client,
                self._repo,
                bundle_marker(b.bundle_key),
                since_us=b.attempt_at_us,
                bot_id=self.service.config.github_bot_user_id,
            )
            if found is not None:
                await self._opened(b, found[0])
                LOG.info("bundle issue found by marker issue=#%s key=%s", found[0], b.bundle_key)
                return
        await self._db(
            partial(
                bs.set_bundle,
                repo_id=self._repo_id,
                bundle_key=b.bundle_key,
                attempt=True,
            )
        )
        response = await self.client.request(
            "POST",
            f"/repos/{self._repo}/issues",
            json_body={"title": self._title(b), "body": await self._issue_body(b)},
            expected=frozenset({201}),
        )
        data: Any = response.json()
        number = data.get("number") if isinstance(data, dict) else None
        if not isinstance(number, int):
            raise GitHubAPIError("issue create returned no number")
        await self._opened(b, number, str(data.get("node_id") or ""))
        LOG.info(
            "bundle issue created issue=#%s key=%s members=%s",
            number,
            b.bundle_key,
            ",".join(f"#{m.issue_number}" for m in b.members),
        )

    async def _opened(self, b: bs.Bundle, number: int, node_id: str = "") -> None:
        await self._db(
            partial(
                bs.set_bundle,
                repo_id=self._repo_id,
                bundle_key=b.bundle_key,
                state="open",
                issue_number=number,
                issue_node_id=node_id or None,
            )
        )

    async def _do(self, b: bs.Bundle, action: str) -> bool:
        assert b.issue_number is not None  # noqa: S101 - open bundles have an issue
        if action == "label":
            if await ensure_label(self.client, self._repo, BUNDLE_LABEL, _LABEL_DESCRIPTION):
                LOG.info("repository label created label=%s", BUNDLE_LABEL)
            await add_label(self.client, self._repo, b.issue_number, BUNDLE_LABEL)
            return True
        if action == "parent":
            return await self._parent(b)
        return await self._plan(b)

    async def _parent(self, b: bs.Bundle) -> bool:
        """Join the members' epic when they all share one (never the members as its
        sub-issues: that would make the bundle issue an epic)."""
        assert b.issue_number is not None  # noqa: S101
        owner, name = self._repo.partition("/")[::2]
        parents: set[int | None] = set()
        for m in b.members:
            member = await read_target(self.client, owner, name, m.issue_number, self._repo_id)
            parents.add(member.parent if member is not None else None)
        if len(parents) != 1 or None in parents:
            return True
        epic_number = next(iter(parents))
        assert epic_number is not None  # noqa: S101
        target = await read_target(self.client, owner, name, b.issue_number, self._repo_id)
        epic = await read_target(self.client, owner, name, epic_number, self._repo_id)
        if target is None or epic is None or target.has_parent:
            return True
        await add_sub_issue(self.client, epic.node_id, target.node_id)
        LOG.info("bundle issue added to epic issue=#%s epic=#%s", b.issue_number, epic_number)
        return True

    async def _plan(self, b: bs.Bundle) -> bool:
        """The owner's confirmation starts the bundle issue's plan (their control)."""
        assert b.issue_number is not None  # noqa: S101
        if self.snapshot is None:
            return True
        node_id = b.issue_node_id
        if not node_id:
            issue: Any = await self.client.get_json(f"/repos/{self._repo}/issues/{b.issue_number}")
            node_id = str(issue.get("node_id") or "") if isinstance(issue, dict) else ""
            if not node_id:
                return False
            await self._db(
                partial(
                    bs.set_bundle,
                    repo_id=self._repo_id,
                    bundle_key=b.bundle_key,
                    issue_node_id=node_id,
                )
            )
        snapshot = await self.snapshot(IssueRef(self._repo_id, b.issue_number, node_id))
        if isinstance(snapshot, RetryableReadFailure):
            LOG.info(
                "bundle issue read failed issue=#%s reason=%s", b.issue_number, snapshot.reason
            )
            return False
        event_id = f"bundle-plan:{b.bundle_key}"
        result = await self.service.apply_event(
            Event(
                event_id=event_id,
                repo_id=self._repo_id,
                parcel_id=node_id,
                source_time_us=max(self.service.clock.now_utc_us(), snapshot.read_at_us + 1),
                # The owner's own authenticated tick or command (or the recovered edit
                # its history shows them making): the same authority as a drag.
                provenance=Provenance.WEBHOOK if b.via == "webhook" else Provenance.RECOVERY,
                body=ev.RequestPlan(via=Via.COMMAND),
                actor_id=b.owner_id,
                issue_number=b.issue_number,
                entropy=hashlib.sha256(event_id.encode()).hexdigest(),
                evidence=snapshot,
            )
        )
        if result.accepted or result.duplicate:
            LOG.info("bundle plan started issue=#%s key=%s", b.issue_number, b.bundle_key)
        else:
            LOG.warning(
                "bundle plan not started issue=#%s reason=%s", b.issue_number, result.reason
            )
            await self._db(
                partial(
                    bs.set_bundle,
                    repo_id=self._repo_id,
                    bundle_key=b.bundle_key,
                    detail=f"plan not started: {result.reason}",
                )
            )
        return True

    # -------------------------------------------------------------- member holds

    async def _apply_members(self, b: bs.Bundle) -> None:
        """Hold every held member, release every member that left (each applied once)."""
        assert b.issue_number is not None  # noqa: S101
        bot = await self._bundle_bot(b)
        for m in b.members:
            if m.applied:
                continue
            p = await self._parcel(m.issue_number)
            if p is None:
                await self._mark_applied(b, m.issue_number)
                continue
            if m.state == "held":
                result = await self._hold_event(p, b.issue_number, bot, f"{b.bundle_key}:held")
                if not (result.accepted or result.duplicate):
                    # Busy since the confirmation (e.g. the owner started it): it is not
                    # in the bundle after all.
                    await self._eject(b, m.issue_number, f"not held: {result.reason}")
                    continue
                LOG.info("bundle member held issue=#%s bundle=#%s", m.issue_number, b.issue_number)
            elif m.state in ("ejected", "released"):
                note = (
                    f"Ejected from bundle #{b.issue_number}: {_one_line(m.reason, 80)}"
                    if m.state == "ejected"
                    else f"Released from bundle #{b.issue_number}"
                )
                await self.service.apply_event(
                    self._event(
                        p,
                        f"bundle-hold:{b.bundle_key}:{m.issue_number}:{m.state}",
                        ev.BundleHold(bundle=0, note=note, to_triage=m.state == "ejected"),
                    )
                )
                LOG.info(
                    "bundle member %s issue=#%s bundle=#%s", m.state, m.issue_number, b.issue_number
                )
            await self._mark_applied(b, m.issue_number)

    async def _mark_applied(self, b: bs.Bundle, number: int) -> None:
        await self._db(
            partial(
                bs.set_member,
                repo_id=self._repo_id,
                bundle_key=b.bundle_key,
                issue_number=number,
                applied=True,
            )
        )

    def _event(self, p: Parcel, event_id: str, body: ev.EventBody) -> Event:
        return Event(
            event_id=event_id,
            repo_id=self._repo_id,
            parcel_id=p.parcel_id,
            source_time_us=self.service.clock.now_utc_us(),
            provenance=Provenance.ADAPTER,
            body=body,
            issue_number=p.issue_number,
        )

    async def _hold_event(self, p: Parcel, bundle: int, bot: str, key: str) -> ApplyResult:
        return await self.service.apply_event(
            self._event(
                p, f"bundle-hold:{key}:{p.issue_number}", ev.BundleHold(bundle=bundle, bot=bot)
            )
        )

    async def _bundle_bot(self, b: bs.Bundle) -> str:
        if b.issue_number is None:
            return BotState.IDLE.value
        p = await self._parcel(b.issue_number)
        return p.bot.value if p is not None else BotState.IDLE.value

    async def mirror_bots(self) -> int:
        """Each held member's card shows its bundle's Bot (changes only)."""
        changed = 0
        for b in await self._db(partial(bs.bundles_of, repo_id=self._repo_id, states=("open",))):
            bot = await self._bundle_bot(b)
            for m in b.members:
                if m.state != "held" or not m.applied:
                    continue
                p = await self._parcel(m.issue_number)
                if p is None or Hold.BUNDLED not in p.holds or p.bundle_bot == bot:
                    continue
                assert b.issue_number is not None  # noqa: S101
                stamp = self.service.clock.now_utc_us()
                result = await self._hold_event(
                    p, b.issue_number, bot, f"{b.bundle_key}:bot:{bot}:{stamp}"
                )
                changed += int(result.accepted)
        return changed

    async def _eject(self, b: bs.Bundle, number: int, reason: str) -> bool:
        changed = await self._db(
            partial(
                bs.set_member,
                repo_id=self._repo_id,
                bundle_key=b.bundle_key,
                issue_number=number,
                state="ejected",
                reason=_one_line(reason, 300),
            )
        )
        if changed:
            LOG.info(
                "bundle member ejected issue=#%s bundle=#%s reason=%s",
                number,
                b.issue_number,
                reason,
            )
        return changed

    async def _after_members_change(self, b: bs.Bundle) -> None:
        b = await self._reload(b)
        await self._apply_members(b)
        b = await self._reload(b)
        if b.issue_number is not None:
            try:
                await self.client.request(
                    "PATCH",
                    f"/repos/{self._repo}/issues/{b.issue_number}",
                    json_body={"body": await self._issue_body(b)},
                    expected=frozenset({200}),
                )
            except GitHubAPIError as exc:
                LOG.warning("bundle issue update failed issue=#%s reason=%s", b.issue_number, exc)
        await self._refresh_status(b)

    async def _refresh_status(self, b: bs.Bundle) -> None:
        status = self.service.status_comments
        if status is None:
            return
        numbers = [m.issue_number for m in b.members]
        if b.issue_number is not None:
            numbers.append(b.issue_number)
        for n in numbers:
            p = await self._parcel(n)
            if p is not None:
                try:
                    await status.refresh(p.parcel_id)
                except Exception:
                    LOG.warning("status comment refresh failed issue=#%s", n, exc_info=True)

    # -------------------------------------------------------------- ejection

    def schedule_result(self, parcel: Parcel, session_id: str, result: Mapping[str, Any]) -> None:
        """Called once a stage result is accepted: eject the members it lists."""
        del session_id
        if result.get("ejected"):
            self._spawn(self.record_ejections(parcel, result))

    async def record_ejections(self, parcel: Parcel, result: Mapping[str, Any]) -> list[int]:
        if parcel.issue_number is None:
            return []
        b = await self._db(
            partial(bs.bundle_by_issue, repo_id=self._repo_id, issue_number=parcel.issue_number)
        )
        if b is None or not b.active:
            return []
        ejected: list[int] = []
        for item in result.get("ejected") or []:
            number = item.get("issue") if isinstance(item, Mapping) else None
            if not isinstance(number, int):
                continue
            reason = str(item.get("reason") or "")
            if await self._eject(b, number, reason):
                ejected.append(number)
        if ejected:
            await self._after_members_change(b)
        return ejected

    # -------------------------------------------------------------- cancel and finish

    async def unbundle(self, issue: int, *, reason: str) -> bool:
        """``/unbundle`` on the bundle issue releases every member; on a member, only that
        member. False: the issue is in no active bundle."""
        b = await self._db(partial(bs.bundle_by_issue, repo_id=self._repo_id, issue_number=issue))
        if b is not None and b.active:
            await self._cancel(b, reason)
            return True
        b = await self._db(partial(bs.bundle_of_member, repo_id=self._repo_id, issue_number=issue))
        if b is None:
            return False
        if await self._db(
            partial(
                bs.set_member,
                repo_id=self._repo_id,
                bundle_key=b.bundle_key,
                issue_number=issue,
                state="released",
                reason=reason,
            )
        ):
            await self._after_members_change(b)
        return True

    async def _cancel(self, b: bs.Bundle, reason: str) -> None:
        for m in b.members:
            await self._db(
                partial(
                    bs.set_member,
                    repo_id=self._repo_id,
                    bundle_key=b.bundle_key,
                    issue_number=m.issue_number,
                    state="released",
                    reason=reason,
                )
            )
        await self._db(
            partial(
                bs.set_bundle,
                repo_id=self._repo_id,
                bundle_key=b.bundle_key,
                state="cancelled",
                detail=_one_line(reason, 200),
            )
        )
        LOG.info("bundle cancelled issue=#%s reason=%s", b.issue_number, reason)
        await self._after_members_change(b)

    async def _merged(self, b: bs.Bundle) -> None:
        """The bundle PR merged: members GitHub closed are done; their close is read again
        so a lost webhook does not leave them open in the factory."""
        await self._db(
            partial(bs.set_bundle, repo_id=self._repo_id, bundle_key=b.bundle_key, state="merged")
        )
        LOG.info("bundle merged issue=#%s", b.issue_number)
        await self._finish_members(b)

    async def _finish_members(self, b: bs.Bundle) -> None:
        """Held members GitHub closed are done; one the factory has not seen closed is
        read now (its close webhook may have been lost)."""
        b = await self._reload(b)
        for m in b.members:
            if m.state != "held":
                continue
            p = await self._parcel(m.issue_number)
            if p is None:
                continue
            if Hold.COMPLETED not in p.holds:
                p = await self._read(p)
            if Hold.COMPLETED in p.holds:
                await self._db(
                    partial(
                        bs.set_member,
                        repo_id=self._repo_id,
                        bundle_key=b.bundle_key,
                        issue_number=m.issue_number,
                        state="done",
                    )
                )
                LOG.info("bundle member done issue=#%s bundle=#%s", m.issue_number, b.issue_number)
        await self._refresh_status(b)

    async def _read(self, p: Parcel) -> Parcel:
        """Apply a fresh read of ``p`` (a closed issue is terminal: completed)."""
        if self.snapshot is None or p.issue_number is None:
            return p
        snapshot = await self.snapshot(IssueRef(self._repo_id, p.issue_number, p.parcel_id))
        if isinstance(snapshot, RetryableReadFailure):
            return p
        result = await self.service.apply_event(
            Event(
                event_id=f"bundle-read:{p.parcel_id}:{snapshot.read_at_us}",
                repo_id=self._repo_id,
                parcel_id=p.parcel_id,
                source_time_us=snapshot.read_at_us,
                provenance=Provenance.ADAPTER,
                body=ev.GitHubSnapshot(),
                issue_number=p.issue_number,
                evidence=snapshot,
            )
        )
        return result.parcel or p

    async def _pr_merged(self, pr_number: int | None) -> bool | None:
        if not pr_number:
            return None
        try:
            pr: Any = await self.client.get_json(f"/repos/{self._repo}/pulls/{pr_number}")
        except GitHubAPIError as exc:
            LOG.info("bundle PR read failed pr=#%s reason=%s", pr_number, exc)
            return None
        return bool(pr.get("merged")) if isinstance(pr, dict) else None

    async def catch_up(self) -> None:
        """The board-diff pass: lost ticks recovered from the comment's edit history,
        proposals gone stale, bundle issues merged, closed or assigned, members stopped
        on their own, and the bundle card's Priority."""
        await self._recover_ticks()
        if (await self.enabled())[0]:
            for p in await self._db(
                partial(bs.proposals_of, repo_id=self._repo_id, states=("posted",))
            ):
                problems = await self.validate(p.members)
                if problems:
                    await self._stale(p, problems[0])
        for b in await self._db(
            partial(bs.bundles_of, repo_id=self._repo_id, states=("open", "merged"))
        ):
            try:
                if b.state == "merged":
                    await self._finish_members(b)
                else:
                    await self._check_bundle(b)
            except GitHubAPIError as exc:
                LOG.warning("bundle check failed issue=#%s reason=%s", b.issue_number, exc)
        await self._priorities()

    async def _check_bundle(self, b: bs.Bundle) -> None:
        assert b.issue_number is not None  # noqa: S101
        parcel = await self._parcel(b.issue_number)
        if parcel is None:
            return
        if Hold.COMPLETED in parcel.holds:
            done = await self._pr_merged(parcel.pr_number)
            if done:
                await self._merged(b)
            elif done is False or not parcel.pr_number:
                await self._cancel(b, f"bundle issue #{b.issue_number} closed unmerged")
            return
        evidence = await self.directory.issue_evidence(parcel)
        if not parcel.eligible and evidence is not None and evidence.human_assigned:
            await self._cancel(b, f"bundle issue #{b.issue_number} assigned to a person")
            return
        changed = False
        merged: bool | None = None
        for m in b.members:
            if m.state != "held":
                continue
            member = await self._parcel(m.issue_number)
            if member is None:
                continue
            why = await self._stopped(member)
            if why == "closed":
                if merged is None:
                    merged = bool(await self._pr_merged(parcel.pr_number))
                if merged:
                    await self._merged(b)  # closed by the bundle PR: done, not ejected
                    return
            if why:
                changed |= await self._eject(b, m.issue_number, why)
        if changed:
            await self._after_members_change(b)

    async def _stopped(self, member: Parcel) -> str:
        """Why a held member left the bundle on its own ("" = it did not)."""
        if Hold.COMPLETED in member.holds:
            return "closed"
        if not member.eligible:
            evidence = await self.directory.issue_evidence(member)
            if evidence is not None and evidence.human_assigned:
                return "assigned to a person"
        return ""

    async def _recover_ticks(self) -> int:
        """A proposal box shown ticked with no recorded confirmation counts only when the
        comment's edit history shows an owner made that edit."""
        if not (await self.enabled())[0]:
            return 0
        owners = self.service.config.owners
        recovered = 0
        for p in await self._db(
            partial(bs.proposals_of, repo_id=self._repo_id, states=("posted",))
        ):
            if p.comment_id is None or not p.comment_node_id:
                continue
            try:
                comment: Any = await self.client.get_json(
                    f"/repos/{self._repo}/issues/comments/{p.comment_id}"
                )
                body = str(comment.get("body") or "") if isinstance(comment, dict) else ""
                if not rl.checked(body, rl.BUNDLE).get(p.action_id):
                    continue
                edits = await comment_edits(self.client, p.comment_node_id)
            except GitHubAPIError as exc:
                LOG.warning("bundle tick catch-up failed key=%s reason=%s", p.proposal_key, exc)
                continue
            editor_found: int | None = None
            previous = False
            for at_us, editor, text in edits or []:
                if text is None:
                    break  # a version without its text: later ticks cannot be attributed
                now = bool(rl.checked(text, rl.BUNDLE).get(p.action_id))
                if now and not previous:
                    if editor in owners:
                        assert editor is not None  # noqa: S101
                        editor_found = editor
                        recovered += await self._tick(
                            p.comment_id, [p.action_id], editor, at_us, "recovered"
                        )
                    else:
                        LOG.info(
                            "bundle tick not recovered: edited by a non-owner key=%s editor=%s",
                            p.proposal_key,
                            editor,
                        )
                    break
                previous = now
            if editor_found is None:
                LOG.info("bundle tick ignored: no owner edit shows it key=%s", p.proposal_key)
        return recovered

    async def _priorities(self) -> None:
        """The bundle card takes its members' highest priority (once, when on the board)."""
        if self.cards is None or self.set_priority is None:
            return
        pending = [
            b
            for b in await self._db(partial(bs.bundles_of, repo_id=self._repo_id, states=("open",)))
            if "priority" not in b.done and b.issue_number is not None
        ]
        if not pending:
            return
        cards = await self.cards()
        if isinstance(cards, RetryableReadFailure):
            return
        by_number = {c.number: c for c in cards}
        for b in pending:
            card = by_number.get(b.issue_number or 0)
            if card is None:
                continue
            priorities = sorted(
                c.priority
                for m in b.members
                if (c := by_number.get(m.issue_number)) is not None and c.priority
            )
            if priorities and card.priority != priorities[0]:
                try:
                    await self.set_priority(card.item_id, priorities[0])
                except Exception as exc:  # RankingWriteError or a GitHub failure
                    LOG.warning("bundle priority failed issue=#%s reason=%s", b.issue_number, exc)
                    continue
                LOG.info("bundle priority set issue=#%s priority=%s", b.issue_number, priorities[0])
            await self._db(
                partial(
                    bs.mark_bundle_done,
                    repo_id=self._repo_id,
                    bundle_key=b.bundle_key,
                    action="priority",
                )
            )

    # -------------------------------------------------------------- issue webhooks

    async def on_issues_delivery(self, body: bytes | str) -> None:
        """An authenticated ``issues`` delivery: the bundle issue closed or assigned (the
        bundle ends), or a member closed, assigned or labelled ``factory:skip`` (it is
        ejected)."""
        try:
            payload = json.loads(body)
        except ValueError:
            return
        if not isinstance(payload, dict):
            return
        action = payload.get("action")
        issue = payload.get("issue")
        number = issue.get("number") if isinstance(issue, dict) else None
        if not isinstance(number, int) or action not in ("closed", "assigned", "labeled"):
            return
        if action == "assigned":
            assignee = payload.get("assignee")
            if isinstance(assignee, dict) and assignee.get("type") == "Bot":
                return
        if action == "labeled" and label_name(payload) != SKIP_LABEL:
            return
        b = await self._db(partial(bs.bundle_by_issue, repo_id=self._repo_id, issue_number=number))
        if b is not None and b.state == "open":
            if action == "assigned":
                await self._cancel(b, f"bundle issue #{number} assigned to a person")
            elif action == "closed":
                parcel = await self._parcel(number)
                merged = await self._pr_merged(parcel.pr_number if parcel is not None else None)
                if merged:
                    await self._merged(b)
                else:
                    await self._cancel(b, f"bundle issue #{number} closed unmerged")
            return
        b = await self._db(partial(bs.bundle_of_member, repo_id=self._repo_id, issue_number=number))
        if b is None or b.state != "open" or b.issue_number is None:
            return
        if action == "closed":
            bundle_parcel = await self._parcel(b.issue_number)
            if await self._pr_merged(bundle_parcel.pr_number if bundle_parcel else None):
                return  # closed by the bundle PR's merge: done, not ejected
        why = {"closed": "closed", "assigned": "assigned to a person"}.get(
            str(action), f"labelled {SKIP_LABEL}"
        )
        if await self._eject(b, number, why):
            await self._after_members_change(b)

    # -------------------------------------------------------------- views

    async def bundle_for(self, parcel: Parcel) -> bs.Bundle | None:
        """The active (or merged) bundle whose bundle issue ``parcel`` is."""
        if parcel.issue_number is None:
            return None
        b = await self._db(
            partial(bs.bundle_by_issue, repo_id=self._repo_id, issue_number=parcel.issue_number)
        )
        return b if b is not None and b.state in ("open", "merged") else None

    async def members_for(self, parcel: Parcel) -> frozenset[int] | None:
        """A bundle issue's members still in it (None: not a bundle issue)."""
        b = await self.bundle_for(parcel)
        return None if b is None else frozenset(b.held)

    async def source_for(self, parcel: Parcel, member: object) -> int | None:
        """The source issue of a follow-up or split a bundle run attributes to ``member``
        (the bundle issue itself unless it names one of its members)."""
        b = await self.bundle_for(parcel)
        if b is not None and isinstance(member, int) and member in b.held:
            return member
        return parcel.issue_number

    async def member_sources(self, parcel: Parcel) -> list[int]:
        """A bundle issue's members (its findings and splits may be attributed to them)."""
        b = await self.bundle_for(parcel)
        return [] if b is None else [m.issue_number for m in b.members]

    async def required_closes(self, parcel_id: str) -> tuple[tuple[int, str], ...]:
        """(number, node id) of the members a bundle PR must close (empty: not a bundle)."""
        rows = await self._db(lambda store: store.load_parcel(parcel_id))
        if rows is None:
            return ()
        b = await self.bundle_for(rows)
        if b is None:
            return ()
        out: list[tuple[int, str]] = []
        for n in b.held:
            p = await self._parcel(n)
            if p is not None:
                out.append((n, p.parcel_id))
        return tuple(out)

    async def members_view(self, parcel: Parcel, boundary: str) -> list[dict[str, Any]] | None:
        """``bundle_members`` of ``factory_get_issue``/``factory_get_plan``: each member's
        title, text (untrusted) and triage. None: not a bundle issue."""
        b = await self.bundle_for(parcel)
        if b is None:
            return None
        out: list[dict[str, Any]] = []
        for m in b.members:
            p = await self._parcel(m.issue_number)
            if m.state not in ("held", "done") or p is None:
                continue
            evidence = await self.directory.issue_evidence(p)
            text = (
                f"Title: {evidence.title}\n\n{evidence.body or ''}"
                if evidence is not None
                else "(the issue content has not been read yet)"
            )
            out.append(
                {
                    "number": m.issue_number,
                    "title": evidence.title if evidence is not None else p.issue_title,
                    "text": untrusted_block(text, boundary, "UNTRUSTED ISSUE"),
                    "triage": self.directory.latest_triage(p),
                }
            )
        return out

    async def section(self, parcel: Parcel) -> str:
        """The status comment's Bundle section ("" when the issue is in no bundle)."""
        if parcel.issue_number is None:
            return ""
        b = await self.bundle_for(parcel)
        if b is not None:
            titles = await self._member_titles(m.issue_number for m in b.members)
            lines = []
            for m in b.members:
                p = await self._parcel(m.issue_number)
                state = {
                    "held": "in the bundle",
                    "done": "done",
                    "released": "released",
                    "ejected": f"ejected: {_one_line(self.publish(m.reason), 160)}",
                }.get(m.state, m.state)
                if m.state == "held" and p is not None and Hold.COMPLETED in p.holds:
                    state = "done"
                title = titles.get(m.issue_number, "")
                lines.append(f"- #{m.issue_number} {title} — {state}".replace("  ", " "))
            state = "merged" if b.state == "merged" else "open"
            return (
                f"Bundle ({state}) of {len(b.held)} issue(s): "
                f"{_one_line(self.publish(b.theme), THEME_MAX)}\n\n" + "\n".join(lines)
            )
        if parcel.bundled_into:
            return f"Bundled into #{parcel.bundled_into}: built as part of its pull request."
        return ""

    # -------------------------------------------------------------- operator

    async def tick(self) -> None:
        """Every reconcile tick: post new proposals, advance bundles, mirror Bot."""
        await self.post_pending()
        await self.sync()
        await self.mirror_bots()

    async def command(self, args: Mapping[str, object]) -> dict[str, object]:
        """``bundles status|on|off|list|propose-now`` over the operator socket."""
        action = str(args.get("action") or "status")
        config = self.service.config
        repo_id = self._repo_id
        if action in ("on", "off"):
            enabled, configured = action == "on", config.bundling
            await self._db(lambda store: store.set_bundling_override(repo_id, enabled, configured))
            LOG.info("operator bundles %s (config bundling=%s)", action, configured)
        elif action == "propose-now":
            if not (await self.enabled())[0]:
                raise ValueError("bundling is off: `bundles on` first")
            if self.service.ranker is None:
                raise ValueError("ranking is not wired")
            # Bundles are proposed by a ranking run: ask for one now.
            await self.service.ranker.command({"action": "now"})
            LOG.info("operator bundles propose-now: ranking requested")
        elif action == "list":
            return {**await self.status(), **await self.listing()}
        elif action != "status":
            raise ValueError("bundles action must be status, on, off, list or propose-now")
        return await self.status()

    async def status(self) -> dict[str, object]:
        enabled, source = await self.enabled()
        config = self.service.config
        proposals = await self._db(
            partial(bs.proposals_of, repo_id=self._repo_id, states=sorted(bs.OPEN_PROPOSAL_STATES))
        )
        active = await self._db(
            partial(bs.bundles_of, repo_id=self._repo_id, states=sorted(bs.ACTIVE_BUNDLE_STATES))
        )
        return {
            "enabled": enabled,
            "enabled_source": source,
            "max_members": min(config.bundle_max_members, MAX_MEMBERS),
            "command_max_members": min(config.bundle_command_max_members, MAX_MEMBERS),
            "max_points": config.bundle_max_points,
            "proposals_pending": len(proposals),
            "active_bundles": len(active),
            "members_held": sum(1 for b in active for m in b.members if m.state == "held"),
        }

    async def listing(self) -> dict[str, object]:
        proposals = await self._db(partial(bs.proposals_of, repo_id=self._repo_id))
        bundles = await self._db(partial(bs.bundles_of, repo_id=self._repo_id))
        return {
            "proposals": [
                {
                    "members": list(p.members),
                    "theme": p.theme,
                    "state": p.state,
                    "detail": p.detail or None,
                    "comment_on": p.anchor_issue,
                }
                for p in proposals
            ],
            "bundles": [
                {
                    "issue": b.issue_number,
                    "theme": b.theme,
                    "state": b.state,
                    "source": b.source,
                    "detail": b.detail or None,
                    "members": {
                        f"#{m.issue_number}": m.state + (f": {m.reason}" if m.reason else "")
                        for m in b.members
                    },
                }
                for b in bundles
            ],
        }


__all__ = [
    "BUNDLE_LABEL",
    "MAX_MEMBERS",
    "POINTS",
    "Bundles",
    "MemberFacts",
    "bundle_marker",
    "member_points",
    "parse_proposals",
    "proposal_marker",
    "validate_bundle",
]
