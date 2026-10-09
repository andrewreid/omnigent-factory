"""Concrete GitHub reader/effect adapter using REST and GraphQL."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from omnigent_factory.core.contract_view import extract_contract_section, marker_hash
from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    JsonValue,
    RetryableReadFailure,
)
from omnigent_factory.core.events import (
    MERGE_CLEAN,
    MERGE_CONFLICT,
    MERGE_UNKNOWN,
    BotEyes,
    ChecksState,
    FindingRef,
)
from omnigent_factory.core.types import IssueSnapshot, Stage
from omnigent_factory.github.client import (
    AmbiguousRequest,
    GitHubAPIError,
    GitHubClient,
    GitHubRejected,
    RateLimited,
)
from omnigent_factory.ports.github import (
    GITHUB_EFFECT_KINDS,
    STATUS_OPTION_IDS,
    BoardCard,
    BoardIssue,
    ContractPublication,
    IssueRef,
    LabelEvent,
    PullRequestEvidence,
    stage_for_option,
)

LOG = logging.getLogger(__name__)

#: Project fields a triage result may fill (design §B: only while the owner left them unset).
TRIAGE_FIELDS = ("Priority", "Size")
#: Label events (newest) one ``label_events`` read returns.
_LABEL_EVENTS_READ = 50
#: Board pages (100 items each) one ``board_issues`` read takes at most.
_BOARD_MAX_PAGES = 20
#: Seconds between re-reads of a PR whose ``mergeable`` GitHub has not computed yet
#: (about 7 s in all); still null after them is "unknown", asked again at the next read.
MERGEABLE_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0)


def _mergeable(pr: Mapping[str, Any]) -> str:
    """Whether the PR merges into its base, from one PR read ("" = not reported).

    ``mergeable`` false or ``mergeable_state`` "dirty" is a conflict; ``mergeable`` true is
    clean (a "behind" PR is clean: it is never updated here); null is unknown.
    """
    if "mergeable" not in pr:
        return ""
    value = pr.get("mergeable")
    if value is False or pr.get("mergeable_state") == "dirty":
        return MERGE_CONFLICT
    if value is True:
        return MERGE_CLEAN
    return MERGE_UNKNOWN


@dataclass(frozen=True, slots=True)
class ParcelBinding:
    """GitHub resource IDs resolved by ingest/reconciliation for one parcel."""

    issue_number: int
    project_item_id: str | None = None


@dataclass(frozen=True, slots=True)
class TriageFields:
    """Board values and informational labels from an accepted triage result."""

    priority: str
    size: str
    labels: tuple[str, ...] = ()


ParcelResolver = Callable[[str], Awaitable[ParcelBinding | None]]
TriageFieldSource = Callable[[EffectIntent], Awaitable[TriageFields | None]]


@dataclass(frozen=True, slots=True)
class BoardSchema:
    """Persisted GitHub field and option IDs; names are display/semantic values only."""

    status_field_id: str
    status_options: Mapping[str, str]
    bot_field_id: str
    bot_options: Mapping[str, str]
    #: The "Factory note" text field ("" when not configured: notes are not written).
    note_field_id: str = ""
    #: The "Auto-build" single-select field and its option IDs by name (Queued, Started);
    #: "" when not configured: the field is neither read nor written.
    auto_build_field_id: str = ""
    auto_build_options: Mapping[str, str] = field(default_factory=dict)


class GitHubAPIAdapter:
    def __init__(
        self,
        client: GitHubClient,
        *,
        repository: str,
        repository_node_id: str,
        project_node_id: str,
        status_field_node_id: str,
        bot_user_id: int,
        required_checks: frozenset[tuple[str, int]],
        parcel_branch_prefix: str = "factory/",
        now_us: Callable[[], int] | None = None,
        parcel_bindings: Mapping[str, ParcelBinding] | None = None,
        board_schema: BoardSchema | None = None,
        publication_renderer: Callable[[EffectIntent], str | Awaitable[str | None] | None]
        | None = None,
        independent_reviewer_ids: frozenset[int] = frozenset(),
        owner_ids: frozenset[int] = frozenset(),
        parcel_resolver: ParcelResolver | None = None,
        triage_fields: TriageFieldSource | None = None,
        cross_vendor_review: Callable[[EffectIntent], Awaitable[bool]] | None = None,
        review_bot_login: str = "",
        review_bot_mention: str = "",
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        mergeable_retry_delays: tuple[float, ...] = MERGEABLE_RETRY_DELAYS,
    ) -> None:
        self.client = client
        #: Waits between re-reads of a PR whose mergeability GitHub is still computing.
        self._sleep = sleep
        self.mergeable_retry_delays = mergeable_retry_delays
        self.repository = repository
        self.repository_node_id = repository_node_id
        self.project_node_id = project_node_id
        self.status_field_node_id = status_field_node_id
        self.bot_user_id = bot_user_id
        self.required_checks = required_checks
        self.parcel_branch_prefix = parcel_branch_prefix
        self._now_us = now_us or (lambda: time.time_ns() // 1000)
        self.parcel_bindings = parcel_bindings or {}
        self.board_schema = board_schema
        self.status_options = _status_options(board_schema)
        self.publication_renderer = publication_renderer
        # Core effect args carry no GitHub resource IDs: the service resolves the
        # parcel's issue from persisted state (static bindings are a test seam).
        self.parcel_resolver = parcel_resolver
        self.triage_fields = triage_fields
        #: Molly's reported opposite-vendor review of the effect's PR head (service-side).
        self.cross_vendor_review = cross_vendor_review
        self._last_checks_summary = ""
        self._last_failing_checks = ""
        #: Kind of the review bot's latest answer to the head last read ("" = none).
        self._last_bot_answer = ""
        #: Commit of each review thread the review bot opened, from the last thread read.
        self._last_bot_thread_commits: tuple[str, ...] = ()
        #: The bot threads without an outcome, from the last thread read.
        self._last_open_findings: tuple[FindingRef, ...] = ()
        #: The review bot's 👀 for its latest unanswered trigger, from the last read.
        self._last_bot_eyes = ""
        if independent_reviewer_ids & (owner_ids | {bot_user_id}):
            raise ValueError("independent reviewers cannot include owners or the factory bot")
        self.independent_reviewer_ids = independent_reviewer_ids
        self.owner_ids = owner_ids
        #: The review bot whose late comments the readiness grace waits for ("" = none
        #: configured: its state is unknown, so the grace always applies).
        self.review_bot_login = review_bot_login
        #: The mention that asks it for a review (e.g. "@codex"); "" = none recognised.
        self.review_bot_mention = review_bot_mention
        #: Told (without waiting) once a triage comment exists, to mark the issues the
        #: triage names as related (``service.related.RelatedMarker.schedule``).
        self.related_marker: Callable[[EffectIntent], None] | None = None

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return GITHUB_EFFECT_KINDS

    async def issue_snapshot(self, ref: IssueRef) -> IssueSnapshot | RetryableReadFailure:
        try:
            issue: Any = await self.client.get_json(
                f"/repos/{self.repository}/issues/{ref.issue_number}"
            )
            if not isinstance(issue, dict) or issue.get("node_id") != ref.parcel_id:
                return RetryableReadFailure("issue identity did not match requested parcel")
            stage, in_project, bot, note, auto_build = await self._project_stage(ref.parcel_id)
            assignees = issue.get("assignees")
            human_assigned = not isinstance(assignees, list) or any(
                not isinstance(user, dict) or user.get("type") != "Bot" for user in assignees
            )
            return IssueSnapshot(
                open=issue.get("state") == "open",
                human_assigned=human_assigned,
                repo_matches=True,
                identity_resolved=True,
                in_project=in_project,
                stage=stage,
                title=str(issue.get("title", "")),
                body=issue.get("body") if isinstance(issue.get("body"), str) else None,
                read_at_us=self._now_us(),
                bot=bot,
                note=note,
                auto_build=auto_build,
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

    async def label_events(self, ref: IssueRef) -> tuple[LabelEvent, ...] | RetryableReadFailure:
        """The issue's latest ``labeled``/``unlabeled`` events, oldest first (one request).

        Only the last ``_LABEL_EVENTS_READ`` label events are read: label recovery needs
        just the newest event of a label still on the issue.
        """
        query = """
        query($id: ID!, $last: Int!) { node(id: $id) { __typename ... on Issue {
          id number
          timelineItems(last: $last, itemTypes: [LABELED_EVENT, UNLABELED_EVENT]) {
            nodes {
              __typename
              ... on LabeledEvent {
                id createdAt label { name }
                actor { __typename ... on User { databaseId } ... on Bot { databaseId } }
              }
              ... on UnlabeledEvent {
                id createdAt label { name }
                actor { __typename ... on User { databaseId } ... on Bot { databaseId } }
              }
            }
          }
        } } }
        """
        try:
            data = await self.client.graphql(
                query, {"id": ref.parcel_id, "last": _LABEL_EVENTS_READ}
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))
        node = data.get("node")
        if (
            not isinstance(node, dict)
            or node.get("id") != ref.parcel_id
            or node.get("number") != ref.issue_number
        ):
            return RetryableReadFailure("issue identity did not match requested parcel")
        timeline = node.get("timelineItems")
        nodes = timeline.get("nodes") if isinstance(timeline, dict) else None
        if not isinstance(nodes, list):
            return RetryableReadFailure("issue timeline was unavailable")
        events: list[LabelEvent] = []
        for item in nodes:
            if not isinstance(item, dict):
                continue
            kind = item.get("__typename")
            label = item.get("label")
            name = label.get("name") if isinstance(label, dict) else None
            event_id = item.get("id")
            created = self._parse_time_us(item.get("createdAt"))
            if (
                kind not in ("LabeledEvent", "UnlabeledEvent")
                or not isinstance(name, str)
                or not isinstance(event_id, str)
                or not created
            ):
                continue
            actor = item.get("actor")
            actor_id = actor.get("databaseId") if isinstance(actor, dict) else None
            events.append(
                LabelEvent(
                    event_id=event_id,
                    label=name,
                    labeled=kind == "LabeledEvent",
                    actor_id=(
                        actor_id
                        if isinstance(actor_id, int) and not isinstance(actor_id, bool)
                        else None
                    ),
                    created_at_us=created,
                )
            )
        return tuple(events)

    async def board_issues(self) -> list[BoardIssue] | RetryableReadFailure:
        """Open issues of the repository on the project board, by Status option ID."""
        query = """
        query($id: ID!, $after: String) { node(id: $id) { ... on ProjectV2 {
          items(first: 100, after: $after) {
            nodes {
              status: fieldValueByName(name: "Status") {
                ... on ProjectV2ItemFieldSingleSelectValue { optionId }
              }
              content { __typename ... on Issue {
                id number title state createdAt repository { id }
                assignees(first: 1) { totalCount }
                labels(first: 20) { nodes { name } }
              } }
            }
            pageInfo { hasNextPage endCursor }
          }
        } } }
        """
        issues: list[BoardIssue] = []
        after: str | None = None
        try:
            for _ in range(_BOARD_MAX_PAGES):
                data = await self.client.graphql(
                    query, {"id": self.project_node_id, "after": after}
                )
                node = data.get("node")
                items = node.get("items") if isinstance(node, dict) else None
                nodes = items.get("nodes") if isinstance(items, dict) else None
                if not isinstance(items, dict) or not isinstance(nodes, list):
                    return RetryableReadFailure("project items were unavailable")
                for item in nodes:
                    issue = self._board_issue(item)
                    if issue is not None:
                        issues.append(issue)
                page = items.get("pageInfo")
                if not isinstance(page, dict) or not page.get("hasNextPage"):
                    break
                cursor = page.get("endCursor")
                if not isinstance(cursor, str):
                    return RetryableReadFailure("project items pagination cursor was missing")
                after = cursor
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))
        return issues

    async def board_cards(self) -> list[BoardCard] | RetryableReadFailure:
        """Every repository issue on the board, open or closed, for the board diff.

        Fails (never a partial list) when any page is unreadable or the board has more
        than ``_BOARD_MAX_PAGES`` pages: a partial list would look like removed cards.
        """
        query = """
        query($id: ID!, $after: String) { node(id: $id) { ... on ProjectV2 {
          items(first: 100, after: $after) {
            nodes {
              status: fieldValueByName(name: "Status") {
                ... on ProjectV2ItemFieldSingleSelectValue { optionId }
              }
              bot: fieldValueByName(name: "Bot") {
                ... on ProjectV2ItemFieldSingleSelectValue { optionId }
              }
              autoBuild: fieldValueByName(name: "Auto-build") {
                ... on ProjectV2ItemFieldSingleSelectValue { optionId }
              }
              content { __typename ... on Issue {
                id number title state updatedAt repository { id }
                assignees(first: 20) { totalCount nodes { id } }
                labels(first: 50) { totalCount nodes { name } }
              } }
            }
            pageInfo { hasNextPage endCursor }
          }
        } } }
        """
        cards: list[BoardCard] = []
        after: str | None = None
        try:
            for _ in range(_BOARD_MAX_PAGES):
                data = await self.client.graphql(
                    query, {"id": self.project_node_id, "after": after}
                )
                node = data.get("node")
                items = node.get("items") if isinstance(node, dict) else None
                nodes = items.get("nodes") if isinstance(items, dict) else None
                if not isinstance(items, dict) or not isinstance(nodes, list):
                    return RetryableReadFailure("project items were unavailable")
                for item in nodes:
                    card = self._board_card(item)
                    if card is not None:
                        cards.append(card)
                page = items.get("pageInfo")
                if not isinstance(page, dict) or not page.get("hasNextPage"):
                    return cards
                cursor = page.get("endCursor")
                if not isinstance(cursor, str):
                    return RetryableReadFailure("project items pagination cursor was missing")
                after = cursor
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))
        return RetryableReadFailure("project board has more items than one diff reads")

    def _board_card(self, item: object) -> BoardCard | None:
        if not isinstance(item, dict):
            return None
        content = item.get("content")
        if not isinstance(content, dict) or content.get("__typename") != "Issue":
            return None
        repository = content.get("repository")
        if not isinstance(repository, dict) or repository.get("id") != self.repository_node_id:
            return None
        node_id, number = content.get("id"), content.get("number")
        if not isinstance(node_id, str) or not isinstance(number, int) or isinstance(number, bool):
            return None

        def option(value: object) -> str:
            found = value.get("optionId") if isinstance(value, dict) else None
            return found if isinstance(found, str) else ""

        def names(connection: object, key: str) -> tuple[str, ...]:
            if not isinstance(connection, dict):
                return ("?",)  # unreadable: never equal to a readable value
            nodes = connection.get("nodes")
            values = tuple(
                str(entry[key])
                for entry in (nodes if isinstance(nodes, list) else [])
                if isinstance(entry, dict) and isinstance(entry.get(key), str)
            )
            total = connection.get("totalCount")
            # More than one page: the count still changes with any addition or removal.
            return (*values, f"#{total}") if isinstance(total, int) else values

        return BoardCard(
            node_id=node_id,
            number=number,
            open=content.get("state") == "OPEN",
            status_option=option(item.get("status")),
            bot_option=option(item.get("bot")),
            auto_build_option=option(item.get("autoBuild")) if self._auto_build_configured else "",
            assignees=names(content.get("assignees"), "id"),
            labels=names(content.get("labels"), "name"),
            title=str(content.get("title") or ""),
            updated_at=str(content.get("updatedAt") or ""),
        )

    def _board_issue(self, item: object) -> BoardIssue | None:
        if not isinstance(item, dict):
            return None
        content = item.get("content")
        if not isinstance(content, dict) or content.get("__typename") != "Issue":
            return None  # pull requests and draft items are never parcels
        repository = content.get("repository")
        if not isinstance(repository, dict) or repository.get("id") != self.repository_node_id:
            return None
        node_id, number = content.get("id"), content.get("number")
        if content.get("state") != "OPEN" or not isinstance(node_id, str):
            return None
        if not isinstance(number, int) or isinstance(number, bool):
            return None
        status = item.get("status")
        stage = stage_for_option(
            status.get("optionId") if isinstance(status, dict) else None, self.status_options
        )
        labels = content.get("labels")
        names = labels.get("nodes") if isinstance(labels, dict) else None
        assignees = content.get("assignees")
        assigned = not isinstance(assignees, dict) or assignees.get("totalCount") != 0
        return BoardIssue(
            node_id=node_id,
            number=number,
            title=str(content.get("title") or ""),
            stage=stage,
            labels=tuple(
                str(label["name"])
                for label in (names if isinstance(names, list) else [])
                if isinstance(label, dict) and isinstance(label.get("name"), str)
            ),
            created_at_us=self._parse_time_us(content.get("createdAt")),
            assigned=assigned,
        )

    async def _project_stage(
        self, parcel_id: str
    ) -> tuple[Stage | None, bool, str | None, str | None, str | None]:
        """(stage by Status option ID, in project, current Bot display value or None,
        current Factory note text or None when not read, current Auto-build option name
        ("" = empty) or None when not configured)."""
        query = """
        query($id: ID!, $after: String) {
          node(id: $id) { ... on Issue {
            projectItems(first: 100, after: $after) {
              nodes { id project { id } fieldValueByName(name: "Status") {
                ... on ProjectV2ItemFieldSingleSelectValue {
                  name
                  optionId
                  field { ... on ProjectV2SingleSelectField { id } }
                }
              }
              bot: fieldValueByName(name: "Bot") {
                ... on ProjectV2ItemFieldSingleSelectValue { optionId }
              }
              note: fieldValueByName(name: "Factory note") {
                ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2Field { id } } }
              }
              autoBuild: fieldValueByName(name: "Auto-build") {
                ... on ProjectV2ItemFieldSingleSelectValue {
                  optionId field { ... on ProjectV2SingleSelectField { id } }
                }
              } }
              pageInfo { hasNextPage endCursor }
            }
          } }
        }
        """
        after: str | None = None
        while True:
            data = await self.client.graphql(query, {"id": parcel_id, "after": after})
            node = data.get("node")
            items = node.get("projectItems") if isinstance(node, dict) else None
            if not isinstance(items, dict):
                raise GitHubAPIError("issue projectItems were unavailable")
            nodes = items.get("nodes")
            if not isinstance(nodes, list):
                raise GitHubAPIError("issue projectItems nodes were malformed")
            for item in nodes:
                if not isinstance(item, dict):
                    continue
                project = item.get("project")
                if not isinstance(project, dict) or project.get("id") != self.project_node_id:
                    continue
                bot = self._bot_name(item.get("bot"))
                note = self._note_text(item.get("note"))
                auto_build = self._auto_build_name(item.get("autoBuild"))
                value = item.get("fieldValueByName")
                if value is None:
                    return None, True, bot, note, auto_build
                if not isinstance(value, dict):
                    raise GitHubAPIError("Status field value was malformed")
                field = value.get("field")
                if not isinstance(field, dict) or field.get("id") != self.status_field_node_id:
                    raise GitHubAPIError("Status field identity changed")
                # Read by option ID only: a renamed option must not change the stage.
                stage = stage_for_option(value.get("optionId"), self.status_options)
                return stage, True, bot, note, auto_build
            page = items.get("pageInfo")
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                return None, False, None, None, None
            after_value = page.get("endCursor")
            if not isinstance(after_value, str):
                raise GitHubAPIError("projectItems pagination cursor was missing")
            after = after_value

    def _bot_name(self, value: object) -> str | None:
        """Bot display value for an option ID, by the persisted schema (never by name)."""
        if self.board_schema is None or not isinstance(value, dict):
            return None
        option = value.get("optionId")
        names = [n for n, o in self.board_schema.bot_options.items() if o == option]
        return names[0] if len(names) == 1 else None

    @property
    def _auto_build_configured(self) -> bool:
        return self.board_schema is not None and bool(self.board_schema.auto_build_field_id)

    def _auto_build_name(self, value: object) -> str | None:
        """Auto-build option name by the persisted IDs ("" = empty, "?" = an unknown
        option or field); None when the field is not configured or the value unreadable."""
        if self.board_schema is None or not self.board_schema.auto_build_field_id:
            return None
        if value is None:
            return ""
        if not isinstance(value, dict):
            return None
        if not value:
            return ""  # another field type answered the name: nothing selected here
        field_ref = value.get("field")
        if isinstance(field_ref, dict) and field_ref.get("id") not in (
            None,
            self.board_schema.auto_build_field_id,
        ):
            return "?"
        option = value.get("optionId")
        names = [n for n, o in self.board_schema.auto_build_options.items() if o == option]
        return names[0] if len(names) == 1 else "?"

    def _note_text(self, value: object) -> str | None:
        """Factory note text ("" when empty), by the persisted field ID; None if unread."""
        if self.board_schema is None or not self.board_schema.note_field_id:
            return None
        if value is None:
            return ""
        if not isinstance(value, dict):
            return None
        field = value.get("field")
        if not isinstance(field, dict) or field.get("id") != self.board_schema.note_field_id:
            return None
        text = value.get("text")
        return text if isinstance(text, str) else ""

    async def pull_request(
        self,
        ref: IssueRef,
        pr_number: int,
        *,
        cross_vendor_review: bool = False,
        reviewed_head: str | None = None,
        sync_carry: bool = True,
    ) -> PullRequestEvidence | RetryableReadFailure:
        """Fresh PR facts for readiness (owner direction: the owner's approval is NOT part
        of Ready; the ruleset requires it at merge).

        ``cross_vendor_review`` is Molly's reported opposite-vendor review of this head
        (clean verdict, reviewer vendor differs). A GitHub approval from
        ``independent_reviewer_ids`` is required additionally only when configured.
        When it attested ``reviewed_head`` and the PR has moved on, it still counts only
        if every newer commit merely syncs the base branch (see ``_base_sync_only``), and
        never when ``sync_carry`` is false (the review predates a merge conflict: the merge
        that resolved it needs a fresh review).

        Mergeability is computed lazily by GitHub (``mergeable`` null on a first read): the
        PR is re-read after each of ``mergeable_retry_delays``; still null after that is
        "unknown", never a conflict and never clean.
        """
        if ref.repo_id != self.repository_node_id:
            return RetryableReadFailure("pull request repository identity mismatch")
        try:
            pr = await self._pull(pr_number)
            for delay in self.mergeable_retry_delays:
                if pr.get("state") != "open" or pr.get("mergeable", False) is not None:
                    break
                await self._sleep(delay)
                pr = await self._pull(pr_number)
            head = pr.get("head")
            head_sha = head.get("sha") if isinstance(head, dict) else None
            branch = head.get("ref") if isinstance(head, dict) else None
            if not isinstance(head_sha, str) or not isinstance(branch, str):
                raise GitHubAPIError("pull request head was malformed")
            base = pr.get("base")
            base_ref = base.get("ref") if isinstance(base, dict) else None
            checks = await self._checks_state(
                head_sha, base_ref if isinstance(base_ref, str) else None
            )
            review_accepted = cross_vendor_review
            base_sync = False
            if review_accepted and reviewed_head and reviewed_head != head_sha:
                review_accepted = (
                    sync_carry
                    and isinstance(base_ref, str)
                    and await self._base_sync_only(reviewed_head, head_sha, base_ref)
                )
                base_sync = review_accepted
            if review_accepted and self.independent_reviewer_ids:
                reviews = await self.client.paginate(
                    f"/repos/{self.repository}/pulls/{pr_number}/reviews?per_page=100"
                )
                review_accepted = self._review_accepted(reviews, head_sha)
            findings_dispositioned = await self._bot_threads_settled(pr_number)
            closes_issue = await self._closes_issue(pr_number, ref.parcel_id)
            review_bot_since = await self._review_bot_pending_since(pr, pr_number, head_sha)
            verdict = self._review_bot_verdict(head_sha, review_bot_since, findings_dispositioned)
            author = pr.get("user")
            return PullRequestEvidence(
                pr_number=pr_number,
                head_sha=head_sha,
                open=pr.get("state") == "open",
                merged=bool(pr.get("merged")),
                bot_authored=isinstance(author, dict) and author.get("id") == self.bot_user_id,
                parcel_branch=branch.startswith(self.parcel_branch_prefix),
                closes_issue=closes_issue,
                checks=checks,
                review_accepted=review_accepted,
                findings_dispositioned=findings_dispositioned,
                checks_summary=self._last_checks_summary,
                review_bot_pending_since_us=review_bot_since,
                base_sync=base_sync,
                failing_checks=self._last_failing_checks,
                review_bot_verdict=verdict,
                review_bot_eyes=self._last_bot_eyes,
                open_findings=() if findings_dispositioned else self._last_open_findings,
                findings_earlier_rounds=any(
                    oid != head_sha for oid in self._last_bot_thread_commits
                ),
                mergeable=_mergeable(pr),
                base_head=str(base.get("sha") or "") if isinstance(base, dict) else "",
                base_ref=base_ref if isinstance(base_ref, str) else "",
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

    async def _pull(self, pr_number: int) -> dict[str, Any]:
        pr: Any = await self.client.get_json(f"/repos/{self.repository}/pulls/{pr_number}")
        if not isinstance(pr, dict):
            raise GitHubAPIError("pull request response was malformed")
        return pr

    async def _review_bot_pending_since(
        self, pr: dict[str, Any], pr_number: int, head_sha: str
    ) -> int | None:
        """When the review bot can still respond to ``head_sha`` (see
        ``PullRequestEvidence.review_bot_pending_since_us``).

        It answered the head with a review of that commit, a comment naming it as the
        reviewed commit, or a +1 on the PR taken after the head was pushed (committer date
        and PR creation, whichever is later). An explicit mention by anyone else after its
        last answer re-pings it. None whenever this cannot be told.

        While a trigger is unanswered, ``_last_bot_eyes`` records whether the bot shows it
        is reviewing: its 👀 on the PR, or on the re-ping comment, taken at or after the
        trigger (reactions send no webhook: each read looks again).
        """
        self._last_bot_answer = ""
        self._last_bot_eyes = ""
        login = self.review_bot_login
        if not login:
            return None
        self._last_bot_eyes = BotEyes.UNKNOWN.value  # until the state is read
        try:
            commit: Any = await self.client.get_json(f"/repos/{self.repository}/commits/{head_sha}")
        except GitHubRejected:
            return None
        detail = commit.get("commit") if isinstance(commit, dict) else None
        committer = detail.get("committer") if isinstance(detail, dict) else None
        committed = self._parse_time_us(committer.get("date")) if isinstance(committer, dict) else 0
        opened = self._parse_time_us(pr.get("created_at"))
        if not committed or not opened:
            return None
        pushed = max(committed, opened)
        answers: list[tuple[int, str]] = []
        pings: list[int] = []
        #: Issue comments that re-ping the bot: (time, comment id) - the bot puts its 👀 there.
        ping_comments: list[tuple[int, int]] = []
        reviews = await self.client.paginate(
            f"/repos/{self.repository}/pulls/{pr_number}/reviews?per_page=100"
        )
        comments = await self.client.paginate(
            f"/repos/{self.repository}/issues/{pr_number}/comments?per_page=100"
        )
        reactions = await self.client.paginate(
            f"/repos/{self.repository}/issues/{pr_number}/reactions?per_page=100"
        )
        for item, at_key, kind in (
            *((r, "submitted_at", "review") for r in reviews),
            *((c, "created_at", "comment") for c in comments),
        ):
            if not isinstance(item, dict):
                continue
            user = item.get("user")
            author = user.get("login") if isinstance(user, dict) else None
            body = str(item.get("body") or "")
            at = self._parse_time_us(item.get(at_key))
            if author == login:
                if at and (item.get("commit_id") == head_sha or self._names_commit(body, head_sha)):
                    answers.append((at, kind))
            elif self.review_bot_mention and self.review_bot_mention.lower() in body.lower():
                if not at:
                    return None  # an undatable re-ping could be the latest one
                pings.append(at)
                if kind == "comment" and isinstance(item.get("id"), int):
                    ping_comments.append((at, item["id"]))
        eyes: list[int] = []
        for reaction in reactions:
            if not isinstance(reaction, dict):
                continue
            user = reaction.get("user")
            if not isinstance(user, dict) or user.get("login") != login:
                continue
            at = self._parse_time_us(reaction.get("created_at"))
            if reaction.get("content") == "+1" and at > pushed:
                answers.append((at, "reaction"))
            elif reaction.get("content") == "eyes" and at:
                eyes.append(at)
        if not answers:
            trigger = max([pushed, *pings])
        else:
            answered, self._last_bot_answer = max(answers)
            later = [at for at in pings if at > answered]
            trigger = max(later) if later else 0
        self._last_bot_eyes = await self._bot_eyes(trigger, eyes, ping_comments) if trigger else ""
        return trigger

    async def _bot_eyes(
        self, trigger: int, pr_eyes: list[int], ping_comments: list[tuple[int, int]]
    ) -> str:
        """ "seen" when the bot's 👀 on the PR, or on the comment that is the trigger, was
        taken at or after ``trigger``; "absent" when not; "unknown" when that comment's
        reactions cannot be read."""
        if any(at >= trigger for at in pr_eyes):
            return BotEyes.SEEN.value
        for at, comment_id in ping_comments:
            if at != trigger:
                continue
            try:
                reactions = await self.client.paginate(
                    f"/repos/{self.repository}/issues/comments/{comment_id}/reactions?per_page=100"
                )
            except GitHubRejected:
                return BotEyes.UNKNOWN.value
            for reaction in reactions:
                user = reaction.get("user") if isinstance(reaction, dict) else None
                if (
                    isinstance(user, dict)
                    and user.get("login") == self.review_bot_login
                    and reaction.get("content") == "eyes"
                ):
                    return BotEyes.SEEN.value
        return BotEyes.ABSENT.value

    def _review_bot_verdict(self, head_sha: str, since: int | None, settled: bool) -> str:
        """The review bot's answer to ``head_sha`` for the Ready report ("" = none yet).

        A +1 is a clean verdict; a review (or verdict comment) of the head counts the
        review threads the bot opened on that commit, with or without outcomes.
        """
        if since != 0 or not self._last_bot_answer:
            return ""
        short = head_sha[:7]
        if self._last_bot_answer == "reaction":
            return f"👍 on `{short}`"
        if not settled:
            return f"reviewed `{short}`, findings without outcomes"
        count = sum(1 for oid in self._last_bot_thread_commits if oid == head_sha)
        if not count:
            return f"reviewed `{short}`, no findings"
        return f"reviewed `{short}`, {count} finding{'s' if count != 1 else ''}, all with outcomes"

    @staticmethod
    def _names_commit(body: str, head_sha: str) -> bool:
        """A verdict comment's "Reviewed commit: `<sha prefix>`" names ``head_sha``."""
        match = _REVIEWED_COMMIT.search(body)
        return match is not None and head_sha.startswith(match.group(1).lower())

    async def review_comments(
        self, pr_number: int, review_id: int
    ) -> list[dict[str, Any]] | RetryableReadFailure:
        """Inline comments of one PR review, oldest first (webhooks carry only its body)."""
        try:
            items = await self.client.paginate(
                f"/repos/{self.repository}/pulls/{pr_number}/reviews/{review_id}"
                "/comments?per_page=100"
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))
        comments = [
            {
                "path": str(c.get("path") or ""),
                "line": c.get("line") if isinstance(c.get("line"), int) else None,
                "body": str(c.get("body") or ""),
                "created_at": str(c.get("created_at") or ""),
            }
            for c in items
            if isinstance(c, dict)
        ]
        return sorted(comments, key=lambda c: c["created_at"])

    async def _base_sync_only(self, reviewed: str, head: str, base_ref: str) -> bool:
        """``head`` descends from the reviewed head only through base-branch syncs.

        Walks the feature first-parent chain from ``head`` back to ``reviewed``; commits
        that arrived through a merge's other parent (the base branch's own history) are
        not on that chain and are not judged. Each chain commit must be a merge whose
        other parent is already on ``base_ref`` (e.g. "Update branch") or be authored by
        an owner. Anything else (e.g. a new bot/agent commit) needs a fresh review;
        anything unreadable counts as not a sync.
        """
        try:
            compare: Any = await self.client.get_json(
                f"/repos/{self.repository}/compare/{reviewed}...{head}"
            )
            if not isinstance(compare, dict):
                return False
            commits = compare.get("commits")
            if (
                compare.get("status") != "ahead"
                or not isinstance(commits, list)
                or compare.get("total_commits") != len(commits)
            ):
                return False
            by_sha = {
                c["sha"]: c
                for c in commits
                if isinstance(c, dict) and isinstance(c.get("sha"), str)
            }
            current = head
            for _ in range(len(by_sha) + 1):
                if current == reviewed:
                    return True
                commit = by_sha.get(current)
                if commit is None:
                    return False
                raw = [p.get("sha") for p in commit.get("parents") or [] if isinstance(p, dict)]
                parents = [p for p in raw if isinstance(p, str)]
                if not parents or len(parents) != len(raw):
                    return False
                author = commit.get("author")
                author_id = author.get("id") if isinstance(author, dict) else None
                owner = author_id in self.owner_ids and author_id != self.bot_user_id
                if not owner:
                    if len(parents) < 2:
                        return False
                    for other in parents[1:]:
                        on_base: Any = await self.client.get_json(
                            f"/repos/{self.repository}/compare/{base_ref}...{other}"
                        )
                        if not isinstance(on_base, dict) or on_base.get("status") not in (
                            "behind",
                            "identical",
                        ):
                            return False
                current = parents[0]
            return False
        except GitHubRejected as exc:
            if exc.status_code in (404, 422):
                return False  # e.g. a force-push rewrote history: not a base sync
            raise

    async def _closes_issue(self, pr_number: int, issue_node_id: str) -> bool:
        """GitHub's closing references for this PR include exactly the parcel's issue.

        Uses ``closingIssuesReferences`` (what "Closes #N" and the Development panel
        produce), matched by issue node ID, so a PR closing some other issue - or text
        naming the right number in another repository - never satisfies the linkage.
        """
        owner, _, name = self.repository.partition("/")
        query = """
        query($owner: String!, $name: String!, $number: Int!, $after: String) {
          repository(owner: $owner, name: $name) {
            id
            pullRequest(number: $number) {
              number
              closingIssuesReferences(first: 100, after: $after) {
                nodes { id repository { id } }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
        """
        after: str | None = None
        while True:
            data = await self.client.graphql(
                query, {"owner": owner, "name": name, "number": pr_number, "after": after}
            )
            repository = data.get("repository")
            if not isinstance(repository, dict) or repository.get("id") != self.repository_node_id:
                raise GitHubAPIError("pull request repository identity changed")
            pull = repository.get("pullRequest")
            if not isinstance(pull, dict) or pull.get("number") != pr_number:
                raise GitHubAPIError("pull request closing references were unavailable")
            refs = pull.get("closingIssuesReferences")
            nodes = refs.get("nodes") if isinstance(refs, dict) else None
            if not isinstance(nodes, list):
                raise GitHubAPIError("closing issue references were malformed")
            for node in nodes:
                if (
                    isinstance(node, dict)
                    and node.get("id") == issue_node_id
                    and isinstance(node.get("repository"), dict)
                    and node["repository"].get("id") == self.repository_node_id
                ):
                    return True
            page = refs.get("pageInfo") if isinstance(refs, dict) else None
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                return False
            cursor = page.get("endCursor")
            if not isinstance(cursor, str):
                raise GitHubAPIError("closing references pagination cursor was missing")
            after = cursor

    async def _checks_state(self, head_sha: str, base_ref: str | None = None) -> ChecksState:
        """CI state of ``head_sha`` for readiness.

        Required set: configured ``required_checks`` if any, else derived from GitHub
        (classic branch protection and/or rulesets on the base branch; unreadable sources
        are skipped). With a required set, each must be present and succeeded (success,
        neutral or skipped). With nothing required, every check run and commit status on
        the head must have completed that way, and there must be at least one. Pending
        means not ready; any failure means failed. "No configuration" is never FAILED.

        A required check is decided by its latest attempt (name + app, highest run ID),
        as in GitHub's own evaluation. Without a required set, a cancelled run superseded
        by a later run of the same check is history, not a failure; every other run
        counts. A latest attempt that is itself cancelled still blocks.
        """
        fetched: list[tuple[int, str, int | None, str, str]] = []  # id, name, app, state, label
        runs: list[tuple[str, int | None, str]] = []  # (name, app id, state)
        labels: list[str] = []  # conclusion-level labels for the human summary
        page = 1
        while True:
            result: Any = await self.client.get_json(
                f"/repos/{self.repository}/commits/{head_sha}/check-runs?per_page=100&page={page}"
            )
            batch = result.get("check_runs") if isinstance(result, dict) else None
            if not isinstance(batch, list):
                raise GitHubAPIError("check-runs response was malformed")
            for run in batch:
                if not isinstance(run, dict) or not isinstance(run.get("name"), str):
                    continue
                app = run.get("app")
                app_id = app.get("id") if isinstance(app, dict) else None
                raw_id = run.get("id")
                conclusion = run.get("conclusion")
                fetched.append(
                    (
                        raw_id if isinstance(raw_id, int) else 0,
                        str(run["name"]),
                        app_id if isinstance(app_id, int) else None,
                        _run_state(run.get("status"), conclusion),
                        str(conclusion)
                        if run.get("status") == "completed" and isinstance(conclusion, str)
                        else "pending",
                    )
                )
            if len(batch) < 100:
                break
            page += 1
        newest: dict[tuple[str, int | None], int] = {}
        for run_id, name, app_id, _, _ in fetched:
            newest[(name, app_id)] = max(run_id, newest.get((name, app_id), run_id))
        latest: dict[tuple[str, int | None], str] = {}  # required-check view
        for run_id, name, app_id, state, label in sorted(fetched, key=lambda r: r[0]):
            latest[(name, app_id)] = state
            if label == "cancelled" and run_id < newest[(name, app_id)]:
                continue  # superseded by a later attempt of the same check
            runs.append((name, app_id, state))
            labels.append(label)
        combined: Any = await self.client.get_json(
            f"/repos/{self.repository}/commits/{head_sha}/status"
        )
        statuses = combined.get("statuses") if isinstance(combined, dict) else None
        # Legacy statuses are history too: the latest per context (highest ID) decides.
        latest_status: dict[str, tuple[int, str, str]] = {}
        for status in statuses if isinstance(statuses, list) else []:
            if isinstance(status, dict) and isinstance(status.get("context"), str):
                raw_id = status.get("id")
                status_id = raw_id if isinstance(raw_id, int) else 0
                context = str(status["context"])
                if context not in latest_status or status_id >= latest_status[context][0]:
                    latest_status[context] = (
                        status_id,
                        _status_state(status.get("state")),
                        str(status.get("state") or "pending"),
                    )
        # Statuses carry no app: they can satisfy only an unpinned requirement.
        legacy = {context: state for context, (_, state, _) in latest_status.items()}
        for context, (_, state, label) in latest_status.items():
            runs.append((context, None, state))
            labels.append(label)
        self._last_checks_summary = _checks_summary(labels)
        self._last_failing_checks = ""
        required: set[tuple[str, int | None]] = set(self.required_checks)
        if not required and base_ref is not None:
            required = await self._derived_required_checks(base_ref)
        if required:
            states = []
            failing: list[str] = []
            for name, app_id in sorted(required, key=lambda c: (c[0], c[1] or 0)):
                # A pinned requirement is satisfied only by that exact app's latest run.
                found = [
                    state
                    for (run_name, run_app), state in latest.items()
                    if run_name == name and (app_id is None or run_app == app_id)
                ]
                if app_id is None and name in legacy:
                    found.append(legacy[name])
                states.append(_combine(found) if found else "pending")
                if states[-1] == "failed":
                    failing.append(name)
            self._last_failing_checks = _names(failing)
            return _checks_from(states)
        self._last_failing_checks = _names([name for name, _, state in runs if state == "failed"])
        return _checks_from([state for _, _, state in runs]) if runs else ChecksState.PENDING

    async def _derived_required_checks(self, base_ref: str) -> set[tuple[str, int | None]]:
        required: set[tuple[str, int | None]] = set()
        try:
            protection: Any = await self.client.get_json(
                f"/repos/{self.repository}/branches/{base_ref}/protection/required_status_checks"
            )
            protection = protection if isinstance(protection, dict) else {}
            for check in protection.get("checks") or []:
                if isinstance(check, dict) and isinstance(check.get("context"), str):
                    app = check.get("app_id")
                    required.add((check["context"], app if isinstance(app, int) else None))
            if not protection.get("checks"):
                for context in protection.get("contexts") or []:
                    if isinstance(context, str):
                        required.add((context, None))
        except GitHubRejected as exc:
            if exc.status_code not in (403, 404):
                raise
        try:
            rules: Any = await self.client.get_json(
                f"/repos/{self.repository}/rules/branches/{base_ref}"
            )
            for rule in rules if isinstance(rules, list) else []:
                if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
                    continue
                params = rule.get("parameters")
                checks = params.get("required_status_checks") if isinstance(params, dict) else None
                for check in checks if isinstance(checks, list) else []:
                    if isinstance(check, dict) and isinstance(check.get("context"), str):
                        app = check.get("integration_id")
                        required.add((check["context"], app if isinstance(app, int) else None))
        except GitHubRejected as exc:
            if exc.status_code not in (403, 404):
                raise
        return required

    async def _bot_threads_settled(self, pr_number: int) -> bool:
        """Every review thread opened by a bot has an outcome: answered by the factory bot,
        or resolved by a person. A thread the factory bot resolved without replying
        (``resolvedBy`` is a user, so a bot resolver reads as null) is still open.

        A complete read also records the commit of each thread the review bot opened
        (its findings count in the Ready report) and the threads without an outcome (for
        the owner's Needs you comment)."""
        owner, _, name = self.repository.partition("/")
        query = """
        query($owner: String!, $name: String!, $number: Int!, $after: String) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) {
              reviewThreads(first: 100, after: $after) {
                nodes {
                  isResolved
                  resolvedBy { login }
                  path
                  comments(first: 50) {
                    nodes {
                      author { __typename login ... on Bot { databaseId } }
                      originalCommit { oid }
                      body
                      url
                    }
                  }
                }
                pageInfo { hasNextPage endCursor }
              }
            }
          }
        }
        """
        after: str | None = None
        self._last_bot_thread_commits = ()
        self._last_open_findings = ()
        bot_login = self.review_bot_login.removesuffix("[bot]")  # GraphQL omits "[bot]"
        bot_commits: list[str] = []
        open_findings: list[FindingRef] = []
        while True:
            data = await self.client.graphql(
                query, {"owner": owner, "name": name, "number": pr_number, "after": after}
            )
            repository = data.get("repository")
            pull = repository.get("pullRequest") if isinstance(repository, dict) else None
            threads = pull.get("reviewThreads") if isinstance(pull, dict) else None
            nodes = threads.get("nodes") if isinstance(threads, dict) else None
            if not isinstance(nodes, list):
                raise GitHubAPIError("review threads were unavailable")
            for thread in nodes:
                if not isinstance(thread, dict):
                    continue
                comments = thread.get("comments")
                oid = _bot_thread_commit(comments, bot_login)
                if oid is not None:
                    bot_commits.append(oid)
                if thread.get("isResolved") is True and thread.get("resolvedBy"):
                    continue  # a person resolved it
                authors = [
                    c.get("author") or {}
                    for c in (comments.get("nodes") if isinstance(comments, dict) else None) or []
                    if isinstance(c, dict)
                ]
                if not authors or authors[0].get("__typename") != "Bot":
                    continue  # human threads are the owner's merge-time concern
                answered = any(
                    a.get("__typename") == "Bot" and a.get("databaseId") == self.bot_user_id
                    for a in authors[1:]
                )
                if not answered:
                    open_findings.append(_finding_ref(thread))
            page = threads.get("pageInfo") if isinstance(threads, dict) else None
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                self._last_bot_thread_commits = tuple(bot_commits)
                self._last_open_findings = tuple(open_findings[:_MAX_FINDINGS])
                return not open_findings
            cursor = page.get("endCursor")
            if not isinstance(cursor, str):
                raise GitHubAPIError("review threads pagination cursor was missing")
            after = cursor

    def _review_accepted(self, reviews: list[Any], head_sha: str) -> bool:
        latest: dict[int, tuple[int, str, str | None]] = {}
        for review in reviews:
            if not isinstance(review, dict):
                continue
            user = review.get("user")
            user_id = user.get("id") if isinstance(user, dict) else None
            review_id = review.get("id")
            state = review.get("state")
            if (
                not isinstance(user_id, int)
                or not isinstance(review_id, int)
                or not isinstance(state, str)
            ):
                continue
            if (
                user_id not in self.independent_reviewer_ids
                or user_id in self.owner_ids
                or user_id == self.bot_user_id
            ):
                continue
            current = latest.get(user_id)
            if current is None or review_id > current[0]:
                latest[user_id] = (review_id, state.upper(), review.get("commit_id"))
        return any(
            state == "APPROVED" and commit_id == head_sha for _, state, commit_id in latest.values()
        )

    async def find_contract_publication(
        self, ref: IssueRef, effect_id: str
    ) -> ContractPublication | RetryableReadFailure | None:
        marker = self._effect_marker(effect_id)
        try:
            comments = await self.client.paginate(
                f"/repos/{self.repository}/issues/{ref.issue_number}/comments?per_page=100"
            )
            for comment in comments:
                if not isinstance(comment, dict) or marker not in str(comment.get("body", "")):
                    continue
                author = comment.get("user")
                author_is_bot = isinstance(author, dict) and author.get("id") == self.bot_user_id
                if not author_is_bot:
                    continue
                body = str(comment.get("body", ""))
                return ContractPublication(
                    comment_id=str(comment.get("id", "")),
                    author_is_bot=True,
                    contract_section=extract_contract_section(body),
                    marker_hash=marker_hash(body),
                    posted_at_us=self._parse_time_us(comment.get("created_at")),
                )
            return None
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        del ctx
        if effect.kind not in self.handled_kinds:
            return DefinitiveFailure(f"unsupported GitHub effect: {effect.kind}")
        try:
            if effect.kind in {
                EffectKind.POST_COMMENT,
                EffectKind.PUBLISH_CONTRACT,
                EffectKind.PUBLISH_TRIAGE,
                EffectKind.PUBLISH_REPORT,
            }:
                return await self._post_comment(effect)
            if effect.kind == EffectKind.EDIT_REPORT:
                return await self._edit_report(effect)
            if effect.kind in {EffectKind.MOVE_CARD, EffectKind.SET_BOT}:
                return await self._write_board(effect)
            if effect.kind in {
                EffectKind.SET_NOTE,
                EffectKind.SET_AUTO_BUILD,
                EffectKind.REACT_COMMENT,
            }:
                return await self._display_write(effect)
            if effect.kind == EffectKind.ENSURE_PROJECT_ITEM:
                return await self._ensure_project_item(effect)
            if effect.kind == EffectKind.FETCH_PR_EVIDENCE:
                read_started_us = self._now_us()  # before any read: covers earlier webhooks
                number = effect.args.get("pr_number")
                if not isinstance(number, int):
                    return DefinitiveFailure("FETCH_PR_EVIDENCE requires pr_number")
                ref = await self._issue_ref(effect)
                if ref is None:
                    return DefinitiveFailure("FETCH_PR_EVIDENCE requires the parcel issue")
                review = (
                    await self.cross_vendor_review(effect)
                    if self.cross_vendor_review is not None
                    else False
                )
                reviewed = effect.args.get("reviewed_head")
                evidence = await self.pull_request(
                    ref,
                    number,
                    cross_vendor_review=review,
                    reviewed_head=reviewed if isinstance(reviewed, str) else None,
                    sync_carry=effect.args.get("sync_carry") is not False,
                )
                if isinstance(evidence, RetryableReadFailure):
                    return evidence
                detail = asdict(evidence)
                detail["checks"] = evidence.checks.value
                expected_head = effect.args.get("head_sha")
                head_matches = (
                    not isinstance(expected_head, str) or expected_head == evidence.head_sha
                )
                detail["head_matches"] = head_matches
                detail["verified"] = evidence.verified and head_matches
                detail["read_started_us"] = read_started_us
                return Ack(str(number), detail)
            if effect.kind == EffectKind.RECONCILE_PARCEL:
                ref = await self._issue_ref(effect)
                if ref is None:
                    return DefinitiveFailure("RECONCILE_PARCEL requires parcel and issue number")
                parcel_id = ref.parcel_id
                snapshot = await self.issue_snapshot(ref)
                if isinstance(snapshot, RetryableReadFailure):
                    return snapshot
                detail = asdict(snapshot)
                detail["stage"] = None if snapshot.stage is None else snapshot.stage.value
                return Ack(parcel_id, detail)
        except AmbiguousRequest as exc:
            return AmbiguousWrite(str(exc))
        except GitHubRejected as exc:
            if 400 <= exc.status_code < 500:
                return DefinitiveFailure(str(exc))
            if effect.kind in {
                EffectKind.MOVE_CARD,
                EffectKind.SET_BOT,
                EffectKind.POST_COMMENT,
                EffectKind.PUBLISH_CONTRACT,
                EffectKind.PUBLISH_TRIAGE,
                EffectKind.PUBLISH_REPORT,
                EffectKind.ENSURE_PROJECT_ITEM,
            }:
                return AmbiguousWrite(str(exc))
            return RetryableReadFailure(str(exc))
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            if effect.kind in {
                EffectKind.MOVE_CARD,
                EffectKind.SET_BOT,
                EffectKind.ENSURE_PROJECT_ITEM,
            }:
                return AmbiguousWrite(str(exc))
            return RetryableReadFailure(str(exc))
        return DefinitiveFailure("unreachable GitHub effect dispatch")

    async def _post_comment(self, effect: EffectIntent) -> AdapterOutcome:
        ref = await self._issue_ref(effect, require_parcel=False)
        text = effect.args.get("body")
        if text is None:
            text = await self._render_publication(effect)
        if ref is None:
            return DefinitiveFailure("comment effect could not resolve the parcel issue")
        if not isinstance(text, str):
            return DefinitiveFailure("comment effect could not render its body")
        issue_number = ref.issue_number
        adopted = await self._find_marked_comment(ref, effect.effect_id)
        if isinstance(adopted, RetryableReadFailure):
            return adopted
        if adopted is not None:
            if effect.kind == EffectKind.PUBLISH_CONTRACT:
                publication = await self.find_contract_publication(ref, effect.effect_id)
                if isinstance(publication, RetryableReadFailure):
                    return publication
                if publication is None or not _publication_matches(effect, publication, text):
                    return AmbiguousWrite("contract comment exists but exact bytes are unverified")
                return Ack(
                    adopted,
                    {
                        "adopted": True,
                        "verified": True,
                        "posted_at_us": publication.posted_at_us,
                    },
                )
            return await self._after_comment(effect, ref, adopted, {"adopted": True})
        body = f"{text}\n\n{self._effect_marker(effect.effect_id)}"
        try:
            response = await self.client.request(
                "POST",
                f"/repos/{self.repository}/issues/{issue_number}/comments",
                json_body={"body": body},
                expected=frozenset({201}),
            )
        except AmbiguousRequest as exc:
            return AmbiguousWrite(str(exc))
        try:
            data: Any = response.json()
        except ValueError:
            return AmbiguousWrite("comment succeeded but response body was malformed")
        comment_id = data.get("id") if isinstance(data, dict) else None
        if not isinstance(comment_id, int):
            return AmbiguousWrite("comment response omitted id")
        if effect.kind == EffectKind.PUBLISH_CONTRACT:
            publication = await self.find_contract_publication(ref, effect.effect_id)
            if isinstance(publication, RetryableReadFailure):
                return publication
            if publication is None or not _publication_matches(effect, publication, text):
                return AmbiguousWrite("contract comment posted but exact bytes are unverified")
            return Ack(
                str(comment_id),
                {
                    "verified": True,
                    "posted_at_us": publication.posted_at_us,
                },
            )
        return await self._after_comment(effect, ref, str(comment_id), {})

    async def _after_comment(
        self,
        effect: EffectIntent,
        ref: IssueRef,
        comment_id: str,
        detail: dict[str, JsonValue],
    ) -> AdapterOutcome:
        """Triage also fills unset Priority/Size and adds existing informational labels.

        Runs after the comment exists (created or adopted by marker), so a retry never
        duplicates the comment; every step below is idempotent. Transient failures retry
        the whole effect; a GitHub refusal is logged and does not undo the publication.
        """
        if effect.kind != EffectKind.PUBLISH_TRIAGE:
            return Ack(comment_id, detail)
        if self.related_marker is not None:
            self.related_marker(effect)  # background, idempotent: a retry marks once
        if self.triage_fields is None:
            return Ack(comment_id, detail)
        fields = await self.triage_fields(effect)
        if fields is None:
            return Ack(comment_id, detail)
        try:
            detail["fields"] = await self._apply_triage_fields(ref, fields)
            detail["labels"] = list(await self._apply_triage_labels(ref, fields))
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubRejected as exc:
            if exc.status_code >= 500:
                return RetryableReadFailure(str(exc))
            LOG.warning(
                "triage fields/labels refused issue=%s status=%s", ref.issue_number, exc.status_code
            )
            detail["fields_error"] = f"HTTP {exc.status_code}"
        except (GitHubAPIError, AmbiguousRequest) as exc:
            return RetryableReadFailure(f"triage fields/labels incomplete: {exc}")
        return Ack(comment_id, detail)

    async def _apply_triage_fields(
        self, ref: IssueRef, fields: TriageFields
    ) -> dict[str, JsonValue]:
        item_id = await self._project_item_id(ref.parcel_id)
        if item_id is None:
            return {}
        schema = await self._single_select_fields(TRIAGE_FIELDS)
        applied: dict[str, JsonValue] = {}
        for name, value in (("Priority", fields.priority), ("Size", fields.size)):
            field = schema.get(name)
            option_id = field[1].get(value) if field is not None else None
            if field is None or option_id is None:
                applied[name] = "unavailable"
                continue
            current = await self._field_option(item_id, field[0], name)
            if current == option_id:
                applied[name] = value
                continue
            if current is not None:
                # The owner (or an earlier triage) already chose: never overwrite it.
                applied[name] = "kept"
                continue
            await self._set_single_select(item_id, field[0], option_id)
            applied[name] = value
        return applied

    async def _apply_triage_labels(self, ref: IssueRef, fields: TriageFields) -> tuple[str, ...]:
        wanted = {
            label.casefold()
            for label in fields.labels
            if not label.casefold().startswith("factory:")
        }
        if not wanted:
            return ()
        existing = await self.client.paginate(f"/repos/{self.repository}/labels?per_page=100")
        names = tuple(
            sorted(
                str(label["name"])
                for label in existing
                if isinstance(label, dict)
                and isinstance(label.get("name"), str)
                and label["name"].casefold() in wanted
            )
        )
        if names:
            await self.client.request(
                "POST",
                f"/repos/{self.repository}/issues/{ref.issue_number}/labels",
                json_body={"labels": list(names)},
                expected=frozenset({200}),
            )
        return names

    async def _single_select_fields(
        self, names: tuple[str, ...]
    ) -> dict[str, tuple[str, dict[str, str]]]:
        query = """
        query($id: ID!) { node(id: $id) { ... on ProjectV2 { fields(first: 100) {
          nodes { ... on ProjectV2SingleSelectField { id name options { id name } } }
        } } } }
        """
        data = await self.client.graphql(query, {"id": self.project_node_id})
        node = data.get("node")
        container = node.get("fields") if isinstance(node, dict) else None
        rows = container.get("nodes") if isinstance(container, dict) else None
        if not isinstance(rows, list):
            raise GitHubAPIError("project fields were unavailable")
        found: dict[str, tuple[str, dict[str, str]]] = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("name") not in names:
                continue
            field_id = row.get("id")
            options = row.get("options")
            if not isinstance(field_id, str) or not isinstance(options, list):
                continue
            found[str(row["name"])] = (
                field_id,
                {
                    str(option["name"]): str(option["id"])
                    for option in options
                    if isinstance(option, dict) and "name" in option and "id" in option
                },
            )
        return found

    async def _set_single_select(self, item_id: str, field_id: str, option_id: str) -> None:
        mutation = """
        mutation($input: UpdateProjectV2ItemFieldValueInput!) {
          updateProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
        }
        """
        await self.client.graphql(
            mutation,
            {
                "input": {
                    "projectId": self.project_node_id,
                    "itemId": item_id,
                    "fieldId": field_id,
                    "value": {"singleSelectOptionId": option_id},
                }
            },
        )

    async def rerender_comment(self, effect: EffectIntent) -> AdapterOutcome:
        """Operator: re-render a published comment in place, found by its effect marker.

        Edits the one bot comment carrying this effect's marker (never posts a second
        one). A contract comment must verify afterwards exactly as at publication.
        """
        if effect.kind not in _COMMENT_KINDS:
            return DefinitiveFailure("only comment effects can be re-rendered")
        ref = await self._issue_ref(effect, require_parcel=False)
        text = effect.args.get("body")
        if text is None:
            text = await self._render_publication(effect)
        if ref is None or not isinstance(text, str):
            return DefinitiveFailure("could not resolve the issue or render the comment")
        try:
            found = await self._find_marked_comment(ref, effect.effect_id)
            if isinstance(found, RetryableReadFailure):
                return found
            if found is None:
                return DefinitiveFailure("no bot comment carries this effect marker")
            await self.client.request(
                "PATCH",
                f"/repos/{self.repository}/issues/comments/{found}",
                json_body={"body": f"{text}\n\n{self._effect_marker(effect.effect_id)}"},
                expected=frozenset({200}),
            )
            if effect.kind == EffectKind.PUBLISH_CONTRACT:
                publication = await self.find_contract_publication(ref, effect.effect_id)
                if isinstance(publication, RetryableReadFailure):
                    return publication
                if not _publication_matches(effect, publication, text):
                    return AmbiguousWrite("re-rendered contract comment did not verify")
        except AmbiguousRequest as exc:
            return AmbiguousWrite(str(exc))
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return DefinitiveFailure(str(exc))
        return Ack(found, {"rerendered": True, "issue_number": ref.issue_number})

    async def _find_marked_comment(
        self, ref: IssueRef, effect_id: str
    ) -> str | RetryableReadFailure | None:
        found = await self._marked_comment(ref, effect_id)
        return found[0] if isinstance(found, tuple) else found

    async def _marked_comment(
        self, ref: IssueRef, effect_id: str
    ) -> tuple[str, str] | RetryableReadFailure | None:
        """(id, body) of the one bot comment carrying ``effect_id``'s marker."""
        marker = self._effect_marker(effect_id)
        try:
            comments = await self.client.paginate(
                f"/repos/{self.repository}/issues/{ref.issue_number}/comments?per_page=100"
            )
            matches = [
                (str(comment.get("id")), str(comment.get("body", "")))
                for comment in comments
                if isinstance(comment, dict)
                and marker in str(comment.get("body", ""))
                and isinstance(comment.get("user"), dict)
                and comment["user"].get("id") == self.bot_user_id
            ]
            if len(matches) > 1:
                return RetryableReadFailure("multiple bot comments carry the same effect marker")
            return matches[0] if matches else None
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

    async def _edit_report(self, effect: EffectIntent) -> AdapterOutcome:
        """Bring a posted Ready report's factory lines up to date, in place.

        Finds the comment by its PUBLISH_REPORT marker (never posts one) and rewrites it
        only when the text differs and everything before the factory lines (the heading
        and the agent's summary) is unchanged. A PATCH of the same body is idempotent, so
        an unclear outcome is retried as a read.
        """
        report_id = effect.args.get("report_effect_id")
        ref = await self._issue_ref(effect, require_parcel=False)
        text = await self._render_publication(effect)
        if not isinstance(report_id, str) or not report_id or ref is None:
            return DefinitiveFailure("report edit could not resolve its comment")
        if not isinstance(text, str):
            return DefinitiveFailure("report edit could not render its body")
        found = await self._marked_comment(ref, report_id)
        if isinstance(found, RetryableReadFailure):
            return found
        if found is None:
            return DefinitiveFailure("no bot comment carries the report marker")
        comment_id, current = found
        body = f"{text}\n\n{self._effect_marker(report_id)}"
        if current == body:
            return Ack(comment_id, {"edited": False})
        cut = text.find(_REPORT_FACTORY_LINES)
        if cut < 0 or not current.startswith(text[:cut]):
            LOG.warning("ready report edit skipped: its summary differs comment=%s", comment_id)
            return Ack(comment_id, {"edited": False, "reason": "summary-differs"})
        try:
            await self.client.request(
                "PATCH",
                f"/repos/{self.repository}/issues/comments/{comment_id}",
                json_body={"body": body},
                expected=frozenset({200}),
            )
        except AmbiguousRequest as exc:
            return RetryableReadFailure(str(exc))
        return Ack(comment_id, {"edited": True})

    async def _write_board(self, effect: EffectIntent) -> AdapterOutcome:
        item_id = effect.args.get("item_id")
        field_id = effect.args.get("field_id")
        option_id = effect.args.get("option_id")
        expected_option_id = effect.args.get("expected_option_id")
        field_name = effect.args.get("field_name", "Status")
        binding = await self._binding(effect)
        if item_id is None and binding is not None:
            item_id = binding.project_item_id
        if item_id is None and effect.parcel_id is not None:
            item_id = await self._project_item_id(effect.parcel_id)
        if self.board_schema is not None and field_id is None:
            if effect.kind == EffectKind.MOVE_CARD:
                field_id = self.board_schema.status_field_id
                option_id = self.board_schema.status_options.get(str(effect.args.get("to")))
                source = effect.args.get("expected_from")
                expected_option_id = (
                    None if source is None else self.board_schema.status_options.get(str(source))
                )
                field_name = "Status"
            else:
                field_id = self.board_schema.bot_field_id
                option_id = self.board_schema.bot_options.get(str(effect.args.get("bot")))
                field_name = "Bot"
        if not all(isinstance(value, str) for value in (item_id, field_id, option_id)):
            return DefinitiveFailure("board write requires item_id, field_id and option_id")
        if not isinstance(field_name, str):
            return DefinitiveFailure("board write field_name must be text")
        current = await self._field_option(str(item_id), str(field_id), field_name)
        if current == option_id:
            return Ack(str(item_id), {"adopted": True, "option_id": str(option_id)})
        if expected_option_id is not None and current != expected_option_id:
            return DefinitiveFailure("board field changed since the effect source snapshot")
        await self._set_single_select(str(item_id), str(field_id), str(option_id))
        observed = await self._field_option(str(item_id), str(field_id), field_name)
        if observed != option_id:
            return AmbiguousWrite("board write did not read back at the requested option id")
        return Ack(str(item_id), {"option_id": str(option_id)})

    async def _display_write(self, effect: EffectIntent) -> AdapterOutcome:
        """Factory note / command reaction: idempotent display writes, never authority.

        Both are safe to repeat (a text value is set, a reaction exists at most once per
        user), so an uncertain outcome is retried instead of blocking the parcel.
        """
        try:
            if effect.kind == EffectKind.SET_NOTE:
                return await self._write_note(effect)
            if effect.kind == EffectKind.SET_AUTO_BUILD:
                return await self._write_auto_build(effect)
            return await self._react(effect)
        except AmbiguousRequest as exc:
            return RetryableReadFailure(f"display write uncertain: {exc}")
        except GitHubRejected as exc:
            if 400 <= exc.status_code < 500:
                return DefinitiveFailure(str(exc))
            return RetryableReadFailure(str(exc))

    async def _write_note(self, effect: EffectIntent) -> AdapterOutcome:
        field_id = self.board_schema.note_field_id if self.board_schema is not None else ""
        note = effect.args.get("note")
        if not field_id:
            return DefinitiveFailure("no Factory note field is configured")
        if not isinstance(note, str):
            return DefinitiveFailure("SET_NOTE requires note text")
        binding = await self._binding(effect)
        item_id = binding.project_item_id if binding is not None else None
        if item_id is None and effect.parcel_id is not None:
            item_id = await self._project_item_id(effect.parcel_id)
        if item_id is None:
            return DefinitiveFailure("the parcel has no project item")
        query = """
        query($id: ID!) { node(id: $id) { ... on ProjectV2Item {
          fieldValueByName(name: "Factory note") { ... on ProjectV2ItemFieldTextValue {
            text field { ... on ProjectV2Field { id } }
          } }
        } } }
        """
        data = await self.client.graphql(query, {"id": item_id})
        node = data.get("node")
        current = self._note_text(node.get("fieldValueByName") if isinstance(node, dict) else None)
        if current == note:
            return Ack(item_id, {"adopted": True, "note": note})
        variables: dict[str, Any] = {
            "input": {"projectId": self.project_node_id, "itemId": item_id, "fieldId": field_id}
        }
        if note:
            variables["input"]["value"] = {"text": note}
            mutation = """
            mutation($input: UpdateProjectV2ItemFieldValueInput!) {
              updateProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
            }
            """
        else:
            mutation = """
            mutation($input: ClearProjectV2ItemFieldValueInput!) {
              clearProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
            }
            """
        await self.client.graphql(mutation, variables)
        return Ack(item_id, {"note": note})

    async def _write_auto_build(self, effect: EffectIntent) -> AdapterOutcome:
        """Set the "Auto-build" field to ``args.value`` (an option name, "" clears it)."""
        schema = self.board_schema
        value = effect.args.get("value")
        if schema is None or not schema.auto_build_field_id:
            return DefinitiveFailure("no Auto-build field is configured")
        if not isinstance(value, str):
            return DefinitiveFailure("SET_AUTO_BUILD requires a value")
        option_id = schema.auto_build_options.get(value) if value else None
        if value and option_id is None:
            return DefinitiveFailure(f"no Auto-build option id is configured for {value}")
        binding = await self._binding(effect)
        item_id = binding.project_item_id if binding is not None else None
        if item_id is None and effect.parcel_id is not None:
            item_id = await self._project_item_id(effect.parcel_id)
        if item_id is None:
            return DefinitiveFailure("the parcel has no project item")
        field_id = schema.auto_build_field_id
        current = await self._field_option(item_id, field_id, "Auto-build")
        if (current or None) == option_id:
            return Ack(item_id, {"adopted": True, "value": value})
        if option_id is not None:
            await self._set_single_select(item_id, field_id, option_id)
        else:
            await self.client.graphql(
                """
                mutation($input: ClearProjectV2ItemFieldValueInput!) {
                  clearProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
                }
                """,
                {
                    "input": {
                        "projectId": self.project_node_id,
                        "itemId": item_id,
                        "fieldId": field_id,
                    }
                },
            )
        return Ack(item_id, {"value": value})

    async def _react(self, effect: EffectIntent) -> AdapterOutcome:
        comment_id = effect.args.get("comment_id")
        content = effect.args.get("content")
        if not isinstance(comment_id, int) or content not in {"+1", "confused"}:
            return DefinitiveFailure("REACT_COMMENT requires comment_id and +1/confused")
        await self.client.request(
            "POST",
            f"/repos/{self.repository}/issues/comments/{comment_id}/reactions",
            json_body={"content": content},
            expected=frozenset({200, 201}),
        )
        return Ack(str(comment_id), {"content": str(content)})

    async def _project_item_id(self, parcel_id: str) -> str | None:
        query = """
        query($id: ID!, $after: String) { node(id: $id) { ... on Issue {
          projectItems(first: 100, after: $after) {
            nodes { id project { id } }
            pageInfo { hasNextPage endCursor }
          }
        } } }
        """
        after: str | None = None
        while True:
            data = await self.client.graphql(query, {"id": parcel_id, "after": after})
            node = data.get("node")
            items = node.get("projectItems") if isinstance(node, dict) else None
            nodes = items.get("nodes") if isinstance(items, dict) else None
            if not isinstance(nodes, list):
                raise GitHubAPIError("issue projectItems nodes were malformed")
            matches = [
                item.get("id")
                for item in nodes
                if isinstance(item, dict)
                and isinstance(item.get("project"), dict)
                and item["project"].get("id") == self.project_node_id
                and isinstance(item.get("id"), str)
            ]
            if len(matches) > 1:
                raise GitHubAPIError("issue has multiple items in the configured project")
            if matches:
                return str(matches[0])
            page = items.get("pageInfo") if isinstance(items, dict) else None
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                return None
            cursor = page.get("endCursor")
            if not isinstance(cursor, str):
                raise GitHubAPIError("projectItems pagination cursor was missing")
            after = cursor

    async def _field_option(self, item_id: str, field_id: str, field_name: str) -> str | None:
        query = """
        query($id: ID!, $fieldName: String!) { node(id: $id) { ... on ProjectV2Item {
          fieldValueByName(name: $fieldName) { ... on ProjectV2ItemFieldSingleSelectValue {
            optionId field { ... on ProjectV2SingleSelectField { id } }
          } }
        } } }
        """
        data = await self.client.graphql(query, {"id": item_id, "fieldName": field_name})
        node = data.get("node")
        value = node.get("fieldValueByName") if isinstance(node, dict) else None
        if value is None:
            return None
        if not isinstance(value, dict):
            raise GitHubAPIError("board field value was malformed")
        field = value.get("field")
        if not isinstance(field, dict) or field.get("id") != field_id:
            raise GitHubAPIError("board field identity changed")
        option = value.get("optionId")
        return option if isinstance(option, str) else None

    async def _ensure_project_item(self, effect: EffectIntent) -> AdapterOutcome:
        content_id = effect.args.get("content_id") or effect.parcel_id
        if not isinstance(content_id, str):
            return DefinitiveFailure("ENSURE_PROJECT_ITEM requires content_id")
        mutation = """
        mutation($input: AddProjectV2ItemByIdInput!) {
          addProjectV2ItemById(input: $input) { item { id } }
        }
        """
        data = await self.client.graphql(
            mutation, {"input": {"projectId": self.project_node_id, "contentId": content_id}}
        )
        result = data.get("addProjectV2ItemById")
        item = result.get("item") if isinstance(result, dict) else None
        item_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(item_id, str):
            return AmbiguousWrite("project item mutation omitted item id")
        return Ack(item_id)

    async def _issue_ref(
        self, effect: EffectIntent, *, require_parcel: bool = True
    ) -> IssueRef | None:
        issue_number = effect.args.get("issue_number")
        if not isinstance(issue_number, int):
            binding = await self._binding(effect)
            issue_number = binding.issue_number if binding is not None else None
        if not isinstance(issue_number, int) or (require_parcel and effect.parcel_id is None):
            return None
        return IssueRef(self.repository_node_id, issue_number, effect.parcel_id or "")

    async def _binding(self, effect: EffectIntent) -> ParcelBinding | None:
        if effect.parcel_id is None:
            return None
        static = self.parcel_bindings.get(effect.parcel_id)
        if static is not None or self.parcel_resolver is None:
            return static
        return await self.parcel_resolver(effect.parcel_id)

    async def _render_publication(self, effect: EffectIntent) -> str | None:
        if self.publication_renderer is not None:
            rendered = self.publication_renderer(effect)
            if inspect.isawaitable(rendered):
                rendered = await rendered
            if rendered is not None:
                return rendered
        if effect.kind != EffectKind.POST_COMMENT:
            return None
        template = effect.args.get("template")
        if not isinstance(template, str):
            return None
        details = " ".join(
            f"{key}={value}" for key, value in sorted(effect.args.items()) if key != "template"
        )
        suffix = f" ({details})" if details else ""
        return f"Status: {template.replace('-', ' ')}{suffix}."

    @staticmethod
    def _effect_marker(effect_id: str) -> str:
        return f"<!-- omnigent-factory effect={effect_id} -->"

    @staticmethod
    def _parse_time_us(value: object) -> int:
        if not isinstance(value, str):
            return 0
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1e6)
        except ValueError:
            return 0


#: Where a Ready report's factory lines begin (see ``service.directory._ready_text``).
_REPORT_FACTORY_LINES = "\n**PR:** "


#: Open bot threads carried to the owner's Needs you comment.
_MAX_FINDINGS = 20
_BADGE = re.compile(r"!\[\s*(P[0-3])\b[^\]]*\]\([^)]*\)", re.IGNORECASE)
_IMAGE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_SEVERITY = re.compile(r"\b(P[0-3])\b")
_TAG = re.compile(r"<[^>]{0,200}>")


def _finding_ref(thread: dict[str, Any]) -> FindingRef:
    """Path, severity badge (P0-P3, when parseable), one-line title and link of a thread,
    from its first comment (a Codex finding starts "**![P1 Badge](...) Title**")."""
    comments = thread.get("comments")
    nodes = comments.get("nodes") if isinstance(comments, dict) else None
    first = nodes[0] if isinstance(nodes, list) and nodes else None
    body = str(first.get("body") or "") if isinstance(first, dict) else ""
    url = str(first.get("url") or "") if isinstance(first, dict) else ""
    lines = [line for line in body.splitlines() if line.strip()]
    head = lines[0] if lines else ""
    badge = _BADGE.search(head) or _SEVERITY.search(_IMAGE.sub("", head)[:40])
    title = _TAG.sub("", _IMAGE.sub("", head)).replace("**", "").replace("__", "")
    title = " ".join(title.split())
    if not title and len(lines) > 1:
        title = " ".join(_TAG.sub("", lines[1]).replace("**", "").split())
    path = thread.get("path")
    return FindingRef(
        path=path[:200] if isinstance(path, str) else "",
        severity=badge.group(1).upper() if badge else "",
        title=title[:120],
        url=url[:300] if url.startswith("https://") else "",
    )


def _bot_thread_commit(comments: object, bot_login: str) -> str | None:
    """The commit of a review thread the review bot opened (None: not its thread)."""
    nodes = comments.get("nodes") if isinstance(comments, dict) else None
    first = nodes[0] if isinstance(nodes, list) and nodes else None
    author = first.get("author") if isinstance(first, dict) else None
    if not bot_login or not isinstance(author, dict) or author.get("login") != bot_login:
        return None
    commit = first.get("originalCommit") if isinstance(first, dict) else None
    oid = commit.get("oid") if isinstance(commit, dict) else None
    return oid if isinstance(oid, str) else None


def _status_options(schema: BoardSchema | None) -> dict[Stage, str]:
    """Stage -> Status option ID, from the persisted board schema or the live defaults."""
    if schema is None:
        return dict(STATUS_OPTION_IDS)
    options: dict[Stage, str] = {}
    for name, option_id in schema.status_options.items():
        try:
            options[Stage(name)] = option_id
        except ValueError:
            continue
    return options


_COMMENT_KINDS = frozenset(
    {
        EffectKind.POST_COMMENT,
        EffectKind.PUBLISH_CONTRACT,
        EffectKind.PUBLISH_TRIAGE,
        EffectKind.PUBLISH_REPORT,
    }
)


def _publication_matches(
    effect: EffectIntent, publication: ContractPublication | None, rendered: str
) -> bool:
    """The posted comment shows exactly the stored contract's section under its hash.

    ``rendered`` is the daemon's own rendering of the stored contract (trusted); the
    posted section must equal its section byte-for-byte and the parcel marker must carry
    the prefix of the effect's full hash.
    """
    full_hash = effect.args.get("full_hash")
    if publication is None or not isinstance(full_hash, str):
        return False
    expected = extract_contract_section(rendered)
    return (
        expected is not None
        and publication.contract_section == expected
        and publication.marker_hash is not None
        and len(publication.marker_hash) >= 12
        and full_hash.startswith(publication.marker_hash)
    )


#: A review bot's clean verdict names the commit it reviewed, e.g.
#: "**Reviewed commit:** `9929332e9c`".
_REVIEWED_COMMIT = re.compile(r"Reviewed commit:[*\s`]*([0-9a-f]{7,40})", re.IGNORECASE)
_SUCCEEDED = frozenset({"success", "neutral", "skipped"})


def _run_state(status: object, conclusion: object) -> str:
    if status != "completed":
        return "pending"
    return "ok" if conclusion in _SUCCEEDED else "failed"


def _status_state(state: object) -> str:
    if state == "success":
        return "ok"
    return "pending" if state == "pending" else "failed"


def _combine(states: list[str]) -> str:
    if "failed" in states:
        return "failed"
    return "pending" if "pending" in states else "ok"


def _checks_from(states: list[str]) -> ChecksState:
    combined = _combine(states)
    if combined == "failed":
        return ChecksState.FAILED
    return ChecksState.PENDING if combined == "pending" else ChecksState.GREEN


def _names(names: list[str]) -> str:
    """Distinct check names in first-seen order, "; "-separated (a name may contain
    commas), e.g. "api / Dependency audit; web / Typecheck, test, lint"."""
    return "; ".join(dict.fromkeys(names))[:200]


def _checks_summary(labels: list[str]) -> str:
    """E.g. "17 checks: 13 success, 4 skipped" (counts by conclusion)."""
    if not labels:
        return "no checks reported"
    counts: dict[str, int] = {}
    for label in labels:
        counts[label] = counts.get(label, 0) + 1
    parts = ", ".join(f"{n} {label}" for label, n in sorted(counts.items(), key=lambda x: -x[1]))
    return f"{len(labels)} checks: {parts}"
