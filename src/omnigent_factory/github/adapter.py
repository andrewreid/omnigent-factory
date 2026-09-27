"""Concrete GitHub reader/effect adapter using REST and GraphQL."""

from __future__ import annotations

import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
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
from omnigent_factory.core.events import ChecksState
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
    ContractPublication,
    IssueRef,
    PullRequestEvidence,
    stage_for_option,
)

LOG = logging.getLogger(__name__)

#: Project fields a triage result may fill (design §B: only while the owner left them unset).
TRIAGE_FIELDS = ("Priority", "Size")


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
    ) -> None:
        self.client = client
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
        if independent_reviewer_ids & (owner_ids | {bot_user_id}):
            raise ValueError("independent reviewers cannot include owners or the factory bot")
        self.independent_reviewer_ids = independent_reviewer_ids
        self.owner_ids = owner_ids

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
            stage, in_project = await self._project_stage(ref.parcel_id)
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
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

    async def _project_stage(self, parcel_id: str) -> tuple[Stage | None, bool]:
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
                value = item.get("fieldValueByName")
                if value is None:
                    return None, True
                if not isinstance(value, dict):
                    raise GitHubAPIError("Status field value was malformed")
                field = value.get("field")
                if not isinstance(field, dict) or field.get("id") != self.status_field_node_id:
                    raise GitHubAPIError("Status field identity changed")
                # Read by option ID only: a renamed option must not change the stage.
                return stage_for_option(value.get("optionId"), self.status_options), True
            page = items.get("pageInfo")
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                return None, False
            after_value = page.get("endCursor")
            if not isinstance(after_value, str):
                raise GitHubAPIError("projectItems pagination cursor was missing")
            after = after_value

    async def pull_request(
        self, ref: IssueRef, pr_number: int
    ) -> PullRequestEvidence | RetryableReadFailure:
        if ref.repo_id != self.repository_node_id:
            return RetryableReadFailure("pull request repository identity mismatch")
        try:
            pr: Any = await self.client.get_json(f"/repos/{self.repository}/pulls/{pr_number}")
            if not isinstance(pr, dict):
                raise GitHubAPIError("pull request response was malformed")
            head = pr.get("head")
            head_sha = head.get("sha") if isinstance(head, dict) else None
            branch = head.get("ref") if isinstance(head, dict) else None
            if not isinstance(head_sha, str) or not isinstance(branch, str):
                raise GitHubAPIError("pull request head was malformed")
            checks = await self._checks_state(head_sha)
            reviews = await self.client.paginate(
                f"/repos/{self.repository}/pulls/{pr_number}/reviews?per_page=100"
            )
            review_accepted = self._review_accepted(reviews, head_sha)
            comments = await self.client.paginate(
                f"/repos/{self.repository}/issues/{pr_number}/comments?per_page=100"
            )
            disposition_marker = f"<!-- factory-findings-dispositioned head={head_sha} -->"
            findings_dispositioned = any(
                isinstance(comment, dict)
                and isinstance(comment.get("user"), dict)
                and comment["user"].get("id") == self.bot_user_id
                and disposition_marker in str(comment.get("body", ""))
                for comment in comments
            )
            closes_issue = await self._closes_issue(pr_number, ref.parcel_id)
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
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

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

    async def _checks_state(self, head_sha: str) -> ChecksState:
        seen: dict[tuple[str, int], str] = {}
        page = 1
        while True:
            result: Any = await self.client.get_json(
                f"/repos/{self.repository}/commits/{head_sha}/check-runs?per_page=100&page={page}"
            )
            runs = result.get("check_runs") if isinstance(result, dict) else None
            if not isinstance(runs, list):
                raise GitHubAPIError("check-runs response was malformed")
            for run in runs:
                if not isinstance(run, dict):
                    continue
                app = run.get("app")
                app_id = app.get("id") if isinstance(app, dict) else None
                name = run.get("name")
                if isinstance(name, str) and isinstance(app_id, int):
                    seen[(name, app_id)] = str(run.get("conclusion") or run.get("status"))
            if len(runs) < 100:
                break
            page += 1
        missing = self.required_checks - seen.keys()
        if missing:
            return ChecksState.PENDING
        conclusions = {seen[key] for key in self.required_checks}
        if conclusions == {"success"}:
            return ChecksState.GREEN
        if conclusions & {"queued", "in_progress", "pending", "requested", "waiting"}:
            return ChecksState.PENDING
        return ChecksState.FAILED

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
            if effect.kind in {EffectKind.MOVE_CARD, EffectKind.SET_BOT}:
                return await self._write_board(effect)
            if effect.kind == EffectKind.ENSURE_PROJECT_ITEM:
                return await self._ensure_project_item(effect)
            if effect.kind == EffectKind.FETCH_PR_EVIDENCE:
                number = effect.args.get("pr_number")
                if not isinstance(number, int):
                    return DefinitiveFailure("FETCH_PR_EVIDENCE requires pr_number")
                ref = await self._issue_ref(effect)
                if ref is None:
                    return DefinitiveFailure("FETCH_PR_EVIDENCE requires the parcel issue")
                evidence = await self.pull_request(ref, number)
                if isinstance(evidence, RetryableReadFailure):
                    return evidence
                detail = asdict(evidence)
                detail["checks"] = evidence.checks.value
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
        if effect.kind != EffectKind.PUBLISH_TRIAGE or self.triage_fields is None:
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
        marker = self._effect_marker(effect_id)
        try:
            comments = await self.client.paginate(
                f"/repos/{self.repository}/issues/{ref.issue_number}/comments?per_page=100"
            )
            matches = [
                str(comment.get("id"))
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
        return f"Factory: {template.replace('-', ' ')}{suffix}."

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
