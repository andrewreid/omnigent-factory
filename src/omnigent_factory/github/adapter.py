"""Concrete GitHub reader/effect adapter using REST and GraphQL."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
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
    ContractPublication,
    IssueRef,
    PullRequestEvidence,
)

_CONTRACT_FENCE = re.compile(r"```parcel-contract\s*\n(?P<body>.*?)\n```", re.DOTALL)
_CLOSES = re.compile(r"\b(?:close[sd]?|fixe[sd]?|resolve[sd]?)\s+#\d+\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ParcelBinding:
    """GitHub resource IDs resolved by ingest/reconciliation for one parcel."""

    issue_number: int
    project_item_id: str | None = None


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
        publication_renderer: Callable[[EffectIntent], str | None] | None = None,
        independent_reviewer_ids: frozenset[int] = frozenset(),
        owner_ids: frozenset[int] = frozenset(),
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
        self.publication_renderer = publication_renderer
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
                name = value.get("name")
                if not isinstance(name, str):
                    return None, True
                try:
                    return Stage(name), True
                except (TypeError, ValueError):
                    return None, True
            page = items.get("pageInfo")
            if not isinstance(page, dict) or not page.get("hasNextPage"):
                return None, False
            after_value = page.get("endCursor")
            if not isinstance(after_value, str):
                raise GitHubAPIError("projectItems pagination cursor was missing")
            after = after_value

    async def pull_request(
        self, repo_id: str, pr_number: int
    ) -> PullRequestEvidence | RetryableReadFailure:
        if repo_id != self.repository_node_id:
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
            author = pr.get("user")
            return PullRequestEvidence(
                pr_number=pr_number,
                head_sha=head_sha,
                open=pr.get("state") == "open",
                merged=bool(pr.get("merged")),
                bot_authored=isinstance(author, dict) and author.get("id") == self.bot_user_id,
                parcel_branch=branch.startswith(self.parcel_branch_prefix),
                closes_issue=bool(_CLOSES.search(str(pr.get("body", "")))),
                checks=checks,
                review_accepted=review_accepted,
                findings_dispositioned=findings_dispositioned,
            )
        except RateLimited as exc:
            return RetryableReadFailure(str(exc), exc.retry_after_us)
        except GitHubAPIError as exc:
            return RetryableReadFailure(str(exc))

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
                match = _CONTRACT_FENCE.search(body)
                return ContractPublication(
                    comment_id=str(comment.get("id", "")),
                    author_is_bot=True,
                    canonical=None if match is None else match.group("body"),
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
                evidence = await self.pull_request(self.repository_node_id, number)
                if isinstance(evidence, RetryableReadFailure):
                    return evidence
                detail = asdict(evidence)
                detail["checks"] = evidence.checks.value
                return Ack(str(number), detail)
            if effect.kind == EffectKind.RECONCILE_PARCEL:
                issue_number = effect.args.get("issue_number")
                binding = self._binding(effect)
                if issue_number is None and binding is not None:
                    issue_number = binding.issue_number
                parcel_id = effect.parcel_id
                if not isinstance(issue_number, int) or parcel_id is None:
                    return DefinitiveFailure("RECONCILE_PARCEL requires parcel and issue number")
                snapshot = await self.issue_snapshot(
                    IssueRef(self.repository_node_id, issue_number, parcel_id)
                )
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
        issue_number = effect.args.get("issue_number")
        text = effect.args.get("body")
        binding = self._binding(effect)
        if issue_number is None and binding is not None:
            issue_number = binding.issue_number
        if text is None:
            text = self._render_publication(effect)
        if not isinstance(issue_number, int) or not isinstance(text, str):
            return DefinitiveFailure("comment effect could not resolve issue or rendered body")
        ref = IssueRef(self.repository_node_id, issue_number, effect.parcel_id or "")
        adopted = await self._find_marked_comment(ref, effect.effect_id)
        if isinstance(adopted, RetryableReadFailure):
            return adopted
        if adopted is not None:
            return Ack(adopted, {"adopted": True})
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
        return Ack(str(comment_id))

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
        binding = self._binding(effect)
        if item_id is None and binding is not None:
            item_id = binding.project_item_id
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
        observed = await self._field_option(str(item_id), str(field_id), field_name)
        if observed != option_id:
            return AmbiguousWrite("board write did not read back at the requested option id")
        return Ack(str(item_id), {"option_id": str(option_id)})

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

    def _binding(self, effect: EffectIntent) -> ParcelBinding | None:
        if effect.parcel_id is None:
            return None
        return self.parcel_bindings.get(effect.parcel_id)

    def _render_publication(self, effect: EffectIntent) -> str | None:
        if self.publication_renderer is not None:
            rendered = self.publication_renderer(effect)
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
