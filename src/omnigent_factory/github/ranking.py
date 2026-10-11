"""GitHub reads and writes of the triage ranking (``service.ranking``).

Board cards with their Rank and Priority values, the two field writes, the marked priority
comment and the project status update. Every write is idempotent or adopted by its
marker, so a retry after a crash or a lost response never duplicates it:

* a field write sets an absolute value (setting it twice is the same write);
* a comment carries the write's marker and is adopted from the issue's comments;
* a status update carries the marker and is adopted from the project's recent updates.

Columns are read by Status option ID; Rank by its configured field ID; Priority by the
same "Priority" field the triage publication fills.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Any

from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.github.client import GitHubAPIError, GitHubRejected, RateLimited
from omnigent_factory.github.links import BOARD_LINK_FIELDS, parse_links
from omnigent_factory.ports.github import IssueRef, RankingCard, stage_for_option

PRIORITY_FIELD = "Priority"
PRIORITIES = ("P0", "P1", "P2", "P3")
#: Board pages (100 items each) one read takes at most.
_MAX_PAGES = 20
#: Recent project status updates searched for a marker before creating one.
_STATUS_UPDATES_SCANNED = 20


class RankingWriteError(RuntimeError):
    """A ranking write failed. ``retry``: transient (rate limit, lost response, 5xx)."""

    def __init__(self, reason: str, *, retry: bool) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retry = retry


def marker_key(write_id: str) -> str:
    return f"ranking-{write_id}"


def marker(write_id: str) -> str:
    """The hidden marker a ranking comment or status update carries (adoption key)."""
    return GitHubAPIAdapter._effect_marker(marker_key(write_id))


class RankingBoard:
    def __init__(self, adapter: GitHubAPIAdapter) -> None:
        self.adapter = adapter

    @property
    def project_node_id(self) -> str:
        return self.adapter.project_node_id

    async def cards(
        self, rank_field_id: str, parallel_field_id: str = ""
    ) -> list[RankingCard] | RetryableReadFailure:
        """Open repository issues on the board with Status, Rank and Priority values (and
        an epic's "Parallel" number when ``parallel_field_id`` is set)."""
        query = """
        query($id: ID!, $after: String) { node(id: $id) { ... on ProjectV2 {
          items(first: 100, after: $after) {
            nodes {
              id
              status: fieldValueByName(name: "Status") {
                ... on ProjectV2ItemFieldSingleSelectValue { optionId }
              }
              fieldValues(first: 30) { nodes {
                ... on ProjectV2ItemFieldNumberValue {
                  number field { ... on ProjectV2Field { id } }
                }
                ... on ProjectV2ItemFieldSingleSelectValue {
                  name field { ... on ProjectV2SingleSelectField { id name } }
                }
              } }
              content { __typename ... on Issue {
                id number title state createdAt repository { id }
                __LINKS__
              } }
            }
            pageInfo { hasNextPage endCursor }
          }
        } } }
        """.replace("__LINKS__", BOARD_LINK_FIELDS)
        cards: list[RankingCard] = []
        after: str | None = None
        try:
            for _ in range(_MAX_PAGES):
                data = await self.adapter.client.graphql(
                    query, {"id": self.project_node_id, "after": after}
                )
                node = data.get("node")
                items = node.get("items") if isinstance(node, dict) else None
                nodes = items.get("nodes") if isinstance(items, dict) else None
                if not isinstance(items, dict) or not isinstance(nodes, list):
                    return RetryableReadFailure("project items were unavailable")
                for item in nodes:
                    card = self._card(item, rank_field_id, parallel_field_id)
                    if card is not None:
                        cards.append(card)
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
        return cards

    def _card(
        self, item: object, rank_field_id: str, parallel_field_id: str = ""
    ) -> RankingCard | None:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            return None
        content = item.get("content")
        if not isinstance(content, dict) or content.get("__typename") != "Issue":
            return None
        repository = content.get("repository")
        if (
            not isinstance(repository, dict)
            or repository.get("id") != self.adapter.repository_node_id
            or content.get("state") != "OPEN"
        ):
            return None
        node_id, number = content.get("id"), content.get("number")
        if not isinstance(node_id, str) or not isinstance(number, int) or isinstance(number, bool):
            return None
        status = item.get("status")
        stage = stage_for_option(
            status.get("optionId") if isinstance(status, dict) else None,
            self.adapter.status_options,
        )
        rank: float | None = None
        parallel: int | None = None
        priority: str | None = None
        values = item.get("fieldValues")
        for value in values.get("nodes", []) if isinstance(values, dict) else []:
            if not isinstance(value, dict):
                continue
            field = value.get("field")
            if not isinstance(field, dict):
                continue
            number_value = value.get("number")
            if field.get("id") == rank_field_id and isinstance(number_value, int | float):
                rank = float(number_value)
            elif (
                parallel_field_id
                and field.get("id") == parallel_field_id
                and isinstance(number_value, int | float)
                and not isinstance(number_value, bool)
            ):
                parallel = int(number_value)
            elif field.get("name") == PRIORITY_FIELD and value.get("name") in PRIORITIES:
                priority = str(value["name"])
        return RankingCard(
            node_id=node_id,
            item_id=str(item["id"]),
            number=number,
            title=str(content.get("title") or ""),
            stage=stage,
            created_at_us=self.adapter._parse_time_us(content.get("createdAt")),
            rank=rank,
            priority=priority,
            links=parse_links(content, self.adapter.repository_node_id),
            parallel=parallel,
        )

    # ------------------------------------------------------------------ writes

    async def set_rank(self, item_id: str, rank_field_id: str, rank: int) -> None:
        await self._write(self._set_number(item_id, rank_field_id, rank))

    async def _set_number(self, item_id: str, field_id: str, value: int) -> None:
        mutation = """
        mutation($input: UpdateProjectV2ItemFieldValueInput!) {
          updateProjectV2ItemFieldValue(input: $input) { projectV2Item { id } }
        }
        """
        await self.adapter.client.graphql(
            mutation,
            {
                "input": {
                    "projectId": self.project_node_id,
                    "itemId": item_id,
                    "fieldId": field_id,
                    "value": {"number": float(value)},
                }
            },
        )

    async def set_priority(self, item_id: str, priority: str) -> None:
        async def write() -> None:
            fields = await self.adapter._single_select_fields((PRIORITY_FIELD,))
            field = fields.get(PRIORITY_FIELD)
            option = field[1].get(priority) if field is not None else None
            if field is None or option is None:
                raise RankingWriteError(f"no {PRIORITY_FIELD} option {priority}", retry=False)
            await self.adapter._set_single_select(item_id, field[0], option)

        await self._write(write())

    async def post_comment(
        self, issue_number: int, issue_node_id: str, write_id: str, text: str
    ) -> str:
        """Post ``text`` once on the issue, or adopt the bot comment carrying the marker."""
        ref = IssueRef(self.adapter.repository_node_id, issue_number, issue_node_id)
        found = await self.adapter._marked_comment(ref, marker_key(write_id))
        if isinstance(found, RetryableReadFailure):
            raise RankingWriteError(found.reason, retry=True)
        if found is not None:
            return found[0]

        async def post() -> str:
            response = await self.adapter.client.request(
                "POST",
                f"/repos/{self.adapter.repository}/issues/{issue_number}/comments",
                json_body={"body": f"{text}\n\n{marker(write_id)}"},
                expected=frozenset({201}),
            )
            data: Any = response.json()
            return str(data.get("id")) if isinstance(data, dict) else ""

        return await self._write(post())

    async def post_status_update(self, write_id: str, text: str) -> str:
        """Create one project status update, or adopt the recent one carrying the marker."""
        tag = marker(write_id)
        query = """
        query($id: ID!, $n: Int!) { node(id: $id) { ... on ProjectV2 {
          statusUpdates(last: $n) { nodes { id body } }
        } } }
        """

        async def find() -> str | None:
            data = await self.adapter.client.graphql(
                query, {"id": self.project_node_id, "n": _STATUS_UPDATES_SCANNED}
            )
            node = data.get("node")
            updates = node.get("statusUpdates") if isinstance(node, dict) else None
            rows = updates.get("nodes") if isinstance(updates, dict) else None
            if not isinstance(rows, list):
                raise GitHubAPIError("project status updates were unavailable")
            for row in rows:
                if isinstance(row, dict) and tag in str(row.get("body") or ""):
                    return str(row.get("id"))
            return None

        found = await self._write(find())
        if found is not None:
            return found
        mutation = """
        mutation($input: CreateProjectV2StatusUpdateInput!) {
          createProjectV2StatusUpdate(input: $input) { statusUpdate { id } }
        }
        """

        async def create() -> str:
            data = await self.adapter.client.graphql(
                mutation,
                {"input": {"projectId": self.project_node_id, "body": f"{text}\n\n{tag}"}},
            )
            created = data.get("createProjectV2StatusUpdate")
            update = created.get("statusUpdate") if isinstance(created, Mapping) else None
            return str(update.get("id")) if isinstance(update, Mapping) else ""

        return await self._write(create())

    @staticmethod
    async def _write[T](operation: Awaitable[T]) -> T:
        try:
            result = await operation
        except RankingWriteError:
            raise
        except RateLimited as exc:
            raise RankingWriteError(str(exc), retry=True) from exc
        except GitHubRejected as exc:
            raise RankingWriteError(str(exc), retry=exc.status_code >= 500) from exc
        except GitHubAPIError as exc:
            # GraphQL errors (e.g. a permission the App lacks), lost responses: retried a
            # bounded number of times by the caller, then recorded as failed.
            raise RankingWriteError(str(exc), retry=True) from exc
        return result
