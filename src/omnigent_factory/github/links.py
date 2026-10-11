"""GitHub-native issue links: read (parent, sub-issues, blocked-by, blocking) and write.

Reads are GraphQL fragments folded into reads the factory already makes (the per-issue
read and the board reads), so links cost no extra request. Writes only ever add: a
blocked-by link between two issues of the configured repository (``addBlockedBy``) and
GitHub's "Epic" issue type on an issue that has sub-issues (``updateIssueIssueType``),
never on an issue whose type the owner set to something else. Nothing is removed.

Schema checked read-only (``gh api graphql`` introspection, 2026-10-09): ``Issue.parent``,
``subIssues``, ``subIssuesSummary {total completed}``, ``blockedBy``, ``blocking``,
``issueType``; mutations ``addBlockedBy(issueId, blockingIssueId)``, ``addSubIssue``,
``updateIssueIssueType(issueId, issueTypeId)``; ``Organization.issueTypes``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from omnigent_factory.core.types import (
    LINK_TITLE_MAX,
    MAX_LINKED_ISSUES,
    IssueLinks,
    LinkedIssue,
)
from omnigent_factory.github.client import GitHubAPIError, GitHubClient

LOG = logging.getLogger(__name__)

_TARGET = "number state title repository { id nameWithOwner }"

#: Full link fields of one issue (the per-issue read: titles for the agent).
ISSUE_LINK_FIELDS = f"""
  parent {{ {_TARGET} }}
  subIssuesSummary {{ total completed }}
  subIssues(first: {MAX_LINKED_ISSUES}) {{ totalCount nodes {{ {_TARGET} }} }}
  blockedBy(first: {MAX_LINKED_ISSUES}) {{ totalCount nodes {{ {_TARGET} }} }}
  blocking(first: {MAX_LINKED_ISSUES}) {{ totalCount nodes {{ {_TARGET} }} }}
"""

#: Board links per card (no titles, no sub-issue list: a child names its parent).
BOARD_LINKS_READ = 20
BOARD_LINK_FIELDS = f"""
  parent {{ number state repository {{ id nameWithOwner }} }}
  subIssuesSummary {{ total completed }}
  blockedBy(first: {BOARD_LINKS_READ}) {{
    totalCount nodes {{ number state repository {{ id nameWithOwner }} }}
  }}
  blocking(first: {BOARD_LINKS_READ}) {{
    totalCount nodes {{ number state repository {{ id nameWithOwner }} }}
  }}
"""


def _linked(node: object, repository_node_id: str) -> LinkedIssue | None:
    if not isinstance(node, dict):
        return None
    number = node.get("number")
    if not isinstance(number, int) or isinstance(number, bool):
        return None
    repo = node.get("repository")
    same = isinstance(repo, dict) and repo.get("id") == repository_node_id
    name = repo.get("nameWithOwner") if isinstance(repo, dict) else None
    title = " ".join(str(node.get("title") or "").split())
    return LinkedIssue(
        number=number,
        open=node.get("state") == "OPEN",
        title=title[:LINK_TITLE_MAX],
        repo="" if same else str(name or "?"),
    )


def _connection(
    value: object, repository_node_id: str, *, complete: bool
) -> tuple[LinkedIssue, ...] | None:
    """The linked issues of one connection; None when unreadable, or (``complete``)
    when GitHub has more than one read returned."""
    if not isinstance(value, dict) or not isinstance(value.get("nodes"), list):
        return None
    found = [_linked(node, repository_node_id) for node in value["nodes"]]
    if any(item is None for item in found):
        return None
    total = value.get("totalCount")
    if complete and (not isinstance(total, int) or total > len(found)):
        return None
    return tuple(item for item in found if item is not None)


def parse_links(content: Mapping[str, Any], repository_node_id: str) -> IssueLinks | None:
    """An issue's links from a read that asked for them; None when they are missing or
    every blocker could not be seen (unreadable: auto-build fails closed)."""
    if "blockedBy" not in content or "subIssuesSummary" not in content:
        return None
    blocked_by = _connection(content.get("blockedBy"), repository_node_id, complete=True)
    blocking = _connection(content.get("blocking"), repository_node_id, complete=False)
    if blocked_by is None or blocking is None:
        return None
    subs: tuple[LinkedIssue, ...] = ()
    if "subIssues" in content:
        found = _connection(content.get("subIssues"), repository_node_id, complete=False)
        if found is None:
            return None
        subs = found
    summary = content.get("subIssuesSummary")
    total = summary.get("total") if isinstance(summary, dict) else None
    completed = summary.get("completed") if isinstance(summary, dict) else None
    if not isinstance(total, int) or not isinstance(completed, int):
        return None
    parent_node = content.get("parent")
    parent = _linked(parent_node, repository_node_id) if parent_node is not None else None
    if parent_node is not None and parent is None:
        return None
    return IssueLinks(
        parent=parent,
        sub_issues=subs,
        sub_total=total,
        sub_completed=completed,
        blocked_by=blocked_by,
        blocking=blocking,
    )


# ------------------------------------------------------------------ writes


@dataclass(frozen=True, slots=True)
class LinkTarget:
    """One issue of the configured repository: node ID, current blockers and type."""

    node_id: str
    number: int
    blocked_by: frozenset[int]
    #: Blockers of this issue were more than one read returned (cannot tell "exists").
    blocked_by_truncated: bool
    issue_type: str | None


async def read_target(
    client: GitHubClient, owner: str, name: str, number: int, repository_node_id: str
) -> LinkTarget | None:
    """The issue ``number`` of the configured repository (None: not found)."""
    query = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) { issue(number: $number) {
        id number repository { id } issueType { name }
        blockedBy(first: 100) { totalCount nodes { number repository { id } } }
      } }
    }
    """
    data = await client.graphql(query, {"owner": owner, "name": name, "number": number})
    repo = data.get("repository")
    issue = repo.get("issue") if isinstance(repo, dict) else None
    if not isinstance(issue, dict) or not isinstance(issue.get("id"), str):
        return None
    home = issue.get("repository")
    if not isinstance(home, dict) or home.get("id") != repository_node_id:
        return None
    connection = issue.get("blockedBy")
    nodes = connection.get("nodes") if isinstance(connection, dict) else None
    numbers = frozenset(
        int(n["number"])
        for n in (nodes if isinstance(nodes, list) else [])
        if isinstance(n, dict)
        and isinstance(n.get("number"), int)
        and isinstance(n.get("repository"), dict)
        and n["repository"].get("id") == repository_node_id
    )
    total = connection.get("totalCount") if isinstance(connection, dict) else None
    kind = issue.get("issueType")
    return LinkTarget(
        node_id=issue["id"],
        number=number,
        blocked_by=numbers,
        blocked_by_truncated=not isinstance(nodes, list)
        or not isinstance(total, int)
        or total > len(nodes),
        issue_type=str(kind["name"]) if isinstance(kind, dict) and kind.get("name") else None,
    )


async def add_blocked_by(client: GitHubClient, issue_id: str, blocking_issue_id: str) -> None:
    """``issue_id`` is blocked by ``blocking_issue_id`` (raises ``GitHubAPIError``)."""
    mutation = """
    mutation($issue: ID!, $blocking: ID!) {
      addBlockedBy(input: {issueId: $issue, blockingIssueId: $blocking}) {
        issue { id } blockingIssue { id }
      }
    }
    """
    await client.graphql(mutation, {"issue": issue_id, "blocking": blocking_issue_id})


async def epic_issue_type(client: GitHubClient, owner: str, name: str) -> str | None:
    """The node ID of the repository's enabled "Epic" issue type (None: none)."""
    query = """
    query($owner: String!, $name: String!) { repository(owner: $owner, name: $name) {
      issueTypes(first: 50) { nodes { id name } }
    } }
    """
    try:
        data = await client.graphql(query, {"owner": owner, "name": name})
    except GitHubAPIError:
        return None
    repo = data.get("repository")
    types = repo.get("issueTypes") if isinstance(repo, dict) else None
    nodes = types.get("nodes") if isinstance(types, dict) else None
    for node in nodes if isinstance(nodes, list) else []:
        if isinstance(node, dict) and str(node.get("name") or "").lower() == "epic":
            value = node.get("id")
            return value if isinstance(value, str) else None
    return None


async def set_issue_type(client: GitHubClient, issue_id: str, type_id: str) -> None:
    mutation = """
    mutation($issue: ID!, $type: ID!) {
      updateIssueIssueType(input: {issueId: $issue, issueTypeId: $type}) { issue { id } }
    }
    """
    await client.graphql(mutation, {"issue": issue_id, "type": type_id})


#: The hidden marker of a human-gate sub-issue the factory created for an epic plan: a
#: restart finds the issue by it among the epic's sub-issues instead of creating another.
_GATE_MARKER = re.compile(r"<!-- factory-gate epic=(\d+) key=([a-z0-9][a-z0-9-]{0,39}) -->")


def gate_marker(epic: int, key: str) -> str:
    return f"<!-- factory-gate epic={epic} key={key} -->"


async def epic_gate_issues(
    client: GitHubClient, owner: str, name: str, epic: int
) -> dict[str, int]:
    """Human-gate sub-issues of ``epic`` found by their marker: gate key -> issue number."""
    query = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) { issue(number: $number) {
        subIssues(first: 100) { nodes { number body } }
      } }
    }
    """
    data = await client.graphql(query, {"owner": owner, "name": name, "number": epic})
    repo = data.get("repository")
    issue = repo.get("issue") if isinstance(repo, dict) else None
    subs = issue.get("subIssues") if isinstance(issue, dict) else None
    nodes = subs.get("nodes") if isinstance(subs, dict) else None
    if not isinstance(nodes, list):
        raise GitHubAPIError("epic sub-issues were unavailable")
    found: dict[str, int] = {}
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("number"), int):
            continue
        for epic_ref, key in _GATE_MARKER.findall(str(node.get("body") or "")):
            if int(epic_ref) == epic:
                found.setdefault(key, int(node["number"]))
    return found


async def create_sub_issue(
    client: GitHubClient,
    *,
    repository_node_id: str,
    parent_node_id: str,
    title: str,
    body: str,
    assignee_node_id: str,
) -> int:
    """Create an issue as a sub-issue of ``parent_node_id``, assigned in the same call
    (one mutation: no half-made gate). Returns its number."""
    mutation = """
    mutation($input: CreateIssueInput!) { createIssue(input: $input) { issue { number } } }
    """
    data = await client.graphql(
        mutation,
        {
            "input": {
                "repositoryId": repository_node_id,
                "title": title,
                "body": body,
                "parentIssueId": parent_node_id,
                "assigneeIds": [assignee_node_id],
            }
        },
    )
    created = data.get("createIssue")
    issue = created.get("issue") if isinstance(created, dict) else None
    number = issue.get("number") if isinstance(issue, dict) else None
    if not isinstance(number, int):
        raise GitHubAPIError("createIssue returned no issue number")
    return number


async def user_node_id(client: GitHubClient, user_id: int) -> str:
    """The GraphQL node ID of the GitHub user with numeric ``user_id``."""
    user = await client.get_json(f"/user/{user_id}")
    node = user.get("node_id") if isinstance(user, dict) else None
    if not isinstance(node, str) or not node:
        raise GitHubAPIError(f"user {user_id} has no node id")
    return node


__all__ = [
    "BOARD_LINK_FIELDS",
    "ISSUE_LINK_FIELDS",
    "LinkTarget",
    "add_blocked_by",
    "create_sub_issue",
    "epic_gate_issues",
    "epic_issue_type",
    "gate_marker",
    "parse_links",
    "read_target",
    "set_issue_type",
    "user_node_id",
]
