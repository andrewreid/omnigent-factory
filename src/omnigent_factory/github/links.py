"""GitHub-native issue links: read (parent, sub-issues, blocked-by, blocking) and write.

Reads are GraphQL fragments folded into reads the factory already makes (the per-issue
read and the board reads), so links cost no extra request. The per-issue read pages a
link list longer than its first read (an epic's sub-issues, most often) to its end with
follow-up requests, only for that list; the board reads never page. Any list that is not
read to its end is unreadable (the whole read's links are None), never a short list.
Writes only ever add: a
blocked-by link between two issues of the configured repository (``addBlockedBy``) and
GitHub's "Epic" issue type on an issue that has sub-issues (``updateIssueIssueType``),
never on an issue whose type the owner set to something else; for follow-ups
(``service.follow_ups``) a sub-issue link (``addSubIssue``) and the ``follow-up`` label.
Nothing is removed.

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
from urllib.parse import quote

from omnigent_factory.core.types import (
    LINK_TITLE_MAX,
    MAX_LINKED_ISSUES,
    IssueLinks,
    LinkedIssue,
)
from omnigent_factory.github.client import GitHubAPIError, GitHubClient, GitHubRejected

LOG = logging.getLogger(__name__)

_TARGET = "number state title repository { id nameWithOwner }"

_PAGE = "pageInfo { hasNextPage endCursor }"

#: Full link fields of one issue (the per-issue read: titles for the agent). A longer
#: list is read to its end by :func:`complete_links`.
ISSUE_LINK_FIELDS = f"""
  parent {{ {_TARGET} }}
  subIssuesSummary {{ total completed }}
  subIssues(first: {MAX_LINKED_ISSUES}) {{ totalCount nodes {{ {_TARGET} }} {_PAGE} }}
  blockedBy(first: {MAX_LINKED_ISSUES}) {{ totalCount nodes {{ {_TARGET} }} {_PAGE} }}
  blocking(first: {MAX_LINKED_ISSUES}) {{ totalCount nodes {{ {_TARGET} }} {_PAGE} }}
"""

#: The per-issue read's link lists :func:`complete_links` pages.
_PAGED = ("subIssues", "blockedBy", "blocking")
#: Links read per follow-up page, and follow-up pages per list at most (a list longer
#: than that stays unreadable).
LINK_PAGE_SIZE = 100
MAX_LINK_PAGES = 20

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


def _connection(value: object, repository_node_id: str) -> tuple[LinkedIssue, ...] | None:
    """The linked issues of one connection; None when unreadable, or when GitHub has
    more than the read returned (a short list is never taken for the whole list)."""
    if not isinstance(value, dict) or not isinstance(value.get("nodes"), list):
        return None
    found = [_linked(node, repository_node_id) for node in value["nodes"]]
    if any(item is None for item in found):
        return None
    total = value.get("totalCount")
    if not isinstance(total, int) or total > len(found):
        return None
    page = value.get("pageInfo")
    if isinstance(page, dict) and page.get("hasNextPage"):
        return None
    return tuple(item for item in found if item is not None)


def parse_links(content: Mapping[str, Any], repository_node_id: str) -> IssueLinks | None:
    """An issue's links from a read that asked for them; None when they are missing or
    any list (sub-issues, blockers, blocked) was not read in full (unreadable:
    auto-build and autopilot fail closed)."""
    if "blockedBy" not in content or "subIssuesSummary" not in content:
        return None
    blocked_by = _connection(content.get("blockedBy"), repository_node_id)
    blocking = _connection(content.get("blocking"), repository_node_id)
    if blocked_by is None or blocking is None:
        return None
    subs: tuple[LinkedIssue, ...] = ()
    if "subIssues" in content:
        found = _connection(content.get("subIssues"), repository_node_id)
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


def _truncated(value: object) -> bool:
    """A connection read with more behind it (``pageInfo.hasNextPage``)."""
    page = value.get("pageInfo") if isinstance(value, dict) else None
    return isinstance(page, dict) and bool(page.get("hasNextPage"))


def _node_key(node: object) -> object:
    if not isinstance(node, dict):
        return id(node)
    repo = node.get("repository")
    return (repo.get("id") if isinstance(repo, dict) else None, node.get("number"))


async def _rest_of(
    client: GitHubClient, issue_id: str, key: str, first: Mapping[str, Any]
) -> dict[str, Any]:
    """Connection ``key`` of issue ``issue_id`` read from where ``first`` stopped to its
    end (at most ``MAX_LINK_PAGES`` more pages). Returned with ``pageInfo`` still showing
    more when it could not finish, so :func:`parse_links` finds it unreadable."""
    query = f"""
    query($id: ID!, $after: String) {{ node(id: $id) {{ ... on Issue {{
      {key}(first: {LINK_PAGE_SIZE}, after: $after) {{
        totalCount nodes {{ {_TARGET} }} {_PAGE}
      }}
    }} }} }}
    """
    nodes = list(first.get("nodes") or [])
    total = first.get("totalCount")
    page = first.get("pageInfo")
    for _ in range(MAX_LINK_PAGES):
        cursor = page.get("endCursor") if isinstance(page, dict) else None
        if not isinstance(cursor, str):
            break
        data = await client.graphql(query, {"id": issue_id, "after": cursor})
        node = data.get("node")
        value = node.get(key) if isinstance(node, dict) else None
        if not isinstance(value, dict) or not isinstance(value.get("nodes"), list):
            raise GitHubAPIError(f"issue {key} page was unavailable")
        nodes.extend(value["nodes"])
        total = value.get("totalCount")
        page = value.get("pageInfo")
        if not _truncated(value):
            break
    # A link added or removed between pages can repeat one: each target counts once.
    unique = list({_node_key(n): n for n in nodes}.values())
    return {"totalCount": total, "nodes": unique, "pageInfo": page}


async def complete_links(
    client: GitHubClient, issue_id: str, content: Mapping[str, Any]
) -> Mapping[str, Any]:
    """``content`` (a per-issue read with ``ISSUE_LINK_FIELDS``) with each link list
    longer than its first read paged to its end. A list that fitted costs nothing more;
    a failed page raises ``GitHubAPIError`` (the read fails and is retried)."""
    longer = [key for key in _PAGED if _truncated(content.get(key))]
    if not longer:
        return content
    out = dict(content)
    for key in longer:
        out[key] = await _rest_of(client, issue_id, key, content[key])
        LOG.info(
            "issue links paged issue_id=%s list=%s read=%s",
            issue_id,
            key,
            len(out[key]["nodes"]),
        )
    return out


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
    #: The parent issue's number when it is in this repository (None: none or elsewhere).
    parent: int | None = None
    #: The issue has a parent (in any repository).
    has_parent: bool = False
    #: Its label names (the first 100).
    labels: frozenset[str] = frozenset()


async def read_target(
    client: GitHubClient, owner: str, name: str, number: int, repository_node_id: str
) -> LinkTarget | None:
    """The issue ``number`` of the configured repository (None: not found)."""
    query = """
    query($owner: String!, $name: String!, $number: Int!) {
      repository(owner: $owner, name: $name) { issue(number: $number) {
        id number repository { id } issueType { name }
        parent { number repository { id } }
        labels(first: 100) { nodes { name } }
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
    parent = issue.get("parent")
    parent_number = parent.get("number") if isinstance(parent, dict) else None
    parent_home = parent.get("repository") if isinstance(parent, dict) else None
    labels = issue.get("labels")
    label_nodes = labels.get("nodes") if isinstance(labels, dict) else None
    return LinkTarget(
        node_id=issue["id"],
        number=number,
        blocked_by=numbers,
        blocked_by_truncated=not isinstance(nodes, list)
        or not isinstance(total, int)
        or total > len(nodes),
        issue_type=str(kind["name"]) if isinstance(kind, dict) and kind.get("name") else None,
        parent=parent_number
        if isinstance(parent_number, int)
        and isinstance(parent_home, dict)
        and parent_home.get("id") == repository_node_id
        else None,
        has_parent=isinstance(parent, dict),
        labels=frozenset(
            str(n["name"])
            for n in (label_nodes if isinstance(label_nodes, list) else [])
            if isinstance(n, dict) and isinstance(n.get("name"), str)
        ),
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


async def add_sub_issue(client: GitHubClient, parent_id: str, sub_issue_id: str) -> None:
    """``sub_issue_id`` becomes a sub-issue of ``parent_id`` (raises ``GitHubAPIError``)."""
    mutation = """
    mutation($parent: ID!, $sub: ID!) {
      addSubIssue(input: {issueId: $parent, subIssueId: $sub}) { issue { id } }
    }
    """
    await client.graphql(mutation, {"parent": parent_id, "sub": sub_issue_id})


#: Colour of a repository label the factory creates (GitHub's default grey).
_LABEL_COLOR = "ededed"


async def ensure_label(client: GitHubClient, repository: str, name: str, description: str) -> bool:
    """Create the repository label ``name`` when it is absent; True when created."""
    try:
        await client.request("GET", f"/repos/{repository}/labels/{quote(name, safe='')}")
        return False
    except GitHubRejected as exc:
        if exc.status_code != 404:
            raise
    await client.request(
        "POST",
        f"/repos/{repository}/labels",
        json_body={"name": name, "color": _LABEL_COLOR, "description": description},
        expected=frozenset({201}),
    )
    return True


async def add_label(client: GitHubClient, repository: str, number: int, name: str) -> None:
    """Add the label ``name`` to issue ``number`` (keeps its other labels)."""
    await client.request(
        "POST",
        f"/repos/{repository}/issues/{number}/labels",
        json_body={"labels": [name]},
        expected=frozenset({200}),
    )


async def remove_label(client: GitHubClient, repository: str, number: int, name: str) -> None:
    """Remove the label ``name`` from issue ``number``; a label it does not carry (404)
    is success."""
    try:
        await client.request(
            "DELETE",
            f"/repos/{repository}/issues/{number}/labels/{quote(name, safe='')}",
            expected=frozenset({200, 204}),
        )
    except GitHubRejected as exc:
        if exc.status_code != 404:
            raise


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
    """Human-gate sub-issues of ``epic`` found by their marker: gate key -> issue number.

    Every sub-issue is read (paged): a gate past the first page that went unseen would be
    created again. Raises ``GitHubAPIError`` when the list cannot be read to its end."""
    query = """
    query($owner: String!, $name: String!, $number: Int!, $after: String) {
      repository(owner: $owner, name: $name) { issue(number: $number) {
        subIssues(first: 100, after: $after) {
          nodes { number body } pageInfo { hasNextPage endCursor }
        }
      } }
    }
    """
    nodes: list[object] = []
    after: str | None = None
    for _ in range(MAX_LINK_PAGES + 1):
        data = await client.graphql(
            query, {"owner": owner, "name": name, "number": epic, "after": after}
        )
        repo = data.get("repository")
        issue = repo.get("issue") if isinstance(repo, dict) else None
        subs = issue.get("subIssues") if isinstance(issue, dict) else None
        page_nodes = subs.get("nodes") if isinstance(subs, dict) else None
        if not isinstance(page_nodes, list):
            raise GitHubAPIError("epic sub-issues were unavailable")
        nodes.extend(page_nodes)
        page = subs.get("pageInfo") if isinstance(subs, dict) else None
        if not isinstance(page, dict) or not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if not isinstance(cursor, str):
            raise GitHubAPIError("epic sub-issues pagination cursor was missing")
        after = cursor
    else:
        raise GitHubAPIError("epic has more sub-issues than one gate lookup reads")
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
    "add_label",
    "add_sub_issue",
    "complete_links",
    "create_sub_issue",
    "ensure_label",
    "epic_gate_issues",
    "epic_issue_type",
    "gate_marker",
    "parse_links",
    "read_target",
    "set_issue_type",
    "user_node_id",
]
