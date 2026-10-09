"""Native links come with reads the factory already makes: the per-issue read (with
titles), the board reads (compact), and the board diff notices a link change."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from omnigent_factory.core.types import IssueLinks, LinkedIssue
from omnigent_factory.github.links import parse_links
from omnigent_factory.ports.github import BoardCard, IssueRef
from tests.github.test_adapter import PROJECT, REPO_NODE, STATUS_FIELD, adapter

OTHER_REPO = {"id": "R_other", "nameWithOwner": "o/x"}
SAME = {"id": REPO_NODE, "nameWithOwner": "SA-Ambulance/timesheets"}


def node(number: int, state: str = "OPEN", repo: dict[str, str] = SAME) -> dict[str, Any]:
    return {"number": number, "state": state, "title": f"T{number}", "repository": repo}


def links_payload(**over: Any) -> dict[str, Any]:
    payload = {
        "parent": node(821),
        "subIssuesSummary": {"total": 0, "completed": 0},
        "subIssues": {"totalCount": 0, "nodes": []},
        "blockedBy": {"totalCount": 2, "nodes": [node(823), node(822, "CLOSED")]},
        "blocking": {"totalCount": 1, "nodes": [node(825)]},
    }
    payload.update(over)
    return payload


def test_links_parse_and_an_incomplete_blocker_list_is_unreadable():
    links = parse_links(links_payload(), REPO_NODE)
    assert links is not None
    assert links.parent == LinkedIssue(821, True, "T821")
    assert [b.ref for b in links.open_blockers] == ["#823"]
    assert [b.ref for b in links.blocking] == ["#825"] and not links.epic
    other = parse_links(
        links_payload(blockedBy={"totalCount": 1, "nodes": [node(5, repo=OTHER_REPO)]}), REPO_NODE
    )
    assert other is not None and other.blocked_by[0].ref == "o/x#5"
    # More blockers than the read returned: never "no blocker" (auto-build fails closed).
    assert parse_links(links_payload(blockedBy={"totalCount": 60, "nodes": []}), REPO_NODE) is None
    assert parse_links({}, REPO_NODE) is None  # the read did not ask: unknown
    epic = parse_links(
        links_payload(
            subIssuesSummary={"total": 8, "completed": 1},
            subIssues={"totalCount": 8, "nodes": [node(822, "CLOSED"), node(823)]},
        ),
        REPO_NODE,
    )
    assert epic is not None and epic.epic and epic.sub_total == 8


@pytest.mark.asyncio
async def test_the_issue_read_carries_the_links_in_the_same_request():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200, json={"node_id": "I_1", "state": "open", "assignees": [], "title": "T"}
            )
        calls.append(request.content.decode())
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        **links_payload(),
                        "projectItems": {
                            "nodes": [
                                {
                                    "id": "PVTI_1",
                                    "project": {"id": PROJECT},
                                    "fieldValueByName": {
                                        "optionId": "3a7f779a",
                                        "field": {"id": STATUS_FIELD},
                                    },
                                }
                            ],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        },
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        snap = await adapter(http).issue_snapshot(IssueRef(REPO_NODE, 824, "I_1"))
    assert len(calls) == 1 and "blockedBy" in calls[0]
    assert snap.links is not None and [b.number for b in snap.links.blocked_by] == [823, 822]


@pytest.mark.asyncio
async def test_board_reads_carry_compact_links():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "blockedBy" in request.content.decode()
        content = {
            "__typename": "Issue",
            "id": "I_824",
            "number": 824,
            "title": "T",
            "state": "OPEN",
            "createdAt": "2026-10-01T00:00:00Z",
            "updatedAt": "2026-10-01T00:00:00Z",
            "repository": {"id": REPO_NODE},
            "assignees": {"totalCount": 0, "nodes": []},
            "labels": {"totalCount": 0, "nodes": []},
            **links_payload(),
        }
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "items": {
                            "nodes": [{"content": content}],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        a = adapter(http)
        [issue] = await a.board_issues()
        [card] = await a.board_cards()
    assert issue.links is not None and issue.links.parent is not None
    assert card.links == issue.links


def test_the_board_diff_digest_changes_when_a_blocker_closes_but_not_for_unlinked_cards():
    base = BoardCard("I_1", 1, True, "s", "b", (), (), "T", "2026-10-01T00:00:00Z")
    assert base.digest == BoardCard(**{**_values(base), "links": IssueLinks()}).digest
    assert base.digest == BoardCard(**{**_values(base), "links": None}).digest
    open_ = IssueLinks(blocked_by=(LinkedIssue(823, True),))
    closed = IssueLinks(blocked_by=(LinkedIssue(823, False),))
    digests = {
        BoardCard(**{**_values(base), "links": links}).digest
        for links in (open_, closed, IssueLinks(sub_total=1), None)
    }
    assert len(digests) == 4  # updatedAt alone would not move for any of these


def _values(card: BoardCard) -> dict[str, Any]:
    return {name: getattr(card, name) for name in card.__dataclass_fields__}


def test_setup_renders_an_epics_view_without_applying_it():
    from omnigent_factory.github.setup import render_app_manifest, render_project_migration

    view = render_project_migration()["create_epics_view"]
    create = view["create"]
    assert create["mutation"] == "createProjectV2View"
    assert create["input"]["name"] == "Epics" and create["input"]["layout"] == "TABLE_LAYOUT"
    assert "PVTF_lADOEanNes4BkJhbzhi7I-Q" in create["input"]["configuration"]["visibleFieldIds"]
    assert view["update"]["mutation"] == "updateProjectV2View"
    assert "sub-issues-progress" in view["update"]["input"]["filter"]
    events = render_app_manifest()["default_events"]
    assert "sub_issues" in events and "issue_dependencies" in events
