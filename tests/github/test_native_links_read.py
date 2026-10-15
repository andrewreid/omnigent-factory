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
            subIssuesSummary={"total": 2, "completed": 1},
            subIssues={"totalCount": 2, "nodes": [node(822, "CLOSED"), node(823)]},
        ),
        REPO_NODE,
    )
    assert epic is not None and epic.epic and epic.sub_total == 2
    # Two of eight sub-issues: never taken for the whole list (autopilot would miss six).
    cut = links_payload(
        subIssuesSummary={"total": 8, "completed": 1},
        subIssues={"totalCount": 8, "nodes": [node(822, "CLOSED"), node(823)]},
    )
    assert parse_links(cut, REPO_NODE) is None


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


# ------------------------------------------------------------------ long lists (#878)


def page(nodes: list[dict[str, Any]], total: int, cursor: str | None) -> dict[str, Any]:
    return {
        "totalCount": total,
        "nodes": nodes,
        "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
    }


def test_any_list_shorter_than_its_total_is_unreadable_never_a_short_list():
    short = {"totalCount": 60, "nodes": [node(1)]}
    assert parse_links(links_payload(blocking=short), REPO_NODE) is None
    assert parse_links(links_payload(subIssues=short), REPO_NODE) is None
    # More behind it by pageInfo even where the count agrees: still not the whole list.
    more = page([node(1)], 1, "c1")
    assert parse_links(links_payload(subIssues=more), REPO_NODE) is None
    assert parse_links(links_payload(blockedBy=more), REPO_NODE) is None


def _issue_read(links: dict[str, Any]) -> dict[str, Any]:
    return {
        "data": {
            "node": {
                **links,
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
    }


async def _snapshot(
    first: dict[str, Any], pages: dict[tuple[str, str], dict[str, Any]], calls: list[Any]
):
    """An issue read whose first request returns ``first``; a follow-up page request for
    list ``key`` after cursor ``c`` returns ``pages[(key, c)]``."""
    import json

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200, json={"node_id": "I_1", "state": "open", "assignees": [], "title": "T"}
            )
        body = json.loads(request.content)
        calls.append(body)
        after = body["variables"].get("after")
        if "projectItems" in body["query"]:
            assert after is None
            return httpx.Response(200, json=_issue_read(first))
        key = next(
            k for k in ("subIssues", "blockedBy", "blocking") if f"{k}(first" in body["query"]
        )
        return httpx.Response(200, json={"data": {"node": {key: pages[(key, after)]}}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        return await adapter(http).issue_snapshot(IssueRef(REPO_NODE, 878, "I_1"))


@pytest.mark.asyncio
async def test_an_epic_with_more_sub_issues_than_one_read_gets_every_one_paged():
    # #878: 86 sub-issues; the first 50 are closed, the open ones come after them.
    closed = [node(100 + i, "CLOSED") for i in range(50)]
    rest = [node(200 + i) for i in range(36)]
    first = links_payload(
        subIssuesSummary={"total": 86, "completed": 50},
        subIssues=page(closed, 86, "c50"),
    )
    pages = {
        ("subIssues", "c50"): page(rest[:30], 86, "c80"),
        ("subIssues", "c80"): page([*rest[29:]], 86, None),  # #229 repeated across pages
    }
    calls: list[Any] = []
    snap = await _snapshot(first, pages, calls)
    assert snap.links is not None
    numbers = [s.number for s in snap.links.sub_issues]
    assert numbers == [n["number"] for n in closed + rest]
    assert [s.number for s in snap.links.sub_issues if s.open] == list(range(200, 236))
    assert len(calls) == 3 and [c["variables"]["after"] for c in calls[1:]] == ["c50", "c80"]
    assert all(c["variables"]["id"] == "I_1" for c in calls[1:])


@pytest.mark.asyncio
async def test_a_list_that_fits_costs_no_extra_request():
    calls: list[Any] = []
    snap = await _snapshot(
        links_payload(subIssues=page([node(2)], 1, None), blockedBy=page([], 0, None)),
        {},
        calls,
    )
    assert snap.links is not None and len(calls) == 1


@pytest.mark.asyncio
async def test_a_blocker_past_the_first_read_is_seen_and_an_unfinished_list_fails_closed():
    blockers = [node(300 + i, "CLOSED") for i in range(50)]
    first = links_payload(blockedBy=page(blockers, 51, "b50"))
    calls: list[Any] = []
    snap = await _snapshot(first, {("blockedBy", "b50"): page([node(999)], 51, None)}, calls)
    assert snap.links is not None and [b.number for b in snap.links.open_blockers] == [999]
    # GitHub says there is more but gives no cursor: unreadable, never "unblocked".
    stuck = links_payload(
        blockedBy={
            "totalCount": 51,
            "nodes": blockers,
            "pageInfo": {"hasNextPage": True, "endCursor": None},
        }
    )
    calls = []
    snap = await _snapshot(stuck, {}, calls)
    assert snap.links is None and len(calls) == 1


@pytest.mark.asyncio
async def test_a_list_longer_than_the_page_limit_stays_unreadable(monkeypatch: pytest.MonkeyPatch):
    import omnigent_factory.github.links as links_module

    monkeypatch.setattr(links_module, "MAX_LINK_PAGES", 2)
    first = links_payload(blocking=page([node(1)], 9, "k1"))
    pages = {
        ("blocking", "k1"): page([node(2)], 9, "k2"),
        ("blocking", "k2"): page([node(3)], 9, "k3"),
    }
    calls: list[Any] = []
    snap = await _snapshot(first, pages, calls)
    assert snap.links is None and len(calls) == 3


def _board_handler(links: dict[str, Any]):
    def handler(request: httpx.Request) -> httpx.Response:
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
            **links,
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

    return handler


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["blockedBy", "blocking"])
async def test_board_reads_never_page_and_a_cut_list_is_unreadable(key: str):
    cut = {"totalCount": 21, "nodes": [node(500 + i, "CLOSED") for i in range(20)]}
    payload = {k: v for k, v in links_payload(**{key: cut}).items() if k != "subIssues"}
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.content.decode())
        return _board_handler(payload)(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        a = adapter(http)
        [issue] = await a.board_issues()
        [card] = await a.board_cards()
    assert issue.links is None and card.links is None
    assert len(calls) == 2  # one read each: the board never pages links


@pytest.mark.asyncio
async def test_gate_lookup_reads_every_sub_issue_page():
    import json

    from omnigent_factory.github.client import GitHubClient
    from omnigent_factory.github.links import epic_gate_issues, gate_marker

    def handler(request: httpx.Request) -> httpx.Response:
        after = json.loads(request.content)["variables"].get("after")
        if after is None:
            subs = {
                "nodes": [{"number": n, "body": ""} for n in range(100, 200)],
                "pageInfo": {"hasNextPage": True, "endCursor": "g100"},
            }
        else:
            assert after == "g100"
            subs = {
                "nodes": [{"number": 1040, "body": gate_marker(878, "deploy")}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        return httpx.Response(200, json={"data": {"repository": {"issue": {"subIssues": subs}}}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        found = await epic_gate_issues(GitHubClient(http, "token"), "o", "n", 878)
    assert found == {"deploy": 1040}
