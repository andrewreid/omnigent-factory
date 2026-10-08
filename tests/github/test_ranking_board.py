"""GitHub reads and writes of the triage ranking."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from omnigent_factory.core.types import Stage
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.github.ranking import RankingBoard, RankingWriteError, marker
from omnigent_factory.github.setup import render_project_migration
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.doctor import DoctorReport, _check_project
from tests.service.test_status_names import _Board, _live

REPO_NODE = "R_kgDOTC12Fg"
PROJECT = "PVT_kwDOEanNes4BkJhb"
RANK = "PVTF_rank"
BOT = 777

Handler = Callable[[httpx.Request], httpx.Response]


async def _board(handler: Handler) -> tuple[RankingBoard, httpx.AsyncClient]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository="SA-Ambulance/timesheets",
        repository_node_id=REPO_NODE,
        project_node_id=PROJECT,
        status_field_node_id="PVTSSF_x",
        bot_user_id=BOT,
        required_checks=frozenset(),
    )
    return RankingBoard(adapter), http


def _item(number: int, *, option: str = "43889573", **content: Any) -> dict[str, Any]:
    base = {
        "__typename": "Issue",
        "id": f"I_{number}",
        "number": number,
        "title": f"t{number}",
        "state": "OPEN",
        "createdAt": "2026-10-01T00:00:00Z",
        "repository": {"id": REPO_NODE},
    }
    base.update(content)
    return {
        "id": f"PVTI_{number}",
        "status": {"optionId": option},
        "fieldValues": {"nodes": []},
        "content": base,
    }


@pytest.mark.asyncio
async def test_cards_read_rank_by_field_id_and_priority_by_name():
    first = _item(1)
    first["fieldValues"] = {
        "nodes": [
            {"number": 3, "field": {"id": RANK}},
            {"number": 9, "field": {"id": "PVTF_other_number"}},
            {"name": "P1", "field": {"id": "PVTSSF_p", "name": "Priority"}},
            {"name": "M", "field": {"id": "PVTSSF_s", "name": "Size"}},
            {},
        ]
    }
    nodes = [
        first,
        _item(2, option="ba3c85dd"),
        _item(3, state="CLOSED"),
        _item(4, repository={"id": "R_x"}),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        page = {"nodes": nodes, "pageInfo": {"hasNextPage": False, "endCursor": None}}
        return httpx.Response(200, json={"data": {"node": {"items": page}}})

    board, http = await _board(handler)
    async with http:
        cards = await board.cards(RANK)
    assert isinstance(cards, list)
    assert [(c.number, c.stage, c.rank, c.priority, c.item_id) for c in cards] == [
        (1, Stage.TRIAGED, 3.0, "P1", "PVTI_1"),
        (2, Stage.BUILDING, None, None, "PVTI_2"),
    ]


@pytest.mark.asyncio
async def test_a_comment_is_adopted_by_its_marker_never_posted_twice():
    comments: list[dict[str, Any]] = []
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if request.method == "GET":
            return httpx.Response(200, json=comments)
        posts += 1
        body = json.loads(request.content)["body"]
        comments.append({"id": 55, "body": body, "user": {"id": BOT}})
        return httpx.Response(201, json={"id": 55})

    board, http = await _board(handler)
    async with http:
        first = await board.post_comment(4, "I_4", "rk_1:comment:4", "Priority changed P2 -> P1")
        second = await board.post_comment(4, "I_4", "rk_1:comment:4", "Priority changed P2 -> P1")
    assert first == second == "55" and posts == 1
    assert comments[0]["body"].endswith(marker("rk_1:comment:4"))


@pytest.mark.asyncio
async def test_status_update_is_adopted_by_marker_and_errors_are_reported():
    updates: list[dict[str, Any]] = []
    created = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal created
        query = json.loads(request.content)["query"]
        if "createProjectV2StatusUpdate" in query:
            created += 1
            body = json.loads(request.content)["variables"]["input"]["body"]
            updates.append({"id": "SU_1", "body": body})
            return httpx.Response(
                200,
                json={"data": {"createProjectV2StatusUpdate": {"statusUpdate": {"id": "SU_1"}}}},
            )
        return httpx.Response(
            200, json={"data": {"node": {"statusUpdates": {"nodes": list(updates)}}}}
        )

    board, http = await _board(handler)
    async with http:
        assert await board.post_status_update("rk_1:status_update:project", "top 5") == "SU_1"
        assert await board.post_status_update("rk_1:status_update:project", "top 5") == "SU_1"
    assert created == 1

    def refused(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"errors": [{"message": "Resource not accessible"}]})

    board, http = await _board(refused)
    async with http:
        with pytest.raises(RankingWriteError) as failed:
            await board.post_status_update("rk_2:status_update:project", "x")
    assert failed.value.retry  # bounded retries by the ranker, then recorded as failed


def test_board_migration_creates_the_rank_number_field():
    fields = [f["input"] for f in render_project_migration()["create_fields"]]
    assert {"projectId": PROJECT, "name": "Rank", "dataType": "NUMBER"} in fields


class _RankBoard(_Board):
    def __init__(self, config: ServiceConfig, rank: dict[str, Any] | None) -> None:
        super().__init__(config, _live())
        self.rank = rank

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if "statusUpdates" in query:
            return {"node": {"statusUpdates": {"nodes": []}}}
        data = await super().graphql(query, variables)
        if self.rank is not None:
            data["node"]["fields"]["nodes"].append(self.rank)
        return data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rank_id", "rank", "check", "ok"),
    [
        (
            "PVTF_r",
            {"id": "PVTF_r", "name": "Rank", "dataType": "NUMBER"},
            "Rank number field ID matches",
            True,
        ),
        ("PVTF_r", {"id": "PVTF_r", "name": "Rank", "dataType": "TEXT"}, "failed", False),
        ("", None, "warning: rank_field_node_id is not set", True),
    ],
)
async def test_doctor_checks_the_rank_field(
    rank_id: str, rank: dict[str, Any] | None, check: str, ok: bool
):
    config = ServiceConfig(
        repo_id="R", owners=frozenset({1}), ranking=True, rank_field_node_id=rank_id
    )
    report = DoctorReport()
    await _check_project(config, _RankBoard(config, rank), report)  # type: ignore[arg-type]
    assert report.checks["github_rank_field"].startswith(check)
    assert report.ok is ok
    if ok and rank is not None:
        assert report.checks["github_status_updates"].startswith("status updates readable")
