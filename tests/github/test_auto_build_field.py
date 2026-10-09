"""The board's "Auto-build" field in the GitHub adapter: read by option ID, written as a
display value (set, adopt, clear), and part of the board-diff card digest."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import httpx
import pytest

from omnigent_factory.core.effects import (
    Ack,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
)
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter, ParcelBinding
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.ports.github import BoardCard, IssueRef

CTX = ExecutionContext("boot", 1, 1, 1)
AUTO_FIELD = "PVTSSF_auto"
OPTIONS = {"Queued": "opt_q", "Started": "opt_s"}


def _adapter(http: httpx.AsyncClient, *, configured: bool = True) -> GitHubAPIAdapter:
    return GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository="SA-Ambulance/timesheets",
        repository_node_id="R_kgDOTC12Fg",
        project_node_id="PVT_1",
        status_field_node_id="STATUS",
        bot_user_id=777,
        required_checks=frozenset(),
        parcel_bindings={"I_1": ParcelBinding(12, "PVTI_1")},
        board_schema=BoardSchema(
            "STATUS",
            {"Scoped": "opt_scoped"},
            "BOT",
            {},
            "NOTE",
            AUTO_FIELD if configured else "",
            OPTIONS if configured else {},
        ),
    )


def _effect(**args: Any) -> EffectIntent:
    return EffectIntent(
        "eff-1", EffectKind.SET_AUTO_BUILD, "I_1", "github", Preconditions(1, 1), args
    )


def _handler(current: str | None, calls: list[dict[str, Any]]):
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        if "mutation" in body["query"]:
            return httpx.Response(200, json={"data": {}})
        value = None if current is None else {"optionId": current, "field": {"id": AUTO_FIELD}}
        return httpx.Response(200, json={"data": {"node": {"fieldValueByName": value}}})

    return handler


@pytest.mark.asyncio
async def test_write_sets_the_option_adopts_a_match_and_clears_empty():
    calls: list[dict[str, Any]] = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler("opt_q", calls))) as c:
        result = await _adapter(c).execute(_effect(value="Started"), CTX)
    assert isinstance(result, Ack)
    assert calls[-1]["variables"]["input"] == {
        "projectId": "PVT_1",
        "itemId": "PVTI_1",
        "fieldId": AUTO_FIELD,
        "value": {"singleSelectOptionId": "opt_s"},
    }
    calls.clear()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler("opt_s", calls))) as c:
        result = await _adapter(c).execute(_effect(value="Started"), CTX)
    assert result == Ack("PVTI_1", {"adopted": True, "value": "Started"}) and len(calls) == 1
    calls.clear()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler("opt_q", calls))) as c:
        result = await _adapter(c).execute(_effect(value=""), CTX)
    assert isinstance(result, Ack) and "clearProjectV2ItemFieldValue" in calls[-1]["query"]
    calls.clear()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler(None, calls))) as c:
        result = await _adapter(c).execute(_effect(value=""), CTX)
    assert result == Ack("PVTI_1", {"adopted": True, "value": ""})
    async with httpx.AsyncClient(transport=httpx.MockTransport(_handler(None, []))) as c:
        result = await _adapter(c, configured=False).execute(_effect(value="Started"), CTX)
    assert isinstance(result, DefinitiveFailure)


@pytest.mark.asyncio
async def test_issue_snapshot_reads_the_field_by_option_id():
    def handler(option: str | None):
        def respond(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(
                    200,
                    json={"node_id": "I_1", "state": "open", "assignees": [], "title": "t"},
                )
            auto = None if option is None else {"optionId": option, "field": {"id": AUTO_FIELD}}
            item = {
                "id": "PVTI_1",
                "project": {"id": "PVT_1"},
                "fieldValueByName": {
                    "name": "Planning",
                    "optionId": "opt_scoped",
                    "field": {"id": "STATUS"},
                },
                "bot": None,
                "note": None,
                "autoBuild": auto,
            }
            page = {"nodes": [item], "pageInfo": {"hasNextPage": False}}
            return httpx.Response(200, json={"data": {"node": {"projectItems": page}}})

        return respond

    ref = IssueRef("R_kgDOTC12Fg", 12, "I_1")
    for option, expected in (("opt_q", "Queued"), (None, ""), ("opt_other", "?")):
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler(option))) as c:
            snap = await _adapter(c).issue_snapshot(ref)
        assert not isinstance(snap, Exception) and snap.auto_build == expected  # type: ignore[union-attr]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler("opt_q"))) as c:
        snap = await _adapter(c, configured=False).issue_snapshot(ref)
    assert snap.auto_build is None  # type: ignore[union-attr]


def test_the_board_diff_sees_an_auto_build_change_and_keeps_old_digests():
    base = BoardCard("I_1", 12, True, "opt_scoped", "", (), (), "t", "2026-10-09T00:00:00Z")
    queued = BoardCard(
        "I_1", 12, True, "opt_scoped", "", (), (), "t", "2026-10-09T00:00:00Z", "opt_q"
    )
    assert queued.digest != base.digest
    # A card without the field keeps the digest stored before the field existed.
    before = json.dumps(
        [True, "opt_scoped", "", [], [], "t", "2026-10-09T00:00:00Z"],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert base.digest == hashlib.sha256(before.encode()).hexdigest()
