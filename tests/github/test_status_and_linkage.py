"""Task 5a port changes: Status by option ID (T2 F3) and parcel-bound PR linkage (T2 F1)."""

from __future__ import annotations

import json

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, DefinitiveFailure, EffectKind
from omnigent_factory.core.types import Stage
from omnigent_factory.github.adapter import BoardSchema
from omnigent_factory.ports.github import (
    STATUS_FIELD_NODE_ID,
    STATUS_OPTION_IDS,
    IssueRef,
    stage_for_option,
)

from .test_adapter import (
    BOT_ID,
    CTX,
    PARCEL_REF,
    PROJECT,
    REPO_NODE,
    STATUS_FIELD,
    adapter,
    closing_refs,
    effect,
)
from .test_webhook_config_setup import project_drag, resolve_drag

LIVE = {
    Stage.INBOX: "915abb46",
    Stage.TRIAGED: "43889573",
    Stage.SCOPED: "3a7f779a",
    Stage.BUILDING: "ba3c85dd",
    Stage.READY: "6df89cbb",
    Stage.DONE: "4980e49d",
}


def test_live_status_option_ids_are_pinned():
    assert dict(STATUS_OPTION_IDS) == LIVE
    assert STATUS_FIELD_NODE_ID == "PVTSSF_lADOEanNes4BkJhbzhi7I9w" == STATUS_FIELD


@pytest.mark.parametrize("stage", list(LIVE))
def test_stage_for_option_is_by_id_only(stage: Stage):
    assert stage_for_option(LIVE[stage]) == stage
    assert stage_for_option(stage.value) is None  # a name is never an ID
    assert stage_for_option(None) is None and stage_for_option("") is None


def test_ambiguous_option_mapping_reads_as_unknown():
    assert stage_for_option("x", {Stage.INBOX: "x", Stage.READY: "x"}) is None


# ----------------------------------------------------------- snapshot reads


def project_response(option_id: str, name: str) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "data": {
                "node": {
                    "projectItems": {
                        "nodes": [
                            {
                                "id": "PVTI_1",
                                "project": {"id": PROJECT},
                                "fieldValueByName": {
                                    "name": name,
                                    "optionId": option_id,
                                    "field": {"id": STATUS_FIELD},
                                },
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        },
    )


async def snapshot_stage(option_id: str, name: str, **kw: object) -> Stage | None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"node_id": "I_1", "state": "open", "assignees": [], "title": "T"},
            )
        return project_response(option_id, name)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        snap = await adapter(http, **kw).issue_snapshot(IssueRef(REPO_NODE, 12, "I_1"))
    assert not isinstance(snap, Exception)
    return snap.stage  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_renamed_option_keeps_its_stage():
    assert await snapshot_stage(LIVE[Stage.SCOPED], "Planning (renamed)") == Stage.SCOPED


@pytest.mark.asyncio
async def test_matching_name_with_unknown_option_id_is_not_a_stage():
    assert await snapshot_stage("recreated-option", "Building") is None


@pytest.mark.asyncio
async def test_board_schema_option_ids_override_defaults():
    schema = BoardSchema(
        status_field_id=STATUS_FIELD,
        status_options={"Ready": "custom-ready"},
        bot_field_id="BOT",
        bot_options={},
    )
    assert await snapshot_stage("custom-ready", "Whatever", board_schema=schema) == Stage.READY
    assert await snapshot_stage(LIVE[Stage.READY], "Ready", board_schema=schema) is None


# ----------------------------------------------------------- webhook reads


@pytest.mark.asyncio
async def test_resolved_drag_uses_option_ids_even_when_names_lie():
    data = project_drag(old="Scoped", new="Building")
    data["changes"]["field_value"]["from"]["name"] = "Ready"
    data["changes"]["field_value"]["to"]["name"] = "Inbox"
    (event,) = (await resolve_drag(data)).events
    assert isinstance(event.body, ev.ApprovePlan)  # Scoped -> Building by ID


@pytest.mark.asyncio
async def test_resolved_drag_with_unknown_option_ids_never_becomes_a_control():
    data = project_drag(old="Scoped", new="Building")
    data["changes"]["field_value"]["from"]["id"] = "unknown-a"
    data["changes"]["field_value"]["to"]["id"] = "unknown-b"
    result = await resolve_drag(data)
    assert all(
        not isinstance(e.body, ev.ApprovePlan | ev.WaivePlan | ev.RequestPlan)
        for e in result.events
    )


@pytest.mark.asyncio
async def test_resolver_with_unknown_current_option_id_stays_unresolved():
    data = project_drag(old="Scoped", new="Building")
    # the fixture's current item carries the option ID for this name; "Nonsense" has none
    data["changes"]["field_value"]["to"]["name"] = "Nonsense"
    result = await resolve_drag(data)
    assert result.events == ()
    assert result.ignored_reason == "current Status option is unknown"


# ----------------------------------------------------------- PR linkage


def pr_handler(graphql: list[httpx.Response], seen: list[dict]):
    pages = iter(graphql)

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/pulls/7"):
            return httpx.Response(
                200,
                json={
                    "state": "open",
                    "merged": False,
                    "head": {"sha": "abc", "ref": "factory/issue-12"},
                    "user": {"id": BOT_ID},
                    "body": "Closes #12",  # text alone never counts
                },
            )
        if path.endswith("/check-runs"):
            return httpx.Response(200, json={"check_runs": []})
        if path.endswith("/reviews") or path.endswith("/comments"):
            return httpx.Response(200, json=[])
        if path == "/graphql":
            seen.append(json.loads(request.content))
            return next(pages)
        raise AssertionError(path)

    return handler


async def closes(graphql: list[httpx.Response], ref: IssueRef = PARCEL_REF) -> bool:
    seen: list[dict] = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(pr_handler(graphql, seen))) as h:
        result = await adapter(h).pull_request(ref, 7)
    assert not isinstance(result, Exception)
    assert seen and seen[0]["variables"]["number"] == 7
    return result.closes_issue  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_pr_closing_another_issue_does_not_close_the_parcel():
    assert await closes([closing_refs("I_other")]) is False


@pytest.mark.asyncio
async def test_pr_closing_the_parcel_issue_matches():
    assert await closes([closing_refs("I_other", "I_1")]) is True


@pytest.mark.asyncio
async def test_same_issue_id_from_another_repository_does_not_match():
    assert await closes([closing_refs("I_1", repo="R_elsewhere")]) is False


@pytest.mark.asyncio
async def test_closing_references_are_paginated():
    assert await closes([closing_refs("I_a", cursor="c1"), closing_refs("I_1")]) is True


@pytest.mark.asyncio
async def test_the_question_is_asked_for_this_parcel_only():
    other = IssueRef(REPO_NODE, 13, "I_2")
    assert await closes([closing_refs("I_1")], other) is False


@pytest.mark.asyncio
async def test_closing_reference_read_failure_is_retryable():
    bad = httpx.Response(200, json={"data": {"repository": {"id": "R_wrong"}}})
    seen: list[dict] = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(pr_handler([bad], seen))) as h:
        result = await adapter(h).pull_request(PARCEL_REF, 7)
    assert not hasattr(result, "closes_issue")


@pytest.mark.asyncio
async def test_pr_reader_rejects_a_ref_for_another_repository():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as h:
        result = await adapter(h).pull_request(IssueRef("R_other", 12, "I_1"), 7)
    assert not hasattr(result, "closes_issue")


@pytest.mark.asyncio
async def test_fetch_pr_evidence_requires_the_parcel_issue():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as h:
        outcome = await adapter(h).execute(effect(EffectKind.FETCH_PR_EVIDENCE, pr_number=7), CTX)
    assert isinstance(outcome, DefinitiveFailure)


@pytest.mark.asyncio
async def test_fetch_pr_evidence_binds_the_effect_parcel():
    seen: list[dict] = []
    handler = pr_handler([closing_refs("I_1")], seen)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as h:
        outcome = await adapter(h).execute(
            effect(EffectKind.FETCH_PR_EVIDENCE, pr_number=7, issue_number=12), CTX
        )
    assert isinstance(outcome, Ack) and outcome.detail["closes_issue"] is True
