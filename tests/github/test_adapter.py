from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from omnigent_factory.core.contract_view import parcel_marker, render_contract_section
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
)
from omnigent_factory.core.events import ChecksState
from omnigent_factory.core.types import Stage
from omnigent_factory.github.adapter import (
    BoardSchema,
    GitHubAPIAdapter,
    ParcelBinding,
)
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.ports.github import IssueRef

REPOSITORY = "SA-Ambulance/timesheets"
REPO_NODE = "R_kgDOTC12Fg"
PROJECT = "PVT_kwDOEanNes4BkJhb"
STATUS_FIELD = "PVTSSF_lADOEanNes4BkJhbzhi7I9w"
BOT_ID = 777
CTX = ExecutionContext("boot", 1, 1, 1)


def adapter(http: httpx.AsyncClient, checks=frozenset(), **kwargs):
    kwargs.setdefault("independent_reviewer_ids", frozenset({5}))
    kwargs.setdefault("owner_ids", frozenset({114979}))
    return GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository=REPOSITORY,
        repository_node_id=REPO_NODE,
        project_node_id=PROJECT,
        status_field_node_id=STATUS_FIELD,
        bot_user_id=BOT_ID,
        required_checks=checks,
        now_us=lambda: 123_000_000,
        **kwargs,
    )


PARCEL_REF = IssueRef(REPO_NODE, 12, "I_1")
GOAL_X = '{"goal":"x"}'
RENDERED_EMPTY = (
    f"{parcel_marker(12, hashlib.sha256(b'{}').hexdigest()[:12])}\n{render_contract_section('{}')}"
)


def closing_refs(*issue_ids: str, repo: str = REPO_NODE, number: int = 7, cursor=None):
    """Source-shaped ``closingIssuesReferences`` GraphQL page."""
    return httpx.Response(
        200,
        json={
            "data": {
                "repository": {
                    "id": REPO_NODE,
                    "pullRequest": {
                        "number": number,
                        "closingIssuesReferences": {
                            "nodes": [{"id": i, "repository": {"id": repo}} for i in issue_ids],
                            "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
                        },
                    },
                }
            }
        },
    )


def effect(kind: EffectKind, effect_id="eff-1", **args):
    return EffectIntent(
        effect_id=effect_id,
        kind=kind,
        parcel_id="I_1",
        target="github",
        preconditions=Preconditions(1, 1),
        args=args,
    )


@pytest.mark.asyncio
async def test_fresh_issue_and_board_snapshot_verifies_identities():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "node_id": "I_1",
                    "state": "open",
                    "assignees": [{"id": BOT_ID, "type": "Bot"}],
                    "title": "Task",
                    "body": "Details",
                },
            )
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
                                        "name": "Scoped",
                                        "optionId": "3a7f779a",
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

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        snapshot = await adapter(http).issue_snapshot(IssueRef(REPO_NODE, 12, "I_1"))
    assert snapshot.stage == Stage.SCOPED
    assert snapshot.in_project is True
    assert snapshot.human_assigned is False
    assert snapshot.read_at_us == 123_000_000


@pytest.mark.asyncio
async def test_pull_request_snapshot_reads_all_check_pages_and_current_reviews():
    required = frozenset({("required", 15368)})

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
                    "body": "Closes #12",
                },
            )
        if path.endswith("/check-runs"):
            page = request.url.params.get("page")
            if page == "1":
                runs = [
                    {"name": f"noise-{index}", "app": {"id": 15368}, "conclusion": "success"}
                    for index in range(100)
                ]
            else:
                runs = [{"name": "required", "app": {"id": 15368}, "conclusion": "success"}]
            return httpx.Response(200, json={"check_runs": runs})
        if path.endswith("/reviews"):
            return httpx.Response(
                200,
                json=[
                    {"id": 1, "state": "APPROVED", "commit_id": "old", "user": {"id": 5}},
                    {"id": 2, "state": "APPROVED", "commit_id": "abc", "user": {"id": 5}},
                ],
            )
        if path.endswith("/comments"):
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 9,
                        "user": {"id": BOT_ID},
                        "body": "<!-- factory-findings-dispositioned head=abc -->",
                    }
                ],
            )
        if path == "/graphql":
            return closing_refs("I_other", "I_1")
        raise AssertionError(str(request.url))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http, required).pull_request(PARCEL_REF, 7)
    assert result.verified is True
    assert result.checks == ChecksState.GREEN


@pytest.mark.asyncio
async def test_comment_lost_ack_is_adopted_across_pages_only_for_bot_author():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.params.get("page") == "2":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 99,
                        "user": {"id": BOT_ID},
                        "body": "done\n<!-- omnigent-factory effect=eff-1 -->",
                    }
                ],
            )
        return httpx.Response(
            200,
            json=[
                {
                    "id": 10,
                    "user": {"id": 123},
                    "body": "forged <!-- omnigent-factory effect=eff-1 -->",
                }
            ],
            headers={
                "Link": (
                    f"<https://api.github.com/repos/{REPOSITORY}/issues/12/comments?page=2>; "
                    'rel="next"'
                )
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http).execute(
            effect(EffectKind.POST_COMMENT, issue_number=12, body="done"), CTX
        )
    assert result == Ack("99", {"adopted": True})
    assert all(request.method == "GET" for request in calls)


@pytest.mark.asyncio
async def test_comment_transport_loss_is_ambiguous_not_blind_retry():
    methods = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.method == "GET":
            return httpx.Response(200, json=[])
        raise httpx.ReadTimeout("response lost", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http).execute(
            effect(EffectKind.POST_COMMENT, issue_number=12, body="done"), CTX
        )
    assert isinstance(result, AmbiguousWrite)
    assert methods == ["GET", "POST"]


@pytest.mark.asyncio
async def test_board_write_preserves_exact_option_id_and_reads_back():
    option_reads = iter(["915abb46", "ba3c85dd"])
    mutation_input = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal mutation_input
        body = json.loads(request.content)
        if "updateProjectV2ItemFieldValue" in body["query"]:
            mutation_input = body["variables"]["input"]
            return httpx.Response(
                200,
                json={
                    "data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "PVTI_1"}}}
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "fieldValueByName": {
                            "optionId": next(option_reads),
                            "field": {"id": STATUS_FIELD},
                        }
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http).execute(
            effect(
                EffectKind.MOVE_CARD,
                item_id="PVTI_1",
                field_id=STATUS_FIELD,
                field_name="Status",
                expected_option_id="915abb46",
                option_id="ba3c85dd",
            ),
            CTX,
        )
    assert result == Ack("PVTI_1", {"option_id": "ba3c85dd"})
    assert mutation_input["value"] == {"singleSelectOptionId": "ba3c85dd"}


@pytest.mark.asyncio
async def test_reducer_shaped_board_effect_resolves_persisted_ids():
    option_reads = iter(["915abb46", "ba3c85dd"])
    mutation_input = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal mutation_input
        body = json.loads(request.content)
        if "updateProjectV2ItemFieldValue" in body["query"]:
            mutation_input = body["variables"]["input"]
            return httpx.Response(
                200,
                json={
                    "data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "PVTI_1"}}}
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "fieldValueByName": {
                            "optionId": next(option_reads),
                            "field": {"id": STATUS_FIELD},
                        }
                    }
                }
            },
        )

    schema = BoardSchema(
        STATUS_FIELD,
        {"Inbox": "915abb46", "Building": "ba3c85dd"},
        "BOT_FIELD",
        {"Working": "bot-working"},
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(
            http,
            parcel_bindings={"I_1": ParcelBinding(12, "PVTI_1")},
            board_schema=schema,
        ).execute(
            effect(EffectKind.MOVE_CARD, to="Building", expected_from="Inbox"),
            CTX,
        )
    assert isinstance(result, Ack)
    assert mutation_input["fieldId"] == STATUS_FIELD
    assert mutation_input["value"] == {"singleSelectOptionId": "ba3c85dd"}


@pytest.mark.asyncio
async def test_reducer_shaped_contract_publication_uses_binding_and_renderer():
    posted = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posted
        if request.method == "GET":
            if posted is None:
                return httpx.Response(200, json=[])
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 55,
                        "body": posted,
                        "user": {"id": BOT_ID},
                        "created_at": "2026-01-01T00:00:00Z",
                    }
                ],
            )
        posted = json.loads(request.content)["body"]
        return httpx.Response(201, json={"id": 55})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(
            http,
            parcel_bindings={"I_1": ParcelBinding(12, "PVTI_1")},
            publication_renderer=lambda _: RENDERED_EMPTY,
        ).execute(
            effect(
                EffectKind.PUBLISH_CONTRACT,
                contract_id="c1",
                full_hash=hashlib.sha256(b"{}").hexdigest(),
            ),
            CTX,
        )
    assert result == Ack("55", {"verified": True, "posted_at_us": 1_767_225_600_000_000})
    assert posted == f"{RENDERED_EMPTY}\n\n<!-- omnigent-factory effect=eff-1 -->"
    assert "parcel-contract" not in posted  # no JSON is published


@pytest.mark.asyncio
async def test_board_write_refuses_stale_source_snapshot():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "fieldValueByName": {
                            "optionId": "owner-newer",
                            "field": {"id": STATUS_FIELD},
                        }
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http).execute(
            effect(
                EffectKind.MOVE_CARD,
                item_id="PVTI_1",
                field_id=STATUS_FIELD,
                expected_option_id="old",
                option_id="target",
            ),
            CTX,
        )
    assert result.reason == "board field changed since the effect source snapshot"


@pytest.mark.asyncio
async def test_contract_publication_adopts_exact_bot_fence():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "id": 5,
                    "user": {"id": BOT_ID},
                    "created_at": "2026-01-01T00:00:00Z",
                    "body": (
                        f"{parcel_marker(12, 'a' * 12)}\nintro\n"
                        f"{render_contract_section(GOAL_X)}\n"
                        "<!-- omnigent-factory effect=pub-1 -->"
                    ),
                }
            ],
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http).find_contract_publication(
            IssueRef(REPO_NODE, 12, "I_1"), "pub-1"
        )
    assert result is not None
    assert result.contract_section == render_contract_section(GOAL_X)
    assert result.marker_hash == "a" * 12
    assert result.author_is_bot is True
