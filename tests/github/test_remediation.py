from __future__ import annotations

import email.utils
import json
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import AmbiguousWrite, EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import Stage
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter, ParcelBinding
from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubAPIError, GitHubClient, RateLimited
from omnigent_factory.github.setup import render_app_manifest
from omnigent_factory.github.webhook import (
    DeliveryIdentity,
    DeliveryNormalizer,
    SignatureError,
    WebhookError,
)
from omnigent_factory.testing.harness import Harness

from .test_adapter import (
    BOT_ID,
    CTX,
    PARCEL_REF,
    PROJECT,
    REPO_NODE,
    REPOSITORY,
    STATUS_FIELD,
    adapter,
    closing_refs,
    no_threads,
)
from .test_auth_client import private_key
from .test_webhook_config_setup import normalize, payload, project_drag, resolve_drag


@pytest.mark.asyncio
async def test_R1_project_resolution_rejects_non_github_provenance():
    from omnigent_factory.github import webhook

    data = project_drag()
    raw = json.dumps(data, separators=(",", ":")).encode()
    unresolved = replace(normalize(data, "projects_v2_item"), provenance=Provenance.ADAPTER)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"node": None}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(WebhookError, match="provenance"):
            await webhook.resolve_project_delivery(
                client=GitHubClient(http, "token"),
                normalizer=DeliveryNormalizer(_identity()),
                unresolved=unresolved,
                raw_body=raw,
                delivery_time_us=2_000_000_000_000_000,
            )


@pytest.mark.asyncio
async def test_R2_deleted_project_item_emits_item_removed_when_absent():
    from omnigent_factory.github import webhook

    data = project_drag(actor=222)
    data["action"] = "deleted"
    data.pop("changes")
    raw = json.dumps(data, separators=(",", ":")).encode()
    unresolved = normalize(data, "projects_v2_item", provenance=Provenance.RECOVERY)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "__typename": "Issue",
                        "id": "I_1",
                        "number": 12,
                        "title": "Task",
                        "body": "Details",
                        "state": "OPEN",
                        "assignees": {"nodes": []},
                        "repository": {
                            "id": REPO_NODE,
                            "databaseId": 1278047766,
                            "nameWithOwner": REPOSITORY,
                        },
                        "projectItems": {
                            "nodes": [],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        },
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await webhook.resolve_project_delivery(
            client=GitHubClient(http, "token"),
            normalizer=DeliveryNormalizer(_identity()),
            unresolved=unresolved,
            raw_body=raw,
            delivery_time_us=2_000_000_000_000_000,
        )
    (event,) = result.events
    assert isinstance(event.body, ev.ItemRemoved)
    assert event.actor_id == 222
    assert event.provenance == Provenance.RECOVERY
    assert event.evidence is not None
    assert event.evidence.in_project is False
    assert event.evidence.stage is None


@pytest.mark.asyncio
async def test_B1_bot_leftward_board_echo_is_observation():
    (event,) = (await resolve_drag(project_drag(actor=BOT_ID, old="Building", new="Scoped"))).events
    assert isinstance(event.body, ev.ColumnObserved)
    assert event.body.stage == Stage.SCOPED


@pytest.mark.asyncio
async def test_B2_repositoryless_project_delivery_resolves_by_authenticated_read():
    from omnigent_factory.github import webhook

    resolver = getattr(webhook, "resolve_project_delivery", None)
    assert callable(resolver)
    data = project_drag()
    raw = json.dumps(data, separators=(",", ":")).encode()
    unresolved = normalize(data, "projects_v2_item", provenance=Provenance.RECOVERY)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer token"
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "__typename": "Issue",
                        "id": "I_1",
                        "number": 12,
                        "title": "Task",
                        "body": "Details",
                        "state": "OPEN",
                        "assignees": {"nodes": []},
                        "repository": {
                            "id": REPO_NODE,
                            "databaseId": 1278047766,
                            "nameWithOwner": REPOSITORY,
                        },
                        "projectItems": {
                            "nodes": [
                                {
                                    "id": "PVTI_1",
                                    "project": {"id": PROJECT},
                                    "fieldValueByName": {
                                        "name": "Building",
                                        "optionId": "ba3c85dd",
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
        result = await resolver(
            client=GitHubClient(http, "token"),
            normalizer=DeliveryNormalizer(_identity()),
            unresolved=unresolved,
            raw_body=raw,
            delivery_time_us=2_000_000_000_000_000,
        )
    (event,) = result.events
    assert isinstance(event.body, ev.ApprovePlan)
    assert event.issue_number == 12
    assert event.provenance == Provenance.RECOVERY
    assert event.evidence is not None and event.evidence.stage == Stage.BUILDING


@pytest.mark.asyncio
async def test_B2_wrong_repository_resolution_produces_no_events():
    from omnigent_factory.github import webhook

    resolver = getattr(webhook, "resolve_project_delivery", None)
    assert callable(resolver)
    data = project_drag()
    raw = json.dumps(data, separators=(",", ":")).encode()
    unresolved = normalize(data, "projects_v2_item")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "__typename": "Issue",
                        "id": "I_1",
                        "number": 12,
                        "repository": {
                            "id": "R_wrong",
                            "databaseId": 1,
                            "nameWithOwner": "other/repo",
                        },
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await resolver(
            client=GitHubClient(http, "token"),
            normalizer=DeliveryNormalizer(_identity()),
            unresolved=unresolved,
            raw_body=raw,
            delivery_time_us=2_000_000_000_000_000,
        )
    assert result.events == ()


@pytest.mark.asyncio
async def test_B2_unknown_content_resolution_produces_no_events():
    from omnigent_factory.github import webhook

    data = project_drag()
    raw = json.dumps(data, separators=(",", ":")).encode()
    unresolved = normalize(data, "projects_v2_item")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"node": None}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await webhook.resolve_project_delivery(
            client=GitHubClient(http, "token"),
            normalizer=DeliveryNormalizer(_identity()),
            unresolved=unresolved,
            raw_body=raw,
            delivery_time_us=2_000_000_000_000_000,
        )
    assert result.events == ()


@pytest.mark.asyncio
async def test_B3_reducer_shaped_set_bot_uses_no_source_cas():
    harness = Harness()
    harness.triage()
    [set_bot] = [
        effect
        for _, result in harness.log
        for effect in result.effects
        if effect.kind == EffectKind.SET_BOT and effect.args == {"bot": "Working"}
    ]
    assert set_bot.args == {"bot": "Working"}
    reads = iter(["bot-idle", "bot-working"])
    mutations = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if "updateProjectV2ItemFieldValue" in body["query"]:
            mutations.append(body["variables"]["input"])
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
                        "fieldValueByName": {"optionId": next(reads), "field": {"id": "BOT_FIELD"}}
                    }
                }
            },
        )

    schema = BoardSchema(
        STATUS_FIELD,
        {"Inbox": "915abb46"},
        "BOT_FIELD",
        {"Idle": "bot-idle", "Working": "bot-working"},
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        outcome = await adapter(
            http,
            parcel_bindings={"I_parcel_1": ParcelBinding(1, "PVTI_1")},
            board_schema=schema,
        ).execute(set_bot, CTX)
    assert outcome.remote_id == "PVTI_1"
    assert mutations


@pytest.mark.asyncio
async def test_B3_reducer_shaped_move_without_source_uses_no_source_cas():
    harness = Harness()
    harness.eligible()
    result = harness.send("I_parcel_1", ev.RequestTriage())
    [move] = Harness.of(result, EffectKind.MOVE_CARD)
    assert move.args == {"to": "Triaged", "expected_from": None}
    reads = iter(["915abb46", "triaged-id"])

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if "updateProjectV2ItemFieldValue" in body["query"]:
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
                        "fieldValueByName": {"optionId": next(reads), "field": {"id": STATUS_FIELD}}
                    }
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        outcome = await adapter(
            http,
            parcel_bindings={"I_parcel_1": ParcelBinding(1, "PVTI_1")},
            board_schema=BoardSchema(
                STATUS_FIELD,
                {"Inbox": "915abb46", "Triaged": "triaged-id"},
                "BOT_FIELD",
                {},
            ),
        ).execute(move, CTX)
    assert outcome.remote_id == "PVTI_1"


def test_B4_only_configured_opposite_vendor_review_is_accepted():
    reviews = [
        {"id": 1, "state": "APPROVED", "commit_id": "head", "user": {"id": 114979}},
        {"id": 2, "state": "APPROVED", "commit_id": "head", "user": {"id": 55}},
    ]
    github = _bare_adapter(independent_reviewer_ids=frozenset({99}), owner_ids=frozenset({114979}))
    assert github._review_accepted(reviews, "head") is False
    reviews.append({"id": 3, "state": "APPROVED", "commit_id": "head", "user": {"id": 99}})
    assert github._review_accepted(reviews, "head") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"x-ratelimit-remaining": "1", "retry-after": "17"},
        {
            "x-ratelimit-remaining": "1",
            "retry-after": email.utils.format_datetime(
                datetime.fromtimestamp(2_000_000_030, UTC), usegmt=True
            ),
        },
        {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "2000000040"},
    ],
)
async def test_S1_all_rate_limit_headers_produce_typed_retry(headers):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, headers=headers, json={"message": "slow down"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(RateLimited) as raised:
            await GitHubClient(http, "token", now=lambda: 2_000_000_000).get_json("/items")
    assert raised.value.retry_after_us > 0


@pytest.mark.asyncio
async def test_S1_secondary_limit_on_write_is_retryable_not_definitive():
    from .test_adapter import effect

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        return httpx.Response(
            403,
            headers={"x-ratelimit-remaining": "1", "retry-after": "17"},
            json={"message": "secondary rate limit"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        outcome = await adapter(http).execute(
            effect(EffectKind.POST_COMMENT, issue_number=12, body="done"), CTX
        )
    assert outcome.retry_after_us == 17_000_000


@pytest.mark.asyncio
async def test_S2_project_replay_new_delivery_guid_has_same_logical_id():
    data = project_drag()
    ids = {
        (await resolve_drag(data, guid=guid)).events[0].event_id for guid in ("guid-1", "guid-2")
    }
    assert len(ids) == 1


@pytest.mark.asyncio
async def test_S3_missing_project_field_identity_fails_closed():
    data = project_drag()
    del data["changes"]["field_value"]["field_node_id"]
    assert (await resolve_drag(data)).events == ()


@pytest.mark.asyncio
async def test_S4_comment_success_with_malformed_response_is_ambiguous():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=[])
        return httpx.Response(201, content=b"not-json")

    from .test_adapter import effect

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        outcome = await adapter(http).execute(
            effect(EffectKind.POST_COMMENT, issue_number=12, body="done"), CTX
        )
    assert isinstance(outcome, AmbiguousWrite)


@pytest.mark.asyncio
async def test_S4_ghost_comment_author_does_not_crash_pr_snapshot():
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
            return httpx.Response(200, json={"check_runs": []})
        if path.endswith("/reviews"):
            return httpx.Response(200, json=[])
        if path.endswith("/comments"):
            return httpx.Response(200, json=[{"id": 1, "user": None, "body": "old"}])
        if path.endswith("/status"):
            return httpx.Response(200, json={"state": "pending", "statuses": []})
        if path == "/graphql":
            if "reviewThreads" in json.loads(request.content)["query"]:
                return no_threads()
            return closing_refs("I_1")
        raise AssertionError(path)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await adapter(http).pull_request(PARCEL_REF, 7)
    assert result.pr_number == 7


def test_S4_malformed_review_number_is_ignored():
    data = payload(
        action="submitted",
        review={"state": "approved"},
        pull_request={"number": "not-an-int", "head": {"sha": "abc"}},
    )
    assert normalize(data, "pull_request_review").events == ()


def test_S6_pr_thread_comment_is_not_issue_control():
    issue = {"id": 10, "node_id": "PR_1", "number": 12, "pull_request": {"url": "x"}}
    assert normalize(payload(issue=issue)).events == ()


@pytest.mark.parametrize("signature", [None, "sha1=bad", "sha256=" + "0" * 64])
def test_S7_authenticated_entrypoint_rejects_signature_before_json(signature):
    with pytest.raises(SignatureError):
        DeliveryNormalizer(_identity()).authenticate_and_normalize(
            secret=b"secret",
            signature=signature,
            raw_body=b"not-json",
            event_name="issues",
            delivery_guid="g",
            delivery_time_us=1,
        )


def test_S7_non_owner_and_bot_labels_do_not_control():
    for actor in (222, BOT_ID):
        data = payload(action="labeled", sender={"id": actor}, label={"name": "factory:build"})
        assert normalize(data, "issues").events == ()


def test_S7_manifest_is_exact_allowlist():
    manifest = render_app_manifest()
    assert manifest["default_permissions"] == {
        "contents": "write",
        "issues": "write",
        "pull_requests": "write",
        "checks": "read",
        "statuses": "read",
        "actions": "write",
        "metadata": "read",
        "workflows": "write",
        "organization_projects": "write",
    }
    assert manifest["default_events"] == [
        "issues",
        "issue_comment",
        "pull_request",
        "pull_request_review",
        "pull_request_review_thread",
        "check_suite",
        "workflow_run",
        "projects_v2_item",
    ]


def test_advisory_bot_issue_edit_is_not_waiver_edit():
    assert normalize(payload(action="edited", sender={"id": BOT_ID}), "issues").events == ()
    assert (
        normalize(payload(action="edited", sender={"id": 222, "type": "Bot"}), "issues").events
        == ()
    )


@pytest.mark.asyncio
async def test_advisory_link_pagination_never_sends_token_off_host():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "evil.example":
            return httpx.Response(200, json=[])
        return httpx.Response(
            200,
            json=[],
            headers={"Link": '<https://evil.example/items?page=2>; rel="next"'},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(GitHubAPIError):
            await GitHubClient(http, "secret").paginate("/items")
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_advisory_daemon_token_is_not_labelled_read_only():
    def handler(request: httpx.Request) -> httpx.Response:
        requested = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "token": "opaque",
                "expires_at": "2030-01-01T00:00:00Z",
                "permissions": requested["permissions"],
                "repositories": [{"full_name": REPOSITORY}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        grant = await InstallationTokenService(
            http,
            AppAuthenticator(1, private_key()),
            99,
            REPOSITORY,
        ).mint_daemon()
    assert not hasattr(grant, "profile")
    assert grant.permissions["organization_projects"] == "write"
    assert "workflows" not in grant.permissions  # only BUILD stage tokens write workflows
    assert grant.permissions["actions"] == "read"  # only BUILD stage tokens re-run CI


def _identity() -> DeliveryIdentity:
    return DeliveryIdentity(
        app_id=900,
        installation_id=901,
        organization_id=296340858,
        project_node_id=PROJECT,
        status_field_node_id=STATUS_FIELD,
        repository_id=1278047766,
        repository_node_id=REPO_NODE,
        repository_full_name=REPOSITORY,
        owner_ids=frozenset({114979}),
        bot_user_id=BOT_ID,
        automation_user_ids=frozenset({888}),
    )


def _bare_adapter(**kwargs) -> GitHubAPIAdapter:
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(500)))
    return GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository=REPOSITORY,
        repository_node_id=REPO_NODE,
        project_node_id=PROJECT,
        status_field_node_id=STATUS_FIELD,
        bot_user_id=BOT_ID,
        required_checks=frozenset(),
        **kwargs,
    )
