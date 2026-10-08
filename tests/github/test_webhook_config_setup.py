from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import Stage
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.github.setup import (
    REQUIRED_CHECKS,
    SetupBundle,
    render_setup,
    validate_setup,
)
from omnigent_factory.github.webhook import (
    DeliveryIdentity,
    DeliveryNormalizer,
    IdentityError,
    SignatureError,
    resolve_project_delivery,
    verify_signature,
)
from omnigent_factory.ports.github import STATUS_OPTION_IDS

OPTION_IDS = {stage.value: option_id for stage, option_id in STATUS_OPTION_IDS.items()}

IDENTITY = DeliveryIdentity(
    app_id=900,
    installation_id=901,
    organization_id=296340858,
    project_node_id="PVT_kwDOEanNes4BkJhb",
    status_field_node_id="PVTSSF_lADOEanNes4BkJhbzhi7I9w",
    repository_id=1278047766,
    repository_node_id="R_kgDOTC12Fg",
    repository_full_name="SA-Ambulance/timesheets",
    owner_ids=frozenset({114979}),
    bot_user_id=777,
    automation_user_ids=frozenset({888}),
)


def payload(**overrides):
    base = {
        "action": "created",
        "installation": {"id": 901, "app_id": 900},
        "organization": {"id": 296340858},
        "repository": {
            "id": 1278047766,
            "node_id": "R_kgDOTC12Fg",
            "full_name": "SA-Ambulance/timesheets",
        },
        "sender": {"id": 114979},
        "issue": {"id": 10, "node_id": "I_1", "number": 12},
        "comment": {
            "id": 44,
            "body": "/plan",
            "created_at": "2026-09-25T01:02:03Z",
        },
    }
    return {**base, **overrides}


def normalize(data, event="issue_comment", provenance=Provenance.WEBHOOK):
    return DeliveryNormalizer(IDENTITY).normalize(
        raw_body=json.dumps(data, separators=(",", ":")).encode(),
        event_name=event,
        delivery_guid="delivery-1",
        delivery_time_us=2_000_000_000_000_000,
        provenance=provenance,
    )


def test_signature_is_over_exact_raw_bytes():
    secret = b"secret"
    raw = b'{"body":"line\\r\\n"}'
    signature = "sha256=" + hmac.new(secret, raw, hashlib.sha256).hexdigest()
    verify_signature(secret, raw, signature)
    with pytest.raises(SignatureError):
        verify_signature(secret, raw.replace(b"\\r", b""), signature)


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ({"installation": {"id": 999, "app_id": 900}}, "installation"),
        ({"organization": {"id": 1}}, "organization"),
        (
            {
                "repository": {
                    "id": 1,
                    "node_id": "wrong",
                    "full_name": "SA-Ambulance/timesheets",
                }
            },
            "repository",
        ),
        ({"sender": {}}, "sender"),
    ],
)
def test_routing_identity_negatives_fail_closed(replacement, message):
    with pytest.raises(IdentityError, match=message):
        normalize(payload(**replacement))


def test_project_event_without_repository_remains_unresolved():
    data = payload(
        action="edited",
        projects_v2_item={
            "node_id": "PVTI_1",
            "project_node_id": IDENTITY.project_node_id,
            "content_node_id": "I_1",
        },
    )
    data.pop("repository")
    result = normalize(data, "projects_v2_item")
    assert result.events == ()
    assert result.unresolved_content_node_id == "I_1"


def test_wrong_project_without_repository_is_rejected():
    data = payload(
        projects_v2_item={"node_id": "PVTI_1", "project_node_id": "wrong", "content_node_id": "I_1"}
    )
    data.pop("repository")
    with pytest.raises(IdentityError, match="project"):
        normalize(data, "projects_v2_item")


def test_edited_comment_never_becomes_control():
    assert normalize(payload(action="edited")).events == ()


def test_delayed_feedback_preserves_original_time_and_recovery_provenance():
    data = payload(
        comment={
            "id": 50,
            "body": "Please keep the API stable",
            "created_at": "2020-01-01T00:00:00Z",
        }
    )
    (event,) = normalize(data, provenance=Provenance.RECOVERY).events
    assert isinstance(event.body, ev.PlanFeedback)
    assert event.source_time_us == 1_577_836_800_000_000
    assert event.provenance == Provenance.RECOVERY
    assert event.event_id == "github:comment:50:created"


def test_non_owner_and_bot_comments_cannot_control():
    assert normalize(payload(sender={"id": 222})).events == ()
    assert normalize(payload(sender={"id": IDENTITY.bot_user_id})).events == ()


def project_drag(actor=114979, old="Scoped", new="Building"):
    result = payload(
        action="edited",
        sender={"id": actor},
        projects_v2_item={
            "node_id": "PVTI_1",
            "project_node_id": IDENTITY.project_node_id,
            "content_node_id": "I_1",
            "updated_at": "2021-02-03T04:05:06Z",
        },
        changes={
            "field_value": {
                "field_node_id": IDENTITY.status_field_node_id,
                "from": {"id": OPTION_IDS.get(old, f"unknown-{old}"), "name": old},
                "to": {"id": OPTION_IDS.get(new, f"unknown-{new}"), "name": new},
            }
        },
    )
    result.pop("repository")
    return result


async def resolve_drag(data, *, guid="delivery-1"):
    raw = json.dumps(data, separators=(",", ":")).encode()
    unresolved = DeliveryNormalizer(IDENTITY).normalize(
        raw_body=raw,
        event_name="projects_v2_item",
        delivery_guid=guid,
        delivery_time_us=2_000_000_000_000_000,
    )
    target = data["changes"]["field_value"].get("to", {}).get("name", "Building")

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
                            "id": IDENTITY.repository_node_id,
                            "databaseId": IDENTITY.repository_id,
                            "nameWithOwner": IDENTITY.repository_full_name,
                        },
                        "projectItems": {
                            "nodes": [
                                {
                                    "id": "PVTI_1",
                                    "project": {"id": IDENTITY.project_node_id},
                                    "fieldValueByName": {
                                        "name": target,
                                        "optionId": OPTION_IDS.get(target, "unknown-option"),
                                        "field": {"id": IDENTITY.status_field_node_id},
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
        return await resolve_project_delivery(
            client=GitHubClient(http, "token"),
            normalizer=DeliveryNormalizer(IDENTITY),
            unresolved=unresolved,
            raw_body=raw,
            delivery_time_us=2_000_000_000_000_000,
        )


@pytest.mark.asyncio
async def test_delayed_owner_drag_maps_to_approval_with_content_identity():
    (event,) = (await resolve_drag(project_drag())).events
    assert isinstance(event.body, ev.ApprovePlan)
    assert event.body.via.value == "drag"
    assert event.parcel_id == "I_1"
    assert event.source_time_us == 1_612_325_106_000_000


@pytest.mark.asyncio
async def test_any_actor_leftward_drag_is_safety_but_automation_rightward_is_observation():
    (left,) = (await resolve_drag(project_drag(actor=222, old="Building", new="Scoped"))).events
    assert isinstance(left.body, ev.LeftwardMove)
    assert left.body.from_stage == Stage.BUILDING
    (right,) = (await resolve_drag(project_drag(actor=888, old="Inbox", new="Triaged"))).events
    assert isinstance(right.body, ev.ColumnObserved)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("old", "new", "body_type"),
    [
        ("Inbox", "Triaged", ev.RequestTriage),
        ("Inbox", "Scoped", ev.RequestPlan),
        ("Triaged", "Building", ev.WaivePlan),
        ("Scoped", "Building", ev.ApprovePlan),
    ],
)
async def test_owner_drag_carries_its_own_source_column(old, new, body_type):
    """#729: the reducer judges a drag by its own columns, not by a read that got there."""
    (event,) = (await resolve_drag(project_drag(old=old, new=new))).events
    assert isinstance(event.body, body_type)
    assert event.body.board_from == Stage(old)


def test_setup_payloads_preserve_ids_and_render_native_review_rule():
    bundle = render_setup(created_field_database_ids={"Bot": 1, "Priority": 2, "Size": 3})
    options = bundle.project_migration["update_status"]["input"]["singleSelectOptions"]
    assert {option["name"]: option.get("id") for option in options} == {
        "Inbox": "915abb46",
        "Triaged": "43889573",
        "Scoped": "3a7f779a",
        "Building": "ba3c85dd",
        "Ready": "6df89cbb",
        "Done": "4980e49d",
    }
    pull_request_rule = bundle.ruleset["rules"][2]["parameters"]
    assert pull_request_rule["required_approving_review_count"] == 1
    assert pull_request_rule["dismiss_stale_reviews_on_push"] is True
    assert pull_request_rule["require_last_push_approval"] is True
    assert bundle.ruleset["bypass_actors"] == [
        {"actor_id": 114979, "actor_type": "User", "bypass_mode": "pull_request"}
    ]
    checks = bundle.ruleset["rules"][3]["parameters"]["required_status_checks"]
    assert [check["context"] for check in checks] == list(REQUIRED_CHECKS)
    assert "human-review-gate" not in str(bundle)
    assert "administration" not in bundle.app_manifest["default_permissions"]


def test_app_manifest_allows_workflows_but_not_admin_members_or_secrets():
    """Owner decision 2026-10-01 (#694): the App may hold workflows:write, nothing broader."""
    bundle = render_setup()
    permissions = bundle.app_manifest["default_permissions"]
    assert permissions["workflows"] == "write"
    validate_setup(bundle)
    for forbidden in ("administration", "members", "secrets"):
        manifest = {
            **bundle.app_manifest,
            "default_permissions": {**permissions, forbidden: "write"},
        }
        widened = SetupBundle(manifest, bundle.project_migration, bundle.ruleset)
        with pytest.raises(ValueError, match="approved permission boundary"):
            validate_setup(widened)
