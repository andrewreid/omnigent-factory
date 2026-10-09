"""Pure owner-setup payload rendering. Nothing in this module applies changes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from omnigent_factory.core.types import Stage
from omnigent_factory.ports.github import DEFAULT_STATUS_NAMES, STATUS_OPTION_IDS

OWNER_ID = 114979
PROJECT_NODE_ID = "PVT_kwDOEanNes4BkJhb"
STATUS_FIELD_NODE_ID = "PVTSSF_lADOEanNes4BkJhbzhi7I9w"
STATUS_FIELD_DATABASE_ID = 414917596

#: Status option color and description per stage (the name comes from ``status_names``).
_STATUS_STYLE: Mapping[Stage, tuple[str, str]] = {
    Stage.INBOX: ("GRAY", "Not started"),
    Stage.TRIAGED: ("BLUE", "Triage requested or complete"),
    Stage.SCOPED: ("PURPLE", "Plan requested or awaiting approval"),
    Stage.BUILDING: ("YELLOW", "Build queued or underway"),
    Stage.READY: ("GREEN", "Ready for owner review and merge"),
    Stage.DONE: ("GREEN", "Merged / closed"),
}

REQUIRED_CHECKS = (
    "api / Lint / Typecheck / Test",
    "api / Dependency audit",
    "web / Typecheck, test, lint",
    "web / Build",
    "api / Image target manifest",
    "api / DB-tier Test",
)


@dataclass(frozen=True, slots=True)
class SetupBundle:
    app_manifest: dict[str, Any]
    project_migration: dict[str, Any]
    ruleset: dict[str, Any]


def render_app_manifest(
    *, webhook_url: str = "https://factory.reid.ee/webhooks/github"
) -> dict[str, Any]:
    return {
        "name": "Reid Factory",
        "url": "https://github.com/andrewreid/omnigent-factory",
        "description": "Owner-controlled issue stages and Omnigent sessions",
        "public": False,
        "hook_attributes": {"url": webhook_url, "active": True},
        "request_oauth_on_install": False,
        "default_permissions": {
            "contents": "write",
            "issues": "write",
            "pull_requests": "write",
            "checks": "read",
            "statuses": "read",
            "actions": "write",
            "metadata": "read",
            "workflows": "write",
            "organization_projects": "write",
        },
        "default_events": [
            "issues",
            "issue_comment",
            "pull_request",
            "pull_request_review",
            "pull_request_review_thread",
            "check_suite",
            "workflow_run",
            "projects_v2_item",
            "push",
        ],
    }


def _option(
    name: str, color: str, description: str, option_id: str | None = None
) -> dict[str, str]:
    result = {"name": name, "color": color, "description": description}
    if option_id is not None:
        result["id"] = option_id
    return result


def render_project_migration(
    *,
    created_field_database_ids: dict[str, int] | None = None,
    status_names: Mapping[Stage, str] = DEFAULT_STATUS_NAMES,
) -> dict[str, Any]:
    """Render replace-all Status and additive field/view operations.

    View creation is withheld until GitHub returns the new fields' integer IDs; this is
    represented as a prerequisite, never an invented ID. Status option names come from
    ``status_names`` (host config); their IDs are always preserved.
    """
    status_options = [
        _option(status_names[stage], *_STATUS_STYLE[stage], option_id)
        for stage, option_id in STATUS_OPTION_IDS.items()
    ]
    fields: list[dict[str, Any]] = [
        {
            "name": "Bot",
            "dataType": "SINGLE_SELECT",
            "singleSelectOptions": [
                _option("Working", "BLUE", "Agent work admitted"),
                _option("Needs you", "YELLOW", "Owner decision or review needed"),
                _option("Checkpoint", "ORANGE", "Time block awaits continuation"),
                _option("Blocked", "RED", "Cannot safely progress"),
                _option("Queued", "PURPLE", "Approved; waiting for build capacity"),
                _option("Idle", "GRAY", "No admitted execution"),
            ],
        },
        {"name": "Factory note", "dataType": "TEXT"},
        {
            "name": "Priority",
            "dataType": "SINGLE_SELECT",
            "singleSelectOptions": [
                _option("P0", "RED", "Highest"),
                _option("P1", "ORANGE", "High"),
                _option("P2", "YELLOW", "Normal"),
                _option("P3", "GRAY", "Low"),
            ],
        },
        {
            "name": "Size",
            "dataType": "SINGLE_SELECT",
            "singleSelectOptions": [
                _option("S", "GREEN", "Molly SMALL"),
                _option("M", "YELLOW", "Molly STANDARD"),
                _option("L", "RED", "Molly HIGH-RISK"),
            ],
        },
        # Triage ranking order (1 = next); record its node ID as rank_field_node_id.
        {"name": "Rank", "dataType": "NUMBER"},
        # Owner approval of the posted plan for auto-build; record its node ID as
        # auto_build_field_node_id and the option IDs as auto_build_options.
        {
            "name": "Auto-build",
            "dataType": "SINGLE_SELECT",
            "singleSelectOptions": [
                _option("Queued", "PURPLE", "Owner: build the posted plan when a slot is free"),
                _option("Started", "BLUE", "Factory: the auto-build was started"),
            ],
        },
    ]
    create_fields = [{"input": {"projectId": PROJECT_NODE_ID, **field}} for field in fields]
    views: list[dict[str, Any]] = []
    prerequisites: list[str] = [
        "pause factory",
        "snapshot fields, options, views, workflows and item values",
        "re-read identities and option IDs immediately before apply",
    ]
    if created_field_database_ids is None:
        prerequisites.append(
            "supply returned Bot/Priority/Size integer field IDs before rendering views"
        )
    else:
        if set(created_field_database_ids) != {"Bot", "Priority", "Size"} or any(
            not isinstance(value, int) or value <= 0
            for value in created_field_database_ids.values()
        ):
            raise ValueError(
                "created field IDs must contain positive integer Bot/Priority/Size ids"
            )
        visible = [
            414917594,
            STATUS_FIELD_DATABASE_ID,
            created_field_database_ids["Bot"],
            created_field_database_ids["Priority"],
            created_field_database_ids["Size"],
        ]
        views = [
            {
                "name": "Factory",
                "layout": "board",
                "filter": "is:open",
                "vertical_group_by": [STATUS_FIELD_DATABASE_ID],
                "visible_fields": visible,
            },
            {
                "name": "Needs you",
                "layout": "table",
                "filter": 'is:open Bot:"Needs you","Checkpoint","Blocked"',
                "visible_fields": visible,
            },
        ]
    return {
        "project_id": PROJECT_NODE_ID,
        "prerequisites": prerequisites,
        "update_status": {
            "input": {
                "fieldId": STATUS_FIELD_NODE_ID,
                "name": "Status",
                "singleSelectOptions": status_options,
            }
        },
        "create_fields": create_fields,
        "create_views": views,
        "post_apply_verification": [
            "persist every returned node and database id (Rank: rank_field_node_id; "
            "Auto-build: auto_build_field_node_id and auto_build_options Queued/Started)",
            "verify every preserved option id and existing item value",
            f"keep item-added to {status_names[Stage.INBOX]} and disable PR-driven moves",
            "retire Agent/Audit and old views only after new views verify",
        ],
    }


def render_ruleset() -> dict[str, Any]:
    """Render the owner-approved native main ruleset (no custom human gate)."""
    return {
        "name": "Factory main",
        "target": "branch",
        "enforcement": "active",
        "bypass_actors": [
            {"actor_id": OWNER_ID, "actor_type": "User", "bypass_mode": "pull_request"}
        ],
        "conditions": {"ref_name": {"include": ["refs/heads/main"], "exclude": []}},
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {
                "type": "pull_request",
                "parameters": {
                    "allowed_merge_methods": ["merge", "squash", "rebase"],
                    "dismiss_stale_reviews_on_push": True,
                    "require_code_owner_review": True,
                    "require_last_push_approval": True,
                    "required_approving_review_count": 1,
                    "required_review_thread_resolution": True,
                },
            },
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": True,
                    "do_not_enforce_on_create": False,
                    "required_status_checks": [
                        {"context": context, "integration_id": 15368} for context in REQUIRED_CHECKS
                    ],
                },
            },
        ],
    }


def render_setup(
    *,
    created_field_database_ids: dict[str, int] | None = None,
    status_names: Mapping[Stage, str] = DEFAULT_STATUS_NAMES,
) -> SetupBundle:
    bundle = SetupBundle(
        render_app_manifest(),
        render_project_migration(
            created_field_database_ids=created_field_database_ids, status_names=status_names
        ),
        render_ruleset(),
    )
    validate_setup(bundle)
    return bundle


def validate_setup(bundle: SetupBundle) -> None:
    permissions = bundle.app_manifest.get("default_permissions")
    if not isinstance(permissions, dict) or any(
        key in permissions for key in ("administration", "members", "secrets")
    ):
        raise ValueError("App manifest exceeds the approved permission boundary")
    options = bundle.project_migration["update_status"]["input"]["singleSelectOptions"]
    preserved = [option.get("id") for option in options if "id" in option]
    if len(options) != len(preserved) or sorted(preserved) != sorted(STATUS_OPTION_IDS.values()):
        raise ValueError("Status migration does not preserve existing option IDs")
    bypass = bundle.ruleset.get("bypass_actors")
    if bypass != [{"actor_id": OWNER_ID, "actor_type": "User", "bypass_mode": "pull_request"}]:
        raise ValueError("ruleset bypass must be owner-only")
    serialized = str(bundle.ruleset)
    if "human-review-gate" in serialized:
        raise ValueError("dropped human-review-gate must not appear in setup")
