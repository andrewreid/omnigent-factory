"""Raw-byte webhook authentication and fail-closed event normalization.

Task-4/5 wiring must persist a repository-less ``projects_v2_item`` delivery first, then
call :func:`resolve_project_delivery` with the authenticated App client and that exact
raw body.  The resolver verifies the content node's repository and current project/Status
before emitting events; unknown or wrong-repository content remains a no-event delivery.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import MICROS_PER_HOUR, IssueSnapshot, Stage, Via, is_leftward
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.ports.github import STATUS_OPTION_IDS, stage_for_option

_COMMAND = re.compile(r"^/(?P<name>[a-z-]+)(?:\s+(?P<args>.*))?$", re.DOTALL)
_DURATION = re.compile(r"(?:^|\s)for\s+(?P<hours>\d+)h(?:\s|$)", re.IGNORECASE)


#: Owner labels that are commands (any other label is no transition). The board diff's
#: label recovery (``service.label_recovery``) maps a lost label webhook the same way.
LABEL_CONTROLS: Mapping[str, ev.EventBody] = MappingProxyType(
    {
        "factory:triage": ev.RequestTriage(via=Via.LABEL),
        "factory:plan": ev.RequestPlan(via=Via.LABEL),
        "factory:build": ev.WaivePlan(via=Via.LABEL),
    }
)


def label_name(payload: Mapping[str, Any]) -> str | None:
    """The label an ``issues`` labeled/unlabeled payload names."""
    label = payload.get("label")
    name = label.get("name") if isinstance(label, dict) else None
    return name if isinstance(name, str) else None


class WebhookError(ValueError):
    """A delivery cannot be trusted or safely interpreted."""


class SignatureError(WebhookError):
    pass


class IdentityError(WebhookError):
    pass


def verify_signature(secret: bytes, raw_body: bytes, signature: str | None) -> None:
    """Verify GitHub's sha256 signature over the untouched request bytes."""
    if not signature or not signature.startswith("sha256="):
        raise SignatureError("missing or unsupported webhook signature")
    supplied = signature.removeprefix("sha256=")
    expected = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
    if len(supplied) != len(expected) or not hmac.compare_digest(supplied, expected):
        raise SignatureError("webhook signature mismatch")


@dataclass(frozen=True, slots=True)
class DeliveryIdentity:
    app_id: int
    installation_id: int
    organization_id: int
    project_node_id: str
    status_field_node_id: str
    repository_id: int
    repository_node_id: str
    repository_full_name: str
    owner_ids: frozenset[int]
    bot_user_id: int
    automation_user_ids: frozenset[int] = frozenset()
    #: Status option IDs per stage; columns are identified by option ID, never by name.
    status_option_ids: Mapping[Stage, str] = STATUS_OPTION_IDS
    #: The "Auto-build" single-select field and its option IDs by name ("" = none).
    auto_build_field_node_id: str = ""
    auto_build_option_ids: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class NormalizedDelivery:
    delivery_guid: str
    event_name: str
    body_sha256: str
    events: tuple[Event, ...]
    unresolved_content_node_id: str | None = None
    ignored_reason: str | None = None
    provenance: Provenance = Provenance.WEBHOOK
    #: Set only when an authenticated lookup *proved* the project card is not a
    #: timesheets issue (draft issue, pull request, or an issue in another repository).
    #: Missing, failed or inconsistent data never sets it.
    foreign_content: bool = False


def _timestamp_us(value: object, fallback: int) -> int:
    if not isinstance(value, str):
        return fallback
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1e6)
    except ValueError:
        return fallback


def _duration(args: str) -> int | None:
    match = _DURATION.search(args)
    return None if match is None else int(match.group("hours")) * MICROS_PER_HOUR


class DeliveryNormalizer:
    """Validate routing identity and map GitHub payloads to Task-1 event types."""

    def __init__(self, identity: DeliveryIdentity) -> None:
        self.identity = identity

    def authenticate_and_normalize(
        self,
        *,
        secret: bytes,
        signature: str | None,
        raw_body: bytes,
        event_name: str,
        delivery_guid: str,
        delivery_time_us: int,
        evidence: IssueSnapshot | None = None,
    ) -> NormalizedDelivery:
        verify_signature(secret, raw_body, signature)
        return self.normalize(
            raw_body=raw_body,
            event_name=event_name,
            delivery_guid=delivery_guid,
            delivery_time_us=delivery_time_us,
            evidence=evidence,
        )

    def normalize(
        self,
        *,
        raw_body: bytes,
        event_name: str,
        delivery_guid: str,
        delivery_time_us: int,
        provenance: Provenance = Provenance.WEBHOOK,
        evidence: IssueSnapshot | None = None,
    ) -> NormalizedDelivery:
        if provenance not in {Provenance.WEBHOOK, Provenance.RECOVERY}:
            raise WebhookError("GitHub delivery provenance must be webhook or recovery")
        try:
            payload: Any = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WebhookError("webhook body is not valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise WebhookError("webhook body must be an object")
        actor_id = self._validate_common_identity(payload)
        repo = payload.get("repository")
        if repo is None and event_name == "projects_v2_item":
            return self._normalize_project_without_repo(
                payload=payload,
                delivery_guid=delivery_guid,
                event_name=event_name,
                raw_body=raw_body,
                actor_id=actor_id,
                provenance=provenance,
            )
        self._validate_repository(repo)
        issue = payload.get("issue")
        issue_number = issue.get("number") if isinstance(issue, dict) else None
        parcel_id = issue.get("node_id") if isinstance(issue, dict) else None
        if isinstance(issue, dict) and "pull_request" in issue:
            # A PR conversation comment: the parcel is resolved from the PR number.
            issue_number = parcel_id = None
        if event_name == "projects_v2_item":
            item = payload.get("projects_v2_item")
            if isinstance(item, dict):
                parcel_id = item.get("content_node_id")
        source_time_us = self._source_time(payload, event_name, delivery_time_us)
        bodies = self._bodies(event_name, payload, actor_id)
        events = tuple(
            Event(
                event_id=self._logical_id(event_name, payload, delivery_guid, body),
                repo_id=self.identity.repository_node_id,
                parcel_id=parcel_id if isinstance(parcel_id, str) else None,
                source_time_us=source_time_us,
                provenance=provenance,
                body=body,
                actor_id=actor_id,
                issue_number=issue_number if isinstance(issue_number, int) else None,
                entropy=hashlib.sha256(f"{delivery_guid}:{index}".encode()).hexdigest(),
                evidence=evidence,
                delivery_guid=delivery_guid,
            )
            for index, body in enumerate(bodies)
        )
        ignored = None if events else "delivery has no trusted state transition"
        return NormalizedDelivery(
            delivery_guid,
            event_name,
            hashlib.sha256(raw_body).hexdigest(),
            events,
            ignored_reason=ignored,
            provenance=provenance,
        )

    def _validate_common_identity(self, payload: dict[str, Any]) -> int:
        installation = payload.get("installation")
        organization = payload.get("organization")
        sender = payload.get("sender")
        if (
            not isinstance(installation, dict)
            or installation.get("id") != self.identity.installation_id
        ):
            raise IdentityError("wrong or missing App installation")
        target_id = installation.get("app_id")
        if target_id is not None and target_id != self.identity.app_id:
            raise IdentityError("wrong GitHub App")
        if (
            not isinstance(organization, dict)
            or organization.get("id") != self.identity.organization_id
        ):
            raise IdentityError("wrong or missing organization")
        actor_id = sender.get("id") if isinstance(sender, dict) else None
        if not isinstance(actor_id, int):
            raise IdentityError("missing numeric sender identity")
        return actor_id

    def _validate_repository(self, repo: object) -> None:
        if not isinstance(repo, dict):
            raise IdentityError("missing repository identity")
        if (
            repo.get("id") != self.identity.repository_id
            or repo.get("node_id") != self.identity.repository_node_id
            or repo.get("full_name") != self.identity.repository_full_name
        ):
            raise IdentityError("wrong repository")

    def _normalize_project_without_repo(
        self,
        *,
        payload: dict[str, Any],
        delivery_guid: str,
        event_name: str,
        raw_body: bytes,
        actor_id: int,
        provenance: Provenance,
    ) -> NormalizedDelivery:
        item = payload.get("projects_v2_item")
        if (
            not isinstance(item, dict)
            or item.get("project_node_id") != self.identity.project_node_id
        ):
            raise IdentityError("wrong or missing project identity")
        content_id = item.get("content_node_id")
        if not isinstance(content_id, str) or not content_id:
            raise IdentityError("project item has no resolvable content identity")
        return NormalizedDelivery(
            delivery_guid,
            event_name,
            hashlib.sha256(raw_body).hexdigest(),
            (),
            unresolved_content_node_id=content_id,
            ignored_reason=f"repository unresolved for actor {actor_id}",
            provenance=provenance,
        )

    def _source_time(self, payload: dict[str, Any], event_name: str, fallback: int) -> int:
        if event_name == "issue_comment":
            comment = payload.get("comment")
            if isinstance(comment, dict):
                return _timestamp_us(comment.get("created_at"), fallback)
        if event_name == "pull_request_review":
            review = payload.get("review")
            if isinstance(review, dict) and review.get("submitted_at"):
                return _timestamp_us(review.get("submitted_at"), fallback)
        for key in ("projects_v2_item", "issue", "pull_request", "review", "check_suite"):
            value = payload.get(key)
            if isinstance(value, dict):
                stamp = value.get("updated_at") or value.get("created_at")
                if stamp is not None:
                    return _timestamp_us(stamp, fallback)
        return fallback

    def _bodies(
        self, event_name: str, payload: dict[str, Any], actor_id: int
    ) -> tuple[ev.EventBody, ...]:
        action = payload.get("action")
        if event_name == "issue_comment":
            if action != "created":
                return ()
            issue = payload.get("issue")
            comment = payload.get("comment")
            text = comment.get("body") if isinstance(comment, dict) else None
            if isinstance(issue, dict) and "pull_request" in issue:
                return self._pr_comment(str(text or ""), actor_id, issue.get("number"))
            return self._comment(str(text or ""), actor_id)
        if event_name == "issues":
            if action in {"assigned", "closed", "deleted", "transferred"}:
                return self._issue_safety(str(action), payload)
            sender = payload.get("sender")
            sender_is_bot = isinstance(sender, dict) and sender.get("type") == "Bot"
            if action == "edited" and actor_id != self.identity.bot_user_id and not sender_is_bot:
                return (ev.WaiverEdited(),)
            if action == "labeled":
                return self._label(payload, actor_id)
            return ()
        if event_name == "projects_v2_item":
            if action == "deleted":
                item = payload.get("projects_v2_item")
                if (
                    not isinstance(item, dict)
                    or item.get("project_node_id") != self.identity.project_node_id
                ):
                    raise IdentityError("wrong or missing project identity")
                return (ev.ItemRemoved(),)
            return self._project_move(payload, actor_id)
        if event_name == "pull_request":
            return self._pull_request(payload)
        if event_name == "pull_request_review":
            return self._owner_review(payload, actor_id) or self._review(payload)
        if event_name in {"check_suite", "workflow_run"}:
            return self._checks(payload, event_name)
        if event_name == "push":
            return self._push(payload)
        return ()

    @staticmethod
    def _push(payload: dict[str, Any]) -> tuple[ev.EventBody, ...]:
        """A push to the repository's default branch (any other ref, or a deletion, is
        no transition)."""
        repo = payload.get("repository")
        default = repo.get("default_branch") if isinstance(repo, dict) else None
        ref = payload.get("ref")
        after = payload.get("after")
        if (
            not isinstance(default, str)
            or not default
            or ref != f"refs/heads/{default}"
            or payload.get("deleted") is True
            or not isinstance(after, str)
        ):
            return ()
        return (ev.BasePushed(ref=ref, head_sha=after),)

    def _comment(self, text: str, actor_id: int) -> tuple[ev.EventBody, ...]:
        if actor_id == self.identity.bot_user_id:
            return ()
        if actor_id not in self.identity.owner_ids:
            return ()
        stripped = text.strip()
        command = _COMMAND.match(stripped)
        if command is None:
            digest = hashlib.sha256(text.encode()).hexdigest()
            return (ev.PlanFeedback(text_digest=digest),)
        name, args = command.group("name"), command.group("args") or ""
        if name == "triage":
            return (ev.RequestTriage(via=Via.COMMAND),)
        if name == "plan":
            return (ev.RequestPlan(via=Via.COMMAND),)
        if name == "replan":
            return (ev.RequestReplan(via=Via.COMMAND),)
        if name == "stop":
            return (ev.Stop(),)
        if name == "continue":
            return (ev.Continue(duration_us=_duration(args)),)
        if name == "approve":
            hash_text = args.split()[0] if args and not args.lower().startswith("for ") else None
            return (
                ev.ApprovePlan(via=Via.COMMAND, hash_text=hash_text, duration_us=_duration(args)),
            )
        if name == "decide":
            decision_id, separator, answer = args.partition(" ")
            return (
                ev.Decide(
                    decision_id=decision_id or None,
                    answer=answer if separator else decision_id,
                ),
            )
        return ()

    def _pr_comment(self, text: str, actor_id: int, number: object) -> tuple[ev.EventBody, ...]:
        """Owner conversation comment on a PR: feedback on the parcel's PR (commands are
        issue-only; a command on a PR is ignored)."""
        if actor_id == self.identity.bot_user_id or actor_id not in self.identity.owner_ids:
            return ()
        if not isinstance(number, int) or _COMMAND.match(text.strip()) is not None:
            return ()
        digest = hashlib.sha256(text.encode()).hexdigest()
        return (ev.PlanFeedback(text_digest=digest, pr_number=number),)

    def _owner_review(self, payload: dict[str, Any], actor_id: int) -> tuple[ev.EventBody, ...]:
        """A submitted owner review that says something: feedback on the parcel's PR.

        Changes-requested and commented reviews always carry text or inline comments (one
        event per review, however many inline comments). An approval counts only with a
        body. Other reviewers' reviews stay observations (``_review``).
        """
        review = payload.get("review")
        pr = payload.get("pull_request")
        if payload.get("action") != "submitted" or not isinstance(review, dict):
            return ()
        if actor_id == self.identity.bot_user_id or actor_id not in self.identity.owner_ids:
            return ()
        number = pr.get("number") if isinstance(pr, dict) else None
        state = str(review.get("state") or "").lower()
        text = str(review.get("body") or "")
        if not isinstance(number, int):
            return ()
        if state not in ("changes_requested", "commented") and not (
            state == "approved" and text.strip()
        ):
            return ()
        digest = hashlib.sha256(f"{review.get('id')}\x1f{text}".encode()).hexdigest()
        return (ev.PlanFeedback(text_digest=digest, pr_number=number),)

    def _issue_safety(self, action: str, payload: dict[str, Any]) -> tuple[ev.EventBody, ...]:
        if action == "assigned":
            assignee = payload.get("assignee")
            if isinstance(assignee, dict) and assignee.get("type") == "Bot":
                return ()
            return (ev.AssignedHuman(),)
        if action == "closed":
            return (ev.Closed(),)
        if action == "deleted":
            return (ev.Deleted(),)
        return (ev.Transferred(),)

    def _label(self, payload: dict[str, Any], actor_id: int) -> tuple[ev.EventBody, ...]:
        if actor_id not in self.identity.owner_ids:
            return ()
        body = LABEL_CONTROLS.get(str(label_name(payload)))
        if body is None:
            return ()
        return (body,)

    def _project_move(self, payload: dict[str, Any], actor_id: int) -> tuple[ev.EventBody, ...]:
        item = payload.get("projects_v2_item")
        if (
            not isinstance(item, dict)
            or item.get("project_node_id") != self.identity.project_node_id
        ):
            raise IdentityError("wrong or missing project identity")
        changes = payload.get("changes")
        field = changes.get("field_value") if isinstance(changes, dict) else None
        if not isinstance(field, dict):
            return ()
        auto_build_field = self.identity.auto_build_field_node_id
        if auto_build_field and field.get("field_node_id") == auto_build_field:
            return self._auto_build_change(field, actor_id)
        if field.get("field_node_id") != self.identity.status_field_node_id:
            return ()
        old = field.get("from")
        new = field.get("to")
        # Single-select changes carry option objects; identify columns by option ID only.
        options = self.identity.status_option_ids
        from_stage = stage_for_option(old.get("id") if isinstance(old, dict) else None, options)
        to_stage = stage_for_option(new.get("id") if isinstance(new, dict) else None, options)
        if actor_id == self.identity.bot_user_id:
            return (ev.ColumnObserved(stage=to_stage),)
        if is_leftward(from_stage, to_stage):
            return (ev.LeftwardMove(from_stage=from_stage, to_stage=to_stage),)
        if actor_id not in self.identity.owner_ids or actor_id in self.identity.automation_user_ids:
            return (ev.ColumnObserved(stage=to_stage),)
        # Each drag carries its own source column: a read that recorded the move first
        # cannot make it look like a no-op (the reducer's ``_drag_origin``).
        if to_stage == Stage.TRIAGED:
            return (ev.RequestTriage(via=Via.DRAG, board_from=from_stage),)
        if to_stage == Stage.SCOPED:
            body: ev.EventBody = (
                ev.RequestReplan(via=Via.DRAG, board_from=from_stage)
                if from_stage == Stage.BUILDING
                else ev.RequestPlan(via=Via.DRAG, board_from=from_stage)
            )
            return (body,)
        if to_stage == Stage.BUILDING:
            if from_stage in {Stage.INBOX, Stage.TRIAGED}:
                return (ev.WaivePlan(via=Via.DRAG, board_from=from_stage),)
            if from_stage == Stage.READY:
                return (ev.RequestRework(),)
            return (ev.ApprovePlan(via=Via.DRAG, board_from=from_stage),)
        return (ev.ColumnObserved(stage=to_stage),)

    def _auto_build_change(
        self, field: Mapping[str, Any], actor_id: int
    ) -> tuple[ev.EventBody, ...]:
        """The "Auto-build" field changed: an owner control only from an owner's own edit.

        The factory's own writes (its bot user) are ignored, as is anyone who is not an
        owner (or is an automation account): a non-owner's Queued approves nothing. The
        option is identified by ID only; an unknown option is ``"?"``.
        """
        if (
            actor_id == self.identity.bot_user_id
            or actor_id not in self.identity.owner_ids
            or actor_id in self.identity.automation_user_ids
        ):
            return ()
        if "to" not in field:
            return ()  # the new value is not in the payload: a read sees it (unconfirmed)
        new = field.get("to")
        option_id = new.get("id") if isinstance(new, dict) else None
        if option_id is None:
            return (ev.AutoBuildMarked(option=""),)
        names = [n for n, o in self.identity.auto_build_option_ids.items() if o == option_id]
        return (ev.AutoBuildMarked(option=names[0] if len(names) == 1 else "?"),)

    def _pull_request(self, payload: dict[str, Any]) -> tuple[ev.EventBody, ...]:
        pr = payload.get("pull_request")
        if not isinstance(pr, dict):
            return ()
        number = payload.get("number")
        head = pr.get("head")
        sha = head.get("sha") if isinstance(head, dict) else ""
        ref = head.get("ref") if isinstance(head, dict) else None
        head_ref = ref if isinstance(ref, str) else ""
        author = pr.get("user")
        return (
            ev.PRObserved(
                pr_number=number if isinstance(number, int) else 0,
                head_sha=sha if isinstance(sha, str) else "",
                open=pr.get("state") == "open",
                merged=bool(pr.get("merged")),
                bot_authored=isinstance(author, dict)
                and author.get("id") == self.identity.bot_user_id,
                parcel_branch=head_ref.startswith("factory/issue-"),
            ),
        )

    @staticmethod
    def _review(payload: dict[str, Any]) -> tuple[ev.EventBody, ...]:
        review = payload.get("review")
        pr = payload.get("pull_request")
        if not isinstance(review, dict) or not isinstance(pr, dict):
            return ()
        number = pr.get("number")
        if not isinstance(number, int):
            return ()
        head = pr.get("head")
        return (
            ev.ReviewChanged(
                pr_number=number,
                head_sha=str(head.get("sha", "")) if isinstance(head, dict) else "",
                changes_requested=review.get("state") == "changes_requested",
            ),
        )

    @staticmethod
    def _checks(payload: dict[str, Any], event_name: str) -> tuple[ev.EventBody, ...]:
        source = payload.get(event_name)
        if not isinstance(source, dict):
            return ()
        prs = source.get("pull_requests")
        if not isinstance(prs, list):
            return ()
        # One suite is only a hint (the reducer re-reads the aggregate): with no or
        # several PRs, the parcel is resolved from the factory head branch (number 0).
        number = prs[0].get("number") if len(prs) == 1 and isinstance(prs[0], dict) else 0
        head_sha = source.get("head_sha")
        conclusion = source.get("conclusion")
        if conclusion == "success":
            state = ev.ChecksState.GREEN
        elif conclusion is None or source.get("status") != "completed":
            state = ev.ChecksState.PENDING
        else:
            state = ev.ChecksState.FAILED
        return (
            ev.ChecksChanged(
                pr_number=number if isinstance(number, int) else 0,
                head_sha=head_sha if isinstance(head_sha, str) else "",
                state=state,
            ),
        )

    @staticmethod
    def _logical_id(
        event_name: str,
        payload: dict[str, Any],
        delivery_guid: str,
        body: ev.EventBody,
    ) -> str:
        if event_name == "issue_comment":
            comment = payload.get("comment")
            if isinstance(comment, dict) and isinstance(comment.get("id"), int):
                return f"github:comment:{comment['id']}:created"
        if event_name == "pull_request_review" and body.KIND == ev.EventKind.PLAN_FEEDBACK:
            review = payload.get("review")
            if isinstance(review, dict) and isinstance(review.get("id"), int):
                return f"github:review:{review['id']}:submitted"
        if event_name == "projects_v2_item":
            item = payload.get("projects_v2_item")
            item_id = item.get("node_id") if isinstance(item, dict) else "unknown"
            changes = payload.get("changes")
            field = changes.get("field_value") if isinstance(changes, dict) else None
            identity = {
                "action": payload.get("action"),
                "item_id": item_id,
                "updated_at": item.get("updated_at") if isinstance(item, dict) else None,
                "field_id": field.get("field_node_id") if isinstance(field, dict) else None,
                "from": field.get("from") if isinstance(field, dict) else None,
                "to": field.get("to") if isinstance(field, dict) else None,
            }
            digest = hashlib.sha256(
                json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            return f"github:project:{item_id}:{digest}"
        return f"github:{event_name}:{body.KIND.value}:{delivery_guid}"


#: Project card content types that can never be a timesheets issue parcel.
_FOREIGN_CONTENT_TYPES = frozenset({"DraftIssue", "PullRequest"})
#: A fully resolved timesheets issue whose delivery changed no field the factory reads.
NO_PROJECT_TRANSITION = "resolved project delivery has no transition"


async def resolve_project_delivery(
    *,
    client: GitHubClient,
    normalizer: DeliveryNormalizer,
    unresolved: NormalizedDelivery,
    raw_body: bytes,
    delivery_time_us: int,
    read_at_us: int | None = None,
) -> NormalizedDelivery:
    """Resolve a persisted repository-less project delivery using authenticated reads.

    Remote read failures propagate as typed :mod:`github.client` errors so the caller can
    retry. Every other no-event outcome never guesses parcel authority: only a node proven
    to be a draft issue, pull request or issue of another repository is marked
    ``foreign_content``; unavailable nodes, inconsistent identities, missing project items
    or changed field identities stay inconclusive and must be set aside by the caller.
    """
    if unresolved.provenance not in {Provenance.WEBHOOK, Provenance.RECOVERY}:
        raise WebhookError("project resolution requires GitHub webhook or recovery provenance")
    content_id = unresolved.unresolved_content_node_id
    if unresolved.event_name != "projects_v2_item" or content_id is None:
        raise WebhookError("delivery is not an unresolved projects_v2_item event")
    if hashlib.sha256(raw_body).hexdigest() != unresolved.body_sha256:
        raise WebhookError("resolution raw body differs from the persisted delivery")
    try:
        payload: Any = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WebhookError("persisted project body is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise WebhookError("persisted project body must be an object")
    actor_id = normalizer._validate_common_identity(payload)
    if "repository" in payload:
        raise WebhookError("project resolution accepts only repository-less deliveries")
    item = payload.get("projects_v2_item")
    if (
        not isinstance(item, dict)
        or item.get("content_node_id") != content_id
        or item.get("project_node_id") != normalizer.identity.project_node_id
    ):
        raise IdentityError("persisted project content identity changed")

    query = """
    query($id: ID!, $after: String) {
      node(id: $id) { __typename id ... on Issue {
        id number title body state
        assignees(first: 100) { nodes { __typename } }
        repository { id databaseId nameWithOwner }
        projectItems(first: 100, after: $after) {
          nodes { id project { id } fieldValueByName(name: "Status") {
            ... on ProjectV2ItemFieldSingleSelectValue {
              name optionId field { ... on ProjectV2SingleSelectField { id } }
            }
          } }
          pageInfo { hasNextPage endCursor }
        }
      } }
    }
    """
    after: str | None = None
    issue: dict[str, Any] | None = None
    status: Stage | None = None
    in_project = False
    while True:
        data = await client.graphql(query, {"id": content_id, "after": after})
        node = data.get("node")
        if not isinstance(node, dict) or node.get("id") != content_id:
            return _unresolved_result(unresolved, "content node is unavailable")
        typename = node.get("__typename")
        if typename in _FOREIGN_CONTENT_TYPES:
            return _unresolved_result(
                unresolved, "content is a draft issue or pull request", foreign=True
            )
        if typename != "Issue":
            return _unresolved_result(unresolved, "content node type is unconfirmed")
        repository = node.get("repository")
        identity = normalizer.identity
        repository_id = repository.get("id") if isinstance(repository, dict) else None
        if isinstance(repository_id, str) and repository_id not in (
            "",
            identity.repository_node_id,
        ):
            return _unresolved_result(
                unresolved, "resolved content belongs to a different repo", foreign=True
            )
        if not isinstance(repository, dict) or (
            repository_id != identity.repository_node_id
            or repository.get("databaseId") != identity.repository_id
            or repository.get("nameWithOwner") != identity.repository_full_name
        ):
            return _unresolved_result(unresolved, "resolved repository identity is inconsistent")
        issue = node
        items = node.get("projectItems")
        if not isinstance(items, dict) or not isinstance(items.get("nodes"), list):
            return _unresolved_result(unresolved, "current project items are unavailable")
        for project_item in items["nodes"]:
            if not isinstance(project_item, dict):
                continue
            project = project_item.get("project")
            if not isinstance(project, dict) or project.get("id") != identity.project_node_id:
                continue
            value = project_item.get("fieldValueByName")
            if not isinstance(value, dict):
                return _unresolved_result(unresolved, "current Status value is unavailable")
            field = value.get("field")
            if not isinstance(field, dict) or field.get("id") != identity.status_field_node_id:
                return _unresolved_result(unresolved, "current Status field identity changed")
            status = stage_for_option(value.get("optionId"), identity.status_option_ids)
            if status is None:
                return _unresolved_result(unresolved, "current Status option is unknown")
            in_project = True
            break
        page = items.get("pageInfo")
        if in_project or not isinstance(page, dict) or not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if not isinstance(cursor, str):
            return _unresolved_result(unresolved, "project item pagination cursor is missing")
        after = cursor
    item_deleted = payload.get("action") == "deleted"
    if not in_project and not item_deleted:
        return _unresolved_result(unresolved, "content is not in the configured project")
    number = issue.get("number")
    title = issue.get("title")
    if not isinstance(number, int) or not isinstance(title, str):
        return _unresolved_result(unresolved, "resolved issue metadata is malformed")
    assignees = issue.get("assignees")
    assignee_nodes = assignees.get("nodes") if isinstance(assignees, dict) else None
    human_assigned = not isinstance(assignee_nodes, list) or any(
        not isinstance(assignee, dict) or assignee.get("__typename") != "Bot"
        for assignee in assignee_nodes
    )
    evidence = IssueSnapshot(
        open=issue.get("state") == "OPEN",
        human_assigned=human_assigned,
        repo_matches=True,
        identity_resolved=True,
        in_project=in_project,
        stage=status if in_project else None,
        title=title,
        body=issue.get("body") if isinstance(issue.get("body"), str) else None,
        read_at_us=delivery_time_us if read_at_us is None else read_at_us,
    )
    bodies = normalizer._bodies("projects_v2_item", payload, actor_id)
    source_time_us = normalizer._source_time(payload, "projects_v2_item", delivery_time_us)
    events = tuple(
        Event(
            event_id=normalizer._logical_id(
                "projects_v2_item", payload, unresolved.delivery_guid, body
            ),
            repo_id=normalizer.identity.repository_node_id,
            parcel_id=content_id,
            source_time_us=source_time_us,
            provenance=unresolved.provenance,
            body=body,
            actor_id=actor_id,
            issue_number=number,
            entropy=hashlib.sha256(f"{unresolved.delivery_guid}:{index}".encode()).hexdigest(),
            evidence=evidence,
            delivery_guid=unresolved.delivery_guid,
        )
        for index, body in enumerate(bodies)
    )
    return NormalizedDelivery(
        unresolved.delivery_guid,
        unresolved.event_name,
        unresolved.body_sha256,
        events,
        ignored_reason=None if events else NO_PROJECT_TRANSITION,
        provenance=unresolved.provenance,
    )


def _unresolved_result(
    delivery: NormalizedDelivery, reason: str, *, foreign: bool = False
) -> NormalizedDelivery:
    return NormalizedDelivery(
        delivery.delivery_guid,
        delivery.event_name,
        delivery.body_sha256,
        (),
        unresolved_content_node_id=delivery.unresolved_content_node_id,
        ignored_reason=reason,
        provenance=delivery.provenance,
        foreign_content=foreign,
    )
