"""Production GitHub webhook verification and durable-inbox processing."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

from omnigent_factory.core.events import Event, EventKind, Provenance
from omnigent_factory.core.types import IssueSnapshot
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.github.client import GitHubAPIError, RateLimited
from omnigent_factory.github.webhook import (
    NO_PROJECT_TRANSITION,
    DeliveryNormalizer,
    WebhookError,
    resolve_project_delivery,
)
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.github import IssueRef
from omnigent_factory.service.interfaces import NonRetryableDelivery, WebhookRejected
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord

LOG = logging.getLogger(__name__)


class GitHubWebhookVerifier:
    """Verify the signature and configured routing tuple before durable acknowledgement."""

    def __init__(self, secret_file: Path, normalizer: DeliveryNormalizer, clock: Clock) -> None:
        self.secret_file = secret_file
        self.normalizer = normalizer
        self.clock = clock

    async def verify(self, body: bytes, headers: Mapping[str, str]) -> DeliveryRecord:
        lowered = {key.lower(): value for key, value in headers.items()}
        guid = lowered.get("x-github-delivery")
        event_name = lowered.get("x-github-event")
        if not guid or not event_name:
            raise WebhookRejected("missing GitHub delivery headers")
        try:
            secret = self.secret_file.read_bytes().strip()
            if not secret:
                raise WebhookError("empty webhook secret")
            normalized = self.normalizer.authenticate_and_normalize(
                secret=secret,
                signature=lowered.get("x-hub-signature-256"),
                raw_body=body,
                event_name=event_name,
                delivery_guid=guid,
                delivery_time_us=self.clock.now_utc_us(),
            )
            payload = json.loads(body)
        except (OSError, ValueError, WebhookError) as exc:
            raise WebhookRejected("GitHub delivery authentication failed") from exc
        action = payload.get("action") if isinstance(payload, dict) else None
        return DeliveryRecord(
            delivery_guid=guid,
            event_name=event_name,
            body=body,
            headers={
                key: value
                for key, value in lowered.items()
                if key in {"x-github-delivery", "x-github-event", "x-github-hook-id"}
            },
            provenance=normalized.provenance.value,
            action=action if isinstance(action, str) else None,
            app_id=self.normalizer.identity.app_id,
            installation_id=self.normalizer.identity.installation_id,
            source_time_us=self.clock.now_utc_us(),
        )


class GitHubDeliveryProcessor:
    """Turn one committed inbox row into at most one normalized reducer event."""

    def __init__(
        self,
        service: FactoryService,
        normalizer: DeliveryNormalizer,
        github: GitHubAPIAdapter,
        clock: Clock,
    ) -> None:
        self.service = service
        self.normalizer = normalizer
        self.github = github
        self.clock = clock

    async def process(self, delivery: DeliveryRecord) -> None:
        try:
            provenance = Provenance(delivery.provenance)
            normalized = self.normalizer.normalize(
                raw_body=delivery.body,
                event_name=delivery.event_name,
                delivery_guid=delivery.delivery_guid,
                delivery_time_us=delivery.source_time_us or self.clock.now_utc_us(),
                provenance=provenance,
            )
        except (ValueError, WebhookError) as exc:
            if delivery.event_name not in SAFETY_EVENTS:
                # Informational (PR/check/workflow/review): reconcile and the PR fetch
                # poll that state anyway, so an unreadable one never halts the factory.
                await self._ignore(delivery, f"unreadable {delivery.event_name}: {exc}")
                return
            raise NonRetryableDelivery(f"unreadable {delivery.event_name}: {exc}") from exc

        content_id = normalized.unresolved_content_node_id
        if content_id is not None:
            lookup_failed = False
            retry_after_us: int | None = None
            try:
                normalized = await resolve_project_delivery(
                    client=self.github.client,
                    normalizer=self.normalizer,
                    unresolved=normalized,
                    raw_body=delivery.body,
                    delivery_time_us=delivery.source_time_us or self.clock.now_utc_us(),
                    read_at_us=self.clock.now_utc_us(),
                )
            except WebhookError as exc:
                raise NonRetryableDelivery(
                    "project delivery is invalid", parcel_id=content_id
                ) from exc
            except GitHubAPIError as exc:
                lookup_failed = True
                retry_after_us = exc.retry_after_us if isinstance(exc, RateLimited) else None
            if lookup_failed:
                # A failed lookup proves nothing about the card: keep it fenced and retry
                # on the durable schedule until the operator must decide.
                await self._set_aside(
                    delivery.delivery_guid, content_id, "lookup failed", retry_after_us
                )
                return
            if not normalized.events:
                known = await self.service.db.call(
                    lambda store: store.load_parcel(content_id) is not None
                )
                # A card that already is a parcel is never "foreign": it is ours and changed.
                if normalized.foreign_content and not known:
                    LOG.info(
                        "project delivery retired as foreign content delivery_guid=%s reason=%s",
                        delivery.delivery_guid,
                        normalized.ignored_reason,
                    )
                    await self.service.ignore_delivery(delivery.delivery_guid)
                    return
                if normalized.ignored_reason == NO_PROJECT_TRANSITION:
                    # Resolved to our issue; no field the factory reads changed.
                    LOG.info(
                        "project delivery has no transition delivery_guid=%s",
                        delivery.delivery_guid,
                    )
                    await self.service.ignore_delivery(delivery.delivery_guid)
                    return
                await self._set_aside(
                    delivery.delivery_guid, content_id, normalized.ignored_reason or "", None
                )
                return

        if len(normalized.events) > 1:
            # The current normalizer deliberately emits one logical transition per
            # delivery. Fail closed if a future change violates the inbox retirement
            # boundary instead of acknowledging a partial batch.
            raise NonRetryableDelivery("delivery expands to multiple transitions")
        if not normalized.events:
            LOG.info("delivery has no transition delivery_guid=%s", delivery.delivery_guid)
            await self.service.db.call(
                lambda store: store.mark_delivery(delivery.delivery_guid, "processed")
            )
            return

        event = await self._freshen(normalized.events[0], delivery)
        if event is None:
            await self._ignore(delivery, "PR/check event for no known parcel")
            return
        if delivery.event_name == "pull_request_review" and event.kind == EventKind.PLAN_FEEDBACK:
            await self._record_review_comments(delivery, event)
        await self.service.apply_event(event, delivery_status="processed")

    async def _record_review_comments(self, delivery: DeliveryRecord, event: Event) -> None:
        """Store the inline comments of an owner review for ``factory_get_feedback``.

        The pull_request_review webhook carries only the review body; its inline comments
        are read here, before the event is applied (a failed read retries the delivery).
        """
        payload = json.loads(delivery.body)
        review = payload.get("review") if isinstance(payload, dict) else None
        review_id = review.get("id") if isinstance(review, dict) else None
        pr_number = getattr(event.body, "pr_number", 0)
        if not isinstance(review_id, int) or not isinstance(pr_number, int) or pr_number <= 0:
            return
        comments = await self.github.review_comments(pr_number, review_id)
        if not isinstance(comments, list):
            raise RuntimeError(f"review comments temporarily unavailable: {comments.reason}")
        guid, text = delivery.delivery_guid, json.dumps(comments)
        await self.service.db.call(lambda store: store.record_review_comments(guid, text))

    async def _ignore(self, delivery: DeliveryRecord, reason: str) -> None:
        LOG.info(
            "delivery ignored delivery_guid=%s event=%s reason=%s",
            delivery.delivery_guid,
            delivery.event_name,
            reason[:300],
        )
        await self.service.db.call(
            lambda store: store.mark_delivery(delivery.delivery_guid, "processed")
        )

    async def _set_aside(
        self, delivery_guid: str, content_id: str, reason: str, retry_after_us: int | None
    ) -> None:
        """Hold a card that may be a timesheets issue; exhaustion parks it for the operator."""
        LOG.warning("project delivery unresolved delivery_guid=%s reason=%s", delivery_guid, reason)
        await self.service.hold_unresolved_delivery(delivery_guid, content_id)
        await self.service.defer_unresolved_delivery(
            delivery_guid, content_id, retry_after_us=retry_after_us
        )

    async def _freshen(self, event: Event, delivery: DeliveryRecord) -> Event | None:
        """Scope and freshen the event; ``None`` when a PR/check event maps to no parcel."""
        if event.parcel_id is None:
            parcel_id = await self._parcel_for_pr(event, delivery)
            if parcel_id is None:
                return None
            event = replace(event, parcel_id=parcel_id)
        if event.issue_number is None:
            parcel = await self.service.db.call(
                lambda store: store.load_parcel(event.parcel_id or "")
            )
            if parcel is not None and parcel.issue_number is not None:
                event = replace(event, issue_number=parcel.issue_number)
        if event.issue_number is None:
            return event
        snapshot = await self.github.issue_snapshot(
            IssueRef(self.github.repository_node_id, event.issue_number, event.parcel_id or "")
        )
        if isinstance(snapshot, IssueSnapshot):
            return replace(event, evidence=snapshot)
        raise RuntimeError("fresh GitHub evidence is temporarily unavailable")

    async def _parcel_for_pr(self, event: Event, delivery: DeliveryRecord) -> str | None:
        """Parcel of a PR/check/review event: its recorded PR number, else the factory
        head branch ``factory/issue-<N>`` of the parcel's issue (before the PR is known)."""
        repo_id = self.service.config.repo_id
        number = getattr(event.body, "pr_number", None)
        if isinstance(number, int) and number > 0:
            rows = await self.service.db.call(
                lambda store: store.query(
                    "SELECT parcel_id FROM parcels WHERE repo_id = ? AND "
                    "json_extract(aggregate_json, '$.parcel.pr_number') = ?",
                    (repo_id, number),
                )
            )
            if len(rows) == 1:
                return str(rows[0][0])
        issue = issue_for_branch(head_branch(delivery.event_name, delivery.body))
        if issue is None:
            return None
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE repo_id = ? AND "
                "json_extract(aggregate_json, '$.parcel.issue_number') = ?",
                (repo_id, issue),
            )
        )
        return str(rows[0][0]) if len(rows) == 1 else None


#: Events that can carry owner controls or safety facts. Only these may be parked when
#: unreadable; everything else is informational and ignored with a log line.
SAFETY_EVENTS = frozenset({"issues", "issue_comment", "projects_v2_item"})

_FACTORY_BRANCH = re.compile(r"factory/issue-([1-9][0-9]{0,9})")


def head_branch(event_name: str, body: bytes) -> str | None:
    """Head branch named by a pull_request / review / check_suite / workflow_run payload."""
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("pull_request", "check_suite", "workflow_run", "check_run"):
        item = payload.get(key)
        if not isinstance(item, dict):
            continue
        head = item.get("head")
        if isinstance(head, dict) and isinstance(head.get("ref"), str):
            return str(head["ref"])
        if isinstance(item.get("head_branch"), str):
            return str(item["head_branch"])
        suite = item.get("check_suite")
        if isinstance(suite, dict) and isinstance(suite.get("head_branch"), str):
            return str(suite["head_branch"])
    return None


def issue_for_branch(branch: str | None) -> int | None:
    match = _FACTORY_BRANCH.fullmatch(branch or "")
    return int(match.group(1)) if match else None
