"""A rejected webhook delivery is logged with its GUID, event, action and the exact check.

Logging only: the acceptance rules are unchanged (still 401, nothing persisted). The log
line never carries the secret, the signature or body text.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from omnigent_factory.github.webhook import DeliveryNormalizer
from omnigent_factory.service.app import create_app
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.github_delivery import GitHubWebhookVerifier
from omnigent_factory.service.interfaces import WebhookRejected
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.fakes import FakeClock
from tests.github.test_webhook_config_setup import IDENTITY, payload

SECRET = b"s3cret-webhook-value"
BODY_TEXT = "PRIVATE-BODY-TEXT"


def _signed(data: dict[str, Any], secret: bytes = SECRET) -> tuple[bytes, str]:
    raw = json.dumps(data).encode()
    return raw, "sha256=" + hmac.new(secret, raw, hashlib.sha256).hexdigest()


def _headers(signature: str | None, event: str = "issue_comment") -> dict[str, str]:
    headers = {"x-github-delivery": "guid-123", "x-github-event": event}
    if signature is not None:
        headers["x-hub-signature-256"] = signature
    return headers


@pytest.fixture
def verifier(tmp_path: Path) -> GitHubWebhookVerifier:
    secret = tmp_path / "webhook_secret"
    secret.write_bytes(SECRET)
    return GitHubWebhookVerifier(secret, DeliveryNormalizer(IDENTITY), FakeClock())


def _comment(**overrides: Any) -> dict[str, Any]:
    data = payload(**overrides)
    data["comment"] = {**data["comment"], "body": BODY_TEXT}
    return data


async def _rejection(
    verifier: GitHubWebhookVerifier, raw: bytes, headers: dict[str, str]
) -> WebhookRejected:
    with pytest.raises(WebhookRejected) as caught:
        await verifier.verify(raw, headers)
    return caught.value


@pytest.mark.asyncio
async def test_missing_and_bad_signatures_name_the_signature_check(
    verifier: GitHubWebhookVerifier,
) -> None:
    raw, _ = _signed(_comment())
    missing = await _rejection(verifier, raw, _headers(None))
    assert (missing.check, missing.reason) == (
        "signature",
        "missing or unsupported webhook signature",
    )
    assert (missing.delivery, missing.event, missing.action) == (
        "guid-123",
        "issue_comment",
        "created",
    )
    _, wrong = _signed(_comment(), secret=b"other")
    bad = await _rejection(verifier, raw, _headers(wrong))
    assert (bad.check, bad.reason) == ("signature", "webhook signature mismatch")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("override", "event", "expected"),
    [
        (
            {"installation": {"id": 999}},
            "issue_comment",
            "wrong or missing App installation (installation=999 ",
        ),
        ({"organization": {"id": 5}}, "issue_comment", "wrong or missing organization"),
        (
            {"repository": {"id": 1, "node_id": "R_x", "full_name": "other/repo"}},
            "issue_comment",
            "wrong repository (installation=901 organization=296340858 repository=other/repo",
        ),
        (
            {
                "repository": None,
                "action": "edited",
                "projects_v2_item": {"project_node_id": "PVT_other", "content_node_id": "I_1"},
            },
            "projects_v2_item",
            "wrong or missing project identity",
        ),
    ],
)
async def test_identity_failures_name_the_failed_check_and_the_ids_seen(
    verifier: GitHubWebhookVerifier, override: dict[str, Any], event: str, expected: str
) -> None:
    data = _comment(**override)
    if data.get("repository") is None:
        data.pop("repository")
    raw, signature = _signed(data)
    rejected = await _rejection(verifier, raw, _headers(signature, event))
    assert rejected.check == "identity"
    assert expected in rejected.reason
    assert rejected.event == event and rejected.delivery == "guid-123"


def test_receiver_logs_the_rejection_without_secret_or_body(
    service_config: ServiceConfig,
    verifier: GitHubWebhookVerifier,
    caplog: pytest.LogCaptureFixture,
) -> None:
    service = FactoryService(service_config, clock=FakeClock())
    raw, signature = _signed(_comment(organization={"id": 5}))
    with (
        caplog.at_level(logging.WARNING, logger="omnigent_factory.service.app"),
        TestClient(create_app(service, verifier)) as client,
    ):
        response = client.post("/webhooks/github", content=raw, headers=_headers(signature))
    assert response.status_code == 401  # acceptance unchanged
    [line] = [r.getMessage() for r in caplog.records if "webhook rejected" in r.getMessage()]
    assert line.startswith(
        "webhook rejected: identity check failed delivery=guid-123 event=issue_comment "
        "action=created reason=wrong or missing organization (installation=901 organization=5 "
    )
    assert SECRET.decode() not in line and BODY_TEXT not in line and signature not in line
