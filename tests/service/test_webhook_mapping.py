"""PR #682 (#677) webhooks: map PR/check events to the parcel by head branch; ignore the rest.

Live pilot: the bot's PR events arrived before the parcel recorded its PR number, could
not be linked, and were parked repository-wide - halting new work on every issue.
Payloads mirror GitHub's shapes (only the fields the normaliser reads are kept).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
import pytest_asyncio

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import Via
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.github_delivery import (
    GitHubDeliveryProcessor,
    head_branch,
    issue_for_branch,
)
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_pilot_677 import eventually

PARCEL = "I_kwDOTC12Fs8AAAABS6JIGw"
ISSUE = 677
BOT = 334191208
HEAD = "4950075d373b7db6c008a502da568ee73e0fdf03"
IDENTITY = DeliveryIdentity(
    app_id=5085812,
    installation_id=165144097,
    organization_id=296340858,
    project_node_id="PVT_kwDOEanNes4BkJhb",
    status_field_node_id="PVTSSF_lADOEanNes4BkJhbzhi7I9w",
    repository_id=1278047766,
    repository_node_id="R_kgDOTC12Fg",
    repository_full_name="SA-Ambulance/timesheets",
    owner_ids=frozenset({114979}),
    bot_user_id=BOT,
)


def _common(sender: int = BOT) -> dict[str, Any]:
    return {
        "installation": {"id": 165144097, "node_id": "MDIz"},
        "organization": {"id": 296340858, "login": "SA-Ambulance"},
        "repository": {
            "id": 1278047766,
            "node_id": "R_kgDOTC12Fg",
            "full_name": "SA-Ambulance/timesheets",
        },
        "sender": {"id": sender, "login": "molly-omnigent-factory[bot]", "type": "Bot"},
    }


def pr_payload(action: str, number: int, branch: str, author: int = BOT) -> dict[str, Any]:
    return {
        **_common(author),
        "action": action,
        "number": number,
        "pull_request": {
            "number": number,
            "state": "open",
            "merged": False,
            "user": {"id": author, "type": "Bot"},
            "head": {"ref": branch, "sha": HEAD},
            "base": {"ref": "main"},
            "body": "…\n\nCloses #677",
        },
    }


def checks_payload(event: str, number: int, branch: str) -> dict[str, Any]:
    return {
        **_common(),
        "action": "completed",
        event: {
            "head_branch": branch,
            "head_sha": HEAD,
            "status": "completed",
            "conclusion": "success",
            "pull_requests": [{"number": number, "head": {"ref": branch, "sha": HEAD}}],
        },
    }


class _GitHub:
    """Only what the processor reads for scoped events: fresh issue evidence."""

    repository_node_id = "R_kgDOTC12Fg"

    async def issue_snapshot(self, ref: Any) -> Any:
        return snapshot(read_at_us=1)


@pytest_asyncio.fixture
async def service(service_config: ServiceConfig):
    svc = FactoryService(
        service_config,
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await svc.start()
    await svc.operator_command("unpause", {})
    factory = EventFactory(PARCEL, issue_number=ISSUE)
    await svc.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    await svc.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))
    svc.delivery_processor = GitHubDeliveryProcessor(
        svc,
        DeliveryNormalizer(IDENTITY),
        _GitHub(),  # type: ignore[arg-type]
        svc.clock,
    )
    try:
        yield svc
    finally:
        await svc.stop()


async def _deliver(svc: FactoryService, guid: str, event: str, payload: dict[str, Any]) -> str:
    await svc.persist_delivery(DeliveryRecord(guid, event, json.dumps(payload).encode(), {}))

    async def settled() -> bool:
        rows = await svc.db.call(
            lambda store: store.query(
                "SELECT status FROM deliveries WHERE delivery_guid = ?", (guid,)
            )
        )
        return bool(rows) and rows[0][0] != "pending"

    await eventually(settled)
    rows = await svc.db.call(
        lambda store: store.query("SELECT status FROM deliveries WHERE delivery_guid = ?", (guid,))
    )
    return str(rows[0][0])


async def _events(svc: FactoryService, kind: str) -> list[Any]:
    return await svc.db.call(
        lambda store: store.query(
            "SELECT parcel_id, delivery_guid FROM events WHERE kind = ?", (kind,)
        )
    )


def test_head_branch_is_read_from_every_payload_shape():
    for event, payload in (
        ("pull_request", pr_payload("opened", 682, "factory/issue-677")),
        ("check_suite", checks_payload("check_suite", 682, "factory/issue-677")),
        ("workflow_run", checks_payload("workflow_run", 682, "factory/issue-677")),
    ):
        branch = head_branch(event, json.dumps(payload).encode())
        assert issue_for_branch(branch) == ISSUE, event
    assert issue_for_branch("feature/x") is None
    assert issue_for_branch("factory/issue-677-extra") is None


@pytest.mark.asyncio
async def test_bot_pr_and_checks_for_the_parcel_branch_map_to_the_parcel(service):
    assert (
        await _deliver(
            service, "pr-opened", "pull_request", pr_payload("opened", 682, "factory/issue-677")
        )
        == "processed"
    )
    for guid, event in (("suite", "check_suite"), ("run", "workflow_run")):
        payload = checks_payload(event, 682, "factory/issue-677")
        assert await _deliver(service, guid, event, payload) == "processed"
    observed = await _events(service, "PRObserved")
    checks = await _events(service, "ChecksChanged")
    assert [(r[0], r[1]) for r in observed] == [(PARCEL, "pr-opened")]
    assert sorted(r[1] for r in checks) == ["run", "suite"]
    assert all(r[0] == PARCEL for r in checks)
    status = await service.operator_command("status", {})
    assert status["parked_deliveries"] == 0


@pytest.mark.asyncio
async def test_unrelated_and_unreadable_informational_events_are_ignored_not_parked(service):
    unrelated = [
        ("other-pr", "pull_request", pr_payload("labeled", 700, "owner/feature", author=114979)),
        ("other-run", "workflow_run", checks_payload("workflow_run", 700, "owner/feature")),
        ("garbled", "check_suite", {"action": "completed"}),  # fails identity checks
    ]
    for guid, event, payload in unrelated:
        assert await _deliver(service, guid, event, payload) == "processed", guid
    status = await service.operator_command("status", {})
    assert status["parked_deliveries"] == 0
    assert await _events(service, "PRObserved") == []


@pytest.mark.asyncio
async def test_unreadable_safety_event_is_still_parked_with_its_reason(service):
    guid = "garbled-issue"
    assert await _deliver(service, guid, "issues", {"action": "closed"}) == "rejected"
    recovery = await service.operator_command("recovery", {})
    [row] = [d for d in recovery["deliveries"] if d["delivery_guid"] == guid]
    assert row["status"] == "parked" and row["reason"].startswith("unreadable issues:")
    await asyncio.sleep(0)
