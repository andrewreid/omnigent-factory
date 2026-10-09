"""A ``factory:*`` label whose webhook was lost is recovered from the issue timeline.

Before this, the board diff saw the label change, scheduled an issue read (which names no
label actor) and stored the new digest: the owner's command was never acted on. Now a
changed card showing a command label gets one timeline read; only an owner's newest
``labeled`` event applies the control, keyed by the timeline event so the webhook and the
recovery (in either order, across restarts) apply it once.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import Stage, Via
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.ports.github import BoardCard, IssueRef, LabelEvent
from omnigent_factory.service.board_diff import BoardDiff
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.github_delivery import GitHubDeliveryProcessor
from omnigent_factory.service.label_recovery import LabelCheck, LabelRecovery, label_check
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID, T0, EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_pilot_677 import eventually

PARCEL = "I_kwDOTC12Fs8AAAABWApPaQ"
ISSUE = 822
STRANGER = 222
BOT = 334191208
LABELED_AT = T0 + 60_000_000  # after the parcel's first read
IDENTITY = DeliveryIdentity(
    app_id=5085812,
    installation_id=165144097,
    organization_id=296340858,
    project_node_id="PVT_kwDOEanNes4BkJhb",
    status_field_node_id="PVTSSF_lADOEanNes4BkJhbzhi7I9w",
    repository_id=1278047766,
    repository_node_id=REPO_ID,
    repository_full_name="SA-Ambulance/timesheets",
    owner_ids=frozenset({OWNER_ID}),
    bot_user_id=BOT,
)


def labeled(
    event_id: str = "LE_1",
    *,
    actor: int | None = OWNER_ID,
    at: int = LABELED_AT,
    label: str = "factory:triage",
    added: bool = True,
) -> LabelEvent:
    return LabelEvent(event_id, label, added, actor, at)


@dataclass
class Timeline:
    """GitHub reads label recovery and the delivery processor make (counted)."""

    events: list[LabelEvent] = field(default_factory=list)
    fail: bool = False
    timeline_reads: int = 0
    snapshot_reads: int = 0
    repository_node_id: str = REPO_ID

    async def label_events(self, ref: IssueRef) -> tuple[LabelEvent, ...] | RetryableReadFailure:
        assert (ref.parcel_id, ref.issue_number) == (PARCEL, ISSUE)
        self.timeline_reads += 1
        if self.fail:
            return RetryableReadFailure("timeline unavailable")
        return tuple(self.events)

    async def issue_snapshot(self, ref: IssueRef) -> Any:
        self.snapshot_reads += 1
        return snapshot(read_at_us=LABELED_AT + 1)


async def _service(config: ServiceConfig, *, seed: bool) -> FactoryService:
    svc = FactoryService(
        config,
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await svc.start()
    if seed:
        await svc.operator_command("unpause", {})
        factory = EventFactory(PARCEL, issue_number=ISSUE)
        await svc.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
    return svc


@pytest_asyncio.fixture
async def service(service_config: ServiceConfig) -> AsyncIterator[FactoryService]:
    svc = await _service(service_config, seed=True)
    try:
        yield svc
    finally:
        await svc.stop()


def _recovery(svc: FactoryService, timeline: Timeline) -> LabelRecovery:
    return LabelRecovery(svc, timeline.label_events, timeline.issue_snapshot)


def _check(updated_at: str = "2026-10-09T01:00:00Z") -> LabelCheck:
    check = label_check(("area:api", "factory:triage"), updated_at)
    assert check is not None
    return check


async def _triage_requests(svc: FactoryService) -> list[tuple[Any, ...]]:
    rows = await svc.db.call(
        lambda store: store.query(
            "SELECT event_id, provenance, actor_id, accepted FROM events "
            "WHERE kind = 'RequestTriage' ORDER BY sequence"
        )
    )
    return [tuple(row) for row in rows]


def _iso(us: int) -> str:
    return datetime.fromtimestamp(us / 1_000_000, UTC).isoformat().replace("+00:00", "Z")


def _label_webhook(sender: int = OWNER_ID, label: str = "factory:triage") -> bytes:
    payload = {
        "action": "labeled",
        "installation": {"id": 165144097},
        "organization": {"id": 296340858},
        "repository": {
            "id": 1278047766,
            "node_id": REPO_ID,
            "full_name": "SA-Ambulance/timesheets",
        },
        "sender": {"id": sender, "type": "User"},
        "issue": {"number": ISSUE, "node_id": PARCEL, "updated_at": _iso(LABELED_AT)},
        "label": {"name": label},
    }
    return json.dumps(payload).encode()


async def _deliver(svc: FactoryService, timeline: Timeline, guid: str) -> str:
    svc.delivery_processor = GitHubDeliveryProcessor(
        svc,
        DeliveryNormalizer(IDENTITY),
        timeline,  # type: ignore[arg-type]
        svc.clock,
    )
    await svc.persist_delivery(DeliveryRecord(guid, "issues", _label_webhook(), {}))

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


# ================================================================= recovery


@pytest.mark.asyncio
async def test_lost_owner_label_is_recovered_once(service: FactoryService) -> None:
    timeline = Timeline([labeled()])
    recovery = _recovery(service, timeline)
    assert await recovery.check(PARCEL, _check())
    assert await _triage_requests(service) == [
        ("github:label:LE_1", Provenance.RECOVERY.value, OWNER_ID, 1)
    ]
    parcel = await service.db.call(lambda store: store.load_parcel(PARCEL))
    assert parcel is not None and parcel.stage == Stage.TRIAGED  # the owner's command acted
    # The card changes again (e.g. a comment): the same label event is never re-applied.
    assert await recovery.check(PARCEL, _check("2026-10-09T02:00:00Z"))
    assert await _triage_requests(service) == [
        ("github:label:LE_1", Provenance.RECOVERY.value, OWNER_ID, 1)
    ]
    assert timeline.timeline_reads == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", [STRANGER, BOT, None])
async def test_non_owner_or_unknown_actor_is_ignored_with_a_log_line(
    service: FactoryService, actor: int | None, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="omnigent_factory.service.label_recovery")
    # An owner labelled long ago; the label's newest add is someone else's.
    timeline = Timeline(
        [
            labeled("LE_old", at=LABELED_AT - 10_000_000),
            labeled("LE_old_off", at=LABELED_AT - 5_000_000, added=False),
            labeled("LE_2", actor=actor),
        ]
    )
    assert await _recovery(service, timeline).check(PARCEL, _check())
    assert await _triage_requests(service) == []
    assert "label recovery ignored: not an owner" in caplog.text
    assert timeline.snapshot_reads == 0


@pytest.mark.asyncio
async def test_label_removed_before_recovery_does_nothing(service: FactoryService) -> None:
    timeline = Timeline([labeled(), labeled("LE_off", at=LABELED_AT + 1_000_000, added=False)])
    assert await _recovery(service, timeline).check(PARCEL, _check())
    assert await _triage_requests(service) == []


@pytest.mark.asyncio
async def test_a_failed_read_is_retried_and_nothing_is_applied(service: FactoryService) -> None:
    timeline = Timeline([labeled()], fail=True)
    recovery = _recovery(service, timeline)
    assert not await recovery.check(PARCEL, _check())
    assert await _triage_requests(service) == []
    timeline.fail = False
    assert await recovery.check(PARCEL, _check())  # same check again: not settled before
    assert len(await _triage_requests(service)) == 1


@pytest.mark.asyncio
async def test_restart_does_not_refire(service_config: ServiceConfig) -> None:
    svc = await _service(service_config, seed=True)
    try:
        assert await _recovery(svc, Timeline([labeled()])).check(PARCEL, _check())
        assert len(await _triage_requests(svc)) == 1
    finally:
        await svc.stop()
    svc = await _service(service_config, seed=False)
    try:
        timeline = Timeline([labeled()])
        assert await _recovery(svc, timeline).check(PARCEL, _check())
        assert timeline.timeline_reads == 1 and timeline.snapshot_reads == 0
        assert len(await _triage_requests(svc)) == 1
    finally:
        await svc.stop()


# ======================================================== webhook and recovery


@pytest.mark.asyncio
async def test_webhook_then_recovery_applies_the_label_once(service: FactoryService) -> None:
    timeline = Timeline([labeled()])
    assert await _deliver(service, timeline, "label-1") == "processed"
    assert await _triage_requests(service) == [
        ("github:label:LE_1", Provenance.WEBHOOK.value, OWNER_ID, 1)
    ]
    assert await _recovery(service, timeline).check(PARCEL, _check())
    assert len(await _triage_requests(service)) == 1


@pytest.mark.asyncio
async def test_recovery_then_late_webhook_applies_the_label_once(service: FactoryService) -> None:
    timeline = Timeline([labeled()])
    assert await _recovery(service, timeline).check(PARCEL, _check())
    assert await _deliver(service, timeline, "label-late") == "processed"
    assert await _triage_requests(service) == [
        ("github:label:LE_1", Provenance.RECOVERY.value, OWNER_ID, 1)
    ]


@pytest.mark.asyncio
async def test_a_label_applied_under_its_delivery_id_is_not_recovered_again(
    service: FactoryService,
) -> None:
    """Commands applied before label events had their own ID (old daemon, or a webhook
    whose timeline event was not found) are recognised by kind and time."""
    legacy = Event(
        event_id="github:issues:RequestTriage:old-delivery",
        repo_id=REPO_ID,
        parcel_id=PARCEL,
        source_time_us=LABELED_AT,  # the payload's issue updated_at
        provenance=Provenance.WEBHOOK,
        body=ev.RequestTriage(via=Via.LABEL),
        actor_id=OWNER_ID,
        issue_number=ISSUE,
        evidence=snapshot(read_at_us=LABELED_AT),
    )
    assert (await service.apply_event(legacy)).accepted
    assert await _recovery(service, Timeline([labeled()])).check(PARCEL, _check())
    assert [row[0] for row in await _triage_requests(service)] == [legacy.event_id]


# ============================================================ board diff wiring


def _card(labels: tuple[str, ...], updated_at: str = "2026-10-09T00:00:00Z") -> BoardCard:
    return BoardCard(
        node_id=PARCEL,
        number=ISSUE,
        open=True,
        status_option="915abb46",
        bot_option="",
        assignees=(),
        labels=(*labels, f"#{len(labels)}"),
        title="Add export button",
        updated_at=updated_at,
    )


@pytest.mark.asyncio
async def test_board_diff_keeps_the_digest_until_the_label_is_checked(
    service: FactoryService,
) -> None:
    """The lost-webhook path end to end: the digest is not advanced past an unchecked
    label (a failed timeline read), and the next diff recovers the command."""
    cards = [_card(())]

    async def board() -> list[BoardCard]:
        return list(cards)

    timeline = Timeline([labeled()], fail=True)
    service.board_diff = BoardDiff(service, board)
    service.label_recovery = _recovery(service, timeline)
    await service.db.call(lambda store: store.save_board_digests({PARCEL: cards[0].digest}))

    async def diff() -> None:
        service._next_board_diff_at = None
        await service._board_diff_tick(0.0, frozenset())

    cards[0] = _card(("factory:triage",), "2026-10-09T01:00:00Z")  # webhook lost
    await diff()
    stored = await service.db.call(lambda store: store.board_digests())
    assert stored[PARCEL] != cards[0].digest  # not compared away unchecked
    assert await _triage_requests(service) == []
    timeline.fail = False
    await diff()
    assert len(await _triage_requests(service)) == 1
    stored = await service.db.call(lambda store: store.board_digests())
    assert stored[PARCEL] == cards[0].digest
    await diff()  # unchanged card: no further read
    assert timeline.timeline_reads == 2


def test_only_cards_showing_a_command_label_are_checked() -> None:
    assert label_check(("area:api", "#1"), "t") is None
    assert label_check(("factory:plan", "area:api", "#2"), "t") == LabelCheck(
        ("factory:plan",), "t"
    )
    # More labels than one board read lists: a command label may be hidden.
    assert label_check(("area:api", "#60"), "t") == LabelCheck(
        ("factory:build", "factory:plan", "factory:triage"), "t"
    )
