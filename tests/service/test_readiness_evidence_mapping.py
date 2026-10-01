"""The executor carries the head GitHub actually reported (not the one it asked about)
and the kind of readiness failure into ``ReadinessEvidence`` (design-r2 §E)."""

from __future__ import annotations

import json
from importlib.resources import files

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.events import ChecksState
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import PublicationRenderer, ServiceDispatchDirectory
from omnigent_factory.service.executor import EffectExecutor, ParcelSerializers
from omnigent_factory.testing.fakes import FakeClock
from tests.service.test_readiness_recovery import BUILD_READY

ASKED = "2041e6e2" + "0" * 32
HEAD = "beae9656" + "0" * 32
REVIEWED = "4950075d373b7db6c008a502da568ee73e0fdf03"


def test_executor_carries_the_actual_pr_head_and_failure_kind_into_the_reducer(
    service_config: ServiceConfig,
):
    executor = EffectExecutor(
        None,  # type: ignore[arg-type]
        service_config.trusted,
        FakeClock(),
        (),
        ParcelSerializers(),
        poll_seconds=1,
    )
    fetch = EffectIntent(
        effect_id="ef_fetch",
        kind=EffectKind.FETCH_PR_EVIDENCE,
        parcel_id="I_1",
        target="I_1",
        preconditions=Preconditions(1, 0),
        args={"pr_number": 683, "head_sha": "2041e6e2" + "0" * 32, "session_id": "ss_b"},
    )
    detail = {
        "head_sha": HEAD,
        "open": True,
        "merged": False,
        "checks": "failed",
        "review_accepted": False,
        "findings_dispositioned": False,
        "verified": False,
        "checks_summary": "3 checks: 2 success, 1 failure",
    }
    event = executor._ack_event(fetch, Ack("683", detail))
    assert event is not None and isinstance(event.body, ev.ReadinessEvidence)
    body = event.body
    assert body.head_sha == "2041e6e2" + "0" * 32 and body.observed_head_sha == HEAD
    assert body.checks == ChecksState.FAILED and body.findings_open
    assert not body.review_accepted and body.pr_open and not body.merged


@pytest.mark.asyncio
async def test_cross_vendor_review_is_checked_against_the_attested_head_not_the_new_one(
    service_config: ServiceConfig,
) -> None:
    """After "Update branch" the read asks about the new head; the stored review still
    attests the reviewed head, and the GitHub read decides if it carries forward."""
    results = service_config.state_dir / "results"
    results.mkdir(mode=0o700, exist_ok=True)
    payload = {"item_id": "i", "factory_result": {"result": BUILD_READY}}
    (results / "ss_build-latest.json").write_text(json.dumps(payload))
    renderer = PublicationRenderer(ServiceDispatchDirectory(None, service_config), service_config)  # type: ignore[arg-type]
    args = {"session_id": "ss_build", "pr_number": 682, "head_sha": HEAD, "reviewed_head": REVIEWED}
    fetch = EffectIntent("ef_f", EffectKind.FETCH_PR_EVIDENCE, "P", "P", Preconditions(1, 0), args)
    assert await renderer.cross_vendor_review(fetch) is True


def test_missing_closing_reference_reaches_the_reducer_and_the_build_template_requires_it(
    service_config: ServiceConfig,
) -> None:
    executor = EffectExecutor(
        None,  # type: ignore[arg-type]
        service_config.trusted,
        FakeClock(),
        (),
        ParcelSerializers(),
        poll_seconds=1,
    )
    fetch = EffectIntent(
        "ef_f",
        EffectKind.FETCH_PR_EVIDENCE,
        "I_1",
        "I_1",
        Preconditions(1, 0),
        {"pr_number": 683, "head_sha": HEAD, "session_id": "ss_b"},
    )
    detail = {"head_sha": HEAD, "checks": "green", "closes_issue": False, "verified": False}
    event = executor._ack_event(fetch, Ack("683", detail))
    assert event is not None and isinstance(event.body, ev.ReadinessEvidence)
    assert event.body.closes_issue is False
    build = (files("omnigent_factory.service") / "templates" / "build-v6.txt").read_text("utf-8")
    assert "`Closes #{issue_number}`" in build


@pytest.mark.parametrize(
    ("value", "carried"), [(0, 0), (1_790_664_471_000_000, 1_790_664_471_000_000), (None, None)]
)
def test_review_bot_pending_time_reaches_the_reducer(
    service_config: ServiceConfig, value: int | None, carried: int | None
) -> None:
    executor = EffectExecutor(
        None,  # type: ignore[arg-type]
        service_config.trusted,
        FakeClock(),
        (),
        ParcelSerializers(),
        poll_seconds=1,
    )
    fetch = EffectIntent(
        "ef_f",
        EffectKind.FETCH_PR_EVIDENCE,
        "I_1",
        "I_1",
        Preconditions(1, 0),
        {"pr_number": 686, "head_sha": HEAD, "session_id": "ss_b"},
    )
    detail = {"head_sha": HEAD, "checks": "green", "verified": True}
    detail["review_bot_pending_since_us"] = value
    event = executor._ack_event(fetch, Ack("686", detail))
    assert event is not None and isinstance(event.body, ev.ReadinessEvidence)
    assert event.body.review_bot_pending_since_us == carried


def test_base_sync_and_failing_check_names_reach_the_reducer(
    service_config: ServiceConfig,
) -> None:
    """#651: the reducer needs both to keep a Ready card in Ready on a base-sync red."""
    executor = EffectExecutor(
        None,  # type: ignore[arg-type]
        service_config.trusted,
        FakeClock(),
        (),
        ParcelSerializers(),
        poll_seconds=1,
    )
    fetch = EffectIntent(
        "ef_f",
        EffectKind.FETCH_PR_EVIDENCE,
        "I_1",
        "I_1",
        Preconditions(1, 0),
        {"pr_number": 686, "head_sha": HEAD, "session_id": "ss_b"},
    )
    detail = {
        "head_sha": HEAD,
        "checks": "failed",
        "verified": False,
        "base_sync": True,
        "failing_checks": "api / Dependency audit",
    }
    event = executor._ack_event(fetch, Ack("686", detail))
    assert event is not None and isinstance(event.body, ev.ReadinessEvidence)
    assert event.body.base_sync is True
    assert event.body.failing_checks == "api / Dependency audit"
    legacy = executor._ack_event(fetch, Ack("686", {"head_sha": HEAD, "verified": False}))
    assert legacy is not None and isinstance(legacy.body, ev.ReadinessEvidence)
    assert legacy.body.base_sync is False and legacy.body.failing_checks == ""
