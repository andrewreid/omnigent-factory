"""Pilot #677 plan rejection: validation errors are precise enough to fix in-turn.

The fixture is Molly's actual first plan block from session fe89b099b3364e4ca147c273b7cd3810
(the transcript format is gone; its ``result`` object is what factory_submit_result would
now receive). With its missing brace restored, ``resolved_decisions`` holds free-text design
choices instead of owner-answered decision objects and ``open_decision_ids`` sits inside
``contract``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.core.types import Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_pilot_677 import eventually

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pilot_677_plan_rejected.txt"
MOLLY = Correlation(
    "I_kwDOTC12Fs8AAAABS6JIGw",
    "ss_1a39c255ac30dfc47ad711b1",
    "d13d07c80222e7dec400bba84b026fff",
    3,
    "plan",
)


def _errors(result: object) -> tuple[str, ...]:
    with pytest.raises(ResultError) as caught:
        validate_result(result, MOLLY)  # type: ignore[arg-type]
    return caught.value.details


def _molly_result() -> dict[str, object]:
    lines = FIXTURE.read_text().splitlines()
    raw = lines[lines.index("```factory-result") + 1] + "}"  # the missing closing brace
    envelope = json.loads(raw)
    return envelope["result"]


def test_molly_plan_reports_each_schema_mismatch():
    details = _errors(_molly_result())
    assert details[:4] == tuple(
        f"result.contract.resolved_decisions.{i}: "
        "Input should be a valid dictionary or instance of ResolvedDecision"
        for i in range(4)
    )
    assert "result.contract.open_decision_ids: Extra inputs are not permitted" in details
    assert "result.open_decision_ids: Field required" in details
    # Locations and messages only: none of Molly's rejected text is echoed back.
    assert not any("Classified SMALL" in d for d in details)
    assert all(len(d) <= 200 for d in details)


def test_error_details_are_capped():
    details = _errors({"kind": "plan", **{f"x{i}": i for i in range(30)}})
    assert len(details) == 11 and details[-1].startswith("... and ")


def test_blocked_result_validates_for_any_stage():
    body = {
        "kind": "blocked",
        "reason": "broker refused: core-work-gate-closed; candidate staged as tree ce3388c",
        "done": ["3 files, 20 tests", "Codex review: no findings"],
    }
    for stage in ("triage", "plan", "build"):
        corr = Correlation(MOLLY.parcel_id, MOLLY.stage_session_id, MOLLY.dispatch_nonce, 3, stage)
        assert validate_result(body, corr).result.result.kind == "blocked"
    from omnigent_factory.service.directory import _public_result

    text = _public_result(body)
    assert "> broker refused: core-work-gate-closed" in text and "Codex review" in text


@pytest.mark.asyncio
async def test_operator_note_renders_for_an_existing_session(service_config: ServiceConfig):
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        factory = EventFactory("P-note", issue_number=677)
        await service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))

        async def created() -> bool:
            parcel = await service.db.call(lambda store: store.load_parcel("P-note"))
            return bool(parcel and parcel.current_session and parcel.current_session.root_id)

        await eventually(created)
        parcel = await service.db.call(lambda store: store.load_parcel("P-note"))
        sid = parcel.current_session.session_id
        directory = ServiceDispatchDirectory(service.db, service_config)
        text = await directory.message_text(
            EffectIntent(
                effect_id="ef_note",
                kind=EffectKind.SEND_MESSAGE,
                parcel_id="P-note",
                target=sid,
                preconditions=Preconditions(1, 0, session_id=sid),
                args={"purpose": "operator_note", "text": "Publish the staged candidate."},
            )
        )
        assert text is not None and "Publish the staged candidate." in text
        assert "does not widen your approved" in text and "factory_submit_result" in text
    finally:
        await service.stop()
