"""Pilot #677 plan rejection: validation errors reach the agent and the owner.

The fixture is Molly's actual first plan block from session fe89b099b3364e4ca147c273b7cd3810.
It is one closing brace short, which also puts ``open_decision_ids`` inside ``contract``;
with the brace restored, ``resolved_decisions`` holds free-text design choices instead of
owner-answered decision objects.
"""

from __future__ import annotations

import json
from importlib.resources import files
from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.protocol import (
    Correlation,
    ResultError,
    extract_result_block,
    parse_factory_result,
)
from omnigent_factory.core.types import Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import PublicationRenderer, ServiceDispatchDirectory
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


def _errors(text: str) -> tuple[str, ...]:
    with pytest.raises(ResultError) as caught:
        parse_factory_result(text, MOLLY)
    return caught.value.details


def test_molly_block_reports_the_json_error_with_location():
    [detail] = _errors(FIXTURE.read_text())
    assert detail.startswith("invalid JSON: Expecting ',' delimiter at line 1 column ")
    assert "closing brace or bracket is probably missing" in detail


def test_molly_block_with_brace_restored_reports_each_schema_mismatch():
    raw = extract_result_block(FIXTURE.read_text()) + "}"
    details = _errors(f"FACTORY_RESULT_V1\n```factory-result\n{raw}\n```\n")
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
    body = {
        "version": 1,
        "parcel_id": MOLLY.parcel_id,
        "stage_session_id": MOLLY.stage_session_id,
        "dispatch_nonce": MOLLY.dispatch_nonce,
        "revision": 3,
        "result": {"kind": "plan", **{f"x{i}": i for i in range(30)}},
    }
    details = _errors(f"FACTORY_RESULT_V1\n```factory-result\n{json.dumps(body)}\n```\n")
    assert len(details) == 11 and details[-1].startswith("... and ")


def test_templates_state_field_shapes():
    root = files("omnigent_factory.service") / "templates"
    plan = (root / "plan-v1.txt").read_text(encoding="utf-8")
    assert '{{"decision_id","answer","source_event_id"}}' in plan
    assert "open_decision_ids: at the result level (not inside contract)" in plan
    assert "own design choices" in plan
    for name in ("triage-v1.txt", "plan-v1.txt", "build-v1.txt", "checkpoint-v1.txt"):
        assert "use exactly the keys shown, no others" in (root / name).read_text("utf-8")
    assert "{errors}" in (root / "correction-v1.txt").read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_correction_message_and_blocked_comment_carry_the_errors(
    service_config: ServiceConfig,
):
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=FakeClock(),
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        factory = EventFactory("P-result", issue_number=677)
        await service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))

        async def created() -> bool:
            parcel = await service.db.call(lambda store: store.load_parcel("P-result"))
            return bool(parcel and parcel.current_session and parcel.current_session.root_id)

        await eventually(created)
        parcel = await service.db.call(lambda store: store.load_parcel("P-result"))
        sid = parcel.current_session.session_id
        directory = ServiceDispatchDirectory(service.db, service_config)
        details = _errors(FIXTURE.read_text())
        await directory.save_rejection(sid, "item-1", "plan", details)

        def effect(kind: EffectKind, **args: object) -> EffectIntent:
            return EffectIntent(
                effect_id="ef_x",
                kind=kind,
                parcel_id="P-result",
                target=sid,
                preconditions=Preconditions(1, 0, session_id=sid),
                args=args,
            )

        message = await directory.message_text(
            effect(EffectKind.SEND_MESSAGE, purpose="correction")
        )
        assert message is not None and f"- {details[0]}" in message
        assert "fixing exactly these errors" in message

        comment = await PublicationRenderer(directory, service_config)(
            effect(EffectKind.POST_COMMENT, template="result-invalid", session_id=sid)
        )
        assert comment is not None
        assert "the plan result failed validation" in comment
        assert "Expecting ',' delimiter" in comment and "`/plan`" in comment
    finally:
        await service.stop()


def test_blocked_result_parses_for_any_stage_and_pr_zero_is_still_invalid():
    body = {
        "version": 1,
        "parcel_id": MOLLY.parcel_id,
        "stage_session_id": MOLLY.stage_session_id,
        "dispatch_nonce": MOLLY.dispatch_nonce,
        "revision": 3,
        "result": {
            "kind": "blocked",
            "reason": "broker refused: core-work-gate-closed; candidate staged as tree ce3388c",
            "done": ["3 files, 20 tests", "Codex review: no findings"],
        },
    }
    for stage in ("triage", "plan", "build"):
        corr = Correlation(MOLLY.parcel_id, MOLLY.stage_session_id, MOLLY.dispatch_nonce, 3, stage)
        parsed = parse_factory_result(
            f"FACTORY_RESULT_V1\n```factory-result\n{json.dumps(body)}\n```\n", corr
        )
        assert parsed.result.result.kind == "blocked"
    from omnigent_factory.service.directory import _public_result

    text = _public_result(body["result"])
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
        assert "does\nnot widen your approved scope" in text
    finally:
        await service.stop()
