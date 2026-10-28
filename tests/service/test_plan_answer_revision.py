"""#342: an open plan question, refusal, answer, revision and successful approval."""

from dataclasses import replace
from importlib.resources import files

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.types import (
    DecisionStatus,
    FenceKind,
    Hold,
    Lifecycle,
    SessionKind,
    Via,
    WaitReason,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import _ANSWER_TEMPLATE, PublicationRenderer
from omnigent_factory.service.mcp import _expected_next
from omnigent_factory.testing.harness import Harness
from tests.service.test_mcp import PLAN, building, planning, started
from tests.service.test_owner_feedback_mcp import say
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("via", [Via.COMMAND, Via.DRAG])
async def test_342_question_answer_revises_published_plan(service_config: ServiceConfig, via: Via):
    async with started(service_config) as rig:
        root = await planning(rig)
        question = await rig.ask(root, "Keep the API?", impact="plan_revision")
        decision_id = question["decision_id"]
        rig.github.script(
            EffectKind.PUBLISH_CONTRACT,
            Ack(remote_id="plan-1", detail={"verified": True, "posted_at_us": 1}),
        )
        receipt = await rig.submit(root, "plan", {**PLAN, "open_decision_ids": [decision_id]})
        assert receipt["accepted"]
        assert "cannot be approved" in receipt["warning"]
        assert "owner answers" in receipt["warning"] and "on GitHub" in receipt["warning"]
        assert decision_id in receipt["warning"]
        assert "Do not mark them resolved yourself" in receipt["warning"]

        async def published(revision: int) -> bool:
            contract = (await rig.parcel()).current_contract
            return contract is not None and contract.revision == revision

        await eventually(lambda: published(receipt["revision"]))
        [publish] = rig.executed(EffectKind.PUBLISH_CONTRACT)
        text = await PublicationRenderer(rig.tools.directory, service_config)(publish)
        assert text is not None
        assert all(bad not in text for bad in ("Factory:", "Next:", "/approve", "\n> ", "/c/"))
        assert "Approval is blocked until the owner answers the open question(s) on GitHub" in text
        refused = await rig.send(ev.ApprovePlan(via=via))
        assert refused.reason == "plan-not-approvable"
        assert f"an owner question is open ({decision_id})" in (await rig.parcel()).note
        before = await rig.run()
        assert before.wait_reason == WaitReason.PLAN_APPROVAL
        await say(rig, "Keep the API.")
        run = await rig.run()
        assert run.lifecycle == Lifecycle.ACTIVE and run.wait_reason is None
        assert run.session_id == before.session_id and run.grant == before.grant
        parcel = await rig.parcel()
        assert parcel.revision_pending and parcel.revision == receipt["revision"] + 1
        assert Hold.AWAITING_OWNER not in parcel.holds and not parcel.open_decisions
        assert parcel.decision(decision_id).status == DecisionStatus.RELAYED

        async def relayed() -> bool:
            return any(
                e.args.get("purpose") == "answer_relay"
                for e in rig.executed(EffectKind.SEND_MESSAGE)
            )

        await eventually(relayed)
        [relay] = [
            e
            for e in rig.executed(EffectKind.SEND_MESSAGE)
            if e.args.get("purpose") == "answer_relay"
        ]
        message = await rig.tools.directory.message_text(relay)
        assert message is not None and "Keep the API." in message
        assert "Apply the owner's answer and resubmit kind plan" in message
        assert "always fill eli5" in message
        status = await rig.tools.get_status(root)
        assert (
            status["expected_next"] == "Apply the owner's answer/feedback and resubmit kind plan."
        )
        await rig.tools.get_feedback(root)
        rig.github.script(
            EffectKind.PUBLISH_CONTRACT,
            Ack(remote_id="plan-2", detail={"verified": True, "posted_at_us": 2}),
        )
        revised = await rig.submit(root, "plan", {**PLAN, "approach": "Keep the API."})
        assert revised["accepted"] and "warning" not in revised
        await eventually(lambda: published(revised["revision"]))
        assert not (await rig.parcel()).revision_pending
        assert (await rig.send(ev.ApprovePlan(via=via))).accepted


async def test_build_answer_keeps_authority_and_existing_relay_wording(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, _ = await building(rig)
        before = await rig.parcel()
        await rig.ask(root, "Keep the label?", impact="within_contract")
        result = await say(rig, "Keep it.")
        parcel = await rig.parcel()
        assert parcel.revision == before.revision and not parcel.revision_pending
        assert parcel.current_approval == before.current_approval
        assert (await rig.run()).lifecycle == Lifecycle.ACTIVE
        [relay] = [e for e in result.effects if e.args.get("purpose") == "answer_relay"]
        message = await rig.tools.directory.message_text(relay)
        old = (files("omnigent_factory.service") / "templates" / "answer-v2.txt").read_text()
        assert message is not None
        assert (
            message.strip()
            == old.format(decision_id=relay.args["decision_id"], answer="Keep it.").strip()
        )


@pytest.mark.parametrize("wait_reason", [None, *WaitReason])
@pytest.mark.parametrize("hold", [None, Hold.AGENT_BLOCKED, Hold.BLOCKERS])
async def test_pending_revision_status_precedes_wait_branches(wait_reason, hold):
    h = Harness()
    h.plan_published()
    parcel = replace(h.p(), revision_pending=True, holds=frozenset({hold}) if hold else frozenset())
    run = replace(
        h.cur(),
        lifecycle=Lifecycle.WAITING if wait_reason else Lifecycle.ACTIVE,
        wait_reason=wait_reason,
    )
    assert "resubmit kind plan" in _expected_next(parcel, run)


async def test_pending_revision_status_keeps_execution_barriers():
    h = Harness()
    h.plan_published()
    parcel = replace(h.p(), revision_pending=True)
    assert "stopped" in _expected_next(
        parcel, replace(h.cur(), fences=frozenset({FenceKind.STOPPED}))
    )
    assert "Checkpoint" in _expected_next(
        parcel, replace(h.cur(), lifecycle=Lifecycle.CHECKPOINT_GRACE)
    )
    assert "closed" in _expected_next(parcel, replace(h.cur(), lifecycle=Lifecycle.CHECKPOINT_WAIT))
    assert "resubmit" not in _expected_next(parcel, replace(h.cur(), kind=SessionKind.BUILD))


async def test_pending_revision_status_mentions_remaining_owner_questions():
    h = Harness()
    h.plan_published()
    h.send(
        "I_parcel_1",
        ev.OwnerQuestion(session_id=h.cur().session_id, question_key="q", summary="Which?"),
    )
    next_step = _expected_next(replace(h.p(), revision_pending=True), h.cur())
    assert "resubmit kind plan" in next_step
    assert "Remaining open questions still need the owner's answer on GitHub" in next_step


async def test_answer_template_pin_preserves_released_version():
    assert _ANSWER_TEMPLATE == "answer-v3.txt"
    templates = files("omnigent_factory.service") / "templates"
    old = (templates / "answer-v2.txt").read_text()
    new = (templates / _ANSWER_TEMPLATE).read_text()
    assert new.startswith(old)
    assert "Apply the owner's answer and resubmit kind plan" in new
