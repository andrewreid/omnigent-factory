"""Owner comments through the factory MCP tools: cumulative context and the submit gate.

``factory_get_feedback`` serves every owner comment on the issue (oldest first, ``new``
since this session's last result for the stage), and a result is refused until the run
has read every recorded comment, so comments made during a run fold into it.
"""

from __future__ import annotations

import json
from dataclasses import replace
from importlib.resources import files
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.types import Lifecycle, SessionKind, Stage, Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.mcp import FactoryToolError
from omnigent_factory.store.sqlite import DeliveryRecord
from tests.service.test_mcp import (
    PLAN,
    SHA,
    TRIAGE,
    Rig,
    build_ready,
    building,
    planning,
    started,
    triaged,
)
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio


async def say(rig: Rig, text: str, body: ev.EventBody | None = None) -> Any:
    """An owner issue comment as the webhook records it (delivery body + control event)."""
    rig.factory.tick()
    guid = f"d-comment-{rig.factory.now}"
    payload = {"action": "created", "comment": {"body": text}}
    await rig.service.db.call(
        lambda store: store.append_delivery(
            DeliveryRecord(guid, "issue_comment", json.dumps(payload).encode(), {})
        )
    )
    event = rig.factory.make(body or ev.PlanFeedback(text_digest=text))
    return await rig.service.apply_event(replace(event, delivery_guid=guid))


def texts(feedback: dict[str, Any]) -> list[tuple[str, bool]]:
    out = []
    for c in feedback["owner_comments"]:
        # Strip the untrusted frame: BEGIN ... \n<text>\nEND ...
        out.append((c["text"].split("\n")[1], c["new"]))
    return out


async def submits_messages(rig: Rig, purpose: str) -> list[Any]:
    return [e for e in rig.executed(EffectKind.SEND_MESSAGE) if e.args.get("purpose") == purpose]


# ------------------------------------------------------------------ A: cumulative context


async def test_A_plan_run_reads_comments_made_before_the_drag(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await triaged(rig)
        await rig.submit(root, "triage", TRIAGE)
        await rig.quiesce()
        await say(rig, "only fix A, B and D; C is not proceeding")
        # The comment re-ran triage; the owner drags to Scoped while it runs.
        await rig.active(SessionKind.TRIAGE)
        rig.service.clock.advance(5_000_000)
        rig.factory.tick(10_000_000)
        await rig.send(ev.RequestPlan(via=Via.DRAG))
        await rig.quiesce()
        root = await rig.active(SessionKind.PLAN)
        rig.factory.tick(10_000_000)
        await say(rig, "and keep the v1 API")
        feedback = await rig.tools.get_feedback(root)
        assert texts(feedback) == [
            ("only fix A, B and D; C is not proceeding", False),
            ("and keep the v1 API", True),
        ]
        assert feedback["stage"] == "plan" and "overriding" in feedback["note"]
        [first, _] = feedback["owner_comments"]
        assert first["text"].startswith("BEGIN OWNER COMMENT FACTORY_DATA_")


async def test_A_result_is_refused_until_every_comment_is_read(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        await say(rig, "use the existing helper")
        with pytest.raises(FactoryToolError, match="factory_get_feedback"):
            await rig.submit(root, "plan", PLAN)
        await rig.tools.get_feedback(root)
        await say(rig, "also keep the API")  # arrives after the read
        with pytest.raises(FactoryToolError, match="factory_get_feedback"):
            await rig.submit(root, "plan", PLAN)
        await rig.tools.get_feedback(root)
        assert (await rig.submit(root, "plan", PLAN))["accepted"] is True
        # One burst while the turn ran: no feedback message was queued for it.
        assert await submits_messages(rig, "feedback") == []
        # "blocked" is never gated.
        await say(rig, "hmm")
        assert (await rig.submit(root, "blocked", {"reason": "x", "done": []}))["accepted"]


async def test_A_stage_templates_point_at_owner_feedback():
    root = files("omnigent_factory.service") / "templates"
    for name in (
        "triage-v5.txt",
        "triage-feedback-v3.txt",
        "plan-v5.txt",
        "build-v6.txt",
        "feedback-v3.txt",
        "build-feedback-v2.txt",
        "build-rework-v3.txt",
        "triage-comment-v1.txt",
        "continuation-v2.txt",
    ):
        text = (root / name).read_text("utf-8")
        assert "factory_get_feedback" in text, name
        assert len(text) < 1200, name
    for name in (
        "triage-v5.txt",
        "triage-feedback-v3.txt",
        "plan-v5.txt",
        "build-v6.txt",
        "build-rework-v3.txt",
    ):
        # The stage prompts say results are written for a human reading GitHub.
        text = " ".join((root / name).read_text("utf-8").split())
        assert "posted for a human: GitHub Markdown, short paragraphs" in text, name
    for gone in (
        "triage-v3.txt",
        "triage-feedback-v1.txt",
        "plan-v3.txt",
        "build-v4.txt",
        "build-rework-v1.txt",
        "triage-v2.txt",
        "plan-v2.txt",
        "build-v2.txt",
        "build-v3.txt",
        "feedback-v2.txt",
        "build-feedback-v1.txt",
        "readiness-wake-v2.txt",
        "build-v5.txt",
        "plan-v4.txt",
        "triage-v4.txt",
        "build-rework-v2.txt",
        "triage-feedback-v2.txt",
        "readiness-wake-v3.txt",
    ):
        assert not (root / gone).is_file()


# ------------------------------------------------------------------ B: re-triage


async def test_B_owner_comment_reruns_triage_and_publishes_a_revision(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root = await triaged(rig)
        await rig.submit(root, "triage", TRIAGE)
        await rig.quiesce()
        first = await rig.run()

        async def published(n: int) -> bool:
            return len(rig.executed(EffectKind.PUBLISH_TRIAGE)) >= n

        await eventually(lambda: published(1))
        await say(rig, "you missed X, Y and Z; try again")
        again = await rig.active(SessionKind.TRIAGE)
        rerun = await rig.run()
        assert again == root and rerun.session_id != first.session_id
        parcel = await rig.parcel()
        assert parcel.stage == Stage.TRIAGED

        def starts() -> list[Any]:
            return [
                e
                for e in rig.executed(EffectKind.SEND_MESSAGE)
                if e.preconditions.session_id == rerun.session_id
            ]

        async def started_rerun() -> bool:
            return bool(starts())

        await eventually(started_rerun)
        [start] = starts()
        text = await rig.tools.directory.message_text(start)
        assert text is not None and "commented on the triage" in " ".join(text.split())
        assert rerun.session_id in text and root in text
        feedback = await rig.tools.get_feedback(root)
        assert texts(feedback) == [("you missed X, Y and Z; try again", True)]
        revised = {**TRIAGE, "summary": "Guard is untested; X, Y and Z too."}
        assert (await rig.submit(root, "triage", revised))["accepted"] is True
        await eventually(lambda: published(2))
        # Only one run was ever started per request; one issue session throughout.
        assert {s.root_id for s in (await rig.parcel()).sessions} == {root}


# ------------------------------------------------------------------ D: build


async def test_D_owner_comment_nudges_waiting_build_within_approval(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, plan_hash = await building(rig)
        await rig.tools.get_plan(root)
        await rig.tools.get_feedback(root)
        await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        run = await rig.run()
        assert run.lifecycle == Lifecycle.WAITING
        approval = (await rig.parcel()).current_approval_id
        await say(rig, "rename the flag to --dry-run")

        async def nudged() -> bool:
            return bool(await submits_messages(rig, "feedback"))

        await eventually(nudged)
        [msg] = await submits_messages(rig, "feedback")
        text = await rig.tools.directory.message_text(msg)
        assert text is not None and "within the approved plan" in text
        assert "factory_ask_owner" in text
        parcel = await rig.parcel()
        assert parcel.current_approval_id == approval and parcel.stage == Stage.BUILDING
        assert (await rig.run()).session_id == run.session_id
        # Gate first, then the same head can be re-submitted (a fresh slot).
        with pytest.raises(FactoryToolError, match="factory_get_feedback"):
            await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        feedback = await rig.tools.get_feedback(root)
        assert texts(feedback)[-1] == ("rename the flag to --dry-run", True)
        receipt = await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        assert receipt["accepted"] is True and receipt["slot"] == f"build-{SHA}-f1"
        assert (await rig.run()).lifecycle == Lifecycle.WAITING


# ------------------------------------------------------------------ F: commands


async def test_F_command_comments_do_not_gate_or_rerun(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        await say(rig, "only A")
        with pytest.raises(FactoryToolError, match="factory_get_feedback"):
            await rig.submit(root, "plan", PLAN)
        await rig.tools.get_feedback(root)
        run = await rig.run()
        # A command keeps its own semantics (here: nothing to continue) and is not
        # feedback: it neither blocks the result nor sends a message nor starts a run.
        await say(rig, "/continue", ev.Continue())
        assert (await rig.submit(root, "plan", PLAN))["accepted"] is True
        assert await submits_messages(rig, "feedback") == []
        assert (await rig.run()).session_id == run.session_id


# ------------------------------------------------------------------ reply answers


async def test_owner_reply_is_the_answer_relayed_to_the_run(service_config: ServiceConfig):
    """A plain owner comment while a question is open answers it: the relayed message and
    factory_get_feedback carry the comment's own text (the decision stores no copy)."""
    async with started(service_config) as rig:
        root = await planning(rig)
        asked = await rig.ask(root, "Keep the v1 API?", options=["keep", "drop"])
        await rig.quiesce()
        rig.factory.tick(10_000_000)
        result = await say(rig, "Keep it.\n\n- v1 stays for a release")
        assert result.accepted
        parcel = await rig.parcel()
        decision = parcel.decision(asked["decision_id"])
        assert decision is not None and decision.answer is None
        assert decision.status.value == "relayed" and not parcel.open_decisions

        async def relayed() -> bool:
            return bool(await submits_messages(rig, "answer_relay"))

        await eventually(relayed)
        [relay] = await submits_messages(rig, "answer_relay")
        text = await rig.tools.directory.message_text(relay)
        assert text is not None and "Keep it.\n\n- v1 stays for a release" in text
        assert not await submits_messages(rig, "feedback")  # not also relayed as feedback
        feedback = await rig.tools.get_feedback(root)
        [answer] = [d for d in feedback["decisions"] if d["decision_id"] == decision.decision_id]
        assert "Keep it." in answer["answer"]


async def test_a_reapplied_comment_delivery_is_listed_once(service_config: ServiceConfig):
    """Operator recovery re-applies a recorded comment's delivery under a new logical id
    (e.g. #477 comment 5903549471 as the answer): the comment is still one comment."""
    async with started(service_config) as rig:
        root = await planning(rig)
        await rig.ask(root, "Keep the v1 API?")
        await rig.quiesce()
        rig.factory.tick(10_000_000)
        guid = f"d-comment-{rig.factory.now + 1}"
        payload = {"action": "created", "comment": {"body": "keep it"}}
        await rig.service.db.call(
            lambda store: store.append_delivery(
                DeliveryRecord(guid, "issue_comment", json.dumps(payload).encode(), {})
            )
        )
        first = rig.factory.make(ev.PlanFeedback(text_digest="keep it"))
        again = replace(
            rig.factory.make(ev.PlanFeedback(text_digest="keep it")),
            event_id="recovery:comment:answer",
            provenance=ev.Provenance.RECOVERY,
        )
        for event in (first, again):
            await rig.service.apply_event(replace(event, delivery_guid=guid))
        feedback = await rig.tools.get_feedback(root)
        assert [t for t, _ in texts(feedback)].count("keep it") == 1


async def test_a_reapplied_comment_delivery_does_not_gate_a_read_run(
    service_config: ServiceConfig,
):
    """#477: the owner's comment was read, then operator recovery re-applied its delivery
    under a new logical id. The re-application is not a new comment: it must not refuse
    build_ready, before or after another factory_get_feedback."""
    async with started(service_config) as rig:
        root, plan_hash = await building(rig)
        await rig.tools.get_plan(root)
        rig.factory.tick(10_000_000)
        guid = f"d-comment-{rig.factory.now + 1}"
        payload = {"action": "created", "comment": {"body": "keep it"}}
        await rig.service.db.call(
            lambda store: store.append_delivery(
                DeliveryRecord(guid, "issue_comment", json.dumps(payload).encode(), {})
            )
        )
        first = rig.factory.make(ev.PlanFeedback(text_digest="keep it"))
        await rig.service.apply_event(replace(first, delivery_guid=guid))
        await rig.tools.get_feedback(root)
        again = replace(
            rig.factory.make(ev.PlanFeedback(text_digest="keep it")),
            event_id="recovery:github:comment:1:answer",
            provenance=ev.Provenance.RECOVERY,
            delivery_guid=guid,
        )
        assert (await rig.service.apply_event(again)).accepted
        receipt = await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        assert receipt["accepted"] is True
        # A fresh read covers the re-application too: the next result is not gated.
        feedback = await rig.tools.get_feedback(root)
        assert [t for t, _ in texts(feedback)] == ["keep it"]
        await say(rig, "hmm")  # a genuinely new owner comment still gates
        with pytest.raises(FactoryToolError, match="factory_get_feedback"):
            await rig.submit(root, "build_ready", build_ready("f" * 40), plan_hash=plan_hash)
        await rig.tools.get_feedback(root)
        receipt = await rig.submit(root, "build_ready", build_ready("f" * 40), plan_hash=plan_hash)
        assert receipt["accepted"] is True
