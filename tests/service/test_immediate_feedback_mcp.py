"""#884 through the service: a busy build gets the mid-build message, and the comment is
marked 👀 when taken and 👍 once ``factory_get_feedback`` served it to the run."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store.sqlite import DeliveryRecord
from tests.service.test_mcp import Rig, building, started
from tests.service.test_owner_feedback_mcp import submits_messages
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio

COMMENT_ID = 6096243669


async def say(rig: Rig, text: str, comment_id: int) -> None:
    """An owner issue comment exactly as the webhook records it (its GitHub ID)."""
    rig.factory.tick()
    guid = f"d-comment-{comment_id}"
    payload = {"action": "created", "comment": {"id": comment_id, "body": text}}
    await rig.service.db.call(
        lambda store: store.append_delivery(
            DeliveryRecord(guid, "issue_comment", json.dumps(payload).encode(), {})
        )
    )
    event = rig.factory.make(
        ev.PlanFeedback(text_digest=text), event_id=f"github:comment:{comment_id}:created"
    )
    result = await rig.service.apply_event(replace(event, delivery_guid=guid))
    assert result.accepted


def reacted(rig: Rig) -> list[tuple[int, str]]:
    return [
        (e.args["comment_id"], e.args["content"]) for e in rig.executed(EffectKind.REACT_COMMENT)
    ]


async def test_busy_build_gets_mid_build_message_and_reactions_follow_the_read(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root, _plan_hash = await building(rig)
        assert not (await rig.run()).quiescent  # its turn (or its workers) still running
        await say(rig, "use the SQL admin group, not a password", COMMENT_ID)

        async def relayed() -> bool:
            return bool(await submits_messages(rig, "feedback"))

        await eventually(relayed)
        [msg] = await submits_messages(rig, "feedback")
        assert msg.args["midturn"] is True
        text = await rig.tools.directory.message_text(msg)
        assert text is not None and "mid-build" in text
        for needed in ("factory_get_feedback", "factory_ask_owner", "approved plan", "workers"):
            assert needed in text, needed

        async def eyes() -> bool:
            return (COMMENT_ID, "eyes") in reacted(rig)

        await eventually(eyes)
        parcel = await rig.parcel()
        assert parcel.unread_feedback == (f"github:comment:{COMMENT_ID}:created",)
        assert parcel.note.startswith("Feedback received")

        await rig.tools.get_feedback(root)

        async def thumbs() -> bool:
            return (COMMENT_ID, "+1") in reacted(rig)

        await eventually(thumbs)
        parcel = await rig.parcel()
        assert parcel.unread_feedback == () and parcel.note == ""
        # A second read: nothing unread, no event and no second 👍.
        before = len(reacted(rig))
        await rig.tools.get_feedback(root)
        assert len(reacted(rig)) == before
        assert reacted(rig).count((COMMENT_ID, "+1")) == 1
