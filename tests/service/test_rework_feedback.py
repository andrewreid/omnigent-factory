"""Rework end to end: owner PR feedback on a Ready card reopens the build in the issue
session, and ``factory_get_feedback`` serves the owner's PR reviews (with their inline
comments) and PR conversation comments next to issue comments, oldest first.

Also the webhook side: which GitHub deliveries are owner feedback, and the delivery
processor reading a review's inline comments (the pull_request_review webhook carries
only the review body; the App has no pull_request_review_comment subscription).
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind, MessagePurpose
from omnigent_factory.core.types import Lifecycle, SessionKind, Stage
from omnigent_factory.github.webhook import DeliveryNormalizer
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.github_delivery import GitHubDeliveryProcessor
from omnigent_factory.store.sqlite import DeliveryRecord
from omnigent_factory.testing.builders import snapshot
from tests.service.test_mcp import SHA, Rig, build_ready, building, started
from tests.service.test_pilot_677 import eventually
from tests.service.test_webhook_mapping import IDENTITY, _common

OWNER = 114979


def review_payload(
    state: str, body: str | None, *, sender: int = OWNER, action: str = "submitted"
) -> dict[str, Any]:
    common = _common(sender)
    common["sender"] = {"id": sender, "login": "someone", "type": "User"}
    return {
        **common,
        "action": action,
        "review": {
            "id": 9001,
            "state": state,
            "body": body,
            "submitted_at": "2030-01-01T00:00:00Z",
        },
        "pull_request": {
            "number": 685,
            "head": {"ref": "factory/issue-677", "sha": SHA},
        },
    }


def pr_comment_payload(text: str, *, sender: int = OWNER) -> dict[str, Any]:
    common = _common(sender)
    common["sender"] = {"id": sender, "login": "someone", "type": "User"}
    return {
        **common,
        "action": "created",
        "issue": {"number": 685, "node_id": "PR_node", "pull_request": {"url": "x"}},
        "comment": {"id": 55, "body": text, "created_at": "2026-09-29T10:00:00Z"},
    }


def normalize(event_name: str, payload: dict[str, Any]) -> tuple[ev.EventBody, ...]:
    out = DeliveryNormalizer(IDENTITY).normalize(
        raw_body=json.dumps(payload).encode(),
        event_name=event_name,
        delivery_guid="g-1",
        delivery_time_us=1,
    )
    for event in out.events:
        assert event.parcel_id is None and event.issue_number is None  # resolved by PR
    return tuple(e.body for e in out.events)


# ------------------------------------------------------------------ webhook mapping


def test_owner_reviews_that_say_something_are_feedback():
    for state, body in (
        ("changes_requested", "No, don't do that"),
        ("commented", ""),  # inline comments only
        ("approved", "Approve, but rename the flag"),
    ):
        [fb] = normalize("pull_request_review", review_payload(state, body))
        assert isinstance(fb, ev.PlanFeedback) and fb.pr_number == 685, state
    out = DeliveryNormalizer(IDENTITY).normalize(
        raw_body=json.dumps(review_payload("commented", "x")).encode(),
        event_name="pull_request_review",
        delivery_guid="g-1",
        delivery_time_us=1,
    )
    assert out.events[0].event_id == "github:review:9001:submitted"  # one per review


def test_approval_without_text_and_other_reviewers_are_not_feedback():
    for payload in (
        review_payload("approved", None),
        review_payload("approved", "  "),
        review_payload("changes_requested", "fix it", sender=4242),  # not an owner
        review_payload("commented", "bot", sender=IDENTITY.bot_user_id),
        review_payload("changes_requested", "x", action="dismissed"),
    ):
        [body] = normalize("pull_request_review", payload)
        assert isinstance(body, ev.ReviewChanged)


def test_owner_pr_conversation_comment_is_feedback_commands_and_others_are_not():
    [fb] = normalize("issue_comment", pr_comment_payload("do X instead"))
    assert isinstance(fb, ev.PlanFeedback) and fb.pr_number == 685
    assert normalize("issue_comment", pr_comment_payload("/approve")) == ()
    assert normalize("issue_comment", pr_comment_payload("do X", sender=4242)) == ()
    edited = {**pr_comment_payload("do X"), "action": "edited"}
    assert normalize("issue_comment", edited) == ()


# ------------------------------------------------------------------ processor


class _GitHub:
    repository_node_id = "R_kgDOTC12Fg"

    def __init__(self) -> None:
        self.reads: list[tuple[int, int]] = []

    async def issue_snapshot(self, ref: Any) -> Any:
        return snapshot(read_at_us=1)

    async def review_comments(self, pr_number: int, review_id: int) -> list[dict[str, Any]]:
        self.reads.append((pr_number, review_id))
        return [{"path": "web/app.ts", "line": 12, "body": "use the helper", "created_at": "t"}]


async def deliver_review(rig: Rig, payload: dict[str, Any], guid: str) -> _GitHub:
    github = _GitHub()
    processor = GitHubDeliveryProcessor(
        rig.service,
        DeliveryNormalizer(replace(IDENTITY, repository_node_id=rig.service.config.repo_id)),
        github,  # type: ignore[arg-type]
        rig.service.clock,
    )
    record = DeliveryRecord(guid, "pull_request_review", json.dumps(payload).encode(), {})
    await rig.service.db.call(lambda store: store.append_delivery(record))
    await processor.process(record)
    return github


# ------------------------------------------------------------------ end to end


async def to_ready(rig: Rig) -> tuple[str, str]:
    root, plan_hash = await building(rig)
    rig.github.script(EffectKind.FETCH_PR_EVIDENCE, Ack("685", {"verified": True, "head_sha": SHA}))
    await rig.tools.get_plan(root)
    await rig.tools.get_feedback(root)
    await rig.submit(root, "build_ready", {**build_ready(), "pr_number": 685}, plan_hash=plan_hash)
    await rig.send(
        ev.PRObserved(pr_number=685, head_sha=SHA, open=True, bot_authored=True, parcel_branch=True)
    )
    await rig.quiesce()

    async def is_ready() -> bool:
        return (await rig.parcel()).stage == Stage.READY

    await eventually(is_ready)
    return root, plan_hash


@pytest.mark.asyncio
async def test_owner_review_on_ready_reworks_in_the_same_session_with_pr_feedback(
    service_config: ServiceConfig,
):
    # One open-PR slot, already used by this PR: a rework needs no new PR slot.
    config = service_config.model_copy(
        update={"review_bot_grace_minutes": 0, "max_open_bot_prs": 1}
    )
    async with started(config) as rig:
        root, _ = await to_ready(rig)
        parcel = await rig.parcel()
        approval, old = parcel.current_approval_id, parcel.current_session
        rig.factory.tick(60_000_000)
        payload = review_payload("changes_requested", "No, don't do that; do X instead")
        payload["repository"] = {
            "id": IDENTITY.repository_id,
            "node_id": rig.service.config.repo_id,
            "full_name": IDENTITY.repository_full_name,
        }
        github = await deliver_review(rig, payload, "d-review")
        assert github.reads == [(685, 9001)]

        async def reworking() -> bool:
            rig.service.clock.advance(1_000_000)  # admission attempts are keyed by time
            run = await rig.run()
            return bool(
                run
                and run.kind == SessionKind.BUILD
                and run.session_id != old.session_id
                and run.lifecycle == Lifecycle.ACTIVE
            )

        await eventually(reworking)
        parcel = await rig.parcel()
        run = parcel.current_session
        assert parcel.stage == Stage.BUILDING and parcel.current_approval_id == approval
        assert run.root_id == root  # the same issue session is woken

        def firsts() -> list[Any]:
            return [
                e
                for e in rig.executed(EffectKind.SEND_MESSAGE)
                if e.preconditions.session_id == run.session_id
                and e.args.get("purpose") == MessagePurpose.FIRST.value
            ]

        await eventually(firsts)
        [first] = firsts()
        text = await rig.tools.directory.message_text(first)
        assert text is not None and "rework" in text and "factory_get_feedback" in text
        assert "PR #685" in text and "factory_ask_owner" in text
        feedback = await rig.tools.get_feedback(root)
        [review] = [c for c in feedback["owner_comments"] if c["source"] == "pr_review"]
        assert review["new"] is True and review["pr_number"] == 685
        body = review["text"]
        assert body.startswith("BEGIN OWNER COMMENT FACTORY_DATA_")
        assert "No, don't do that; do X instead" in body
        assert "- web/app.ts:12: use the helper" in body
