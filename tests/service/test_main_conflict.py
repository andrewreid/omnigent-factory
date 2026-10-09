"""The ``push`` webhook end to end: a push to the default branch re-reads every open
factory PR; a conflict on a Ready card sends it back to the build with the conflict
instruction. Pushes to any other branch are ignored (logged, never set aside)."""

from __future__ import annotations

import json
import logging
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
from tests.service.test_mcp import SHA, Rig, started
from tests.service.test_pilot_677 import eventually
from tests.service.test_rework_feedback import to_ready
from tests.service.test_webhook_mapping import IDENTITY, _common

OWNER = 114979
MAIN = "9" * 40


def push_payload(repo_node_id: str, ref: str, *, after: str = MAIN) -> dict[str, Any]:
    common = _common(OWNER)
    common["sender"] = {"id": OWNER, "login": "someone", "type": "User"}
    common["repository"] = {
        "id": IDENTITY.repository_id,
        "node_id": repo_node_id,
        "full_name": IDENTITY.repository_full_name,
        "default_branch": "main",
    }
    return {**common, "ref": ref, "before": "0" * 40, "after": after, "deleted": False}


def repo_of(rig: Rig) -> str:
    return rig.service.config.repo_id


class _GitHub:
    repository_node_id = "R_kgDOTC12Fg"

    async def issue_snapshot(self, ref: Any) -> Any:
        return snapshot(read_at_us=1)


async def deliver_push(rig: Rig, payload: dict[str, Any], guid: str) -> DeliveryRecord:
    processor = GitHubDeliveryProcessor(
        rig.service,
        DeliveryNormalizer(replace(IDENTITY, repository_node_id=rig.service.config.repo_id)),
        _GitHub(),  # type: ignore[arg-type]
        rig.service.clock,
    )
    record = DeliveryRecord(guid, "push", json.dumps(payload).encode(), {})
    await rig.service.db.call(lambda store: store.append_delivery(record))
    await processor.process(record)
    return record


async def status(rig: Rig, guid: str) -> str | None:
    rows = await rig.service.db.call(
        lambda store: store.query("SELECT status FROM deliveries WHERE delivery_guid = ?", (guid,))
    )
    return str(rows[0][0]) if rows else None


async def pushes_applied(rig: Rig) -> int:
    rows = await rig.service.db.call(
        lambda store: store.query(
            "SELECT COUNT(*) FROM events WHERE kind = ?", (ev.EventKind.BASE_PUSHED.value,)
        )
    )
    return int(rows[0][0])


def normalize(payload: dict[str, Any]) -> tuple[ev.EventBody, ...]:
    out = DeliveryNormalizer(IDENTITY).normalize(
        raw_body=json.dumps(payload).encode(),
        event_name="push",
        delivery_guid="g-push",
        delivery_time_us=1,
    )
    return tuple(e.body for e in out.events)


def test_push_maps_to_base_pushed_only_for_the_default_branch():
    repo = IDENTITY.repository_node_id
    assert normalize(push_payload(repo, "refs/heads/main")) == (
        ev.BasePushed(ref="refs/heads/main", head_sha=MAIN),
    )
    assert normalize(push_payload(repo, "refs/heads/factory/issue-42")) == ()
    assert normalize(push_payload(repo, "refs/tags/v1")) == ()
    assert normalize({**push_payload(repo, "refs/heads/main"), "deleted": True}) == ()


def config_for(service_config: ServiceConfig) -> ServiceConfig:
    return service_config.model_copy(update={"review_bot_grace_minutes": 0, "max_open_bot_prs": 1})


@pytest.mark.asyncio
async def test_push_to_another_branch_is_ignored_with_a_log_line(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    async with started(config_for(service_config)) as rig:
        await to_ready(rig)
        reads = len(rig.executed(EffectKind.FETCH_PR_EVIDENCE))
        with caplog.at_level(logging.DEBUG):
            await deliver_push(
                rig, push_payload(repo_of(rig), "refs/heads/factory/issue-42"), "d-feat"
            )
        assert await status(rig, "d-feat") == "processed"  # retired, never set aside
        # Routine: logged at DEBUG only.
        ignored = [r for r in caplog.records if "push to a branch other than" in r.getMessage()]
        assert [r.levelno for r in ignored] == [logging.DEBUG]
        assert await pushes_applied(rig) == 0
        assert len(rig.executed(EffectKind.FETCH_PR_EVIDENCE)) == reads
        assert (await rig.parcel()).stage == Stage.READY


@pytest.mark.asyncio
async def test_push_to_main_rereads_and_a_conflict_goes_back_to_the_build(
    service_config: ServiceConfig,
):
    async with started(config_for(service_config)) as rig:
        root, _ = await to_ready(rig)
        old = (await rig.parcel()).current_session
        reads = len(rig.executed(EffectKind.FETCH_PR_EVIDENCE))
        rig.github.script(
            EffectKind.FETCH_PR_EVIDENCE,
            Ack(
                "685",
                {
                    "verified": False,
                    "head_sha": SHA,
                    "checks": "green",
                    "open": True,
                    "closes_issue": True,
                    "review_accepted": True,
                    "findings_dispositioned": True,
                    "review_bot_pending_since_us": 0,
                    "mergeable": "conflict",
                    "base_head": MAIN,
                    "base_ref": "main",
                },
            ),
        )
        await deliver_push(rig, push_payload(repo_of(rig), "refs/heads/main"), "d-main")
        assert await status(rig, "d-main") == "processed"
        assert await pushes_applied(rig) == 1

        async def read_again() -> bool:
            return len(rig.executed(EffectKind.FETCH_PR_EVIDENCE)) > reads

        await eventually(read_again)

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
        assert parcel.stage == Stage.BUILDING
        assert parcel.conflict_wake == f"{SHA}:{MAIN}"
        run = parcel.current_session
        assert run.root_id == root  # the same issue session

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
        assert text is not None
        assert "merge conflict with main" in text and "merge origin/main" in text
        assert "fresh cross-vendor review" in text and "PR\n#685" in text


@pytest.mark.asyncio
async def test_redelivered_push_applies_each_parcel_event_once(service_config: ServiceConfig):
    async with started(config_for(service_config)) as rig:
        await to_ready(rig)
        payload = push_payload(repo_of(rig), "refs/heads/main")
        processor = GitHubDeliveryProcessor(
            rig.service,
            DeliveryNormalizer(replace(IDENTITY, repository_node_id=rig.service.config.repo_id)),
            _GitHub(),  # type: ignore[arg-type]
            rig.service.clock,
        )
        record = DeliveryRecord("d-twice", "push", json.dumps(payload).encode(), {})
        await rig.service.db.call(lambda store: store.append_delivery(record))
        await processor.process(record)
        await processor.process(record)  # re-processed before it was retired
        assert await pushes_applied(rig) == 1


@pytest.mark.parametrize(
    "command",
    [
        "git fetch origin main",
        "git merge origin/main",
        "git add -A && git commit --no-edit",
        "git push origin HEAD:factory/issue-42",
    ],
)
def test_build_stage_policy_already_allows_resolving_a_conflict(command: str) -> None:
    """The conflict instruction needs no wider rule: build stages may merge the default
    branch into the issue branch and push it (only the default branch is refused)."""
    from omnigent.policies.builtins.cel import cel_policy

    from omnigent_factory.omnigent import policies as pol

    build = cel_policy(**pol.factory_cel_policy("main").factory_params)
    verdict = build(
        {"type": "tool_call", "data": {"name": "Bash", "arguments": {"command": command}}}
    )
    assert verdict is None or verdict.get("result") == "ALLOW", command
    denied = build(
        {
            "type": "tool_call",
            "data": {"name": "Bash", "arguments": {"command": "git push origin main"}},
        }
    )
    assert denied is not None and denied["result"] == "DENY"
