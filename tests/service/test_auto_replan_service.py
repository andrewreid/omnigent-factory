"""``replan_required`` end to end: the MCP tool, the store, a restart and the plan run's
first message (#1141)."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.types import ReplanStatus, SessionKind, Stage
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.mcp import FactoryToolError
from tests.service.test_mcp import PLAN, building, planning, started
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio

REPLAN = {
    "reason": "The owner's late-finish threshold reverses part of the approved plan.",
    "changes": ["Flag late finishes after 30 minutes, not 15", "Drop the overtime banner"],
}
BLOCKED = {"reason": "The plan has to change first.", "done": [], "replan_required": REPLAN}


async def test_a_build_reporting_replan_required_replans_in_the_same_session(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        build_root, _ = await building(rig)
        build_run = (await rig.run()).session_id
        receipt = await rig.submit(build_root, "blocked", BLOCKED)
        assert receipt["accepted"] and receipt["stage"] == Stage.SCOPED.value
        assert "replans with your changes" in receipt["next"]
        parcel = await rig.parcel()
        assert parcel.replan_request.status == ReplanStatus.STARTED
        assert parcel.replan_request.session_id == build_run
        await rig.quiesce()
        plan_root = await rig.active(SessionKind.PLAN)
        assert plan_root == build_root  # the issue's one session
        parcel = await rig.parcel()
        plan = parcel.current_session
        directory = ServiceDispatchDirectory(rig.service.db, service_config)
        assert await directory.stage_spec(plan.session_id) is not None  # pins the snapshot
        snapshot = directory.dispatch_snapshot(plan.session_id)
        assert snapshot["template"] == "plan-replan-v3.txt"
        assert "Drop the overtime banner" in snapshot["brief"]

        async def first_sent() -> bool:
            return any(
                e.preconditions.session_id == plan.session_id
                and e.args.get("purpose") == MessagePurpose.FIRST.value
                for e in rig.executed(EffectKind.SEND_MESSAGE)
            )

        await eventually(first_sent)
        [first] = [
            e
            for e in rig.executed(EffectKind.SEND_MESSAGE)
            if e.preconditions.session_id == plan.session_id
        ]
        text = await directory.message_text(first)
        assert text is not None
        assert REPLAN["reason"] in text and "- Flag late finishes after 30 minutes" in text
        got = await rig.tools.get_plan(plan_root)
        assert got["approved"] is None  # the approved plan is superseded
        assert got["pending_revision"]["replan_request"] == REPLAN


async def test_replan_required_is_refused_outside_a_build(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        with pytest.raises(FactoryToolError, match="replan_required: only a build run"):
            await rig.submit(root, "blocked", BLOCKED)
        await rig.submit(root, "plan", PLAN)  # the run is unaffected


async def test_a_restart_after_the_result_starts_the_plan_run_exactly_once(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        build_root, _ = await building(rig)
        await rig.submit(build_root, "blocked", BLOCKED)
    async with started(service_config, rig) as again:
        # A retried submit after the restart replays its receipt, it is not a new result.
        replay = await again.submit(build_root, "blocked", BLOCKED, run_id=None)
        assert replay.get("replayed") is True
        await again.quiesce()
        await again.active(SessionKind.PLAN)
        await again.send(ev.ReconcileDue())
        parcel = await again.parcel()
        plans = [a for a in parcel.authorizations if a.kind == SessionKind.PLAN]
        assert len(plans) == 2  # the first plan and the one replan
        runs = [s for s in parcel.sessions if s.kind == SessionKind.PLAN]
        assert len(runs) == 2 and parcel.auto_replans == 1
