"""factory_submit_result enforces the follow-up depth limit from the factory's own lineage
records, and hands accepted results to the deferred-findings recorder."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.mcp import FactoryToolError
from tests.service.test_mcp import build_ready, building, started

pytestmark = pytest.mark.asyncio

FOLLOW_UP = {
    "severity": "P2",
    "path": "api/src/export.ts",
    "title": "Retry leaks the delivery row",
    "reason": "outside the plan",
}


async def test_a_follow_up_issue_may_not_defer_follow_ups(service_config: ServiceConfig):
    async with started(service_config) as rig:
        # Issue #42 is the consolidated follow-up issue of #41.
        await rig.service.db.call(
            lambda store: store.record_follow_up(
                service_config.repo_id, 42, source_issue=41, origin="reported"
            )
        )
        seen: list[tuple[int | None, str, dict[str, Any]]] = []
        rig.tools.result_observer = lambda parcel, sid, result: seen.append(
            (parcel.issue_number, sid, result)
        )
        root, plan_hash = await building(rig)
        await rig.tools.get_plan(root)
        with pytest.raises(FactoryToolError) as error:
            await rig.submit(
                root, "build_ready", {**build_ready(), "deferred": [FOLLOW_UP]}, plan_hash=plan_hash
            )
        assert "itself a follow-up (a follow-up of #41)" in str(error.value)
        assert "fix each of these findings in this PR" in str(error.value)
        assert seen == []
        advisory = {**FOLLOW_UP, "outcome": "advisory", "reason": "style only"}
        receipt = await rig.submit(
            root, "build_ready", {**build_ready(), "deferred": [advisory]}, plan_hash=plan_hash
        )
        assert receipt["accepted"] is True
        [(issue, sid, result)] = seen
        assert issue == 42 and sid == (await rig.run()).session_id
        assert result["deferred"][0]["outcome"] == "advisory"
        status = await rig.tools.get_status(root)
        assert status["follow_up_depth"] == {
            "depth": 1,
            "source_issue": 41,
            "max_depth": 1,
            "may_defer_follow_ups": False,
        }


async def test_an_ordinary_issue_defers_follow_ups_and_a_raised_limit_allows_one_more(
    service_config: ServiceConfig,
):
    async with started(service_config.model_copy(update={"follow_up_max_depth": 2})) as rig:
        await rig.service.db.call(
            lambda store: store.record_follow_up(
                service_config.repo_id, 42, source_issue=41, origin="reported"
            )
        )
        root, plan_hash = await building(rig)
        await rig.tools.get_plan(root)
        receipt = await rig.submit(
            root, "build_ready", {**build_ready(), "deferred": [FOLLOW_UP]}, plan_hash=plan_hash
        )
        assert receipt["accepted"] is True


async def test_a_split_out_issue_is_not_a_follow_up_and_may_defer(service_config: ServiceConfig):
    from functools import partial

    from omnigent_factory.store import discipline as ds

    async with started(service_config) as rig:
        # Issue #42 was split out of #41 at the owner's direction (filed by the factory).
        await rig.service.db.call(
            partial(
                ds.record_splits,
                repo_id=service_config.repo_id,
                source_issue=41,
                session_id="s",
                items=[ds.NewSplit("k", "t", "b", "r")],
            )
        )
        await rig.service.db.call(
            partial(
                ds.set_split_state,
                repo_id=service_config.repo_id,
                source_issue=41,
                item_key="k",
                state="created",
                issue_number=42,
            )
        )
        root, plan_hash = await building(rig)
        await rig.tools.get_plan(root)
        receipt = await rig.submit(
            root, "build_ready", {**build_ready(), "deferred": [FOLLOW_UP]}, plan_hash=plan_hash
        )
        assert receipt["accepted"] is True


async def test_every_accepted_stage_result_reaches_the_split_filer(service_config: ServiceConfig):
    from tests.service.test_mcp import TRIAGE, triaged

    split = {"title": "Admin export", "body": "File separately.", "reason": "owner said so"}
    async with started(service_config) as rig:
        seen: list[tuple[str, list[Any]]] = []
        rig.tools.split_observer = lambda _p, _sid, result: seen.append(
            (str(result["kind"]), list(result.get("split_out") or []))
        )
        root = await triaged(rig)
        await rig.submit(root, "triage", {**TRIAGE, "split_out": [split]})
        assert seen == [("triage", [{**split, "relation": "none", "blocks": [], "member": None}])]


async def test_stage_instructions_route_owner_directed_splits_to_split_out():
    from omnigent_factory.service import directory as d

    root = Path(d.__file__).parent / "templates"
    current = {
        *d._FIRST_TEMPLATES.values(),
        d._RETRIAGE_TEMPLATE,
        d._EPIC_TRIAGE_TEMPLATE,
        d._EPIC_PLAN_TEMPLATE,
        d._AUTOPILOT_PLAN_TEMPLATE,
        d._REWORK_TEMPLATE,
        d._ITEMS_REWORK_TEMPLATE,
    }
    for name in current:
        text = " ".join((root / name).read_text("utf-8").split())
        assert "split_out" in text and "the factory files it" in text, name
        assert "never open issues" in text.lower(), name
