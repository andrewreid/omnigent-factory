"""A bundle issue's runs through the factory tools: ``factory_get_issue`` and
``factory_get_plan`` list the members, a plan must name each of them, and an accepted
result's ``ejected`` members reach the bundling service."""

from __future__ import annotations

from typing import Any

import pytest

from omnigent_factory.core.types import Parcel
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.mcp import FactoryToolError
from omnigent_factory.testing.builders import contract
from tests.service.test_mcp import planning, started

pytestmark = pytest.mark.asyncio


class StubBundles:
    """Issue #42 is a bundle of #12 and #13."""

    def __init__(self) -> None:
        self.results: list[dict[str, Any]] = []

    async def members_for(self, parcel: Parcel) -> frozenset[int] | None:
        return frozenset({12, 13}) if parcel.issue_number == 42 else None

    async def members_view(self, parcel: Parcel, boundary: str) -> list[dict[str, Any]] | None:
        if parcel.issue_number != 42:
            return None
        return [
            {"number": n, "title": f"Member {n}", "text": f"#{n} text", "triage": {"size": "S"}}
            for n in (12, 13)
        ]

    def schedule_result(self, parcel: Parcel, session_id: str, result: dict[str, Any]) -> None:
        self.results.append(result)


def bundle_plan(*criteria: str, **over: Any) -> dict[str, Any]:
    c = contract("S", "Ship both")
    c["acceptance_criteria"] = [
        {"id": f"ac{i}", "criterion": text, "verification": "pytest"}
        for i, text in enumerate(criteria, start=1)
    ]
    return {"approach": "One PR for both.", "risks": [], "contract": c, **over}


async def test_bundle_runs_read_their_members(service_config: ServiceConfig):
    async with started(service_config) as rig:
        rig.tools.bundles = StubBundles()
        root = await planning(rig)
        issue = await rig.tools.get_issue(root)
        assert [m["number"] for m in issue["bundle_members"]] == [12, 13]
        assert "Closes #N" in issue["bundle_note"]
        plan = await rig.tools.get_plan(root)
        assert [m["title"] for m in plan["bundle_members"]] == ["Member 12", "Member 13"]


async def test_an_ordinary_issue_has_no_bundle_members(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        assert "bundle_members" not in await rig.tools.get_issue(root)
        rig.tools.bundles = StubBundles()
        rig.tools.bundles.members_for = _none  # type: ignore[method-assign]
        rig.tools.bundles.members_view = _none_view  # type: ignore[method-assign]
        assert "bundle_members" not in await rig.tools.get_plan(root)


async def _none(parcel: Parcel) -> None:
    return None


async def _none_view(parcel: Parcel, boundary: str) -> None:
    return None


async def test_a_bundle_plan_names_each_member_and_may_eject_one(service_config: ServiceConfig):
    async with started(service_config) as rig:
        stub = StubBundles()
        rig.tools.bundles = stub
        root = await planning(rig)
        with pytest.raises(FactoryToolError, match="none name #13"):
            await rig.submit(root, "plan", bundle_plan("#12: the export works"))
        receipt = await rig.submit(
            root,
            "plan",
            bundle_plan(
                "#12: the export works", ejected=[{"issue": 13, "reason": "needs a decision"}]
            ),
        )
        assert receipt["accepted"] is True
        assert stub.results[-1]["ejected"] == [{"issue": 13, "reason": "needs a decision"}]
