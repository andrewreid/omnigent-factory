"""Epic autopilot through the factory tools: the epic planning pass (its first message,
the required plan, the hash-headed comment) and the autopilot sub-issue plan run (its
first message and the required epic_fit). Rosie's tools are unchanged."""

from __future__ import annotations

from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.core.types import IssueLinks, LinkedIssue, SessionKind, Via
from omnigent_factory.service.directory import _public_result
from omnigent_factory.service.mcp import TOOL_NAMES, FactoryToolError
from omnigent_factory.testing.builders import EventFactory, contract, snapshot
from tests.service.test_epic_triage_mcp import first_message
from tests.service.test_mcp import Rig, started
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio

EPIC = IssueLinks(
    sub_issues=(LinkedIssue(43, True, "API"), LinkedIssue(44, True, "Web")),
    sub_total=2,
)
SUB = IssueLinks(parent=LinkedIssue(41, True, "Epic"))
EPIC_PLAN: dict[str, Any] = {
    "summary": "API first, then the web page.",
    "coverage": {"gaps": [], "overlaps": []},
    "build_order": [
        {"issue": 43, "reason": "the web page calls it"},
        {"issue": 44, "reason": "after the API"},
    ],
    "plan": {
        "parts": [
            {"issue": 43, "scope": "The export endpoint only."},
            {"issue": 44, "scope": "The button, calling the endpoint."},
        ],
        "coordination": ["43 fixes the response shape before 44 starts."],
        "human_gates": [
            {
                "key": "storage-account",
                "title": "Create the export storage account",
                "steps": "Create it in the tenant and add its connection string as a secret.",
                "blocks": [43],
            }
        ],
        "external_blockers": [{"issue": 12, "note": "the auth upgrade lands first"}],
    },
}
PLAN = {"approach": "Add the endpoint.", "risks": [], "contract": contract("S", "Export API")}


async def on_autopilot(rig: Rig) -> str:
    rig.github.snapshots["I_mcp_parcel"] = snapshot(links=EPIC)
    await rig.send(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=rig.factory.now, links=EPIC))
    r = await rig.send(
        ev.AutopilotMarked(option="Full"),
        evidence=snapshot(read_at_us=rig.factory.now, links=EPIC),
    )
    assert r.accepted, r.reason
    return await rig.active(SessionKind.TRIAGE)


async def test_an_autopilot_epic_gets_the_epic_planning_pass(service_config):
    async with started(service_config) as rig:
        root = await on_autopilot(rig)
        text = await first_message(rig)
        assert "epic on autopilot" in text and "plan.human_gates" in text
        without = {k: v for k, v in EPIC_PLAN.items() if k != "plan"}
        with pytest.raises(FactoryToolError, match="this epic is on autopilot: include plan"):
            await rig.submit(root, "epic_triage", without)
        outside = {
            **EPIC_PLAN,
            "plan": {**EPIC_PLAN["plan"], "parts": [{"issue": 9, "scope": "x"}]},
        }
        with pytest.raises(FactoryToolError, match="only this epic's sub-issues"):
            await rig.submit(root, "epic_triage", outside)
        receipt = await rig.submit(root, "epic_triage", EPIC_PLAN)
        assert receipt["accepted"]

        async def published() -> Any:
            return rig.github.executed(EffectKind.PUBLISH_TRIAGE)

        await eventually(published)
        parcel = await rig.parcel()
        assert parcel.autopilot is not None and parcel.autopilot.plan is not None
        stored = rig.tools.directory.latest_result(parcel.autopilot.plan.session_id)
        assert stored is not None
        body = stored["factory_result"]["result"]
        comment = _public_result(body)
        lines = comment.splitlines()
        # Headed by the hash /approve binds, like a sub-issue plan.
        assert lines[0] == f"### Epic plan · hash `{parcel.autopilot.plan.prefix}`"
        assert "**Steps for you** (each becomes a sub-issue assigned to you):" in lines
        assert any("Create the export storage account" in line and "#43" in line for line in lines)
        assert "**Waiting on outside this epic:**" in lines
        assert "- #12: the auth upgrade lands first" in lines
        assert "**Coordination:**" in lines
        assert "ef_" not in comment and "omnigent" not in comment.lower()

        # Once approved, a sub-issue autopilot drives reads its part of the plan.
        async def posted() -> bool:
            ap = (await rig.parcel()).autopilot
            return ap is not None and ap.plan is not None and ap.plan.posted_at_us is not None

        await eventually(posted)
        r = await rig.send(
            ev.ApprovePlan(via=Via.COMMAND),
            evidence=snapshot(read_at_us=rig.factory.now, links=EPIC),
        )
        assert r.accepted, r.reason
        ap = (await rig.parcel()).autopilot
        assert ap is not None and ap.approved
        epoch = ap.source_event_id
        sub = EventFactory("I_sub_43", issue_number=43)
        links = IssueLinks(parent=LinkedIssue(42, True, "Epic"))
        await rig.service.apply_event(
            sub.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=sub.now, links=links))
        )
        r = await rig.service.apply_event(
            sub.make(
                ev.AutopilotPlan(epic=42, epoch=epoch),
                provenance=Provenance.SCHEDULER,
                evidence=snapshot(read_at_us=sub.now, links=links),
            )
        )
        assert r.accepted, r.reason
        child = await rig.service.db.call(lambda store: store.load_parcel("I_sub_43"))
        assert child is not None
        part = await rig.tools._epic_part(child)
        assert part is not None and part["epic"] == 42
        assert part["this_issue_part"] == "The export endpoint only."
        assert part["build_order"] == [43, 44]


async def test_a_sub_issue_autopilot_drives_must_say_whether_its_plan_fits(service_config):
    async with started(service_config) as rig:
        rig.factory.issue_number = 43
        rig.github.snapshots["I_mcp_parcel"] = snapshot(links=SUB)
        await rig.send(
            ev.GitHubSnapshot(), evidence=snapshot(read_at_us=rig.factory.now, links=SUB)
        )
        r = await rig.send(
            ev.AutopilotPlan(epic=41, epoch="ep-1"),
            provenance=Provenance.SCHEDULER,
            evidence=snapshot(read_at_us=rig.factory.now, links=SUB),
        )
        assert r.accepted, r.reason
        root = await rig.active(SessionKind.PLAN)
        text = await first_message(rig)
        assert "sub-issue of an epic on autopilot" in text and "epic_fit" in text
        with pytest.raises(FactoryToolError, match="include epic_fit"):
            await rig.submit(root, "plan", PLAN)
        fit = {"within": False, "note": "it also needs the web change"}
        receipt = await rig.submit(root, "plan", {**PLAN, "epic_fit": fit})
        assert receipt["accepted"]
        claim = (await rig.parcel()).autopilot_claim
        assert claim is not None and claim.drift and claim.drift_hash == receipt["plan_hash"]
    # Reused tools only (submit with epic_fit, get_issue with epic_plan): Rosie's tool
    # allowlist needs no change.
    assert TOOL_NAMES == (
        "factory_get_issue",
        "factory_get_plan",
        "factory_get_feedback",
        "factory_get_status",
        "factory_list_issues",
        "factory_ask_owner",
        "factory_submit_result",
        "factory_submit_ranking",
    )


async def test_epic_fit_is_optional_outside_autopilot_and_required_inside():
    corr = Correlation("I_p", "ss_1", "n", 1, "plan", issue_number=5)
    plan = {"kind": "plan", "publication_kind": "contract", "open_decision_ids": [], **PLAN}
    validate_result(plan, corr)
    with pytest.raises(ResultError, match="include epic_fit"):
        validate_result(plan, Correlation("I_p", "ss_1", "n", 1, "plan", epic_part=True))
    fit = {"within": True, "note": "only the endpoint"}
    validate_result(
        {**plan, "epic_fit": fit}, Correlation("I_p", "ss_1", "n", 1, "plan", epic_part=True)
    )
