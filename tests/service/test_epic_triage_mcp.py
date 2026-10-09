"""Rosie sees native links (factory_get_issue, factory_list_issues), and an epic gets the
epic triage pass: its own first message, sub-issue view, result kind and comment."""

from __future__ import annotations

from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.types import IssueLinks, LinkedIssue, SessionKind, Stage, Via
from omnigent_factory.service.directory import _public_result
from omnigent_factory.service.mcp import FactoryToolError, compact_links
from omnigent_factory.testing.builders import snapshot
from tests.service.test_mcp import TRIAGE, P, Rig, started
from tests.service.test_pilot_677 import eventually
from tests.service.test_related_issues import board_issue, with_board

pytestmark = pytest.mark.asyncio

EPIC = IssueLinks(
    sub_issues=(LinkedIssue(822, False, "ADR"), LinkedIssue(823, True, "Framework")),
    sub_total=2,
    sub_completed=1,
)
BLOCKED = IssueLinks(
    parent=LinkedIssue(821, True, "Epic"),
    blocked_by=(LinkedIssue(823, True, "Framework"), LinkedIssue(822, False, "ADR")),
    blocking=(LinkedIssue(825, True, "Bicep"),),
)
EPIC_RESULT = {
    "summary": "Two sub-issues; one done.",
    "coverage": {"gaps": ["no alerting"], "overlaps": []},
    "build_order": [{"issue": 823, "reason": "the framework comes first"}],
    "links": [],
}


async def triage_with(rig: Rig, links: IssueLinks) -> str:
    rig.github.snapshots[P] = snapshot(links=links)
    await rig.send(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=rig.factory.now, links=links))
    await rig.send(
        ev.RequestTriage(via=Via.DRAG),
        evidence=snapshot(read_at_us=rig.factory.now, links=links),
    )
    return await rig.active(SessionKind.TRIAGE)


async def first_message(rig: Rig) -> str:
    run = await rig.run()
    text = await rig.tools.directory.message_text(
        EffectIntent(
            effect_id="ef_first",
            kind=EffectKind.SEND_MESSAGE,
            parcel_id=P,
            target=run.session_id,
            preconditions=Preconditions(1, 0, session_id=run.session_id),
            args={"purpose": "first"},
        )
    )
    assert text is not None
    return text


async def test_get_issue_shows_the_native_links(service_config):
    async with started(service_config) as rig:
        root = await triage_with(rig, BLOCKED)
        issue = await rig.tools.get_issue(root)
        links = issue["links"]
        assert links["parent"] == {"issue": "#821", "state": "open", "title": "Epic"}
        assert [b["issue"] for b in links["blocked_by"]] == ["#823", "#822"]
        assert links["blocked_by"][1]["state"] == "closed"
        assert links["blocking"] == [{"issue": "#825", "state": "open", "title": "Bicep"}]
        assert "epic" not in issue
        # A plain issue keeps the plain triage pass.
        assert 'kind "triage"' in await first_message(rig)
        assert "GitHub blocked-by link" in await first_message(rig)


async def test_list_issues_carries_compact_links(service_config):
    async with started(service_config) as rig:
        root = await triage_with(rig, IssueLinks())
        with_board(
            rig,
            [
                board_issue(824, "I_824", Stage.TRIAGED, links=BLOCKED),
                board_issue(
                    821, "I_821", Stage.INBOX, links=IssueLinks(sub_total=8, sub_completed=1)
                ),
            ],
        )
        lines = (await rig.tools.list_issues(root))["index"].splitlines()[1:-1]
        assert lines == [
            "#821 [Inbox] Issue 821 | epic 1/8 done",
            "#824 [Triaged] Issue 824 | parent #821; blocked by #823 (open), #822 (closed); "
            "blocks #825",
        ]
    assert compact_links(None) == "" and compact_links(IssueLinks()) == ""


async def test_an_epic_gets_the_epic_triage_pass(service_config):
    async with started(service_config) as rig:
        root = await triage_with(rig, EPIC)
        text = await first_message(rig)
        assert "This issue is an epic" in text and '"epic_triage"' in text
        issue = await rig.tools.get_issue(root)
        epic = issue["epic"]
        assert epic["sub_issues_total"] == 2 and epic["sub_issues_completed"] == 1
        assert [(s["issue"], s["state"]) for s in epic["sub_issues"]] == [
            ("#822", "closed"),
            ("#823", "open"),
        ]
        # The plain triage kind is refused for an epic; the epic kind is accepted.
        with pytest.raises(FactoryToolError, match="submit epic_triage"):
            await rig.submit(root, "triage", TRIAGE)
        receipt = await rig.submit(root, "epic_triage", EPIC_RESULT)
        assert receipt["accepted"]

        async def published() -> Any:
            return rig.github.executed(EffectKind.PUBLISH_TRIAGE)

        await eventually(published)
        # Size is not taken from an epic's triage (it is never built).
        assert (await rig.parcel()).size is None


async def test_epic_triage_kind_is_refused_for_a_plain_issue(service_config):
    async with started(service_config) as rig:
        root = await triage_with(rig, IssueLinks())
        with pytest.raises(FactoryToolError, match="only for an epic"):
            await rig.submit(root, "epic_triage", EPIC_RESULT)


async def test_the_epic_comment_is_the_order_and_the_gaps():
    text = _public_result(
        {
            "kind": "epic_triage",
            **EPIC_RESULT,
            "links": [{"issue": 824, "blocked_by": 823, "reason": "x"}],
        }
    )
    assert text.splitlines() == [
        "### Epic triage",
        "",
        "Two sub-issues; one done.",
        "",
        "**Build order:**",
        "1. #823: the framework comes first",
        "",
        "**Gaps:**",
        "- no alerting",
        "",
        "**Blocked by:**",
        "- #824 after #823",
    ]
    assert "Priority" not in text and "omnigent" not in text.lower()
