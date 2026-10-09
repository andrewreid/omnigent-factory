"""Every stage result may report dependencies in ``related``; the factory itself creates
the native blocked-by links (a plan or blocked run never asks the owner to)."""

from __future__ import annotations

import asyncio
from importlib.resources import files

import pytest

from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.service.links import NativeLinks
from tests.service.test_mcp import PLAN, planning, started, triaged
from tests.service.test_native_links import FakeClient, issue

pytestmark = pytest.mark.asyncio

DEPENDS = [{"issue": 821, "relation": "depends_on", "note": "needs the epic's worker first"}]


def native(rig, client: FakeClient) -> NativeLinks:
    async def read() -> list:
        return []

    links = NativeLinks(rig.service, client, lambda _sid: None, read)  # type: ignore[arg-type]
    rig.tools.link_writer = links.schedule_result
    return links


async def settled(links: NativeLinks) -> None:
    for _ in range(50):
        if not links._tasks:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("link writes did not finish")


async def test_a_plan_result_with_depends_on_creates_the_link(service_config):
    async with started(service_config) as rig:
        root = await planning(rig)
        client = FakeClient({42: issue(42), 821: issue(821)})
        links = native(rig, client)
        receipt = await rig.submit(root, "plan", {**PLAN, "related": DEPENDS})
        assert receipt["accepted"]
        await settled(links)
        assert client.mutations == [("addBlockedBy", {"issue": "I_42", "blocking": "I_821"})]


async def test_a_blocked_result_with_depends_on_creates_the_link_once(service_config):
    async with started(service_config) as rig:
        root = await triaged(rig)
        # #42 is already blocked by #900: that one is not added again.
        client = FakeClient({42: issue(42, blocked_by=(900,)), 821: issue(821), 900: issue(900)})
        links = native(rig, client)
        related = [*DEPENDS, {"issue": 900, "relation": "depends_on", "note": "x"}]
        result = {"reason": "#821 must land first", "done": [], "related": related}
        assert (await rig.submit(root, "blocked", result))["accepted"]
        await settled(links)
        assert client.mutations == [("addBlockedBy", {"issue": "I_42", "blocking": "I_821"})]


async def test_every_result_kind_takes_related_and_refuses_itself():
    corr = Correlation("P", "S", "N", 1, "build", issue_number=42)
    blocked = {"kind": "blocked", "reason": "x", "done": [], "related": DEPENDS}
    assert validate_result(blocked, corr).result.result.related[0].issue == 821  # type: ignore[union-attr]
    with pytest.raises(ResultError, match="cannot relate to itself"):
        validate_result(
            {**blocked, "related": [{"issue": 42, "relation": "blocks", "note": "x"}]}, corr
        )


async def test_stage_instructions_say_the_factory_creates_the_link():
    root = files("omnigent_factory.service") / "templates"
    for name in ("plan-v7.txt", "build-v9.txt", "build-rework-v6.txt", "triage-v7.txt"):
        text = " ".join((root / name).read_text("utf-8").split())
        assert "never ask the owner" in text, name
        assert "factory" in text and ("blocked-by link" in text or "links them" in text), name
        assert "posted for a human: GitHub Markdown, short paragraphs" in text, name
        assert "no internal labels or IDs (de_..., ef_...)" in text, name
        assert "factory_get_feedback" in text and len(text) < 1300, name
    build = " ".join((root / "build-v9.txt").read_text("utf-8").split())
    assert "`Closes #{issue_number}`" in build
    assert "Only resolve threads you have replied to." in build
    assert "never push an empty commit to retrigger CI" in build
    assert "the factory reports" in build
