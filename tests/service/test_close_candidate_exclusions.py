"""A close candidate (``factory:close-candidate``) is never proposed for a bundle (an
owner's ``/bundle`` may take it) and epic autopilot leaves it alone like
``factory:skip``."""

from __future__ import annotations

import json

import pytest

from omnigent_factory.core.events import EventKind
from omnigent_factory.core.protocol import CLOSE_CANDIDATE_LABEL
from omnigent_factory.core.types import Stage
from omnigent_factory.service.bundles import validate_bundle
from omnigent_factory.service.config import ServiceConfig
from tests.service import test_bundles as tb
from tests.service import test_epic_autopilot as te
from tests.service.test_related_issues import board_issue


def test_bundle_proposals_refuse_a_close_candidate_an_owner_bundle_may_take_it():
    h = tb.history()
    labelled = frozenset({CLOSE_CANDIDATE_LABEL, "area:api"})
    found = {
        1: tb.facts(h, tb.P1, labels=labelled),
        2: tb.facts(h, tb.P2),
    }
    suggested = validate_bundle((1, 2), found, max_members=3, max_points=4)
    assert suggested == [f"#1 is labelled {CLOSE_CANDIDATE_LABEL}"]
    assert validate_bundle((1, 2), found, max_members=3, max_points=4, explicit=True) == []


@pytest.mark.asyncio
async def test_bundle_ranking_text_names_close_candidates(service_config: ServiceConfig):
    async with tb.rig(tb.on(service_config), tb.history()) as r:
        text, _field = await r.bundles.ranking_text()
    assert "close-candidate" in text


@pytest.mark.asyncio
async def test_bundle_discovery_marks_close_candidate_hints_ineligible(
    service_config: ServiceConfig,
):
    async with tb.rig(tb.on(service_config)) as r:
        page = await r.bundles.discovery(
            [
                board_issue(
                    1,
                    tb.P1,
                    Stage.TRIAGED,
                    labels=("area:api", CLOSE_CANDIDATE_LABEL),
                ),
                board_issue(2, tb.P2, Stage.TRIAGED, labels=("area:api",)),
            ]
        )
    [hint] = json.loads(page["index"].splitlines()[1])
    assert hint["eligible"] is False
    assert hint["problems"] == [f"#1 is labelled {CLOSE_CANDIDATE_LABEL}"]


@pytest.mark.asyncio
async def test_autopilot_leaves_a_close_candidate_sub_issue_alone(service_config: ServiceConfig):
    h = te.history(subs=(te.S1, te.S2))
    async with te.rig(te.on(service_config), h) as r:
        r.labels[te.S1] = [CLOSE_CANDIDATE_LABEL]
        r.labels[te.S2] = [CLOSE_CANDIDATE_LABEL]
        assert await r.autopilot.run_once() == "#1: nothing to start"
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 0
