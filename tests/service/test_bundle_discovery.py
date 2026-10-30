"""Deterministic, paginated bundle discovery and proposal limits."""

import json
from dataclasses import replace
from functools import partial

import pytest

from omnigent_factory.core.types import IssueLinks, LinkedIssue, Stage
from omnigent_factory.service.bundles import DISCOVERY_PAGE_SIZE, parse_proposals
from omnigent_factory.store import bundles as bs
from tests.service.test_bundles import on, proposal, rig, triage
from tests.service.test_related_issues import board_issue


def entries(page):
    return json.loads(page["index"].splitlines()[1])


def cards():
    return [board_issue(n, f"I_m{n}", Stage.TRIAGED, labels=("area:api",)) for n in (1, 2, 3)]


def test_ten_proposals_accepted_eleven_rejected():
    raw = [proposal(2 * i + 1, 2 * i + 2) for i in range(11)]
    parsed, errors = parse_proposals(raw[:10])
    assert len(parsed) == 10 and errors == []
    assert parse_proposals(raw) == ([], ["bundles: a list of at most 10"])


@pytest.mark.asyncio
async def test_hints_include_related_parent_and_area_with_suggestion_verdicts(service_config):
    async with rig(on(service_config)) as r:
        r.directory.triage[1] = triage(related=[{"issue": 2, "relation": "depends_on"}])
        r.directory.triage[3] = triage("L")
        board = cards()
        board[0] = replace(board[0], links=IssueLinks(parent=LinkedIssue(40, True)))
        board[1] = replace(
            board[1], stage=Stage.SCOPED, links=IssueLinks(parent=LinkedIssue(40, True))
        )
        found = entries(await r.bundles.discovery(board))
        assert found[0] == {
            "kind": "candidate",
            "members": [1, 2],
            "signals": ["triage: #1 depends_on #2", "parent: #40", "label: area:api"],
            "eligible": True,
            "problems": [],
        }
        invalid = next(item for item in found if item["members"] == [1, 3])
        assert invalid["eligible"] is False
        assert "#1 depends on open #2" in invalid["problems"]
        assert any("#3 is size L" in p for p in invalid["problems"])
        # Only Triage/Planning cards supply candidates, never Building or Inbox.
        board[2] = replace(board[2], stage=Stage.BUILDING)
        assert len(entries(await r.bundles.discovery(board))) == 1
        assert r.repo.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state", ["pending", "posting", "posted", "confirmed", "created", "stale", "refused"]
)
async def test_existing_sets_excluded_in_every_state(service_config, state):
    async with rig(on(service_config)) as r:
        parsed, _ = parse_proposals([proposal(2, 1)])
        await r.bundles.record_proposals("rk_old", parsed)
        await r.service.db.call(
            partial(
                bs.set_proposal,
                repo_id=r.service.config.repo_id,
                proposal_key=bs.member_key((1, 2)),
                state=state,
            )
        )
        found = entries(await r.bundles.discovery(cards()))
        assert found[0] == {
            "kind": "existing",
            "members": [1, 2],
            "state": state,
            "open": state in bs.OPEN_PROPOSAL_STATES,
        }
        assert all(item["members"] != [1, 2] for item in found[1:])
        assert len(found) == 3


@pytest.mark.asyncio
async def test_discovery_pages_all_existing_proposals_and_hints(service_config):
    async with rig(on(service_config)) as r:
        await r.bundles.record_proposals(
            "rk_old",
            [
                bs.NewProposal((100 + 2 * i, 101 + 2 * i), "theme", "shared implementation")
                for i in range(DISCOVERY_PAGE_SIZE + 1)
            ],
        )
        first = await r.bundles.discovery(cards())
        assert first["count"] == DISCOVERY_PAGE_SIZE
        assert first["total"] == DISCOVERY_PAGE_SIZE + 4
        second = await r.bundles.discovery(cards(), offset=first["next_offset"])
        assert second["next_offset"] is None
        all_entries = entries(first) + entries(second)
        assert len({tuple(e["members"]) for e in all_entries}) == first["total"]
        assert sum(e["kind"] == "candidate" for e in all_entries) == 3
        assert (await r.bundles.discovery(cards(), offset=999))["count"] == 0


@pytest.mark.asyncio
async def test_hints_use_assignment_labels_missing_triage_and_points(service_config):
    async with rig(on(service_config, bundle_max_points=4)) as r:
        r.directory.triage.update({1: triage("M"), 2: triage("M"), 3: None})
        board = cards()
        board[1] = replace(board[1], assigned=True, labels=("area:api", "needs-review"))
        found = entries(await r.bundles.discovery(board))
        pair = next(e for e in found if e["members"] == [1, 2])
        assert not pair["eligible"]
        assert any("6 size points" in p for p in pair["problems"])
        assert any("assigned" in p for p in pair["problems"])
        assert any("needs-review" in p for p in pair["problems"])
        missing = next(e for e in found if e["members"] == [1, 3])
        assert not missing["eligible"]
        assert any("triage result" in p for p in missing["problems"])


@pytest.mark.asyncio
async def test_generic_labels_do_not_make_hints_and_disabled_returns_none(service_config):
    async with rig(on(service_config), parent=None) as r:
        board = [replace(c, labels=("bug", "follow-up")) for c in cards()]
        assert entries(await r.bundles.discovery(board)) == []
        await r.bundles.command({"action": "off"})
        assert (await r.bundles.discovery(cards()))["count"] == 0


@pytest.mark.asyncio
async def test_candidate_pages_are_bounded_and_complete(service_config):
    from omnigent_factory.testing.harness import Harness

    h = Harness()
    for n in range(1, 13):
        h.triage(f"I_m{n}")
    async with rig(on(service_config), h) as r:
        board = [
            board_issue(n, f"I_m{n}", Stage.TRIAGED, labels=("area:api",)) for n in range(1, 13)
        ]
        first = await r.bundles.discovery(board)
        second = await r.bundles.discovery(board, offset=first["next_offset"])
        assert first["count"] == 50 and second["count"] == 16
        assert second["next_offset"] is None
        pairs = [tuple(e["members"]) for e in entries(first) + entries(second)]
        assert len(set(pairs)) == 66
        # Input order does not change page membership or ordering.
        assert entries(await r.bundles.discovery(list(reversed(board)))) == entries(first)


@pytest.mark.asyncio
async def test_bundle_discovery_is_available_through_ranking_tool(service_config, monkeypatch):
    from types import SimpleNamespace

    from omnigent_factory.service.board_index import BoardIndex
    from omnigent_factory.service.mcp import FactoryTools, build_mcp_server

    async with rig(on(service_config)) as r:

        async def read_board():
            return cards()

        async def ranking_caller(_session_id):
            return SimpleNamespace(run_id="rk_hints")

        tools = FactoryTools(
            r.service, r.directory, r.service.config, BoardIndex(read_board, r.clock)
        )
        tools.bundles = r.bundles
        monkeypatch.setattr(tools, "_ranking_caller", ranking_caller)
        server = build_mcp_server(tools, r.service.config)
        metadata = {tool.name: tool for tool in await server.list_tools()}
        listing = metadata["factory_list_issues"]
        assert listing.inputSchema["properties"]["offset"]["default"] == 0
        assert listing.inputSchema["properties"]["view"]["enum"] == ["issues", "bundles"]
        assert "until next_offset is null" in listing.description
        assert "at most 10" in metadata["factory_submit_ranking"].description
        _, page = await server.call_tool(
            "factory_list_issues", {"session_id": "conv_ranking", "view": "bundles"}
        )
        assert page["run_id"] == "rk_hints"
        assert page["count"] == 3
        assert all(e["kind"] == "candidate" for e in entries(page))
        _, page = await server.call_tool(
            "factory_list_issues", {"session_id": "conv_ranking", "view": "bundles", "offset": 2}
        )
        assert page["count"] == 1 and page["next_offset"] is None
        text, submit_field = await r.bundles.ranking_text()
        assert "view=bundles" in text and "until next_offset is null" in text
        assert "up to 10 non-overlapping" in text and "at most 10" in submit_field
        assert "Do not re-propose existing member sets" in text
