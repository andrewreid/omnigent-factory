"""Review rounds: the owner's "Review rounds" field (their own board edits only; an epic's
value applies to sub-issues without their own), one round per reviewed head across runs
(an owner comment or rework never resets it), findings on heads beyond the budget not
waited for, and the rounds in the run's status."""

from __future__ import annotations

import json
from functools import partial
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import Stage
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.ports.github import IssueRef, PullRequestEvidence
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.review_rounds import ReviewRounds, RoundsView, distinct_rounds, view
from omnigent_factory.store import discipline as ds
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID
from tests.github.test_adapter import adapter
from tests.github.test_review_bot_pending import (
    CODEX,
    HEAD,
    OLD,
    OPENED_US,
    PR,
    S,
    comment,
    review,
    server,
)
from tests.service.test_deferred import rig as deferred_rig
from tests.service.test_epic_autopilot import EPIC, S1, S2, history, rig

pytestmark = pytest.mark.asyncio

FIELD = "PVTF_review_rounds"


def rounds_config(config: ServiceConfig) -> ServiceConfig:
    return config.model_copy(update={"review_rounds_field_node_id": FIELD})


def field_edit(node: str, to: Any, *, sender: int = OWNER_ID, field: str = FIELD) -> bytes:
    """The owner's ``projects_v2_item`` edit of a NUMBER field (GitHub's payload shape)."""
    change: dict[str, Any] = {"field_node_id": field, "field_type": "number"}
    if to is not ...:
        change["to"] = to
    return json.dumps(
        {
            "action": "edited",
            "projects_v2_item": {
                "node_id": f"PVTI_{node}",
                "project_node_id": "PVT_kwDOEanNes4BkJhb",
                "content_node_id": node,
                "updated_at": "2027-01-15T08:20:00Z",
            },
            "changes": {"field_value": change},
            "sender": {"id": sender},
        }
    ).encode()


async def budget(rounds: ReviewRounds, r: Any, pid: str) -> int:
    return await rounds.budget(await r.parcel(pid))


# ------------------------------------------------------------------ budget


async def test_empty_field_takes_the_default_and_only_owner_edits_count(
    service_config: ServiceConfig,
):
    config = rounds_config(service_config)
    async with rig(config, history()) as r:
        rounds = ReviewRounds(r.service)
        assert await budget(rounds, r, S1) == 2  # review_rounds_default
        await rounds.note_owner_field(field_edit(S1, 4, sender=OTHER_USER_ID))
        await rounds.note_owner_field(field_edit(S1, 4, field="PVTF_other"))
        assert await budget(rounds, r, S1) == 2  # someone else's value is not trusted
        await rounds.note_owner_field(field_edit(S1, 4))
        assert await budget(rounds, r, S1) == 4
        await rounds.note_owner_field(field_edit(S1, None))  # cleared: the default again
        assert await budget(rounds, r, S1) == 2
        # A reload changes the default.
        r.service.config = r.service.config.model_copy(update={"review_rounds_default": 3})
        assert await budget(rounds, r, S1) == 3


async def test_an_epics_value_applies_to_sub_issues_without_their_own(
    service_config: ServiceConfig,
):
    config = rounds_config(service_config)
    async with rig(config, history()) as r:
        rounds = ReviewRounds(r.service)
        await rounds.note_owner_field(field_edit(EPIC, 5))
        assert await budget(rounds, r, S1) == 5 and await budget(rounds, r, S2) == 5
        await rounds.note_owner_field(field_edit(S2, 1))
        assert await budget(rounds, r, S2) == 1  # its own value wins
        await rounds.note_owner_field(field_edit(S2, 0))
        assert await budget(rounds, r, S2) == 5  # below 1 is ignored


async def test_an_edit_without_its_value_is_read_from_the_board(service_config: ServiceConfig):
    config = rounds_config(service_config)
    async with rig(config, history()) as r:
        reads: list[tuple[str, str]] = []

        async def read(item: str, field: str) -> float | None:
            reads.append((item, field))
            return 6.0

        rounds = ReviewRounds(r.service, read)
        await rounds.note_owner_field(field_edit(S1, ...))
        assert reads == [(f"PVTI_{S1}", FIELD)] and await budget(rounds, r, S1) == 6


# ------------------------------------------------------------------ rounds


async def test_one_round_per_reviewed_commit_in_review_order():
    full = "a" * 40
    rounds = [(full, 3), ("aaaaaaa", 5), ("b" * 40, 1), ("c" * 40, 9)]
    assert distinct_rounds(rounds) == [("b" * 40, 1), (full, 3), ("c" * 40, 9)]
    v = view(rounds, 2)
    assert (v.used, v.budget, v.exhausted, v.over_budget) == (3, 2, True, {"c" * 40})
    assert view(rounds, 4).over_budget == frozenset() and not view(rounds, 4).exhausted
    assert RoundsView(1, 2).strict_from == 2 and RoundsView(1, 5).strict_from == 5


async def test_an_owner_comment_and_rework_do_not_reset_the_count(service_config: ServiceConfig):
    async with deferred_rig(service_config) as r:
        rounds = r.rounds
        first = await rounds.observe("I_parcel_1", 7, [("a" * 40, 10), ("b" * 40, 20)])
        assert first is not None and (first.used, first.budget) == (2, 2) and first.exhausted
        # The owner comments on the Ready card: a rework under the same approval.
        result = await r.service.apply_event(
            Event(
                event_id="comment-1",
                repo_id=r.service.config.repo_id,
                parcel_id="I_parcel_1",
                source_time_us=r.clock.now_utc_us(),
                provenance=Provenance.WEBHOOK,
                body=ev.PlanFeedback(text_digest="please also do X"),
                actor_id=OWNER_ID,
                issue_number=1,
            )
        )
        assert result.accepted and (await r.parcel()).stage == Stage.BUILDING
        # The rework's PR read sees the same reviews and one more: still counted from 2.
        again = await rounds.observe(
            "I_parcel_1", 7, [("a" * 40, 10), ("b" * 40, 20), ("c" * 40, 30)]
        )
        assert again is not None and again.used == 3 and again.over_budget == {"c" * 40}
        stored = await r.service.db.call(
            partial(ds.review_rounds, repo_id=r.service.config.repo_id, parcel_id="I_parcel_1")
        )
        assert len(stored) == 3


async def test_the_run_sees_its_rounds_in_factory_status(service_config: ServiceConfig):
    from omnigent_factory.service.mcp import FactoryTools

    async with deferred_rig(service_config) as r:
        await r.rounds.observe("I_parcel_1", 7, [("a" * 40, 10)])
        tools = FactoryTools(r.service, None, r.service.config)  # type: ignore[arg-type]
        tools.review_rounds = r.rounds
        status = await tools._review_rounds(await r.parcel())
        assert status == {"used": 1, "budget": 2, "exhausted": False, "strict_from_round": 2}
        depth = await tools._follow_up_depth(await r.parcel())
        assert depth == {
            "depth": 0,
            "source_issue": None,
            "max_depth": 1,
            "may_defer_follow_ups": True,
        }


# ------------------------------------------------------------------ the PR read


async def test_the_pr_read_collects_one_round_per_reviewed_head():
    handler = server(
        reviews=[review(OLD, OPENED_US - 600 * S), review(HEAD, OPENED_US + 149 * S)],
        comments=[
            comment(
                f"Didn't find any major issues. Reviewed commit: `{OLD[:7]}`", OPENED_US, CODEX
            ),
            comment("@codex review", OPENED_US + 60 * S),
        ],
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh: GitHubAPIAdapter = adapter(http, review_bot_login=CODEX, review_bot_mention="@codex")
        await gh._review_bot_pending_since(PR, 686, HEAD)
    assert sorted(gh._last_bot_rounds) == sorted(
        [(OLD, OPENED_US - 600 * S), (HEAD, OPENED_US + 149 * S), (OLD[:7], OPENED_US)]
    )
    assert len(distinct_rounds(gh._last_bot_rounds)) == 2


async def test_findings_on_a_head_beyond_the_budget_neither_wake_nor_hold_ready(
    monkeypatch: pytest.MonkeyPatch,
):
    within, beyond = "a" * 40, "c" * 40
    seen: list[Any] = []

    async def observe(parcel_id: str, pr: int, rounds: Any) -> RoundsView:
        seen.append((parcel_id, pr, list(rounds)))
        return view([(within, 1), ("b" * 40, 2), (beyond, 3)], 2)

    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: httpx.Response(500)))
    gh: GitHubAPIAdapter = adapter(
        http,
        review_bot_login=CODEX,
        review_bot_mention="@codex",
        independent_reviewer_ids=frozenset(),
    )
    gh.review_rounds = observe
    ref_a = ev.FindingRef(path="a.ts", severity="P2", title="old round")
    ref_c = ev.FindingRef(path="c.ts", severity="P2", title="beyond the budget")

    async def pull(_n: int) -> dict[str, Any]:
        return {
            "state": "open",
            "mergeable": True,
            "head": {"sha": beyond, "ref": "factory/issue-12"},
            "base": {"ref": "main", "sha": "0" * 40},
            "user": {"id": gh.bot_user_id},
        }

    async def checks(*_a: Any) -> Any:
        from omnigent_factory.core.events import ChecksState

        return ChecksState.GREEN

    async def threads(open_findings: list[tuple[Any, str]]) -> Any:
        async def settled(_n: int) -> bool:
            gh._last_open_commits = tuple(open_findings)
            gh._last_open_findings = tuple(f for f, _ in open_findings)
            return not open_findings

        return settled

    async def closes(*_a: Any) -> bool:
        return True

    async def since(*_a: Any) -> int:
        gh._last_bot_rounds = ((beyond, 3),)
        return 0

    monkeypatch.setattr(gh, "_pull", pull)
    monkeypatch.setattr(gh, "_checks_state", checks)
    monkeypatch.setattr(gh, "_closes_issue", closes)
    monkeypatch.setattr(gh, "_review_bot_pending_since", since)
    ref = IssueRef(gh.repository_node_id, 12, "I_1")

    monkeypatch.setattr(gh, "_bot_threads_settled", await threads([(ref_c, beyond)]))
    evidence = await gh.pull_request(ref, 7, cross_vendor_review=True)
    assert isinstance(evidence, PullRequestEvidence)
    assert evidence.findings_dispositioned and evidence.open_findings == ()
    assert (evidence.review_rounds_used, evidence.review_rounds_budget) == (3, 2)
    assert evidence.review_rounds_note == "Codex round 3/2"
    assert seen == [("I_1", 7, [(beyond, 3)])]

    # A finding of a round within the budget still holds Ready (and gets its wake).
    monkeypatch.setattr(
        gh, "_bot_threads_settled", await threads([(ref_a, within), (ref_c, beyond)])
    )
    evidence = await gh.pull_request(ref, 7, cross_vendor_review=True)
    assert isinstance(evidence, PullRequestEvidence)
    assert not evidence.findings_dispositioned and evidence.open_findings == (ref_a,)
    await http.aclose()
