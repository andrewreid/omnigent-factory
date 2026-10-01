"""A review accepted for an earlier head carries to a head that only syncs the base
branch (owner "Update branch"); any other new commit needs fresh review evidence."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from omnigent_factory.core.effects import Ack, EffectKind

from .test_adapter import BOT_ID, CTX, adapter, effect
from .test_readiness_682 import HEAD, server

OWNER = 114979
OTHER = 4242  # some teammate whose commits landed on main
REVIEWED = "2041e6e2" + "0" * 32
MAIN = "9" * 40
FEATURE = "f" * 40
MID = "d" * 40
BASE1 = "b1" + "0" * 38


def commit(sha: str, author: int | None, *parents: str) -> dict[str, Any]:
    return {
        "sha": sha,
        "author": None if author is None else {"id": author},
        "parents": [{"sha": p} for p in parents],
    }


#: What GitHub's compare returns for a real "Update branch": the base commits the merge
#: introduces (non-owner, single-parent) as well as the merge commit itself.
BASE_COMMITS = [commit(BASE1, OTHER, "a" * 40), commit(MAIN, OTHER, BASE1)]


def with_compare(commits: list[dict[str, Any]], status: str = "ahead"):
    base = server()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(f"/compare/{REVIEWED}...{HEAD}"):
            return httpx.Response(
                200, json={"status": status, "total_commits": len(commits), "commits": commits}
            )
        if "/compare/main..." in path:
            on_main = path.endswith((f"...{MAIN}", f"...{BASE1}"))
            return httpx.Response(200, json={"status": "behind" if on_main else "ahead"})
        return base(request)

    return handler


async def review_accepted(handler: Any) -> bool:
    async def cross_vendor(_: Any) -> bool:
        return True  # Molly's clean review of REVIEWED

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh = adapter(http, independent_reviewer_ids=frozenset(), cross_vendor_review=cross_vendor)
        outcome = await gh.execute(
            effect(
                EffectKind.FETCH_PR_EVIDENCE,
                pr_number=682,
                issue_number=677,
                head_sha=HEAD,
                reviewed_head=REVIEWED,
            ),
            CTX,
        )
    assert isinstance(outcome, Ack), outcome
    assert outcome.detail["verified"] == outcome.detail["review_accepted"]
    # The carried-forward review is reported as a base sync (the reducer keeps a Ready
    # card in Ready when only a required check is red on such a head, #651).
    assert outcome.detail["base_sync"] is outcome.detail["review_accepted"]
    return bool(outcome.detail["review_accepted"])


@pytest.mark.asyncio
async def test_update_branch_with_introduced_base_commits_carries_the_review_forward():
    merge = commit(HEAD, OWNER, REVIEWED, MAIN)
    assert await review_accepted(with_compare([*BASE_COMMITS, merge]))


@pytest.mark.asyncio
async def test_bot_merge_of_main_is_also_a_base_sync():
    assert await review_accepted(
        with_compare([*BASE_COMMITS, commit(HEAD, BOT_ID, REVIEWED, MAIN)])
    )


@pytest.mark.asyncio
async def test_owner_authored_commit_counts_as_sync():
    assert await review_accepted(with_compare([commit(HEAD, OWNER, REVIEWED)]))


@pytest.mark.parametrize(
    ("commits", "status"),
    [
        ([commit(HEAD, BOT_ID, REVIEWED)], "ahead"),  # new agent change: needs fresh review
        (  # base sync, then a new agent commit on top
            [*BASE_COMMITS, commit(MID, OWNER, REVIEWED, MAIN), commit(HEAD, BOT_ID, MID)],
            "ahead",
        ),
        ([commit(HEAD, None, REVIEWED, FEATURE)], "ahead"),  # merge of a non-base branch
        ([*BASE_COMMITS, commit(HEAD, OWNER, REVIEWED, MAIN)], "diverged"),  # rewritten
    ],
)
@pytest.mark.asyncio
async def test_any_other_new_commit_needs_fresh_review(commits, status):
    assert not await review_accepted(with_compare(commits, status))
