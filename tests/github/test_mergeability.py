"""PR reads report whether the PR merges into its base (GitHub computes it lazily), and a
review that predates a merge conflict is never carried to a later head as a base sync."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.events import MERGE_CLEAN, MERGE_CONFLICT, MERGE_UNKNOWN
from omnigent_factory.service.executor import _mergeable_state

from .test_adapter import BOT_ID, CTX, adapter, effect
from .test_base_sync_review import BASE_COMMITS, REVIEWED, commit, with_compare
from .test_readiness_682 import HEAD, server

MAIN = "9" * 40


def mergeability(*reads: tuple[bool | None, str]):
    """The PR read returns each (``mergeable``, ``mergeable_state``) in turn, then the last."""
    base = server()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        response = base(request)
        if not request.url.path.endswith("/pulls/682"):
            return response
        mergeable, state = reads[min(calls["n"], len(reads) - 1)]
        calls["n"] += 1
        pr = response.json()
        pr["base"] = {"ref": "main", "sha": MAIN}
        return httpx.Response(200, json={**pr, "mergeable": mergeable, "mergeable_state": state})

    return handler, calls


async def read(handler: Any, **args: Any) -> tuple[dict[str, Any], list[float]]:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    async def cross_vendor(_: Any) -> bool:
        return True

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh = adapter(
            http,
            independent_reviewer_ids=frozenset(),
            cross_vendor_review=cross_vendor,
            sleep=sleep,
        )
        outcome = await gh.execute(
            effect(
                EffectKind.FETCH_PR_EVIDENCE,
                pr_number=682,
                issue_number=677,
                head_sha=HEAD,
                **args,
            ),
            CTX,
        )
    assert isinstance(outcome, Ack), outcome
    return outcome.detail, slept


@pytest.mark.asyncio
async def test_null_then_false_is_a_conflict_after_a_bounded_reread():
    handler, calls = mergeability((None, "unknown"), (False, "dirty"))
    detail, slept = await read(handler)
    assert calls["n"] == 2 and slept == [1.0]
    assert detail["mergeable"] == MERGE_CONFLICT
    assert detail["base_head"] == MAIN and detail["base_ref"] == "main"
    assert detail["verified"] is False  # green and reviewed, but it cannot merge
    assert _mergeable_state(detail["mergeable"]) == MERGE_CONFLICT


@pytest.mark.asyncio
async def test_still_null_after_the_cap_is_unknown_never_conflict_or_clean():
    handler, calls = mergeability((None, "unknown"))
    detail, slept = await read(handler)
    assert slept == [1.0, 2.0, 4.0] and calls["n"] == 4  # a few seconds, then give up
    assert detail["mergeable"] == MERGE_UNKNOWN
    assert detail["verified"] is True  # readiness is not held on an unknown


@pytest.mark.asyncio
async def test_behind_but_clean_is_clean():
    handler, calls = mergeability((True, "behind"))
    detail, slept = await read(handler)
    assert calls["n"] == 1 and not slept
    assert detail["mergeable"] == MERGE_CLEAN and detail["verified"] is True


@pytest.mark.asyncio
async def test_dirty_state_is_a_conflict():
    handler, _ = mergeability((True, "dirty"))
    detail, _ = await read(handler)
    assert detail["mergeable"] == MERGE_CONFLICT and detail["verified"] is False


@pytest.mark.asyncio
async def test_a_read_without_mergeability_reports_nothing_and_never_waits():
    detail, slept = await read(server())
    assert detail["mergeable"] == "" and not slept


def test_unrecognised_mergeability_is_never_a_conflict():
    assert _mergeable_state("dirty") == "" and _mergeable_state(None) == ""


# ------------------------------------------------------------------ review carry-over


@pytest.mark.parametrize("sync_carry", [True, False])
@pytest.mark.asyncio
async def test_merge_of_main_after_a_conflict_needs_a_fresh_review(sync_carry: bool):
    # The agent's merge of main onto the reviewed head: a clean sync carries the review;
    # the same shape after a conflict was seen (``sync_carry`` false) does not.
    handler = with_compare([*BASE_COMMITS, commit(HEAD, BOT_ID, REVIEWED, MAIN)])
    args: dict[str, Any] = {"reviewed_head": REVIEWED}
    if not sync_carry:
        args["sync_carry"] = False
    detail, _ = await read(handler, **args)
    assert detail["review_accepted"] is sync_carry
    assert detail["base_sync"] is sync_carry
