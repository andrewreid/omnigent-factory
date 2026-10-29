"""#1309 / #1514: cached clean after main moves must not stop conflict catch-up."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import (
    event_from_json,
    event_to_json,
    parcel_from_json,
    parcel_to_json,
)
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.reducer import awaiting_evidence
from omnigent_factory.core.types import Stage
from omnigent_factory.service.executor import EffectExecutor, ParcelSerializers
from omnigent_factory.testing.fakes import FakeClock
from tests.github.test_adapter import CTX, adapter, closing_refs
from tests.github.test_readiness_682 import HEAD as FIXTURE_HEAD
from tests.github.test_readiness_682 import server
from tests.test_merge_conflict import (
    HEAD,
    MAIN1,
    MAIN2,
    MERGED,
    MIN,
    PR,
    P,
    fetches,
    harness,
    push,
    read,
    ready,
    submit,
)

TEST_MERGE = "a" * 40


def github_answer(*, clean=True, parents=(MAIN1, HEAD), base=MAIN1, commit_status=200):
    """A cached PR response and the immutable test merge it names."""
    fixture = server()
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append(path)
        if path.endswith(f"/git/commits/{TEST_MERGE}"):
            return httpx.Response(
                commit_status,
                json={"sha": TEST_MERGE, "parents": [{"sha": p} for p in parents]},
            )
        if path.endswith(f"/pulls/{PR}"):
            response = fixture(
                httpx.Request("GET", request.url.copy_with(path=path.replace(str(PR), "682")))
            )
            pr = response.json()
            pr["head"]["sha"] = HEAD
            return httpx.Response(
                200,
                json={
                    **pr,
                    "base": {"ref": "main", "sha": base},
                    "mergeable": clean,
                    "mergeable_state": "clean" if clean else "dirty",
                    "merge_commit_sha": TEST_MERGE,
                },
            )
        if path == "/graphql" and "reviewThreads" not in json.loads(request.content)["query"]:
            return closing_refs(P, number=PR)
        if path.endswith(f"/commits/{HEAD}/status"):
            request = httpx.Request(
                "GET", request.url.copy_with(path=path.replace(HEAD, FIXTURE_HEAD))
            )
        return fixture(request)

    return handler, calls


async def execute_read(h, fetch, handler):
    async def reviewed(_: Any) -> bool:
        return True

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh = adapter(http, independent_reviewer_ids=frozenset(), cross_vendor_review=reviewed)
        ack = await gh.execute(fetch, CTX)
    assert isinstance(ack, Ack), ack
    executor = EffectExecutor(None, h.cfg, FakeClock(), (), ParcelSerializers(), poll_seconds=1)
    event = executor._ack_event(fetch, ack)
    assert event is not None and isinstance(event.body, ev.ReadinessEvidence)
    return event.body


def assert_quiet(h, before, result):
    p = h.p()
    assert (p.stage, p.bot, p.note, p.board_note) == (
        before.stage,
        before.bot,
        before.note,
        before.board_note,
    )
    assert p.readiness.ready and p.readiness.verified
    assert not {
        EffectKind.MOVE_CARD,
        EffectKind.SET_BOT,
        EffectKind.SET_NOTE,
        EffectKind.SEND_MESSAGE,
        EffectKind.POST_COMMENT,
    }.intersection(e.kind for e in result.effects)


@pytest.mark.asyncio
async def test_1514_stale_clean_after_push_is_polled_until_dirty_starts_conflict_rework():
    h = harness()
    ready(h)
    before = h.p()
    pushed = push(h)
    immediate = next(e for e in fetches(pushed) if "not_before_us" not in e.args)
    handler, _ = github_answer()  # 4-6 seconds after the merge: still clean on MAIN1
    body = await execute_read(h, immediate, handler)
    assert body.mergeable == ev.MERGE_UNKNOWN  # fails on the original adapter
    result = h.send(P, body)
    assert_quiet(h, before, result)
    assert h.p().readiness.merge_unknown and awaiting_evidence(h.p(), h.f().now)
    [catch_up] = fetches(h.send(P, ev.ReconcileDue()))
    assert catch_up.args["expected_base_head"] == MAIN2
    handler, calls = github_answer(clean=False, base=MAIN2)
    body = await execute_read(h, catch_up, handler)
    assert body.mergeable == ev.MERGE_CONFLICT
    assert not any("/git/commits/" in path for path in calls)
    h.send(P, body)
    p = h.p()
    assert p.stage == Stage.BUILDING and p.note == "Merge conflict with main"
    assert p.conflict_wake == f"{HEAD}:{MAIN2}"
    assert len([a for a in p.authorizations if a.rework and a.conflict]) == 1


@pytest.mark.asyncio
async def test_stale_then_fresh_clean_resolves_without_status_or_note_churn():
    h = harness()
    ready(h)
    before = h.p()
    pushed = push(h)
    assert_quiet(h, before, pushed)
    for parents, expected in [((MAIN1, HEAD), ev.MERGE_UNKNOWN), ((MAIN2, HEAD), ev.MERGE_CLEAN)]:
        [fetch] = fetches(h.send(P, ev.ReconcileDue()))
        # Even when PR base.sha is stale, the test merge proves the actual base.
        handler, _ = github_answer(parents=parents)
        body = await execute_read(h, fetch, handler)
        assert body.mergeable == expected
        if expected == ev.MERGE_CLEAN:
            assert body.base_head == MAIN2
        assert_quiet(h, before, h.send(P, body))
    assert not h.p().expected_base_head and not h.p().readiness.merge_unknown
    assert not fetches(h.send(P, ev.ReconcileDue()))


@pytest.mark.parametrize(
    "parents,commit_status",
    [
        ((MAIN1, HEAD), 200),  # a current base.sha does not prove current mergeability
        ((MAIN2, MERGED), 200),  # wrong PR head
        ((MAIN2,), 200),
        ((MAIN2, HEAD), 404),  # test merge temporarily unavailable
    ],
)
@pytest.mark.asyncio
async def test_clean_requires_test_merge_proof_even_with_current_base_sha(parents, commit_status):
    h = harness()
    ready(h)
    fetch = fetches(push(h))[0]
    handler, _ = github_answer(parents=parents, base=MAIN2, commit_status=commit_status)
    body = await execute_read(h, fetch, handler)
    assert body.mergeable == ev.MERGE_UNKNOWN and body.verified


@pytest.mark.asyncio
async def test_unconstrained_read_keeps_existing_behavior_without_commit_get():
    h = harness()
    ready(h)
    fetch = fetches(h.send(P, ev.ChecksChanged(state=ev.ChecksState.PENDING)))[0]
    handler, calls = github_answer()
    body = await execute_read(h, fetch, handler)
    assert body.mergeable == ev.MERGE_CLEAN
    assert not any("/git/commits/" in path for path in calls)


@pytest.mark.parametrize("old_expected", ["", MAIN1])
def test_older_clean_read_cannot_clear_a_newer_push_expectation(old_expected):
    h = harness()
    ready(h)
    push(h, MAIN1)
    push(h, MAIN2)
    before = h.p()
    result = h.send(P, read(h, expected_base_head=old_expected))
    assert_quiet(h, before, result)
    assert h.p().expected_base_head == MAIN2 and h.p().readiness.merge_unknown
    [fetch] = fetches(h.send(P, ev.ReconcileDue()))
    assert fetch.args["expected_base_head"] == MAIN2


def test_delayed_read_is_coalesced_across_push_burst_and_restart():
    h = harness()
    ready(h)
    first = push(h, MAIN1)
    [immediate, delayed] = fetches(first)
    assert "not_before_us" not in immediate.args
    push_event = next(e for e, _ in reversed(h.log) if isinstance(e.body, ev.BasePushed))
    repeated = h.apply(push_event)
    assert repeated.duplicate and not fetches(repeated)
    due = delayed.args["not_before_us"]
    assert due == h.f().now + MIN
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    for tip in [MAIN1, MAIN2, MERGED]:
        [immediate] = fetches(push(h, tip))
        assert immediate.args["expected_base_head"] == tip
        assert "not_before_us" not in immediate.args
        assert h.p().base_read_due_us == due
    h.f().tick(MIN)
    assert len(fetches(push(h))) == 2  # a later burst gets its own delayed read


def test_head_change_and_resubmission_keep_the_outstanding_expectation():
    h = harness()
    h.to_building()
    submit(h)
    push(h)
    result = h.send(P, read(h, observed_head_sha=MERGED))
    [fetch] = fetches(result)
    assert fetch.args["head_sha"] == MERGED and fetch.args["expected_base_head"] == MAIN2
    submit(h, MERGED)
    reads = [e for _, r in h.log for e in fetches(r) if e.args["head_sha"] == MERGED]
    assert all(e.args["expected_base_head"] == MAIN2 for e in reads)
    assert awaiting_evidence(h.p(), h.f().now)


def test_old_events_and_aggregate_without_new_fields_replay():
    h = harness()
    ready(h)
    data = json.loads(parcel_to_json(h.p()))
    del data["parcel"]["expected_base_head"]
    del data["parcel"]["base_read_due_us"]
    h.parcels[P] = parcel_from_json(json.dumps(data))
    for body in [ev.BasePushed(), read(h)]:
        provenance = (
            ev.Provenance.WEBHOOK if isinstance(body, ev.BasePushed) else ev.Provenance.ADAPTER
        )
        data = json.loads(event_to_json(h.f().make(body, provenance=provenance)))
        data["body"].pop("expected_base_head", None)
        if isinstance(body, ev.BasePushed):
            data["body"].pop("head_sha", None)
        assert h.apply(event_from_json(json.dumps(data))).audit.accepted
    assert h.p().stage == Stage.READY and not h.p().expected_base_head
    assert not h.p().readiness.merge_unknown
