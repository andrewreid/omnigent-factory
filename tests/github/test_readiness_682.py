"""Readiness for PR #682 (#677) as agreed with the owner.

Ready = PR open from the parcel branch, CI green, bot review findings each have an
outcome, and Molly's opposite-vendor review clean. The owner's approval is not part of
Ready (REVIEW_REQUIRED is expected; the ruleset enforces it at merge).
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.events import ChecksState
from omnigent_factory.ports.github import IssueRef

from .test_adapter import BOT_ID, CTX, REPO_NODE, adapter, closing_refs, effect

HEAD = "4950075d373b7db6c008a502da568ee73e0fdf03"
REF = IssueRef(REPO_NODE, 677, "I_1")
#: The 17 check runs on PR #682's head (names, all github-actions app 15368).
RUNS_682 = [
    ("web / Build", "skipped"),
    ("web / Generated client freshness", "skipped"),
    ("web / E2E smoke", "skipped"),
    ("web / Typecheck, test, lint", "skipped"),
    ("web / API contract regen-required detector", "success"),
    ("api / Azurite blob tier", "success"),
    ("api / Lint / Typecheck / Test", "success"),
    ("api / Image target manifest", "success"),
    ("api / Requirements DAG", "success"),
    ("api / DB-tier Test", "success"),
    ("Apply area labels", "success"),
    ("changes", "success"),
    ("api / Dependency audit", "success"),
    ("test (ubuntu-latest)", "success"),
    ("changes", "success"),
    ("test (macos-latest)", "success"),
    ("changes", "success"),
]
PROTECTED = [
    "api / Lint / Typecheck / Test",
    "api / Dependency audit",
    "web / Typecheck, test, lint",
    "web / Build",
    "api / Image target manifest",
    "api / DB-tier Test",
]


def _threads(threads: list[dict[str, Any]]) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "data": {
                "repository": {
                    "pullRequest": {
                        "reviewThreads": {
                            "nodes": threads,
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            }
        },
    )


def server(
    *,
    runs: list[tuple[str, str]] = RUNS_682,
    statuses: list[dict[str, str]] | None = None,
    protection: int | list[str] = 403,
    threads: list[dict[str, Any]] | None = None,
):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/pulls/682"):
            return httpx.Response(
                200,
                json={
                    "state": "open",
                    "merged": False,
                    "head": {"sha": HEAD, "ref": "factory/issue-677"},
                    "base": {"ref": "main"},
                    "user": {"id": BOT_ID, "login": "molly-omnigent-factory[bot]"},
                },
            )
        if path.endswith("/check-runs"):
            return httpx.Response(
                200,
                json={
                    "check_runs": [
                        {
                            "name": name,
                            "app": {"id": 15368},
                            "status": "in_progress" if conclusion is None else "completed",
                            "conclusion": conclusion,
                        }
                        for name, conclusion in runs
                    ]
                },
            )
        if path.endswith(f"/commits/{HEAD}/status"):
            # GitHub reports a combined "pending" when there are no statuses at all.
            return httpx.Response(200, json={"state": "pending", "statuses": statuses or []})
        if path.endswith("/branches/main/protection/required_status_checks"):
            if isinstance(protection, int):
                return httpx.Response(protection, json={"message": "Resource not accessible"})
            return httpx.Response(
                200,
                json={
                    "contexts": protection,
                    "checks": [{"context": c, "app_id": 15368} for c in protection],
                },
            )
        if path.endswith("/rules/branches/main"):
            return httpx.Response(
                200,
                json=[
                    {"type": "pull_request", "parameters": {"required_approving_review_count": 1}}
                ],
            )
        if path == "/graphql":
            if "reviewThreads" in json.loads(request.content)["query"]:
                return _threads(threads or [])
            return closing_refs("I_1", number=682)
        raise AssertionError(str(request.url))

    return handler


async def evidence(handler: Any, *, review: bool = True, reviewers: frozenset[int] = frozenset()):
    async def cross_vendor(_: Any) -> bool:
        return review

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh = adapter(http, independent_reviewer_ids=reviewers, cross_vendor_review=cross_vendor)
        return await gh.execute(
            effect(EffectKind.FETCH_PR_EVIDENCE, pr_number=682, issue_number=677, head_sha=HEAD),
            CTX,
        )


@pytest.mark.asyncio
async def test_pr_682_as_live_is_verified_without_owner_approval():
    outcome = await evidence(server())  # protection unreadable → all checks must pass
    assert isinstance(outcome, Ack), outcome
    assert outcome.detail["checks"] == ChecksState.GREEN.value
    assert outcome.detail["findings_dispositioned"] is True
    assert outcome.detail["review_accepted"] is True
    assert outcome.detail["verified"] is True
    assert outcome.detail["checks_summary"] == "17 checks: 13 success, 4 skipped"


@pytest.mark.asyncio
async def test_derived_required_checks_accept_skipped_protected_checks():
    outcome = await evidence(server(protection=PROTECTED))
    assert isinstance(outcome, Ack) and outcome.detail["verified"] is True


@pytest.mark.parametrize(
    ("runs", "state"),
    [
        ([*RUNS_682[:-1], ("changes", "failure")], ChecksState.FAILED),
        ([*RUNS_682[:-1], ("changes", None)], ChecksState.PENDING),
        ([], ChecksState.PENDING),  # nothing reported yet is never green
    ],
)
@pytest.mark.asyncio
async def test_failing_or_pending_ci_is_not_ready(runs, state):
    outcome = await evidence(server(runs=runs))
    assert isinstance(outcome, Ack)
    assert outcome.detail["checks"] == state.value and outcome.detail["verified"] is False


@pytest.mark.asyncio
async def test_failing_commit_status_is_not_ready():
    outcome = await evidence(server(statuses=[{"context": "ext/ci", "state": "failure"}]))
    assert isinstance(outcome, Ack) and outcome.detail["verified"] is False


def _thread(*authors: tuple[str, int | None], resolved: bool = False) -> dict[str, Any]:
    return {
        "isResolved": resolved,
        "comments": {
            "nodes": [
                {"author": {"__typename": kind, **({"databaseId": db} if db else {})}}
                for kind, db in authors
            ]
        },
    }


@pytest.mark.asyncio
async def test_unresolved_bot_review_thread_blocks_until_resolved_or_answered():
    unresolved = [_thread(("Bot", 999))]
    outcome = await evidence(server(threads=unresolved))
    assert isinstance(outcome, Ack) and outcome.detail["findings_dispositioned"] is False
    answered = [_thread(("Bot", 999), ("Bot", BOT_ID))]
    resolved = [_thread(("Bot", 999), resolved=True)]
    human = [_thread(("User", None))]  # the owner's thread is a merge-time concern
    for threads in (answered, resolved, human):
        outcome = await evidence(server(threads=threads))
        assert isinstance(outcome, Ack) and outcome.detail["verified"] is True, threads


@pytest.mark.asyncio
async def test_no_clean_cross_vendor_review_is_not_ready():
    outcome = await evidence(server(), review=False)
    assert isinstance(outcome, Ack)
    assert outcome.detail["review_accepted"] is False and outcome.detail["verified"] is False


@pytest.mark.asyncio
async def test_configured_independent_reviewers_are_an_extra_requirement():
    handler = server()

    def with_reviews(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/reviews"):
            return httpx.Response(200, json=[])
        return handler(request)

    outcome = await evidence(with_reviews, reviewers=frozenset({5}))
    assert isinstance(outcome, Ack) and outcome.detail["verified"] is False


@pytest.mark.asyncio
async def test_head_moved_since_the_report_is_not_verified():
    async def cross_vendor(_: Any) -> bool:
        return True

    async with httpx.AsyncClient(transport=httpx.MockTransport(server())) as http:
        gh = adapter(http, independent_reviewer_ids=frozenset(), cross_vendor_review=cross_vendor)
        outcome = await gh.execute(
            effect(
                EffectKind.FETCH_PR_EVIDENCE, pr_number=682, issue_number=677, head_sha="0" * 40
            ),
            CTX,
        )
    assert isinstance(outcome, Ack) and outcome.detail["verified"] is False
