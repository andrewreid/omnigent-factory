"""Only current applicable check attempts decide readiness (design-r2 §E).

Live: five CANCELLED runs (superseded by later attempts) defeated readiness although
every required check passed. Also: check webhooks with no PR number are still hints for
the parcel branch.
"""

from __future__ import annotations

import json

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.events import ChecksState
from omnigent_factory.github.webhook import DeliveryNormalizer
from tests.service.test_webhook_mapping import IDENTITY, _common

from .test_adapter import adapter

HEAD = "beae9656" + "0" * 32
APP = 15368
PASSING = [(10, "api / Lint / Typecheck / Test", "success"), (11, "api / DB-tier Test", "success")]
#: Earlier attempts of the same checks, cancelled by a later run (e.g. a concurrency group).
SUPERSEDED = [
    (1, "api / Lint / Typecheck / Test", "cancelled"),
    (2, "api / DB-tier Test", "cancelled"),
    (3, "api / Lint / Typecheck / Test", "cancelled"),
    (4, "web / Build", "cancelled"),
    (5, "changes", "cancelled"),
]
LATER = [(12, "web / Build", "skipped"), (13, "changes", "success")]


def server(
    runs: list[tuple[int, str, str]],
    protection: int | list[str | tuple[str, int | None]],
    statuses: list[tuple[int, str, str]] = (),  # type: ignore[assignment]
):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/check-runs"):
            return httpx.Response(
                200,
                json={
                    "check_runs": [
                        {
                            "id": run_id,
                            "name": name,
                            "app": {"id": APP},
                            "status": "completed",
                            "conclusion": conclusion,
                        }
                        for run_id, name, conclusion in runs
                    ]
                },
            )
        if path.endswith("/status"):
            return httpx.Response(
                200,
                json={
                    "state": "pending",
                    "statuses": [{"id": i, "context": c, "state": st} for i, c, st in statuses],
                },
            )
        if path.endswith("/protection/required_status_checks"):
            if isinstance(protection, int):
                return httpx.Response(protection, json={"message": "Not Found"})
            pinned = [c if isinstance(c, tuple) else (c, APP) for c in protection]
            return httpx.Response(
                200, json={"checks": [{"context": c, "app_id": a} for c, a in pinned]}
            )
        if path.endswith("/rules/branches/main"):
            return httpx.Response(200, json=[])
        raise AssertionError(str(request.url))

    return handler


async def checks(
    runs: list[tuple[int, str, str]],
    protection: int | list[str | tuple[str, int | None]] = 404,
    statuses: list[tuple[int, str, str]] = (),  # type: ignore[assignment]
):
    handler = server(runs, protection, statuses)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        return await adapter(http)._checks_state(HEAD, "main")


REQUIRED = ["api / Lint / Typecheck / Test", "api / DB-tier Test"]


@pytest.mark.asyncio
async def test_required_checks_pass_despite_five_superseded_cancelled_runs():
    assert await checks(SUPERSEDED + PASSING + LATER, REQUIRED) == ChecksState.GREEN


@pytest.mark.asyncio
async def test_latest_required_attempt_cancelled_still_blocks():
    runs = [*PASSING, (20, "api / DB-tier Test", "cancelled")]
    assert await checks(runs, REQUIRED) == ChecksState.FAILED


@pytest.mark.asyncio
async def test_without_required_checks_superseded_cancellations_are_history():
    assert await checks(SUPERSEDED + PASSING + LATER) == ChecksState.GREEN
    # A cancellation nothing superseded is still a failure; never a blanket ignore.
    assert await checks([*PASSING, (30, "e2e", "cancelled")]) == ChecksState.FAILED
    # A failed earlier attempt is not hidden either (only cancellations are superseded).
    assert await checks([(1, "e2e", "failure"), (2, "e2e", "success")]) == ChecksState.FAILED


def test_check_suite_without_pr_numbers_is_still_a_hint_for_the_parcel_branch():
    payload = {
        **_common(),
        "action": "completed",
        "check_suite": {
            "head_branch": "factory/issue-462",
            "head_sha": HEAD,
            "status": "completed",
            "conclusion": "success",
            "pull_requests": [],
        },
    }
    normalized = DeliveryNormalizer(IDENTITY).normalize(
        raw_body=json.dumps(payload).encode(),
        event_name="check_suite",
        delivery_guid="suite-empty",
        delivery_time_us=1,
    )
    [event] = normalized.events
    assert isinstance(event.body, ev.ChecksChanged)
    assert event.body.pr_number == 0 and event.body.head_sha == HEAD


@pytest.mark.asyncio
async def test_legacy_status_latest_per_context_decides():
    """A newer success of a legacy status context supersedes its older failure."""
    history = [(1, "ci/legacy", "failure"), (2, "ci/legacy", "success")]
    assert await checks(PASSING, [*REQUIRED, ("ci/legacy", None)], history) == ChecksState.GREEN
    assert await checks(PASSING, statuses=history) == ChecksState.GREEN
    newer_failure = [(1, "ci/legacy", "success"), (2, "ci/legacy", "failure")]
    assert await checks(PASSING, statuses=newer_failure) == ChecksState.FAILED


@pytest.mark.asyncio
async def test_app_pinned_check_is_not_satisfied_by_an_unpinned_legacy_status():
    # Required "ci" from the pinned app; only a commit status (no app) named "ci" exists.
    result = await checks(PASSING, [*REQUIRED, "ci"], [(1, "ci", "success")])
    assert result != ChecksState.GREEN


async def failing_names(
    runs: list[tuple[int, str, str]],
    protection: int | list[str | tuple[str, int | None]] = 404,
) -> tuple[ChecksState, str]:
    handler = server(runs, protection)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh = adapter(http)
        state = await gh._checks_state(HEAD, "main")
        return state, gh._last_failing_checks


@pytest.mark.asyncio
async def test_failing_required_checks_are_named_for_the_board_note():
    """#651: the note names the red required check ("api / Dependency audit")."""
    audit = "api / Dependency audit"
    runs = [*PASSING, (20, audit, "failure"), (21, "optional lint", "failure")]
    assert await failing_names(runs, [*REQUIRED, audit]) == (ChecksState.FAILED, audit)
    assert await failing_names(PASSING, REQUIRED) == (ChecksState.GREEN, "")
    # Without a required set every failing check counts.
    state, names = await failing_names([*PASSING, (20, audit, "failure")])
    assert state == ChecksState.FAILED and names == audit


@pytest.mark.asyncio
async def test_failing_check_names_are_semicolon_separated_since_names_contain_commas():
    """#461: "web / Typecheck, test, lint" must stay one name in the note."""
    audit, web = "api / Dependency audit", "web / Typecheck, test, lint"
    runs = [*PASSING, (20, web, "failure"), (21, audit, "failure")]
    state, names = await failing_names(runs, [*REQUIRED, audit, web])
    assert state == ChecksState.FAILED
    assert names == f"{audit}; {web}"  # required names in sorted order
    assert names.split("; ") == [audit, web]
