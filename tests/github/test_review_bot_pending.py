"""When the review bot can still respond to a PR head (#651 / PR #686).

The grace window only applies while the configured bot has not answered the current head,
or was explicitly re-pinged after its last answer; anything unreadable keeps the wait.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from omnigent_factory.github.adapter import GitHubAPIAdapter

from .test_adapter import adapter

HEAD = "fb2159af9a4bdd4c7aaa6f590cf3ac795e8210d6"
OLD = "11d5cfae" + "0" * 32
CODEX = "chatgpt-codex-connector[bot]"
S = 1_000_000  # microseconds per second
COMMITTED = "2026-09-29T06:45:13Z"
COMMITTED_US = 1_790_664_313 * S
OPENED = "2026-09-29T06:47:51Z"
OPENED_US = 1_790_664_471 * S
PR = {"number": 686, "created_at": OPENED}


def at(us: int) -> str:
    return datetime.fromtimestamp(us / S, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def review(commit: str, when: int, login: str = CODEX, body: str = "### 💡 Codex Review"):
    return {"user": {"login": login}, "commit_id": commit, "submitted_at": at(when), "body": body}


def comment(body: str, when: int, login: str = "andrewreid"):
    return {"user": {"login": login}, "created_at": at(when), "body": body}


def server(
    *,
    reviews: list[dict[str, Any]] | None = None,
    comments: list[dict[str, Any]] | None = None,
    reactions: list[dict[str, Any]] | None = None,
    commit_status: int = 200,
):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(f"/commits/{HEAD}"):
            if commit_status != 200:
                return httpx.Response(commit_status, json={"message": "No commit found"})
            return httpx.Response(200, json={"commit": {"committer": {"date": COMMITTED}}})
        if path.endswith("/pulls/686/reviews"):
            return httpx.Response(200, json=reviews or [])
        if path.endswith("/issues/686/comments"):
            return httpx.Response(200, json=comments or [])
        if path.endswith("/issues/686/reactions"):
            return httpx.Response(200, json=reactions or [])
        raise AssertionError(str(request.url))

    return handler


async def pending(handler: Any, login: str = CODEX) -> int | None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh: GitHubAPIAdapter = adapter(http, review_bot_login=login, review_bot_mention="@codex")
        return await gh._review_bot_pending_since(PR, 686, HEAD)


@pytest.mark.asyncio
async def test_pr_686_reviewed_head_and_only_thread_replies_is_not_pending():
    """Codex reviewed fb2159af; the build and the owner only replied (no mention)."""
    handler = server(
        reviews=[
            review(HEAD, OPENED_US + 149 * S),
            review(HEAD, OPENED_US + 500 * S, login="molly-omnigent-factory[bot]"),
        ],
        comments=[comment("This is intended: SWA deployment ...", OPENED_US + 80_000 * S)],
    )
    assert await pending(handler) == 0


@pytest.mark.asyncio
async def test_new_commit_not_yet_reviewed_is_pending_since_the_push():
    handler = server(reviews=[review(OLD, OPENED_US - 600 * S)])
    assert await pending(handler) == OPENED_US  # later of commit date and PR opening


@pytest.mark.asyncio
async def test_re_ping_after_the_last_answer_is_pending_since_the_ping():
    pinged = OPENED_US + 900 * S
    handler = server(
        reviews=[review(HEAD, OPENED_US + 149 * S)],
        comments=[
            comment("@codex review", OPENED_US + 60 * S),  # answered by the review
            comment("@Codex review please", pinged),
        ],
    )
    assert await pending(handler) == pinged


@pytest.mark.asyncio
async def test_the_bots_own_summary_mentioning_itself_is_not_a_re_ping():
    handler = server(
        reviews=[review(HEAD, OPENED_US + 149 * S)],
        comments=[comment("Summary. Comment @codex review", OPENED_US + 600 * S, login=CODEX)],
    )
    assert await pending(handler) == 0


@pytest.mark.asyncio
async def test_clean_verdicts_answer_the_head():
    thumbs = {"user": {"login": CODEX}, "content": "+1", "created_at": at(OPENED_US + 99 * S)}
    assert await pending(server(reactions=[thumbs])) == 0
    verdict = comment(
        "Codex Review: Didn't find any major issues.\n\n**Reviewed commit:** `fb2159af9a`",
        OPENED_US + 99 * S,
        login=CODEX,
    )
    assert await pending(server(comments=[verdict])) == 0


@pytest.mark.asyncio
async def test_verdicts_that_do_not_name_or_postdate_the_head_do_not_answer_it():
    early = {"user": {"login": CODEX}, "content": "+1", "created_at": at(OPENED_US - 5 * S)}
    eyes = {"user": {"login": CODEX}, "content": "eyes", "created_at": at(OPENED_US + 9 * S)}
    other = comment("**Reviewed commit:** `11d5cfae00`", OPENED_US + 9 * S, login=CODEX)
    handler = server(reactions=[early, eyes], comments=[other])
    assert await pending(handler) == OPENED_US


@pytest.mark.asyncio
async def test_unknown_bot_state_is_none():
    assert await pending(server(reviews=[review(HEAD, OPENED_US)]), login="") is None
    assert await pending(server(commit_status=404)) is None


@pytest.mark.asyncio
async def test_unanswered_ping_after_the_push_is_the_trigger():
    pinged = OPENED_US + 300 * S
    handler = server(comments=[comment("@codex review", pinged)])
    assert await pending(handler) == pinged


# ------------------------------------------------------- 👀 while reviewing (#799)


def eyes_server(comment_reactions: Any = None, **kw: Any):
    """``server`` plus the reactions of issue comment 42 (a list, or an HTTP status)."""
    base = server(**kw)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/issues/comments/42/reactions"):
            if isinstance(comment_reactions, int):
                return httpx.Response(comment_reactions, json={"message": "Not Found"})
            return httpx.Response(200, json=comment_reactions or [])
        return base(request)

    return handler


async def eyes_of(handler: Any) -> tuple[int | None, str]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh: GitHubAPIAdapter = adapter(http, review_bot_login=CODEX, review_bot_mention="@codex")
        since = await gh._review_bot_pending_since(PR, 686, HEAD)
        return since, gh._last_bot_eyes


def eyes(when: int, login: str = CODEX) -> dict[str, Any]:
    return {"user": {"login": login}, "content": "eyes", "created_at": at(when)}


@pytest.mark.asyncio
async def test_eyes_on_the_pr_after_the_push_are_seen():
    since, state = await eyes_of(eyes_server(reactions=[eyes(OPENED_US + 20 * S)]))
    assert (since, state) == (OPENED_US, "seen")


@pytest.mark.asyncio
async def test_eyes_before_the_trigger_or_by_someone_else_are_absent():
    old = eyes(OPENED_US - 60 * S)
    person = eyes(OPENED_US + 20 * S, login="andrewreid")
    since, state = await eyes_of(eyes_server(reactions=[old, person]))
    assert (since, state) == (OPENED_US, "absent")


@pytest.mark.asyncio
async def test_eyes_on_the_re_ping_comment_are_seen():
    """#799: Rosie's `@codex review` after the push got Codex's 👀, then nothing for 14
    minutes: it was still reviewing."""
    pinged = OPENED_US + 900 * S
    ping = {**comment("@codex review", pinged, login="molly-omnigent-factory[bot]"), "id": 42}
    handler = eyes_server(
        comment_reactions=[eyes(pinged + 5 * S)],
        reviews=[review(HEAD, OPENED_US + 149 * S)],
        comments=[ping],
        reactions=[eyes(OPENED_US + 10 * S)],  # the PR-open review's 👀: before the ping
    )
    assert await eyes_of(handler) == (pinged, "seen")


@pytest.mark.asyncio
async def test_unreadable_ping_reactions_are_unknown_and_answers_need_no_eyes():
    pinged = OPENED_US + 900 * S
    ping = {**comment("@codex review", pinged), "id": 42}
    handler = eyes_server(comment_reactions=404, comments=[ping])
    assert await eyes_of(handler) == (pinged, "unknown")
    answered = eyes_server(reviews=[review(HEAD, OPENED_US + 149 * S)])
    assert await eyes_of(answered) == (0, "")
    assert await eyes_of(eyes_server(commit_status=404)) == (None, "unknown")


def test_codex_finding_threads_carry_path_severity_title_and_link():
    from omnigent_factory.github.adapter import _finding_ref

    body = (
        "**<sub><sub>![P1 Badge](https://img.shields.io/badge/P1-orange?style=flat)</sub>"
        "</sub>  Keep the delivery row when the retry fails**\n\nThe retry deletes ..."
    )
    thread = {
        "path": "api/src/export.ts",
        "comments": {"nodes": [{"body": body, "url": "https://github.com/o/r/pull/1#r1"}]},
    }
    ref = _finding_ref(thread)
    assert (ref.path, ref.severity, ref.title, ref.url) == (
        "api/src/export.ts",
        "P1",
        "Keep the delivery row when the retry fails",
        "https://github.com/o/r/pull/1#r1",
    )
    plain = _finding_ref({"comments": {"nodes": [{"body": "Consider a guard here", "url": ""}]}})
    assert (plain.path, plain.severity, plain.title, plain.url) == (
        "",
        "",
        "Consider a guard here",
        "",
    )
