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
