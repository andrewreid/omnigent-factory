"""EDIT_REPORT: the Ready report's factory lines are edited in place, never re-posted.

The comment is found by its PUBLISH_REPORT effect marker. A replay (restart, retry) of
the same edit finds the body already current and writes nothing; a missing comment is a
definitive failure, never a second comment; a changed summary part is never rewritten.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from omnigent_factory.core.effects import Ack, DefinitiveFailure, EffectKind
from omnigent_factory.github.adapter import GitHubAPIAdapter, ParcelBinding

from .test_adapter import BOT_ID, CTX, adapter, effect
from .test_review_bot_pending import CODEX, HEAD, OPENED_US, S, at, pending, review, server

REPORT_ID = "ef_report"
MARKER = f"<!-- omnigent-factory effect={REPORT_ID} -->"
HEADING = "### PR #714 is ready for review\n\nAdds the thing."
OLD = f"{HEADING}\n\n**PR:** https://github.com/o/r/pull/714 (head `abc`)\n**CI:** 18 green"
NEW = f"{OLD}\n**Review bot:** Codex: 👍 on `abc1234`"


class Repo:
    def __init__(self, body: str | None) -> None:
        self.body = body
        self.posts = 0
        self.patches = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/issues/12/comments"):
            rows = (
                [] if self.body is None else [{"id": 55, "body": self.body, "user": {"id": BOT_ID}}]
            )
            return httpx.Response(200, json=rows)
        if request.method == "PATCH" and request.url.path.endswith("/issues/comments/55"):
            self.patches += 1
            self.body = json.loads(request.content)["body"]
            return httpx.Response(200, json={"id": 55})
        if request.method == "POST":
            self.posts += 1
        raise AssertionError(f"{request.method} {request.url}")


async def run(repo: Repo, text: str) -> Any:
    async with httpx.AsyncClient(transport=httpx.MockTransport(repo.handler)) as http:
        gh = adapter(
            http,
            parcel_bindings={"I_1": ParcelBinding(12, "PVTI_1")},
            publication_renderer=lambda _: text,
        )
        intent = effect(EffectKind.EDIT_REPORT, effect_id="ef_edit", report_effect_id=REPORT_ID)
        return await gh.execute(intent, CTX)


@pytest.mark.asyncio
async def test_edit_patches_the_marked_report_once_and_a_replay_writes_nothing():
    repo = Repo(f"{OLD}\n\n{MARKER}")
    first = await run(repo, NEW)
    assert isinstance(first, Ack) and first.detail == {"edited": True}
    assert repo.body == f"{NEW}\n\n{MARKER}"  # the original marker: still adoptable
    again = await run(repo, NEW)  # e.g. the effect re-run after a restart
    assert isinstance(again, Ack) and again.detail == {"edited": False}
    assert repo.patches == 1 and repo.posts == 0


@pytest.mark.asyncio
async def test_a_missing_report_is_never_posted_again():
    repo = Repo(None)
    result = await run(repo, NEW)
    assert isinstance(result, DefinitiveFailure)
    assert repo.posts == 0 and repo.patches == 0


@pytest.mark.asyncio
async def test_a_changed_summary_part_is_never_rewritten():
    repo = Repo(f"{OLD}\n\n{MARKER}")
    other = NEW.replace("Adds the thing.", "Adds another thing.")
    result = await run(repo, other)
    assert isinstance(result, Ack) and result.detail["edited"] is False
    assert repo.patches == 0 and repo.body == f"{OLD}\n\n{MARKER}"


# -------------------------------------------------------- the review-bot verdict


async def verdict(handler: Any, settled: bool = True, threads: tuple[str, ...] = ()) -> str:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh: GitHubAPIAdapter = adapter(http, review_bot_login=CODEX, review_bot_mention="@codex")
        since = await gh._review_bot_pending_since(
            {"number": 686, "created_at": at(OPENED_US)}, 686, HEAD
        )
        gh._last_bot_thread_commits = threads
        return gh._review_bot_verdict(HEAD, since, settled)


@pytest.mark.asyncio
async def test_a_plus_one_reads_as_a_thumbs_up_on_the_head():
    thumbs = {"user": {"login": CODEX}, "content": "+1", "created_at": at(OPENED_US + 99 * S)}
    assert await verdict(server(reactions=[thumbs])) == f"👍 on `{HEAD[:7]}`"


@pytest.mark.asyncio
async def test_a_review_counts_its_findings_on_the_head():
    handler = server(reviews=[review(HEAD, OPENED_US + 149 * S)])
    two = (HEAD, HEAD, "0" * 40)  # a thread on an older commit is not counted
    assert await verdict(handler, threads=two) == (
        f"reviewed `{HEAD[:7]}`, 2 findings, all with outcomes"
    )
    assert await verdict(handler) == f"reviewed `{HEAD[:7]}`, no findings"
    assert await verdict(handler, settled=False, threads=two) == (
        f"reviewed `{HEAD[:7]}`, findings without outcomes"
    )


@pytest.mark.asyncio
async def test_no_answer_yet_has_no_verdict():
    assert await verdict(server()) == ""
    assert await pending(server()) != 0


@pytest.mark.asyncio
async def test_thread_read_records_the_review_bots_finding_commits():
    def thread(login: str, oid: str, replied: bool) -> dict[str, Any]:
        first = {"author": {"__typename": "Bot", "login": login}, "originalCommit": {"oid": oid}}
        reply = {"author": {"__typename": "Bot", "databaseId": BOT_ID}}
        nodes = [first, reply] if replied else [first]
        return {"isResolved": True, "resolvedBy": None, "comments": {"nodes": nodes}}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/graphql")
        nodes = [
            thread("chatgpt-codex-connector", HEAD, True),  # GraphQL login: no "[bot]"
            thread("chatgpt-codex-connector", HEAD, True),
            thread("another-bot", HEAD, True),
        ]
        page = {"nodes": nodes, "pageInfo": {"hasNextPage": False, "endCursor": None}}
        return httpx.Response(
            200, json={"data": {"repository": {"pullRequest": {"reviewThreads": page}}}}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh: GitHubAPIAdapter = adapter(http, review_bot_login=CODEX)
        assert await gh._bot_threads_settled(686) is True
        assert gh._last_bot_thread_commits == (HEAD, HEAD)


@pytest.mark.asyncio
async def test_thread_read_lists_every_open_bot_thread_and_earlier_rounds():
    """#799: the owner's Needs you comment names each open thread (the read no longer
    stops at the first one)."""
    old = "11d5cfae" + "0" * 32
    badge = "**<sub><sub>![P{n} Badge](https://img.shields.io/badge/P{n}-red)</sub></sub> {t}**"

    def thread(n: int, title: str, oid: str, replied: bool) -> dict[str, Any]:
        first = {
            "author": {"__typename": "Bot", "login": "chatgpt-codex-connector"},
            "originalCommit": {"oid": oid},
            "body": badge.format(n=n, t=title) + "\n\nWhy ...",
            "url": f"https://github.com/o/r/pull/799#discussion_r{n}",
        }
        reply = {"author": {"__typename": "Bot", "databaseId": BOT_ID}}
        nodes = [first, reply] if replied else [first]
        return {
            "isResolved": False,
            "resolvedBy": None,
            "path": f"api/f{n}.ts",
            "comments": {"nodes": nodes},
        }

    def handler(request: httpx.Request) -> httpx.Response:
        nodes = [
            thread(2, "Earlier, answered", old, True),
            thread(1, "First open", HEAD, False),
            thread(0, "Second open", HEAD, False),
        ]
        page = {"nodes": nodes, "pageInfo": {"hasNextPage": False, "endCursor": None}}
        return httpx.Response(
            200, json={"data": {"repository": {"pullRequest": {"reviewThreads": page}}}}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        gh: GitHubAPIAdapter = adapter(http, review_bot_login=CODEX)
        assert await gh._bot_threads_settled(799) is False
        assert [(f.path, f.severity, f.title) for f in gh._last_open_findings] == [
            ("api/f1.ts", "P1", "First open"),
            ("api/f0.ts", "P0", "Second open"),
        ]
        assert gh._last_bot_thread_commits == (old, HEAD, HEAD)
