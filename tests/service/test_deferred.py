"""Deferred findings in the service: one consolidated follow-up issue per PR (idempotent
across restarts and unclear creates), the Ready report's Deferred checkboxes (owner ticks
only, batched into one rework, exactly once, other ticks kept) and lost-webhook recovery
from the comment's edit history (owner edits only)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import MICROS_PER_SECOND, Parcel, Stage
from omnigent_factory.github.client import AmbiguousRequest
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.deferred import Deferrals, issue_marker
from omnigent_factory.service.follow_ups import FOLLOW_UP_LABEL
from omnigent_factory.service.review_rounds import ReviewRounds
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import discipline as ds
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from omnigent_factory.testing.harness import HEAD, Harness
from tests.service.test_epic_autopilot import seed
from tests.service.test_follow_ups import FakeGitHubClient, gh_issue, tracker
from tests.test_rework import ready

pytestmark = pytest.mark.asyncio

P = "I_parcel_1"
BOT = 334_191_208
REPO = "/repos/SA-Ambulance/timesheets"
S = MICROS_PER_SECOND
FIX = "f" * 40

FU_A = {
    "severity": "P2",
    "path": "api/src/export.ts",
    "title": "Retry leaks the delivery row",
    "thread_url": "https://github.com/o/r/pull/7#discussion_r1",
    "reason": "outside the plan",
    "blocks": [5],
}
FU_B = {
    "severity": "P1",
    "path": "api/src/retry.ts",
    "title": "Unbounded retry loop",
    "thread_url": "https://github.com/o/r/pull/7#discussion_r2",
    "reason": "too large for this PR",
}
ADV = {
    "severity": "P3",
    "title": "Clock going backwards",
    "reason": "no realistic trigger here",
    "outcome": "advisory",
}


@dataclass
class FakeRepo(FakeGitHubClient):
    """Adds what the deferred findings use: issue create/read/edit, the newest issues,
    comments (read, edit) and a comment's edit history."""

    comments: dict[int, dict[str, Any]] = field(default_factory=dict)
    edits: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    creates: int = 0
    #: The next create lands on GitHub but its response is lost.
    lose_create: bool = False
    next_number: int = 100

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if "userContentEdits" in query:
            nodes = self.edits.get(str(variables["id"]))
            return {"node": None if nodes is None else {"userContentEdits": {"nodes": nodes}}}
        return await super().graphql(query, variables)

    async def get_json(self, path: str) -> Any:
        if path.startswith(f"{REPO}/issues?"):
            newest = sorted(
                (i for i in self.issues.values() if "title" in i), key=lambda i: -i["number"]
            )
            page = int(path.rsplit("page=", 1)[1])
            return [self._rest(i) for i in newest[(page - 1) * 100 : page * 100]]
        if path.startswith(f"{REPO}/issues/comments/"):
            return dict(self.comments[int(path.rsplit("/", 1)[1])])
        if path.startswith(f"{REPO}/issues/"):
            return self._rest(self.issues[int(path.rsplit("/", 1)[1])])
        raise AssertionError(path)

    async def paginate(self, path: str) -> list[Any]:
        number = int(path.split("/issues/")[1].split("/", maxsplit=1)[0])
        return [dict(c) for c in self.comments.values() if c["issue"] == number]

    def _rest(self, issue: dict[str, Any]) -> dict[str, Any]:
        return {
            "number": issue["number"],
            "title": issue.get("title", ""),
            "body": issue.get("body", ""),
            "user": {"id": BOT},
            "created_at": "2026-10-17T00:00:00Z",
        }

    async def request(
        self, method: str, path: str, *, json_body: Any = None, expected: Any = None
    ) -> Any:
        if method == "POST" and path == f"{REPO}/issues":
            self.creates += 1
            number = self.next_number
            self.next_number += 1
            self.issues[number] = {**gh_issue(number), **json_body}
            self.writes.append(("createIssue", (number, json_body["title"])))
            if self.lose_create:
                self.lose_create = False
                raise AmbiguousRequest("GitHub request failed: ReadTimeout")
            return SimpleNamespace(status_code=201, json=lambda: {"number": number})
        if method == "PATCH" and path.startswith(f"{REPO}/issues/comments/"):
            self.comments[int(path.rsplit("/", 1)[1])]["body"] = json_body["body"]
            self.writes.append(("editComment", int(path.rsplit("/", 1)[1])))
            return SimpleNamespace(status_code=200)
        if method == "PATCH" and path.startswith(f"{REPO}/issues/"):
            self.issues[int(path.rsplit("/", 1)[1])]["body"] = json_body["body"]
            self.writes.append(("editIssue", int(path.rsplit("/", 1)[1])))
            return SimpleNamespace(status_code=200)
        return await super().request(method, path, json_body=json_body, expected=expected)


@dataclass
class Rig:
    service: FactoryService
    repo: FakeRepo
    deferred: Deferrals
    rounds: ReviewRounds
    clock: FakeClock

    async def parcel(self) -> Parcel:
        found = await self.service.db.call(lambda store: store.load_parcel(P))
        assert found is not None
        return found

    def restarted(self) -> Deferrals:
        """The same database and GitHub after a restart (nothing in memory carried)."""
        follow = tracker(self.service, self.repo)
        return Deferrals(self.service, self.repo, follow, ReviewRounds(self.service), str)


@asynccontextmanager
async def rig(config: ServiceConfig, h: Harness | None = None) -> AsyncIterator[Rig]:
    seed(config, h or ready())
    clock = FakeClock()
    clock.advance(10**9)  # after the seeded history
    service = FactoryService(
        config, adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        repo = FakeRepo({1: gh_issue(1, parent=40), 40: gh_issue(40), 5: gh_issue(5, parent=40)})
        rounds = ReviewRounds(service)
        deferred = Deferrals(service, repo, tracker(service, repo), rounds, str)
        yield Rig(service, repo, deferred, rounds, clock)
    finally:
        await service.stop()


def result(*items: dict[str, Any], head: str = HEAD, **over: Any) -> dict[str, Any]:
    return {
        "kind": "build_ready",
        "pr_number": 7,
        "head_sha": head,
        "deferred": list(items),
        **over,
    }


async def report(r: Rig, comment_id: int = 555) -> str:
    """Post the Ready report as the publication renderer would (section with markers)."""
    parcel = await r.parcel()
    assert parcel.readiness is not None
    marker = f"<!-- omnigent-factory effect={parcel.readiness.report_effect_id} -->"
    body = (
        "### PR #7 is ready for review\n\nDone.\n\n**PR:** https://github.com/o/r/pull/7"
        + await r.deferred.report_section(parcel)
        + f"\n\n{marker}"
    )
    r.repo.comments[comment_id] = {
        "id": comment_id,
        "issue": 1,
        "node_id": f"IC_{comment_id}",
        "body": body,
        "user": {"id": BOT},
    }
    return body


def ticked(body: str, *idents: str) -> str:
    markers = [rl.marker(rl.ACTION, i) for i in idents]
    return "\n".join(
        line.replace("- [ ]", "- [x]", 1) if any(m in line for m in markers) else line
        for line in body.splitlines()
    )


def iso(us: int) -> str:
    return datetime.fromtimestamp(us / S, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def edited(
    r: Rig, before: str, after: str, *, sender: int = OWNER_ID, comment_id: int = 555
) -> bytes:
    """The ``issue_comment`` ``edited`` webhook of an edit made now."""
    return json.dumps(
        {
            "action": "edited",
            "issue": {"number": 1, "node_id": P},
            "comment": {
                "id": comment_id,
                "body": after,
                "user": {"id": BOT},
                "updated_at": iso(r.clock.now_utc_us()),
            },
            "changes": {"body": {"from": before}},
            "sender": {"id": sender},
        }
    ).encode()


async def items(r: Rig) -> list[ds.DeferredItem]:
    return await r.service.db.call(
        partial(ds.deferred_items, repo_id=r.service.config.repo_id, source_issue=1)
    )


def action(item: dict[str, Any]) -> str:
    from omnigent_factory.service.deferred import item_key

    key = item_key(
        item["severity"], item.get("path", ""), item["title"], item.get("thread_url", "")
    )
    return f"d-{key[:16]}"


# ------------------------------------------------------------------ one issue per PR


async def test_one_consolidated_issue_per_pr_filed_by_the_factory_and_linked(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        await r.deferred.record_result(parcel, "s1", result(FU_A, FU_B, ADV))
        await r.deferred.drain()
        assert r.repo.creates == 1
        issue = r.repo.issues[100]
        assert issue["title"] == "Follow-ups: Add export button (#1)"
        assert "PR #7" in issue["body"]
        assert set(rl.checked(issue["body"], rl.ITEM)) == {
            i.item_key for i in await items(r) if i.outcome == "follow_up"
        }
        assert issue_marker(1, 7) in issue["body"] and "Clock going backwards" not in issue["body"]
        # Labelled follow-up, in the source's epic, blocking what the items block.
        assert ("addLabel", (100, (FOLLOW_UP_LABEL,))) in r.repo.writes
        assert ("addSubIssue", (40, 100)) in r.repo.writes
        assert ("addBlockedBy", (5, 100)) in r.repo.writes
        lineage = await r.service.db.call(
            lambda store: store.follow_up_lineage(r.service.config.repo_id, 100)
        )
        assert lineage == (1, 1)  # a follow-up of #1: its own build may defer nothing
        # A later result for the same PR adds to the same issue; repeats add nothing.
        more = {**FU_B, "title": "Second retry path", "thread_url": ""}
        await r.deferred.record_result(parcel, "s2", result(FU_A, more))
        assert r.repo.creates == 1
        assert len(rl.ids(r.repo.issues[100]["body"], rl.ITEM)) == 3
        writes = len(r.repo.writes)
        restarted = r.restarted()
        await restarted.record_result(parcel, "s2", result(FU_A, more))
        await restarted.retry_pending()
        assert r.repo.creates == 1 and len(r.repo.writes) == writes


@pytest.mark.parametrize("existing_title", ["Follow-ups from #7", "Owner renamed this issue"])
async def test_an_unclear_create_is_found_by_its_marker_never_created_twice(
    service_config: ServiceConfig,
    existing_title: str,
):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        r.repo.lose_create = True
        await r.deferred.record_result(parcel, "s1", result(FU_A))
        assert r.repo.creates == 1  # it landed, but the factory does not know its number
        assert r.repo.issues[100]["title"] == "Follow-ups: Add export button (#1)"
        r.repo.issues[100]["title"] = existing_title
        rows = await r.service.db.call(
            partial(ds.consolidated, repo_id=r.service.config.repo_id, source_issue=1)
        )
        assert [(row.state, row.issue_number) for row in rows] == [("creating", None)]
        await r.restarted().retry_pending()  # e.g. the board diff after a restart
        assert r.repo.creates == 1
        rows = await r.service.db.call(
            partial(ds.consolidated, repo_id=r.service.config.repo_id, source_issue=1)
        )
        assert [(row.state, row.issue_number) for row in rows] == [("created", 100)]
        assert all(i.listed for i in await items(r))
        assert r.repo.issues[100]["title"] == existing_title


@pytest.mark.parametrize(
    ("source_title", "expected"),
    [
        ("  Admin\n broadcasts\t on GET /me  ", "Follow-ups: Admin broadcasts on GET /me (#1)"),
        ("feat(api)!: Admin broadcasts", "Follow-ups: Admin broadcasts (#1)"),
        ("fix: Retry exports", "Follow-ups: Retry exports (#1)"),
        ("RFC: Export design", "Follow-ups: RFC: Export design (#1)"),
        ("fix:", "Follow-ups: fix: (#1)"),
        ("x" * 103, "Follow-ups: " + "x" * 103 + " (#1)"),
        ("x" * 104, "Follow-ups: " + "x" * 102 + "… (#1)"),
        ("界" * 200, "Follow-ups: " + "界" * 102 + "… (#1)"),
        ("", None),
        (" \t\n ", None),
        (None, None),  # older stored parcels have no title field
    ],
)
@pytest.mark.parametrize("pr_number", [0, 7])
async def test_consolidated_title_uses_stored_source_title(
    service_config: ServiceConfig, monkeypatch, source_title, expected, pr_number
):
    async with rig(service_config) as r:
        await r.service.db.call(
            lambda store: store.query(
                "UPDATE parcels SET aggregate_json = "
                + (
                    "json_remove(aggregate_json, '$.parcel.issue_title')"
                    if source_title is None
                    else "json_set(aggregate_json, '$.parcel.issue_title', ?)"
                )
                + " WHERE parcel_id = ?",
                (P,) if source_title is None else (source_title, P),
            )
        )
        get_json = r.repo.get_json

        async def no_source_read(path: str):
            assert path != f"{REPO}/issues/1", "The title must come from the stored parcel"
            return await get_json(path)

        monkeypatch.setattr(r.repo, "get_json", no_source_read)
        # A fresh Deferrals instance has only persisted state to work from.
        await r.restarted().record_result(await r.parcel(), "s1", result(FU_A, pr_number=pr_number))
        title = r.repo.issues[100]["title"]
        assert title == (expected or f"Follow-ups from #{pr_number or 1}")
        assert len(title) <= 120


async def test_the_report_lists_each_item_with_a_hidden_action_marker(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, ADV))
        section = await r.deferred.report_section(await r.parcel())
        assert "**Deferred**" in section
        lines = [line for line in section.splitlines() if line.startswith("- [ ]")]
        assert lines == [
            "- [ ] Fix now: P2 `api/src/export.ts` Retry leaks the delivery row (in #100) "
            f"{rl.marker(rl.ACTION, action(FU_A))}",
            "- [ ] Fix now: P3 Clock going backwards (advisory: no realistic trigger here) "
            f"{rl.marker(rl.ACTION, action(ADV))}",
        ]
        assert "One more" not in section  # rounds not used up


# ------------------------------------------------------------------ owner ticks


async def test_owner_ticks_within_the_window_start_exactly_one_rework(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B, ADV))
        body = await report(r)
        first = ticked(body, action(FU_A), action(ADV))
        assert await r.deferred.on_comment_delivery(edited(r, body, first)) == 2
        assert await r.deferred.on_comment_delivery(edited(r, body, first)) == 0  # redelivery
        r.clock.advance(60 * S)
        assert await r.deferred.tick() == 0  # inside the window: nothing yet
        second = ticked(first, action(FU_B))
        r.repo.comments[555]["body"] = second
        assert await r.deferred.on_comment_delivery(edited(r, first, second)) == 1
        r.clock.advance(61 * S)
        assert await r.deferred.tick() == 1
        parcel = await r.parcel()
        assert parcel.stage == Stage.BUILDING and parcel.note == "Rework: Ready report items ticked"
        reworks = [
            a for a in parcel.authorizations if a.source_event_id.startswith("report-items:")
        ]
        assert len(reworks) == 1 and reworks[0].rework
        brief = await r.deferred.rework_brief(parcel, reworks[0].source_event_id)
        assert brief is not None and len(brief.splitlines()) == 3
        assert "Retry leaks the delivery row" in brief and "listed in follow-up issue #100" in brief
        text = r.repo.comments[555]["body"]
        assert text.count("→ fixing (rework started)") == 3
        # A restart, more ticks of the same boxes, another tick pass: nothing twice.
        again = r.restarted()
        assert await again.on_comment_delivery(edited(r, body, second)) == 0
        r.clock.advance(600 * S)
        assert await again.tick() == 0
        parcel = await r.parcel()
        assert (
            len([a for a in parcel.authorizations if a.source_event_id.startswith("report-")]) == 1
        )


async def test_a_tick_by_anyone_else_is_ignored(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A))
        body = await report(r)
        theirs = ticked(body, action(FU_A))
        assert (
            await r.deferred.on_comment_delivery(edited(r, body, theirs, sender=OTHER_USER_ID)) == 0
        )
        assert await r.deferred.on_comment_delivery(edited(r, body, theirs, sender=BOT)) == 0
        r.clock.advance(600 * S)
        assert await r.deferred.tick() == 0
        assert (await r.parcel()).stage == Stage.READY


async def test_rewriting_lines_keeps_ticks_not_yet_processed(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        body = await report(r)
        mine = ticked(body, action(FU_A))
        await r.deferred.on_comment_delivery(edited(r, body, mine))
        # The owner ticks FU_B in the moment between that webhook and the factory's write.
        r.repo.comments[555]["body"] = ticked(mine, action(FU_B))
        r.clock.advance(121 * S)
        await r.deferred.tick()
        text = r.repo.comments[555]["body"]
        assert rl.checked(text) == {action(FU_A): True, action(FU_B): True}
        assert "Retry leaks the delivery row (in #100) → fixing" in text
        assert "Unbounded retry loop (in #100) <!--" in text  # untouched: not processed yet


async def test_fixed_items_are_shown_and_ticked_in_the_follow_up_issue(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        body = await report(r)
        await r.deferred.on_comment_delivery(edited(r, body, ticked(body, action(FU_A))))
        r.clock.advance(121 * S)
        await r.deferred.tick()
        parcel = await r.parcel()
        rework = parcel.authorizations[-1]
        # The rework run's build_ready (its session is the one the rework authorized).
        session = SimpleNamespace(authorization_id=rework.authorization_id)
        rework_run = _with_session(parcel, "s-rework", session)
        await r.deferred.record_result(rework_run, "s-rework", result(head=FIX))
        assert f"→ fixed in `{FIX[:7]}`" in r.repo.comments[555]["body"]
        issue = r.repo.issues[100]["body"]
        assert rl.checked(issue, rl.ITEM) == {
            next(i.item_key for i in await items(r) if i.title == FU_A["title"]): True,
            next(i.item_key for i in await items(r) if i.title == FU_B["title"]): False,
        }
        assert f"→ fixed in #7 (`{FIX[:7]}`)" in issue
        assert ("closeIssue", 100) not in r.repo.writes  # the owner closes it


def _with_session(parcel: Parcel, session_id: str, session: Any) -> Any:
    """``parcel`` whose ``session(session_id)`` is ``session`` (the rework run)."""

    class View:
        def __getattr__(self, name: str) -> Any:
            return getattr(parcel, name)

        def session(self, sid: str) -> Any:
            return session if sid == session_id else parcel.session(sid)

    return View()


# ------------------------------------------------------------------ one more round


def exhausted() -> Harness:
    h = ready()
    r = h.p().readiness
    assert r is not None
    h.send(
        P,
        ev.ReadinessEvidence(
            session_id=r.session_id,
            pr_number=7,
            head_sha=HEAD,
            verified=True,
            review_bot_pending_since_us=0,
            review_rounds_used=2,
            review_rounds_budget=2,
            review_rounds_note="Codex round 2/2",
        ),
    )
    assert h.p().stage == Stage.READY and h.p().readiness.review_rounds_budget == 2
    return h


async def test_one_more_round_raises_the_budget_once(service_config: ServiceConfig):
    async with rig(service_config, exhausted()) as r:
        assert await r.rounds.budget(await r.parcel()) == 2
        body = await report(r)
        assert "One more Codex review round" in body
        more = ticked(body, "round-2")
        assert await r.deferred.on_comment_delivery(edited(r, body, more)) == 1
        r.clock.advance(121 * S)
        assert await r.deferred.tick() == 1
        parcel = await r.parcel()
        assert parcel.stage == Stage.BUILDING
        assert await r.rounds.budget(parcel) == 3
        brief = await r.deferred.rework_brief(parcel, parcel.authorizations[-1].source_event_id)
        assert brief is not None and "comment `@codex review` on the PR" in brief
        # Replays (a restart, the same webhook again): still one more, not two.
        restarted = r.restarted()
        await restarted.on_comment_delivery(edited(r, body, more))
        r.clock.advance(600 * S)
        await restarted.tick()
        assert await r.rounds.budget(await r.parcel()) == 3
        assert "→ one more round granted (rework started)" in r.repo.comments[555]["body"]


# ------------------------------------------------------------------ lost webhooks


async def test_a_lost_tick_is_recovered_only_from_an_owner_edit(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        body = await report(r)
        by_owner = ticked(body, action(FU_A))
        by_other = ticked(by_owner, action(FU_B))
        r.repo.comments[555]["body"] = by_other  # both webhooks were lost
        now = r.clock.now_utc_us()
        r.repo.edits["IC_555"] = [
            # newest first, as GitHub lists them
            {"editedAt": iso(now), "diff": by_other, "editor": {"databaseId": 99}},
            {"editedAt": iso(now - 10 * S), "diff": by_owner, "editor": {"databaseId": OWNER_ID}},
            {"editedAt": iso(now - 20 * S), "diff": body, "editor": {"databaseId": BOT}},
        ]
        assert await r.deferred.catch_up() == 1
        actions = await r.service.db.call(
            partial(ds.report_actions, repo_id=r.service.config.repo_id, parcel_id=P)
        )
        assert set(actions) == {action(FU_A)} and actions[action(FU_A)].via == "recovered"
        assert await r.deferred.catch_up() == 0  # once
        r.clock.advance(121 * S)
        assert await r.deferred.tick() == 1
        parcel = await r.parcel()
        assert parcel.stage == Stage.BUILDING
        stored = await r.service.db.call(
            lambda store: store.query(
                "SELECT provenance FROM events WHERE event_id LIKE 'report-items:%'"
            )
        )
        assert [row[0] for row in stored] == [Provenance.RECOVERY.value]


async def test_a_lost_tick_without_edit_history_is_ignored(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A))
        body = await report(r)
        r.repo.comments[555]["body"] = ticked(body, action(FU_A))
        assert await r.deferred.catch_up() == 0  # no userContentEdits readable
        r.clock.advance(600 * S)
        assert await r.deferred.tick() == 0
        assert (await r.parcel()).stage == Stage.READY


async def test_the_admitted_rework_is_briefed_with_exactly_the_ticked_items(
    service_config: ServiceConfig,
):
    from omnigent_factory.service.directory import ServiceDispatchDirectory

    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A, FU_B))
        body = await report(r)
        assert (
            await r.deferred.on_comment_delivery(edited(r, body, ticked(body, action(FU_B)))) == 1
        )
        r.clock.advance(121 * S)
        assert await r.deferred.tick() == 1
        await r.service._admit_once()
        parcel = await r.parcel()
        run = parcel.current_session
        assert run is not None and parcel.authorization(run.authorization_id).rework
        directory = ServiceDispatchDirectory(r.service.db, r.service.config)
        directory.rework_brief = r.deferred.rework_brief
        snapshot = await directory._make_snapshot(parcel, run)
        assert snapshot["template"] == "build-rework-items-v6.txt"
        assert snapshot["brief"].startswith("- Fix the P1 finding in `api/src/retry.ts`")
        assert "Retry leaks" not in snapshot["brief"]  # FU_A was not ticked
        # A rework from an ordinary owner comment keeps the ordinary template.
        directory.rework_brief = None
        assert (await directory._make_snapshot(parcel, run))["template"] == ("build-rework-v15.txt")


async def test_a_crash_before_the_follow_up_issue_update_is_finished_after_restart(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.deferred.record_result(await r.parcel(), "s1", result(FU_A))
        body = await report(r)
        await r.deferred.on_comment_delivery(edited(r, body, ticked(body, action(FU_A))))
        r.clock.advance(121 * S)
        await r.deferred.tick()
        before = r.repo.issues[100]["body"]
        # The rework's build_ready is recorded, then the process dies before the issue
        # update: the batch is done, its item not yet ticked in the follow-up issue.
        await r.service.db.call(
            partial(
                ds.finish_batch,
                repo_id=r.service.config.repo_id,
                batch_id=(
                    await r.service.db.call(partial(ds.batches, repo_id=r.service.config.repo_id))
                )[0].batch_id,
                state="done",
                reason=FIX[:12],
            )
        )
        assert r.repo.issues[100]["body"] == before
        await r.restarted().retry_pending()
        assert f"→ fixed in #7 (`{FIX[:7]}`)" in r.repo.issues[100]["body"]
        writes = len(r.repo.writes)
        await r.restarted().retry_pending()  # done once
        assert len(r.repo.writes) == writes
