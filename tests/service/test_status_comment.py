"""The status comment: one factory-owned dashboard comment per issue, edited in place.

Created once and adopted by its marker after an unclear create or a restart, edited only
when its text changes and at most once per ``status_comment_edit_seconds`` (a burst is
coalesced), owner ticks kept and acted on as in the Ready report, pinned once (a refusal
recorded and never retried; never on a pull request), the triage result and checkpoint
report folded into it (their side effects unchanged), the Ready report reduced to one
line linking to it, and the epic variant.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import partial
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.effects import (
    Ack,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
)
from omnigent_factory.core.types import (
    MICROS_PER_SECOND,
    BotState,
    IssueLinks,
    LinkedIssue,
    Parcel,
    Stage,
)
from omnigent_factory.github.adapter import GitHubAPIAdapter, TriageFields
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.deferred import Deferrals
from omnigent_factory.service.directory import (
    PublicationRenderer,
    ServiceDispatchDirectory,
    ServiceParcelResolver,
)
from omnigent_factory.service.doctor import DoctorReport, _check_status_pin
from omnigent_factory.service.review_rounds import ReviewRounds
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.status_comment import (
    EpicView,
    Question,
    StatusComments,
    SubIssue,
    epic_headline,
    render_epic,
    status_marker,
)
from omnigent_factory.store import discipline as ds
from omnigent_factory.store import status as st
from omnigent_factory.testing.builders import OWNER_ID, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from omnigent_factory.testing.harness import HEAD, Harness
from tests.service.test_epic_autopilot import seed
from tests.test_rework import ready

pytestmark = pytest.mark.asyncio

P = "I_parcel_1"
ISSUE = 1
S = MICROS_PER_SECOND
CTX = ExecutionContext("boot", 1, 1, 1)


@dataclass
class GitHubServer:
    """The REST/GraphQL shapes the status comment and the comment publications use."""

    repository: str
    bot_id: int
    project_id: str = ""
    comments: dict[int, dict[str, Any]] = field(default_factory=dict)
    requests: list[tuple[str, str, Any]] = field(default_factory=list)
    next_id: int = 9000
    #: The next comment POST lands but its response is lost.
    lose_post: bool = False
    #: GraphQL errors answering pinIssueComment (None: it succeeds).
    pin_error: str | None = None
    #: Comments are created under /pull/ (a pull request's conversation).
    as_pull: bool = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        path = request.url.path
        self.requests.append((request.method, path, body))
        repo = f"/repos/{self.repository}"
        if path.startswith(f"{repo}/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            if comment_id not in self.comments:
                return httpx.Response(404)
            if request.method == "PATCH":
                self.comments[comment_id]["body"] = body["body"]
            return httpx.Response(200, json=self.comments[comment_id])
        if path.startswith(f"{repo}/issues/") and path.endswith("/comments"):
            number = int(path.split("/issues/")[1].split("/")[0])
            if request.method == "GET":
                return httpx.Response(
                    200, json=[c for c in self.comments.values() if c["issue"] == number]
                )
            self.next_id += 1
            kind = "pull" if self.as_pull else "issues"
            comment = {
                "id": self.next_id,
                "node_id": f"IC_{self.next_id}",
                "html_url": f"https://github.com/{self.repository}/{kind}/{number}"
                f"#issuecomment-{self.next_id}",
                "body": body["body"],
                "user": {"id": self.bot_id},
                "issue": number,
                "created_at": "2026-10-25T00:00:00Z",
            }
            self.comments[self.next_id] = comment
            if self.lose_post:
                self.lose_post = False
                raise httpx.ReadTimeout("lost", request=request)
            return httpx.Response(201, json=comment)
        if path == f"{repo}/labels":
            return httpx.Response(200, json=[{"name": "area:api"}])
        if path.endswith("/labels"):
            return httpx.Response(200, json=[])
        if path == "/graphql":
            return httpx.Response(200, json=self._graphql(body))
        return httpx.Response(404)

    def _graphql(self, body: dict[str, Any]) -> dict[str, Any]:
        query = body["query"]
        if "pinIssueComment" in query:
            if self.pin_error is not None:
                return {"data": None, "errors": [{"type": "FORBIDDEN", "message": self.pin_error}]}
            return {"data": {"pinIssueComment": {"clientMutationId": None}}}
        if "updateProjectV2ItemFieldValue" in query:
            return {"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "PVTI"}}}}
        if "fields(first: 100)" in query:
            options = [{"id": "o_p2", "name": "P2"}]
            nodes = [{"id": "F_PRIORITY", "name": "Priority", "options": options}]
            return {"data": {"node": {"fields": {"nodes": nodes}}}}
        if "fieldValueByName(name: $fieldName)" in query:
            return {"data": {"node": {"fieldValueByName": None}}}
        return {
            "data": {
                "node": {
                    "projectItems": {
                        "nodes": [{"id": "PVTI", "project": {"id": self.project_id}}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    def posts(self) -> list[str]:
        return [
            b["body"]
            for m, p, b in self.requests
            if m == "POST" and p.endswith("/comments") and "/issues/comments/" not in p
        ]

    def patches(self) -> list[str]:
        return [b["body"] for m, p, b in self.requests if m == "PATCH"]

    def pins(self) -> int:
        return sum(1 for m, p, b in self.requests if p == "/graphql" and "pinIssue" in b["query"])

    def status(self, issue: int = ISSUE) -> list[dict[str, Any]]:
        return [c for c in self.comments.values() if status_marker(issue) in c["body"]]


@dataclass
class Rig:
    service: FactoryService
    server: GitHubServer
    client: GitHubClient
    directory: ServiceDispatchDirectory
    status: StatusComments
    clock: FakeClock
    section: list[str]

    def restarted(self) -> StatusComments:
        """A new process on the same database and GitHub (nothing in memory carried)."""
        return self.make()

    def make(self) -> StatusComments:
        return StatusComments(
            self.service,
            self.client,
            self.directory,
            str,
            deferred_section=self._section,
        )

    async def _section(self, parcel: Parcel) -> str:
        return self.section[0]

    async def parcel(self) -> Parcel:
        found = await self.service.db.call(lambda store: store.load_parcel(P))
        assert found is not None
        return found

    def write_result(self, session_id: str, result: dict[str, Any]) -> None:
        results = self.service.config.state_dir / "results"
        results.mkdir(mode=0o700, exist_ok=True)
        payload = {"factory_result": {"result": result}, "contract_canonical": None}
        (results / f"{session_id}-latest.json").write_text(json.dumps(payload))

    def adapter(self) -> GitHubAPIAdapter:
        config = self.service.config
        renderer = PublicationRenderer(self.directory, config)
        adapter = GitHubAPIAdapter(
            self.client,
            repository=config.repository,
            repository_node_id=config.repo_id,
            project_node_id=config.project_node_id,
            status_field_node_id=config.status_field_node_id,
            bot_user_id=config.github_bot_user_id,
            required_checks=frozenset(),
            publication_renderer=renderer,
            parcel_resolver=ServiceParcelResolver(self.service.db),
            triage_fields=renderer.triage_fields,
        )
        adapter.status_comments = self.status
        return adapter


@asynccontextmanager
async def rig(config: ServiceConfig, h: Harness | None = None) -> AsyncIterator[Rig]:
    seed(config, h or ready())
    clock = FakeClock()
    clock.advance(10**9)  # after the seeded history
    service = FactoryService(
        config, adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    server = GitHubServer(config.repository, config.github_bot_user_id, config.project_node_id)
    http = httpx.AsyncClient(transport=httpx.MockTransport(server))
    try:
        await service.operator_command("unpause", {})
        client = GitHubClient(http, "token")
        directory = ServiceDispatchDirectory(service.db, config)
        r = Rig(service, server, client, directory, None, clock, [""])  # type: ignore[arg-type]
        r.status = r.make()
        yield r
    finally:
        await http.aclose()
        await service.stop()


def effect(kind: EffectKind, session_id: str = "", **args: Any) -> EffectIntent:
    return EffectIntent(
        effect_id=f"ef_{kind.value}_{args.get('report', '')}",
        kind=kind,
        parcel_id=P,
        target=P,
        preconditions=Preconditions(
            parcel_version=1, eligibility_epoch=0, session_id=session_id or None
        ),
        args={"session_id": session_id, **args} if session_id else args,
    )


TRIAGE = {
    "kind": "triage",
    "summary": "The export retry leaks rows; a small fix in the worker.",
    "recommendation": "fix",
    "priority": "P2",
    "size": "S",
    "labels": ["area:api", "factory:build"],
    "missing_information": [],
    "related": [{"issue": 12, "relation": "overlaps", "note": "#12 owns the UI side"}],
}


# ------------------------------------------------------------------ create / adopt


async def test_created_once_and_adopted_after_an_unclear_create_and_a_restart(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        r.server.lose_post = True  # the create lands; its response is lost
        first = await r.status.publish(P)
        assert not first.ok
        row = await r.service.db.call(
            partial(st.status_row, repo_id=service_config.repo_id, parcel_id=P)
        )
        assert row is not None and row.state == "creating"
        # A restart: the marker finds the comment, nothing is posted twice.
        again = await r.restarted().publish(P)
        assert again.ok and again.comment_id is not None
        assert len(r.server.posts()) == 1 and len(r.server.status()) == 1
        # Later refreshes (another restart included) edit that one comment only.
        await r.restarted().refresh(P)
        await r.status.tick()
        assert len(r.server.posts()) == 1 and len(r.server.status()) == 1
        [comment] = r.server.status()
        assert comment["body"].startswith("### Ready · PR #7")
        assert comment["body"].endswith(status_marker(ISSUE))


async def test_a_deleted_status_comment_is_created_again(service_config: ServiceConfig):
    async with rig(service_config) as r:
        first = await r.status.publish(P)
        assert first.comment_id is not None
        del r.server.comments[first.comment_id]
        r.section[0] = "\n\nchanged"
        again = await r.status.publish(P)
        assert again.ok and again.comment_id != first.comment_id
        assert len(r.server.status()) == 1


async def test_no_backfill_sweep_existing_parcels_get_one_at_their_next_change(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        assert await r.status.tick() == 0  # unchanged since status comments started
        assert r.server.posts() == []
        r.clock.advance(5 * S)
        await r.service.operator_command("pause", {})  # touches the repository only
        assert await r.status.tick() == 0 and r.server.posts() == []
        # A change of the parcel: its status comment is created then.
        await r.service.db.call(
            lambda store: store.query(
                "UPDATE parcels SET updated_at_us = ? WHERE parcel_id = ?",
                (r.clock.now_utc_us(), P),
            )
        )
        assert await r.status.tick() == 1
        assert len(r.server.status()) == 1


# ------------------------------------------------------------------ edits


async def test_edited_only_on_change_and_rate_limited_with_bursts_coalesced(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        await r.status.publish(P)
        assert len(r.server.posts()) == 1
        await r.status.refresh(P)
        assert r.server.patches() == []  # nothing changed: no edit
        r.clock.advance(10 * S)
        r.section[0] = "\n\nfirst change"
        await r.status.refresh(P)
        r.clock.advance(10 * S)
        r.section[0] = "\n\nsecond change"
        await r.status.tick()
        assert r.server.patches() == []  # within the edit interval: held back
        row = await r.service.db.call(
            partial(st.status_row, repo_id=service_config.repo_id, parcel_id=P)
        )
        assert row is not None and row.pending
        r.clock.advance(170 * S)  # 190 s after the create
        assert await r.status.tick() == 1
        [edit] = r.server.patches()
        assert "second change" in edit and "first change" not in edit
        assert await r.status.tick() == 0 and len(r.server.patches()) == 1


async def test_the_edit_interval_applies_on_reload(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.status.publish(P)
        r.service.config = r.service.config.model_copy(update={"status_comment_edit_seconds": 0})
        r.clock.advance(1 * S)
        r.section[0] = "\n\nchanged"
        await r.status.refresh(P)
        assert len(r.server.patches()) == 1


async def test_an_edit_keeps_the_owners_ticks(service_config: ServiceConfig):
    async with rig(service_config) as r:
        r.section[0] = "\n\n" + "\n".join(
            [rl.line(rl.ACTION, "d-one", "Fix now: one"), rl.line(rl.ACTION, "d-two", "two")]
        )
        written = await r.status.publish(P)
        assert written.comment_id is not None
        comment = r.server.comments[written.comment_id]
        comment["body"] = comment["body"].replace("- [ ] Fix now: one", "- [x] Fix now: one")
        r.section[0] += "\n\nsomething else changed"
        await r.status.publish(P)
        [edit] = r.server.patches()
        assert rl.checked(edit) == {"d-one": True, "d-two": False}
        assert "something else changed" in edit


async def test_owner_ticks_on_the_status_comment_start_the_same_rework(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        rounds = ReviewRounds(r.service)
        deferred = Deferrals(r.service, r.client, None, rounds, str)  # type: ignore[arg-type]
        status = StatusComments(
            r.service, r.client, r.directory, str, deferred_section=deferred.report_section
        )
        deferred.status = status
        parcel = await r.parcel()
        [item] = await r.service.db.call(
            partial(
                ds.record_deferred,
                repo_id=service_config.repo_id,
                source_issue=ISSUE,
                pr_number=7,
                session_id=parcel.readiness.session_id if parcel.readiness else "",
                items=[
                    ds.NewDeferred(
                        item_key="k" * 24,
                        severity="P2",
                        path="api/x.ts",
                        title="Retry leaks a row",
                        thread_url="",
                        reason="outside the plan",
                        outcome="advisory",
                    )
                ],
            )
        )
        written = await status.publish(P)
        assert written.comment_id is not None
        before = r.server.comments[written.comment_id]["body"]
        assert f"<!-- factory-action:{item.action_id} -->" in before
        after = before.replace("- [ ] Fix now", "- [x] Fix now")
        r.server.comments[written.comment_id]["body"] = after
        stamp = datetime.fromtimestamp(r.clock.now_utc_us() / S, UTC).isoformat()
        delivery = {
            "action": "edited",
            "comment": {
                "id": written.comment_id,
                "user": {"id": service_config.github_bot_user_id},
                "body": after,
                "updated_at": stamp.replace("+00:00", "Z"),
            },
            "issue": {"node_id": P, "number": ISSUE},
            "changes": {"body": {"from": before}},
            "sender": {"id": OWNER_ID},
        }
        assert await deferred.on_comment_delivery(json.dumps(delivery)) == 1
        actions = await r.service.db.call(
            partial(ds.report_actions, repo_id=service_config.repo_id, parcel_id=P)
        )
        assert actions[item.action_id].comment_id == written.comment_id
        r.clock.advance(121 * S)  # past the batch window
        assert await deferred.tick() == 1
        assert (await r.parcel()).stage == Stage.BUILDING  # the rework started
        # The status comment shows it (written at once: it answers the owner's tick).
        body = r.server.comments[written.comment_id]["body"]
        assert "→ fixing (rework started)" in body
        assert rl.checked(body)[item.action_id] is True
        assert len(r.server.status()) == 1


# ------------------------------------------------------------------ pinning


async def test_pinned_once_after_it_is_created(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.status.publish(P)
        assert r.server.pins() == 1
        r.section[0] = "\n\nchanged"
        await r.status.publish(P)
        assert r.server.pins() == 1
        settings = await r.service.db.call(
            partial(st.status_settings, repo_id=service_config.repo_id)
        )
        assert settings.pin_state == "supported"


async def test_a_pin_refusal_is_recorded_once_and_never_retried(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    h = ready()
    h.to_building("I_parcel_2")  # a second issue in the same repository
    async with rig(service_config, h) as r:
        r.server.pin_error = "Resource not accessible by integration"
        with caplog.at_level(logging.WARNING):
            await r.status.publish(P)
            await r.status.publish("I_parcel_2")
        assert r.server.pins() == 1  # the second issue's comment is not tried
        settings = await r.service.db.call(
            partial(st.status_settings, repo_id=service_config.repo_id)
        )
        assert settings.pin_state == "unsupported"
        assert "Resource not accessible by integration" in settings.pin_message
        unsupported = [m for m in caplog.messages if "pinning unsupported" in m]
        assert len(unsupported) == 1
        assert "Resource not accessible by integration" in unsupported[0]
        report = DoctorReport()
        _check_status_pin(service_config, report)
        assert "Resource not accessible by integration" in report.checks["status_comment_pin"]


async def test_a_pull_request_comment_is_never_pinned(service_config: ServiceConfig):
    async with rig(service_config) as r:
        r.server.as_pull = True
        written = await r.status.publish(P)
        assert written.ok and r.server.pins() == 0
        row = await r.service.db.call(
            partial(st.status_row, repo_id=service_config.repo_id, parcel_id=P)
        )
        assert row is not None and row.pin == "skipped"


async def test_doctor_reports_pin_not_tried_on_a_fresh_database(service_config: ServiceConfig):
    report = DoctorReport()
    _check_status_pin(service_config, report)
    assert report.checks["status_comment_pin"].startswith("not tried yet")


# ------------------------------------------------------------------ folded publications


def triage_harness() -> tuple[Harness, str]:
    h = Harness()
    s = h.triage()
    return h, s.session_id


async def test_triage_posts_no_comment_of_its_own_but_its_side_effects_happen(
    service_config: ServiceConfig,
):
    h, sid = triage_harness()
    async with rig(service_config, h) as r:
        r.write_result(sid, TRIAGE)
        adapter = r.adapter()
        marked: list[str] = []
        adapter.related_marker = lambda e: marked.append(e.effect_id)
        fields: list[TriageFields] = []

        async def triage_fields(e: EffectIntent) -> TriageFields | None:
            found = await PublicationRenderer(r.directory, service_config).triage_fields(e)
            fields.append(found)  # type: ignore[arg-type]
            return found

        adapter.triage_fields = triage_fields
        publish = effect(EffectKind.PUBLISH_TRIAGE, sid)
        outcome = await adapter.execute(publish, CTX)
        assert isinstance(outcome, Ack), outcome
        [status] = r.server.status()
        assert outcome.remote_id == str(status["id"])  # the owner sees it there
        assert r.server.posts() == [status["body"]]  # no triage comment of its own
        assert "#### Triage" in status["body"] and TRIAGE["summary"] in status["body"]
        assert "Overlaps #12: #12 owns the UI side" in status["body"]  # the Related list
        # Labels, Priority and related marks exactly as before.
        assert marked == [publish.effect_id] and fields and fields[0].priority == "P2"
        label_posts = [b for m, p, b in r.server.requests if m == "POST" and p.endswith("/labels")]
        assert label_posts == [{"labels": ["area:api"]}]
        assert outcome.detail["labels"] == ["area:api"]
        assert outcome.detail["fields"] == {"Priority": "P2", "Size": "unavailable"}


async def test_an_epic_triage_keeps_its_own_comment(service_config: ServiceConfig):
    h, sid = triage_harness()
    async with rig(service_config, h) as r:
        r.write_result(
            sid,
            {"kind": "epic_triage", "summary": "Three parts.", "build_order": [], "coverage": {}},
        )
        outcome = await r.adapter().execute(effect(EffectKind.PUBLISH_TRIAGE, sid), CTX)
        assert isinstance(outcome, Ack)
        [post] = r.server.posts()
        assert "### Epic triage" in post and status_marker(ISSUE) not in post


async def test_ready_posts_one_line_linking_the_updated_status_comment(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        assert parcel.readiness is not None
        r.write_result(
            parcel.readiness.session_id,
            {
                "kind": "build_ready",
                "pr_number": 7,
                "head_sha": HEAD,
                "summary": "Retries now release the row.",
                "review": {
                    "implementation_vendor": "anthropic",
                    "review_vendor": "openai",
                    "accepted": True,
                    "reviewed_head": HEAD,
                },
                "findings": [],
            },
        )
        ready_report = effect(
            EffectKind.PUBLISH_REPORT,
            report="ready",
            pr_number=7,
            head_sha=HEAD,
            checks_summary="3 checks: 3 success",
        )
        outcome = await r.adapter().execute(ready_report, CTX)
        assert isinstance(outcome, Ack), outcome
        status, line = r.server.posts()
        assert status_marker(ISSUE) in status
        assert "#### Pull request" in status and "Retries now release the row." in status
        assert "Cross-vendor review" in status
        [comment] = r.server.status()
        url = comment["html_url"]
        assert line.splitlines()[0] == f"PR #7 is ready for review — {url}"
        assert "omnigent-factory effect=" in line


async def test_checkpoint_posts_one_comment_and_shows_the_report_in_the_status(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        parcel = await r.parcel()
        sid = parcel.current_session_id or ""
        r.write_result(
            sid,
            {"kind": "checkpoint", "done": ["wrote the worker"], "remaining": ["tests"]},
        )
        adapter = r.adapter()
        report = effect(EffectKind.PUBLISH_REPORT, sid, report="checkpoint")
        assert isinstance(await adapter.execute(report, CTX), Ack)
        assert len(r.server.posts()) == 1  # the status comment only
        [status] = r.server.status()
        assert "#### Last checkpoint" in status["body"] and "wrote the worker" in status["body"]
        template = effect(EffectKind.POST_COMMENT, template="checkpoint")
        assert isinstance(await adapter.execute(template, CTX), Ack)
        posts = r.server.posts()
        assert len(posts) == 2 and "/continue" in posts[1]  # the one checkpoint comment


async def test_a_ready_report_edit_updates_the_status_comment_not_the_old_report(
    service_config: ServiceConfig,
):
    async with rig(service_config) as r:
        old = {
            "id": 555,
            "issue": ISSUE,
            "user": {"id": service_config.github_bot_user_id},
            "body": "### PR #7 is ready for review\n\n**PR:** x\n\n"
            "<!-- omnigent-factory effect=ef_old_report -->",
        }
        r.server.comments[555] = dict(old)
        await r.status.publish(P)
        r.section[0] = "\n\nlate bot verdict"
        r.clock.advance(200 * S)
        edit = effect(
            EffectKind.EDIT_REPORT,
            report="ready",
            pr_number=7,
            head_sha=HEAD,
            report_effect_id="ef_old_report",
        )
        assert isinstance(await r.adapter().execute(edit, CTX), Ack)
        assert r.server.comments[555]["body"] == old["body"]  # left as it is
        [patch] = r.server.patches()
        assert "late bot verdict" in patch and status_marker(ISSUE) in patch


# ------------------------------------------------------------------ rendering


async def test_the_issue_status_shows_plan_pr_and_history(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.status.publish(P)
        [comment] = r.server.status()
        body = comment["body"]
        assert body.startswith("### Ready · PR #7 · checks")
        assert "#### Plan" in body and "· approved" in body
        assert "<details><summary>History</summary>" in body
        assert "ef_" not in body and "I_parcel" not in body  # no internal ids


def sub(number: int, **kw: Any) -> SubIssue:
    return SubIssue(number=number, title=f"Part {number}", open=kw.pop("open", True), **kw)


def epic_view() -> EpicView:
    return EpicView(
        issue=40,
        done=1,
        total=8,
        autopilot="Full",
        order=(43, 44, 45, 46),
        next_issue=43,
        plan_url="https://github.com/o/r/issues/40#issuecomment-1",
        plan_label="Epic plan `abc`",
        gate_assignee="You",
        question_assignee="You",
        subs=(
            sub(41, open=False, pr_number=101),
            sub(
                42,
                stage="Building",
                bot=BotState.WORKING,
                pr_number=102,
                building=True,
                started=True,
            ),
            sub(43),
            sub(44, blockers=(("#42", False), ("other/repo#9", True))),
            sub(45, gate=True),
            sub(
                46,
                stage="Planning",
                bot=BotState.NEEDS_YOU,
                question=Question("Which API?"),
                started=True,
            ),
            sub(47, epic=(2, 3)),
            sub(48, held=True),
        ),
    )


async def test_epic_rendering_groups_waiting_and_lists_blocked_nested_and_done():
    view = epic_view()
    headline = epic_headline(view)
    assert headline == "Epic · 1/8 done · 1 building · autopilot full · next: #43"
    body = render_epic(view, [(1_800_000_000_000_000, headline)], headline)
    waiting = body.split("#### Waiting on people")[1].split("####")[0]
    assert "**You**" in waiting
    assert "#45 Part 45 (a step only a person can do)" in waiting
    assert "#46 Part 46: Which API?" in waiting
    assert "#42 Part 42 — Building · PR #102 · working" in body.split("#### In progress")[1]
    assert "#44 Part 44 — waits on #42, other/repo#9 (outside this epic)" in body
    assert "#47 Part 47 — epic · 2/3 done" in body
    up_next = body.split("#### Up next")[1].split("**")[0]
    assert up_next.strip().splitlines() == ["- #43 Part 43", "- #48 Part 48"]
    assert "**Follow-ups held:** 1 (#48)" in body
    assert "<details><summary>Done (1)</summary>" in body and "#41 Part 41 (PR #101)" in body
    assert "Epic plan `abc`: https://github.com/o/r/issues/40#issuecomment-1" in body


async def test_epic_waiting_on_people_is_grouped_by_assignee():
    view = replace(epic_view(), gate_assignee="Owner (GitHub user 7)", question_assignee="Owners")
    body = render_epic(view, [], epic_headline(view))
    waiting = body.split("#### Waiting on people")[1].split("####")[0]
    gate_group = waiting.split("**Owner (GitHub user 7)**")[1].split("**Owners**")[0]
    assert "#45" in gate_group and "#46" not in gate_group
    assert "#46 Part 46: Which API?" in waiting.split("**Owners**")[1]


async def test_the_epic_status_is_built_from_stored_parcels(service_config: ServiceConfig):
    h = ready()  # issue #1: Ready with PR #7, a sub-issue of the epic
    epic = "I_epic"
    f = h.f(epic)  # issue #2
    links = IssueLinks(
        sub_issues=(
            LinkedIssue(1, True, "Export button"),
            LinkedIssue(50, True, "Docs page"),
            LinkedIssue(51, False, "Old cleanup"),
        ),
        sub_total=3,
        sub_completed=1,
    )
    h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, links=links)))
    async with rig(service_config, h) as r:
        written = await r.status.publish(epic)
        assert written.ok
        [comment] = r.server.status(2)
        body = comment["body"]
        assert body.startswith("### Epic · 1/3 done · 0 building · autopilot off · next: #50")
        assert "- #1 Export button — Ready · PR #7 · idle" in body.split("#### In progress")[1]
        assert "- #50 Docs page" in body.split("#### Up next")[1]
        assert "<details><summary>Done (1)</summary>" in body and "#51 Old cleanup" in body


async def test_the_reconcile_tick_writes_pending_status_comments(service_config: ServiceConfig):
    async with rig(service_config) as r:
        await r.status.publish(P)
        r.service.status_comments = r.status
        r.section[0] = "\n\nchanged meanwhile"
        r.clock.advance(200 * S)
        await r.service._reconcile_tick()
        [edit] = r.server.patches()
        assert "changed meanwhile" in edit
