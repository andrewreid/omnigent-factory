"""Factory MCP tools: caller resolution, stage gating, validation, idempotency, restart.

Tool semantics run against a real :class:`FactoryService` (SQLite store, reducer, outbox)
with fake adapters; the HTTP tests drive the mounted ``/mcp`` endpoint through the ASGI app
with JSON-RPC, as Omnigent's MCP client does.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import (
    DecisionSource,
    Hold,
    Lifecycle,
    Parcel,
    SessionKind,
    Via,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.mcp import FactoryToolError, FactoryTools
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, contract, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_pilot_677 import eventually

pytestmark = pytest.mark.asyncio

P = "I_mcp_parcel"
SHA = "a" * 40
TRIAGE = {
    "summary": "Guard is untested.",
    "priority": "P3",
    "size": "S",
    "recommendation": "fix",
    "duplicate_issue": None,
    "labels": [],
    "missing_information": [],
}
PLAN = {"approach": "Add the test.", "risks": [], "contract": contract("S", "Test the guard")}


def build_ready(head: str = SHA) -> dict[str, Any]:
    return {
        "pr_number": 7,
        "head_sha": head,
        "summary": "done",
        "verification": [
            {"command": "pytest", "file_set": ["tests"], "outcome": "passed", "evidence": "ok"}
        ],
        "review": {
            "implementation_vendor": "anthropic",
            "review_vendor": "openai",
            "reviewed_head": head,
            "artifact_reference": "review.md",
            "artifact_sha256": "c" * 64,
            "accepted": True,
        },
        "findings": [],
        "remediation_batches_used": 0,
        "targeted_rechecks_used": 0,
        "release_readiness": "ready",
    }


@dataclass
class Rig:
    service: FactoryService
    tools: FactoryTools
    github: FakeGitHub
    omnigent: FakeOmnigent
    factory: EventFactory

    async def parcel(self) -> Parcel:
        parcel = await self.service.db.call(lambda store: store.load_parcel(P))
        assert parcel is not None
        return parcel

    async def send(self, body: ev.EventBody, **kw: Any) -> Any:
        return await self.service.apply_event(self.factory.make(body, **kw))

    async def run(self) -> Any:
        return (await self.parcel()).current_session

    async def run_id(self, root: str) -> str:
        """The caller's current run, as the start pointer names it."""
        run = await self.run()
        return run.session_id if run is not None and run.root_id == root else "ss_none"

    async def submit(
        self,
        root: str,
        kind: str,
        result: dict[str, Any],
        *,
        run_id: str | None = None,
        plan_hash: str | None = None,
    ) -> dict[str, Any]:
        return await self.tools.submit_result(
            root, kind, result, run_id=run_id or await self.run_id(root), plan_hash=plan_hash
        )

    async def ask(self, root: str, question: str, **kw: Any) -> dict[str, Any]:
        return await self.tools.ask_owner(root, await self.run_id(root), question, **kw)

    async def root(self) -> str:
        run = await self.run()
        assert run is not None and run.root_id is not None
        return run.root_id

    async def active(self, kind: SessionKind) -> str:
        async def ready() -> bool:
            run = await self.run()
            return bool(run and run.kind == kind and run.lifecycle == Lifecycle.ACTIVE)

        await eventually(ready)
        return await self.root()

    async def quiesce(self) -> None:
        run = await self.run()
        await self.service.apply_event(
            Event(
                event_id=f"test-quiet:{run.session_id}:{self.factory.tick()}",
                repo_id=self.service.config.repo_id,
                parcel_id=P,
                source_time_us=self.factory.now,
                provenance=Provenance.ADAPTER,
                body=ev.TreeQuiescent(session_id=run.session_id, complete=True, busy=False),
            )
        )

    def executed(self, kind: EffectKind) -> list[Any]:
        adapter = self.omnigent if kind in self.omnigent.kinds else self.github
        return adapter.executed(kind)


@asynccontextmanager
async def started(config: ServiceConfig, rig: Rig | None = None) -> AsyncIterator[Rig]:
    github = rig.github if rig is not None else FakeGitHub()
    omnigent = rig.omnigent if rig is not None else FakeOmnigent()
    service = FactoryService(
        config, adapters=(github, omnigent, FakeCredentialBroker()), clock=FakeClock()
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        tools = FactoryTools(service, ServiceDispatchDirectory(service.db, config), config)
        factory = rig.factory if rig is not None else EventFactory(P, issue_number=42)
        yield Rig(service, tools, github, omnigent, factory)
    finally:
        await service.stop()


async def triaged(rig: Rig) -> str:
    await rig.send(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=rig.factory.now))
    await rig.send(ev.RequestTriage(via=Via.DRAG))
    return await rig.active(SessionKind.TRIAGE)


async def planning(rig: Rig) -> str:
    root = await triaged(rig)
    await rig.submit(root, "triage", TRIAGE)
    await rig.quiesce()
    await rig.send(ev.RequestPlan(via=Via.DRAG))
    return await rig.active(SessionKind.PLAN)


async def building(rig: Rig) -> tuple[str, str]:
    root = await planning(rig)
    rig.github.script(
        EffectKind.PUBLISH_CONTRACT,
        Ack(remote_id="c-1", detail={"verified": True, "posted_at_us": 1}),
    )
    receipt = await rig.submit(root, "plan", PLAN)

    async def published() -> bool:
        return (await rig.parcel()).current_contract is not None

    await eventually(published)
    await rig.send(ev.ApprovePlan(via=Via.DRAG))
    await rig.quiesce()
    return await rig.active(SessionKind.BUILD), receipt["plan_hash"]


# ------------------------------------------------------------------ resolution


@pytest.mark.parametrize("bad", [None, "", 42, "has space", "x" * 200, ["conv"]])
async def test_session_id_must_be_a_well_formed_id(service_config: ServiceConfig, bad: Any):
    async with started(service_config) as rig:
        with pytest.raises(FactoryToolError, match="session_id: required"):
            await rig.tools.get_status(bad)


async def test_unknown_and_child_session_ids_are_refused(service_config: ServiceConfig):
    async with started(service_config) as rig:
        await triaged(rig)
        with pytest.raises(FactoryToolError, match="worker/child sessions cannot"):
            await rig.tools.get_status("conv_child_of_root")
        with pytest.raises(FactoryToolError, match="not a factory issue session"):
            await rig.submit("conv_unknown", "triage", TRIAGE)


async def test_issue_and_stage_come_from_the_store_not_arguments(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await triaged(rig)
        status = await rig.tools.get_status(root)
        run = await rig.run()
        assert status["run_id"] == run.session_id and status["stage"] == "triage"
        assert status["issue_number"] == 42 and status["work_allowed"] is True
        issue = await rig.tools.get_issue(root)
        boundary = issue["issue"]["untrusted_boundary"]
        assert issue["issue"]["text"].startswith(f"BEGIN UNTRUSTED ISSUE {boundary}\nTitle: ")
        assert issue["issue"]["text"].endswith(f"END UNTRUSTED ISSUE {boundary}")
        assert len(boundary) >= 32  # unguessable per call/run
        # A different parcel's issue session cannot be named to read this issue's data.
        with pytest.raises(FactoryToolError):
            await rig.tools.get_issue("conv_other_parcel_root")


async def test_superseded_issue_session_is_refused(service_config: ServiceConfig):
    async with started(service_config) as rig:
        old_root = await triaged(rig)
        rig.omnigent.script(
            EffectKind.PREPARE_SESSION,
            Ack(detail={"ok": False, "unusable": True, "reason": "issue session gone"}),
        )
        await rig.submit(old_root, "triage", TRIAGE)
        await rig.quiesce()
        await rig.send(ev.RequestPlan(via=Via.DRAG))
        new_root = await rig.active(SessionKind.PLAN)
        assert new_root != old_root
        with pytest.raises(FactoryToolError, match="superseded"):
            await rig.tools.get_status(old_root)
        assert (await rig.tools.get_status(new_root))["stage"] == "plan"


# ------------------------------------------------------------------ submit_result


async def test_triage_submission_is_validated_in_turn_and_idempotent(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root = await triaged(rig)
        with pytest.raises(FactoryToolError) as error:
            await rig.submit(root, "triage", {**TRIAGE, "priority": "urgent"})
        assert "priority" in str(error.value) and "invalid result" in str(error.value)
        assert '"priority":"P0|P1|P2|P3"' in str(error.value)  # the expected shape is quoted
        with pytest.raises(FactoryToolError, match="not allowed for this triage run"):
            await rig.submit(root, "plan", PLAN)
        first = await rig.submit(root, "triage", TRIAGE)
        assert first["accepted"] is True and first["kind"] == "triage"
        again = await rig.submit(root, "triage", TRIAGE)
        assert again == {**first, "replayed": True}
        with pytest.raises(FactoryToolError, match="different submission was already accepted"):
            await rig.submit(root, "triage", {**TRIAGE, "summary": "other"})

        async def published() -> bool:
            return bool(rig.executed(EffectKind.PUBLISH_TRIAGE))

        await eventually(published)
        assert len(rig.executed(EffectKind.PUBLISH_TRIAGE)) == 1
        results = await rig.service.db.call(
            lambda store: store.query("SELECT COUNT(*) FROM events WHERE kind='ResultCandidate'")
        )
        assert results[0][0] == 1


async def test_run_id_is_required_and_checked_against_the_current_run(
    service_config: ServiceConfig,
):
    """F3: one issue session spans runs, so a delayed call from an earlier run must never
    be attributed to its successor; only an exact replay of what it already holds."""
    async with started(service_config) as rig:
        root = await triaged(rig)
        triage_run = (await rig.run()).session_id
        for missing in ("", None):
            with pytest.raises(FactoryToolError, match="run_id: required"):
                await rig.tools.submit_result(root, "triage", TRIAGE, run_id=missing)  # type: ignore[arg-type]
            with pytest.raises(FactoryToolError, match="run_id: required"):
                await rig.tools.ask_owner(root, missing, "Proceed?")  # type: ignore[arg-type]
        with pytest.raises(FactoryToolError, match="not a run of this session"):
            await rig.submit(root, "triage", TRIAGE, run_id="ss_other")
        accepted = await rig.submit(root, "triage", TRIAGE)
        await rig.quiesce()
        await rig.send(ev.RequestPlan(via=Via.DRAG))
        assert await rig.active(SessionKind.PLAN) == root
        plan_run = (await rig.run()).session_id
        assert plan_run != triage_run
        # The earlier run replays its own accepted receipt ...
        replay = await rig.tools.submit_result(root, "triage", TRIAGE, run_id=triage_run)
        assert replay == {**accepted, "replayed": True}
        # ... but cannot submit or ask anything new against the successor run.
        blocked = {"reason": "late", "done": []}
        with pytest.raises(FactoryToolError, match="is not the current run"):
            await rig.tools.submit_result(root, "blocked", blocked, run_id=triage_run)
        with pytest.raises(FactoryToolError, match="is not the current run"):
            await rig.tools.ask_owner(root, triage_run, "Proceed?")
        parcel = await rig.parcel()
        assert not parcel.decisions and Hold.AGENT_BLOCKED not in parcel.holds
        assert (await rig.submit(root, "blocked", blocked))["run_id"] == plan_run


async def test_plan_result_returns_its_hash_and_revision(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        with pytest.raises(FactoryToolError, match="not allowed for this plan run"):
            await rig.submit(root, "build_ready", build_ready(), plan_hash="x")
        receipt = await rig.submit(root, "plan", PLAN)
        parcel = await rig.parcel()
        assert receipt["plan_hash"] == parcel.contracts[-1].full_hash
        assert receipt["revision"] == parcel.revision
        with pytest.raises(FactoryToolError, match="different submission"):
            await rig.submit(root, "plan", {**PLAN, "approach": "other"})


async def test_build_ready_must_echo_the_plan_hash_it_fetched(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root, plan_hash = await building(rig)
        with pytest.raises(FactoryToolError, match="plan_hash: required"):
            await rig.submit(root, "build_ready", build_ready())
        with pytest.raises(FactoryToolError, match="call factory_get_plan in this run first"):
            await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        plan = await rig.tools.get_plan(root)
        assert plan["plan_hash"] == plan_hash and plan["approved"]["contract"]["goal"]
        with pytest.raises(FactoryToolError, match="does not match the current approved plan"):
            await rig.submit(root, "build_ready", build_ready(), plan_hash="0" * 64)
        receipt = await rig.submit(root, "build_ready", build_ready(), plan_hash=plan_hash)
        assert receipt["accepted"] is True and receipt["slot"] == f"build-{SHA}"
        parcel = await rig.parcel()
        assert parcel.readiness is not None and parcel.readiness.head_sha == SHA
        # The same issue session carried triage, plan and build.
        roots = {s.root_id for s in parcel.sessions}
        assert roots == {root} and [s.kind for s in parcel.sessions] == [
            SessionKind.TRIAGE,
            SessionKind.PLAN,
            SessionKind.BUILD,
        ]
        assert len(rig.executed(EffectKind.CREATE_SESSION)) == 1


async def test_submission_after_stop_is_rejected_but_exact_retry_replays(
    service_config: ServiceConfig,
):
    async with started(service_config) as rig:
        root = await planning(rig)
        before = await rig.submit(root, "blocked", {"reason": "no DB", "done": []})
        await rig.send(ev.Stop())
        with pytest.raises(FactoryToolError, match="this run is closed"):
            await rig.submit(root, "plan", PLAN)
        with pytest.raises(FactoryToolError, match="this run is closed"):
            await rig.ask(root, "Proceed?")
        replay = await rig.submit(root, "blocked", {"reason": "no DB", "done": []})
        assert replay == {**before, "replayed": True}
        status = await rig.tools.get_status(root)
        assert status["stopped"] is True and status["work_allowed"] is False


async def test_stop_then_replan_reuses_the_same_issue_session(service_config: ServiceConfig):
    """Regression for the old deadlock: a stop fences the run, never the conversation."""
    async with started(service_config) as rig:
        root = await planning(rig)
        stopped = await rig.run()
        await rig.send(ev.Stop())
        await rig.quiesce()
        await rig.send(ev.RequestPlan(via=Via.COMMAND))
        assert await rig.active(SessionKind.PLAN) == root
        run = await rig.run()
        assert run.session_id != stopped.session_id
        parcel = await rig.parcel()
        assert parcel.session(stopped.session_id).fences  # the stopped run stays fenced
        receipt = await rig.submit(root, "plan", PLAN)
        assert receipt["accepted"] is True and receipt["run_id"] == run.session_id


async def test_checkpoint_accepts_only_blocked_as_the_wrap_up(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await triaged(rig)
        run = await rig.run()
        await rig.service.apply_event(
            Event(
                event_id="limit-1",
                repo_id=service_config.repo_id,
                parcel_id=P,
                source_time_us=rig.factory.tick(),
                provenance=Provenance.SCHEDULER,
                body=ev.ActiveLimitReached(session_id=run.session_id, grant_id=run.grant.grant_id),
            )
        )
        with pytest.raises(FactoryToolError, match="at a checkpoint"):
            await rig.submit(root, "triage", TRIAGE)
        receipt = await rig.submit(
            root, "blocked", {"reason": "needs a DB", "done": ["read the code"]}
        )
        assert receipt["slot"] == f"checkpoint-{run.grant.grant_id}"
        assert (await rig.run()).lifecycle == Lifecycle.CHECKPOINT_WAIT


# ------------------------------------------------------------------ ask_owner


async def test_ask_owner_posts_once_and_answer_flows_back(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        first = await rig.ask(
            root, "Keep the v1 API?", options=["keep", "drop"], recommendation="keep"
        )
        # F4: an equivalent question (normalized text + options) is the same open question,
        # whatever wording/recommendation the retry carries: original receipt, no comment.
        for retry in (
            {"question": "Keep the  v1 API?", "recommendation": "keep"},
            {"question": "keep the v1 api?", "recommendation": "drop it", "impact": "unknown"},
        ):
            again = await rig.ask(root, retry.pop("question"), options=["keep", "drop"], **retry)
            assert again == {**first, "replayed": True}
        parcel = await rig.parcel()
        [decision] = parcel.open_decisions
        assert decision.decision_id == first["decision_id"]
        assert decision.source == DecisionSource.MCP and parcel.bot.value == "Needs you"
        assert first["link"].endswith(f"/c/{root}")

        async def posted() -> bool:
            return bool(
                [
                    e
                    for e in rig.executed(EffectKind.POST_COMMENT)
                    if e.args.get("template") == "decision"
                ]
            )

        await eventually(posted)
        comments = [
            e for e in rig.executed(EffectKind.POST_COMMENT) if e.args.get("template") == "decision"
        ]
        assert len(comments) == 1 and "Keep the v1 API?" in str(comments[0].args["summary"])
        from omnigent_factory.service.directory import PublicationRenderer

        text = await PublicationRenderer(rig.tools.directory, service_config)(comments[0])
        assert text is not None
        assert text.startswith("Keep the v1 API?\n\n- keep\n- drop")
        assert "/c/" not in text and decision.decision_id not in text and ">" not in text
        assert text.endswith("\n\nI recommend: keep")
        await rig.send(ev.Decide(decision_id=decision.decision_id, answer="keep"))

        async def relayed() -> bool:
            return any(
                e.args.get("purpose") == "answer_relay"
                for e in rig.executed(EffectKind.SEND_MESSAGE)
            )

        await eventually(relayed)
        feedback = await rig.tools.get_feedback(root)
        [answer] = [d for d in feedback["decisions"] if d["decision_id"] == decision.decision_id]
        assert "keep" in answer["answer"] and answer["status"] == "relayed"


# ------------------------------------------------------------------ restart


async def test_restart_between_calls_never_duplicates(service_config: ServiceConfig):
    """A retry after a daemon restart (lost response) returns the stored receipt: no
    second event, decision, comment or Omnigent session."""

    async def counts(rig: Rig) -> dict[str, int]:
        rows = await rig.service.db.call(
            lambda store: store.query(
                "SELECT kind, COUNT(*) FROM events WHERE provenance='mcp' GROUP BY kind"
            )
        )
        comments = await rig.service.db.call(
            lambda store: store.query(
                "SELECT COUNT(*) FROM effects WHERE kind = 'post_comment' "
                "AND payload_json LIKE '%\"decision\"%'"
            )
        )
        return {**{str(r[0]): int(r[1]) for r in rows}, "decision_comments": comments[0][0]}

    async with started(service_config) as rig:
        root = await planning(rig)
        asked = await rig.ask(root, "Which DB?")
        submitted = await rig.submit(root, "blocked", {"reason": "x", "done": []})
        before = await counts(rig)
    assert before["OwnerQuestion"] == 1 and before["decision_comments"] == 1
    async with started(service_config, rig) as again:
        assert await again.ask(root, "Which DB?") == {**asked, "replayed": True}
        replay = await again.submit(root, "blocked", {"reason": "x", "done": []})
        assert replay == {**submitted, "replayed": True}
        assert await counts(again) == before
        parcel = await again.parcel()
        assert len(parcel.decisions) == 1
        # Exactly one Omnigent session was ever created for this issue.
        assert {s.root_id for s in parcel.sessions} == {root}
        assert len(again.executed(EffectKind.CREATE_SESSION)) == 1


# ------------------------------------------------------------------ title and handoff


async def test_issue_session_title_and_replacement_start_pointer(service_config: ServiceConfig):
    from omnigent_factory.core.effects import EffectIntent, Preconditions

    async with started(service_config) as rig:
        root = await triaged(rig)
        run = await rig.run()
        spec = await rig.tools.directory.stage_spec(run.session_id)
        assert spec is not None and spec.title == "#42 · Add export button"
        rig.omnigent.script(
            EffectKind.PREPARE_SESSION,
            Ack(detail={"ok": False, "unusable": True, "reason": "context rollover"}),
        )
        await rig.submit(root, "triage", TRIAGE)
        await rig.quiesce()
        await rig.send(ev.RequestPlan(via=Via.DRAG))
        new_root = await rig.active(SessionKind.PLAN)
        plan = await rig.run()
        assert new_root != root and (await rig.parcel()).issue_session.generation == 2
        text = await rig.tools.directory.message_text(
            EffectIntent(
                effect_id="ef_first",
                kind=EffectKind.SEND_MESSAGE,
                parcel_id=P,
                target=plan.session_id,
                preconditions=Preconditions(1, 0, session_id=plan.session_id),
                args={"purpose": "first"},
            )
        )
        assert text is not None
        assert f"Your Omnigent session id is {new_root}" in text
        assert "Start with factory_get_issue" in text
        assert "replacing an earlier one" in text
        assert "triage: fix, priority P3, size S" in text
        assert "FACTORY_RESULT" not in text


async def test_same_question_can_be_asked_again_once_answered(service_config: ServiceConfig):
    async with started(service_config) as rig:
        root = await planning(rig)
        first = await rig.ask(root, "Which DB?", options=["sqlite", "postgres"])
        other = await rig.ask(root, "Which DB?", options=["sqlite"])  # a distinct question
        assert other["decision_id"] != first["decision_id"]
        await rig.send(ev.Decide(decision_id=first["decision_id"], answer="sqlite"))
        again = await rig.ask(root, "Which DB?", options=["sqlite", "postgres"])
        assert again.get("replayed") is not True
        assert again["decision_id"] not in (first["decision_id"], other["decision_id"])
