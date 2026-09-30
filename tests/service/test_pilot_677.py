"""Regressions from the first live pilot run (SA-Ambulance/timesheets#677).

The triage session produced a valid result, but: the publication could not resolve the
issue (core effect args carry no GitHub IDs and the service never supplied them), the
card still went to Needs you, reconcile failed every cycle for the same reason, a
refused preparation was recorded as success so issuance was enabled without a
capability, and doctor could not read the live ``/v1/hosts`` shape.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
    RetryableReadFailure,
)
from omnigent_factory.core.types import BotState, Hold, Via
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.service.composition import owned_worktree_roots
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import (
    PublicationRenderer,
    ServiceDispatchDirectory,
    ServiceParcelResolver,
)
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, result_candidate, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)

PARCEL = "I_kwDOTC12Fs8AAAABS6JIGw"
ISSUE = 677
SESSION = "ss_pilot"
CTX = ExecutionContext("boot", 1, 1, 1)
TRIAGE = {
    "kind": "triage",
    "summary": "Route guard is untested; fix is test-only.",
    "priority": "P3",
    "size": "S",
    "recommendation": "fix",
    "duplicate_issue": None,
    "labels": ["area:api", "factory:build", "not-a-repo-label"],
    "missing_information": [],
}


async def eventually(predicate: Callable[[], Any], *, timeout: float = 3.0) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        if asyncio.get_running_loop().time() >= end:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.01)


def _write_result(config: ServiceConfig, session_id: str) -> None:
    results = config.state_dir / "results"
    results.mkdir(mode=0o700, exist_ok=True)
    payload = {"item_id": "item", "factory_result": {"result": TRIAGE}, "contract_canonical": None}
    (results / f"{session_id}-latest.json").write_text(json.dumps(payload))


class _GitHubServer:
    """Records requests; answers the REST/GraphQL shapes the adapter reads."""

    def __init__(self, config: ServiceConfig, *, size_option_set: str | None = None) -> None:
        self.config = config
        self.size_option_set = size_option_set
        self.requests: list[tuple[str, str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.requests.append((request.method, request.url.path, body))
        repo = f"/repos/{self.config.repository}"
        if request.method == "GET" and request.url.path == f"{repo}/issues/{ISSUE}/comments":
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path == f"{repo}/issues/{ISSUE}/comments":
            return httpx.Response(201, json={"id": 9001})
        if request.method == "GET" and request.url.path == f"{repo}/labels":
            return httpx.Response(200, json=[{"name": "area:api"}, {"name": "testing"}])
        if request.method == "POST" and request.url.path == f"{repo}/issues/{ISSUE}/labels":
            return httpx.Response(200, json=[])
        if request.method == "GET" and request.url.path == f"{repo}/issues/{ISSUE}":
            return httpx.Response(
                200,
                json={"node_id": PARCEL, "state": "open", "assignees": [], "title": "t"},
            )
        if request.url.path == "/graphql":
            return httpx.Response(200, json={"data": self._graphql(body)})
        return httpx.Response(404)

    def _graphql(self, body: dict[str, Any]) -> dict[str, Any]:
        query = body["query"]
        variables = body["variables"]
        if "updateProjectV2ItemFieldValue" in query:
            return {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "PVTI_1"}}}
        if "fields(first: 100)" in query:
            return {
                "node": {
                    "fields": {
                        "nodes": [
                            {
                                "id": "F_PRIORITY",
                                "name": "Priority",
                                "options": [{"id": "o_p3", "name": "P3"}],
                            },
                            {
                                "id": "F_SIZE",
                                "name": "Size",
                                "options": [{"id": "o_s", "name": "S"}],
                            },
                        ]
                    }
                }
            }
        if "fieldValueByName(name: $fieldName)" in query:
            if variables["fieldName"] == "Size" and self.size_option_set:
                value = {"optionId": self.size_option_set, "field": {"id": "F_SIZE"}}
                return {"node": {"fieldValueByName": value}}
            return {"node": {"fieldValueByName": None}}
        # projectItems (item lookup / status read)
        return {
            "node": {
                "projectItems": {
                    "nodes": [
                        {
                            "id": "PVTI_1",
                            "project": {"id": self.config.project_node_id},
                            "fieldValueByName": None,
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        }

    def posts(self, suffix: str) -> list[Any]:
        return [b for m, p, b in self.requests if m == "POST" and p.endswith(suffix)]


def _effect(kind: EffectKind, **args: Any) -> EffectIntent:
    return EffectIntent(
        effect_id=f"ef_{kind.value}",
        kind=kind,
        parcel_id=PARCEL,
        target=PARCEL,
        preconditions=Preconditions(parcel_version=1, eligibility_epoch=0, session_id=SESSION),
        args=args,
    )


async def _context_adapter(
    config: ServiceConfig, server: _GitHubServer
) -> tuple[GitHubAPIAdapter, FactoryService, httpx.AsyncClient]:
    service = FactoryService(config, clock=FakeClock())
    await service.start()
    factory = EventFactory(PARCEL, issue_number=ISSUE)
    await service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    _write_result(config, SESSION)
    renderer = PublicationRenderer(ServiceDispatchDirectory(service.db, config), config)
    http = httpx.AsyncClient(transport=httpx.MockTransport(server))
    adapter = GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository=config.repository,
        repository_node_id=config.repo_id,
        project_node_id=config.project_node_id,
        status_field_node_id=config.status_field_node_id,
        bot_user_id=config.github_bot_user_id,
        required_checks=frozenset(),
        publication_renderer=renderer,
        parcel_resolver=ServiceParcelResolver(service.db),
        triage_fields=renderer.triage_fields,
    )
    return adapter, service, http


# --------------------------------------------------------------- bugs 1 and 2


@pytest.mark.asyncio
async def test_publish_triage_gets_issue_and_body_from_service_and_applies_fields(
    service_config: ServiceConfig,
):
    server = _GitHubServer(service_config, size_option_set="o_owner_choice")
    adapter, service, http = await _context_adapter(service_config, server)
    try:
        # Exactly the live effect: args carry only the session, no issue or body.
        outcome = await adapter.execute(_effect(EffectKind.PUBLISH_TRIAGE, session_id=SESSION), CTX)
    finally:
        await http.aclose()
        await service.stop()
    assert isinstance(outcome, Ack), outcome
    [comment] = server.posts(f"/issues/{ISSUE}/comments")
    text = comment["body"]
    assert TRIAGE["summary"] in text and "P3" in text and "fix" in text
    assert "`area:api`" in text and "omnigent-factory effect=ef_publish_triage" in text
    # Priority was unset -> filled; Size was set by the owner -> kept.
    mutations = [
        b["variables"]["input"]
        for b in server.posts("/graphql")
        if "updateProjectV2ItemFieldValue" in b["query"]
    ]
    assert mutations == [
        {
            "projectId": service_config.project_node_id,
            "itemId": "PVTI_1",
            "fieldId": "F_PRIORITY",
            "value": {"singleSelectOptionId": "o_p3"},
        }
    ]
    assert outcome.detail["fields"] == {"Priority": "P3", "Size": "kept"}
    # Only existing, non-control labels are added.
    assert server.posts(f"/issues/{ISSUE}/labels") == [{"labels": ["area:api"]}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "args"),
    [
        (EffectKind.POST_COMMENT, {"template": "result-invalid"}),
        (EffectKind.PUBLISH_REPORT, {"report": "ready", "pr_number": 5, "head_sha": "a" * 40}),
        (EffectKind.PUBLISH_REPORT, {"report": "checkpoint", "session_id": SESSION}),
        (EffectKind.RECONCILE_PARCEL, {}),
    ],
)
async def test_every_issue_bound_effect_resolves_the_parcel_issue(
    service_config: ServiceConfig, kind: EffectKind, args: dict[str, Any]
):
    server = _GitHubServer(service_config)
    adapter, service, http = await _context_adapter(service_config, server)
    try:
        outcome = await adapter.execute(_effect(kind, **args), CTX)
    finally:
        await http.aclose()
        await service.stop()
    assert isinstance(outcome, Ack), outcome
    paths = {path for _, path, _ in server.requests}
    assert any(f"/issues/{ISSUE}" in path for path in paths)


# --------------------------------------------------------------- bugs 3 and 7


async def _triage_result_published(
    config: ServiceConfig, github: FakeGitHub, clock: FakeClock | None = None
) -> tuple[FactoryService, str]:
    omnigent = FakeOmnigent()
    quiet = Ack("root", {"complete": True, "busy": False, "pending_waiter": False})
    omnigent.script(EffectKind.SCAN_TREE, *([quiet] * 5))
    service = FactoryService(
        config,
        adapters=(github, omnigent, FakeCredentialBroker()),
        clock=clock or FakeClock(),
    )
    await service.start()
    await service.operator_command("unpause", {})
    factory = EventFactory(PARCEL, issue_number=ISSUE)
    await service.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
    )
    await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))

    async def sent() -> bool:
        parcel = await service.db.call(lambda store: store.load_parcel(PARCEL))
        s = parcel.current_session if parcel else None
        return bool(s and s.own_items)

    await eventually(sent)
    parcel = await service.db.call(lambda store: store.load_parcel(PARCEL))
    s = parcel.current_session
    await service.apply_event(
        factory.make(
            result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE),
        )
    )
    [publish] = [
        row
        for row in await service.db.call(
            lambda store: store.query(
                "SELECT effect_id FROM effects WHERE kind = ?", ("publish_triage",)
            )
        )
    ]
    return service, str(publish[0])


async def _state(service: FactoryService, effect_id: str) -> tuple[str, Any]:
    stored = await service.db.call(lambda store: store.get_effect(effect_id))
    parcel = await service.db.call(lambda store: store.load_parcel(PARCEL))
    return stored.state, parcel


@pytest.mark.asyncio
async def test_transient_publication_failure_retries_then_idle(service_config):
    github = FakeGitHub()
    github.script(
        EffectKind.PUBLISH_TRIAGE, RetryableReadFailure("rate limited", 1), Ack("comment-1")
    )
    clock = FakeClock()
    service, effect_id = await _triage_result_published(service_config, github, clock)
    try:

        async def done() -> bool:
            clock.advance(1_000_000)  # let the scheduled retry come due
            return (await _state(service, effect_id))[0] == "done"

        await eventually(done)
        _, parcel = await _state(service, effect_id)
        assert len(github.executed(EffectKind.PUBLISH_TRIAGE)) == 2
        assert parcel.bot == BotState.IDLE and Hold.AWAITING_OWNER in parcel.holds
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_definitive_publication_failure_is_blocked_and_operator_retry_recovers(
    service_config,
):
    github = FakeGitHub()
    github.script(EffectKind.PUBLISH_TRIAGE, DefinitiveFailure("could not resolve issue"))
    service, effect_id = await _triage_result_published(service_config, github)
    try:

        async def failed() -> bool:
            return (await _state(service, effect_id))[0] == "failed"

        await eventually(failed)
        _, parcel = await _state(service, effect_id)
        assert parcel.bot == BotState.BLOCKED
        assert Hold.PUBLICATION_FAILED in parcel.holds

        # Recovery (bug 7): requeue the same effect; no new session is started and the
        # adapter adopts by effect marker, so the retry cannot duplicate the comment.
        sessions_before = len(parcel.sessions)
        reply = await service.operator_command("retry-effect", {"effect": effect_id})
        assert reply["requeued"] == effect_id

        async def done() -> bool:
            return (await _state(service, effect_id))[0] == "done"

        await eventually(done)
        _, parcel = await _state(service, effect_id)
        assert parcel.bot == BotState.IDLE
        assert Hold.PUBLICATION_FAILED not in parcel.holds
        assert len(parcel.sessions) == sessions_before
        assert [e.effect_id for e in github.executed(EffectKind.PUBLISH_TRIAGE)] == [
            effect_id,
            effect_id,
        ]
        with pytest.raises(ValueError, match="not a failed"):
            await service.operator_command("retry-effect", {"effect": effect_id})
    finally:
        await service.stop()


# --------------------------------------------------------------- bug 4


@pytest.mark.asyncio
async def test_refused_preparation_is_not_recorded_as_success(service_config):
    omnigent = FakeOmnigent()
    omnigent.script(
        EffectKind.PREPARE_SESSION,
        Ack("root", {"ok": False, "unexpected_turn": False, "reason": "workspace: outside"}),
    )
    credentials = FakeCredentialBroker()
    service = FactoryService(
        service_config,
        adapters=(FakeGitHub(), omnigent, credentials),
        clock=FakeClock(),
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        factory = EventFactory(PARCEL, issue_number=ISSUE)
        await service.apply_event(
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
        )
        await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))

        async def held() -> bool:
            parcel = await service.db.call(lambda store: store.load_parcel(PARCEL))
            return parcel is not None and Hold.PREPARE_FAILED in parcel.holds

        await eventually(held)
        parcel = await service.db.call(lambda store: store.load_parcel(PARCEL))
        assert not parcel.current_session.prepared
        assert parcel.bot == BotState.BLOCKED
        assert not omnigent.executed(EffectKind.SEND_MESSAGE)
        assert not credentials.executed(EffectKind.ENABLE_ISSUANCE)
    finally:
        await service.stop()


def test_omnigent_created_worktrees_are_inside_owned_roots(service_config, tmp_path: Path):
    """Omnigent creates ``<clone>-worktrees/<branch>``; prepare rejected it as foreign."""
    config = service_config.model_copy(update={"source_clone": tmp_path / "clones" / "timesheets"})
    roots = owned_worktree_roots(config)
    assert tmp_path / "clones" / "timesheets-worktrees" in roots
    assert config.worktree_root in roots


# --------------------------------------------------------------- bug 5


def test_webhook_log_summary_carries_identifiers_not_content():
    from omnigent_factory.service.app import delivery_summary
    from omnigent_factory.store.sqlite import DeliveryRecord

    body = json.dumps(
        {
            "action": "created",
            "sender": {"login": "owner", "id": 1},
            "issue": {"number": ISSUE},
            "comment": {"body": "token=ghs_secretvalue please triage"},
        }
    ).encode()
    line = delivery_summary(
        DeliveryRecord("guid-1", "issue_comment", body, {"x-hub-signature-256": "sha256=abc"})
    )
    assert line == ("delivery=guid-1 event=issue_comment action=created sender=owner issue=#677")
