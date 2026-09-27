from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from omnigent_factory import cli
from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Event
from omnigent_factory.core.types import Stage, Via
from omnigent_factory.credentials.broker import GateDecision
from omnigent_factory.github.adapter import GitHubAPIAdapter
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.omnigent.rest import OmnigentReadError
from omnigent_factory.service.app import create_app
from omnigent_factory.service.config import ServiceConfig, load_config
from omnigent_factory.service.credentials import StoreExecutionGate
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.directory import _new_boundary, _safe_publication, _untrusted
from omnigent_factory.service.github_delivery import GitHubDeliveryProcessor, GitHubWebhookVerifier
from omnigent_factory.service.observer import OmnigentObserver
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.setup import OperationsRenderer
from omnigent_factory.store.sqlite import DeliveryRecord
from omnigent_factory.testing.fakes import FakeClock, FakeCredentialBroker, FakeGitHub, FakeOmnigent
from omnigent_factory.testing.harness import Harness


def _identity() -> DeliveryIdentity:
    return DeliveryIdentity(
        app_id=900,
        installation_id=901,
        organization_id=296340858,
        project_node_id="PVT_kwDOEanNes4BkJhb",
        status_field_node_id="PVTSSF_lADOEanNes4BkJhbzhi7I9w",
        repository_id=1278047766,
        repository_node_id="R_kgDOTC12Fg",
        repository_full_name="SA-Ambulance/timesheets",
        owner_ids=frozenset({114979}),
        bot_user_id=777,
    )


def _project_delivery() -> bytes:
    return json.dumps(
        {
            "action": "edited",
            "installation": {"id": 901, "app_id": 900},
            "organization": {"id": 296340858},
            "sender": {"id": 114979},
            "projects_v2_item": {
                "node_id": "PVTI_1",
                "project_node_id": "PVT_kwDOEanNes4BkJhb",
                "content_node_id": "I_1",
            },
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.asyncio
async def test_unresolved_project_resolution_is_durably_bounded_across_restart(
    service_config: ServiceConfig,
) -> None:
    clock = FakeClock()
    config = service_config.model_copy(
        update={"delivery_resolution_backoff_seconds": 900.0, "delivery_resolution_max_attempts": 3}
    )
    calls = 0

    def github(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "__typename": "Issue",
                        "id": "I_1",
                        "number": 1,
                        "title": "issue",
                        "body": "body",
                        "state": "OPEN",
                        "repository": {
                            "id": "R_kgDOTC12Fg",
                            "databaseId": 1278047766,
                            "nameWithOwner": config.repository,
                        },
                        "assignees": {"nodes": []},
                        "projectItems": {
                            "nodes": [
                                {
                                    "id": "PVTI_1",
                                    "project": {"id": config.project_node_id},
                                    "fieldValueByName": None,
                                }
                            ],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        },
                    }
                }
            },
        )

    body = _project_delivery()
    record = DeliveryRecord(
        "stuck",
        "projects_v2_item",
        body,
        {},
        app_id=900,
        installation_id=901,
        source_time_us=clock.now_utc_us(),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as http:
        normalizer = DeliveryNormalizer(_identity())
        adapter = GitHubAPIAdapter(
            GitHubClient(http, "token"),
            repository=config.repository,
            repository_node_id=config.repo_id,
            project_node_id=config.project_node_id,
            status_field_node_id=config.status_field_node_id,
            bot_user_id=config.github_bot_user_id,
            required_checks=frozenset(),
        )
        service = FactoryService(config, clock=clock)
        await service.db.start()
        await service.db.call(lambda store: store.ensure_repository(config.trusted))
        await service.db.call(lambda store: store.append_delivery(record))
        processor = GitHubDeliveryProcessor(service, normalizer, adapter, clock)
        try:
            for second in range(3_601):
                for delivery in await service.db.call(lambda store: store.pending_deliveries()):
                    await processor.process(delivery)
                if second == 100:
                    await service.db.close()
                    service = FactoryService(config, clock=clock)
                    await service.db.start()
                    processor = GitHubDeliveryProcessor(service, normalizer, adapter, clock)
                clock.advance(1_000_000)
            status = await service.db.call(
                lambda store: store.query(
                    "SELECT status, resolution_attempts FROM deliveries WHERE delivery_guid = ?",
                    ("stuck",),
                )
            )
            parked = await service.db.call(lambda store: store.parked_delivery_rows())
        finally:
            await service.db.close()
    assert calls == 3
    # Exhaustion sets the delivery aside for the operator; it is never retired unapplied.
    assert tuple(status[0]) == ("rejected", 3)
    assert parked == (("stuck", None),)


def test_rendered_example_parses_with_blank_discoverable_ids(
    service_config: ServiceConfig, tmp_path: Path
) -> None:
    rendered = OperationsRenderer(tmp_path / "config.toml").render(service_config)
    path = tmp_path / "config.toml"
    path.write_text(rendered["config.example.toml"])
    parsed = load_config(path)
    assert parsed.omnigent_host_id is None
    assert parsed.omnigent_agent_id is None
    assert parsed.omnigent_project_id is None


def test_untrusted_frames_are_random_unspoofable_and_publication_is_safe(
    service_config: ServiceConfig,
) -> None:
    first, second = _new_boundary(), _new_boundary()
    assert first != second and len(first) >= 32
    assert first not in _untrusted(f"payload closes {first}", first)
    published = _safe_publication("hello @team " + "x" * 9_000, service_config)
    assert "@team" not in published
    assert "@\u200bteam" in published
    assert len(published) <= 8_000


@pytest.mark.asyncio
async def test_checkpoint_cleanup_token_gate_is_the_only_closed_gate_allowance(
    service_config: ServiceConfig,
) -> None:
    harness = Harness()
    harness.eligible("P")
    harness.send("P", ev.RequestTriage(via=Via.DRAG))
    session = harness.create_ok("P")
    harness.send(
        "P",
        ev.ActiveLimitReached(session_id=session.session_id, grant_id=session.grant.grant_id),
    )
    clock = FakeClock(start_us=harness.f("P").now)
    worker = StoreWorker(service_config.database_path, clock)
    await worker.start()
    try:
        await worker.call(lambda store: store.ensure_repository(service_config.trusted))
        for event, _ in harness.log:
            await worker.call(
                lambda store, event=event: store.apply_event(event, service_config.trusted)
            )
        decision = await StoreExecutionGate(worker, clock).token_gate(session.session_id)
    finally:
        await worker.close()
    assert decision == GateDecision(True, "checkpoint-cleanup", decision.profile)


class _ObserverService:
    def __init__(self, parcel: Any, config: ServiceConfig) -> None:
        self.parcel = parcel
        self.config = config
        self.events: list[Event] = []
        self.failures: list[str] = []

    async def _parcel_ids(self) -> list[str]:
        return [self.parcel.parcel_id]

    async def apply_event(self, event: Event, **_: object) -> SimpleNamespace:
        self.events.append(event)
        return SimpleNamespace()

    def managed_task_failed(self, name: str) -> None:
        self.failures.append(name)


class _ParcelDB:
    def __init__(self, service: _ObserverService) -> None:
        self.service = service

    async def call(self, operation: Any) -> Any:
        if getattr(operation, "func", None) is not None:  # functools.partial store probes
            return False  # e.g. "already applied?": nothing is in this fake store
        return self.service.parcel


class _BrokenObserverAdapter:
    class Rest:
        async def paginate(self, *_: object) -> list[object]:
            return []

    rest = Rest()

    async def observe_tree(self, root_id: str) -> None:
        del root_id
        raise OmnigentReadError("stale")


@pytest.mark.asyncio
async def test_observer_staleness_consumes_time_and_irrecoverable_failure_is_fatal(
    service_config: ServiceConfig,
) -> None:
    harness = Harness()
    harness.eligible("P")
    harness.send("P", ev.RequestTriage(via=Via.DRAG))
    harness.create_ok("P")
    parcel = harness.p("P")
    config = service_config.model_copy(
        update={"background_failure_limit": 2, "background_error_backoff_seconds": 0.001}
    )
    service = _ObserverService(parcel, config)
    service.db = _ParcelDB(service)  # type: ignore[attr-defined]
    clock = FakeClock()
    observer = OmnigentObserver(
        service,  # type: ignore[arg-type]
        _BrokenObserverAdapter(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        clock,
        interval_seconds=0.001,
    )
    await observer.observe_once()
    clock.advance(60_000_000)
    await observer.observe_once()
    samples = [
        event.body for event in service.events if isinstance(event.body, ev.ActiveTimeSample)
    ]
    assert samples[-1].consumed_us >= 60_000_000

    async def explode() -> frozenset[str]:
        raise AssertionError("corrupt observation")

    observer.observe_once = explode  # type: ignore[method-assign]
    await observer.start()
    for _ in range(100):
        if service.failures:
            break
        await asyncio.sleep(0.002)
    await observer.close()
    assert service.failures == ["observer"]


@pytest.mark.asyncio
async def test_invalid_checkpoint_result_is_recorded_as_checkpoint(
    service_config: ServiceConfig,
) -> None:
    harness = Harness()
    harness.eligible("P")
    harness.send("P", ev.RequestTriage(via=Via.DRAG))
    session = harness.create_ok("P")
    harness.send(
        "P",
        ev.ActiveLimitReached(session_id=session.session_id, grant_id=session.grant.grant_id),
    )
    parcel = harness.p("P")
    service = _ObserverService(parcel, service_config)
    service.db = _ParcelDB(service)  # type: ignore[attr-defined]

    class Rest:
        async def paginate(self, *_: object) -> list[object]:
            return [
                {
                    "id": "bad-result",
                    "status": "completed",
                    "data": {"role": "assistant", "content": "FACTORY_RESULT_V1\nnot-json"},
                }
            ]

    adapter = SimpleNamespace(rest=Rest())
    rejections: list[tuple[object, ...]] = []

    async def save_rejection(*args: object) -> None:
        rejections.append(args)

    observer = OmnigentObserver(
        service,  # type: ignore[arg-type]
        adapter,  # type: ignore[arg-type]
        SimpleNamespace(save_rejection=save_rejection),  # type: ignore[arg-type]
        FakeClock(),
        interval_seconds=1,
    )
    await observer._results(parcel)
    # The validation errors are persisted for the correction message and status comment.
    [(sid, item, stage, details)] = rejections
    assert (sid, item, stage) == (session.session_id, "bad-result", "triage")
    assert details == ("expected exactly one factory-result fence",)
    [candidate] = [
        event.body for event in service.events if isinstance(event.body, ev.ResultCandidate)
    ]
    assert candidate.valid is False
    assert candidate.result_kind == ev.ResultKind.CHECKPOINT


def test_cli_serve_builds_runs_and_stops_on_one_loop(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    class Lock:
        def close(self) -> None:
            pass

    class Service:
        def __init__(self) -> None:
            self.config = service_config
            self.ready = False
            self._tasks: list[object] = []
            self.process_lock = Lock()

        async def start(self) -> None:
            loops.append(asyncio.get_running_loop())
            self.ready = True

        async def stop(self) -> None:
            loops.append(asyncio.get_running_loop())
            self.ready = False

    service = Service()

    async def build(*_: object, **__: object) -> SimpleNamespace:
        loops.append(asyncio.get_running_loop())
        return SimpleNamespace(service=service, verifier=SimpleNamespace())

    class Server:
        def __init__(self, config: Any) -> None:
            self.config = config

        async def serve(self) -> None:
            loops.append(asyncio.get_running_loop())
            app = self.config.app
            async with app.router.lifespan_context(app):
                pass

    monkeypatch.setattr(cli, "_load", lambda _: (Path("config.toml"), service_config))
    monkeypatch.setattr(cli, "build_production", build)
    monkeypatch.setattr(cli.uvicorn, "Server", Server)
    assert cli.main(["serve"]) == 0
    assert len(loops) == 4
    assert len({id(loop) for loop in loops}) == 1


@pytest.mark.asyncio
async def test_signed_webhook_reaches_reducer_executor_and_starts_triage(
    service_config: ServiceConfig, tmp_path: Path
) -> None:
    secret = tmp_path / "webhook"
    secret.write_bytes(b"secret")
    secret.chmod(0o600)
    config = service_config.model_copy(update={"webhook_secret_file": secret})
    identity = _identity()
    normalizer = DeliveryNormalizer(identity)

    def github(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "node_id": "I_1",
                    "state": "open",
                    "assignees": [],
                    "title": "Start triage",
                    "body": "Please inspect this.",
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "projectItems": {
                            "nodes": [
                                {
                                    "id": "PVTI_1",
                                    "project": {"id": identity.project_node_id},
                                    "fieldValueByName": {
                                        "optionId": identity.status_option_ids[Stage.INBOX],
                                        "field": {"id": identity.status_field_node_id},
                                    },
                                }
                            ],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            },
        )

    github_http = httpx.AsyncClient(transport=httpx.MockTransport(github))
    reader = GitHubAPIAdapter(
        GitHubClient(github_http, "token"),
        repository=config.repository,
        repository_node_id=config.repo_id,
        project_node_id=config.project_node_id,
        status_field_node_id=config.status_field_node_id,
        bot_user_id=config.github_bot_user_id,
        required_checks=frozenset(),
    )
    gh_effects, omni, broker = FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()
    service = FactoryService(config, adapters=(gh_effects, omni, broker), clock=FakeClock())
    service.delivery_processor = GitHubDeliveryProcessor(service, normalizer, reader, service.clock)
    verifier = GitHubWebhookVerifier(secret, normalizer, service.clock)
    await service.start()
    try:
        await service.operator_command("unpause", {})
        payload = {
            "action": "created",
            "installation": {"id": 901, "app_id": 900},
            "organization": {"id": 296340858},
            "repository": {
                "id": 1278047766,
                "node_id": "R_kgDOTC12Fg",
                "full_name": config.repository,
            },
            "sender": {"id": 114979},
            "issue": {"id": 10, "node_id": "I_1", "number": 12},
            "comment": {
                "id": 44,
                "body": "/triage",
                "created_at": "2026-09-25T01:02:03Z",
            },
        }
        body = json.dumps(payload, separators=(",", ":")).encode()
        signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(service, verifier)),
            base_url="http://factory.test",
        ) as client:
            response = await client.post(
                "/webhooks/github",
                content=body,
                headers={
                    "x-github-delivery": "signed-1",
                    "x-github-event": "issue_comment",
                    "x-hub-signature-256": signature,
                },
            )
        assert response.status_code == 202
        for _ in range(200):
            if omni.executed(EffectKind.CREATE_SESSION):
                break
            await asyncio.sleep(0.01)
        assert omni.executed(EffectKind.CREATE_SESSION)
        parcel = await service.db.call(lambda store: store.load_parcel("I_1"))
        assert parcel is not None and parcel.current_session is not None
    finally:
        await service.stop()
        await github_http.aclose()
