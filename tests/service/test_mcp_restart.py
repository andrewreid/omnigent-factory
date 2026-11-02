"""One HTTP client keeps calling tools after the daemon and MCP manager are replaced."""

from __future__ import annotations

import httpx
import pytest
from starlette.types import Receive, Scope, Send

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import Lifecycle, Via
from omnigent_factory.service.app import create_app
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.mcp import build_endpoint
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from tests.service.test_mcp_http import HEADERS, _rpc, _token
from tests.service.test_pilot_677 import eventually


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_stateful", [False, True], ids=["stateless", "legacy-session"])
async def test_mcp_client_survives_daemon_restart(
    service_config: ServiceConfig, legacy_stateful: bool
) -> None:
    token = _token(service_config)
    adapters = (FakeGitHub(), FakeOmnigent(), FakeCredentialBroker())

    def server():
        service = FactoryService(service_config, adapters=adapters, clock=FakeClock())
        endpoint = build_endpoint(
            service, ServiceDispatchDirectory(service.db, service_config), service_config
        )
        app = create_app(service, object(), endpoint)  # type: ignore[arg-type]
        return app, service, endpoint

    app, service, endpoint = server()
    if legacy_stateful:
        # Model upgrading a stateful daemon: A actually issues the ID that B must accept.
        endpoint.manager.stateless = False

    async def route(scope: Scope, receive: Receive, send: Send) -> None:
        await app(scope, receive, send)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=route),
        base_url="http://127.0.0.1:8787",
        headers={**HEADERS, "authorization": f"Bearer {token}"},
    ) as client:
        async with app.router.lifespan_context(app):
            await eventually(lambda: service.ready)
            response = await client.post(
                "/mcp/",
                json=_rpc(
                    "initialize",
                    {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "restart-test", "version": "1"},
                    },
                ),
            )
            assert response.status_code == 200, response.text
            client.headers["mcp-protocol-version"] = response.json()["result"]["protocolVersion"]
            if legacy_stateful:
                client.headers["mcp-session-id"] = response.headers["mcp-session-id"]
            else:
                assert "mcp-session-id" not in response.headers
                client.headers["mcp-session-id"] = "stale-session-from-an-earlier-process"
            initialized = await client.post(
                "/mcp/", json={"jsonrpc": "2.0", "method": "notifications/initialized"}
            )
            assert initialized.status_code == 202
            factory = EventFactory("I_mcp_restart", issue_number=42)
            await service.operator_command("unpause", {})
            await service.apply_event(
                factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now))
            )
            await service.apply_event(factory.make(ev.RequestTriage(via=Via.DRAG)))

            async def active() -> bool:
                parcel = await service.db.call(lambda store: store.load_parcel(factory.parcel_id))
                run = parcel.current_session if parcel is not None else None
                return run is not None and run.lifecycle == Lifecycle.ACTIVE

            await eventually(active)
            parcel = await service.db.call(lambda store: store.load_parcel(factory.parcel_id))
            assert parcel is not None and parcel.current_session is not None
            root = parcel.current_session.root_id
            call = _rpc(
                "tools/call", {"name": "factory_get_status", "arguments": {"session_id": root}}
            )
            before = await client.post("/mcp/", json=call)
            assert before.status_code == 200, before.text
            assert before.json()["result"]["isError"] is False
            headers = dict(client.headers)

        # New service/store connection, FastMCP server and manager; same database and client.
        app, service, replacement = server()
        assert replacement.manager is not endpoint.manager
        async with app.router.lifespan_context(app):
            await eventually(lambda: service.ready)
            after = await client.post("/mcp/", json=call)
            assert after.status_code == 200, after.text
            result = after.json()["result"]
            assert result["isError"] is False
            assert result["structuredContent"]["session_id"] == root
            assert (
                result["structuredContent"]["run_id"]
                == before.json()["result"]["structuredContent"]["run_id"]
            )
            assert "mcp-session-id" not in after.headers
            assert dict(client.headers) == headers  # no client reset or reinitialization

            for authorization in (None, "Bearer wrong-token"):
                request = client.build_request("POST", "/mcp/", json=call)
                del request.headers["authorization"]
                if authorization is not None:
                    request.headers["authorization"] = authorization
                denied = await client.send(request)
                assert denied.status_code == 401
            for arguments in ({}, {"session_id": "unknown-child"}):
                denied = await client.post(
                    "/mcp/",
                    json=_rpc("tools/call", {"name": "factory_get_status", "arguments": arguments}),
                )
                assert denied.status_code == 200
                assert denied.json()["result"]["isError"] is True
