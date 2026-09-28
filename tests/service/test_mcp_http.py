"""The mounted ``/mcp`` endpoint over HTTP: loopback-only, bearer, six tools, JSON-RPC."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import Lifecycle, Via
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import ServiceDispatchDirectory
from omnigent_factory.service.mcp import TOOL_NAMES
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)

P = "I_mcp_http"


def _token(config: ServiceConfig) -> str:
    token = "t" * 43
    path = config.resolved_mcp_token_file
    path.write_text(token)
    os.chmod(path, 0o600)
    return token


def _rpc(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}


HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json"}


def _client(app: Any, *, server: str = "127.0.0.1", peer: str = "127.0.0.1") -> Any:
    from starlette.testclient import TestClient

    return TestClient(app, base_url=f"http://{server}:{8787}", client=(peer, 50123))


def _app(config: ServiceConfig) -> tuple[Any, FactoryService]:
    from omnigent_factory.service.app import create_app
    from omnigent_factory.service.mcp import build_endpoint

    service = FactoryService(
        config,
        adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()),
        clock=FakeClock(),
    )
    endpoint = build_endpoint(service, ServiceDispatchDirectory(service.db, config), config)
    return create_app(service, object(), endpoint), service  # type: ignore[arg-type]


def test_mcp_http_requires_loopback_and_bearer(service_config: ServiceConfig) -> None:
    token = _token(service_config)
    app, _service = _app(service_config)
    listing = _rpc("tools/list")
    with _client(app) as client:
        assert client.post("/mcp/", json=listing, headers=HEADERS).status_code == 401
        wrong = {**HEADERS, "authorization": "Bearer " + "x" * 43}
        assert client.post("/mcp/", json=listing, headers=wrong).status_code == 401
        query = client.post(f"/mcp/?token={token}", json=listing, headers=HEADERS)
        assert query.status_code == 401  # never accepted in the query string
    auth = {**HEADERS, "authorization": f"Bearer {token}"}
    app, _service = _app(service_config)
    with _client(app, peer="192.168.1.20") as client:
        assert client.post("/mcp/", json=listing, headers=auth).status_code == 404
    app, _service = _app(service_config)
    with _client(app, server="192.168.1.5", peer="127.0.0.1") as client:
        # The LAN listener never serves /mcp, whoever connects.
        assert client.post("/mcp/", json=listing, headers=auth).status_code == 404
        assert client.get("/healthz").status_code in (200, 503)


def test_mcp_http_missing_token_file_refuses(service_config: ServiceConfig) -> None:
    app, _service = _app(service_config)
    with _client(app) as client:
        auth = {**HEADERS, "authorization": "Bearer " + "t" * 43}
        assert client.post("/mcp/", json=_rpc("tools/list"), headers=auth).status_code == 503


def test_mcp_http_lists_and_calls_the_six_tools(service_config: ServiceConfig) -> None:
    token = _token(service_config)
    app, service = _app(service_config)
    auth = {**HEADERS, "authorization": f"Bearer {token}"}
    with _client(app) as client:
        for path in ("/mcp", "/mcp/"):
            response = client.post(path, json=_rpc("tools/list"), headers=auth)
            assert response.status_code == 200, response.text
            tools = response.json()["result"]["tools"]
            assert [t["name"] for t in tools] == list(TOOL_NAMES)
            assert all("session_id" in t["inputSchema"]["required"] for t in tools)
        call = _rpc(
            "tools/call", {"name": "factory_get_status", "arguments": {"session_id": "conv_x"}}
        )
        body = client.post("/mcp/", json=call, headers=auth).json()["result"]
        assert body["isError"] is True
        assert "not a factory issue session" in body["content"][0]["text"]
        bad = _rpc("tools/call", {"name": "factory_get_status", "arguments": {}})
        assert client.post("/mcp/", json=bad, headers=auth).json()["result"]["isError"] is True
        # A registered issue session gets its state as text content (and structured).
        factory = EventFactory(P, issue_number=42)
        portal = client.portal
        assert portal is not None
        portal.call(service.operator_command, "unpause", {})
        portal.call(
            service.apply_event,
            factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        )
        portal.call(service.apply_event, factory.make(ev.RequestTriage(via=Via.DRAG)))
        root = None
        for _ in range(300):
            parcel = portal.call(service.db.call, lambda store: store.load_parcel(P))
            run = parcel.current_session if parcel is not None else None
            if run is not None and run.lifecycle == Lifecycle.ACTIVE:
                root = run.root_id
                break
            time.sleep(0.01)
        assert root is not None
        ok = _rpc("tools/call", {"name": "factory_get_status", "arguments": {"session_id": root}})
        result = client.post("/mcp/", json=ok, headers=auth).json()["result"]
        assert result.get("isError") is not True
        text = json.loads(result["content"][0]["text"])
        assert text["stage"] == "triage" and text["session_id"] == root
        assert result["structuredContent"]["run_id"] == text["run_id"]
