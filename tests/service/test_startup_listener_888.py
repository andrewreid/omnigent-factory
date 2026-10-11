"""#888: a boot took 6 minutes before uvicorn accepted connections (the whole startup
reconciliation ran inside the ASGI lifespan), so webhooks and /mcp were unreachable and
live runners got ConnectError. The listener now comes up once the store is open; the
slow reconciliation (GitHub, Omnigent) finishes behind it, and nothing is admitted, sent
or issued, and no MCP tool call accepted, before it completes.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from omnigent_factory.service.app import create_app
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.fakes import FakeClock
from tests.service.test_app import Verifier

pytestmark = pytest.mark.asyncio

HOOK = {"x-github-delivery": "D1", "x-github-event": "issues"}


class SlowReconcile:
    """A managed integration whose start (startup reconciliation) waits to be released."""

    def __init__(self, *, fail: bool = False) -> None:
        self.release = asyncio.Event()
        self.fail = fail
        self.started = False
        self.closed = False

    async def start(self) -> None:
        await self.release.wait()
        if self.fail:
            raise RuntimeError("reconciliation failed")
        self.started = True

    async def close(self) -> None:
        self.closed = True

    def healthy(self) -> bool:
        return True


def _client(app: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://f")  # type: ignore[arg-type]


async def test_webhooks_are_stored_while_startup_reconciles(service_config: ServiceConfig):
    service = FactoryService(service_config, clock=FakeClock())
    slow = SlowReconcile()
    service.bind_integrations(managed=(slow,))
    app = create_app(service, Verifier())
    async with asyncio.timeout(5), app.router.lifespan_context(app), _client(app) as client:
        # The listener is up although reconciliation has not finished.
        response = await client.post("/webhooks/github", content=b"{}", headers=HOOK)
        assert response.status_code == 202  # committed before the 2xx
        health = await client.get("/healthz")
        assert (health.status_code, health.json()) == (503, {"status": "starting"})
        assert not service.ready and not service.accepting_admission  # MCP refused, no admission
        assert not service._tasks  # no outbox (sends), deliveries or admission loop yet
        slow.release.set()
        while not service.ready:
            await asyncio.sleep(0.01)
        assert slow.started and service.accepting_admission
        assert (await client.get("/healthz")).json()["status"] == "ok"
    store = SqliteStore.open(service_config.database_path, FakeClock())
    try:
        rows = store.query("SELECT delivery_guid FROM deliveries", ())
        assert [row[0] for row in rows] == ["D1"]
    finally:
        store.close()


async def test_shutdown_during_startup_reconciliation_releases_everything(
    service_config: ServiceConfig,
):
    service = FactoryService(service_config, clock=FakeClock())
    slow = SlowReconcile()
    service.bind_integrations(managed=(slow,))
    app = create_app(service, Verifier())
    async with asyncio.timeout(5), app.router.lifespan_context(app):
        pass  # stopped before reconciliation finished
    assert slow.closed and not slow.started and not service.ready
    # The process lock and store were released: a new daemon starts.
    again = FactoryService(service_config, clock=FakeClock())
    await again.start()
    await again.stop()


async def test_a_failed_startup_reconciliation_is_fatal(service_config: ServiceConfig):
    exits: list[int] = []
    service = FactoryService(service_config, clock=FakeClock(), fatal_exit=exits.append)
    slow = SlowReconcile(fail=True)
    service.bind_integrations(managed=(slow,))
    app = create_app(service, Verifier())
    async with asyncio.timeout(5), app.router.lifespan_context(app), _client(app) as client:
        slow.release.set()
        while not exits:
            await asyncio.sleep(0.01)
        assert exits == [1] and not service.ready and slow.closed
        assert (await client.get("/healthz")).json()["status"] == "unhealthy"
        # The store is closed: a webhook is refused, never acknowledged.
        response = await client.post("/webhooks/github", content=b"{}", headers=HOOK)
        assert response.status_code == 503
