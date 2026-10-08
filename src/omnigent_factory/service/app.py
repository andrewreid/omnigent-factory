"""The deliberately tiny ASGI surface: health, the GitHub webhook and (loopback) MCP.

Public ingress forwards only ``/webhooks/github``. ``/mcp`` is refused on any listener or
peer that is not loopback (:class:`omnigent_factory.service.mcp.McpGate`).
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Mount, Route

from omnigent_factory.service.interfaces import WebhookRejected, WebhookVerifier
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryOutcome, DeliveryRecord

if TYPE_CHECKING:
    from omnigent_factory.service.mcp import McpEndpoint

LOG = logging.getLogger(__name__)


def create_app(
    service: FactoryService, verifier: WebhookVerifier, mcp: McpEndpoint | None = None
) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        del app
        async with AsyncExitStack() as stack:
            if mcp is not None:
                # Tool calls are refused until the service reports ready (after store
                # migration and startup recovery), see McpGate.
                await stack.enter_async_context(mcp.manager.run())
            await service.start()
            try:
                yield
            finally:
                await service.stop()

    async def health(request: Request) -> Response:
        del request
        status = await service.health()
        return JSONResponse(status, status_code=200 if status["status"] == "ok" else 503)

    async def webhook(request: Request) -> Response:
        length = request.headers.get("content-length")
        if length is not None:
            try:
                if int(length) > service.config.webhook_max_bytes:
                    return Response(status_code=413)
            except ValueError:
                return Response(status_code=400)
        body = await _bounded_body(request, service.config.webhook_max_bytes)
        if body is None:
            return Response(status_code=413)
        try:
            delivery = await verifier.verify(body, dict(request.headers))
        except WebhookRejected as exc:
            # Fixed reason text and IDs only: never the body, headers or secret.
            LOG.warning(
                "webhook rejected: %s check failed delivery=%s event=%s action=%s reason=%s",
                exc.check,
                exc.delivery or "-",
                exc.event or "-",
                exc.action or "-",
                exc.reason,
            )
            return Response(status_code=401)
        except Exception:
            # Verifier/SDK exceptions can contain signature or credential material.
            return Response(status_code=500)
        try:
            outcome = await service.persist_delivery(delivery)
        except Exception:
            # SQLite full/unavailable and worker failures must not be acknowledged.
            # Deliberately omit exception text: it may contain filesystem secrets.
            return Response(status_code=503)
        LOG.info("webhook received %s outcome=%s", delivery_summary(delivery), outcome)
        # Every response here follows a committed inbox transaction. A conflicting
        # duplicate is retained in delivery_attempts and acknowledged as quarantined.
        return JSONResponse(
            {"accepted": outcome != DeliveryOutcome.QUARANTINED, "outcome": outcome},
            status_code=202 if outcome == DeliveryOutcome.INSERTED else 200,
        )

    routes: list[BaseRoute] = [
        Route("/healthz", health, methods=["GET"]),
        Route("/webhooks/github", webhook, methods=["POST"]),
    ]
    if mcp is not None:
        routes += [Route("/mcp", mcp.gate), Mount("/mcp", app=mcp.gate)]
    return Starlette(debug=False, lifespan=lifespan, routes=routes)


async def _bounded_body(request: Request, limit: int) -> bytes | None:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > limit:
            return None
        body.extend(chunk)
    return bytes(body)


def delivery_summary(delivery: DeliveryRecord) -> str:
    """Event/action/sender/issue identifiers only; never body text or headers."""
    try:
        payload: Any = json.loads(delivery.body)
    except ValueError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    sender = payload.get("sender")
    login = sender.get("login") if isinstance(sender, dict) else None
    target: object = None
    for key in ("issue", "pull_request"):
        item = payload.get(key)
        if isinstance(item, dict) and isinstance(item.get("number"), int):
            target = f"#{item['number']}"
            break
    else:
        item = payload.get("projects_v2_item")
        if isinstance(item, dict):
            target = item.get("content_node_id")
    return (
        f"delivery={delivery.delivery_guid} event={delivery.event_name} "
        f"action={payload.get('action')} sender={login} issue={target}"
    )
