"""The deliberately tiny public ASGI surface."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from omnigent_factory.service.interfaces import WebhookRejected, WebhookVerifier
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryOutcome


def create_app(service: FactoryService, verifier: WebhookVerifier) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        del app
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
        except WebhookRejected:
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
        # Every response here follows a committed inbox transaction. A conflicting
        # duplicate is retained in delivery_attempts and acknowledged as quarantined.
        return JSONResponse(
            {"accepted": outcome != DeliveryOutcome.QUARANTINED, "outcome": outcome},
            status_code=202 if outcome == DeliveryOutcome.INSERTED else 200,
        )

    return Starlette(
        debug=False,
        lifespan=lifespan,
        routes=[
            Route("/healthz", health, methods=["GET"]),
            Route("/webhooks/github", webhook, methods=["POST"]),
        ],
    )


async def _bounded_body(request: Request, limit: int) -> bytes | None:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > limit:
            return None
        body.extend(chunk)
    return bytes(body)
