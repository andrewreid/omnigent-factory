from __future__ import annotations

import sqlite3
from collections.abc import Mapping

from starlette.testclient import TestClient

from omnigent_factory.service.app import create_app
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.interfaces import WebhookRejected
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore
from omnigent_factory.testing.fakes import FakeClock


class Verifier:
    def __init__(self, *, reject: bool = False) -> None:
        self.reject = reject
        self.seen: list[bytes] = []

    async def verify(self, body: bytes, headers: Mapping[str, str]) -> DeliveryRecord:
        self.seen.append(body)
        if self.reject:
            raise WebhookRejected("bad signature")
        return DeliveryRecord(
            delivery_guid=headers["x-github-delivery"],
            event_name=headers["x-github-event"],
            body=body,
            headers={"x-github-event": headers["x-github-event"]},
        )


def test_receiver_verifies_raw_bytes_and_acks_only_after_durable_commit(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    verifier = Verifier()
    service = FactoryService(service_config, clock=clock)
    with TestClient(create_app(service, verifier)) as client:
        response = client.post(
            "/webhooks/github",
            content=b'{"exact": "bytes"}\n',
            headers={"x-github-delivery": "D1", "x-github-event": "issues"},
        )
        assert response.status_code == 202
        assert response.json()["outcome"] == "inserted"
        assert verifier.seen == [b'{"exact": "bytes"}\n']
        assert client.get("/healthz").json() == {"status": "ok", "paused": True}
        assert client.get("/status").status_code == 404
    store = SqliteStore.open(service_config.database_path, clock)
    try:
        [persisted] = store.pending_deliveries()
        assert persisted.delivery_guid == "D1"
        assert persisted.body == b'{"exact": "bytes"}\n'
    finally:
        store.close()


def test_receiver_rejects_before_persistence_and_quarantines_conflicting_duplicate(
    service_config: ServiceConfig,
):
    clock = FakeClock()
    rejected = FactoryService(service_config, clock=clock)
    with TestClient(create_app(rejected, Verifier(reject=True))) as client:
        assert client.post("/webhooks/github", content=b"secret").status_code == 401

    accepted = FactoryService(service_config, clock=clock)
    with TestClient(create_app(accepted, Verifier())) as client:
        headers = {"x-github-delivery": "D2", "x-github-event": "issues"}
        assert client.post("/webhooks/github", content=b"one", headers=headers).status_code == 202
        response = client.post("/webhooks/github", content=b"two", headers=headers)
        assert response.status_code == 200
        assert response.json() == {"accepted": False, "outcome": "quarantined"}


def test_receiver_enforces_body_bound_before_verification(service_config: ServiceConfig):
    config = service_config.model_copy(update={"webhook_max_bytes": 3})
    verifier = Verifier()
    service = FactoryService(config, clock=FakeClock())
    with TestClient(create_app(service, verifier)) as client:
        assert client.post("/webhooks/github", content=b"four").status_code == 413
    assert verifier.seen == []


def test_failed_delivery_commit_returns_503(service_config: ServiceConfig):
    service = FactoryService(service_config, clock=FakeClock())

    async def fail(delivery: DeliveryRecord) -> str:
        del delivery
        raise sqlite3.OperationalError("database is full")

    service.persist_delivery = fail  # type: ignore[method-assign]
    with TestClient(create_app(service, Verifier())) as client:
        response = client.post(
            "/webhooks/github",
            content=b"{}",
            headers={"x-github-delivery": "D-full", "x-github-event": "issues"},
        )
    assert response.status_code == 503


def test_unexpected_verifier_error_is_non_2xx_without_exception_leak(
    service_config: ServiceConfig,
):
    class BrokenVerifier:
        async def verify(self, body: bytes, headers: Mapping[str, str]) -> DeliveryRecord:
            del body, headers
            raise RuntimeError("authorization=must-not-be-logged")

    service = FactoryService(service_config, clock=FakeClock())
    with TestClient(create_app(service, BrokenVerifier()), raise_server_exceptions=False) as client:
        response = client.post("/webhooks/github", content=b"{}")
    assert response.status_code >= 500
