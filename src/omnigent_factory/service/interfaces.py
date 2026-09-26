"""Task-2 integration seams owned by the service, without widening Task-1 ports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from omnigent_factory.core.events import Event
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store.sqlite import ApplyResult, DeliveryRecord


class WebhookRejected(ValueError):
    """A request failed signature, identity, or routing validation."""


class NonRetryableDelivery(RuntimeError):
    """The durable delivery is poison and requires explicit operator release.

    ``parcel_id`` scopes the work gate when normalization identified the parcel. Without
    one, the delivery gates work repository-wide. Exception text is never logged.
    """

    def __init__(self, reason: str, *, parcel_id: str | None = None) -> None:
        super().__init__(reason)
        self.parcel_id = parcel_id


class WebhookVerifier(Protocol):
    async def verify(self, body: bytes, headers: Mapping[str, str]) -> DeliveryRecord:
        """Verify raw bytes before parsing and return the trusted durable envelope."""
        ...


class DeliveryProcessor(Protocol):
    async def process(self, delivery: DeliveryRecord) -> None:
        """Normalize one already-durable delivery through the application service.

        Task 5 must bind the processor to ``FactoryService.apply_event`` after service
        construction. It must apply normalized events through that method (preserving the
        parcel lock), set ``Event.delivery_guid``, and pass ``delivery_status`` as
        ``processed``, ``unresolved`` or ``quarantined`` so the event and inbox retirement
        commit atomically. Transient failures raise an ordinary exception and are retried
        indefinitely with capped backoff. Only deterministic poison may raise
        ``NonRetryableDelivery``; it remains work-gating until an operator releases it.
        Returning without changing the delivery status is not completion.
        """
        ...


class ObservationSink(Protocol):
    """Task-5 feedback seam for reconcile/scan adapters.

    Adapters producing GitHub snapshots, PR evidence, session-tree scans, or stream
    observations must feed normalized events back through this sink; an ``Ack`` alone is
    not state evidence. Task 5 binds this to ``FactoryService.apply_event`` so the same
    per-parcel serialization and reducer validation apply to webhook and polled facts.
    """

    async def apply_event(self, event: Event) -> ApplyResult: ...


class SetupRenderer(Protocol):
    def render(self, config: ServiceConfig) -> Mapping[str, str]: ...

    def validate(self, config: ServiceConfig) -> tuple[str, ...]: ...
