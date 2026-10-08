"""Task-2 integration seams owned by the service, without widening Task-1 ports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from omnigent_factory.core.events import Event
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store.sqlite import ApplyResult, DeliveryRecord


class WebhookRejected(ValueError):
    """A request failed signature, identity, or routing validation.

    ``check`` is ``signature``, ``identity`` or ``request`` and ``reason`` a fixed,
    secret-free description (which identity check failed and the IDs it saw); the
    delivery GUID, event and action come from the headers and the body's ``action``
    only, never from body text.
    """

    def __init__(
        self,
        reason: str,
        *,
        check: str = "request",
        delivery: str | None = None,
        event: str | None = None,
        action: str | None = None,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.check = check
        self.delivery = delivery
        self.event = event
        self.action = action


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
        ``NonRetryableDelivery``; it remains work-gating until an operator releases it, and
        a parcel-scoped park also fences and interrupts that parcel (``InboxHoldSet``).
        A delivery whose identity cannot yet be verified is retired with
        ``FactoryService.hold_unresolved_delivery(guid, candidate_parcel_id)``: a known
        candidate parcel is fenced and held until the delivery is later processed. Only a
        card proven foreign by the lookup may be retired unapplied; an inconclusive one is
        retried on a durable backoff and, once exhausted, parked for the operator.
        A recovered delivery whose GUID is already stored is a durable no-op (the stored
        copy wins even when GitHub re-serialised the recovered bytes).
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


class ManagedRuntime(Protocol):
    """A production integration whose lifetime is owned by ``FactoryService``."""

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    def healthy(self) -> bool: ...
