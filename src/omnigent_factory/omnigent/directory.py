"""Adapter-side context ports for the Omnigent executor.

Task-1 effect ``args`` are deliberately sparse (no message text, answers, paths or
secrets). The adapter obtains the rest from these narrow protocols, which the service
(Task 4/5) implements from the store, dispatch snapshots and message templates:

* :class:`DispatchDirectory` - the persisted stage tuple and daemon-rendered texts;
* :class:`OwnItemLedger` - own-send intents recorded *before* a POST (effect ID, session,
  node, exact text digest) and returned item IDs, for lost-ack adoption;
* :class:`CredentialProvisioner` - the broker's capability provisioning and branch scope.

None of these carries authority: the executor has already re-checked preconditions.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import EffectIntent
from omnigent_factory.core.types import SessionKind
from omnigent_factory.credentials.capabilities import CapabilityRecord

FormValue = str | int | float | bool | list[str] | None


@dataclass(frozen=True, slots=True)
class StageSpec:
    """Persisted dispatch tuple for one stage run (§3.4 create row).

    ``bind_worktree`` set means a successor stage binding the verified existing parcel
    worktree (``existing_worktree=true``, no base branch). Otherwise Omnigent creates a
    worktree for ``branch`` from ``base_branch`` in the dedicated source clone.
    """

    session_id: str
    parcel_id: str
    kind: SessionKind
    attempt: int
    nonce: str
    branch: str
    title: str
    base_branch: str | None = "main"
    bind_worktree: Path | None = None
    root_id: str | None = None
    grant_id: str = ""
    granted_us: int = 0
    policy_generation: int = 1
    #: The ``factory.dispatch`` label of the issue session this run executes in (the
    #: creating run's nonce); ``None`` means the run's own ``nonce``.
    root_nonce: str | None = None


@runtime_checkable
class DispatchDirectory(Protocol):
    async def stage_spec(self, session_id: str) -> StageSpec | None: ...

    async def message_text(self, effect: EffectIntent) -> str | None:
        """Daemon-rendered text for a ``SEND_MESSAGE`` intent (without the marker)."""
        ...

    async def elicitation_content(self, effect: EffectIntent) -> Mapping[str, FormValue] | None:
        """Flat MCP form content for a ``RESOLVE_ELICITATION`` intent."""
        ...


@dataclass(frozen=True, slots=True)
class OwnSend:
    effect_id: str
    session_id: str
    node_id: str
    kind: str  # "message" | "resolve"
    text_sha256: str = ""
    elicitation_id: str = ""
    item_id: str | None = None


@runtime_checkable
class OwnItemLedger(Protocol):
    async def record_intent(self, send: OwnSend) -> None: ...

    async def record_item(self, effect_id: str, item_id: str) -> None: ...

    async def lookup(self, effect_id: str) -> OwnSend | None: ...

    async def own_item_ids(self, session_id: str) -> frozenset[str]: ...


class VolatileStateError(RuntimeError):
    """An in-memory stand-in was constructed without explicitly accepting volatility."""


class MemoryOwnItemLedger:
    """In-process ledger. NOT FOR PRODUCTION.

    Pre-POST send/resolve intents are lost on restart, so an ambiguous write could not be
    reconciled after a daemon crash. Production uses the store-backed
    :class:`omnigent_factory.service.durable.StoreOwnItemLedger` (``own_sends`` table).
    Construction requires ``volatile_ok=True`` so this cannot be wired silently.
    """

    def __init__(self, *, volatile_ok: bool) -> None:
        if volatile_ok is not True:
            raise VolatileStateError("MemoryOwnItemLedger is test-only; wire a durable ledger")
        self._sends: dict[str, OwnSend] = {}

    async def record_intent(self, send: OwnSend) -> None:
        self._sends.setdefault(send.effect_id, send)

    async def record_item(self, effect_id: str, item_id: str) -> None:
        send = self._sends.get(effect_id)
        if send is not None:
            self._sends[effect_id] = OwnSend(
                send.effect_id,
                send.session_id,
                send.node_id,
                send.kind,
                send.text_sha256,
                send.elicitation_id,
                item_id,
            )

    async def lookup(self, effect_id: str) -> OwnSend | None:
        return self._sends.get(effect_id)

    async def own_item_ids(self, session_id: str) -> frozenset[str]:
        return frozenset(
            s.item_id for s in self._sends.values() if s.session_id == session_id and s.item_id
        )


@runtime_checkable
class CredentialProvisioner(Protocol):
    async def provision(self, session_id: str) -> CapabilityRecord:
        """Create or rotate the stage capability, durably recorded before returning."""
        ...
