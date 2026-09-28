"""Omnigent port (implemented by Task 3).

Effect kinds: ``CREATE_SESSION`` (empty session, no initial items), ``PREPARE_SESSION``
(worktree verification + policies), ``SEND_MESSAGE``, ``RESOLVE_ELICITATION``,
``INTERRUPT_TREE``, ``SCAN_TREE``, ``RECONCILE_SESSION``, ``REPLACE_COST_POLICY``
(see :data:`OMNIGENT_EFFECT_KINDS`).

Create/message POSTs are never blindly retried. After an ambiguous create, adoption is by
exact nonce across all pages including archived roots; exactly one verified match is
required. Quiescence requires a complete recursive scan (all pages, archived included).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.ports.adapter import EffectAdapter

OMNIGENT_EFFECT_KINDS = frozenset(
    {
        EffectKind.CREATE_SESSION,
        EffectKind.PREPARE_SESSION,
        EffectKind.SEND_MESSAGE,
        EffectKind.RESOLVE_ELICITATION,
        EffectKind.INTERRUPT_TREE,
        EffectKind.SCAN_TREE,
        EffectKind.RECONCILE_SESSION,
        EffectKind.REPLACE_COST_POLICY,
        EffectKind.CLOSE_SESSION,
        EffectKind.VERIFY_POLICIES,
    }
)


@dataclass(frozen=True, slots=True)
class SessionMatch:
    """A listed root whose labels carry the searched nonce (correlation only)."""

    root_id: str
    nonce: str
    workspace: str | None
    branch: str | None
    agent_id: str | None


@dataclass(frozen=True, slots=True)
class TreeScan:
    """Result of one recursive scan of a stage tree.

    ``complete`` is false if any page/node could not be read; then quiescence is unknown.
    """

    root_id: str
    complete: bool
    busy: bool
    pending_waiter: bool
    node_ids: frozenset[str]


@runtime_checkable
class OmnigentObserver(Protocol):
    async def find_by_nonce(self, nonce: str) -> list[SessionMatch] | RetryableReadFailure: ...

    async def scan_tree(self, root_id: str) -> TreeScan: ...


@runtime_checkable
class OmnigentAdapter(OmnigentObserver, EffectAdapter, Protocol):
    """Combined Omnigent observer + effect executor."""
