"""The common effect-execution protocol (architecture §3.1).

An executor (Task 4) claims an intent from the outbox under the parcel lease, re-checks
``core.preconditions.effect_still_valid`` against the current persisted parcel, then calls
the adapter whose ``handled_kinds`` contains the intent's kind. The adapter performs at
most one external operation and returns an outcome; it never decides a new stage and never
writes to the store. The executor converts the outcome into a store transition
(complete / unknown / failed / retry) and, where relevant, a new observation event.

Ambiguity rules (§3.4): a write whose outcome is unknown MUST return
:class:`~omnigent_factory.core.effects.AmbiguousWrite`; only a server-proven no-side-effect
rejection may return :class:`~omnigent_factory.core.effects.DefinitiveFailure`.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import (
    AdapterOutcome,
    EffectIntent,
    EffectKind,
    ExecutionContext,
)


@runtime_checkable
class EffectAdapter(Protocol):
    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        """Effect kinds this adapter executes. Kinds are disjoint across adapters."""
        ...

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        """Perform the single external operation for ``effect``. Never raises for remote
        failures; returns the matching outcome type instead."""
        ...
