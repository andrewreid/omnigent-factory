"""Scheduler port: ``ARM_TIMER`` and ``WAKE_SCHEDULER`` intents (implemented by Task 4).

A fired timer becomes an ``ActiveLimitReached`` / ``GraceExpired`` / ``CapacityAvailable``
event with ``Provenance.SCHEDULER``. Timers never approve, decide or reset an allowance.
"""

from __future__ import annotations

from omnigent_factory.core.effects import EffectKind

SCHEDULER_EFFECT_KINDS = frozenset({EffectKind.ARM_TIMER, EffectKind.WAKE_SCHEDULER})
