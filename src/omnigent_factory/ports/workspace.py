"""Workspace port: ``CLEANUP_WORKSPACE`` (finished parcels, factory-owned clone only)."""

from __future__ import annotations

from omnigent_factory.core.effects import EffectKind

WORKSPACE_EFFECT_KINDS = frozenset({EffectKind.CLEANUP_WORKSPACE})
