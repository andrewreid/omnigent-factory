"""Shared pytest configuration: Hypothesis profiles.

``default`` (local/CI): 200 examples, 40 stateful steps, no deadline, derandomized so the
suite is reproducible. ``thorough``: 2000 examples / 80 steps, randomized. Select with
``HYPOTHESIS_PROFILE=thorough``.
"""

from __future__ import annotations

import os

from hypothesis import HealthCheck, settings

settings.register_profile(
    "default",
    max_examples=200,
    stateful_step_count=40,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.register_profile(
    "thorough",
    max_examples=2000,
    stateful_step_count=80,
    deadline=None,
    derandomize=False,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))
