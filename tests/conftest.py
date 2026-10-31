"""Shared pytest configuration: Hypothesis profiles.

``default`` (local/CI): 200 examples, 40 stateful steps, no deadline, derandomized so the
suite is reproducible. ``thorough``: 2000 examples / 80 steps, randomized. Select with
``HYPOTHESIS_PROFILE=thorough``.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID
from tests.credentials.repos import GitEnv, make_git_env

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


@pytest.fixture
def service_config(tmp_path: Path) -> ServiceConfig:
    """A private service filesystem, available to every test package."""
    state = tmp_path / "state"
    secrets = tmp_path / "secrets"
    state.mkdir(mode=0o700)
    secrets.mkdir(mode=0o700)
    os.chmod(state, 0o700)
    os.chmod(secrets, 0o700)
    return ServiceConfig(
        state_dir=state,
        secrets_dir=secrets,
        repo_id=REPO_ID,
        owners=frozenset({OWNER_ID}),
        effect_poll_seconds=0.01,
        clock_interval_seconds=0.01,
        reconcile_interval_seconds=60,
    )


@pytest.fixture
def git_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[GitEnv]:
    """A private Git installation and fake HOME, available to every test package."""
    env = make_git_env(tmp_path, monkeypatch)
    try:
        yield env
    finally:
        shutil.rmtree(env.runtime, ignore_errors=True)
