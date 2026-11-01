"""Shared pytest configuration: Hypothesis profiles.

``default`` (local/CI): 200 examples, 40 stateful steps, no deadline, derandomized so the
suite is reproducible. ``thorough``: 2000 examples / 80 steps, randomized. Select with
``HYPOTHESIS_PROFILE=thorough``.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from functools import cache
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID
from omnigent_factory.testing.fakes import FakeClock
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


@pytest.fixture(scope="session", autouse=True)
def cel_policy():
    """Compile each expression/reason once per worker; evaluate every event normally.

    PolicySpec construction also calls this builtin to validate expressions. Sharing
    that compilation avoids rebuilding the same policies in every service fixture.
    The pinned interpreted runner creates a fresh activation for each evaluation.
    """
    from omnigent.policies.builtins import cel

    compiled = cache(cel.cel_policy)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(cel, "cel_policy", compiled)
        try:
            yield compiled
        finally:
            compiled.cache_clear()


@pytest.fixture(scope="session")
def sqlite_template(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A closed, checkpointed empty database, private to this xdist worker."""
    path = tmp_path_factory.mktemp("sqlite-template") / "empty.sqlite3"
    SqliteStore.open(path, FakeClock()).close()
    return path


@pytest.fixture(autouse=True)
def template_databases(request, monkeypatch, tmp_path_factory):
    """Copy only new, ordinary test stores; retain the real open/checksum checks.

    Schema tests opt out with fresh_database. Explicit migrations, injected faults,
    existing files and in-memory stores always follow the original migration path.
    Restrict copying to pytest's temporary directory, never an external database.
    """
    if request.node.get_closest_marker("fresh_database") is not None:
        return
    template = request.getfixturevalue("sqlite_template")
    base = tmp_path_factory.getbasetemp().resolve()
    original = SqliteStore.open.__func__

    def open_store(cls, path, clock, **kwargs):
        target = Path(path)
        if (
            cls is SqliteStore
            and str(path) != ":memory:"
            and target.resolve().is_relative_to(base)
            and target.parent.is_dir()
            and not target.exists()
            and "migrations" not in kwargs
            and kwargs.get("fault_hook") is None
        ):
            shutil.copyfile(template, target)
        return original(cls, path, clock, **kwargs)

    monkeypatch.setattr(SqliteStore, "open", classmethod(open_store))


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
