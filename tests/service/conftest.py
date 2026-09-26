from __future__ import annotations

import os
from pathlib import Path

import pytest

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID


@pytest.fixture
def service_config(tmp_path: Path) -> ServiceConfig:
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
