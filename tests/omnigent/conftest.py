from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.credentials.repos import GitEnv, make_git_env


@pytest.fixture
def git_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[GitEnv]:
    env = make_git_env(tmp_path, monkeypatch)
    try:
        yield env
    finally:
        shutil.rmtree(env.runtime, ignore_errors=True)
