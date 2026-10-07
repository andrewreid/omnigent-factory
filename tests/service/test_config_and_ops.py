from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from omnigent_factory.service.config import ConfigError, ServiceConfig, load_config
from omnigent_factory.service.locking import AlreadyRunning, ProcessLock
from omnigent_factory.service.redaction import redact_fields
from omnigent_factory.service.setup import OperationsRenderer
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID


def test_load_config_resolves_paths_and_keeps_loopback_default(tmp_path: Path):
    path = tmp_path / "factory.toml"
    path.write_text(
        f'[service]\nrepo_id = "{REPO_ID}"\nowners = [{OWNER_ID}]\nstate_dir = "state"\n',
        encoding="utf-8",
    )
    config = load_config(path)
    assert config.state_dir == tmp_path / "state"
    assert config.bind_host == "127.0.0.1"
    assert config.bind_port == 8787
    assert config.max_building == 1


def test_private_directory_and_secret_modes_are_enforced(tmp_path: Path):
    state = tmp_path / "state"
    secrets = tmp_path / "secrets"
    state.mkdir(mode=0o700)
    secrets.mkdir(mode=0o700)
    secret = secrets / "webhook"
    secret.write_text("do-not-log", encoding="utf-8")
    os.chmod(secret, 0o644)
    config = ServiceConfig(
        state_dir=state,
        secrets_dir=secrets,
        repo_id=REPO_ID,
        owners=frozenset({OWNER_ID}),
    )
    with pytest.raises(ConfigError, match="0600"):
        config.prepare_private_directories()
    os.chmod(secret, 0o600)
    config.prepare_private_directories()


def test_service_refuses_non_private_state_directory(tmp_path: Path):
    state = tmp_path / "state"
    state.mkdir(mode=0o755)
    os.chmod(state, 0o755)  # noqa: S103 - deliberately unsafe fixture
    config = ServiceConfig(
        state_dir=state,
        secrets_dir=tmp_path / "missing-secrets",
        repo_id=REPO_ID,
        owners=frozenset({OWNER_ID}),
    )
    with pytest.raises(ConfigError, match="0700"):
        config.prepare_private_directories()


def test_service_refuses_state_directory_owned_by_another_uid(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch
):
    owner = service_config.state_dir.stat().st_uid
    monkeypatch.setattr("omnigent_factory.service.config.os.getuid", lambda: owner + 1)
    with pytest.raises(ConfigError, match="owned by the daemon user"):
        service_config.prepare_private_directories()


def test_process_lock_excludes_second_instance(service_config: ServiceConfig):
    first = ProcessLock(service_config.state_dir)
    second = ProcessLock(service_config.state_dir)
    first.acquire()
    try:
        assert stat.S_IMODE(first.path.stat().st_mode) == 0o600
        with pytest.raises(AlreadyRunning):
            second.acquire()
    finally:
        first.close()
    second.acquire()
    second.close()


def test_setup_renderer_is_non_applying_and_documents_private_ingress(
    service_config: ServiceConfig, tmp_path: Path
):
    renderer = OperationsRenderer(tmp_path / "config.toml")
    artifacts = renderer.render(service_config)
    assert set(artifacts) == {
        "omnigent-factory.service",
        "INGRESS.md",
        "config.example.toml",
    }
    assert "127.0.0.1:8787" in artifacts["INGRESS.md"]
    assert "POST /webhooks/github" in artifacts["INGRESS.md"]
    assert "UMask=0077" in artifacts["omnigent-factory.service"]
    assert "github_app_id = 5085812" in artifacts["config.example.toml"]
    assert renderer.validate(service_config) == ()


def test_structured_secret_fields_are_redacted():
    fields = redact_fields(
        {"parcel": "P1", "Authorization": "Bearer secret", "webhook_signature": "sha256=x"}
    )
    assert fields == {
        "parcel": "P1",
        "Authorization": "[REDACTED]",
        "webhook_signature": "[REDACTED]",
    }


def test_review_bot_grace_defaults_to_ten_minutes_and_is_hot():
    from omnigent_factory.core.types import MICROS_PER_MINUTE
    from omnigent_factory.service.config import HOT_RELOAD_KEYS

    config = ServiceConfig(repo_id="R", owners=frozenset({1}))
    assert config.trusted.review_grace_us == 10 * MICROS_PER_MINUTE
    assert (
        ServiceConfig(
            repo_id="R", owners=frozenset({1}), review_bot_grace_minutes=0
        ).trusted.review_grace_us
        == 0
    )
    assert "review_bot_grace_minutes" in HOT_RELOAD_KEYS


def test_review_bot_eyes_windows_default_and_are_hot():
    """#799: no 👀 within 5 minutes ends the wait; 👀 (or an unreadable state) waits up
    to 45 minutes; both apply on `reload`."""
    from omnigent_factory.core.types import MICROS_PER_MINUTE
    from omnigent_factory.service.config import HOT_RELOAD_KEYS

    trusted = ServiceConfig(repo_id="R", owners=frozenset({1})).trusted
    assert trusted.review_ack_us == 5 * MICROS_PER_MINUTE
    assert trusted.review_cap_us == 45 * MICROS_PER_MINUTE
    custom = ServiceConfig(
        repo_id="R",
        owners=frozenset({1}),
        review_bot_ack_minutes=3,
        review_bot_max_wait_minutes=60,
    ).trusted
    assert (custom.review_ack_us, custom.review_cap_us) == (3 * MICROS_PER_MINUTE, 3600_000_000)
    assert {"review_bot_ack_minutes", "review_bot_max_wait_minutes"} <= HOT_RELOAD_KEYS
