from __future__ import annotations

import os
from importlib.resources import files
from pathlib import Path

from omnigent_factory.core.protocol import Correlation, parse_factory_result
from omnigent_factory.core.types import MICROS_PER_MINUTE, Size
from omnigent_factory.service.config import ServiceConfig, load_app_env
from omnigent_factory.service.setup import OperationsRenderer


def test_stage_templates_state_outcomes_guardrails_and_complete_result_shapes():
    root = files("omnigent_factory.service") / "templates"
    triage = (root / "triage-v1.txt").read_text(encoding="utf-8")
    plan = (root / "plan-v1.txt").read_text(encoding="utf-8")
    build = (root / "build-v1.txt").read_text(encoding="utf-8")
    checkpoint = (root / "checkpoint-v1.txt").read_text(encoding="utf-8")
    correction = (root / "correction-v1.txt").read_text(encoding="utf-8")

    for template in (triage, plan, build, checkpoint):
        assert "Outcome:" in template
        assert "FACTORY_RESULT_V1" in template
        assert "...}" not in template
    assert "scope, behaviour, cost, or risk" in plan
    assert "at most one fix batch and one targeted recheck" in build
    assert "Every bot finding" in build
    assert "Never\nmerge" in build
    assert "Stop starting new work" in checkpoint
    assert "only automatic\nformat-correction request" in correction

    values = {
        "repository": "owner/repo",
        "issue_number": 1,
        "parcel_id": "parcel",
        "session_id": "session",
        "nonce": "nonce",
        "revision": 2,
        "granted_us": 7_200_000_000,
        "issue_snapshot": "issue",
        "guidance": "guidance",
        "branch": "factory/issue-1",
        "gh_wrapper": "/factory/gh",
        "capability_file": "/factory/cap",
        "authority": "authority",
        "authority_hash": "0" * 64,
        "grant_id": "grant",
        "untrusted_boundary": "FACTORY_DATA_test",
    }
    for stage, template, checkpointed in (
        ("triage", triage, False),
        ("plan", plan, False),
        ("build", build, False),
        ("build", checkpoint, True),
    ):
        parse_factory_result(
            template.format(**values),
            Correlation("parcel", "session", "nonce", 2, stage, in_checkpoint=checkpointed),
        )


def test_app_env_is_private_and_checkpoint_config_reaches_trust_root(tmp_path: Path):
    env = tmp_path / "app.env"
    env.write_text("FACTORY_APP_ID=5085812\nFACTORY_BOT_USER_ID=334191208\n")
    os.chmod(env, 0o600)
    assert load_app_env(env)["FACTORY_APP_ID"] == "5085812"

    config = ServiceConfig(
        state_dir=tmp_path / "state",
        secrets_dir=tmp_path / "secrets",
        app_env=env,
        repo_id="R_1",
        owners=frozenset({114979}),
        checkpoint_block_hours={"S": 3, "M": 5, "L": 7},
        checkpoint_grace_minutes=19,
        cost_backstop_usd_per_hour=41,
    )
    assert config.trusted.block_hours == {Size.S: 3, Size.M: 5, Size.L: 7}
    assert config.trusted.grace_us == 19 * MICROS_PER_MINUTE
    assert config.trusted.cost_usd_per_hour_micros == 41_000_000


def test_setup_render_contains_deploy_config_and_private_systemd_unit(
    service_config: ServiceConfig, tmp_path: Path
):
    artifacts = OperationsRenderer(tmp_path / "config.toml").render(service_config)
    unit = artifacts["omnigent-factory.service"]
    example = artifacts["config.example.toml"]
    assert "UMask=0077" in unit
    assert 'bind_host = "127.0.0.1"' in example
    assert "github_installation_id = 165144097" in example
    assert 'project_node_id = "PVT_kwDOEanNes4BkJhb"' in example
    assert 'omnigent_base_url = "https://omnigent.reid.ee"' in example
    assert "max_building = 1" in example


def test_setup_validation_refuses_wildcard_bind_hosts(
    service_config: ServiceConfig, tmp_path: Path
):
    renderer = OperationsRenderer(tmp_path / "config.toml")
    for host in ("0.0.0.0", "::", "[::]"):  # noqa: S104
        wildcard = service_config.model_copy(update={"bind_host": host})
        assert "bind_host must be a specific address, not a wildcard" in renderer.validate(wildcard)
    for host in ("127.0.0.1", "192.0.2.10"):
        specific = service_config.model_copy(update={"bind_host": host})
        assert not any("bind_host" in error for error in renderer.validate(specific))
