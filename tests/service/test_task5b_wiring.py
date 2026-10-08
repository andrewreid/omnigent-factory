from __future__ import annotations

import os
from importlib.resources import files
from pathlib import Path

from omnigent_factory.core.types import MICROS_PER_MINUTE, Size
from omnigent_factory.service.config import ServiceConfig, load_app_env
from omnigent_factory.service.setup import OperationsRenderer


def test_stage_templates_are_short_tool_pointers():
    root = files("omnigent_factory.service") / "templates"
    values = {
        "repository": "owner/repo",
        "issue_number": 1,
        "run_id": "ss_run",
        "session_id": "conv_root",
        "revision": 2,
        "granted_minutes": 120,
        "handoff": "",
        "branch": "factory/issue-1",
        "gh_wrapper": "/factory/gh",
        "capability_file": "/factory/cap",
        "plan_hash": "0" * 64,
    }
    first_tool = {
        "triage": "factory_get_issue",
        "plan": "factory_get_issue",
        "build": "factory_get_plan",
    }
    names = {"triage": "triage-v6.txt", "plan": "plan-v6.txt", "build": "build-v8.txt"}
    for stage, tool in first_tool.items():
        text = (root / names[stage]).read_text(encoding="utf-8").format(**values)
        assert f"Start with {tool}" in text
        assert "conv_root" in text and "ss_run" in text and "factory_submit_result" in text
        # No result-format instructions or correction templates in the pointer.
        assert "FACTORY_RESULT" not in text and "```" not in text
        assert len(text) < 1300
    build = (root / "build-v8.txt").read_text(encoding="utf-8").format(**values)
    assert "`Closes #1`" in build and "Never merge" in build and "0" * 64 in build
    assert "/factory/gh" in build and "/factory/cap" in build
    assert not (root / "correction-v1.txt").is_file()


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
