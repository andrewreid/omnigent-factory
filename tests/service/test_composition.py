from __future__ import annotations

import base64
import os
import subprocess
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from omnigent_factory.core.effects import EffectKind
from omnigent_factory.service.composition import build_production
from omnigent_factory.service.config import ServiceConfig


def _secret(path: Path, value: bytes) -> None:
    path.write_bytes(value)
    os.chmod(path, 0o600)


@pytest.mark.asyncio
async def test_production_composition_binds_every_external_effect_and_real_verifier(
    tmp_path: Path,
):
    secrets = tmp_path / "secrets"
    secrets.mkdir(mode=0o700)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    _secret(
        secrets / "app.pem",
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )
    _secret(secrets / "webhook_secret", b"webhook")
    _secret(secrets / "omnigent-token", b"owner")
    clone = tmp_path / "clone"
    subprocess.run(  # noqa: S603
        ("git", "init", str(clone)),  # noqa: S607
        check=True,
        capture_output=True,
    )
    subprocess.run(  # noqa: S603
        (  # noqa: S607
            "git",
            "-C",
            str(clone),
            "remote",
            "add",
            "origin",
            "https://github.com/SA-Ambulance/timesheets.git",
        ),
        check=True,
    )
    config = ServiceConfig(
        state_dir=tmp_path / "state",
        runtime_dir=tmp_path / "runtime",
        secrets_dir=secrets,
        app_env=tmp_path / "absent.env",
        source_clone=clone,
        worktree_root=tmp_path / "worktrees",
        wrapper_bin_dir=tmp_path / "bin",
        gh_config_dir=tmp_path / "gh",
        real_gh_path=Path("/bin/true"),
        repo_id="R_NODE",
        repository_database_id=123,
        organization_id=456,
        owners=frozenset({114979}),
        omnigent_host_id="host",
        omnigent_agent_id="agent",
        omnigent_project_id="project",
    )
    factory = b"""version: 1
concurrency: {max_building: 2, max_open_bot_prs: 5}
checkpoints:
  block_hours: {S: 3, M: 5, L: 7}
  grace_minutes: 19
  cost_backstop_usd_per_hour: 41
review:
  bot_login: molly-omnigent-factory[bot]
  approver_ids: [114979]
  independent_reviewer_ids: [999]
guidance: {triage: Triage safely., engineering: Build safely.}
"""

    token_calls = 0

    def github(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        assert (config.state_dir / "daemon.lock").is_file()
        if request.url.path.endswith("/access_tokens"):
            token_calls += 1
            requested = __import__("json").loads(request.content)
            return httpx.Response(
                201,
                json={
                    "token": "token",
                    "expires_at": "2030-01-01T00:00:00Z",
                    "permissions": requested["permissions"],
                    "repositories": [{"full_name": config.repository}],
                },
            )
        if request.url.path == f"/repos/{config.repository}":
            return httpx.Response(200, json={"default_branch": "main"})
        if request.url.path.endswith("/.github/factory.yml"):
            return httpx.Response(
                200,
                json={"encoding": "base64", "content": base64.b64encode(factory).decode()},
            )
        return httpx.Response(500)

    production = await build_production(
        config,
        github_transport=httpx.MockTransport(github),
        omnigent_transport=httpx.MockTransport(lambda _: httpx.Response(500)),
    )
    started = False
    try:
        adapters = production.service.executor._adapters
        externally_handled = set(EffectKind) - {
            EffectKind.ARM_TIMER,
            EffectKind.WAKE_SCHEDULER,
        }
        assert externally_handled <= adapters.keys()
        assert production.service.delivery_processor is not None
        assert production.verifier.secret_file == config.resolved_webhook_secret_file
        assert production.service.config.max_building == 1
        assert production.service.config.checkpoint_block_hours == {"S": 3, "M": 5, "L": 7}
        assert production.service.config.independent_reviewer_ids == frozenset({999})
        assert production.service.config.engineering_guidance == "Build safely."
        assert token_calls == 1
        await production.service.start()
        started = True
        assert (config.wrapper_bin_dir / "git-credential-omnigent-factory").is_file()
        assert (config.wrapper_bin_dir / "gh").is_file()
        assert config.broker_socket.is_socket()
        assert await production.service.health() == {"status": "ok", "paused": True}
    finally:
        if started:
            await production.service.stop()
        else:
            for managed in production.service._managed:
                await managed.close()
    assert not config.broker_socket.exists()
