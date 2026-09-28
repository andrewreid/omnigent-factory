from __future__ import annotations

import base64
import os
import subprocess
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.doctor import run_doctor


def _secret(path: Path, value: bytes) -> None:
    path.write_bytes(value)
    os.chmod(path, 0o600)


@pytest.mark.asyncio
async def test_doctor_resolves_live_ids_and_does_not_create_local_state(tmp_path: Path):
    secrets = tmp_path / "secrets"
    secrets.mkdir(mode=0o700)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    _secret(secrets / "app.pem", pem)
    _secret(secrets / "webhook_secret", b"webhook")
    _secret(secrets / "omnigent-token", b"owner-token")
    _secret(secrets / "mcp-token", b"m" * 43)

    clone = tmp_path / "clone"
    subprocess.run(("git", "init", str(clone)), check=True, capture_output=True)  # noqa: S603,S607
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
        secrets_dir=secrets,
        app_env=tmp_path / "absent.env",
        repo_id="R_NODE",
        repository_database_id=123,
        organization_id=456,
        owners=frozenset({114979}),
        source_clone=clone,
        real_gh_path=Path("/bin/true"),
        omnigent_host_id=None,
        omnigent_agent_id=None,
        omnigent_project_id=None,
    )
    factory_yml = b"""version: 1
concurrency: {max_building: 1, max_open_bot_prs: 3}
checkpoints:
  block_hours: {S: 2, M: 4, L: 6}
  grace_minutes: 15
  cost_backstop_usd_per_hour: 35
review:
  bot_login: molly-omnigent-factory[bot]
  approver_ids: [114979]
  independent_reviewer_ids: []
guidance: {triage: Triage safely., engineering: Build safely.}
"""

    revoked = 0

    def github(request: httpx.Request) -> httpx.Response:
        nonlocal revoked
        if request.url.path.endswith("/access_tokens"):
            requested = __import__("json").loads(request.content)
            return httpx.Response(
                201,
                json={
                    "token": "installation",
                    "expires_at": "2030-01-01T00:00:00Z",
                    "permissions": requested["permissions"],
                    "repositories": [{"full_name": config.repository}],
                },
            )
        if request.method == "DELETE" and request.url.path == "/installation/token":
            revoked += 1
            return httpx.Response(204)
        if request.url.path == "/installation/repositories":
            return httpx.Response(
                200,
                json={
                    "total_count": 1,
                    "repositories": [
                        {
                            "id": 123,
                            "node_id": "R_NODE",
                            "full_name": config.repository,
                            "owner": {"id": 456},
                        }
                    ],
                },
            )
        if request.url.path == f"/repos/{config.repository}":
            return httpx.Response(200, json={"default_branch": "main"})
        if request.url.path.endswith("/.github/factory.yml"):
            return httpx.Response(
                200,
                json={
                    "encoding": "base64",
                    "content": base64.b64encode(factory_yml).decode(),
                },
            )
        if request.url.path == "/graphql":
            fields = []
            for name, field_id, options in (
                ("Status", config.status_field_node_id, config.status_options),
                ("Bot", config.bot_field_node_id, config.bot_options),
            ):
                fields.append(
                    {
                        "id": field_id,
                        "name": name,
                        "options": [
                            {"name": option_name, "id": option_id}
                            for option_name, option_id in options.items()
                        ],
                    }
                )
            return httpx.Response(
                200,
                json={
                    "data": {"node": {"id": config.project_node_id, "fields": {"nodes": fields}}}
                },
            )
        return httpx.Response(404)

    def omnigent(request: httpx.Request) -> httpx.Response:
        # Shapes observed on the live server (2026-09-27): hosts is a bare
        # ``{hosts: [...]}`` keyed by ``host_id``; projects has no ``has_more``.
        values = {
            "/v1/hosts": {"hosts": [{"host_id": "host-1", "name": "coder", "status": "online"}]},
            "/v1/agents": {
                "object": "list",
                "data": [{"id": "agent-1", "name": "Rosie"}],
                "has_more": False,
            },
            "/v1/projects": {"object": "list", "data": [{"id": "project-1", "name": "Timesheets"}]},
            "/api/version": {"version": "0.0.0-other"},  # drift is a warning, never a failure
        }
        return httpx.Response(200, json=values[request.url.path])

    before = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}
    mcp_calls: list[str] = []

    def mcp(request: httpx.Request) -> httpx.Response:
        mcp_calls.append(str(request.url))
        assert "authorization" not in request.headers  # the probe never sends the token
        return httpx.Response(401)

    report = await run_doctor(
        config,
        github_transport=httpx.MockTransport(github),
        omnigent_transport=httpx.MockTransport(omnigent),
        mcp_transport=httpx.MockTransport(mcp),
    )
    after = {path.relative_to(tmp_path) for path in tmp_path.rglob("*")}

    assert report.ok, report.errors
    assert mcp_calls == [f"http://127.0.0.1:{config.mcp_port}/mcp/"]
    assert report.checks["mcp_listener"].endswith("requires the bearer token")
    assert report.checks["mcp_token"].endswith("is private (mode 0600)")
    assert report.checks["caller_policy"].startswith("compiles")
    assert report.resolved["omnigent_agent_id"] == "agent-1"
    assert any(
        w.startswith("omnigent_version: server 0.0.0-other differs") for w in report.warnings
    )
    assert report.resolved["omnigent_host_id"] == "host-1"
    assert report.resolved["omnigent_project_id"] == "project-1"
    assert report.resolved["repository_database_id"] == 123
    assert "github_token_revoke" in report.checks
    assert revoked == 1
    assert before == after


@pytest.mark.asyncio
async def test_doctor_mcp_checks_fail_without_token_and_on_an_open_endpoint(tmp_path: Path):
    from omnigent_factory.service.doctor import (
        DoctorReport,
        _check_caller_policy,
        _check_mcp_listener,
        _check_mcp_token,
    )

    secrets = tmp_path / "secrets"
    secrets.mkdir(mode=0o700)
    config = ServiceConfig(repo_id="R", owners=frozenset({1}), secrets_dir=secrets)
    report = DoctorReport()
    _check_mcp_token(config, report)
    assert not report.ok and "setup mcp-token" in report.errors[0]
    (secrets / "mcp-token").write_text("x" * 40)
    os.chmod(secrets / "mcp-token", 0o644)
    report = DoctorReport()
    _check_mcp_token(config, report)
    assert not report.ok and "0600" in report.errors[0]

    report = DoctorReport()
    await _check_mcp_listener(
        config, report, transport=httpx.MockTransport(lambda _r: httpx.Response(200))
    )
    assert not report.ok  # an MCP endpoint answering without a token is a failure

    report = DoctorReport()
    _check_caller_policy(report)
    assert report.ok, report.errors
