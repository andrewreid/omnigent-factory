from __future__ import annotations

import io
import os
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.doctor import (
    DoctorReport,
    _bundle_mcp_servers,
    _check_agent_factory_tools,
    missing_factory_tools,
    run_doctor,
)
from omnigent_factory.service.mcp import TOOL_NAMES


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
            fields.append(
                {"id": config.note_field_node_id, "name": "Factory note", "dataType": "TEXT"}
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
            "/v1/sessions": {
                "object": "list",
                "data": [
                    {"id": "conv_other", "agent_id": "agent-2"},
                    {"id": "conv_rosie", "agent_id": "agent-1"},
                ],
                "has_more": False,
            },
        }
        if request.url.path == "/v1/sessions/conv_rosie/agent/contents":
            return httpx.Response(200, content=bundle(inline={"factory": {"type": "mcp"}}))
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
    assert report.checks["agent_factory_tools"] == "the agent can call all 8 factory tools"
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


def _live_rest(handler) -> OmnigentRest:
    return OmnigentRest("https://omnigent.test", transport=httpx.MockTransport(handler))


def _resolved_report():
    report = DoctorReport()
    report.resolved.update(
        omnigent_agent_id="agent-1", omnigent_host_id="host-1", omnigent_project_id="project-1"
    )
    return report


@pytest.mark.asyncio
async def test_doctor_live_creates_and_archives_a_throwaway_session(tmp_path: Path):
    from omnigent_factory.service.doctor import _check_live_session

    config = ServiceConfig(repo_id="R", owners=frozenset({1}), source_clone=tmp_path)
    calls: list[tuple[str, str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = __import__("json").loads(request.content)
        calls.append((request.method, request.url.path, body))
        if request.method == "POST":
            return httpx.Response(201, json={"id": "conv_probe"})
        return httpx.Response(200, json={"id": "conv_probe", "archived": True})

    report = _resolved_report()
    rest = _live_rest(handler)
    await _check_live_session(config, rest, report)
    await rest.aclose()
    assert report.ok and report.checks["live_session"] == "created and archived conv_probe"
    (m1, p1, create), (m2, p2, patch) = calls
    assert (m1, p1, m2, p2) == ("POST", "/v1/sessions", "PATCH", "/v1/sessions/conv_probe")
    assert create["agent_id"] == "agent-1" and create["project_id"] == "project-1"
    assert create["initial_items"] == [] and patch == {"archived": True}


@pytest.mark.asyncio
async def test_doctor_live_reports_a_refused_create(tmp_path: Path):
    """E.g. the server cannot resolve an env variable the agent config references."""
    from omnigent_factory.service.doctor import _check_live_session

    config = ServiceConfig(repo_id="R", owners=frozenset({1}), source_clone=tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"  # nothing to archive
        return httpx.Response(
            422,
            json={"error": {"code": "unresolved_env", "message": "FACTORY_MCP_TOKEN unset"}},
        )

    report = _resolved_report()
    rest = _live_rest(handler)
    await _check_live_session(config, rest, report)
    await rest.aclose()
    assert not report.ok and "HTTP 422 unresolved_env" in report.errors[0]


@pytest.mark.asyncio
async def test_doctor_is_not_live_by_default():
    import inspect

    from omnigent_factory.service.doctor import run_doctor as doctor

    assert inspect.signature(doctor).parameters["live"].default is False


# ------------------------------------------------------------------ agent factory tools


def bundle(
    *, inline: dict[str, Any] | None = None, sidecars: dict[str, dict[str, Any]] | None = None
) -> bytes:
    """An agent bundle ``.tar.gz``: ``config.yaml`` (inline ``tools:``) and sidecars."""
    files = {
        "./config.yaml": {"spec_version": 1, "name": "rosie", "tools": inline or {}},
        **{f"tools/mcp/{name}.yaml": body for name, body in (sidecars or {}).items()},
    }
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        for name, body in files.items():
            data = yaml.safe_dump(body).encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


ROSIE_ALLOWLIST = [
    "factory_get_issue",
    "factory_get_plan",
    "factory_get_feedback",
    "factory_get_status",
    "factory_ask_owner",
    "factory_submit_result",
]


def test_missing_factory_tools_compares_the_allowlist_with_the_served_tools():
    url = "${FACTORY_MCP_URL}"
    # Rosie's allowlist before the fix: ranking and related-issue reads were invisible.
    assert missing_factory_tools([("factory", url, ROSIE_ALLOWLIST)], 8787) == [
        "factory_list_issues",
        "factory_submit_ranking",
    ]
    assert missing_factory_tools([("factory", url, None)], 8787) == []  # no allowlist: all
    assert missing_factory_tools([("factory", url, list(TOOL_NAMES))], 8787) == []
    # Prefixed names count; the server is found by its loopback URL under another name.
    prefixed = [f"mcp__factory__{t}" for t in TOOL_NAMES[:-1]]
    assert missing_factory_tools([("fac", "http://127.0.0.1:9999/mcp/", prefixed)], 9999) == [
        "factory_submit_ranking"
    ]
    # Other servers' allowlists never count; no factory server at all is None.
    others = [("github", "https://api.example/mcp", list(TOOL_NAMES))]
    assert missing_factory_tools(others, 8787) is None
    assert missing_factory_tools([], 8787) is None


def test_bundle_mcp_servers_reads_inline_and_sidecar_declarations():
    data = bundle(
        inline={
            "factory": {"type": "mcp", "url": "${FACTORY_MCP_URL}", "tools": ROSIE_ALLOWLIST},
            "web": {"type": "builtin"},
        },
        sidecars={"gh": {"name": "github", "transport": "http", "url": "https://x", "tools": []}},
    )
    assert sorted(_bundle_mcp_servers(data)) == [
        ("factory", "${FACTORY_MCP_URL}", ROSIE_ALLOWLIST),
        ("github", "https://x", None),
    ]


def _agent_rest(contents: httpx.Response | None, sessions: list[dict[str, Any]]) -> OmnigentRest:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"data": sessions, "has_more": False})
        assert request.url.path == "/v1/sessions/conv_1/agent/contents"
        assert contents is not None
        return contents

    return _live_rest(handler)


@pytest.mark.asyncio
async def test_doctor_names_factory_tools_the_agent_cannot_call():
    config = ServiceConfig(repo_id="R", owners=frozenset({1}))
    inline = {"factory": {"type": "mcp", "url": "${FACTORY_MCP_URL}", "tools": ROSIE_ALLOWLIST}}
    rest = _agent_rest(
        httpx.Response(200, content=bundle(inline=inline)),
        [{"id": "conv_1", "agent_id": "agent-1"}],
    )
    report = _resolved_report()
    await _check_agent_factory_tools(config, rest, report)
    await rest.aclose()
    assert not report.ok
    assert report.errors == [
        "agent_factory_tools: the agent's factory MCP allowlist lacks factory_list_issues, "
        "factory_submit_ranking (add them to the agent's `tools:` list for the factory server)"
    ]
    # No factory server declared at all: also an error.
    rest = _agent_rest(httpx.Response(200, content=bundle()), [])
    report = _resolved_report()
    await _check_agent_factory_tools(config, rest, report, session_id="conv_1")
    await rest.aclose()
    assert not report.ok and "declares no factory MCP server" in report.errors[0]


@pytest.mark.asyncio
async def test_doctor_warns_when_the_agent_bundle_cannot_be_read():
    config = ServiceConfig(repo_id="R", owners=frozenset({1}))
    for rest, expected in (
        (_agent_rest(None, [{"id": "conv_x", "agent_id": "agent-2"}]), "no session"),
        (
            _agent_rest(httpx.Response(404), [{"id": "conv_1", "agent_id": "agent-1"}]),
            "HTTP 404",
        ),
        (
            _agent_rest(
                httpx.Response(200, content=b"not a tarball"),
                [{"id": "conv_1", "agent_id": "agent-1"}],
            ),
            "ReadError",
        ),
    ):
        report = _resolved_report()
        await _check_agent_factory_tools(config, rest, report)
        await rest.aclose()
        assert report.ok, report.errors
        [warning] = report.warnings
        assert warning.startswith("agent_factory_tools: agent bundle unreadable") and (
            expected in warning
        ), warning
