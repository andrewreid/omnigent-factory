"""``mcp_allowed_hosts``: extra Host header values for ``/mcp`` (config, transport,
reload and doctor)."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from omnigent_factory.service.config import HOT_RELOAD_KEYS, ServiceConfig, load_config
from omnigent_factory.service.mcp import configured_host_authorities
from omnigent_factory.service.operator import operator_request
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID
from omnigent_factory.testing.fakes import FakeClock
from tests.service.test_mcp_http import HEADERS, _app, _client, _rpc, _token
from tests.service.test_reload import _write

LISTING = _rpc("tools/list")


def _config(**update: Any) -> ServiceConfig:
    return ServiceConfig(repo_id="R", owners=frozenset({1}), **update)


# ------------------------------------------------------------------ config


def test_allowed_hosts_default_empty_and_normalized() -> None:
    assert _config().mcp_allowed_hosts == ()
    config = _config(
        mcp_allowed_hosts=[
            "Coder.Infra.WP.reid.ee",
            "10.0.0.5:9000",
            "[FD00::5]",
            "fd00::6",
            "[fd00::7]:8787",
            "coder.infra.wp.reid.ee",
        ]
    )
    assert config.mcp_allowed_hosts == (
        "coder.infra.wp.reid.ee",
        "10.0.0.5:9000",
        "[fd00::5]",
        "[fd00::6]",
        "[fd00::7]:8787",
    )


@pytest.mark.parametrize(
    "entry",
    [
        "*",
        "*.reid.ee",
        "",
        "  ",
        " coder.lan",
        "http://coder.lan",
        "coder.lan/mcp",
        "user@coder.lan",
        "coder.lan:0",
        "coder.lan:65536",
        "coder.lan:http",
        "coder.lan:",
        "0.0.0.0",  # noqa: S104 - a rejected config value, never bound
        "[::]",
        "[fd00::5",
        "[not-v6]:80",
        "fd00::5:zz",
        "a..b",
        "-coder.lan",
        "1.2.3",
    ],
)
def test_invalid_allowed_hosts_are_rejected(entry: str) -> None:
    with pytest.raises(ValidationError, match="mcp_allowed_hosts"):
        _config(mcp_allowed_hosts=[entry])


def test_invalid_allowed_hosts_fail_config_load(tmp_path: Path) -> None:
    path = tmp_path / "factory.toml"
    base = f'[service]\nrepo_id = "{REPO_ID}"\nowners = [{OWNER_ID}]\nstate_dir = "state"\n'
    path.write_text(base + 'mcp_allowed_hosts = ["coder.lan", "10.0.0.5:8787"]\n')
    assert load_config(path).mcp_allowed_hosts == ("coder.lan", "10.0.0.5:8787")
    path.write_text(base + 'mcp_allowed_hosts = ["*"]\n')
    with pytest.raises(ValidationError, match="mcp_allowed_hosts"):
        load_config(path)


def test_portless_entries_mean_both_listener_ports() -> None:
    config = _config(
        bind_port=8787,
        mcp_port=8788,
        mcp_allowed_hosts=["coder.lan", "10.0.0.5:9000", "[fd00::5]"],
    )
    assert configured_host_authorities(config) == [
        "coder.lan:8787",
        "coder.lan:8788",
        "10.0.0.5:9000",
        "[fd00::5]:8787",
        "[fd00::5]:8788",
    ]
    assert configured_host_authorities(_config(mcp_allowed_hosts=["coder.lan"])) == [
        "coder.lan:8787"
    ]


def test_allowed_hosts_are_restart_only() -> None:
    assert "mcp_allowed_hosts" not in HOT_RELOAD_KEYS


# ------------------------------------------------------------------ transport


def _lan(service_config: ServiceConfig, **update: Any) -> ServiceConfig:
    return service_config.model_copy(
        update={"bind_host": "192.168.1.5", "bind_port": 8787, **update}
    )


def test_configured_hostname_is_accepted_behind_the_bearer(
    service_config: ServiceConfig,
) -> None:
    token = _token(service_config)
    auth = {**HEADERS, "authorization": f"Bearer {token}"}
    config = _lan(service_config, mcp_allowed_hosts=("coder.infra.wp.reid.ee",))
    named = {"host": "coder.infra.wp.reid.ee:8787"}
    app, _service = _app(config)
    with _client(app, server="192.168.1.5", peer="192.168.1.20") as client:
        assert client.post("/mcp/", json=LISTING, headers={**HEADERS, **named}).status_code == 401
        response = client.post("/mcp/", json=LISTING, headers={**auth, **named})
        assert response.status_code == 200, response.text
        origin = {**auth, **named, "origin": "http://coder.infra.wp.reid.ee:8787"}
        assert client.post("/mcp/", json=LISTING, headers=origin).status_code == 200
        # An unlisted name is still refused by the Host check, token or not.
        other = {"host": "evil.example:8787"}
        response = client.post("/mcp/", json=LISTING, headers={**auth, **other})
        assert response.status_code == 421
        assert "Invalid Host header" in response.text
        assert client.post("/mcp/", json=LISTING, headers={**HEADERS, **other}).status_code == 401
        # The default authorities keep working.
        assert client.post("/mcp/", json=LISTING, headers=auth).status_code == 200
        assert client.get("/healthz", headers=named).status_code in (200, 503)


def test_unlisted_hostname_is_refused_by_default(service_config: ServiceConfig) -> None:
    token = _token(service_config)
    auth = {**HEADERS, "authorization": f"Bearer {token}"}
    app, _service = _app(_lan(service_config))
    with _client(app, server="192.168.1.5", peer="192.168.1.20") as client:
        named = {**auth, "host": "coder.infra.wp.reid.ee:8787"}
        assert client.post("/mcp/", json=LISTING, headers=named).status_code == 421


def test_configured_ip_and_ipv6_hosts(service_config: ServiceConfig) -> None:
    token = _token(service_config)
    auth = {**HEADERS, "authorization": f"Bearer {token}"}
    config = _lan(service_config, mcp_allowed_hosts=("10.0.0.5:8787", "[fd00::5]"))
    app, _service = _app(config)
    with _client(app, server="192.168.1.5", peer="192.168.1.20") as client:
        for host, status in (
            ("10.0.0.5:8787", 200),
            ("[fd00::5]:8787", 200),
            ("10.0.0.5:9999", 421),  # an explicit port is the only one allowed
            ("fd00::5:8787", 421),
        ):
            got = client.post("/mcp/", json=LISTING, headers={**auth, "host": host})
            assert got.status_code == status, host


def test_allowed_host_does_not_widen_the_listener_gate(service_config: ServiceConfig) -> None:
    token = _token(service_config)
    auth = {**HEADERS, "authorization": f"Bearer {token}", "host": "coder.lan:8787"}
    config = _lan(service_config, mcp_allowed_hosts=("coder.lan",))
    app, _service = _app(config)
    with _client(app, server="192.168.1.6", peer="127.0.0.1") as client:
        assert client.post("/mcp/", json=LISTING, headers=auth).status_code == 404


# ------------------------------------------------------------------ reload


@pytest.mark.asyncio
async def test_reload_refuses_an_allowed_hosts_change(tmp_path: Path) -> None:
    for name in ("state", "secrets"):
        (tmp_path / name).mkdir(mode=0o700)
        os.chmod(tmp_path / name, 0o700)
    config_file = tmp_path / "config.toml"
    _write(config_file, tmp_path)
    service = FactoryService(load_config(config_file), clock=FakeClock())
    service.config_path = config_file
    await service.start()
    try:
        _write(config_file, tmp_path, mcp_allowed_hosts=["coder.lan"])
        response = await operator_request(service.config.operator_socket, "reload")
        assert response["ok"] is False
        assert response["message"] == "restart required: mcp_allowed_hosts; nothing applied"
        assert service.config.mcp_allowed_hosts == ()
    finally:
        await service.stop()


# ------------------------------------------------------------------ doctor


@pytest.mark.asyncio
async def test_doctor_probes_each_allowed_host_by_name() -> None:
    from omnigent_factory.service.doctor import DoctorReport, _check_mcp_listener

    config = _config(
        bind_host="192.168.1.5",
        bind_port=8787,
        mcp_port=8787,
        mcp_allowed_hosts=["coder.lan", "gone.lan", "10.0.0.5:9000", "open.lan"],
    )
    probes: list[tuple[str, str, bool]] = []
    resolved: list[tuple[str, int]] = []

    def resolve(host: str, port: int) -> object:
        resolved.append((host, port))
        if host == "gone.lan":
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        return [()]

    def mcp(request: httpx.Request) -> httpx.Response:
        probes.append((request.method, str(request.url), "authorization" in request.headers))
        return httpx.Response(200 if request.url.host == "open.lan" else 401)

    report = DoctorReport()
    await _check_mcp_listener(config, report, transport=httpx.MockTransport(mcp), resolve=resolve)
    assert resolved == [
        ("coder.lan", 8787),
        ("gone.lan", 8787),
        ("10.0.0.5", 9000),
        ("open.lan", 8787),
    ]
    assert probes == [
        ("POST", "http://192.168.1.5:8787/mcp/", False),
        ("POST", "http://127.0.0.1:8787/mcp/", False),
        ("POST", "http://coder.lan:8787/mcp/", False),
        ("POST", "http://10.0.0.5:9000/mcp/", False),
        ("POST", "http://open.lan:8787/mcp/", False),
    ]
    assert report.checks["mcp_allowed_host:coder.lan:8787"].endswith("requires the bearer token")
    assert report.checks["mcp_allowed_host:10.0.0.5:9000"].endswith("requires the bearer token")
    assert "does not resolve" in report.checks["mcp_allowed_host:gone.lan:8787"]
    assert any("gone.lan" in warning for warning in report.warnings)
    # An allowed host answering without a token is a failure, like any listener.
    assert report.errors == [
        "mcp_allowed_host:open.lan:8787: "
        "http://open.lan:8787/mcp/ answered HTTP 200 without a token"
    ]


@pytest.mark.asyncio
async def test_doctor_unreachable_allowed_host_is_a_warning() -> None:
    from omnigent_factory.service.doctor import DoctorReport, _check_mcp_listener

    config = _config(mcp_allowed_hosts=["coder.lan"])

    def mcp(request: httpx.Request) -> httpx.Response:
        if request.url.host == "coder.lan":
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(401)

    report = DoctorReport()
    await _check_mcp_listener(
        config, report, transport=httpx.MockTransport(mcp), resolve=lambda _h, _p: [()]
    )
    assert report.ok, report.errors
    assert "unreachable" in report.checks["mcp_allowed_host:coder.lan:8787"]
