"""MCP deployment pieces: token file creation and the loopback listener sockets."""

from __future__ import annotations

import stat

import pytest

from omnigent_factory import cli
from omnigent_factory.service.config import ServiceConfig


def test_setup_mcp_token_creates_a_private_token_once(
    service_config: ServiceConfig, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setattr(cli, "_load", lambda _args: (None, service_config))
    assert cli.main(["setup", "mcp-token"]) == 0
    path = service_config.resolved_mcp_token_file
    token = path.read_text().strip()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600 and len(token) >= 40
    out = capsys.readouterr().out
    assert out.startswith("created: ") and token not in out  # never printed
    assert cli.main(["setup", "mcp-token"]) == 0
    assert path.read_text().strip() == token  # never rotated silently
    assert capsys.readouterr().out.startswith("exists: ")
    service_config.validate_paths()  # the secrets dir stays valid (0600 file)
    assert service_config.validate_paths() == []


def test_loopback_mcp_listener_is_added_beside_the_lan_listener(
    service_config: ServiceConfig,
) -> None:
    config = service_config.model_copy(
        update={"bind_host": "127.0.0.2", "bind_port": 0, "mcp_port": 0}
    )
    sockets = cli.listener_sockets(config)
    try:
        addresses = [s.getsockname() for s in sockets]
        assert [address[0] for address in addresses] == ["127.0.0.2", "127.0.0.1"]
        assert all(address[1] > 0 for address in addresses)
        assert addresses[0][1] != addresses[1][1]
    finally:
        for s in sockets:
            s.close()
    same = service_config.model_copy(update={"bind_port": 0, "mcp_port": 0})
    sockets = cli.listener_sockets(same)
    try:
        [address] = [s.getsockname() for s in sockets]
        assert address[0] == "127.0.0.1" and address[1] > 0
    finally:
        for s in sockets:
            s.close()
