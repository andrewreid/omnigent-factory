"""MCP deployment pieces: token file creation and the loopback listener sockets."""

from __future__ import annotations

import socket
import stat

import pytest

from omnigent_factory import cli
from omnigent_factory.service.config import ServiceConfig


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


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
    lan_port, mcp_port = _free_port(), _free_port()
    config = service_config.model_copy(
        update={"bind_host": "127.0.0.2", "bind_port": lan_port, "mcp_port": mcp_port}
    )
    sockets = cli.listener_sockets(config)
    try:
        assert [s.getsockname() for s in sockets] == [
            ("127.0.0.2", lan_port),
            ("127.0.0.1", mcp_port),
        ]
    finally:
        for s in sockets:
            s.close()
    same = service_config.model_copy(update={"bind_port": lan_port, "mcp_port": lan_port})
    sockets = cli.listener_sockets(same)
    try:
        assert [s.getsockname() for s in sockets] == [("127.0.0.1", lan_port)]
    finally:
        for s in sockets:
            s.close()
