"""Private newline-delimited JSON control protocol over a Unix socket."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

OperatorHandler = Callable[[str, Mapping[str, object]], Awaitable[Mapping[str, object]]]


class OperatorServer:
    def __init__(self, path: Path, handler: OperatorHandler, *, timeout_seconds: float = 5) -> None:
        self.path = path
        self._handler = handler
        self._timeout_seconds = timeout_seconds
        self._server: asyncio.Server | None = None
        self._clients: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        if self.path.exists() or self.path.is_symlink():
            self.path.unlink()
        self._server = await asyncio.start_unix_server(self._client, path=self.path)
        os.chmod(self.path, 0o600)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            close_clients = getattr(self._server, "close_clients", None)
            if close_clients is not None:
                close_clients()
            for writer in tuple(self._clients):
                writer.close()
                transport = writer.transport
                if not transport.is_closing():
                    transport.abort()
            await self._server.wait_closed()
            self._server = None
        if self.path.exists() or self.path.is_symlink():
            self.path.unlink()

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._clients.add(writer)
        try:
            raw = await asyncio.wait_for(reader.readline(), self._timeout_seconds)
            if len(raw) > 64 * 1024:
                raise ValueError("request too large")
            request = json.loads(raw)
            if not isinstance(request, dict) or not isinstance(request.get("command"), str):
                raise ValueError("invalid request")
            args = request.get("args", {})
            if not isinstance(args, dict):
                raise ValueError("invalid args")
            response = dict(await self._handler(request["command"], args))
            response.setdefault("ok", True)
        except Exception as exc:
            response = {"ok": False, "error": type(exc).__name__}
        try:
            writer.write(json.dumps(response, sort_keys=True).encode() + b"\n")
            await writer.drain()
        except (ConnectionError, RuntimeError):
            pass
        finally:
            self._clients.discard(writer)
            writer.close()
            await writer.wait_closed()


async def operator_request(
    path: Path,
    command: str,
    args: Mapping[str, object] | None = None,
    *,
    timeout_seconds: float = 5,
) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(path)
    try:
        writer.write(
            json.dumps({"command": command, "args": dict(args or {})}, sort_keys=True).encode()
            + b"\n"
        )
        await writer.drain()
        raw = await asyncio.wait_for(reader.readline(), timeout_seconds)
        response = json.loads(raw)
        if not isinstance(response, dict):
            raise RuntimeError("invalid operator response")
        return response
    finally:
        writer.close()
        await writer.wait_closed()
