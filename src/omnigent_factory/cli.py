"""``omnigent-factory`` service and operator command line."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import socket
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

import uvicorn

from omnigent_factory import __version__
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.service.app import create_app
from omnigent_factory.service.composition import build_production
from omnigent_factory.service.config import ServiceConfig, load_config
from omnigent_factory.service.doctor import run_doctor
from omnigent_factory.service.operator import operator_request
from omnigent_factory.service.redaction import configure_logging
from omnigent_factory.service.setup import OperationsRenderer, write_rendered
from omnigent_factory.store.sqlite import SqliteStore

DEFAULT_CONFIG = Path.home() / ".config/omnigent-factory/config.toml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="omnigent-factory")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the package version")

    db = sub.add_parser("db", help="state database maintenance")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    init = db_sub.add_parser("init", help="create or migrate the SQLite state database")
    init.add_argument("path")

    serve = sub.add_parser("serve", help="run the single-process daemon")
    _config_arg(serve)
    for name in ("status", "doctor", "pause", "unpause", "recovery"):
        command = sub.add_parser(name)
        _config_arg(command)
    explain = sub.add_parser("explain", help="explain persisted state for a parcel")
    explain.add_argument("parcel")
    _config_arg(explain)
    release = sub.add_parser(
        "release-delivery", help="explicitly release one parked webhook delivery"
    )
    release.add_argument("delivery")
    _config_arg(release)
    retry = sub.add_parser(
        "retry-effect",
        help="requeue a failed/unknown triage/report/status publication (adopts, no duplicate)",
    )
    retry.add_argument("effect")
    _config_arg(retry)
    resume = sub.add_parser(
        "resume",
        help="re-open a parcel's existing stage session and send it one operator note",
    )
    resume.add_argument("parcel")
    note = resume.add_mutually_exclusive_group(required=True)
    note.add_argument("--message")
    note.add_argument("--message-file", type=Path)
    _config_arg(resume)
    cleanup = sub.add_parser(
        "cleanup",
        help="remove a finished parcel's factory worktree and local branch (factory clone only)",
    )
    cleanup.add_argument("parcel")
    cleanup.add_argument("--merged", action="store_true", help="the parcel's PR is merged")
    _config_arg(cleanup)
    rerender = sub.add_parser(
        "rerender-comment",
        help="re-render a published comment in place, found by its effect marker",
    )
    rerender.add_argument("effect")
    _config_arg(rerender)

    setup = sub.add_parser("setup", help="render or validate owner-applied setup artifacts")
    setup_sub = setup.add_subparsers(dest="setup_command", required=True)
    render = setup_sub.add_parser("render")
    _config_arg(render)
    render.add_argument("--output", type=Path)
    validate = setup_sub.add_parser("validate")
    _config_arg(validate)
    token = setup_sub.add_parser(
        "mcp-token", help="create the factory MCP bearer token file if absent (mode 0600)"
    )
    _config_arg(token)
    return parser


def _config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)


def _load(args: argparse.Namespace) -> tuple[Path, ServiceConfig]:
    path = args.config.expanduser().resolve()
    return path, load_config(path)


def _operator(
    config: ServiceConfig,
    command: str,
    args: Mapping[str, object] | None = None,
    *,
    startup_wait_seconds: float = 90.0,
) -> int:
    # Re-rendering reads, edits and re-verifies a GitHub comment: allow more time.
    timeout = 60.0 if command in ("rerender-comment", "cleanup") else 5.0
    deadline = time.monotonic() + startup_wait_seconds
    waiting = False
    while True:
        try:
            result = asyncio.run(
                operator_request(config.operator_socket, command, args, timeout_seconds=timeout)
            )
            break
        except (ConnectionError, FileNotFoundError) as exc:
            # The socket opens once startup reconcile finishes (tens of seconds after a
            # restart with live sessions): wait for it.
            if time.monotonic() >= deadline:
                print(f"operator socket unavailable: {exc}", file=sys.stderr)
                return 2
            if not waiting:
                print("waiting for the daemon to finish starting...", file=sys.stderr)
                waiting = True
            time.sleep(0.5)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok", False) else 1


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "db" and args.db_command == "init":
        store = SqliteStore.open(args.path, SystemClock())
        try:
            print(f"schema version {store.schema_version()}")
        finally:
            store.close()
        return 0
    config_path, config = _load(args)
    if args.command == "serve":
        asyncio.run(_serve(config))
        return 0
    if args.command == "doctor":
        report = asyncio.run(run_doctor(config))
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
        return 0 if report.ok else 1
    if args.command in ("status", "pause", "unpause", "recovery"):
        return _operator(config, args.command)
    if args.command == "explain":
        return _operator(config, "explain", {"parcel": args.parcel})
    if args.command == "release-delivery":
        return _operator(config, "release-delivery", {"delivery": args.delivery})
    if args.command == "resume":
        message = (
            args.message
            if args.message is not None
            else args.message_file.read_text(encoding="utf-8")
        )
        return _operator(config, "resume", {"parcel": args.parcel, "message": message})
    if args.command == "cleanup":
        return _operator(config, "cleanup", {"parcel": args.parcel, "merged": args.merged})
    if args.command == "rerender-comment":
        return _operator(config, "rerender-comment", {"effect": args.effect})
    if args.command == "retry-effect":
        return _operator(config, "retry-effect", {"effect": args.effect})
    if args.command == "setup" and args.setup_command == "mcp-token":
        path, created = ensure_mcp_token(config)
        print(f"{'created' if created else 'exists'}: {path}")
        return 0
    renderer = OperationsRenderer(config_path)
    if args.command == "setup" and args.setup_command == "validate":
        errors = renderer.validate(config)
        if errors:
            for error in errors:
                print(error, file=sys.stderr)
            return 1
        print("setup configuration valid")
        return 0
    if args.command == "setup" and args.setup_command == "render":
        artifacts = renderer.render(config)
        if args.output is not None:
            write_rendered(artifacts, args.output)
            for name in sorted(artifacts):
                print(args.output / name)
        else:
            for name, content in artifacts.items():
                print(f"--- {name} ---")
                print(content, end="" if content.endswith("\n") else "\n")
        return 0
    raise AssertionError("unhandled command")


async def _serve(config: ServiceConfig) -> None:
    """Build and run the complete daemon on one asyncio event loop."""
    configure_logging()
    production = await build_production(config, fatal_exit=os._exit)
    app = create_app(production.service, production.verifier, production.mcp)
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=config.bind_host,
            port=config.bind_port,
            workers=1,
            access_log=False,
        )
    )
    sockets = listener_sockets(config)
    try:
        await server.serve(sockets=sockets)
    finally:
        # Lifespan normally owns shutdown; this also releases a pre-acquired lock if
        # uvicorn fails before entering lifespan.
        if production.service.ready or production.service._tasks:
            await production.service.stop()
        production.service.process_lock.close()


def listener_sockets(config: ServiceConfig) -> list[socket.socket]:
    """The configured (LAN) listener plus the loopback-only MCP listener.

    One socket when both are the same address. ``/mcp`` is refused on every listener
    except loopback (``McpGate``); only ``/webhooks/github`` is forwarded by ingress.
    """
    wanted = [(config.bind_host, config.bind_port)]
    if (MCP_HOST, config.mcp_port) != (config.bind_host, config.bind_port):
        wanted.append((MCP_HOST, config.mcp_port))
    sockets: list[socket.socket] = []
    try:
        for host, port in wanted:
            family = socket.AF_INET6 if ":" in host.strip("[]") else socket.AF_INET
            sock = socket.socket(family, socket.SOCK_STREAM)
            sockets.append(sock)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host.strip("[]"), port))
            sock.set_inheritable(True)
    except OSError:
        for sock in sockets:
            sock.close()
        raise
    return sockets


def ensure_mcp_token(config: ServiceConfig) -> tuple[Path, bool]:
    """Create ``<secrets>/mcp-token`` (0600, owner-only) unless present. Never printed."""
    path = config.resolved_mcp_token_file
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return path, False
    try:
        os.write(fd, (secrets.token_urlsafe(32) + "\n").encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)
    return path, True


MCP_HOST = "127.0.0.1"


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
