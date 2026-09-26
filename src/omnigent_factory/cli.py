"""``omnigent-factory`` service and operator command line."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import uvicorn

from omnigent_factory import __version__
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.service.app import create_app
from omnigent_factory.service.config import ServiceConfig, load_config
from omnigent_factory.service.interfaces import WebhookRejected
from omnigent_factory.service.operator import operator_request
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.service.setup import OperationsRenderer, write_rendered
from omnigent_factory.store.sqlite import DeliveryRecord, SqliteStore

DEFAULT_CONFIG = Path.home() / ".config/omnigent-factory/config.toml"


class IntegrationPendingVerifier:
    """Fail closed until Task 5 wires the Task-2 signed-delivery adapter."""

    async def verify(self, body: bytes, headers: Mapping[str, str]) -> DeliveryRecord:
        del body, headers
        raise WebhookRejected("GitHub verifier is not wired")


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

    setup = sub.add_parser("setup", help="render or validate owner-applied setup artifacts")
    setup_sub = setup.add_subparsers(dest="setup_command", required=True)
    render = setup_sub.add_parser("render")
    _config_arg(render)
    render.add_argument("--output", type=Path)
    validate = setup_sub.add_parser("validate")
    _config_arg(validate)
    return parser


def _config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)


def _load(args: argparse.Namespace) -> tuple[Path, ServiceConfig]:
    path = args.config.expanduser().resolve()
    return path, load_config(path)


def _operator(config: ServiceConfig, command: str, args: Mapping[str, object] | None = None) -> int:
    try:
        result = asyncio.run(operator_request(config.operator_socket, command, args))
    except (ConnectionError, FileNotFoundError) as exc:
        print(f"operator socket unavailable: {exc}", file=sys.stderr)
        return 2
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
        service = FactoryService(config, fatal_exit=os._exit)
        app = create_app(service, IntegrationPendingVerifier())
        uvicorn.run(
            app,
            host=config.bind_host,
            port=config.bind_port,
            workers=1,
            access_log=False,
        )
        return 0
    if args.command in ("status", "doctor", "pause", "unpause", "recovery"):
        return _operator(config, args.command)
    if args.command == "explain":
        return _operator(config, "explain", {"parcel": args.parcel})
    if args.command == "release-delivery":
        return _operator(config, "release-delivery", {"delivery": args.delivery})
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


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
