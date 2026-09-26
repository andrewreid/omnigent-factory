"""``omnigent-factory`` console entry point.

Task 1 ships a minimal CLI: ``version`` and ``db init`` (create/migrate a state database).
Later tasks add ``serve``, ``status``, ``doctor``, ``pause``/``unpause``, ``explain`` and
setup commands; operator commands must call the application service, never edit SQL.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from omnigent_factory import __version__
from omnigent_factory.ports.clock import SystemClock
from omnigent_factory.store.sqlite import SqliteStore


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="omnigent-factory")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the package version")
    db = sub.add_parser("db", help="state database maintenance")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    init = db_sub.add_parser("init", help="create or migrate the SQLite state database")
    init.add_argument("path")
    args = parser.parse_args(argv)
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
    parser.error("unknown command")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
