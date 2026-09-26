"""Persistent service-owned scope for operator-releasable parked deliveries."""

from __future__ import annotations

import json
import os
from pathlib import Path


class ParkedDeliveryRegistry:
    """Persist only delivery and parcel identifiers; webhook bodies never enter this file."""

    def __init__(self, state_dir: Path) -> None:
        self.path = state_dir / "parked-deliveries.json"
        self._items: dict[str, str | None] = {}

    def load(self) -> None:
        if not self.path.exists():
            self._items = {}
            return
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or any(
            not isinstance(guid, str) or (scope is not None and not isinstance(scope, str))
            for guid, scope in raw.items()
        ):
            raise RuntimeError("invalid parked delivery registry")
        self._items = dict(raw)

    def contains(self, delivery_guid: str) -> bool:
        return delivery_guid in self._items

    def park(self, delivery_guid: str, parcel_id: str | None) -> None:
        previous = self._items.get(delivery_guid)
        existed = delivery_guid in self._items
        self._items[delivery_guid] = parcel_id
        try:
            self._save()
        except BaseException:
            if existed:
                self._items[delivery_guid] = previous
            else:
                del self._items[delivery_guid]
            raise

    def release(self, delivery_guid: str) -> None:
        if delivery_guid not in self._items:
            raise ValueError("delivery is not parked")
        scope = self._items.pop(delivery_guid)
        try:
            self._save()
        except BaseException:
            self._items[delivery_guid] = scope
            raise

    def blocks(self, parcel_id: str | None) -> bool:
        return any(scope is None or scope == parcel_id for scope in self._items.values())

    def records(self) -> tuple[tuple[str, str | None], ...]:
        return tuple(sorted(self._items.items()))

    def _save(self) -> None:
        temporary = self.path.with_suffix(".json.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(self._items, stream, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        os.replace(temporary, self.path)
