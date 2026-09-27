"""Drive one parcel through the real reducer into a real SQLite store (test support)."""

from __future__ import annotations

from typing import Any

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind
from omnigent_factory.core.types import TrustedConfig
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import EventFactory, snapshot

ISSUE = "I_1"


class StoreDriver:
    """Drive one parcel through the real reducer into a real SQLite store.

    Mirrors ``testing.harness.Harness``: daemon board writes are acknowledged by the
    trusted read-after-write observation, as the executor would.
    """

    def __init__(
        self,
        store: SqliteStore,
        trusted: TrustedConfig,
        parcel_id: str = ISSUE,
        *,
        start_us: int,
    ) -> None:
        self.store = store
        self.trusted = trusted
        self.f = EventFactory(parcel_id, start_us=start_us)
        self.effects: list[EffectIntent] = []

    def send(self, body: ev.EventBody, *, ack_moves: bool = True, **kw: Any) -> Any:
        result = self.store.apply_event(self.f.make(body, **kw), self.trusted)
        assert result.accepted, result.reason
        self.effects.extend(result.effects)
        for _ in range(8 if ack_moves else 0):
            parcel = self.store.load_parcel(self.f.parcel_id)
            if parcel is None or not parcel.pending_moves:
                break
            move = parcel.pending_moves[0]
            acked = self.store.apply_event(
                self.f.make(
                    ev.ColumnObserved(stage=move.to_stage, daemon_effect_id=move.effect_id),
                    provenance=ev.Provenance.ADAPTER,
                ),
                self.trusted,
            )
            self.effects.extend(acked.effects)
        return result

    def unpause(self) -> None:
        result = self.store.apply_event(self.f.make(ev.Unpause(), parcel_id=None), self.trusted)
        assert result.accepted, result.reason

    def eligible(self) -> None:
        self.send(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=self.f.now))

    def session(self) -> Any:
        parcel = self.store.load_parcel(self.f.parcel_id)
        assert parcel is not None and parcel.current_session is not None
        return parcel.current_session

    def create_ok(self, root: str) -> Any:
        s = self.session()
        self.send(ev.SessionCreated(session_id=s.session_id, root_id=root, nonce=s.nonce))
        self.send(ev.Prepared(session_id=s.session_id, ok=True))
        return self.session()

    def of(self, kind: EffectKind) -> list[EffectIntent]:
        return [e for e in self.effects if e.kind == kind]
