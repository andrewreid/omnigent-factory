"""The board diff corrects Bot / "Factory note" drift with no extra read.

The factory writes Bot and Factory note only when their derived value changes. A write
lost, cancelled or overwritten by hand left the card showing a wrong value for good. The
periodic board diff now reads both fields in its existing query and rewrites a card whose
value differs from the one the factory last wrote, once, never while a write of either
field is in flight or settled less than ``DRIFT_SETTLE_US`` ago.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.types import BotState
from omnigent_factory.github.adapter import BoardSchema
from omnigent_factory.ports.github import BoardCard
from omnigent_factory.service.board_diff import DRIFT_SETTLE_US, BoardDiff
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.executor import ParcelSerializers
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import OWNER_ID, REPO_ID, EventFactory, snapshot
from omnigent_factory.testing.fakes import FakeClock
from tests.github.test_adapter import PROJECT, REPO_NODE, STATUS_FIELD, adapter

PARCEL = "I_kwDOTC12Fs8AAAABWApPaQ"
ISSUE = 891
MINUTE = 60 * 1_000_000


@dataclass
class _DB:
    store: SqliteStore

    async def call(self, fn: Callable[[SqliteStore], Any]) -> Any:
        return fn(self.store)


@dataclass
class _Service:
    """What :class:`BoardDiff` uses of the service (real store, real reducer)."""

    config: ServiceConfig
    clock: FakeClock
    db: _DB
    serializers: ParcelSerializers = field(default_factory=ParcelSerializers)


@dataclass
class Board:
    cards: list[BoardCard] = field(default_factory=list)
    reads: int = 0

    async def read(self) -> list[BoardCard]:
        self.reads += 1
        return list(self.cards)


def card(bot: str | None = "Idle", note: str | None = "", node_id: str = PARCEL) -> BoardCard:
    return BoardCard(
        node_id,
        ISSUE,
        True,
        "opt",
        "bot-opt",
        (),
        (),
        "T",
        "2026-10-15T04:00:00Z",
        bot=bot,
        note=note,
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "factory.db"


@pytest.fixture
def store(db_path: Path, clock: FakeClock) -> Iterator[SqliteStore]:
    s = SqliteStore.open(db_path, clock)
    s.ensure_repository(_config(db_path).trusted)
    factory = EventFactory(PARCEL, issue_number=ISSUE, start_us=clock.now_utc_us())
    s.apply_event(
        factory.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=factory.now)),
        _config(db_path).trusted,
    )
    # Whatever the seed issued has long settled.
    s._conn.execute("UPDATE effects SET state = 'done', updated_at_us = 0")
    clock.advance(10 * MINUTE)
    try:
        yield s
    finally:
        s.close()


def _config(db_path: Path) -> ServiceConfig:
    state = db_path.parent / "state"
    secrets = db_path.parent / "secrets"
    for d in (state, secrets):
        d.mkdir(mode=0o700, exist_ok=True)
    return ServiceConfig(
        state_dir=state, secrets_dir=secrets, repo_id=REPO_ID, owners=frozenset({OWNER_ID})
    )


def diff(store: SqliteStore, clock: FakeClock, db_path: Path, board: Board) -> BoardDiff:
    svc = _Service(_config(db_path), clock, _DB(store))
    return BoardDiff(svc, board.read)  # type: ignore[arg-type]


def display_effects(store: SqliteStore) -> list[tuple[str, str, str]]:
    return [
        (str(r["kind"]), str(r["state"]), str(r["payload_json"]))
        for r in store.query(
            "SELECT kind, state, payload_json FROM effects WHERE kind IN ('set_bot', 'set_note') "
            "AND state != 'done' ORDER BY created_at_us"
        )
    ]


@pytest.mark.asyncio
async def test_bot_drift_in_the_board_diff_gets_one_corrective_write(store, clock, db_path, caplog):
    board = Board([card(bot="Blocked")])  # the factory last wrote Idle
    parcel = store.load_parcel(PARCEL)
    assert parcel is not None and parcel.bot == BotState.IDLE
    caplog.set_level(logging.INFO, logger="omnigent_factory.service.board_diff")
    await diff(store, clock, db_path, board).run_once()
    effects = display_effects(store)
    assert len(effects) == 1
    kind, state, payload = effects[0]
    assert (kind, state) == ("set_bot", "pending") and '"bot": "Idle"' in payload.replace(
        '":"', '": "'
    )
    lines = [r.getMessage() for r in caplog.records if "board drift corrected" in r.getMessage()]
    assert lines == [
        f"board drift corrected issue=#{ISSUE} field=bot seen='Blocked' expected='Idle'"
    ]
    assert board.reads == 1  # no per-issue read, no extra request
    # The next diff: the write is still pending, so nothing more.
    await diff(store, clock, db_path, board).run_once()
    assert len(display_effects(store)) == 1
    # Not a command, no transition: the column and holds are what they were.
    after = store.load_parcel(PARCEL)
    assert after is not None
    assert (after.stage, after.holds, after.bot) == (parcel.stage, parcel.holds, parcel.bot)


@pytest.mark.asyncio
async def test_note_drift_is_rewritten(store, clock, db_path):
    await diff(store, clock, db_path, Board([card(note="Required check red: x")])).run_once()
    [(kind, state, _payload)] = display_effects(store)
    assert (kind, state) == ("set_note", "pending")


@pytest.mark.asyncio
async def test_no_write_when_the_values_match(store, clock, db_path):
    version = store.load_parcel(PARCEL).version
    await diff(store, clock, db_path, Board([card()])).run_once()
    assert display_effects(store) == []
    assert store.load_parcel(PARCEL).version == version  # no event at all


@pytest.mark.asyncio
async def test_unread_empty_and_untracked_cards_are_left_alone(store, clock, db_path):
    board = Board([card(bot="", note=None), card(bot="Blocked", node_id="I_untracked")])
    await diff(store, clock, db_path, board).run_once()
    board = Board([card(bot=None, note=None)])
    await diff(store, clock, db_path, board).run_once()
    assert display_effects(store) == []


@pytest.mark.asyncio
async def test_no_write_while_a_recent_or_pending_write_exists(store, clock, db_path):
    board = Board([card(bot="Blocked")])
    first = diff(store, clock, db_path, board)
    await first.run_once()
    assert len(display_effects(store)) == 1
    # The write lands (done) but the board read lags behind it.
    store._conn.execute(
        "UPDATE effects SET state = 'done', updated_at_us = ? WHERE kind = 'set_bot'",
        (clock.now_utc_us(),),
    )
    clock.advance(DRIFT_SETTLE_US // 2)
    await first.run_once()
    assert display_effects(store) == []
    # An unknown outcome is never fought either, however old.
    store._conn.execute("UPDATE effects SET state = 'unknown' WHERE kind = 'set_bot'")
    clock.advance(10 * MINUTE)
    await first.run_once()
    assert [k for k, s, _ in display_effects(store)] == ["set_bot"]  # the unknown row only
    # Settled long ago and the card still shows the wrong value: corrected again.
    store._conn.execute("UPDATE effects SET state = 'done' WHERE kind = 'set_bot'")
    await first.run_once()
    assert [(k, s) for k, s, _ in display_effects(store)] == [("set_bot", "pending")]


@pytest.mark.asyncio
async def test_a_restart_does_not_duplicate_the_write(store, clock, db_path):
    board = Board([card(bot="Blocked")])
    await diff(store, clock, db_path, board).run_once()
    store.close()
    clock.advance(5 * MINUTE)
    reopened = SqliteStore.open(db_path, clock)
    try:
        await diff(reopened, clock, db_path, board).run_once()
        await diff(reopened, clock, db_path, board).run_once()
        assert [k for k, _, _ in display_effects(reopened)] == ["set_bot"]
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_the_board_read_carries_bot_and_note_in_the_same_request():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.content.decode())

        def item(number: int, bot: Any, note: Any) -> dict[str, Any]:
            return {
                "status": {"optionId": "s"},
                "bot": bot,
                "note": note,
                "content": {
                    "__typename": "Issue",
                    "id": f"I_{number}",
                    "number": number,
                    "title": "T",
                    "state": "OPEN",
                    "updatedAt": "2026-10-15T04:00:00Z",
                    "repository": {"id": REPO_NODE},
                    "assignees": {"totalCount": 0, "nodes": []},
                    "labels": {"totalCount": 0, "nodes": []},
                },
            }

        nodes = [
            item(1, {"optionId": "opt_blocked"}, {"text": "Hi", "field": {"id": "NOTE"}}),
            item(2, None, None),
            item(3, {"optionId": "opt_gone"}, {"text": "x", "field": {"id": "OTHER"}}),
        ]
        return httpx.Response(
            200,
            json={
                "data": {
                    "node": {
                        "items": {
                            "nodes": nodes,
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    }
                }
            },
        )

    schema = BoardSchema(
        STATUS_FIELD, {}, "BOT", {"Blocked": "opt_blocked", "Idle": "opt_idle"}, "NOTE"
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        cards = await adapter(http, board_schema=schema).board_cards()
    assert len(calls) == 1 and "Factory note" in calls[0] and PROJECT in calls[0]
    assert isinstance(cards, list)
    assert [(c.bot, c.note) for c in cards] == [("Blocked", "Hi"), ("", ""), (None, None)]
    # The values are not part of the digest: the factory's own writes are no card change.
    plain = card(bot=None, note=None)
    assert plain.digest == card(bot="Blocked", note="x").digest
