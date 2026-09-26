"""Store-backed own-send ledger (Task 5a, T3 FOLLOW_UP): a send intent recorded before
the POST survives a daemon crash, so a lost acknowledgement is still reconciled."""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent_factory.core.effects import Ack, AmbiguousWrite, EffectKind
from omnigent_factory.omnigent.directory import OwnItemLedger, OwnSend
from omnigent_factory.service.db import StoreWorker
from omnigent_factory.service.durable import StoreOwnItemLedger
from omnigent_factory.testing.fakes import FakeClock
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import FakeSession
from tests.omnigent.support import AGENT, CTX, intent, make_rig, spec

pytestmark = pytest.mark.asyncio

ROOT = "conv_root"


async def _worker(path: Path) -> StoreWorker:
    worker = StoreWorker(path, FakeClock())
    await worker.start()
    return worker


async def test_ledger_round_trip_across_reopen(tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    worker = await _worker(db)
    ledger = StoreOwnItemLedger(worker)
    assert isinstance(ledger, OwnItemLedger)
    await ledger.record_intent(OwnSend("e1", "S1", "root", "message", "a" * 64))
    await ledger.record_intent(OwnSend("e2", "S1", "child", "resolve", "", "elic-1"))
    await ledger.record_intent(OwnSend("e1", "S1", "root", "message", "b" * 64))  # replay
    await worker.close()

    worker = await _worker(db)
    ledger = StoreOwnItemLedger(worker)
    assert await ledger.lookup("e1") == OwnSend("e1", "S1", "root", "message", "a" * 64)
    assert await ledger.lookup("e2") == OwnSend("e2", "S1", "child", "resolve", "", "elic-1")
    assert await ledger.lookup("missing") is None
    await ledger.record_item("e1", "item-1")
    await ledger.record_item("missing", "item-x")  # no recorded intent: nothing to bind
    assert await ledger.own_item_ids("S1") == frozenset({"item-1"})
    assert await ledger.own_item_ids("S2") == frozenset()
    await worker.close()


async def test_lost_ack_is_reconciled_after_daemon_restart(git_env: GitEnv, tmp_path: Path) -> None:
    db = tmp_path / "state.sqlite3"
    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT))
    rig.directory.specs["S1"] = spec(root_id=ROOT)
    rig.directory.texts["ef_send_1"] = "Stage brief"
    worker = await _worker(db)
    rig.adapter.ledger = StoreOwnItemLedger(worker)

    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append("timeout-after")
    send = intent(EffectKind.SEND_MESSAGE, id="ef_send_1", purpose="first", revision=1)
    assert isinstance(await rig.adapter.execute(send, CTX), AmbiguousWrite)
    await worker.close()  # crash: every in-memory structure is gone

    worker = await _worker(db)
    rig.adapter.ledger = StoreOwnItemLedger(worker)
    reconcile = intent(EffectKind.RECONCILE_SESSION, effect_id="ef_send_1")
    outcome = await rig.adapter.execute(reconcile, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["state"] == "delivered"
    item_id = rig.server.sessions[ROOT].items[-1]["id"]
    recorded = await rig.adapter.ledger.lookup("ef_send_1")
    assert recorded is not None and recorded.item_id == item_id
    assert rig.server.count("POST", f"/v1/sessions/{ROOT}/events") == 1  # never resent
    await worker.close()


async def test_without_the_durable_intent_a_lost_ack_is_not_adopted(
    git_env: GitEnv, tmp_path: Path
) -> None:
    """Control for the test above: a fresh ledger (the volatile failure mode) cannot
    claim the delivered item as ours."""
    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT))
    rig.directory.specs["S1"] = spec(root_id=ROOT)
    rig.directory.texts["ef_send_1"] = "Stage brief"
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append("timeout-after")
    send = intent(EffectKind.SEND_MESSAGE, id="ef_send_1", purpose="first", revision=1)
    await rig.adapter.execute(send, CTX)
    worker = await _worker(tmp_path / "empty.sqlite3")
    rig.adapter.ledger = StoreOwnItemLedger(worker)
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, effect_id="ef_send_1"), CTX
    )
    assert not (isinstance(outcome, Ack) and outcome.detail.get("state") == "delivered")
    await worker.close()
