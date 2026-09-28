"""F1/F2: a run opens only after its policy set propagated and was re-verified.

Prepare -> wait the cross-replica barrier -> re-verify the exact set -> only then enable
the credential and send work. The same holds after a stage switch and a replacement, and
survives a restart; a boot-detected guard failure closes a live run until the
reconcile -> barrier -> verify sequence succeeds, then re-enables it once.
"""

from __future__ import annotations

from pathlib import Path

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import WORK_BEARING_KINDS, EffectKind
from omnigent_factory.core.predicates import work_allowed
from omnigent_factory.core.types import Hold, Lifecycle, SessionKind, Via
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import T0
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.store_driver import StoreDriver

P = "I_parcel_1"


def work(result):
    return [e for e in result.effects if e.kind in WORK_BEARING_KINDS]


def verify_of(result):
    return [e for e in result.effects if e.kind == EffectKind.VERIFY_POLICIES]


def test_stage_switch_waits_for_verification_before_credential_or_work():
    h = Harness()
    h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    plan = h.cur()
    r = h.send(P, ev.Prepared(session_id=plan.session_id, ok=True, policy_ready_at_us=777))
    assert not work(r) and h.cur().lifecycle == Lifecycle.PREPARING
    [verify] = verify_of(r)
    assert verify.args == {"root_id": plan.root_id, "not_before_us": 777, "reconcile": False}
    assert not work_allowed(h.p(), h.cur())  # the broker gate refuses tokens too
    r = h.verify_policies()
    kinds = [e.kind for e in r.effects]
    assert kinds.index(EffectKind.ENABLE_ISSUANCE) < kinds.index(EffectKind.SEND_MESSAGE)
    assert h.cur().lifecycle == Lifecycle.ACTIVE and h.cur().policy_ready


def test_replacement_session_also_waits_for_verification():
    h = Harness()
    h.triage()
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    plan = h.cur()
    h.send(P, ev.Prepared(session_id=plan.session_id, ok=False, unusable=True, reason="gone"))
    h.send(P, ev.SessionCreated(session_id=plan.session_id, root_id="root-new", nonce=plan.nonce))
    r = h.send(P, ev.Prepared(session_id=plan.session_id, ok=True, policy_ready_at_us=9))
    assert not work(r) and [v.args["root_id"] for v in verify_of(r)] == ["root-new"]
    assert Harness.of(h.verify_policies(), EffectKind.SEND_MESSAGE)


def test_failed_verification_fails_closed_and_visible():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.cur()
    h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="r", nonce=s.nonce))
    h.send(P, ev.Prepared(session_id=s.session_id, ok=True, policy_ready_at_us=1))
    r = h.send(P, ev.PoliciesVerified(session_id=s.session_id, ok=False))
    assert not work(r) and Hold.PREPARE_FAILED in h.p().holds
    assert not h.cur().policy_ready and h.p().bot.value == "Blocked"


def test_pending_verification_survives_a_restart(tmp_path: Path):
    db = tmp_path / "state.sqlite3"
    h = Harness()
    store = SqliteStore.open(db, FakeClock())
    store.ensure_repository(h.cfg)
    driver = StoreDriver(store, h.cfg, P, start_us=T0)
    driver.unpause()
    driver.eligible()
    driver.send(ev.RequestTriage(via=Via.DRAG))
    s = driver.session()
    driver.send(ev.SessionCreated(session_id=s.session_id, root_id="r", nonce=s.nonce))
    driver.send(ev.Prepared(session_id=s.session_id, ok=True, policy_ready_at_us=T0 + 35))
    store.close()

    store = SqliteStore.open(db, FakeClock())
    run = store.load_parcel(P).current_session
    assert run.prepared and not run.policy_ready and run.policy_ready_at_us == T0 + 35
    pending = [e.effect for e in store.pending_effects() if e.effect.kind.value.startswith("v")]
    assert [e.args["not_before_us"] for e in pending] == [T0 + 35]
    assert not [e for e in store.pending_effects() if e.effect.work_bearing]
    after = driver.f.make(ev.PoliciesVerified(session_id=run.session_id, ok=True))
    result = store.apply_event(after, h.cfg)
    assert result.accepted, result.reason
    assert {e.kind for e in result.effects} >= {EffectKind.ENABLE_ISSUANCE, EffectKind.SEND_MESSAGE}
    store.close()


def test_boot_guard_failure_closes_a_live_run_until_reverified():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok()
    assert s.issuance_enabled and work_allowed(h.p(), s)
    r = h.send(P, ev.PolicyGuardFailed(session_id=s.session_id))
    assert [e.kind for e in r.effects if e.kind != EffectKind.SET_BOT] == [
        EffectKind.DISABLE_ISSUANCE,
        EffectKind.VERIFY_POLICIES,
    ]
    assert verify_of(r)[0].args["reconcile"] is True
    assert not work_allowed(h.p(), h.cur())
    # Work relays are refused meanwhile (e.g. an operator note).
    assert not h.send(P, ev.OperatorResume(text="go on")).audit.accepted
    r = h.send(P, ev.PoliciesVerified(session_id=s.session_id, reconciled=True, ready_at_us=55))
    assert [v.args["not_before_us"] for v in verify_of(r)] == [55] and not work(r)
    r = h.verify_policies()
    assert [e.kind for e in work(r)] == [EffectKind.ENABLE_ISSUANCE]  # re-enabled once
    assert work_allowed(h.p(), h.cur()) and h.cur().kind == SessionKind.TRIAGE
    assert not h.send(P, ev.PoliciesVerified(session_id=s.session_id, ok=True)).audit.accepted
