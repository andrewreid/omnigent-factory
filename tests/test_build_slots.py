"""Build slots count only builds that are actually running.

A parked build (Blocked, Needs you, a settled checkpoint, or idle waiting on checks)
releases its ``max_building`` slot (and its auto-build slot); a message for it re-acquires
one through admission, resuming ahead of new builds, and is held until then.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import (
    BotState,
    DecisionImpact,
    Lifecycle,
    QueueEntry,
    QueueStatus,
    TrustedConfig,
)
from omnigent_factory.service.auto_build import admission_blocker
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import EventFactory, config, result_candidate, snapshot
from omnigent_factory.testing.fakes import FakeClock
from omnigent_factory.testing.harness import Harness
from tests.test_auto_build_reducer import auto_build, mark, planned

A, B, C = "I_slot_a", "I_slot_b", "I_slot_c"


@dataclass
class _Config:
    trusted: TrustedConfig


class _Service:
    def __init__(self, trusted: TrustedConfig) -> None:
        self.config = _Config(trusted)


def building(h: Harness, pid: str):
    """Plan, approve and admit a build: its run is working and holds a slot."""
    h.plan_published(pid)
    h.approve(pid)
    return h.admit(pid)


def blocked(h: Harness, pid: str) -> None:
    """The run reports blocked and its tree goes idle: Bot Blocked, parked."""
    s = h.cur(pid)
    r = h.send(pid, result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    assert r.audit.accepted, r.audit.reason
    h.quiesce(pid, s.session_id)
    assert h.p(pid).bot == BotState.BLOCKED


def feedback_sends(result) -> list:
    return [
        e
        for e in Harness.of(result, EffectKind.SEND_MESSAGE)
        if e.args.get("purpose") == MessagePurpose.FEEDBACK.value
    ]


def test_a_parked_blocked_build_frees_its_slot_and_a_queued_build_starts():
    h = Harness()  # max_building = 1
    building(h, A)
    assert h.admission.building_count == 1
    h.plan_published(B)
    h.approve(B)
    assert h.admission.queue_entry(B).status == QueueStatus.QUEUED
    assert not h.send(B, ev.CapacityAvailable()).audit.accepted  # A still runs
    blocked(h, A)
    assert h.admission.building_count == 0
    assert h.admission.queue_entry(A).status == QueueStatus.HELD and h.p(A).slot_parked
    assert [q.parcel_id for q in h.admission.parked()] == [A]
    s = h.admit(B)
    assert s.lifecycle == Lifecycle.ACTIVE and h.admission.building_count == 1


def test_a_parked_build_resumes_first_and_its_message_is_held_until_then():
    h = Harness()
    building(h, A)
    blocked(h, A)
    building(h, B)  # takes the freed slot
    h.plan_published(C)
    h.approve(C)  # a new build waits too
    # The owner answers A's block: the run must work again, but no slot is free.
    r = h.send(A, ev.PlanFeedback(text_digest="try the other way"))
    assert r.audit.accepted, r.audit.reason
    assert not feedback_sends(r)  # held: never delivered without a slot
    p = h.p(A)
    assert len(p.held_wakes) == 1 and p.bot == BotState.QUEUED
    assert p.note == "Queued: waiting for a build slot to resume (1/1 running: #2)"
    entry = h.admission.queue_entry(A)
    assert entry.status == QueueStatus.QUEUED and entry.resume
    # An idle scan of A's untouched tree is no turn ending while its message is held.
    h.quiesce(A, h.cur(A).session_id)
    assert h.p(A).held_wakes
    # B parks: A (resuming) goes before C (new).
    blocked(h, B)
    assert not h.send(C, ev.CapacityAvailable()).audit.accepted
    r = h.send(A, ev.CapacityAvailable())
    assert r.audit.accepted, r.audit.reason
    [sent] = feedback_sends(r)
    assert sent.args["text_digest"] == "try the other way"
    assert sent.preconditions.session_id == h.cur(A).session_id
    p = h.p(A)
    assert not p.held_wakes and not p.slot_parked
    assert h.admission.queue_entry(A).status == QueueStatus.RESERVED
    assert h.admission.building_count == 1
    # Exactly once: a second admission is refused and sends nothing.
    r = h.send(A, ev.CapacityAvailable())
    assert not r.audit.accepted and not feedback_sends(r)


def test_a_message_for_a_parked_build_takes_a_free_slot_at_once():
    h = Harness()
    building(h, A)
    blocked(h, A)
    r = h.send(A, ev.PlanFeedback(text_digest="go on"))
    assert len(feedback_sends(r)) == 1
    assert h.admission.queue_entry(A).status == QueueStatus.RESERVED
    assert h.admission.building_count == 1 and not h.p(A).held_wakes


def test_a_checkpoint_holds_its_slot_until_the_wrap_up_drain_completes():
    h = Harness()
    b = building(h, A)
    h.send(A, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    assert h.cur(A).lifecycle == Lifecycle.CHECKPOINT_GRACE
    assert h.admission.building_count == 1
    h.send(A, result_candidate(b.session_id, b.root_id, b.revision, ev.ResultKind.CHECKPOINT))
    assert h.cur(A).lifecycle == Lifecycle.CHECKPOINT_WAIT
    assert h.admission.building_count == 1  # still winding down
    h.quiesce(A, b.session_id)
    assert h.cur(A).lifecycle == Lifecycle.FENCED
    assert h.admission.building_count == 0 and h.p(A).bot == BotState.CHECKPOINT


def test_an_incomplete_or_busy_scan_counts_a_parked_build_as_running():
    h = Harness()
    building(h, A)
    blocked(h, A)
    building(h, B)
    assert h.admission.building_count == 1
    # A's idle read was stale: the next scan cannot see the whole tree.
    h.send(A, ev.TreeQuiescent(session_id=h.cur(A).session_id, complete=False, busy=False))
    assert h.admission.queue_entry(A).status == QueueStatus.RESERVED
    assert h.admission.building_count == 2  # counted, even over the cap (fail closed)


def test_1550_incomplete_idle_observation_persists_over_cap(tmp_path):
    from tests.test_store import open_store, store_harness

    store = open_store(tmp_path / "slots.sqlite3")
    try:
        h = store_harness(store, max_building=4, max_open_bot_prs=10)
        building(h, A)
        s = h.cur(A)
        h.send(
            A,
            result_candidate(
                s.session_id,
                s.root_id,
                s.revision,
                ev.ResultKind.BUILD_READY,
                pr_number=1550,
                head_sha="a" * 40,
            ),
        )
        h.quiesce(A, s.session_id)
        assert h.cur(A).lifecycle == Lifecycle.WAITING
        assert h.admission.queue_entry(A).status == QueueStatus.HELD
        building(h, "I_parked")
        blocked(h, "I_parked")
        for pid in (B, C, "I_slot_d", "I_slot_e"):
            building(h, pid)
        h.send(A, ev.TreeQuiescent(session_id=h.cur(A).session_id, complete=False, busy=False))
        assert store.load_admission(h.cfg.repo_id).building_count == 5
        assert h.admission.observed_over_cap_count(4) == 1
        h.plan_published("I_new")
        h.approve("I_new")
        assert not h.send("I_new", ev.CapacityAvailable()).audit.accepted
        wake = h.send("I_parked", ev.PlanFeedback(text_digest="wait for capacity"))
        assert not feedback_sends(wake) and h.p("I_parked").held_wakes
        for complete, busy in ((False, False), (True, True), (False, False)):
            h.send(A, ev.TreeQuiescent(session_id=s.session_id, complete=complete, busy=busy))
            assert h.admission.building_count == 5
            assert h.admission.observed_over_cap_count(4) == 1
        store.close()
        store = open_store(tmp_path / "slots.sqlite3")
        h.store = store
        h.admission = store.load_admission(h.cfg.repo_id)
        assert h.admission.building_count == 5
        assert h.admission.observed_over_cap_count(4) == 1
        assert store.load_parcel(A).current_session.lifecycle == Lifecycle.WAITING
        h.send(A, ev.TreeQuiescent(session_id=s.session_id, complete=False, busy=False))
        assert h.admission.building_count == 5
        h.quiesce(A, s.session_id)
        assert h.admission.building_count == 4
        assert h.admission.observed_over_cap_count(4) == 0
        assert not h.send("I_new", ev.CapacityAvailable()).audit.accepted
        assert not h.send("I_parked", ev.CapacityAvailable()).audit.accepted
        blocked(h, B)
        resumed = h.send("I_parked", ev.CapacityAvailable())
        assert resumed.audit.accepted and len(feedback_sends(resumed)) == 1
        assert h.admission.building_count == 4
        blocked(h, C)
        assert h.send("I_new", ev.CapacityAvailable()).audit.accepted
        assert h.admission.building_count == 4
    finally:
        store.close()


def test_auto_build_concurrency_counts_only_running_auto_builds():
    h = Harness(cfg=config(max_building=2, auto_build_concurrency=1))
    planned(h, A)
    planned(h, B)
    mark(h, A)
    mark(h, B)
    plan = h.cur(A)
    assert auto_build(h, A).audit.accepted
    h.quiesce(A, plan.session_id)
    h.admit(A)
    assert h.admission.auto_build_count == 1
    assert auto_build(h, B).audit.reason == "auto-build-cap"
    blocked(h, A)
    assert h.admission.auto_build_count == 0  # parked: not running
    assert auto_build(h, B).audit.accepted


def test_a_queued_auto_build_names_the_running_auto_build_it_waits_for():
    h = Harness(cfg=config(max_building=2, auto_build_concurrency=1))
    planned(h, A)
    planned(h, B)
    mark(h, A)
    mark(h, B)
    plan = h.cur(A)
    auto_build(h, A)
    h.quiesce(A, plan.session_id)
    h.admit(A)
    # B's auto-build waits (a build slot is free, the auto-build slot is not).
    waiting = QueueEntry(B, "ap_b", 99, QueueStatus.QUEUED, auto=True, issue_number=2)
    h.admission = replace(h.admission, queue=(*h.admission.queue, waiting))
    h.parcels[B] = replace(h.p(B), note="")
    h.send(B, ev.ReconcileDue())
    assert h.p(B).note == "Waiting for an auto-build slot (1/1 in use: #1)"
    blocker = admission_blocker(h.admission, _Service(h.cfg))  # type: ignore[arg-type]
    assert blocker == "waiting for an auto-build slot (1/1 in use: #1)"


def test_held_messages_survive_a_restart_and_are_sent_once(tmp_path):
    h = Harness()
    building(h, A)
    blocked(h, A)
    building(h, B)
    h.send(A, ev.PlanFeedback(text_digest="resume please"))
    assert h.p(A).held_wakes
    store = SqliteStore.open(tmp_path / "f.sqlite3", FakeClock())
    try:
        store.ensure_repository(h.cfg)
        unpause = EventFactory("-", start_us=1).make(ev.Unpause(), parcel_id=None, time_us=1)
        assert store.apply_event(unpause, h.cfg).accepted
        for event, _ in h.log:
            store.apply_event(event, h.cfg)
        # Restart: the held message and the resume entry come back from the store.
        parcel = store.load_parcel(A)
        assert parcel is not None and parcel.held_wakes == h.p(A).held_wakes
        assert parcel_from_json(parcel_to_json(parcel)) == parcel
        entry = store.load_admission(h.cfg.repo_id).queue_entry(A)
        assert entry is not None and entry.resume and entry.status == QueueStatus.QUEUED
        blocked_b = h.cur(B)
        for body in (
            result_candidate(
                blocked_b.session_id, blocked_b.root_id, blocked_b.revision, ev.ResultKind.BLOCKED
            ),
            ev.TreeQuiescent(session_id=blocked_b.session_id, complete=True, busy=False),
        ):
            store.apply_event(h.f(B).make(body), h.cfg)
        for _ in range(2):
            store.apply_event(h.f(A).make(ev.CapacityAvailable()), h.cfg)
        rows = store.query(
            "SELECT payload_json FROM effects WHERE parcel_id = ? AND kind = 'send_message'",
            (A,),
        )
        sent = [r[0] for r in rows if '"text_digest":"resume please"' in r[0]]
        assert len(sent) == 1
        assert not store.load_parcel(A).held_wakes  # type: ignore[union-attr]
    finally:
        store.close()


# ------------------------------------------------------------------ #673


def test_a_read_showing_queued_after_the_start_never_cancels_the_started_write():
    """#673: a read taken after the start but before the factory's "Started" write landed
    showed the owner's Queued; it was taken for a new value, the write was cancelled as
    superseded and the board kept Queued (with a bogus "not confirmed" note)."""
    h = Harness()
    planned(h, A)
    mark(h, A)
    r = auto_build(h, A)
    [write] = [e for e in r.effects if e.kind == EffectKind.SET_AUTO_BUILD]
    assert write.args["value"] == "Started"
    f = h.f(A)
    h.apply(
        f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now + 5, auto_build="Queued"))
    )
    p = h.p(A)
    assert p.auto_build_field == "Started"
    assert effect_still_valid(p, write) is None  # not superseded: it still runs
    assert p.note != "Auto-build mark not confirmed: re-select it"


def test_a_board_left_on_queued_for_a_started_build_is_set_to_started_again():
    h = Harness()
    planned(h, A)
    mark(h, A)
    auto_build(h, A)
    h.parcels[A] = replace(h.p(A), auto_build_field="Queued")  # the state #673 was left in
    f = h.f(A)
    r = h.apply(
        f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now + 5, auto_build="Queued"))
    )
    writes = [e.args["value"] for e in r.effects if e.kind == EffectKind.SET_AUTO_BUILD]
    assert writes == ["Started"] and h.p(A).auto_build_field == "Started"


@pytest.fixture
def observed_build(tmp_path):
    from tests.test_store import open_store, store_harness

    store = open_store(tmp_path / "observed.sqlite3")
    h = store_harness(store, max_building=4, max_open_bot_prs=10)
    try:
        s = building(h, A)
        h.send(
            A,
            result_candidate(
                s.session_id,
                s.root_id,
                s.revision,
                ev.ResultKind.BUILD_READY,
                pr_number=1550,
                head_sha="a" * 40,
            ),
        )
        h.quiesce(A, s.session_id)
        for pid in (B, C, "I_slot_d", "I_slot_e"):
            building(h, pid)
        h.send(A, ev.TreeQuiescent(session_id=s.session_id, complete=False, busy=False))
        assert h.admission.building_count == 5
        assert h.admission.queue_entry(A).status == QueueStatus.RESERVED
        yield h
    finally:
        h.store.close()


def _observed_wake(h, kind):
    if kind == "feedback":
        return h.send(A, ev.PlanFeedback(text_digest="please change x"))
    if kind == "continue":
        s = h.cur(A)
        h.send(A, ev.ActiveLimitReached(session_id=s.session_id, grant_id=s.grant.grant_id))
        assert h.send(A, ev.Continue()).audit.accepted
        return h.send(A, ev.PolicyReady(session_id=s.session_id, grant_id=h.cur(A).grant.grant_id))
    if kind == "answer":
        h.send(
            A,
            ev.ElicitationOpened(
                session_id=h.cur(A).session_id,
                elicitation_id="ask",
                impact=DecisionImpact.WITHIN_CONTRACT,
            ),
        )
        return h.send(A, ev.Decide(answer="yes", within_contract=True))
    # Readiness handlers require idle eligibility. Seed that state with the observed
    # reservation retained to exercise the shared post-handler gate for each wake.
    p = h.p(A)
    p = replace(
        p,
        version=p.version + 1,
        sessions=tuple(
            replace(s, quiescent=True) if s.session_id == p.current_session_id else s
            for s in p.sessions
        ),
    )
    h.store._write_parcel(h.store._conn, p, h.f(A).now)
    h.parcels[A] = p
    return h.send(
        A,
        ev.ReadinessEvidence(
            session_id=h.cur(A).session_id,
            pr_number=1550,
            head_sha="a" * 40,
            checks=ev.ChecksState.FAILED if kind == "checks" else ev.ChecksState.GREEN,
            findings_open=kind == "findings",
            review_bot_pending_since_us=0,
            mergeable=ev.MERGE_CONFLICT if kind == "conflict" else ev.MERGE_CLEAN,
            base_head="b" * 40,
            base_ref="main",
        ),
    )


def _wakes(result):
    return [
        e
        for e in result.effects
        if e.kind
        in (
            EffectKind.SEND_MESSAGE,
            EffectKind.RESOLVE_ELICITATION,
        )
    ]


@pytest.mark.parametrize(
    "kind", ["feedback", "checks", "findings", "conflict", "answer", "continue"]
)
@pytest.mark.parametrize("release", ["observed_idle", "other_build_idle"])
def test_observed_overcap_wakes_wait_and_replay_once(observed_build, kind, release):
    h = observed_build
    result = _observed_wake(h, kind)
    assert result.audit.accepted, result.audit.reason
    assert not _wakes(result)
    held = h.p(A).held_wakes
    assert len(held) == 1
    args = json.loads(held[0].args_json)
    if kind in ("checks", "findings", "conflict"):
        assert args["wake"] == kind
    elif kind == "answer":
        assert held[0].kind == EffectKind.RESOLVE_ELICITATION.value
    else:
        assert args["purpose"] == ("continuation" if kind == "continue" else "feedback")
    assert h.admission.building_count == 5
    assert h.admission.queue_entry(A).status == QueueStatus.RESERVED
    assert not _wakes(h.send(A, ev.ReconcileDue()))
    assert h.p(A).held_wakes == held
    # A restart must preserve both occupancy and the undelivered wake.
    from tests.test_store import open_store

    path = h.store._conn.execute("PRAGMA database_list").fetchone()[2]
    h.store.close()
    h.store = open_store(path)
    assert h.store.load_parcel(A).held_wakes == held
    assert h.store.load_admission(h.cfg.repo_id).building_count == 5
    if release == "observed_idle":
        for complete, busy in ((False, False), (True, True)):
            assert not _wakes(
                h.send(
                    A,
                    ev.TreeQuiescent(
                        session_id=h.cur(A).session_id,
                        complete=complete,
                        busy=busy,
                    ),
                )
            )
            assert h.admission.building_count == 5
        h.quiesce(A, h.cur(A).session_id)
        assert h.admission.building_count == 4
        assert h.admission.queue_entry(A).status == QueueStatus.QUEUED
        assert h.p(A).slot_parked and h.p(A).held_wakes == held
        assert not _wakes(h.send(A, ev.CapacityAvailable()))
        blocked(h, B)
        replay = h.send(A, ev.CapacityAvailable())
        assert replay.audit.accepted
    else:
        blocked(h, B)
        assert h.admission.building_count == 4
        replay = h.send(A, ev.ReconcileDue())
    [wake] = _wakes(replay)
    assert wake.kind.value == held[0].kind
    assert wake.args == json.loads(held[0].args_json)
    assert h.admission.building_count == 4
    assert not h.p(A).held_wakes and not h.p(A).slot_parked
    assert h.admission.queue_entry(A).status == QueueStatus.RESERVED
    assert not _wakes(h.send(A, ev.ReconcileDue()))


def test_stopped_observed_build_never_replays_held_wakes(observed_build):
    h = observed_build
    assert not _wakes(_observed_wake(h, "feedback"))
    assert h.p(A).held_wakes
    h.send(A, ev.Stop())
    assert not h.p(A).held_wakes
    blocked(h, B)
    h.quiesce(A, h.cur(A).session_id)
    assert not _wakes(h.send(A, ev.ReconcileDue()))
