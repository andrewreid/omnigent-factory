"""Build slots count only builds that are actually running.

A parked build (Blocked, Needs you, a settled checkpoint, or idle waiting on checks)
releases its ``max_building`` slot (and its auto-build slot); a message for it re-acquires
one through admission, resuming ahead of new builds, and is held until then.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectKind, MessagePurpose
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import BotState, Lifecycle, QueueEntry, QueueStatus, TrustedConfig
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
