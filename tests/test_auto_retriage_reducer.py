"""Idle-time auto-retriage and the stale-triage flag in the pure reducer."""

from __future__ import annotations

from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import ADMISSION
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import EventKind, Provenance
from omnigent_factory.core.projection import project_note
from omnigent_factory.core.types import BotState, Hold, Lifecycle, SessionKind, Stage, Via
from omnigent_factory.testing.builders import OWNER_ID, result_candidate, snapshot
from omnigent_factory.testing.harness import Harness

SIGNALS = ("cited paths changed: src/a.py", "triaged 31 days ago")


def retriage(h: Harness, pid: str, **snap: object):
    """The clock's AutoRetriage for ``pid`` with a fresh Triaged read."""
    f = h.f(pid)
    fields = {"stage": Stage.TRIAGED, "read_at_us": f.now + 1, **snap}
    return h.apply(
        f.make(ev.AutoRetriage(signals=SIGNALS), evidence=snapshot(**fields))  # type: ignore[arg-type]
    )


def triaged(h: Harness, pid: str = "A") -> None:
    h.triage(pid)
    p = h.p(pid)
    # The posted triage's resting state: the next move is the owner's.
    assert p.stage == Stage.TRIAGED and p.holds == {Hold.AWAITING_OWNER}
    assert p.bot == BotState.IDLE


def stale(h: Harness, pid: str, text: str):
    return h.apply(h.f(pid).make(ev.TriageStale(text=text), provenance=Provenance.ADAPTER))


def test_auto_retriage_is_admitted_only_from_the_trusted_clock():
    assert ADMISSION[EventKind.AUTO_RETRIAGE].provenances == frozenset({Provenance.SCHEDULER})
    assert ADMISSION[EventKind.TRIAGE_STALE].provenances == frozenset({Provenance.ADAPTER})
    h = Harness()
    triaged(h)
    f = h.f("A")
    before = len(h.p("A").authorizations)
    for provenance in (Provenance.WEBHOOK, Provenance.OPERATOR, Provenance.ADAPTER):
        r = h.apply(
            f.make(
                ev.AutoRetriage(),
                provenance=provenance,
                actor=OWNER_ID,
                evidence=snapshot(stage=Stage.TRIAGED, read_at_us=f.now + 1),
            )
        )
        assert not r.audit.accepted and r.audit.reason == "provenance-not-admitted"
    assert len(h.p("A").authorizations) == before


def test_auto_retriage_starts_a_triage_run_on_a_quiet_triaged_card():
    h = Harness()
    triaged(h)
    r = retriage(h, "A")
    assert r.audit.accepted, r.audit.reason
    assert h.of(r, EffectKind.MOVE_CARD) == []  # already in Triage
    p = h.p("A")
    auth = p.authorizations[-1]
    assert auth.kind == SessionKind.TRIAGE and not auth.cancelled
    assert auth.source_event_id == r.audit.event_id  # the dispatch finds the signals by it
    assert p.current_session is not None and p.current_session.authorization_id == (
        auth.authorization_id
    )
    assert p.holds == {Hold.AWAITING_OWNER}  # kept: it is no blocking hold
    s = h.create_ok("A")  # created, prepared, policies verified: the run works
    assert s.kind == SessionKind.TRIAGE and s.lifecycle == Lifecycle.ACTIVE
    p = h.p("A")
    assert p.stage == Stage.TRIAGED and p.bot == BotState.WORKING
    # ACTIVE only once work_allowed passed and the first message went out: the hold is
    # no obstacle to the run.
    assert p.holds == {Hold.AWAITING_OWNER}


def test_auto_retriage_needs_a_fresh_read_and_a_triaged_card():
    h = Harness()
    triaged(h)
    r = h.apply(h.f("A").make(ev.AutoRetriage(), evidence=None))
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-without-fresh-read"
    r = retriage(h, "A", stage=Stage.SCOPED)
    assert not r.audit.accepted
    h2 = Harness()
    h2.eligible("N")  # never triaged
    h2.apply(
        h2.f("N").make(ev.ColumnObserved(stage=Stage.TRIAGED), provenance=Provenance.RECONCILER)
    )
    r = retriage(h2, "N")
    assert not r.audit.accepted


@pytest.mark.parametrize("hold", [Hold.AGENT_BLOCKED, Hold.STOPPED, Hold.BUNDLED, Hold.SAFETY])
def test_auto_retriage_never_clears_a_hold(hold):
    """Unlike an owner's /triage (which clears control holds), a held card is refused."""
    h = Harness()
    triaged(h)
    h.parcels["A"] = replace(h.p("A"), holds=h.p("A").holds | {hold})
    r = retriage(h, "A")
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-held"
    p = h.p("A")
    assert hold in p.holds and p.pending_authorization_id is None


def test_auto_retriage_refuses_a_card_that_needs_the_owner():
    h = Harness()
    triaged(h)
    h.parcels["A"] = replace(h.p("A"), bot=BotState.NEEDS_YOU)
    r = retriage(h, "A")
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-needs-owner"


def test_auto_retriage_refuses_a_recorded_or_running_run():
    h = Harness()
    triaged(h)
    assert retriage(h, "A").audit.accepted
    r = retriage(h, "A", read_at_us=h.f("A").now + 10)
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-run-in-flight"
    h.create_ok("A")
    r = retriage(h, "A", read_at_us=h.f("A").now + 20)
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-run-in-flight"


def test_auto_retriage_respects_pause_and_the_triage_slots():
    h = Harness()
    triaged(h)
    h.admission = replace(h.admission, paused=True)
    r = retriage(h, "A")
    assert not r.audit.accepted and r.audit.reason == "paused"
    h.admission = replace(h.admission, paused=False)
    h.eligible("B")
    h.send("B", ev.RequestTriage(via=Via.DRAG))
    h.create_ok("B")  # the one triage slot (triage_concurrency = 1)
    r = retriage(h, "A")
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-slot-busy"


def test_auto_retriage_refused_while_a_queued_build_can_start():
    h = Harness()
    triaged(h)
    h.plan_published("C")
    h.approve("C")
    r = retriage(h, "A")
    assert not r.audit.accepted and r.audit.reason == "auto-retriage-factory-busy"


def test_triage_stale_flag_sets_the_note_and_a_new_triage_clears_it():
    h = Harness()
    triaged(h)
    r = stale(h, "A", "cited paths changed: src/a.py")
    assert r.audit.accepted, r.audit.reason
    p = h.p("A")
    assert p.triage_stale == "cited paths changed: src/a.py"
    assert project_note(p, p.bot) == "Triage may be stale: cited paths changed: src/a.py"
    [note] = h.of(r, EffectKind.SET_NOTE)
    assert note.args["note"].startswith("Triage may be stale:")
    again = stale(h, "A", "cited paths changed: src/a.py")
    assert not again.audit.accepted and again.audit.reason == "triage-stale-unchanged"
    assert retriage(h, "A").audit.accepted
    assert h.p("A").triage_stale == ""


def test_triage_stale_flag_is_cleared_when_the_card_leaves_triage_and_refused_elsewhere():
    h = Harness()
    triaged(h)
    stale(h, "A", "triaged 40 days ago")
    h.send("A", ev.RequestPlan(via=Via.DRAG))
    assert h.p("A").stage == Stage.SCOPED and h.p("A").triage_stale == ""
    r = stale(h, "A", "triaged 40 days ago")
    assert not r.audit.accepted and r.audit.reason == "triage-stale-not-triaged"
    assert stale(h, "A", "").audit.reason == "triage-stale-unchanged"


def test_a_failed_refresh_run_keeps_every_hold_the_card_had():
    h = Harness()
    triaged(h)
    before = h.p("A").holds
    assert retriage(h, "A").audit.accepted
    s = h.create_ok("A")
    assert s.root_id is not None
    h.send("A", result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.BLOCKED))
    p = h.p("A")
    assert before <= p.holds and Hold.AGENT_BLOCKED in p.holds


def test_a_failed_refresh_publication_keeps_every_hold_the_card_had():
    h = Harness()
    triaged(h)
    before = h.p("A").holds
    assert retriage(h, "A").audit.accepted
    s = h.create_ok("A")
    assert s.root_id is not None
    r = h.send("A", result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE))
    [publish] = h.of(r, EffectKind.PUBLISH_TRIAGE)
    h.send(
        "A",
        ev.EffectCancelled(
            effect_id=publish.effect_id,
            effect_kind=EffectKind.PUBLISH_TRIAGE.value,
            session_id=s.session_id,
            failed=True,
        ),
    )
    p = h.p("A")
    assert before <= p.holds and Hold.PUBLICATION_FAILED in p.holds


def test_a_failed_create_keeps_every_hold_the_card_had():
    h = Harness()
    triaged(h)
    before = h.p("A").holds
    assert retriage(h, "A").audit.accepted
    run = h.cur("A")
    h.send("A", ev.CreateRejected(session_id=run.session_id, reason="refused"))
    assert before <= h.p("A").holds


def test_auto_retriage_is_bound_to_the_triage_it_judged():
    h = Harness()
    triaged(h)
    judged = h.cur("A").session_id
    f = h.f("A")
    old = h.apply(
        f.make(
            ev.AutoRetriage(signals=SIGNALS, triage_session="ss_older"),
            evidence=snapshot(stage=Stage.TRIAGED, read_at_us=f.now + 1),
        )
    )
    assert not old.audit.accepted and old.audit.reason == "auto-retriage-triage-changed"
    ok = h.apply(
        f.make(
            ev.AutoRetriage(signals=SIGNALS, triage_session=judged),
            evidence=snapshot(stage=Stage.TRIAGED, read_at_us=f.now + 2),
        )
    )
    assert ok.audit.accepted, ok.audit.reason


# ----------------------------------------------------------------- close-candidate label


def _label_effects(r) -> list:
    return Harness.of(r, EffectKind.SET_CLOSE_CANDIDATE)


def test_each_published_triage_issues_one_label_convergence_for_its_generation():
    h = Harness()
    h.eligible("A")
    h.send("A", ev.RequestTriage(via=Via.DRAG))
    s = h.create_ok("A")
    assert s.root_id is not None
    r = h.send("A", result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.TRIAGE))
    [publish] = h.of(r, EffectKind.PUBLISH_TRIAGE)
    assert _label_effects(r) == []  # only once the triage is visible
    ack = ev.PublicationAcked(
        effect_id=publish.effect_id,
        effect_kind=publish.kind.value,
        session_id=s.session_id,
        comment_id="c-1",
    )
    [label] = _label_effects(h.send("A", ack))
    assert label.args == {"action": "triage", "session_id": s.session_id}
    assert label.retry_class.value == "adoptable_write"
    assert h.p("A").close_candidate_session == s.session_id
    # A repeated acknowledgement never issues it again (an owner's removal sticks).
    again = replace(ack, comment_id="c-2")
    assert _label_effects(h.send("A", again)) == []


def test_leaving_triage_issues_a_removal_however_the_move_is_seen():
    for move in (
        ev.RequestPlan(via=Via.DRAG),
        ev.LeftwardMove(from_stage=Stage.TRIAGED, to_stage=Stage.INBOX),
    ):
        h = Harness()
        triaged(h)
        gen = h.p("A").close_candidate_session
        assert gen
        [label] = _label_effects(h.send("A", move))
        assert label.args == {"action": "remove", "session_id": gen}
    h = Harness()
    triaged(h)
    f = h.f("A")
    observed = h.apply(  # a move a read observes (a lost webhook), no owner control
        f.make(ev.ColumnObserved(stage=Stage.SCOPED), provenance=Provenance.RECONCILER)
    )
    assert observed.audit.accepted, observed.audit.reason
    assert h.p("A").stage == Stage.SCOPED
    assert [e.args["action"] for e in _label_effects(observed)] == ["remove"]
    # Never triaged here: nothing to remove.
    h2 = Harness()
    h2.eligible("B")
    assert _label_effects(h2.send("B", ev.RequestPlan(via=Via.DRAG))) == []


def test_label_effects_of_an_older_generation_or_a_moved_card_are_superseded():
    from omnigent_factory.core.effects import EffectIntent, Preconditions
    from omnigent_factory.core.preconditions import effect_still_valid

    h = Harness()
    triaged(h)
    p = h.p("A")
    gen = p.close_candidate_session

    def effect(action: str, session: str) -> EffectIntent:
        return EffectIntent(
            effect_id="ef",
            kind=EffectKind.SET_CLOSE_CANDIDATE,
            parcel_id="A",
            target="A",
            preconditions=Preconditions(parcel_version=1, eligibility_epoch=0),
            args={"action": action, "session_id": session},
        )

    assert effect_still_valid(p, effect("triage", gen)) is None
    assert effect_still_valid(p, effect("remove", gen)) is None
    assert effect_still_valid(p, effect("triage", "older")) == "close-candidate-superseded"
    assert effect_still_valid(p, effect("remove", "older")) == "close-candidate-superseded"
    moved = replace(p, stage=Stage.SCOPED)
    assert effect_still_valid(moved, effect("triage", gen)) == "close-candidate-superseded"
    assert effect_still_valid(moved, effect("remove", gen)) is None


# ----------------------------------------------------------------- owner label removal


def _removed(h: Harness, pid: str, *, at: int, **kw: object):
    f = h.f(pid)
    return h.apply(
        f.make(ev.CloseCandidateRemoved(), event_id=f"{pid}:unlabel:{at}", **kw)  # type: ignore[arg-type]
    )


def test_an_owner_label_removal_supersedes_this_generations_add_only():
    from omnigent_factory.core.effects import EffectIntent, Preconditions
    from omnigent_factory.core.preconditions import effect_still_valid

    assert ADMISSION[EventKind.CLOSE_CANDIDATE_REMOVED].provenances == frozenset(
        {Provenance.WEBHOOK, Provenance.RECOVERY}
    )
    h = Harness()
    triaged(h)
    gen = h.p("A").close_candidate_session

    def add(session: str) -> EffectIntent:
        return EffectIntent(
            effect_id="ef",
            kind=EffectKind.SET_CLOSE_CANDIDATE,
            parcel_id="A",
            target="A",
            preconditions=Preconditions(parcel_version=1, eligibility_epoch=0),
            args={"action": "triage", "session_id": session},
        )

    assert effect_still_valid(h.p("A"), add(gen)) is None
    # Not the owner, or not from GitHub: no decision recorded.
    assert _removed(h, "A", at=1, actor=4242).audit.reason == "control-from-non-owner"
    assert not _removed(h, "A", at=2, provenance=Provenance.ADAPTER).audit.accepted
    r = _removed(h, "A", at=3)
    assert r.audit.accepted, r.audit.reason
    p = h.p("A")
    assert p.close_candidate_removed == gen
    assert effect_still_valid(p, add(gen)) == "close-candidate-superseded"
    assert _removed(h, "A", at=4).audit.reason == "close-candidate-removal-recorded"
    # The card's other label work is unaffected; the next triage may converge again.
    remove = replace(add(gen), args={"action": "remove", "session_id": gen})
    assert effect_still_valid(p, remove) is None
    next_gen = replace(p, close_candidate_session="ss_next")
    assert effect_still_valid(next_gen, add("ss_next")) is None


def test_a_removal_older_than_the_published_triage_is_of_the_earlier_label():
    h = Harness()
    triaged(h)
    p = h.p("A")
    f = h.f("A")
    old = h.apply(
        replace(
            f.make(ev.CloseCandidateRemoved(), event_id="A:unlabel-old"),
            source_time_us=p.close_candidate_at_us - 1_000_001,  # beyond the slack
        )
    )
    assert not old.audit.accepted
    assert old.audit.reason == "close-candidate-removal-before-triage"
    assert h.p("A").close_candidate_removed == ""
    boundary = h.apply(
        replace(
            f.make(ev.CloseCandidateRemoved(), event_id="A:unlabel-boundary"),
            source_time_us=p.close_candidate_at_us - 1_000_000,  # within the slack
        )
    )
    assert boundary.audit.accepted, boundary.audit.reason
    assert h.p("A").close_candidate_removed == p.close_candidate_session
    h2 = Harness()
    h2.eligible("B")  # never published a triage: nothing to supersede
    r = h2.apply(h2.f("B").make(ev.CloseCandidateRemoved()))
    assert r.audit.reason == "close-candidate-never-published"


def _owner_unlabel_at(updated_at_s: int, delivered_us: int):
    """The real normalizer's event for an owner's close-candidate unlabel whose payload
    says ``updated_at`` (GitHub: whole seconds), delivered at ``delivered_us``."""
    from datetime import UTC, datetime

    from omnigent_factory.github.webhook import DeliveryNormalizer
    from tests.github.test_webhook_config_setup import IDENTITY, payload

    stamp = datetime.fromtimestamp(updated_at_s, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = payload(
        action="unlabeled",
        sender={"id": OWNER_ID, "type": "User"},
        label={"name": "factory:close-candidate"},
        issue={"id": 10, "node_id": "I_1", "number": 12, "updated_at": stamp},
    )
    import json

    [event] = (
        DeliveryNormalizer(IDENTITY)
        .normalize(
            raw_body=json.dumps(data).encode(),
            event_name="issues",
            delivery_guid=f"d-{updated_at_s}",
            delivery_time_us=delivered_us,
            provenance=Provenance.WEBHOOK,
        )
        .events
    )
    return event


@pytest.mark.parametrize(
    ("updated_at_s", "accepted"),
    [
        (1_900_000_008, True),  # the publication's own second: counts for it
        (1_900_000_009, True),  # after it
        (1_900_000_007, False),  # an earlier second, 1.5 s before: beyond the slack
        (1_900_000_005, False),  # clearly older: the earlier triage's label
    ],
)
def test_a_same_second_owner_unlabel_counts_for_the_new_generation(updated_at_s, accepted):
    """Publication acknowledged at ...8.5, the webhook delivered at ...8.9 with GitHub's
    whole-second ``updated_at``."""
    h = Harness()
    triaged(h, "I_1")
    published = 1_900_000_008_500_000
    h.parcels["I_1"] = replace(h.p("I_1"), close_candidate_at_us=published)
    event = _owner_unlabel_at(updated_at_s, 1_900_000_008_900_000)
    assert event.source_time_us == updated_at_s * 1_000_000  # second precision
    r = h.apply(event)
    assert r.audit.accepted is accepted, r.audit.reason
    gen = h.p("I_1").close_candidate_session
    assert h.p("I_1").close_candidate_removed == (gen if accepted else "")
    if not accepted:
        assert r.audit.reason == "close-candidate-removal-before-triage"
