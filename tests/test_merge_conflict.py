"""Open bot PRs that conflict with main after main moved are sent back to the build.

A push to the default branch re-reads every Building/Ready PR (``BasePushed``); the read
reports GitHub's mergeability. A conflict on a Ready card (or a Building card whose run
finished) sends the issue session one conflict wake per (PR head, main head); a run
mid-turn is not interrupted; "behind" but clean is no work at all. A review that predates
the conflict never carries to the merge that resolves it.
"""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.codec import parcel_from_json, parcel_to_json
from omnigent_factory.core.effects import EffectIntent, EffectKind, MessagePurpose
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    BotState,
    Hold,
    Lifecycle,
    Stage,
    WaitReason,
)
from omnigent_factory.testing.builders import OWNER_ID, config, result_candidate
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
PR = 812
HEAD = "e" * 40
MERGED = "f" * 40  # the head after the conflict was resolved
MAIN1 = "1" * 40
MAIN2 = "2" * 40
MIN = MICROS_PER_MINUTE
GREEN = "12 checks: 12 success"


def harness() -> Harness:
    return Harness(cfg=replace(config(), review_grace_us=10 * MIN))


def push(h: Harness, head: str = MAIN2):
    """The default-branch push webhook fanned out to the parcel."""
    return h.send(
        P, ev.BasePushed(ref="refs/heads/main", head_sha=head), provenance=Provenance.WEBHOOK
    )


def kinds(result) -> list[EffectKind]:
    return [e.kind for e in result.effects]


def sends(result) -> list[EffectIntent]:
    return [e for e in result.effects if e.kind == EffectKind.SEND_MESSAGE]


def fetches(result) -> list[EffectIntent]:
    return [e for e in result.effects if e.kind == EffectKind.FETCH_PR_EVIDENCE]


def needs_you_comments(result) -> list[EffectIntent]:
    return [
        e
        for e in result.effects
        if e.kind == EffectKind.POST_COMMENT and e.args.get("template") == "ready-blocked"
    ]


def read(h: Harness, head: str = HEAD, **kw) -> ev.ReadinessEvidence:
    """A fresh PR read: green, reviewed, the review bot answered."""
    r = h.p().readiness
    assert r is not None
    kw.setdefault("verified", True)
    kw.setdefault("checks", ev.ChecksState.GREEN)
    kw.setdefault("checks_summary", GREEN)
    kw.setdefault("review_bot_pending_since_us", 0)
    kw.setdefault("mergeable", ev.MERGE_CLEAN)
    kw.setdefault("base_head", MAIN1)
    kw.setdefault("base_ref", "main")
    return ev.ReadinessEvidence(session_id=r.session_id, pr_number=PR, head_sha=head, **kw)


def conflict(h: Harness, head: str = HEAD, base: str = MAIN1) -> ev.ReadinessEvidence:
    """GitHub: ``mergeable: false, mergeable_state: dirty`` (the adapter does not verify)."""
    return read(h, head, verified=False, mergeable=ev.MERGE_CONFLICT, base_head=base)


def submit(h: Harness, head: str = HEAD, *, end_turn: bool = True) -> None:
    s = h.cur(P)
    assert s.root_id is not None
    r = h.send(
        P,
        result_candidate(
            s.session_id,
            s.root_id,
            s.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=PR,
            head_sha=head,
        ),
    )
    assert r.audit.accepted, r.audit.reason
    h.send(P, ev.PRObserved(pr_number=PR, head_sha=head, bot_authored=True, parcel_branch=True))
    if end_turn:
        h.quiesce(P, h.cur(P).session_id)


def ready(h: Harness) -> None:
    h.to_building()
    submit(h)
    h.send(P, read(h))
    p = h.p()
    assert p.stage == Stage.READY and p.readiness is not None and p.readiness.ready
    assert p.session(p.readiness.session_id).lifecycle == Lifecycle.RETIRED


# ------------------------------------------------------------------ push to main


def test_push_to_main_rereads_a_ready_or_building_pr():
    h = harness()
    ready(h)
    r = push(h)
    assert r.audit.accepted
    # Only GitHub-origin input (the signed webhook, a recovery or a daemon read).
    assert not h.send(P, ev.BasePushed(head_sha=MAIN2), provenance=Provenance.MCP).audit.accepted
    [fetch] = fetches(r)
    assert fetch.args["pr_number"] == PR and fetch.args["head_sha"] == HEAD


def test_push_is_nothing_for_a_parcel_without_a_build_ready():
    h = harness()
    h.to_building()  # the build has not submitted yet: its build_ready reads the PR
    r = push(h)
    assert r.audit.accepted and not fetches(r)


# ------------------------------------------------------------------ Ready: conflict


def test_conflict_on_ready_reworks_once_per_pr_and_main_head():
    h = harness()
    ready(h)
    approval = h.p().current_approval_id
    r = h.send(P, conflict(h))
    p = h.p()
    assert p.stage == Stage.BUILDING
    assert p.note == "Merge conflict with main"
    assert p.conflict_wake == f"{HEAD}:{MAIN1}"
    assert p.current_approval_id == approval  # same approval, no owner nudge
    [auth] = [a for a in p.authorizations if a.rework]
    assert auth.conflict  # the rework run's first message is the conflict instruction
    assert h.admission.queue_entry(P) is not None  # admitted like any build (slot or queue)
    assert not needs_you_comments(r)
    # Admitted: the rework run starts in the issue session.
    run = h.admit(P)
    assert run.authorization_id == auth.authorization_id
    # The run pushes nothing and resubmits the same head: the conflict for the same
    # (PR head, main head) is not sent again; it is Needs you with one comment.
    submit(h)
    r = h.send(P, conflict(h))
    assert not sends(r)
    assert h.p().stage == Stage.BUILDING
    assert Hold.READINESS_FAILED in h.p().holds and h.p().bot == BotState.NEEDS_YOU
    [comment] = needs_you_comments(r)
    assert "merge conflict with main" in str(comment.args["reason"])
    again = h.send(P, conflict(h))
    assert not sends(again) and not needs_you_comments(again)


def test_conflict_on_ready_with_no_build_slot_queues():
    h = harness()
    ready(h)
    h.f("I_other").tick(0)
    h.to_building("I_other")  # holds the only build slot (max_building = 1)
    assert h.admission.building_count >= 1
    h.send(P, conflict(h))
    p = h.p()
    assert p.stage == Stage.BUILDING and p.bot == BotState.QUEUED
    assert p.conflict_wake == f"{HEAD}:{MAIN1}"


# ------------------------------------------------------------------ Building


def test_conflict_wakes_a_finished_build_run_once_then_needs_you():
    h = harness()
    h.to_building()
    submit(h)  # waiting on checks, its turn ended
    s = h.cur(P)
    assert (s.lifecycle, s.wait_reason, s.quiescent) == (
        Lifecycle.WAITING,
        WaitReason.CHECKS,
        True,
    )
    r = h.send(P, conflict(h))
    [wake] = sends(r)
    assert wake.args["purpose"] == MessagePurpose.READINESS_WAKE.value
    assert wake.args["wake"] == "conflict" and wake.args["base_ref"] == "main"
    assert EffectKind.ENABLE_ISSUANCE in kinds(r)
    p = h.p()
    assert p.note == "Merge conflict with main" and p.bot == BotState.WORKING
    assert p.readiness_wakes == 0 and p.findings_wakes == 0  # its own budget
    # Re-reads while the woken run works: nothing more.
    r = h.send(P, conflict(h))
    assert not sends(r) and not needs_you_comments(r)
    # She ends the turn without resolving it: Needs you, one comment, no second wake.
    h.quiesce(P, s.session_id)
    r = h.send(P, conflict(h))
    assert not sends(r)
    assert Hold.READINESS_FAILED in h.p().holds
    assert len(needs_you_comments(r)) == 1
    # Main moves again: a new pair gets its own wake.
    r = h.send(P, conflict(h, base=MAIN2))
    [wake] = sends(r)
    assert wake.args["wake"] == "conflict"
    assert Hold.READINESS_FAILED not in h.p().holds


def test_conflict_does_not_interrupt_a_build_run_mid_turn():
    h = harness()
    h.to_building()
    submit(h, end_turn=False)  # build_ready submitted; the turn has not ended yet
    r = h.send(P, conflict(h))
    assert not sends(r) and not needs_you_comments(r)
    assert EffectKind.INTERRUPT_TREE not in kinds(r)
    p = h.p()
    assert p.stage == Stage.BUILDING and p.conflict_wake == ""
    assert Hold.READINESS_FAILED not in p.holds
    assert p.conflict_reviewed_head == HEAD  # recorded: its review won't carry
    s = h.cur(P)
    assert s.lifecycle == Lifecycle.WAITING and not s.quiescent
    # Its turn ends with the conflict still there: the next read wakes it once.
    h.quiesce(P, s.session_id)
    r = h.send(P, conflict(h))
    assert [w.args["wake"] for w in sends(r)] == ["conflict"]


def test_active_build_turn_is_not_interrupted():
    h = harness()
    h.to_building()
    submit(h)
    # An owner comment relayed to the run: it is working again (mid-turn).
    h.send(P, ev.PlanFeedback(text_digest="d1"))
    s = h.cur(P)
    assert s.lifecycle == Lifecycle.ACTIVE and not s.quiescent
    r = h.send(P, conflict(h))
    assert not sends(r) and not needs_you_comments(r)
    assert h.p().conflict_wake == ""


# ------------------------------------------------------------------ behind / unknown


def test_behind_but_clean_is_no_work():
    h = harness()
    ready(h)
    before = h.p()
    r = h.send(P, read(h, base_head=MAIN2))  # behind main, merges cleanly
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.ready
    assert p.note == before.note and p.bot == before.bot
    assert not sends(r) and not needs_you_comments(r)
    assert EffectKind.MOVE_CARD not in kinds(r)
    assert p.conflict_wake == "" and p.conflict_reviewed_head == ""


def test_unknown_mergeability_is_neither_conflict_nor_clean_and_is_asked_again():
    h = harness()
    ready(h)
    assert not fetches(h.send(P, ev.ReconcileDue()))  # verified Ready: no catch-up read
    r = h.send(P, read(h, mergeable=ev.MERGE_UNKNOWN))
    p = h.p()
    assert p.stage == Stage.READY and p.readiness.merge_unknown
    assert not sends(r) and p.conflict_reviewed_head == ""
    # The next catch-up read asks again; it then shows the conflict.
    assert fetches(h.send(P, ev.ReconcileDue()))
    h.send(P, conflict(h))
    assert h.p().stage == Stage.BUILDING and h.p().conflict_wake == f"{HEAD}:{MAIN1}"


# ------------------------------------------------------------------ restart


def test_restart_does_not_resend_the_conflict_wake():
    h = harness()
    h.to_building()
    submit(h)
    [_] = sends(h.send(P, conflict(h)))
    s = h.cur(P)
    h.quiesce(P, s.session_id)
    # Restart: the aggregate is reloaded from its stored JSON.
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    assert h.p().conflict_wake == f"{HEAD}:{MAIN1}"
    r = h.send(P, conflict(h))
    assert not sends(r)


def test_restart_does_not_rework_a_ready_card_twice():
    h = harness()
    ready(h)
    h.send(P, conflict(h))
    reworks = [a for a in h.p().authorizations if a.rework]
    h.parcels[P] = parcel_from_json(parcel_to_json(h.p()))
    h.admit(P)
    submit(h)  # same head again, nothing resolved
    h.send(P, conflict(h))
    assert [a for a in h.p().authorizations if a.rework] == reworks


# ------------------------------------------------------------------ review carry-over


def test_review_before_a_conflict_never_carries_to_the_resolving_merge():
    h = harness()
    h.to_building()
    submit(h)
    h.send(P, conflict(h))
    # She merges main and pushes: the new head is read with the old review, which must
    # not be carried as a base sync.
    r = h.send(P, read(h, observed_head_sha=MERGED, verified=False))
    [fetch] = fetches(r)
    assert fetch.args["head_sha"] == MERGED and fetch.args["reviewed_head"] == HEAD
    assert fetch.args["sync_carry"] is False
    # She resubmits with a fresh review of the merged head: its reads carry normally.
    submit(h, MERGED)
    r = push(h)
    [fetch] = fetches(r)
    assert fetch.args["reviewed_head"] == MERGED and "sync_carry" not in fetch.args


def test_clean_owner_sync_still_carries_the_review():
    h = harness()
    ready(h)
    # The owner's "Update branch" with no conflict seen: the existing carry-over rule.
    r = h.send(P, read(h, observed_head_sha=MERGED))
    [fetch] = fetches(r)
    assert fetch.args["reviewed_head"] == HEAD and "sync_carry" not in fetch.args


def test_owner_drag_to_ready_with_the_run_open_is_kept_in_building_on_a_conflict():
    h = harness()
    h.to_building()
    submit(h)  # waiting on checks, its turn ended
    h.send(P, ev.ColumnObserved(stage=Stage.READY), actor=OWNER_ID, provenance=Provenance.WEBHOOK)
    assert h.p().stage == Stage.READY
    r = h.send(P, conflict(h))
    moves = [e for e in r.effects if e.kind == EffectKind.MOVE_CARD]
    assert [m.args["to"] for m in moves] == ["Building"]
    assert h.p().stage == Stage.BUILDING
    assert not needs_you_comments(r) and Hold.READINESS_FAILED not in h.p().holds
    # The move landed: the next read wakes the waiting run once.
    r = h.send(P, conflict(h))
    assert [w.args["wake"] for w in sends(r)] == ["conflict"]
