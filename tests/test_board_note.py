"""Status on the card, not in comments: the "Factory note" field, Bot=Queued, reactions.

Bot comments notify the owner, so an issue comment is posted only for what he must read
or act on. Informational status is the card's "Factory note" (written only when it
changes) and owner command comments get a reaction.
"""

from __future__ import annotations

import ast
import inspect
import json
from dataclasses import replace

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core import reducer
from omnigent_factory.core.effects import (
    Ack,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
    RetryableReadFailure,
)
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import MICROS_PER_HOUR, BotState, Hold, QueueStatus, Stage, Via
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter, ParcelBinding
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.testing.builders import result_candidate, snapshot
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
Q = "I_parcel_2"
NOTE_FIELD = "PVTF_note"

#: Comment templates the owner must read or act on; everything else is a card note.
KEPT_COMMENTS = {
    "checkpoint",
    "decision",
    "ready-blocked",
    "create-rejected",
    "adoption-ambiguous",
    "stop-unverified",
    "restart-exhausted",
}


def kinds(result):
    return [e.kind for e in result.effects]


def notes(result):
    return [e.args["note"] for e in Harness.of(result, EffectKind.SET_NOTE)]


def reactions(result):
    return [
        (e.args["comment_id"], e.args["content"])
        for e in Harness.of(result, EffectKind.REACT_COMMENT)
    ]


# ------------------------------------------------------------------ classification


def test_reducer_posts_only_the_kept_comment_templates():
    """Static guard: every ``ctx.comment(<template>)`` in the reducer is a kept one."""
    tree = ast.parse(inspect.getsource(reducer))
    used = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "comment"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    assert used == KEPT_COMMENTS


def test_queued_is_a_note_and_bot_queued_not_a_comment():
    h = Harness()
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(Q)
    r = h.send(P, ev.ApprovePlan(via=Via.DRAG))
    assert EffectKind.POST_COMMENT not in kinds(r)
    assert h.admission.queue_entry(P).status == QueueStatus.QUEUED
    assert h.p(P).bot == BotState.QUEUED
    assert {"bot": "Queued"} in [e.args for e in Harness.of(r, EffectKind.SET_BOT)]
    assert notes(r) == ["Queued: 2nd in line"]


def test_admission_clears_the_queued_note():
    h = Harness()
    h.plan_published()
    h.approve()
    assert h.p().note.startswith("Queued:")
    r = h.send(P, ev.CapacityAvailable())
    assert h.p().bot == BotState.WORKING and h.p().note == ""
    assert notes(r) == [""]


def test_stop_is_a_note_and_survives_the_drain():
    h = Harness()
    b = h.to_building()
    r = h.send(P, ev.Stop())
    assert EffectKind.POST_COMMENT not in kinds(r) and notes(r) == ["Stopped: /stop"]
    r = h.quiesce(P, b.session_id)  # drain finished: Working -> Idle keeps the reason
    assert h.p().bot == BotState.IDLE and h.p().note == "Stopped: /stop" and not notes(r)


def test_approval_acknowledged_is_a_note():
    h = Harness()
    h.plan_published()
    h.approve()
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND))
    assert r.audit.accepted and EffectKind.POST_COMMENT not in kinds(r)
    assert notes(r) == ["Approved: build starts when capacity allows"]


def test_unsupported_rework_is_a_note():
    h = Harness()
    h.to_building()
    h.parcels[P] = replace(h.p(), stage=Stage.READY)
    r = h.send(P, ev.RequestRework())
    assert EffectKind.POST_COMMENT not in kinds(r)
    assert Hold.UNSUPPORTED_REWORK in h.p().holds
    assert h.p().note.startswith("Rework refused:")


def test_invalid_result_is_blocked_with_a_note_not_a_comment():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestPlan())
    s = h.create_ok()
    bad = result_candidate(s.session_id, s.root_id, s.revision, ev.ResultKind.PLAN, valid=False)
    r = h.send(P, bad)
    assert Hold.RESULT_INVALID in h.p().holds and EffectKind.POST_COMMENT not in kinds(r)
    assert h.p().bot == BotState.BLOCKED and notes(r) == ["Blocked: stage result invalid"]


def test_refused_drag_is_a_note_and_rollback_without_comment():
    h = Harness()
    h.plan_published()
    h.send(P, ev.PlanFeedback(text_digest="x"))  # revision pending
    r = h.send(P, ev.ApprovePlan(via=Via.DRAG))
    assert not r.audit.accepted and EffectKind.POST_COMMENT not in kinds(r)
    assert notes(r) == ["Command refused: plan-not-approvable; card moved back to Scoped"]
    assert not reactions(r)  # a drag has no comment to react to


# ----------------------------------------------------------------- kept comments


def test_checkpoint_still_comments_and_notes():
    h = Harness()
    b = h.to_building()
    r = h.send(P, ev.ActiveLimitReached(session_id=b.session_id, grant_id=b.grant.grant_id))
    [c] = Harness.of(r, EffectKind.POST_COMMENT)
    assert c.args["template"] == "checkpoint"
    assert notes(r) == ["Checkpoint: comment /continue to grant more time"]


def test_restart_exhausted_still_comments_and_notes_the_block():
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        ev.ActiveTimeSample(
            session_id=b.session_id, grant_id=b.grant.grant_id, consumed_us=MICROS_PER_HOUR
        ),
    )
    h.send(P, ev.SessionCrashed(session_id=b.session_id))
    h.quiesce(P, b.session_id)
    s2 = h.create_ok()
    r = h.send(P, ev.SessionCrashed(session_id=s2.session_id))
    [c] = Harness.of(r, EffectKind.POST_COMMENT)
    assert c.args["template"] == "restart-exhausted"
    assert h.p().note == "Blocked: session stopped and could not be restarted"


# ------------------------------------------------------------ note only on change


def test_note_is_written_only_when_it_changes():
    h = Harness()
    h.plan_published()
    h.approve()
    assert h.p().board_note.startswith("Queued:")
    for _ in range(2):
        r = h.send(P, ev.ReconcileDue())
        assert EffectKind.SET_NOTE not in kinds(r)


def test_blocked_card_without_a_reason_gets_a_derived_note():
    h = Harness()
    h.eligible()
    h.parcels[P] = replace(h.p(), holds=frozenset({Hold.AGENT_BLOCKED}), bot=BotState.BLOCKED)
    r = h.send(P, ev.ReconcileDue())
    assert notes(r) == ["Blocked: agent reported blocked"]


def test_snapshot_corrects_note_drift_and_adopts_a_match():
    h = Harness()
    h.plan_published()
    h.approve()
    note = h.p().board_note
    f = h.f()
    drift = h.apply(
        f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, note="edited by hand"))
    )
    assert notes(drift) == [note]
    f = h.f()
    same = h.apply(f.make(ev.GitHubSnapshot(), evidence=snapshot(read_at_us=f.now, note=note)))
    assert EffectKind.SET_NOTE not in kinds(same)


def test_superseded_note_write_is_cancelled_at_the_call_boundary():
    h = Harness()
    h.plan_published()
    h.approve()
    p = h.p()
    stale = EffectIntent("ef_n", EffectKind.SET_NOTE, P, P, Preconditions(1, 0), {"note": "old"})
    current = EffectIntent(
        "ef_c", EffectKind.SET_NOTE, P, P, Preconditions(1, 0), {"note": p.board_note}
    )
    assert effect_still_valid(p, stale) == "note-superseded"
    assert effect_still_valid(p, current) is None


# -------------------------------------------------------------------- reactions


def _command(h: Harness, body: ev.EventBody, comment_id: int = 55):
    return h.apply(h.f().make(body, event_id=f"github:comment:{comment_id}:created"))


def test_accepted_command_comment_gets_a_thumbs_up():
    h = Harness()
    h.to_building()
    r = _command(h, ev.Stop())
    assert r.audit.accepted and reactions(r) == [(55, "+1")]
    assert EffectKind.POST_COMMENT not in kinds(r)


def test_refused_command_comment_gets_confused_and_a_note():
    h = Harness()
    h.eligible()
    r = _command(h, ev.Continue(), comment_id=56)
    assert not r.audit.accepted and reactions(r) == [(56, "confused")]
    assert notes(r) == ["Command refused: nothing-to-continue"]
    assert EffectKind.POST_COMMENT not in kinds(r)


def test_replayed_command_does_not_react_twice():
    h = Harness()
    h.to_building()
    event = h.f().make(ev.Stop(), event_id="github:comment:57:created")
    assert reactions(h.apply(event)) == [(57, "+1")]
    again = h.apply(event)
    assert again.duplicate and again.effects == ()


def test_free_form_feedback_and_non_comment_controls_get_no_reaction():
    h = Harness()
    h.plan_published()
    r = _command(h, ev.PlanFeedback(text_digest="x"), comment_id=58)
    assert not reactions(r)
    r = h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert not reactions(r)


# ---------------------------------------------------------------------- adapter

CTX = ExecutionContext("boot", 1, 1, 1)


def _adapter(http: httpx.AsyncClient) -> GitHubAPIAdapter:
    return GitHubAPIAdapter(
        GitHubClient(http, "token"),
        repository="SA-Ambulance/timesheets",
        repository_node_id="R_kgDOTC12Fg",
        project_node_id="PVT_1",
        status_field_node_id="STATUS",
        bot_user_id=777,
        required_checks=frozenset(),
        parcel_bindings={"I_1": ParcelBinding(12, "PVTI_1")},
        board_schema=BoardSchema("STATUS", {}, "BOT", {}, NOTE_FIELD),
    )


def _effect(kind: EffectKind, **args) -> EffectIntent:
    return EffectIntent("eff-1", kind, "I_1", "github", Preconditions(1, 1), args)


def _note_handler(current: str | None, calls: list[dict]):
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        if "mutation" in body["query"]:
            return httpx.Response(200, json={"data": {}})
        value = None if current is None else {"text": current, "field": {"id": NOTE_FIELD}}
        return httpx.Response(200, json={"data": {"node": {"fieldValueByName": value}}})

    return handler


@pytest.mark.asyncio
async def test_note_write_sets_text_adopts_a_match_and_clears_empty():
    calls: list[dict] = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(_note_handler("x", calls))) as c:
        result = await _adapter(c).execute(_effect(EffectKind.SET_NOTE, note="Queued"), CTX)
    assert isinstance(result, Ack)
    assert calls[-1]["variables"]["input"] == {
        "projectId": "PVT_1",
        "itemId": "PVTI_1",
        "fieldId": NOTE_FIELD,
        "value": {"text": "Queued"},
    }
    calls.clear()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_note_handler("Q", calls))) as c:
        result = await _adapter(c).execute(_effect(EffectKind.SET_NOTE, note="Q"), CTX)
    assert result == Ack("PVTI_1", {"adopted": True, "note": "Q"}) and len(calls) == 1
    calls.clear()
    async with httpx.AsyncClient(transport=httpx.MockTransport(_note_handler("Q", calls))) as c:
        result = await _adapter(c).execute(_effect(EffectKind.SET_NOTE, note=""), CTX)
    assert isinstance(result, Ack) and "clearProjectV2ItemFieldValue" in calls[-1]["query"]


@pytest.mark.asyncio
async def test_reaction_posts_once_and_uncertainty_retries_instead_of_blocking():
    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"id": 1})  # 200: the reaction already existed

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await _adapter(c).execute(
            _effect(EffectKind.REACT_COMMENT, comment_id=55, content="+1"), CTX
        )
    assert result == Ack("55", {"content": "+1"})
    assert seen == [
        ("/repos/SA-Ambulance/timesheets/issues/comments/55/reactions", {"content": "+1"})
    ]

    def lost(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("lost", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(lost)) as c:
        result = await _adapter(c).execute(
            _effect(EffectKind.REACT_COMMENT, comment_id=55, content="confused"), CTX
        )
    assert isinstance(result, RetryableReadFailure)

    def gone(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(gone)) as c:
        result = await _adapter(c).execute(
            _effect(EffectKind.REACT_COMMENT, comment_id=55, content="+1"), CTX
        )
    assert isinstance(result, DefinitiveFailure)
