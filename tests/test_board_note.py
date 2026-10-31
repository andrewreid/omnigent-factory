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
from omnigent_factory.core.projection import project_bot, project_note
from omnigent_factory.core.types import (
    MICROS_PER_HOUR,
    BotState,
    Hold,
    Lifecycle,
    QueueStatus,
    Reservation,
    ReservationKind,
    Stage,
    Via,
)
from omnigent_factory.github.adapter import BoardSchema, GitHubAPIAdapter, ParcelBinding
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.testing.builders import config, result_candidate, snapshot
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


def test_pr_cap_queue_notes_name_the_block_and_the_head():
    h = Harness(cfg=config(max_open_bot_prs=1, max_building=2))
    h.plan_published(P)
    h.plan_published(Q)
    h.approve(P)
    h.approve(Q)
    q = h.admission.queue_entry(Q)
    assert q is not None
    h.admission = replace(
        h.admission,
        open_bot_prs=frozenset({99}),
        reservations=(
            *h.admission.reservations,
            Reservation("rs-q-pr", Q, ReservationKind.OPEN_PR, q.approval_id, 99),
        ),
    )
    h.send(P, ev.ReconcileDue())
    h.send(Q, ev.ReconcileDue())
    assert h.p(P).note == "Queued: waiting for an open-PR slot (1/1 open)"
    assert h.p(Q).note == (
        f"Queued: 2nd in line behind #{h.p(P).issue_number}, which is waiting for "
        "an open-PR slot (1/1 open)"
    )


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
    r = h.send(P, ev.ApprovePlan(via=Via.DRAG))
    assert r.audit.accepted and EffectKind.POST_COMMENT not in kinds(r)
    assert notes(r) == ["Approved: build starts when capacity allows"]
    # /approve of the plan already approved is refused with the reason, still no comment.
    r = h.send(P, ev.ApprovePlan(via=Via.COMMAND))
    assert not r.audit.accepted and EffectKind.POST_COMMENT not in kinds(r)
    assert notes(r) == ["Nothing new to approve: this plan is already approved"]


def test_rework_is_a_note_not_a_comment():
    h = Harness()
    h.to_building()
    h.build_ready()
    r = h.send(P, ev.RequestRework())
    assert r.audit.accepted and EffectKind.POST_COMMENT not in kinds(r)
    assert h.p().stage == Stage.BUILDING and h.p().note == "Rework: owner feedback"


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
    assert notes(r) == [
        (
            "Cannot approve: "
            "the plan is being revised after your answer/feedback; card moved back to Scoped"
        )
    ]
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
    assert notes(r) == ["Cannot continue: this issue has no current run."]
    assert EffectKind.POST_COMMENT not in kinds(r)


def test_replayed_command_does_not_react_twice():
    h = Harness()
    h.to_building()
    event = h.f().make(ev.Stop(), event_id="github:comment:57:created")
    assert reactions(h.apply(event)) == [(57, "+1")]
    again = h.apply(event)
    assert again.duplicate and again.effects == ()


def test_free_form_feedback_gets_eyes_and_non_comment_controls_no_reaction():
    h = Harness()
    h.plan_published()
    r = _command(h, ev.PlanFeedback(text_digest="x"), comment_id=58)
    assert reactions(r) == [(58, "eyes")]  # taken; +1 once a run reads it
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


class _Reactions:
    """GitHub's reactions on comment 60: the bot (777) and another user both have 👀."""

    PATH = "/repos/SA-Ambulance/timesheets/issues/comments/60/reactions"

    def __init__(self, *, delete_status: int = 204, list_bot_eyes: bool = True) -> None:
        self.delete_status = delete_status
        self.listed: list[dict] = [{"id": 900, "content": "eyes", "user": {"id": 555}}]
        if list_bot_eyes:
            self.listed.append({"id": 901, "content": "eyes", "user": {"id": 777}})
        self.listed.append({"id": 902, "content": "+1", "user": {"id": 777}})
        self.calls: list[tuple[str, str]] = []
        self.fail_delete_once: Exception | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if request.method == "POST":
            return httpx.Response(200, json={"id": 902})
        if request.method == "GET":
            return httpx.Response(200, json=self.listed)
        if self.fail_delete_once is not None:
            exc, self.fail_delete_once = self.fail_delete_once, None
            raise exc
        reaction_id = int(request.url.path.rsplit("/", 1)[1])
        self.listed = [r for r in self.listed if r["id"] != reaction_id]
        return httpx.Response(self.delete_status)


def _read_ack() -> EffectIntent:
    return _effect(EffectKind.REACT_COMMENT, comment_id=60, content="+1", supersedes="eyes")


@pytest.mark.asyncio
async def test_read_thumbs_up_goes_up_first_then_removes_only_the_bots_eyes():
    gh = _Reactions()
    async with httpx.AsyncClient(transport=httpx.MockTransport(gh)) as c:
        result = await _adapter(c).execute(_read_ack(), CTX)
    assert result == Ack("60", {"content": "+1", "eyes_removed": True})
    assert gh.calls == [
        ("POST", gh.PATH),
        ("GET", gh.PATH),
        ("DELETE", f"{gh.PATH}/901"),  # never 900: another user's 👀
    ]
    assert [r["id"] for r in gh.listed] == [900, 902]


@pytest.mark.asyncio
async def test_eyes_already_gone_is_success():
    gh = _Reactions(delete_status=404)  # deleted between the list and the delete
    async with httpx.AsyncClient(transport=httpx.MockTransport(gh)) as c:
        result = await _adapter(c).execute(_read_ack(), CTX)
    assert isinstance(result, Ack) and result.detail["eyes_removed"] is True
    gone = _Reactions(list_bot_eyes=False)  # no 👀 of the bot's at all
    async with httpx.AsyncClient(transport=httpx.MockTransport(gone)) as c:
        result = await _adapter(c).execute(_read_ack(), CTX)
    assert result == Ack("60", {"content": "+1", "eyes_removed": False})
    assert [m for m, _ in gone.calls] == ["POST", "GET"]


@pytest.mark.asyncio
async def test_eyes_removal_interrupted_after_thumbs_up_completes_once_on_retry():
    gh = _Reactions()
    gh.fail_delete_once = httpx.ReadTimeout("lost", request=httpx.Request("DELETE", "/"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(gh)) as c:
        first = await _adapter(c).execute(_read_ack(), CTX)  # 👍 up, delete uncertain
        assert isinstance(first, RetryableReadFailure)
        # The daemon restarts: the unacked effect runs again on a fresh adapter.
        second = await _adapter(c).execute(_read_ack(), CTX)
        third = await _adapter(c).execute(_read_ack(), CTX)  # a late duplicate retry
    assert second == Ack("60", {"content": "+1", "eyes_removed": True})
    assert third == Ack("60", {"content": "+1", "eyes_removed": False})
    deletes = [p for m, p in gh.calls if m == "DELETE"]
    assert deletes == [f"{gh.PATH}/901", f"{gh.PATH}/901"]  # the lost one, then once
    assert [r["id"] for r in gh.listed] == [900, 902]


@pytest.mark.asyncio
async def test_eyes_removal_failure_never_fails_the_thumbs_up():
    refused = _Reactions(delete_status=403)
    async with httpx.AsyncClient(transport=httpx.MockTransport(refused)) as c:
        result = await _adapter(c).execute(_read_ack(), CTX)
    assert result == Ack("60", {"content": "+1", "eyes_removed": False})
    flaky = _Reactions(delete_status=502)  # server error: retried by the effect machinery
    async with httpx.AsyncClient(transport=httpx.MockTransport(flaky)) as c:
        result = await _adapter(c).execute(_read_ack(), CTX)
    assert isinstance(result, RetryableReadFailure)
    assert flaky.calls[0] == ("POST", flaky.PATH)


@pytest.mark.asyncio
async def test_command_thumbs_up_does_not_touch_eyes():
    gh = _Reactions()
    async with httpx.AsyncClient(transport=httpx.MockTransport(gh)) as c:
        result = await _adapter(c).execute(
            _effect(EffectKind.REACT_COMMENT, comment_id=60, content="+1"), CTX
        )
    assert result == Ack("60", {"content": "+1"})
    assert gh.calls == [("POST", gh.PATH)]


@pytest.mark.asyncio
async def test_feedback_eyes_reaction_posts_and_unknown_content_is_refused():
    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(201, json={"id": 2})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        result = await _adapter(c).execute(
            _effect(EffectKind.REACT_COMMENT, comment_id=58, content="eyes"), CTX
        )
        refused = await _adapter(c).execute(
            _effect(EffectKind.REACT_COMMENT, comment_id=58, content="heart"), CTX
        )
    assert result == Ack("58", {"content": "eyes"})
    assert seen == [
        ("/repos/SA-Ambulance/timesheets/issues/comments/58/reactions", {"content": "eyes"})
    ]
    assert isinstance(refused, DefinitiveFailure)


@pytest.mark.parametrize(
    "hold", [Hold.STOP_UNVERIFIED, Hold.REWORK_CONTROL_REQUIRED, Hold.EXTERNAL_ACTIVITY]
)
@pytest.mark.parametrize("existing", ["", "Starting: retrying unconfirmed session create"])
def test_create_deferred_preserves_projected_hold_note(hold, existing):
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    held = replace(h.p(), holds=frozenset({hold}), note="")
    bot = project_bot(held)
    expected = project_note(held, bot)
    h.parcels[P] = replace(held, bot=bot, note=existing, board_note=expected)
    for _ in range(2):
        result = h.send(
            P, ev.CreateDeferred(session_id=h.cur().session_id, reason="read timed out")
        )
        assert result.audit.accepted
        assert h.p().board_note == expected
        assert h.p().bot == bot
        assert not notes(result)


def test_create_retry_status_clears_on_success_and_triage_settlement():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    s = h.cur()
    h.send(P, ev.CreateDeferred(session_id=s.session_id, reason="read timed out"))
    assert "retrying" in h.p().board_note
    created = h.send(P, ev.SessionCreated(session_id=s.session_id, root_id="root", nonce=s.nonce))
    assert "retrying" not in h.p().board_note
    assert h.cur().create_retry_note == ""
    assert notes(created) == [""]
    h.create_ok()
    result = h.send(P, result_candidate(s.session_id, "root", s.revision, ev.ResultKind.TRIAGE))
    h.quiesce(P, s.session_id)
    for publish in h.of(result, EffectKind.PUBLISH_TRIAGE):
        h.send(
            P,
            ev.PublicationAcked(
                effect_id=publish.effect_id,
                effect_kind=publish.kind.value,
                session_id=s.session_id,
                comment_id="triage-comment",
            ),
        )
    assert h.p().bot == BotState.IDLE
    assert "retrying" not in h.p().board_note
    assert h.cur().create_retry_note == ""


@pytest.mark.parametrize(
    "legacy",
    [
        "Starting: retrying unconfirmed session create",
        "Starting: retrying session start (read timed out)",
    ],
)
def test_reconcile_repairs_stale_retry_note_on_idle_parcel(legacy):
    h = Harness()
    h.triage()
    assert h.p().bot == BotState.IDLE
    expected = project_note(replace(h.p(), note=""), BotState.IDLE)
    h.parcels[P] = replace(h.p(), note=legacy, board_note=legacy)
    result = h.send(P, ev.ReconcileDue())
    assert result.audit.accepted
    assert h.p().note == ""
    assert h.p().board_note == expected
    assert notes(result) == [expected]
    assert "retrying" not in expected
    assert not notes(h.send(P, ev.ReconcileDue()))


@pytest.mark.parametrize("end", ["reject", "cancel"])
def test_create_retry_status_clears_on_exhaustion_or_cancellation(end):
    h = Harness()
    h.eligible()
    result = h.send(P, ev.RequestTriage(via=Via.DRAG))
    [create] = h.of(result, EffectKind.CREATE_SESSION)
    s = h.cur()
    h.send(P, ev.CreateDeferred(session_id=s.session_id, reason="read timed out"))
    if end == "reject":
        h.send(P, ev.CreateRejected(session_id=s.session_id, reason="read timed out"))
    else:
        h.send(P, ev.LeftwardMove(to_stage=Stage.INBOX))
        h.send(
            P,
            ev.EffectCancelled(
                effect_id=create.effect_id, effect_kind=create.kind.value, session_id=s.session_id
            ),
        )
    assert h.p().session(s.session_id).create_retry_note == ""
    assert "retrying" not in h.p().board_note


def test_create_retry_status_does_not_override_checkpoint_projection():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    current = replace(
        h.cur(),
        lifecycle=Lifecycle.CHECKPOINT_WAIT,
        create_retry_note="Starting: retrying session start (read timed out)",
    )
    parcel = replace(
        h.p(), sessions=(current,), note="Starting: retrying unconfirmed session create"
    )
    assert project_bot(parcel) == BotState.CHECKPOINT
    assert project_note(parcel, BotState.CHECKPOINT).startswith("Checkpoint:")


def test_create_deferred_preserves_specific_working_note():
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    note = "Waiting: source maintenance"
    h.parcels[P] = replace(h.p(), note=note, board_note=note)
    result = h.send(P, ev.CreateDeferred(session_id=h.cur().session_id, reason="read timed out"))
    assert result.audit.accepted and h.p().bot == BotState.WORKING
    assert h.p().board_note == note and not notes(result)
