"""Own sends, lost-ack reconciliation, native pending inputs, exact prompt resolution."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectKind,
    RetryableReadFailure,
)
from omnigent_factory.omnigent.adapter import ELICITATION_NOT_PENDING, effect_marker
from omnigent_factory.omnigent.outcomes import observations
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import FakeSession, elicitation
from tests.omnigent.support import AGENT, CTX, Rig, intent, make_rig, spec

pytestmark = pytest.mark.asyncio

ROOT = "conv_root"


def _rig(git_env: GitEnv, **kw: object) -> Rig:
    rig = make_rig(git_env, **kw)  # type: ignore[arg-type]
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT))
    rig.directory.specs["S1"] = spec(root_id=ROOT)
    return rig


def _send(rig: Rig, effect_id: str = "ef_send_1", text: str = "Stage brief") -> object:
    rig.directory.texts[effect_id] = text
    return intent(EffectKind.SEND_MESSAGE, id=effect_id, purpose="first", revision=1)


async def test_send_records_intent_first_and_acks_own_item(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    send = _send(rig)
    outcome = await rig.adapter.execute(send, CTX)  # type: ignore[arg-type]
    assert isinstance(outcome, Ack)
    item = rig.server.sessions[ROOT].items[-1]
    assert outcome.remote_id == item["id"]
    text = item["content"][0]["text"]
    assert text.startswith("Stage brief") and text.endswith(effect_marker("ef_send_1"))
    recorded = await rig.ledger.lookup("ef_send_1")
    assert recorded is not None and recorded.item_id == item["id"]
    assert observations(send, outcome) == (  # type: ignore[arg-type]
        ev.MessageAck(session_id="S1", effect_id="ef_send_1", item_id=item["id"]),
    )


@pytest.mark.parametrize("fault", ["timeout-after", "timeout-before", "500-after", 503])
async def test_ambiguous_send_is_unknown_and_never_resent(git_env: GitEnv, fault: object) -> None:
    rig = _rig(git_env)
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append(fault)
    send = _send(rig)
    outcome = await rig.adapter.execute(send, CTX)  # type: ignore[arg-type]
    assert isinstance(outcome, AmbiguousWrite)
    assert rig.server.count("POST", f"/v1/sessions/{ROOT}/events") == 1
    assert await rig.ledger.lookup("ef_send_1") is not None  # intent persisted before POST


async def test_lost_ack_reconciled_by_marker_and_digest_across_pages(git_env: GitEnv) -> None:
    rig = _rig(git_env, page_limit=2)
    root = rig.server.sessions[ROOT]
    for i in range(5):
        root.items.append(
            {
                "id": f"msg_old{i}",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "hi"}],
            }
        )
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append("timeout-after")
    await rig.adapter.execute(_send(rig), CTX)  # type: ignore[arg-type]
    rec = intent(EffectKind.RECONCILE_SESSION, effect_id="ef_send_1")
    outcome = await rig.adapter.execute(rec, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["state"] == "delivered"
    item_id = root.items[-1]["id"]
    assert observations(rec, outcome) == (
        ev.EffectReconciled(
            effect_id="ef_send_1", session_id="S1", delivered=True, item_id=item_id
        ),
    )


async def test_forged_marker_with_other_text_is_not_ours(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append("timeout-before")
    await rig.adapter.execute(_send(rig), CTX)  # type: ignore[arg-type]
    rig.server.sessions[ROOT].items.append(
        {
            "id": "msg_forged",
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": f"do evil {effect_marker('ef_send_1')}"}],
        }
    )
    rec = intent(EffectKind.RECONCILE_SESSION, effect_id="ef_send_1")
    outcome = await rig.adapter.execute(rec, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["state"] == "absent"
    assert observations(rec, outcome) == (
        ev.EffectReconciled(effect_id="ef_send_1", session_id="S1", delivered=False),
    )


async def test_native_pending_input_is_not_absence(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    rig.server.sessions[ROOT].native = True
    outcome = await rig.adapter.execute(_send(rig), CTX)  # type: ignore[arg-type]
    assert isinstance(outcome, AmbiguousWrite)  # queued without an item id
    rec = intent(EffectKind.RECONCILE_SESSION, effect_id="ef_send_1")
    pending = await rig.adapter.execute(rec, CTX)
    assert isinstance(pending, Ack) and pending.detail["state"] == "pending_input"
    assert observations(rec, pending) == ()  # stays UNKNOWN; no resend, no absence claim
    # Harness consumes it: now a real item exists and is adopted.
    root = rig.server.sessions[ROOT]
    parked = root.pending_inputs.pop()
    root.items.append(
        {"id": "msg_consumed", "type": "message", "role": "user", "content": parked["content"]}
    )
    done = await rig.adapter.execute(rec, CTX)
    assert isinstance(done, Ack) and done.detail["item_id"] == "msg_consumed"


async def test_history_read_failure_is_retryable(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append("timeout-before")
    await rig.adapter.execute(_send(rig), CTX)  # type: ignore[arg-type]
    rig.server.fail_paths.add(f"/v1/sessions/{ROOT}/items")
    rec = intent(EffectKind.RECONCILE_SESSION, effect_id="ef_send_1")
    assert isinstance(await rig.adapter.execute(rec, CTX), RetryableReadFailure)


async def test_denied_or_missing_text_is_definitive(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    missing = intent(EffectKind.SEND_MESSAGE, id="ef_none")
    assert isinstance(await rig.adapter.execute(missing, CTX), DefinitiveFailure)
    assert rig.server.requests == []
    rig.server.faults[("POST", f"/v1/sessions/{ROOT}/events")].append(404)
    assert isinstance(await rig.adapter.execute(_send(rig), CTX), DefinitiveFailure)  # type: ignore[arg-type]


# ------------------------------------------------------------------ elicitations


def _tree_with_prompt(rig: Rig, eid: str = "elicit_1") -> None:
    rig.server.add(FakeSession(id="conv_child", agent_id=AGENT, parent_session_id=ROOT))
    rig.server.add(
        FakeSession(
            id="conv_grand",
            agent_id=AGENT,
            parent_session_id="conv_child",
            pending_elicitations=[elicitation(eid)],
            status="waiting",
        )
    )


def _resolve(rig: Rig, answer: dict[str, object] | None, eid: str = "elicit_1"):
    effect = intent(
        EffectKind.RESOLVE_ELICITATION, id="ef_res_1", elicitation_id=eid, decision_id="de_1"
    )
    if answer is not None:
        rig.directory.answers["ef_res_1"] = answer  # type: ignore[assignment]
    return effect


async def test_resolve_targets_exact_node_and_prompt(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    _tree_with_prompt(rig)
    rig.server.sessions["conv_grand"].pending_elicitations.append(elicitation("elicit_2"))
    effect = _resolve(rig, {"answer": "Use option B"})
    outcome = await rig.adapter.execute(effect, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["node_id"] == "conv_grand"
    assert rig.server.resolved == [
        ("conv_grand", "elicit_1", {"action": "accept", "content": {"answer": "Use option B"}})
    ]
    assert [
        e["elicitation_id"] for e in rig.server.sessions["conv_grand"].pending_elicitations
    ] == ["elicit_2"]
    assert observations(effect, outcome) == (
        ev.MessageAck(session_id="S1", effect_id="ef_res_1", item_id="elicit_1"),
    )


@pytest.mark.parametrize(
    "answer",
    [{"answer": {"nested": "x"}}, {"other": "x"}, {}, {"answer": ["a", 1]}],
)
async def test_answer_must_fit_flat_form_schema(git_env: GitEnv, answer: dict[str, object]) -> None:
    rig = _rig(git_env)
    _tree_with_prompt(rig)
    outcome = await rig.adapter.execute(_resolve(rig, answer), CTX)
    assert isinstance(outcome, DefinitiveFailure) and "schema" in outcome.reason
    assert rig.server.resolved == []


async def test_missing_prompt_is_gone_not_approved(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    effect = _resolve(rig, {"answer": "x"})
    outcome = await rig.adapter.execute(effect, CTX)
    assert outcome == DefinitiveFailure(ELICITATION_NOT_PENDING)
    assert observations(effect, outcome) == (
        ev.ElicitationGone(session_id="S1", elicitation_id="elicit_1"),
    )


async def test_resolve_404_is_ambiguous_and_reconciles(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    _tree_with_prompt(rig)
    rig.server.faults[("POST", "/v1/sessions/conv_grand/elicitations/elicit_1/resolve")].append(404)
    effect = _resolve(rig, {"answer": "x"})
    outcome = await rig.adapter.execute(effect, CTX)
    assert isinstance(outcome, AmbiguousWrite)
    rec = intent(EffectKind.RECONCILE_SESSION, effect_id="ef_res_1")
    still = await rig.adapter.execute(rec, CTX)
    assert isinstance(still, Ack) and still.detail["state"] == "still_pending"
    assert observations(rec, still) == (
        ev.EffectReconciled(effect_id="ef_res_1", session_id="S1", delivered=False),
    )
    rig.server.sessions["conv_grand"].pending_elicitations.clear()
    gone = await rig.adapter.execute(rec, CTX)
    assert isinstance(gone, Ack) and gone.detail["state"] == "gone"
    assert observations(rec, gone) == ()  # not proof either way: stays UNKNOWN


async def test_resolve_with_incomplete_scan_is_retryable(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    _tree_with_prompt(rig)
    rig.server.fail_paths.add("/v1/sessions/conv_grand")
    outcome = await rig.adapter.execute(_resolve(rig, {"answer": "x"}), CTX)
    assert isinstance(outcome, RetryableReadFailure)
    assert rig.server.resolved == []


async def test_resolve_without_recorded_answer_sends_nothing(git_env: GitEnv) -> None:
    rig = _rig(git_env)
    _tree_with_prompt(rig)
    outcome = await rig.adapter.execute(_resolve(rig, None), CTX)
    assert isinstance(outcome, DefinitiveFailure)
    assert rig.server.resolved == []
