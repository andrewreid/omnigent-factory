"""Adapter side of issue-session reuse: prepare a reused root for the next stage run,
switch stage policies add-before-delete, bind the caller identity, archive at the end."""

from __future__ import annotations

from dataclasses import replace

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind, RetryableReadFailure
from omnigent_factory.core.types import SessionKind
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.outcomes import observations
from tests.credentials.repos import GitEnv
from tests.omnigent.support import CTX, Rig, intent, make_rig, spec

pytestmark = pytest.mark.asyncio


async def _root_with_triage_run(rig: Rig) -> str:
    """Create the issue session for a triage run and prepare it."""
    rig.directory.specs["T1"] = replace(
        spec("T1", kind=SessionKind.TRIAGE), title="#42 · Fix the guard"
    )
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, "T1", nonce="nonce-abc"), CTX)
    assert isinstance(out, Ack) and out.remote_id is not None
    root = out.remote_id
    rig.directory.set_root("T1", root)
    prep = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, "T1", root_id=root), CTX)
    assert isinstance(prep, Ack) and prep.detail["ok"] is True
    # The triage run talked in this conversation.
    rig.server.sessions[root].items.append(
        {"id": "msg_1", "type": "message", "role": "assistant", "content": "triaged"}
    )
    return root


def _reuse_spec(rig: Rig, sid: str, root: str, kind: SessionKind, generation: int = 2) -> None:
    rig.directory.specs[sid] = replace(
        spec(sid, kind=kind, nonce=f"nonce-{sid}", policy_generation=generation),
        root_id=root,
        root_nonce="nonce-abc",
    )


def _family_rows(rig: Rig, root: str, family: str) -> list[dict[str, object]]:
    return [p for p in rig.server.policies[root] if pol.family_of(p["name"]) == family]


async def test_create_titles_the_issue_session(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    assert rig.server.sessions[root].title == "#42 · Fix the guard"


async def test_reused_root_with_history_prepares_the_next_run(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    _reuse_spec(rig, "B1", root, SessionKind.BUILD)
    prep = intent(EffectKind.PREPARE_SESSION, "B1", root_id=root, reuse=True)
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is True, outcome.detail
    assert outcome.detail["unexpected_turn"] is False and outcome.detail["unusable"] is False
    assert rig.broker.capabilities.get("B1") is not None  # the run's own capability
    # Without ``reuse`` the same history is an unexpected turn (fresh-root contract).
    _reuse_spec(rig, "B2", root, SessionKind.BUILD, generation=3)
    fresh = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, "B2", root_id=root), CTX)
    assert isinstance(fresh, Ack) and fresh.detail["unexpected_turn"] is True


async def test_stage_switch_replaces_github_policy_add_before_delete(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    [readonly] = _family_rows(rig, root, pol.GITHUB_NAME)
    assert readonly["factory_params"]["write_repos"] == []
    [caller] = _family_rows(rig, root, pol.CALLER_NAME)
    assert f'"{root}"' in str(caller["factory_params"]["expression"])
    rig.server.requests.clear()
    _reuse_spec(rig, "B1", root, SessionKind.BUILD)
    outcome = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "B1", root_id=root, reuse=True), CTX
    )
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is True
    writes = [
        (m, (b or {}).get("name") or p.rsplit("/", 1)[-1])
        for m, p, b in rig.server.requests
        if "/policies" in p and m != "GET"
    ]
    posts = [i for i, (m, _) in enumerate(writes) if m == "POST"]
    deletes = [i for i, (m, _) in enumerate(writes) if m == "DELETE"]
    assert posts and deletes and max(posts) < min(deletes)  # nothing removed before added
    [build] = _family_rows(rig, root, pol.GITHUB_NAME)
    assert build["factory_params"]["write_repos"] != []
    # The caller guard was never touched (same root, same literal) and is still exact.
    assert _family_rows(rig, root, pol.CALLER_NAME) == [caller]
    costs = [p for p in rig.server.policies[root] if pol.family_of(p["name"]) == pol.COST_PREFIX]
    assert [c["name"] for c in costs] == [pol.cost_policy_name(2)]


async def test_stage_switch_back_to_read_only_restores_the_read_only_policy(
    git_env: GitEnv,
) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    _reuse_spec(rig, "B1", root, SessionKind.BUILD)
    await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "B1", root_id=root, reuse=True), CTX
    )
    _reuse_spec(rig, "P2", root, SessionKind.PLAN, generation=3)
    outcome = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "P2", root_id=root, reuse=True), CTX
    )
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is True
    [github] = _family_rows(rig, root, pol.GITHUB_NAME)
    assert github["factory_params"]["write_repos"] == []


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda s: setattr(s, "archived", True), "archived"),
        (lambda s: setattr(s, "status", "failed"), "failed"),
        (lambda s: setattr(s, "agent_id", "ag_molly_old"), "another agent"),
        (
            lambda s: (
                setattr(s, "context_window", 200_000),
                setattr(s, "last_total_tokens", 170_000),
            ),
            "context rollover",
        ),
    ],
)
async def test_unusable_reused_root_is_reported_for_replacement(
    git_env: GitEnv, mutate, reason: str
) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    mutate(rig.server.sessions[root])
    _reuse_spec(rig, "P2", root, SessionKind.PLAN)
    prep = intent(EffectKind.PREPARE_SESSION, "P2", root_id=root, reuse=True)
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["unusable"] is True
    assert reason in str(outcome.detail["reason"]) and outcome.detail["ok"] is False
    [prepared] = observations(prep, outcome)
    assert isinstance(prepared, ev.Prepared) and prepared.unusable and prepared.reason
    assert rig.broker.capabilities.get("P2") is None  # nothing provisioned


async def test_missing_reused_root_is_unusable_and_context_below_watermark_is_fine(
    git_env: GitEnv,
) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    rig.server.sessions[root].context_window = 200_000
    rig.server.sessions[root].last_total_tokens = 100_000
    _reuse_spec(rig, "P2", root, SessionKind.PLAN)
    ok = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "P2", root_id=root, reuse=True), CTX
    )
    assert isinstance(ok, Ack) and ok.detail["ok"] is True
    del rig.server.sessions[root]
    _reuse_spec(rig, "B3", root, SessionKind.BUILD, generation=3)
    gone = await rig.adapter.execute(
        intent(EffectKind.PREPARE_SESSION, "B3", root_id=root, reuse=True), CTX
    )
    assert isinstance(gone, Ack) and gone.detail["unusable"] is True


async def test_boot_upgrade_reads_back_the_caller_guard(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    rig.server.policies[root] = [
        p for p in rig.server.policies[root] if pol.family_of(p["name"]) != pol.CALLER_NAME
    ]
    assert await rig.adapter.upgrade_static_policies("T1") is True
    [caller] = _family_rows(rig, root, pol.CALLER_NAME)
    assert caller["factory_params"] == dict(pol.caller_policy(root).factory_params)
    assert await rig.adapter.upgrade_static_policies("T1") is False


async def test_close_archives_the_issue_session(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    close = intent(EffectKind.CLOSE_SESSION, "T1", root_id=root)
    outcome = await rig.adapter.execute(close, CTX)
    assert isinstance(outcome, Ack) and rig.server.sessions[root].archived is True
    assert observations(close, outcome) == (ev.IssueSessionClosed(root_id=root),)
    again = await rig.adapter.execute(close, CTX)  # idempotent
    assert isinstance(again, Ack)
    del rig.server.sessions[root]
    assert isinstance(await rig.adapter.execute(close, CTX), Ack)  # gone counts as closed
    rig.server.faults[("PATCH", "/v1/sessions/conv_x")].append(500)
    other = intent(EffectKind.CLOSE_SESSION, "T1", root_id="conv_x")
    assert isinstance(await rig.adapter.execute(other, CTX), RetryableReadFailure)


async def test_verify_waits_the_barrier_then_checks_the_exact_set(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    barrier = rig.clock.now_utc_us() + 35_000_000
    verify = intent(EffectKind.VERIFY_POLICIES, "T1", root_id=root, not_before_us=barrier)
    early = await rig.adapter.execute(verify, CTX)
    assert isinstance(early, RetryableReadFailure) and early.retry_after_us == 35_000_000
    rig.clock.advance(35_000_000)
    ok = await rig.adapter.execute(verify, CTX)
    assert isinstance(ok, Ack) and ok.detail["ok"] is True
    assert observations(verify, ok) == (ev.PoliciesVerified(session_id="T1", ok=True),)
    # The caller guard disappears (e.g. removed by hand): verification fails closed.
    rig.server.policies[root] = [
        p for p in rig.server.policies[root] if pol.family_of(p["name"]) != pol.CALLER_NAME
    ]
    bad = await rig.adapter.execute(verify, CTX)
    assert isinstance(bad, Ack) and bad.detail["ok"] is False
    # Reconcile mode re-establishes it and reports a fresh barrier to wait.
    fix = intent(EffectKind.VERIFY_POLICIES, "T1", root_id=root, reconcile=True)
    out = await rig.adapter.execute(fix, CTX)
    assert isinstance(out, Ack) and out.detail["reconciled"] is True
    assert out.detail["ready_at_us"] == rig.clock.now_utc_us() + 35_000_000
    assert len(_family_rows(rig, root, pol.CALLER_NAME)) == 1
