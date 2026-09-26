"""Empty-session create, ambiguity, nonce adoption and orphan worktrees (§5.1, §3.4)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectKind,
    RetryableReadFailure,
)
from omnigent_factory.omnigent.outcomes import observations
from omnigent_factory.ports.omnigent import OmnigentAdapter
from tests.credentials.repos import GitEnv, git
from tests.omnigent.fake_server import FakeSession
from tests.omnigent.support import AGENT, CTX, HOST, PROJECT, intent, make_rig, spec

pytestmark = pytest.mark.asyncio


async def test_adapter_satisfies_port(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    assert isinstance(rig.adapter, OmnigentAdapter)


async def test_create_posts_empty_session_with_nonce_and_project(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    create = intent(EffectKind.CREATE_SESSION, nonce="nonce-abc")
    outcome = await rig.adapter.execute(create, CTX)
    assert isinstance(outcome, Ack)
    method, path, body = rig.server.requests[-1]
    assert (method, path) == ("POST", "/v1/sessions")
    assert body["initial_items"] == []
    assert body["agent_id"] == AGENT and body["host_id"] == HOST and body["host_type"] == "external"
    assert body["project_id"] == PROJECT
    assert body["labels"]["factory.dispatch"] == "nonce-abc"
    assert body["labels"]["factory.stage"] == "build"
    assert body["git"] == {"branch_name": "factory/issue-42-g1", "base_branch": "origin/main"}
    assert body["workspace"] == str(git_env.source.resolve())
    # Explicit fetch before branching; the actual base OID is recorded.
    assert outcome.detail["base_oid"] == git("rev-parse", "main", cwd=git_env.remote)
    assert outcome.remote_id is not None
    (created,) = observations(create, outcome)
    assert created == ev.SessionCreated(
        session_id="S1", root_id=outcome.remote_id, nonce="nonce-abc"
    )
    assert rig.server.count("POST", "/v1/sessions") == 1


async def test_bind_mode_for_successor_uses_existing_worktree_without_base(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    first = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    assert isinstance(first, Ack)
    wt = Path(str(first.detail["workspace"]))
    rig.directory.specs["S2"] = spec("S2", nonce="nonce-2", bind_worktree=wt)
    second = await rig.adapter.execute(
        intent(EffectKind.CREATE_SESSION, "S2", nonce="nonce-2"), CTX
    )
    assert isinstance(second, Ack)
    body = rig.server.requests[-1][2]
    assert body["git"] == {"branch_name": "factory/issue-42-g1", "existing_worktree": True}
    assert "base_branch" not in body["git"] and body["workspace"] == str(wt.resolve())


@pytest.mark.parametrize("fault", ["timeout-after", "500-after", "timeout-before", 409, 400])
async def test_ambiguous_create_is_never_retried(git_env: GitEnv, fault: object) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("POST", "/v1/sessions")].append(fault)
    create = intent(EffectKind.CREATE_SESSION, nonce="nonce-abc")
    outcome = await rig.adapter.execute(create, CTX)
    assert isinstance(outcome, AmbiguousWrite)
    assert rig.server.count("POST", "/v1/sessions") == 1
    assert observations(create, outcome) == (
        ev.EffectUnknown(create.effect_id, EffectKind.CREATE_SESSION.value, "S1"),
    )


@pytest.mark.parametrize("status", [401, 403, 404, 422, 429])
async def test_pre_side_effect_rejection_is_definitive(git_env: GitEnv, status: int) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("POST", "/v1/sessions")].append(status)
    create = intent(EffectKind.CREATE_SESSION, nonce="nonce-abc")
    outcome = await rig.adapter.execute(create, CTX)
    assert isinstance(outcome, DefinitiveFailure)
    (rejected,) = observations(create, outcome)
    assert isinstance(rejected, ev.CreateRejected) and rejected.session_id == "S1"


async def test_lost_ack_adopted_by_nonce_across_pages_and_archive(git_env: GitEnv) -> None:
    rig = make_rig(git_env, page_limit=2)
    for i in range(5):  # unrelated sessions push ours onto a later page
        rig.server.add(FakeSession(id=f"conv_other{i}", agent_id=AGENT, archived=i % 2 == 0))
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("POST", "/v1/sessions")].append("timeout-after")
    await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    ours = next(s for s in rig.server.sessions.values() if s.labels.get("factory.dispatch"))
    ours.archived = True  # archived roots are still searched
    reconcile = intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc")
    outcome = await rig.adapter.execute(reconcile, CTX)
    assert isinstance(outcome, Ack)
    assert outcome.detail["matches"] == 1 and outcome.detail["root_id"] == ours.id
    listed = [p for m, p, _ in rig.server.requests if m == "GET" and p == "/v1/sessions"]
    assert len(listed) >= 3  # walked every page
    (adopt,) = observations(reconcile, outcome)
    assert adopt == ev.AdoptionResult(
        session_id="S1", matches=1, root_id=ours.id, nonce="nonce-abc"
    )
    assert rig.server.count("POST", "/v1/sessions") == 1


async def test_forged_or_conflicting_nonce_matches_are_not_adopted(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    rig.server.add(
        FakeSession(id="conv_forged", agent_id="ag_other", labels={"factory.dispatch": "nonce-abc"})
    )
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc"), CTX
    )
    assert isinstance(outcome, Ack)
    assert outcome.detail["matches"] == 2 and outcome.detail["root_id"] is None


async def test_single_unverified_match_is_not_adopted(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.add(
        FakeSession(
            id="conv_lookalike",
            agent_id=AGENT,
            labels={"factory.dispatch": "nonce-abc"},
            workspace=str(git_env.home),
            git_branch="factory/issue-42-g1",
        )
    )
    reconcile = intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc")
    outcome = await rig.adapter.execute(reconcile, CTX)
    assert isinstance(outcome, Ack)
    assert outcome.detail["matches"] == 1 and outcome.detail["root_id"] is None
    (adopt,) = observations(reconcile, outcome)
    assert isinstance(adopt, ev.AdoptionResult) and adopt.root_id is None


async def test_orphan_worktree_is_reported_and_blocks_a_second_create(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("POST", "/v1/sessions")].append("worktree-then-500")
    first = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    assert isinstance(first, AmbiguousWrite)
    assert rig.server.sessions == {}
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc"), CTX
    )
    assert isinstance(outcome, Ack)
    assert outcome.detail["matches"] == 0 and outcome.detail["root_id"] is None
    assert outcome.detail["branch_exists"] is True
    assert str(outcome.detail["orphan_worktree"]).endswith("factory-issue-42-g1")
    # Never delete/recreate: another create for that branch is refused locally, no POST.
    again = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    assert isinstance(again, DefinitiveFailure) and "branch-already-exists" in again.reason
    assert rig.server.count("POST", "/v1/sessions") == 1
    assert Path(str(outcome.detail["orphan_worktree"])).is_dir()


async def test_incomplete_nonce_search_is_retryable_not_zero(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    rig.server.fail_paths.add("/v1/sessions")
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc"), CTX
    )
    assert isinstance(outcome, RetryableReadFailure)
    assert observations(intent(EffectKind.RECONCILE_SESSION, nonce="n"), outcome) == ()


async def test_create_refuses_mismatched_spec_without_posting(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec()
    outcome = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="other"), CTX)
    assert isinstance(outcome, DefinitiveFailure)
    assert rig.server.requests == []
