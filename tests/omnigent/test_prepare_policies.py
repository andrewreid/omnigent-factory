"""Launch barrier, worktree wiring, policy attach/replace and failure handling (§5.1-§5.2)."""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import (
    Ack,
    DefinitiveFailure,
    EffectKind,
    RetryableReadFailure,
)
from omnigent_factory.core.types import SessionKind
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.adapter import OmnigentConfig
from omnigent_factory.omnigent.outcomes import observations
from tests.credentials.repos import BOT, REPO, GitEnv, git
from tests.omnigent.support import CTX, Rig, intent, make_rig, spec

pytestmark = pytest.mark.asyncio


async def _created(rig: Rig, sid: str = "S1", **kw: object) -> str:
    rig.directory.specs[sid] = spec(sid, **kw)  # type: ignore[arg-type]
    nonce = rig.directory.specs[sid].nonce
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, sid, nonce=nonce), CTX)
    assert isinstance(out, Ack) and out.remote_id is not None
    rig.directory.set_root(sid, out.remote_id)
    return out.remote_id


def _names(rig: Rig, root: str) -> list[str]:
    return sorted(p["name"] for p in rig.server.policies[root])


async def test_prepare_wires_worktree_and_attaches_verified_policies(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    prep = intent(EffectKind.PREPARE_SESSION, root_id=root, profile="build")
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, Ack), outcome
    assert outcome.detail["ok"] is True and outcome.detail["unexpected_turn"] is False
    assert outcome.detail["policy_ready_at_us"] == rig.clock.now_utc_us() + 35_000_000
    assert observations(prep, outcome) == (
        ev.Prepared(session_id="S1", ok=True, unexpected_turn=False),
    )
    wt = rig.server.sessions[root].workspace
    assert wt is not None
    assert git("config", "--get", "factory.stageSession", cwd=Path(wt)) == "S1"
    assert git("config", "--get", "user.email", cwd=Path(wt)) == BOT.email
    rows = {p["name"]: p for p in rig.server.policies[root]}
    gh = rows["factory-github"]["factory_params"]
    assert gh["write_repos"] == [REPO] and gh["write_branches"] == ["factory/issue-42-g1"]
    # MCP surface only (the shell surface ASKed); force-push to the parcel branch is fine.
    assert gh["shell_tools"] == [] and gh["read_all"] is True
    assert not gh["deny_force_push"] and gh["deny_tag_push"] and not gh["allow_destructive"]
    assert rows["factory-github"]["handler"] == pol.GITHUB_HANDLER
    cost = rows["factory-cost-grant-0001"]
    assert cost["handler"] == pol.COST_HANDLER and cost["type"] == "python"
    assert cost["factory_params"] == {"ask_thresholds_usd": [140.0]}  # $35 x 4h, zero spend
    assert "max_cost_usd" not in cost["factory_params"]
    assert all(p["type"] == "python" for p in rig.server.policies[root])
    # Issuance stays disabled until the reducer's ENABLE_ISSUANCE.
    assert not rig.broker.issuance_enabled("S1")


async def test_plan_stage_gets_no_write_scope(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig, kind=SessionKind.PLAN)
    await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    gh = next(p for p in rig.server.policies[root] if p["name"] == "factory-github")
    assert (
        gh["factory_params"]["write_repos"] == [] and gh["factory_params"]["write_branches"] == []
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: setattr(s, "status", "running"),
        lambda s: s.items.append({"id": "msg_x", "type": "message", "role": "user", "content": []}),
        lambda s: s.pending_inputs.append({"pending_id": "p1", "content": []}),
        lambda s: setattr(s, "active_response_id", "resp_1"),
    ],
)
async def test_unexpected_turn_before_first_message_fences(git_env: GitEnv, mutate) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    mutate(rig.server.sessions[root])
    prep = intent(EffectKind.PREPARE_SESSION, root_id=root)
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["unexpected_turn"] is True
    assert observations(prep, outcome) == (
        ev.Prepared(session_id="S1", ok=True, unexpected_turn=True),
    )
    assert rig.server.policies[root] == [] and rig.broker.capabilities.get("S1") is None


@pytest.mark.parametrize(
    ("field", "value"),
    [("agent_id", "ag_other"), ("labels", {"factory.dispatch": "forged"}), ("project_id", "p2")],
)
async def test_prepare_rejects_wrong_identity(git_env: GitEnv, field: str, value: object) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    setattr(rig.server.sessions[root], field, value)
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is False


async def test_prepare_rejects_workspace_outside_clone(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    rig.server.sessions[root].workspace = str(git_env.home)
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is False
    assert "workspace" in str(outcome.detail["reason"])


async def test_policy_create_failure_leaves_prepare_unready(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    rig.server.faults[("POST", f"/v1/sessions/{root}/policies")].append("timeout-before")
    prep = intent(EffectKind.PREPARE_SESSION, root_id=root)
    outcome = await rig.adapter.execute(prep, CTX)
    assert isinstance(outcome, RetryableReadFailure)
    assert observations(prep, outcome) == ()
    # Re-running is safe: adopts what exists by exact name + params, creates the rest.
    again = await rig.adapter.execute(prep, CTX)
    assert isinstance(again, Ack) and again.detail["ok"] is True
    assert _names(rig, root) == ["factory-cel", "factory-cost-grant-0001", "factory-github"]


async def test_lost_policy_ack_is_adopted_not_duplicated(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    rig.server.faults[("POST", f"/v1/sessions/{root}/policies")].append("timeout-after")
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is True
    assert _names(rig, root) == ["factory-cel", "factory-cost-grant-0001", "factory-github"]


async def test_same_name_policy_with_other_params_blocks(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    rig.server.policies[root].append(
        {
            "id": "pol_x",
            "name": "factory-github",
            "type": "python",
            "handler": pol.GITHUB_HANDLER,
            "factory_params": {"read_all": True},
            "enabled": True,
            "source": "session",
        }
    )
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is False


async def _prepared(rig: Rig) -> str:
    root = await _created(rig)
    out = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(out, Ack) and out.detail["ok"] is True
    return root


async def test_replace_cost_policy_new_generation_then_delete_old(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _prepared(rig)
    rig.server.sessions[root].total_cost_usd = 12.5  # inclusive subtree spend
    replace = intent(
        EffectKind.REPLACE_COST_POLICY, grant_id="gr_2", generation=2, granted_us=2 * 3_600_000_000
    )
    outcome = await rig.adapter.execute(replace, CTX)
    assert isinstance(outcome, Ack), outcome
    assert _names(rig, root) == ["factory-cel", "factory-cost-grant-0002", "factory-github"]
    row = next(p for p in rig.server.policies[root] if p["name"] == "factory-cost-grant-0002")
    assert row["factory_params"] == {"ask_thresholds_usd": [82.5]}  # 12.5 + 35 x 2
    assert outcome.detail["spend_known"] is True
    # Cross-replica cache TTL (30 s): readiness is deferred past the barrier, not immediate.
    assert outcome.detail["ready_at_us"] == rig.clock.now_utc_us() + 35_000_000
    assert observations(replace, outcome) == ()  # no PolicyReady until the barrier passes
    order = [(m, p) for m, p, _ in rig.server.requests if "/policies" in p and m != "GET"]
    assert order[-2][0] == "POST" and order[-1][0] == "DELETE"


async def test_cost_policy_delete_failure_leaves_grant_unready(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _prepared(rig)
    old = next(p for p in rig.server.policies[root] if p["name"] == "factory-cost-grant-0001")
    rig.server.faults[("DELETE", f"/v1/sessions/{root}/policies/{old['id']}")].append(500)
    replace = intent(EffectKind.REPLACE_COST_POLICY, grant_id="g", generation=2, granted_us=1)
    outcome = await rig.adapter.execute(replace, CTX)
    assert isinstance(outcome, RetryableReadFailure)
    assert observations(replace, outcome) == ()
    # Retry converges: adopts generation 2, deletes generation 1, verifies.
    again = await rig.adapter.execute(replace, CTX)
    assert isinstance(again, Ack)
    assert _names(rig, root) == ["factory-cel", "factory-cost-grant-0002", "factory-github"]


async def test_cost_policy_create_failure_and_unknown_spend(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _prepared(rig)
    rig.server.faults[("POST", f"/v1/sessions/{root}/policies")].append(500)
    replace = intent(
        EffectKind.REPLACE_COST_POLICY, grant_id="g", generation=2, granted_us=3_600_000_000
    )
    assert isinstance(await rig.adapter.execute(replace, CTX), RetryableReadFailure)
    assert _names(rig, root) == ["factory-cel", "factory-cost-grant-0001", "factory-github"]
    rig.server.sessions[root].total_cost_usd = None  # unpriced: unknown, not zero
    outcome = await rig.adapter.execute(replace, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["spend_known"] is False


async def test_policy_list_failure_is_not_success(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _prepared(rig)
    rig.server.fail_paths.add(f"/v1/sessions/{root}/policies")
    replace = intent(EffectKind.REPLACE_COST_POLICY, grant_id="g", generation=2, granted_us=1)
    assert isinstance(await rig.adapter.execute(replace, CTX), RetryableReadFailure)


async def test_conflicting_cost_generation_is_definitive(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _prepared(rig)
    rig.server.policies[root].append(
        {
            "id": "pol_y",
            "name": "factory-cost-grant-0002",
            "type": "python",
            "handler": pol.COST_HANDLER,
            "factory_params": {"ask_thresholds_usd": [1.0]},
            "enabled": True,
            "source": "session",
        }
    )
    replace = intent(EffectKind.REPLACE_COST_POLICY, grant_id="g", generation=2, granted_us=1)
    assert isinstance(await rig.adapter.execute(replace, CTX), DefinitiveFailure)


async def test_barrier_exceeds_policy_cache_ttl_and_threshold_math() -> None:
    assert OmnigentConfig(agent_id="a", host_id="h", repository=REPO).policy_barrier_us > 30_000_000
    assert pol.cost_threshold_usd(None, 6 * 3_600_000_000) == 210.0  # 6h not capped at $70
    assert pol.cost_threshold_usd(0.004, 0) == 0.004
    assert pol.is_cost_ask("factory-cost-grant-0003") and not pol.is_cost_ask("factory-github")


async def test_operator_cel_must_compile_and_is_attached_in_addition(git_env: GitEnv) -> None:
    with pytest.raises(ValueError, match="CEL"):
        make_rig(git_env, cel_expression="event.type ==")
    with pytest.raises(ValueError, match="required"):
        make_rig(git_env, cel_expression="   ")
    rig = make_rig(git_env, cel_expression='{"result": "ALLOW"}')
    root = await _created(rig)
    await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    rows = {p["name"]: p for p in rig.server.policies[root]}
    assert rows["factory-cel"]["factory_params"]["expression"] == pol.factory_cel_expression()
    assert rows["factory-cel-operator"]["handler"] == pol.CEL_HANDLER
    assert rows["factory-cel-operator"]["factory_params"]["expression"] == '{"result": "ALLOW"}'


async def test_boot_upgrades_a_live_sessions_old_factory_policies_in_place(
    git_env: GitEnv,
) -> None:
    """#677: sessions prepared before the liberal-policy change keep the ASKing shell
    surface; boot replaces the daemon-owned static policies without touching cost."""
    rig = make_rig(git_env)
    root = await _created(rig)
    out = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(out, Ack) and out.detail["ok"] is True
    rows = rig.server.policies[root]
    github = next(r for r in rows if r["name"] == "factory-github")
    github["factory_params"] = {**github["factory_params"], "read_all": False}
    github["factory_params"].pop("shell_tools")  # pre-change parameters
    cost_before = [r for r in rows if r["name"].startswith(pol.COST_PREFIX)]
    assert await rig.adapter.upgrade_static_policies("S1") is True
    after = rig.server.policies[root]
    upgraded = next(r for r in after if r["name"] == "factory-github")
    assert upgraded["factory_params"]["shell_tools"] == []
    assert [r for r in after if r["name"].startswith(pol.COST_PREFIX)] == cost_before
    assert sorted(r["name"] for r in after).count("factory-github") == 1
    assert await rig.adapter.upgrade_static_policies("S1") is False  # idempotent
