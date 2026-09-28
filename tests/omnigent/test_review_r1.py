"""Regressions for review t3/initial (sha256 90bdc5c5...): findings 1, 3, 5, 6 and the
FOLLOW_UP guard. Each test fails against candidate tree 1d285c01 and passes after."""

from __future__ import annotations

import pytest

from omnigent_factory.core.effects import (
    Ack,
    DefinitiveFailure,
    EffectKind,
    RetryableReadFailure,
)
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.activity import ActivityTracker
from omnigent_factory.omnigent.observe import StreamNormalizer
from omnigent_factory.omnigent.tree import scan_tree
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import FakeSession, elicitation
from tests.omnigent.support import AGENT, CTX, Rig, intent, make_rig, spec

pytestmark = pytest.mark.asyncio

ROOT = "conv_root"
CHILD = "conv_child"


def _mirrored_rig(git_env: GitEnv) -> Rig:
    """Running root whose snapshot mirrors its child's prompt (helpers.py:1584-1660)."""
    rig = make_rig(git_env)
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, status="running"))
    rig.server.add(
        FakeSession(
            id=CHILD,
            agent_id=AGENT,
            parent_session_id=ROOT,
            status="waiting",
            pending_elicitations=[elicitation("elicit_c")],
        )
    )
    rig.directory.specs["S1"] = spec(root_id=ROOT)
    return rig


# ------------------------------------------------------------ 1. mirrored prompts


async def test_fixture_is_pinned_source_shaped(git_env: GitEnv) -> None:
    rig = _mirrored_rig(git_env)
    snap = await rig.rest.get_json(f"/v1/sessions/{ROOT}")
    (mirror,) = snap["pending_elicitations"]
    assert mirror["elicitation_id"] == "elicit_c"
    assert mirror["params"]["target_session_id"] == CHILD


async def test_mirrored_prompt_resolves_at_owning_child(git_env: GitEnv) -> None:
    rig = _mirrored_rig(git_env)
    effect = intent(
        EffectKind.RESOLVE_ELICITATION, id="ef_res", elicitation_id="elicit_c", decision_id="d"
    )
    rig.directory.answers["ef_res"] = {"answer": "B"}
    outcome = await rig.adapter.execute(effect, CTX)
    assert isinstance(outcome, Ack) and outcome.detail["node_id"] == CHILD
    method, path, body = rig.server.requests[-1]
    assert (method, path) == ("POST", f"/v1/sessions/{CHILD}/elicitations/elicit_c/resolve")
    assert body == {"action": "accept", "content": {"answer": "B"}}
    assert rig.server.misrouted == []
    assert rig.server.resolved == [(CHILD, "elicit_c", body)]


async def test_running_root_with_mirrored_prompt_stays_busy_and_productive(
    git_env: GitEnv,
) -> None:
    rig = _mirrored_rig(git_env)
    obs = await scan_tree(rig.rest, ROOT)
    root = obs.nodes[ROOT]
    assert not root.parked and root.busy and root.productive
    assert obs.busy and not obs.quiescent and obs.pending_waiter
    assert obs.nodes[CHILD].parked
    assert obs.owned_elicitations()["elicit_c"][0] == CHILD
    tracker = ActivityTracker(rig.clock, "S1", "gr")
    tracker.observe_tree(obs)
    rig.clock.advance(60_000_000)
    assert tracker.estimate().lower_us == 60_000_000  # root keeps accruing time
    normalizer = StreamNormalizer("S1", ROOT)
    [opened] = normalizer.on_snapshot(obs)  # deduplicated by the true owner
    assert (opened.session_id, opened.elicitation_id, opened.cost_ask) == ("S1", "elicit_c", False)
    assert opened.node_id == CHILD  # the child holds the prompt: the deeplink targets it


# ------------------------------------------------------------ 3. adoption tuple


async def _lost_create(rig: Rig) -> FakeSession:
    rig.directory.specs["S1"] = spec()
    rig.server.faults[("POST", "/v1/sessions")].append("timeout-after")
    await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    return next(iter(rig.server.sessions.values()))


@pytest.mark.parametrize(("field", "value"), [("host_id", "host_other"), ("project_id", "p2")])
async def test_wrong_host_or_project_is_not_adopted(
    git_env: GitEnv, field: str, value: str
) -> None:
    rig = make_rig(git_env)
    session = await _lost_create(rig)
    setattr(session, field, value)
    outcome = await rig.adapter.execute(
        intent(EffectKind.RECONCILE_SESSION, nonce="nonce-abc"), CTX
    )
    assert isinstance(outcome, Ack)
    assert outcome.detail["matches"] == 1 and outcome.detail["verified"] == 0
    assert outcome.detail["root_id"] is None


@pytest.mark.parametrize(("field", "value"), [("host_id", "host_other"), ("project_id", "p2")])
async def test_wrong_host_or_project_fails_preparation(
    git_env: GitEnv, field: str, value: str
) -> None:
    rig = make_rig(git_env)
    session = await _lost_create(rig)
    setattr(session, field, value)
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=session.id), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is False
    assert field.split("_", maxsplit=1)[0] in str(outcome.detail["reason"])
    assert rig.server.policies[session.id] == []


# ------------------------------------------------------------ 5. answer schema


def _typed_prompt(rig: Rig) -> None:
    prompt = elicitation("elicit_t")
    prompt["params"]["requestedSchema"] = {
        "type": "object",
        "properties": {
            "answer": {"type": "string", "enum": ["A", "B"]},
            "count": {"type": "integer"},
        },
        "required": ["answer"],
    }
    rig.server.add(FakeSession(id=ROOT, agent_id=AGENT, pending_elicitations=[prompt]))
    rig.directory.specs["S1"] = spec(root_id=ROOT)


@pytest.mark.parametrize(
    "answer",
    [{"answer": 42}, {"answer": "C"}, {"answer": "A", "count": "three"}, {"answer": True}],
)
async def test_wrong_type_or_enum_answer_is_refused(
    git_env: GitEnv, answer: dict[str, object]
) -> None:
    rig = make_rig(git_env)
    _typed_prompt(rig)
    rig.directory.answers["ef_t"] = answer  # type: ignore[assignment]
    effect = intent(
        EffectKind.RESOLVE_ELICITATION, id="ef_t", elicitation_id="elicit_t", decision_id="d"
    )
    outcome = await rig.adapter.execute(effect, CTX)
    assert isinstance(outcome, DefinitiveFailure) and "schema" in outcome.reason
    assert rig.server.resolved == []


async def test_conforming_typed_answer_is_sent(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    _typed_prompt(rig)
    rig.directory.answers["ef_t"] = {"answer": "B", "count": 3}
    effect = intent(
        EffectKind.RESOLVE_ELICITATION, id="ef_t", elicitation_id="elicit_t", decision_id="d"
    )
    assert isinstance(await rig.adapter.execute(effect, CTX), Ack)


# ------------------------------------------------------------ 6. CEL safety policy


async def _created(rig: Rig) -> str:
    rig.directory.specs["S1"] = spec()
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    assert isinstance(out, Ack) and out.remote_id is not None
    return out.remote_id


async def test_cel_safety_policy_attached_by_default(git_env: GitEnv) -> None:
    rig = make_rig(git_env)  # no CEL configuration supplied
    root = await _created(rig)
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is True
    cel = next(
        (p for p in rig.server.policies[root] if pol.family_of(p["name"]) == "factory-cel"), None
    )
    assert cel is not None and cel["handler"] == pol.CEL_HANDLER
    # The attached expression, evaluated by the pinned factory, denies the unsafe paths.
    from omnigent.policies.builtins.cel import cel_policy

    check = cel_policy(**cel["factory_params"])

    def verdict(name: str, args: dict[str, object]) -> str | None:
        out = check(
            {"type": "tool_call", "target": name, "data": {"name": name, "arguments": args}}
        )
        return None if out is None else out["result"]

    assert verdict("sys_os_shell", {"command": "gh pr merge 7 --squash"}) == "DENY"
    assert verdict("sys_os_shell", {"command": "git push origin HEAD:main"}) == "DENY"
    assert verdict("sys_os_shell", {"command": "gh api repos/o/r/rulesets -X POST"}) == "DENY"
    assert verdict("github__merge_pull_request", {"pull_number": 7}) == "DENY"
    assert verdict("github__push_files", {"branch": "main", "files": []}) == "DENY"
    assert verdict("sys_os_shell", {"command": "git push -u origin factory/issue-42-g1"}) == "ALLOW"
    assert verdict("github__push_files", {"branch": "factory/issue-42-g1"}) == "ALLOW"


async def test_preparation_fails_closed_when_cel_policy_cannot_attach(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    rig.server.faults[("POST", f"/v1/sessions/{root}/policies")].extend(
        [None, 500]  # github policy lands; the CEL policy POST fails
    )
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, RetryableReadFailure)
    posted = [
        pol.family_of(b["name"])
        for m, path, b in rig.server.requests
        if m == "POST" and "policies" in path
    ]
    assert posted == ["factory-github", "factory-cel"]  # the CEL attach failed; nothing later
    assert "factory-cel" not in {pol.family_of(p["name"]) for p in rig.server.policies[root]}


async def test_cel_policy_removed_later_is_caught_by_verification(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    rig.directory.set_root("S1", root)
    rig.server.policies[root] = [
        p for p in rig.server.policies[root] if pol.family_of(p["name"]) != "factory-cel"
    ]
    rig.server.faults[("POST", f"/v1/sessions/{root}/policies")].extend([None, 500])
    replace = intent(EffectKind.REPLACE_COST_POLICY, grant_id="g", generation=2, granted_us=1)
    assert not isinstance(await rig.adapter.execute(replace, CTX), Ack)


# ------------------------------------------------------------ FOLLOW_UP guard


async def test_volatile_ledger_cannot_be_wired_silently() -> None:
    from omnigent_factory.omnigent.directory import MemoryOwnItemLedger, VolatileStateError

    with pytest.raises(VolatileStateError):
        MemoryOwnItemLedger(volatile_ok=False)
    with pytest.raises(TypeError):
        MemoryOwnItemLedger()  # type: ignore[call-arg]
