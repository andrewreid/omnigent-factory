"""Regressions for review t3/recheck-1 (sha256 53ae4fb1...): late mirrored prompt owners,
config-source environment at preparation, and the non-replaceable CEL rule. Each test
fails against candidate tree e631cba4 and passes after."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.omnigent.observe import StreamNormalizer
from omnigent_factory.omnigent.rest import OmnigentRest
from omnigent_factory.omnigent.tree import scan_tree
from tests.credentials.repos import GitEnv
from tests.omnigent.fake_server import elicitation
from tests.omnigent.support import CTX, Rig, intent, make_rig, spec

pytestmark = pytest.mark.asyncio

ROOT = "root"
LATE = "late-child"
EMPTY_PAGE = {"object": "list", "data": [], "first_id": None, "last_id": None, "has_more": False}


def _snapshot(sid: str, status: str, prompts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": sid,
        "agent_id": "ag_molly",
        "status": status,
        "created_at": 1,
        "labels": {},
        "items": [],
        "pending_elicitations": prompts,
        "pending_inputs": [],
        "archived": False,
    }


def _mirror(eid: str, target: str) -> dict[str, Any]:
    event = elicitation(eid)
    event["params"]["target_session_id"] = target
    return event


def _late_child_rest(
    snapshots: dict[str, Callable[[], httpx.Response]],
) -> OmnigentRest:
    """Reviewer's fixture: children and inventory were enumerated *before* the late child
    existed (empty pages); the root snapshot, read afterwards, mirrors its prompt."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/child_sessions") or path == "/v1/sessions":
            return httpx.Response(200, json=EMPTY_PAGE)
        sid = path.removeprefix("/v1/sessions/")
        if sid in snapshots:
            return snapshots[sid]()
        return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})

    return OmnigentRest("http://o.test", transport=httpx.MockTransport(handler))


def _ok(body: dict[str, Any]) -> Callable[[], httpx.Response]:
    return lambda: httpx.Response(200, json=body)


def _root_with_late_mirror() -> Callable[[], httpx.Response]:
    return _ok(_snapshot(ROOT, "idle", [_mirror("e_late", LATE)]))


async def test_late_mirrored_owner_is_read_and_counted() -> None:
    rest = _late_child_rest(
        {
            ROOT: _root_with_late_mirror(),
            LATE: _ok(_snapshot(LATE, "waiting", [elicitation("e_late")])),
        }
    )
    obs = await scan_tree(rest, ROOT)
    assert LATE in obs.nodes
    assert obs.complete and obs.pending_waiter
    assert obs.owned_elicitations()["e_late"][0] == LATE
    normalizer = StreamNormalizer("S1", ROOT, seen_elicitations={"e_late"})
    assert normalizer.on_snapshot(obs, open_elicitations={"e_late"}) == []


async def test_late_running_owner_makes_the_tree_busy() -> None:
    rest = _late_child_rest(
        {
            ROOT: _root_with_late_mirror(),
            LATE: _ok(_snapshot(LATE, "running", [])),  # prompt answered, child working
        }
    )
    obs = await scan_tree(rest, ROOT)
    assert obs.busy and not obs.quiescent


@pytest.mark.parametrize("status", [404, 503])
async def test_unreadable_late_owner_is_incomplete_never_idle_or_gone(status: int) -> None:
    rest = _late_child_rest(
        {
            ROOT: _root_with_late_mirror(),
            LATE: lambda: httpx.Response(status, json={"error": {"code": "x", "message": "x"}}),
        }
    )
    obs = await scan_tree(rest, ROOT)
    assert not obs.complete and not obs.quiescent
    assert not obs.to_scan().complete
    normalizer = StreamNormalizer("S1", ROOT)
    assert not any(
        isinstance(e, ev.ElicitationGone)
        for e in normalizer.on_snapshot(obs, open_elicitations={"e_late"})
    )


async def test_discoveries_are_closed_over_transitively() -> None:
    rest = _late_child_rest(
        {
            ROOT: _root_with_late_mirror(),
            LATE: _ok(_snapshot(LATE, "idle", [_mirror("e_grand", "late-grand")])),
            "late-grand": _ok(_snapshot("late-grand", "waiting", [elicitation("e_grand")])),
        }
    )
    obs = await scan_tree(rest, ROOT)
    assert obs.complete and {"late-child", "late-grand"} <= set(obs.nodes)
    assert obs.owned_elicitations()["e_grand"][0] == "late-grand"


async def test_late_owner_beyond_node_ceiling_is_incomplete() -> None:
    rest = _late_child_rest(
        {
            ROOT: _root_with_late_mirror(),
            LATE: _ok(_snapshot(LATE, "waiting", [elicitation("e_late")])),
        }
    )
    obs = await scan_tree(rest, ROOT, max_nodes=1)
    assert not obs.complete and "node-ceiling" in obs.errors
    normalizer = StreamNormalizer("S1", ROOT)
    out = normalizer.on_snapshot(obs, open_elicitations={"e_late"})
    assert not any(isinstance(e, ev.ElicitationGone) for e in out)


# ------------------------------------------------------------ config-source environment


async def test_preparation_fails_when_daemon_git_config_sources_differ(
    git_env: GitEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = make_rig(git_env)
    root = await _created(rig)
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is False
    assert "config-source" in str(outcome.detail["reason"])
    assert rig.server.policies[root] == []


# ------------------------------------------------------------ CEL rule is not replaceable


async def _created(rig: Rig) -> str:
    rig.directory.specs["S1"] = spec()
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    assert isinstance(out, Ack) and out.remote_id is not None
    return out.remote_id


@pytest.mark.parametrize("override", ["1 / 0", '{"result":"ALLOW"}'])
async def test_operator_cel_cannot_disable_the_fixed_rule(git_env: GitEnv, override: str) -> None:
    from omnigent.policies.builtins.cel import cel_policy

    rig = make_rig(git_env, cel_expression=override)
    root = await _created(rig)
    outcome = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(outcome, Ack) and outcome.detail["ok"] is True
    rows = {p["name"]: p for p in rig.server.policies[root]}
    merge = {
        "type": "tool_call",
        "target": "sys_os_shell",
        "data": {"name": "sys_os_shell", "arguments": {"command": "gh pr merge 7 --squash"}},
    }
    fixed = cel_policy(**rows["factory-cel"]["factory_params"])(merge)  # type: ignore[arg-type]
    assert fixed is not None and fixed["result"] == "DENY"
    assert rows["factory-cel-operator"]["factory_params"]["expression"] == override
