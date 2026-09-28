"""The per-session factory caller-identity policy, evaluated by Omnigent's own CEL builtin
(``omnigent.policies.builtins.cel.cel_policy`` from the pinned dependency)."""

from __future__ import annotations

import itertools

import pytest
from omnigent.policies.builtins.cel import cel_policy

from omnigent_factory.omnigent import policies as pol
from omnigent_factory.service.mcp import TOOL_NAMES

ROOT = "conv_7f3a9c"
FORMS = ("{}", "factory__{}", "mcp__omnigent__factory__{}", "mcp__factory__{}")


_SPEC = pol.caller_policy(ROOT)
_CHECK = cel_policy(**_SPEC.factory_params)


def _verdict(event: object) -> str | None:
    assert _SPEC.handler == pol.CEL_HANDLER
    out = _CHECK(event)  # type: ignore[arg-type]
    return None if out is None else str(out["result"])  # type: ignore[index]


def _call(name: object, arguments: object) -> dict[str, object]:
    return {"type": "tool_call", "target": name, "data": {"name": name, "arguments": arguments}}


@pytest.mark.parametrize(("tool", "form"), list(itertools.product(TOOL_NAMES, FORMS)))
def test_factory_tools_are_bound_to_the_session(tool: str, form: str) -> None:
    name = form.format(tool)
    assert _verdict(_call(name, {"session_id": ROOT})) == "ALLOW"
    assert _verdict(_call(name, {"session_id": ROOT, "kind": "triage"})) == "ALLOW"
    for arguments in (
        {"session_id": "conv_other"},  # another (e.g. another parcel's) session
        {"session_id": ROOT + "x"},
        {"session_id": ""},
        {},
        {"session_id": None},
        {"session_id": 7},
        {"session_id": [ROOT]},
        {"session_id": {"id": ROOT}},
        [ROOT],
        ROOT,
        None,
    ):
        assert _verdict(_call(name, arguments)) == "DENY", arguments
    assert _verdict({"type": "tool_call", "data": {"name": name}}) == "DENY"


@pytest.mark.parametrize(
    "event",
    [
        _call("sys_os_shell", {"command": "ls"}),
        _call("github__get_issue", "not-a-map"),
        _call("sys_session_get_info", {}),
        _call("my_factory_get_status", {}),  # not anchored as a factory tool
        _call(5, {}),
        {"type": "tool_call", "data": "junk"},
        {"type": "tool_call"},
        {"type": "request", "data": "hello"},
        {"type": "tool_result", "data": {"name": "factory_get_status"}},
    ],
)
def test_other_calls_are_unaffected_and_never_abstain(event: dict[str, object]) -> None:
    # Always an explicit result map: the builtin abstains (fails open) on errors.
    assert _verdict(event) == "ALLOW"


def test_any_factory_prefixed_name_is_guarded() -> None:
    # The anchored pattern also covers future/unknown factory_ tools (fail closed).
    assert _verdict(_call("factory__factory_new_tool", {})) == "DENY"
    assert _verdict(_call("factory_new_tool", {"session_id": ROOT})) == "ALLOW"


def test_policy_literal_is_quoted_and_versioned() -> None:
    spec = pol.caller_policy('conv"x')
    assert pol.family_of(spec.name) == pol.CALLER_NAME and spec.name.startswith("factory-caller@")
    assert '"conv\\"x"' in spec.factory_params["expression"]
    check = cel_policy(**spec.factory_params)
    assert check(_call("factory_get_status", {"session_id": 'conv"x'}))["result"] == "ALLOW"  # type: ignore[index]
    assert pol.caller_policy(ROOT).name != pol.caller_policy("conv_other").name
    with pytest.raises(ValueError, match="session id"):
        pol.caller_policy("")
