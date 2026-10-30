"""#1394: ``factory-cel`` evaluation cost must not grow with command length.

celpy evaluates both operands of ``&&``/``||`` and every element of ``exists``. The
list-form command clause (``type(args.command) == list && args.command.exists(...)``)
therefore iterated a *string* command character by character, one ``matches`` (and one
re-expansion of the ~1 KB pattern literal) per character: a 10 KB heredoc took tens of
seconds and the Omnigent proxy timed out. The guards are now ``?:`` (lazy in celpy).

Decisions must not change: the legacy expression (rebuilt from the previous
``_command_matches`` and pinned to the previous policy names) and the current one are run
over the existing corpora, every argument shape, and long generated commands. Omnigent
treats an abstention (``None``) as ALLOW, so verdicts are compared with that applied.

One deliberate difference: a command key holding a non-string scalar (``null``, a number
or a boolean) made the legacy rule *raise*, so the whole policy abstained - even when a
sibling key carried ``gh pr merge``. That fail-open is closed; see
``test_scalar_command_values_no_longer_fail_open``.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from typing import Any
from unittest import mock

import pytest
from omnigent.policies.builtins.cel import cel_policy

from omnigent_factory.omnigent import policies as pol
from tests.omnigent.test_liberal_policies import (
    CMD_462,
    CMD_677,
    FORBIDDEN,
    ISSUE_CREATE,
    ISSUE_READS,
    NORMAL,
)
from tests.omnigent.test_read_only_git_761 import MOVES_HEAD, READS


def _legacy_command_matches(regex: str, *, keys: tuple[str, ...] = pol._COMMAND_KEYS) -> str:
    """Pre-#1394 traversal, with today's argument keys and deny patterns."""
    args = "event.data.arguments"
    has_args = f"has({args}) && type({args}) == map"
    pattern = pol._cel_string(regex)
    command_key = " || ".join(f'k == "{key}"' for key in keys)
    string_form = (
        f"({has_args} && {args}.exists(k, ({command_key})"
        f" && type({args}[k]) == string && {args}[k].matches({pattern})))"
    )
    list_forms = [
        f"({has_args} && has({args}.{key}) && type({args}.{key}) == list"
        f" && {args}.{key}.exists(x, type(x) == string && x.matches({pattern})))"
        for key in keys
    ]
    return "(" + " || ".join([string_form, *list_forms]) + ")"


def _legacy_spec(read_only: bool) -> pol.PolicySpec:
    with mock.patch.object(pol, "_command_matches", _legacy_command_matches):
        return pol.factory_cel_policy("main", read_only=read_only)


Policy = Callable[[dict[str, Any]], Any]


@pytest.fixture(scope="module", params=[False, True], ids=["build", "read_only"])
def pair(request: pytest.FixtureRequest) -> tuple[Policy, Policy]:
    read_only: bool = request.param
    legacy = _legacy_spec(read_only)
    current = pol.factory_cel_policy("main", read_only=read_only)
    assert current.name != legacy.name  # content changed: a new version re-attaches
    return cel_policy(**legacy.factory_params), cel_policy(**current.factory_params)


def _decision(policy: Policy, event: dict[str, Any]) -> Any:
    return policy(event) or {"result": "ALLOW"}  # abstain is ALLOW in Omnigent


def _call(arguments: Any, name: Any = "sys_os_shell") -> dict[str, Any]:
    return {"type": "tool_call", "data": {"name": name, "arguments": arguments}}


SHORT = [
    *NORMAL,
    *FORBIDDEN,
    *ISSUE_CREATE,
    *ISSUE_READS,
    *MOVES_HEAD,
    *READS,
    "gh workflow run deploy.yml",
    "gh api -X DELETE repos/SA-Ambulance/timesheets/actions/caches",
    "gh run delete 123",
    "gh api repos/o/r/dispatches -f event_type=x",
]

#: Every argument and event shape a policy event can carry (string, list, wrong types).
SHAPES: list[dict[str, Any]] = [
    _call({"command": "gh pr merge 1"}, "Bash"),
    _call({"cmd": "git push origin main"}, "exec_command"),
    _call({"script": "gh workflow run ci.yml"}),
    _call({"command": ["bash", "-lc", "gh pr merge 3 --admin"]}),
    _call({"cmd": ["git", "push", "origin", "main"]}),
    _call({"script": ["gh issue create -t x"]}),
    _call({"command": ["ls", 7, None, {"a": "gh pr merge 1"}, "gh pr merge 1"]}),
    _call({"command": ["ls", 7, None, ["gh pr merge 1"]]}),
    _call({"command": {"inner": "gh pr merge 1"}}),
    _call({"command": "git status", "cmd": ["gh pr merge 1"]}),
    _call({"command": ["git status"], "script": "git checkout main"}),
    _call({"content": "gh pr merge 1", "file_path": "/w/a.md"}, "Write"),
    _call({"description": "gh pr merge 1", "command": "echo ok"}),
    _call({}),
    _call([]),
    _call("gh pr merge 1"),
    _call(None),
    _call({"branch": "main", "files": []}, "mcp__github__push_files"),
    _call({"branch": "factory/x"}, "mcp__github__create_or_update_file"),
    _call({}, "mcp__github__merge_pull_request"),
    _call({"pull_number": 7}, 42),
    {"type": "tool_call", "data": {"name": "Bash"}},
    {"type": "tool_call", "data": "gh pr merge 1"},
    {"type": "tool_call", "data": None},
    {"type": "tool_result", "data": {"name": "Bash", "arguments": {"command": "gh pr merge"}}},
    {"type": "request", "data": "please gh pr merge 1"},
    {"type": "response", "data": "git push origin main"},
    {"data": {"name": "Bash", "arguments": {"command": "gh pr merge 1"}}},
]

#: Forbidden operations to embed in long commands (and their harmless near-misses).
EMBEDDED = [
    "gh pr merge 12 --squash",
    "git push origin main",
    "git push --force origin HEAD:refs/heads/main",
    "gh pr checks 12 --admin",
    "gh workflow run deploy-staging.yml",
    "gh api -X DELETE repos/SA-Ambulance/timesheets/git/refs/heads/x",
    "gh issue create -t x -F body.md",
    "git checkout origin/main",
    "git stash",
    # near-misses that must stay allowed
    "gh pr view 12 --json state",
    "git show origin/main:src/merge.ts",
]

_BODY_LINE = "    result = compute(value) | other; ok && done  # merge main branch notes\n"


def _wraps(op: str) -> Iterator[str]:
    yield f"pnpm test && {op}"
    yield f"(cd /w && {op})"
    yield f"bash -lc '{op}'"
    yield f"echo $({op}); ls"


def _heredoc(size: int, *, before: str = "", inside: str = "", after: str = "") -> str:
    """A long code-writing heredoc, optionally with ``inside`` mid-body and commands
    chained before or after it."""
    head = f"{before} && " if before else ""
    lines = [f"{head}cat > src/pkg/module.py <<'EOF'\n"]
    total = len(lines[0])
    while total < size:
        if inside and abs(total - size // 2) < len(_BODY_LINE):
            lines.append(inside + "\n")
            inside = ""
        lines.append(_BODY_LINE)
        total += len(_BODY_LINE)
    lines.append("EOF")
    if after:
        lines.append(f"\n{after}")
    return "".join(lines)


def _long_commands(size: int) -> list[str]:
    """A clean heredoc, then each operation in turn mid-body, chained after and chained
    before it, rotating through the chain/subshell/``bash -lc``/``$()`` wrappers."""
    out = [_heredoc(size)]
    for i, op in enumerate(EMBEDDED):
        wrapped = list(_wraps(op))[i % 4]
        out.append(
            [
                _heredoc(size, inside=op),
                _heredoc(size, after=wrapped),
                _heredoc(size, before=wrapped),
            ][i % 3]
        )
    return out


def test_identical_decisions_on_the_existing_corpus(pair: tuple[Policy, Policy]) -> None:
    legacy, current = pair
    for command in [*SHORT, CMD_677, CMD_462]:
        event = _call({"command": command})
        assert _decision(current, event) == _decision(legacy, event), command


def test_identical_decisions_on_every_argument_shape(pair: tuple[Policy, Policy]) -> None:
    legacy, current = pair
    for event in SHAPES:
        assert _decision(current, event) == _decision(legacy, event), event
    for command in SHORT[::15]:
        for key in pol._COMMAND_KEYS:
            for value in (command, [command], ["bash", "-lc", command]):
                event = _call({key: value})
                assert _decision(current, event) == _decision(legacy, event), event


def test_identical_decisions_on_long_commands(pair: tuple[Policy, Policy]) -> None:
    """~400-byte heredocs: long enough to exercise the change, short enough that the
    legacy rule (the slow side, ~4 ms per character) keeps this test reasonable."""
    legacy, current = pair
    commands = _long_commands(400)
    denied = 0
    for command in commands:
        event = _call({"command": command})
        verdict = _decision(current, event)
        assert verdict == _decision(legacy, event), command
        denied += verdict["result"] == "DENY"
    assert 0 < denied < len(commands)


@pytest.mark.parametrize(
    ("arguments", "result"),
    [
        ({"command": None}, "ALLOW"),
        ({"command": 7}, "ALLOW"),
        ({"command": True}, "ALLOW"),
        ({"command": 1.5}, "ALLOW"),
        ({"command": 7, "cmd": "gh pr merge 1"}, "DENY"),
        ({"script": None, "command": "gh pr merge 1"}, "DENY"),
        ({"command": None, "script": "git push origin main"}, "DENY"),
    ],
)
def test_scalar_command_values_no_longer_fail_open(
    pair: tuple[Policy, Policy], arguments: dict[str, Any], result: str
) -> None:
    """The one changed decision: the legacy rule raised and abstained on every one of
    these (ALLOW, even beside a forbidden command); the current rule evaluates them."""
    legacy, current = pair
    event = _call(arguments)
    assert legacy(event) is None
    assert _decision(current, event)["result"] == result


@pytest.mark.parametrize("read_only", [False, True])
def test_long_heredoc_evaluates_fast(read_only: bool) -> None:
    """A 10 KB heredoc took ~20-45 s before #1394; now ~20-30 ms. The 1 s bound is
    generous for a loaded host and still far below the old cost."""
    check = cel_policy(**pol.factory_cel_policy("main", read_only=read_only).factory_params)
    clean = _heredoc(10_000)
    assert len(clean) >= 10_000
    started = time.perf_counter()
    verdict = check(_call({"command": clean}))
    elapsed = time.perf_counter() - started
    assert verdict["result"] == "ALLOW"
    assert elapsed < 1.0, f"{elapsed:.2f}s for a 10 KB heredoc"
    # Size does not hide an embedded operation, and stays cheap with it.
    for command in (
        _heredoc(50_000, after="gh pr merge 12"),
        _heredoc(50_000, inside="git push origin main"),
        _heredoc(50_000, before="bash -lc 'gh workflow run ci.yml'"),
    ):
        started = time.perf_counter()
        assert check(_call({"command": command}))["result"] == "DENY"
        assert time.perf_counter() - started < 1.0
