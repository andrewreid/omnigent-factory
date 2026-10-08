"""#761: read-only stages (triage, plan, ranking) may not move HEAD or change the working
tree. A plan-stage ``git checkout origin/main`` detached the issue worktree and the build
could not be prepared. ``factory-cel`` has a read-only version for those sessions; build
and rework keep the unchanged rule. Sub-agents (Rosie's workers) inherit their root's
session policies in Omnigent (``runtime/policies/builder.py``: root session policies are
prepended to a child's), so attaching the rule to the stage root covers them.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from omnigent.policies.builtins.cel import cel_policy

from omnigent_factory.core.effects import Ack, EffectKind
from omnigent_factory.core.types import SessionKind
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.ranking import RankingSessions
from tests.credentials.repos import GitEnv
from tests.omnigent.support import CTX, Rig, intent, make_rig, spec
from tests.omnigent.test_issue_session_adapter import _reuse_spec, _root_with_triage_run
from tests.omnigent.test_liberal_policies import FORBIDDEN

MOVES_HEAD = [
    "git checkout origin/main",  # the #761 command
    "git checkout main",
    "git checkout -b scratch origin/main",
    "git checkout origin/main -- src/app.ts",
    "git checkout -- README.md",
    "git switch main",
    "git switch --detach origin/main",
    "git reset --hard origin/main",
    "git reset HEAD~1",
    "git restore README.md",
    "git restore --staged .",
    "git merge origin/main",
    "git rebase origin/main",
    "git cherry-pick abc1234",
    "git revert HEAD",
    "git pull",
    "git pull --rebase origin main",
    "git stash",
    "git stash -u",
    "git stash push -m wip",
    "git stash pop",
    "git stash apply stash@{0}",
    "git commit -m wip",
    "git commit --amend --no-edit",
    "git worktree add /tmp/x origin/main",
    "git worktree remove /tmp/x",
    "git worktree move /tmp/x /tmp/y",
    # global options before the subcommand
    "git -C /work/factory-issue-761 checkout origin/main",
    'git -C "/work/with space" switch main',
    "git -c advice.detachedHead=false checkout origin/main",
    "git --no-pager --git-dir=/w/.git reset --hard",
    "/usr/bin/git checkout origin/main",
    # chained, piped and nested like the existing rules
    "git fetch origin main && git checkout origin/main",
    "git status; git checkout origin/main",
    "git log -1 | cat && git reset --hard",
    "git diff || git stash",
    "(cd /w && git checkout origin/main)",
    "echo $(git rebase origin/main)",
    "bash -lc 'git switch main'",
    "cd /w &&\ngit checkout origin/main",
    "git \\\n  checkout origin/main",
]

READS = [
    "git status -sb",
    "git log --oneline -5 origin/main",
    "git log --grep checkout --oneline",
    "git diff origin/main -- src/app.ts",
    "git diff --stat origin/main...HEAD",
    "git show origin/main:src/reset.ts",
    "git show origin/main:src/checkout/page.tsx | head -40",
    "git fetch origin main && git log origin/main -3",
    "git ls-tree -r origin/main src",
    "git rev-parse HEAD origin/main",
    "git blame src/merge.ts",
    "git merge-base HEAD origin/main",
    "git stash list",
    "git stash show -p stash@{0}",
    "git worktree list",
    "git branch -a --contains HEAD",
    "git -C /work/factory-issue-761 log -1",
    "git help checkout",
    "gh issue view 761 --json body --jq .body",
    "ls .git/worktrees && cat .github/workflows/ci.yml",
]


def _event(command: Any, tool: str = "sys_os_shell", key: str = "command") -> dict[str, Any]:
    return {"type": "tool_call", "data": {"name": tool, "arguments": {key: command}}}


def _result(verdict: Any) -> str:
    if verdict is None:
        return "ALLOW"
    return str(verdict.get("result") if isinstance(verdict, dict) else verdict)


@pytest.fixture(scope="module")
def read_only() -> Any:
    return cel_policy(**pol.factory_cel_policy("main", read_only=True).factory_params)


@pytest.fixture(scope="module")
def build() -> Any:
    return cel_policy(**pol.factory_cel_policy("main").factory_params)


@pytest.mark.parametrize("command", MOVES_HEAD)
def test_read_only_stages_cannot_move_head(read_only: Any, command: str) -> None:
    verdict = read_only(_event(command))
    assert _result(verdict) == "DENY", command
    reason = verdict["reason"]
    assert "read-only" in reason
    # Tells the agent how to inspect other refs without moving HEAD.
    assert "git show <ref>:<path>" in reason and "git diff <ref>" in reason
    assert "git log <ref>" in reason


@pytest.mark.parametrize("command", READS)
def test_read_only_stages_keep_every_read(read_only: Any, command: str) -> None:
    assert _result(read_only(_event(command))) == "ALLOW", command


@pytest.mark.parametrize("command", MOVES_HEAD)
def test_build_stage_rule_is_unchanged(build: Any, command: str) -> None:
    assert _result(build(_event(command))) == "ALLOW", command


def test_build_rule_is_byte_identical_to_the_default() -> None:
    assert pol.factory_cel_expression("main", read_only=False) == pol.factory_cel_expression()
    assert (
        pol.factory_cel_policy("main").name != pol.factory_cel_policy("main", read_only=True).name
    )


@pytest.mark.parametrize("command", FORBIDDEN)
def test_read_only_rule_keeps_the_owner_only_deny_list(read_only: Any, command: str) -> None:
    assert _result(read_only(_event(command))) == "DENY", command


@pytest.mark.parametrize(
    ("tool", "key", "command"),
    [
        ("Bash", "command", "git checkout origin/main"),
        ("exec_command", "cmd", "git reset --hard"),
        ("shell", "command", ["bash", "-lc", "git switch main"]),
    ],
)
def test_every_shell_surface_is_scanned(read_only: Any, tool: str, key: str, command: Any) -> None:
    assert _result(read_only(_event(command, tool, key))) == "DENY"


def test_quoted_mentions_are_denied_conservatively(read_only: Any) -> None:
    """A raw-text scan, like the owner-only deny list: quoting ``git checkout`` (a search
    pattern) is denied too, so ``bash -lc 'git switch main'`` cannot slip through."""
    assert _result(read_only(_event("rg -n 'git checkout' docs/"))) == "DENY"


def _cel_row(rig: Rig, root: str) -> dict[str, Any]:
    [row] = [p for p in rig.server.policies[root] if pol.family_of(p["name"]) == pol.CEL_NAME]
    return row


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [SessionKind.TRIAGE, SessionKind.PLAN])
async def test_triage_and_plan_sessions_get_the_read_only_rule(
    git_env: GitEnv, kind: SessionKind
) -> None:
    rig = make_rig(git_env)
    rig.directory.specs["S1"] = spec("S1", kind=kind)
    out = await rig.adapter.execute(intent(EffectKind.CREATE_SESSION, nonce="nonce-abc"), CTX)
    assert isinstance(out, Ack) and out.remote_id is not None
    root = out.remote_id
    rig.directory.set_root("S1", root)
    prep = await rig.adapter.execute(intent(EffectKind.PREPARE_SESSION, root_id=root), CTX)
    assert isinstance(prep, Ack) and prep.detail["ok"] is True
    expected = pol.factory_cel_expression("main", read_only=True)
    assert _cel_row(rig, root)["factory_params"]["expression"] == expected


@pytest.mark.asyncio
async def test_build_on_a_reused_triage_root_swaps_to_the_build_rule_add_before_delete(
    git_env: GitEnv,
) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    triage_rule = _cel_row(rig, root)["name"]
    assert triage_rule == pol.factory_cel_policy("main", read_only=True).name
    _reuse_spec(rig, "B1", root, SessionKind.BUILD)
    rig.server.requests.clear()
    prep = intent(EffectKind.PREPARE_SESSION, "B1", root_id=root, reuse=True)
    out = await rig.adapter.execute(prep, CTX)
    assert isinstance(out, Ack) and out.detail["ok"] is True, out.detail
    assert _cel_row(rig, root)["name"] == pol.factory_cel_policy("main").name
    cel_writes = [
        (method, body.get("name") if isinstance(body, dict) else None)
        for method, path, body in rig.server.requests
        if "/policies" in path and method != "GET"
    ]
    posts = [
        i
        for i, (m, n) in enumerate(cel_writes)
        if m == "POST" and str(n).startswith("factory-cel@")
    ]
    deletes = [i for i, (m, _) in enumerate(cel_writes) if m == "DELETE"]
    assert posts and deletes and max(posts) < min(deletes)  # never without a guard


@pytest.mark.asyncio
async def test_boot_upgrade_moves_a_live_plan_session_to_the_read_only_rule(
    git_env: GitEnv,
) -> None:
    rig = make_rig(git_env)
    root = await _root_with_triage_run(rig)
    _reuse_spec(rig, "P1", root, SessionKind.PLAN)
    rig.directory.specs["P1"] = replace(rig.directory.specs["P1"], root_id=root)
    # A plan run prepared by the previous daemon carries the build-form rule.
    row = _cel_row(rig, root)
    old = pol.factory_cel_policy("main")
    row["name"], row["factory_params"] = old.name, dict(old.factory_params)
    assert await rig.adapter.upgrade_static_policies("P1") is True
    assert _cel_row(rig, root)["name"] == pol.factory_cel_policy("main", read_only=True).name


@pytest.mark.asyncio
async def test_ranking_sessions_get_the_read_only_rule(git_env: GitEnv) -> None:
    rig = make_rig(git_env)
    sessions = RankingSessions(rig.adapter, str(git_env.source))
    [rule] = [p for p in sessions.policies("conv_x") if pol.family_of(p.name) == pol.CEL_NAME]
    assert rule == pol.factory_cel_policy("main", read_only=True)


def test_operator_rule_is_attached_to_read_only_stages_too(git_env: GitEnv) -> None:
    rig = make_rig(git_env, cel_expression='{"result": "ALLOW"}')
    names = [p.name for p in rig.adapter._static_policies(spec(kind=SessionKind.PLAN), "conv_x")]
    assert any(n.startswith(f"{pol.CEL_OPERATOR_NAME}@") for n in names)
