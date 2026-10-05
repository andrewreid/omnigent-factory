"""Liberal factory policies (owner direction 2026-09-27): default allow, short deny list,
never a human prompt.

Pilot evidence: the #677 build and #462 triage prompts came from ``factory-github`` (the
pinned ``github_policy`` builtin) returning ASK for shell text it could not tokenise or
whose ``origin`` it could not resolve. The factory now attaches it for MCP tools only and
enforces the deny list in ``factory-cel`` (raw-text scan, DENY or ALLOW) plus the
worktree ``pre-push`` guard (exact branch confinement, see test_push_guard).
"""

from __future__ import annotations

from typing import Any

import pytest
from omnigent.policies.builtins.cel import cel_policy
from omnigent.policies.builtins.github import github_policy as builtin_github_policy

from omnigent_factory.core.types import SessionKind
from omnigent_factory.omnigent import policies as pol

REPO = "SA-Ambulance/timesheets"
BRANCH = "factory/issue-677"

#: The exact #677 build command (Omnigent session 53085a29…, 2026-09-27 14:53:04).
CMD_677 = (
    "cat > /home/user/.local/share/omnigent-factory/clones/timesheets/.git/worktrees/"
    "factory-issue-677/molly/registry.json <<'EOF'\n"
    '{"parcel":"SA-Ambulance/timesheets#677","branch":"factory/issue-677",'
    '"base_oid":"edd497e7968ebf676db36608ac7ad0fd278023e5",'
    '"base_tree":"e94c9d9dda11350d6ecd300de99680e5f459605d",'
    '"class":"SMALL test-only; factory mandates opposite-vendor review",'
    '"acceptance":"AC-1..AC-5 per frozen authority",'
    '"non_goals":"auth changes; POST guard tests; harness refactor; requirement status; '
    'other modules","implementer":"claude_code","reviewer":"codex",'
    '"delivery":"PR via factory gh wrapper, never merge",'
    '"budget":{"remediation":0,"recheck":0}}\n'
    "EOF\necho ok"
)
#: #462 triage (session aab01192…): reconstructed from the owner-quoted prompt, which was
#: truncated at ``--jq '.labels[].nam``; the shape (``;`` ``|`` ``2>&1`` ``--jq '…'``) is exact.
CMD_462 = (
    "cd /home/user/.local/share/omnigent-factory/clones/timesheets-worktrees/"
    "factory-issue-462 && gh issue view 462 --json title,body 2>&1 | head -80; "
    "gh issue view 462 --json comments,labels --jq '.labels[].name, "
    '(.comments[] | "\\(.author.login): \\(.body)")\' 2>&1 | head -60; git log --oneline -5'
)

NORMAL = [
    CMD_677,
    CMD_462,
    "git status -sb && git log --oneline -3 && git rev-parse HEAD",
    "git diff --stat origin/main...HEAD | tail -5",
    "git fetch origin main && git rebase origin/main",
    "git push -u origin factory/issue-677",
    "git push origin HEAD:factory/issue-677",
    "git push --force-with-lease origin factory/issue-677",  # own branch: fine
    "git commit -F- <<'MSG'\ntest: guard read routes\n\nNever merge from here.\nMSG",
    "gh pr view 12 --json state,headRefOid --jq '.state'",
    "gh pr create --title 'test: read guard' --body-file /tmp/pr.md --head factory/issue-677",
    "gh pr edit 12 --body-file /tmp/pr.md && gh pr comment 12 --body-file /tmp/c.md",
    "gh api repos/SA-Ambulance/timesheets/pulls/12/comments --paginate --jq '.[].body'",
    "gh api graphql -f query='query { viewer { login } }'",
    "gh run list --branch factory/issue-677 --limit 5",
    "pnpm install --frozen-lockfile && pnpm -C api build && pnpm -C api test",
    "docker compose ps; ./scripts/dev-environment.sh status 2>&1 | tail -20",
    "curl -fsSL https://registry.npmjs.org/vitest | head -c 200",
    "rm -rf api/node_modules/.vite && mkdir -p /tmp/factory-scratch",
    "echo 'the owner merges; the factory never runs gh pr' > NOTES.txt",
    # Review-bot dispositions: a follow-up issue, a thread reply and resolving the thread.
    "gh issue create --title 'Ship SWA config' --body-file /tmp/f.md --label area:tooling",
    "gh api repos/SA-Ambulance/timesheets/pulls/686/comments/4130465711/replies -f body=@/tmp/r.md",
    "gh api graphql -f query='mutation { addPullRequestReviewThreadReply("
    'input: {pullRequestReviewThreadId: "PRRT_x", body: "FOLLOW_UP: #700"}) { comment { id } } }\'',
    "gh api graphql -f query='mutation { resolveReviewThread("
    'input: {threadId: "PRRT_kwDOTC12Fs6m_Zg1"}) { thread { isResolved } } }\'',
    # Workflow files on the parcel's own branch (owner decision 2026-10-01, #694).
    "sed -i 's/node 20/node 22/' .github/workflows/deploy-staging.yml",
    "git add .github/workflows/deploy-staging.yml && git commit -m 'ci: bump node'"
    " && git push origin factory/issue-677",
    "git push origin HEAD:refs/heads/factory/issue-677",
    "gh run list --workflow deploy-staging.yml --branch factory/issue-677",
    # Re-running and cancelling CI on the parcel's PR (owner decision 2026-10-06).
    "gh run rerun 123456",
    "gh run rerun 123456 --failed",
    "gh run rerun --job 987654",
    "gh run cancel 123456",
    "gh run view 123456 --log-failed | tail -50",
    "gh run watch 123456 --exit-status",
    "gh pr checks 12 || gh run rerun 123456 --failed && gh run watch 123456",
    "gh api -X POST repos/SA-Ambulance/timesheets/actions/runs/123456/rerun",
    "gh api -X POST repos/SA-Ambulance/timesheets/actions/runs/123456/rerun-failed-jobs",
    "gh api --method POST repos/SA-Ambulance/timesheets/actions/jobs/987654/rerun",
    "gh api -X POST repos/SA-Ambulance/timesheets/actions/runs/123456/cancel",
    "gh workflow list && gh workflow view ci.yml",
]

FORBIDDEN = [
    "gh pr merge 12 --squash",
    "gh pr merge 12 --auto --squash",
    "gh pr merge 12 --admin --squash",
    "cd api && pnpm test && gh pr merge 12",
    "gh pr checks 12 || gh pr merge 12 --admin",
    "gh api -X PUT repos/SA-Ambulance/timesheets/pulls/12/merge",
    "gh api repos/SA-Ambulance/timesheets/pulls/12/merge -X PUT -f merge_method=squash",
    "gh api graphql -f query='mutation { enablePullRequestAutoMerge(input: {}) { id } }'",
    "git push origin main",
    "git push origin HEAD:main",
    "git push --force origin main",
    "git fetch origin && git push origin +HEAD:refs/heads/main",
    "git push --no-verify origin factory/issue-677",
    "git -c core.hooksPath=/dev/null push origin HEAD:other",
    "git config --worktree core.hooksPath /tmp/x",
    "gh api repos/SA-Ambulance/timesheets/rulesets",
    "gh api -X PUT repos/SA-Ambulance/timesheets/branches/main/protection --input p.json",
    "gh api -X PUT repos/SA-Ambulance/timesheets/collaborators/someone",
    "gh api repos/SA-Ambulance/timesheets/actions/secrets",
    "gh api -X PATCH repos/SA-Ambulance/timesheets -f allow_auto_merge=true",
    "gh api -X DELETE repos/SA-Ambulance/timesheets/git/refs/heads/other",
    "gh repo delete SA-Ambulance/timesheets --yes",
    "gh repo edit --enable-auto-merge",
    "gh workflow disable ci.yml",
    "gh workflow enable deploy-staging.yml",
    "gh api -X PUT repos/SA-Ambulance/timesheets/actions/workflows/123/enable",
    "gh api -X PUT repos/SA-Ambulance/timesheets/actions/workflows/ci.yml/disable",
    "gh api -X PUT repos/SA-Ambulance/timesheets/actions/permissions -F enabled=true",
    "git add .github/workflows/ci.yml && git commit -m ci && git push origin HEAD:main",
    "gh issue close 462",
    "gh issue delete 462 --yes",
    "gh api -X PATCH repos/SA-Ambulance/timesheets/issues/462 -f state=closed",
    "gh pr close 686",
    "gh api -X PATCH repos/SA-Ambulance/timesheets/pulls/686 -f state=closed",
    "gh issue comment 651 --delete-last --yes",
    "gh api graphql -f query='mutation { closeIssue(input: {issueId: \"I_x\"}) { issue { id } } }'",
    "gh api graphql -f query='mutation { deleteIssue(input: {issueId: \"I_x\"}) { x } }'",
    "gh api graphql -F q=@m -f query='mutation { closePullRequest(input: {}) { x } }'",
    "gh api graphql -f query='mutation { transferIssue(input: {}) { clientMutationId } }'",
    "gh ruleset list; gh secret set X",
    # Dispatching workflows and deleting runs, logs, artifacts or caches stay the owner's.
    "gh workflow run ci.yml --ref factory/issue-677",
    "gh run rerun 1 --failed && gh workflow run deploy-staging.yml",
    "gh api -X POST repos/SA-Ambulance/timesheets/actions/workflows/ci.yml/dispatches"
    " -f ref=factory/issue-677",
    "cd api; gh api repos/SA-Ambulance/timesheets/actions/workflows/123/dispatches -f ref=x",
    "gh api -X POST repos/SA-Ambulance/timesheets/dispatches -f event_type=deploy",
    "gh run view 1 | head && gh api /repos/SA-Ambulance/timesheets/dispatches -f event_type=x",
    "gh run delete 123456",
    "gh run list --limit 1 | gh run delete 123456",
    "gh cache delete --all",
    "gh run rerun 1; gh cache delete 42",
    "gh api -X DELETE repos/SA-Ambulance/timesheets/actions/runs/123456",
    "gh api -X DELETE repos/SA-Ambulance/timesheets/actions/runs/123456/logs",
    "gh api -X delete repos/SA-Ambulance/timesheets/actions/runs/123456/logs",
    "gh api --method=delete repos/SA-Ambulance/timesheets/actions/artifacts/77",
    "gh api repos/SA-Ambulance/timesheets/actions/caches?key=x --method Delete",
    "gh run cancel 1 && gh api -X delete repos/SA-Ambulance/timesheets/actions/caches/9",
    # Unparseable text still gets the raw scan.
    "echo 'unbalanced && gh pr merge 12",
]


def _event(command: Any, tool: str = "sys_os_shell", key: str = "command") -> dict[str, Any]:
    return {"type": "tool_call", "data": {"name": tool, "arguments": {key: command}}}


@pytest.fixture(scope="module")
def cel() -> Any:
    spec = pol.factory_cel_policy("main")
    return cel_policy(**spec.factory_params)


def _result(verdict: Any) -> str:
    if verdict is None:
        return "ALLOW"
    return str(verdict.get("result") if isinstance(verdict, dict) else verdict)


@pytest.mark.parametrize("command", NORMAL)
def test_normal_work_is_allowed(cel: Any, command: str) -> None:
    assert _result(cel(_event(command))) == "ALLOW", command


@pytest.mark.parametrize("command", FORBIDDEN)
def test_forbidden_operations_are_denied_with_guidance(cel: Any, command: str) -> None:
    verdict = cel(_event(command))
    assert _result(verdict) == "DENY", command
    assert "--body-file" in verdict["reason"]  # tells the agent how to proceed


@pytest.mark.parametrize(
    ("tool", "key", "command"),
    [
        ("Bash", "command", "gh pr merge 3"),
        ("exec_command", "cmd", "git push origin main"),
        ("shell", "command", ["bash", "-lc", "gh pr merge 3 --admin"]),
    ],
)
def test_every_harness_shell_surface_is_scanned(cel: Any, tool: str, key: str, command: Any):
    assert _result(cel(_event(command, tool, key))) == "DENY"


def test_file_content_mentioning_commands_is_not_scanned(cel: Any) -> None:
    event = {
        "type": "tool_call",
        "data": {
            "name": "Write",
            "arguments": {"file_path": "/w/AGENTS.md", "content": "Never run gh pr merge."},
        },
    }
    assert _result(cel(event)) == "ALLOW"


def test_mcp_workflow_file_write_is_allowed_on_the_parcel_branch_only(cel: Any) -> None:
    def push(branch: str) -> dict[str, Any]:
        args = {
            "owner": "SA-Ambulance",
            "repo": "timesheets",
            "branch": branch,
            "files": [{"path": ".github/workflows/deploy-staging.yml", "content": "on: push"}],
        }
        return {"type": "tool_call", "data": {"name": "mcp__github__push_files", "arguments": args}}

    assert _result(cel(push(BRANCH))) == "ALLOW"
    assert _result(cel(push("main"))) == "DENY"


def test_mcp_merge_and_admin_tools_are_denied(cel: Any) -> None:
    for tool in ("mcp__github__merge_pull_request", "mcp__github__delete_branch"):
        assert _result(cel({"type": "tool_call", "data": {"name": tool, "arguments": {}}})) == (
            "DENY"
        )


@pytest.mark.parametrize("kind", list(SessionKind))
def test_factory_github_never_asks_on_shell(kind: SessionKind) -> None:
    """The ASK source: shell parsing is off; the MCP surface only ever DENYs."""
    spec = pol.github_policy(kind, REPO, BRANCH)
    assert spec.factory_params["shell_tools"] == []
    policy = builtin_github_policy(**spec.factory_params)
    for command in [*NORMAL, *FORBIDDEN]:
        assert policy(_event(command)) is None, command
    merge = policy(
        {
            "type": "tool_call",
            "data": {
                "name": "mcp__github__merge_pull_request",
                "arguments": {"owner": "SA-Ambulance", "repo": "timesheets", "pullNumber": 1},
            },
        }
    )
    assert merge is None or merge["result"] == "DENY"


def test_pinned_builtin_with_the_old_parameters_asked_on_both_pilot_commands() -> None:
    """Regression evidence: the previous parameters produced the live prompts."""
    old = builtin_github_policy(
        read_all=False,
        read_repos=[REPO],
        write_repos=[REPO],
        write_branches=[BRANCH],
        allow_destructive=False,
        deny_tag_push=True,
        deny_force_push=True,
    )
    assert old(_event(CMD_677))["result"] == "ASK"
    assert old(_event("git push -u origin factory/issue-677"))["result"] == "ASK"


@pytest.mark.parametrize(
    "arguments",
    [
        {"pull_number": 7},
        {"command": "gh pr merge 7", "timeout": 30},
        {"command": ["gh pr merge 7"], "env": {"A": "1"}, "n": 3},
    ],
)
def test_non_string_arguments_cannot_make_the_policy_abstain(cel: Any, arguments: Any) -> None:
    """celpy raised on a list macro over an int; the pinned cel_policy abstains (ALLOW)."""
    name = "github__merge_pull_request" if "pull_number" in arguments else "sys_os_shell"
    verdict = cel({"type": "tool_call", "data": {"name": name, "arguments": arguments}})
    assert _result(verdict) == "DENY"
