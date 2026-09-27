"""Session policy specs and idempotent REST reconciliation (architecture §5.2).

Only ``type=python`` registered factories are attached (``type=url`` is stored but not
instantiated by the inspected runtime). ``factory_params`` cannot be PATCHed, so a new
cost grant is a *new* uniquely named generation: create (or adopt by exact name and
parameters), verify, delete older generations, verify the final set. Any GET/POST/DELETE
failure leaves the grant unready. The cost policy is a non-hard backstop: ``ask`` only,
never ``max_cost_usd``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from omnigent_factory.core.types import SessionKind
from omnigent_factory.omnigent.rest import (
    OmnigentReadError,
    OmnigentRest,
    WriteClass,
    classify_write,
)

COST_HANDLER = "omnigent.policies.builtins.cost.cost_budget"
GITHUB_HANDLER = "omnigent.policies.builtins.github.github_policy"
CEL_HANDLER = "omnigent.policies.builtins.cel.cel_policy"

COST_PREFIX = "factory-cost-grant-"
GITHUB_NAME = "factory-github"
CEL_NAME = "factory-cel"
CEL_OPERATOR_NAME = "factory-cel-operator"

#: $35/hour: the proposal's $70 per 2h reference block (§4).
MICRODOLLARS_PER_HOUR = 35_000_000
MICROS_PER_HOUR = 3_600_000_000


@dataclass(frozen=True, slots=True)
class PolicySpec:
    name: str
    handler: str
    factory_params: Mapping[str, Any]
    type: str = "python"

    def body(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "handler": self.handler,
            "factory_params": dict(self.factory_params),
        }

    def matches(self, row: Mapping[str, Any]) -> bool:
        return (
            row.get("name") == self.name
            and row.get("type") == self.type
            and row.get("handler") == self.handler
            and (row.get("factory_params") or {}) == dict(self.factory_params)
            and row.get("enabled", True) is True
        )


class PolicyError(RuntimeError):
    """Policy state could not be established or verified."""

    def __init__(self, reason: str, *, conflict: bool = False) -> None:
        super().__init__(reason)
        self.reason = reason
        self.conflict = conflict


def cost_policy_name(generation: int) -> str:
    return f"{COST_PREFIX}{generation:04d}"


def cost_threshold_usd(spent_usd: float | None, granted_us: int) -> float:
    """Next ASK threshold: current inclusive subtree spend + $35 x granted hours.

    Unknown spend is treated as zero *for the threshold only* (the backstop then asks
    earlier, never later); it is not reported as zero cost anywhere else.
    """
    base_micro = round((spent_usd or 0.0) * 1_000_000)
    add_micro = granted_us * MICRODOLLARS_PER_HOUR // MICROS_PER_HOUR
    return (base_micro + add_micro) / 1_000_000


def cost_policy(generation: int, threshold_usd: float) -> PolicySpec:
    return PolicySpec(
        cost_policy_name(generation), COST_HANDLER, {"ask_thresholds_usd": [threshold_usd]}
    )


def github_policy(kind: SessionKind, repository: str, branch: str) -> PolicySpec:
    """Structured GitHub MCP tools only: build writes its parcel branch; others write nothing.

    Liberal posture (owner direction 2026-09-27): no factory policy may raise a human
    prompt. The pinned builtin ASKs whenever it cannot tokenise a shell command or resolve
    a remote alias such as ``origin`` - i.e. on ordinary ``git push`` / ``gh pr create`` and
    on heredocs that merely mention ``gh``. Its shell surface is therefore disabled
    (``shell_tools: []``); the MCP surface fails closed with DENY, never ASK. Shell git/gh
    is governed by the deny-only ``factory-cel`` scan and the worktree ``pre-push`` guard.
    """
    build = kind == SessionKind.BUILD
    return PolicySpec(
        GITHUB_NAME,
        GITHUB_HANDLER,
        {
            "read_all": True,
            "write_repos": [repository] if build else [],
            "write_branches": [branch] if build else [],
            "allow_destructive": False,
            "deny_tag_push": True,
            "deny_force_push": False,
            "shell_tools": [],
        },
    )


CEL_REASON = (
    "Denied by factory policy: merging, --admin or hook bypass, pushing the default "
    "branch, repository/ruleset/branch-protection/collaborator/secret/workflow "
    "administration, closing or deleting issues and deleting repositories are the owner's. "
    "Everything else is allowed. If this matched text inside a message, commit body or "
    "file content, write that text to a file and pass it by path (e.g. --body-file / -F) "
    "and rerun the command."
)

#: MCP GitHub tools that merge or administer (matched against ``event.data.name``).
_ADMIN_TOOLS = (
    r"(?i)(merge_pull_request|merge_pr|update_pull_request_branch|enable_auto_merge|"
    r"delete_repository|update_repository|transfer_repository|"
    r"create_or_update_ruleset|update_ruleset|delete_ruleset|"
    r"update_branch_protection|delete_branch_protection|add_collaborator|"
    r"remove_collaborator|create_or_update_secret|delete_secret|delete_branch|"
    r"close_issue|delete_issue|lock_issue)"
)
#: MCP file-write tools that commit straight to a branch named in ``arguments.branch``.
_FILE_TOOLS = r"(?i)(push_files|create_or_update_file|delete_file)"

#: Argument keys that carry shell command text across harness shell tools
#: (``sys_os_shell``/``Bash``: ``command``; Codex ``exec_command``: ``cmd``). File content
#: written by Write/Edit tools is not scanned, so documentation may mention these commands.
_COMMAND_KEYS = ("command", "cmd", "script")

#: One shell "segment": text up to the next separator, so a match cannot straddle
#: ``a && b`` or a new line. Scanning raw text needs no parse, so it cannot fail.
_SEG = r"[^\n;&|]*"


def _shell_pattern(default_branch: str) -> str:
    """Deny-list scan over raw command text (any string argument of any tool).

    Conservative substring rules, never an ASK. Branch confinement for pushes is exact in
    the worktree ``pre-push`` guard; this scan covers what git hooks cannot see.
    """
    branch = re.escape(default_branch)
    rules = [
        # merging and auto-merge (CLI, REST merge endpoints, GraphQL mutations)
        r"\bgh\s+pr\s+merge\b",
        r"/pulls/[0-9]+/merge\b",
        r"\brepos/[^\s/]+/[^\s/]+/merges\b",
        r"\b(mergePullRequest|enablePullRequestAutoMerge|mergeBranch)\b",
        # admin override and hook bypass (the pre-push guard is the branch boundary)
        r"(^|\s)--admin\b",
        r"\bgit\b" + _SEG + r"\bpush\b" + _SEG + r"\s--no-verify\b",
        r"(?i:hookspath)",
        # pushing the default branch by name (belt to the guard and the ruleset)
        r"\bgit\b"
        + _SEG
        + r"\bpush\b"
        + _SEG
        + r"(\s|:|\+|refs/heads/)"
        + branch
        + r"(\s|$|['\"])",
        # repository administration through the API or CLI
        r"\bgh\s+api\b" + _SEG + r"(rulesets|/protection\b|/collaborators\b|/hooks\b"
        r"|/keys\b|/secrets\b|/variables\b|/environments\b|/transfer\b"
        r"|/actions/permissions\b|/branches/[^\s/]+/rename\b"
        r"|/actions/workflows/[^\s/]+/(enable|disable)\b)",
        r"\bgh\s+api\b"
        + _SEG
        + r"(-X|--method)[\s=]*(PATCH|PUT)\b"
        + _SEG
        + r"\brepos/[^\s/]+/[^\s/'\"]+/?(\s|$|['\"])",
        r"\bgh\s+api\b" + _SEG + r"(-X|--method)[\s=]*DELETE\b",
        r"\bgh\s+(repo\s+(delete|edit|rename|archive|transfer)|ruleset|secret|variable)\b",
        r"\bgh\s+workflow\s+(enable|disable)\b",
        # closing / deleting issues and deleting repositories
        r"\bgh\s+issue\s+(close|delete|transfer|lock)\b",
        r"\bgh\s+api\b" + _SEG + r"/issues/[0-9]+\b" + _SEG + r"\bstate[\s=:]+[\"']?closed\b",
    ]
    return "(" + "|".join(rules) + ")"


def _cel_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def factory_cel_expression(default_branch: str = "main") -> str:
    """Fixed deny rule over the pinned ``PolicyEvent`` shape (policies/schema.py @1c0153aa).

    ``tool_call`` events carry ``data = {"name": <tool>, "arguments": {...}}``. The rule
    denies MCP merge/administration tools, MCP file writes to the default branch, and any
    command argument (shell text) matching the deny list. It only ever returns DENY
    or ALLOW. Every access is type-guarded: the pinned ``cel_policy`` *abstains* (allows)
    on an evaluation error, so an unguarded field access would fail open.
    """
    if not default_branch or any(c.isspace() for c in default_branch):
        raise ValueError("default branch name is required")
    data = "event.data"
    args = f"{data}.arguments"
    is_call = f'event.type == "tool_call" && type({data}) == map'
    named = f"has({data}.name) && type({data}.name) == string"
    has_args = f"has({args}) && type({args}) == map"
    admin_tool = f"({named} && {data}.name.matches({_cel_string(_ADMIN_TOOLS)}))"
    file_to_default = (
        f"({named} && {data}.name.matches({_cel_string(_FILE_TOOLS)}) && {has_args}"
        f" && has({args}.branch) && {args}.branch == {_cel_string(default_branch)})"
    )
    pattern = _cel_string(_shell_pattern(default_branch))
    command_key = " || ".join(f'k == "{key}"' for key in _COMMAND_KEYS)
    # celpy evaluates a nested ``exists`` target eagerly even behind ``&&``: a list
    # comprehension over a non-list argument raises, the pinned cel_policy then abstains
    # and the call would be ALLOWED. Keep string and list forms in separate guarded
    # clauses (list form keyed by ``has()``), so no argument type can make this fail open.
    string_form = (
        f"({has_args} && {args}.exists(k, ({command_key})"
        f" && type({args}[k]) == string && {args}[k].matches({pattern})))"
    )
    list_forms = [
        f"({has_args} && has({args}.{key}) && type({args}.{key}) == list"
        f" && {args}.{key}.exists(x, type(x) == string && x.matches({pattern})))"
        for key in _COMMAND_KEYS
    ]
    shell = "(" + " || ".join([string_form, *list_forms]) + ")"
    return (
        f"({is_call}) && ({admin_tool} || {file_to_default} || {shell})"
        f' ? {{"result": "DENY", "reason": {_cel_string(CEL_REASON)}}}'
        ' : {"result": "ALLOW"}'
    )


def _compiled(name: str, expression: str, reason: str) -> PolicySpec:
    if not expression.strip():
        raise ValueError("CEL expression is required")
    from omnigent.policies.builtins.cel import cel_policy as compile_cel  # noqa: PLC0415

    compile_cel(expression=expression, reason=reason)  # raises ValueError if invalid
    return PolicySpec(name, CEL_HANDLER, {"expression": expression, "reason": reason})


def factory_cel_policy(default_branch: str = "main") -> PolicySpec:
    """The required fixed safety rule, always attached as ``factory-cel``. Not replaceable."""
    return _compiled(CEL_NAME, factory_cel_expression(default_branch), CEL_REASON)


def operator_cel_policy(expression: str, reason: str = CEL_REASON) -> PolicySpec:
    """An optional operator rule, attached *in addition* under its own name.

    Policies are evaluated independently, so an operator rule that allows, abstains or
    errors (e.g. ``1 / 0``) cannot weaken the fixed ``factory-cel`` deny rule.
    """
    return _compiled(CEL_OPERATOR_NAME, expression, reason)


def is_cost_ask(policy_name: str | None) -> bool:
    return bool(policy_name) and str(policy_name).startswith(COST_PREFIX)


async def list_session_policies(rest: OmnigentRest, root_id: str) -> list[dict[str, Any]]:
    try:
        body = await rest.get_json(f"/v1/sessions/{root_id}/policies")
    except OmnigentReadError as exc:
        raise PolicyError(f"policy list failed: {exc.reason}") from exc
    data = body.get("data")
    if not isinstance(data, list):
        raise PolicyError("policy list malformed")
    return [r for r in data if isinstance(r, dict) and r.get("source", "session") == "session"]


async def ensure_policy(rest: OmnigentRest, root_id: str, spec: PolicySpec) -> str:
    """Create ``spec`` or adopt an exact same-name policy. Returns the policy ID."""
    for row in await list_session_policies(rest, root_id):
        if row.get("name") == spec.name:
            if spec.matches(row) and isinstance(row.get("id"), str):
                return str(row["id"])
            raise PolicyError(f"policy {spec.name} exists with different contents", conflict=True)
    resp = await rest.post_json(f"/v1/sessions/{root_id}/policies", spec.body())
    cls = classify_write(resp)
    if cls == WriteClass.OK and resp.body is not None and spec.matches(resp.body):
        pid = resp.body.get("id")
        if isinstance(pid, str):
            return pid
    if cls == WriteClass.DEFINITIVE:
        raise PolicyError(f"policy {spec.name} rejected: HTTP {resp.status}")
    # Ambiguous or unexpected echo: adopt only on an exact re-read.
    for row in await list_session_policies(rest, root_id):
        if spec.matches(row) and isinstance(row.get("id"), str):
            return str(row["id"])
    raise PolicyError(f"policy {spec.name} outcome unverified (HTTP {resp.status})")


async def delete_policy(rest: OmnigentRest, root_id: str, policy_id: str) -> None:
    resp = await rest.delete(f"/v1/sessions/{root_id}/policies/{policy_id}")
    if classify_write(resp) != WriteClass.OK and resp.status not in (204, 404):
        raise PolicyError(f"policy {policy_id} delete failed: HTTP {resp.status}")


async def replace_cost_policy(
    rest: OmnigentRest, root_id: str, spec: PolicySpec, *, keep: Sequence[PolicySpec] = ()
) -> str:
    """Install ``spec``, delete every other cost generation, verify the final set."""
    new_id = await ensure_policy(rest, root_id, spec)
    for row in await list_session_policies(rest, root_id):
        name = str(row.get("name") or "")
        if name.startswith(COST_PREFIX) and name != spec.name and isinstance(row.get("id"), str):
            await delete_policy(rest, root_id, str(row["id"]))
    await verify_policies(rest, root_id, (spec, *keep))
    return new_id


async def verify_policies(rest: OmnigentRest, root_id: str, expected: Sequence[PolicySpec]) -> None:
    rows = await list_session_policies(rest, root_id)
    for spec in expected:
        if not any(spec.matches(r) for r in rows):
            raise PolicyError(f"policy {spec.name} missing or altered")
    costs = [r for r in rows if str(r.get("name") or "").startswith(COST_PREFIX)]
    want = [s for s in expected if s.name.startswith(COST_PREFIX)]
    if len(costs) != len(want):
        raise PolicyError("unexpected cost policy generations remain")
