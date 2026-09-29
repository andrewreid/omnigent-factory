"""Session policy specs and idempotent REST reconciliation (architecture §5.2).

Only ``type=python`` registered factories are attached (``type=url`` is stored but not
instantiated by the inspected runtime). ``factory_params`` cannot be PATCHed, so every
change is a *new* uniquely named policy: create (or adopt by exact name and parameters),
verify, then delete what it supersedes and verify the final set. Nothing is ever deleted
before its replacement is verified, so a guard is never absent during a stage switch or a
boot upgrade (overlap may briefly deny more, which is fine while the run is closed).

* Static policies (``factory-github``, ``factory-cel``, ``factory-cel-operator``,
  ``factory-caller``) are named ``<family>@<digest of handler+params>``; any other member
  of the family (or its legacy bare name) is superseded.
* A cost grant is a new generation ``factory-cost-grant-NNNN``; older generations are
  superseded. The cost policy is a non-hard backstop: ``ask`` only, never ``max_cost_usd``.

Any GET/POST/DELETE failure leaves the preparation or grant unready.
"""

from __future__ import annotations

import hashlib
import json
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
CALLER_NAME = "factory-caller"
#: Static policy families the daemon owns; members are ``<family>@<digest>``.
STATIC_FAMILIES = (GITHUB_NAME, CEL_NAME, CEL_OPERATOR_NAME, CALLER_NAME)
VERSION_SEPARATOR = "@"

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


def versioned(family: str, handler: str, params: Mapping[str, Any]) -> PolicySpec:
    """A static policy named by its content, so a change is always an add-then-remove."""
    digest = hashlib.sha256(
        json.dumps({"handler": handler, "params": params}, sort_keys=True).encode("utf-8")
    ).hexdigest()[:12]
    return PolicySpec(f"{family}{VERSION_SEPARATOR}{digest}", handler, params)


def family_of(name: object) -> str | None:
    """The daemon-owned family a policy row belongs to (legacy bare names included)."""
    if not isinstance(name, str):
        return None
    if name.startswith(COST_PREFIX):
        return COST_PREFIX
    base = name.split(VERSION_SEPARATOR, 1)[0]
    return base if base in STATIC_FAMILIES else None


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
    return versioned(
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
    "administration, closing, deleting, transferring or locking issues and pull requests "
    "and deleting repositories are the owner's. Everything else is allowed (e.g. opening "
    "a follow-up issue, replying to and resolving review threads). If this matched text "
    "inside a message, commit body or file content, write that text to a file and pass it "
    "by path (e.g. --body-file / -F) and rerun the command."
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
        # closing / deleting issues and pull requests (CLI, REST, GraphQL) and deleting
        # repositories; creating issues, replying to and resolving threads stay allowed
        r"\bgh\s+issue\s+(close|delete|transfer|lock)\b",
        r"\bgh\s+pr\s+(close|lock)\b",
        r"\bgh\s+(issue|pr)\s+comment\b" + _SEG + r"\s--delete-last\b",
        r"\bgh\s+api\b"
        + _SEG
        + r"/(issues|pulls)/[0-9]+\b"
        + _SEG
        + r"\bstate[\s=:]+[\"']?closed\b",
        r"\b(closeIssue|deleteIssue|transferIssue|lockLockable|closePullRequest"
        r"|deleteIssueComment|deletePullRequestReview|deletePullRequestReviewComment)\b",
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
    return versioned(name, CEL_HANDLER, {"expression": expression, "reason": reason})


#: Every factory tool, under each name form a policy event can carry: the managed
#: namespace (``factory__``), Claude-facing wrappers and the bare server name. The
#: root's MCP server entry must stay named ``factory``.
FACTORY_TOOL_PATTERN = r"^(factory__|mcp__omnigent__factory__|mcp__factory__)?factory_.*$"

CALLER_REASON = (
    "Denied by factory policy: factory tools must be called with this session's own "
    "Omnigent session id as the session_id argument (see sys_session_get_info)."
)


def caller_cel_expression(session_id: str) -> str:
    """Bind factory tool calls to ``session_id`` (the root this policy is attached to).

    Total and type-guarded: every branch returns an explicit result map, because the
    pinned ``cel_policy`` abstains (allows) on an evaluation error or a non-map result.
    A factory tool whose ``arguments`` are missing or not a map, or whose ``session_id``
    is absent, not a string or another id, is denied; every other call is allowed. The
    literal comes from the session the daemon attaches the policy to, never from input.
    """
    if not session_id or any(c.isspace() for c in session_id):
        raise ValueError("a session id is required")
    data = "event.data"
    args = f"{data}.arguments"
    allow = '{"result": "ALLOW"}'
    deny = f'{{"result": "DENY", "reason": {_cel_string(CALLER_REASON)}}}'
    is_call = 'has(event.type) && event.type == "tool_call"'
    has_data = f"has({data}) && type({data}) == map"
    named = f"has({data}.name) && type({data}.name) == string"
    factory = f"{data}.name.matches({_cel_string(FACTORY_TOOL_PATTERN)})"
    has_args = f"has({args}) && type({args}) == map"
    has_id = f"has({args}.session_id) && type({args}.session_id) == string"
    return (
        f"!({is_call}) ? {allow}"
        f" : !({has_data}) ? {allow}"
        f" : !({named}) ? {allow}"
        f" : !({factory}) ? {allow}"
        f" : !({has_args}) ? {deny}"
        f" : !({has_id}) ? {deny}"
        f" : {args}.session_id == {_cel_string(session_id)} ? {allow}"
        f" : {deny}"
    )


def caller_policy(session_id: str) -> PolicySpec:
    """The per-root caller-identity policy, compiled before it is attached."""
    return _compiled(CALLER_NAME, caller_cel_expression(session_id), CALLER_REASON)


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
    await _delete_superseded(rest, root_id, (spec,))
    await verify_policies(rest, root_id, (spec, *keep))
    return new_id


async def reconcile_policies(
    rest: OmnigentRest, root_id: str, wanted: Sequence[PolicySpec]
) -> bool:
    """Make ``wanted`` the exact daemon-owned set: add and verify first, then remove what
    it supersedes (same family, other version), then verify the final set.

    Returns whether anything changed. Raises :class:`PolicyError` on any failure; a
    retry converges (adopts what exists by exact name and parameters).
    """
    before = await list_session_policies(rest, root_id)
    changed = False
    for spec in wanted:
        if not any(spec.matches(r) for r in before):
            await ensure_policy(rest, root_id, spec)
            changed = True
    await verify_policies(rest, root_id, wanted, exact=False)
    changed = await _delete_superseded(rest, root_id, wanted) or changed
    await verify_policies(rest, root_id, wanted)
    return changed


async def _delete_superseded(
    rest: OmnigentRest, root_id: str, wanted: Sequence[PolicySpec]
) -> bool:
    """Delete daemon-owned rows of a wanted family that are not themselves wanted."""
    names = {spec.name for spec in wanted}
    families = {family_of(spec.name) for spec in wanted}
    deleted = False
    for row in await list_session_policies(rest, root_id):
        name = row.get("name")
        if name in names or family_of(name) not in families or not isinstance(row.get("id"), str):
            continue
        await delete_policy(rest, root_id, str(row["id"]))
        deleted = True
    return deleted


async def verify_policies(
    rest: OmnigentRest, root_id: str, expected: Sequence[PolicySpec], *, exact: bool = True
) -> None:
    """Every expected policy is present and enabled with exactly its parameters.

    ``exact``: additionally no other member of an expected family remains (the final set
    after a switch); overlap is allowed only between add and remove.
    """
    rows = await list_session_policies(rest, root_id)
    for spec in expected:
        if not any(spec.matches(r) for r in rows):
            raise PolicyError(f"policy {spec.name} missing or altered")
    if not exact:
        return
    names = {spec.name for spec in expected}
    families = {family_of(spec.name) for spec in expected}
    stale = [r for r in rows if r.get("name") not in names and family_of(r.get("name")) in families]
    if stale:
        raise PolicyError("superseded factory policies remain")
