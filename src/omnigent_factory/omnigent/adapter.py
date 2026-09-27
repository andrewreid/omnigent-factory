"""Omnigent execution adapter: implements ``ports.omnigent.OmnigentAdapter`` (§5, §3.4).

Every ``execute`` performs at most one externally visible *work-bearing* write and
returns an :data:`~omnigent_factory.core.effects.AdapterOutcome`; it never decides a
stage and never writes the store. :mod:`.outcomes` maps outcomes to observation events.

Outcome contract per effect kind (``Ack.detail`` keys are stable):

``CREATE_SESSION``
    One JSON ``POST /v1/sessions`` with ``initial_items: []`` and the unique
    ``factory.dispatch`` nonce label. ``Ack(remote_id=root)`` with ``nonce``, ``workspace``,
    ``branch``, ``base_oid``. Rejections before the POST (spec mismatch, pre-existing
    branch/orphan worktree, bind-worktree verification) and server-proven pre-side-effect
    4xx are ``DefinitiveFailure``. Transport errors, timeouts, 5xx, 400/409 and a 2xx
    without an ID are ``AmbiguousWrite`` - never retried blindly.
``RECONCILE_SESSION``
    ``args.nonce``: nonce search over every page including archived roots. ``detail``:
    ``kind="adoption"``, ``matches`` (all label matches), ``root_id`` only for exactly one
    match whose agent/branch/workspace tuple verifies, ``orphan_worktree`` /
    ``branch_exists`` when nothing matched. ``args.effect_id``: own-send reconciliation.
    Messages: ``state`` is ``delivered`` (+``item_id``, exact marker *and* text digest),
    ``pending_input`` (parked native input - not absence), ``absent`` (complete history
    and pending-input scan) or ``ambiguous``. Resolves: ``still_pending`` (our resolve did
    not land) or ``gone`` (resolved/cancelled by someone; not proof either way).
    Incomplete reads are ``RetryableReadFailure``.
``PREPARE_SESSION``
    Verifies the root (nonce label, agent, project, workspace = our clone's worktree on the
    recorded branch inside owned roots), checks no unexpected turn ran, provisions/rotates
    the stage capability, wires the worktree, attaches github/CEL/cost policies and
    verifies them. ``detail``: ``ok``, ``unexpected_turn``, ``reason``, ``policy_ready_at_us``
    (cross-replica propagation barrier). Transient/ambiguous steps are
    ``RetryableReadFailure``: re-running adopts by exact policy name and parameters.
``SEND_MESSAGE``
    Records the own-send intent (effect ID, node, text digest) *before* one events POST;
    the text carries a unique effect marker. ``Ack(remote_id=item_id)``; ``denied`` input
    policy is definitive; ambiguous otherwise, including a 2xx without an item ID.
``RESOLVE_ELICITATION``
    Locates the exact node holding the pending elicitation in a fresh tree scan, checks
    the stored answer conforms to its flat form schema, records intent, resolves that one
    ID. Not pending -> ``DefinitiveFailure(ELICITATION_NOT_PENDING)``. A 404 on resolve is
    ambiguous (not proof the tool did or did not run).
``INTERRUPT_TREE``
    Interrupts the root and every busy or parked descendant. ``Ack`` detail lists
    interrupted/failed nodes. A receipt is never stop evidence; follow with ``SCAN_TREE``.
``SCAN_TREE``
    Complete recursive scan; ``detail``: ``complete``, ``busy``, ``pending_waiter``,
    ``node_ids``, ``errors``, ``pending_elicitations``.
``REPLACE_COST_POLICY``
    New cost generation (threshold = inclusive subtree spend + $35 x granted hours),
    delete older generations, verify final set. ``detail``: ``grant_id``, ``generation``,
    ``threshold_usd``, ``spend_known``, ``ready_at_us``. Same-name conflict is definitive;
    other failures are retryable and leave the grant unready.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from omnigent.tools._elicitation_schema import validate_content_against_schema

from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    AmbiguousWrite,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    JsonValue,
    RetryableReadFailure,
)
from omnigent_factory.credentials.worktree import (
    BotIdentity,
    StageWiring,
    VerifiedWorktree,
    Workspaces,
    WorktreeError,
)
from omnigent_factory.omnigent import policies as pol
from omnigent_factory.omnigent.directory import (
    CredentialProvisioner,
    DispatchDirectory,
    FormValue,
    OwnItemLedger,
    OwnSend,
    StageSpec,
)
from omnigent_factory.omnigent.rest import (
    OmnigentReadError,
    OmnigentRest,
    WriteClass,
    as_list,
    as_map,
    classify_write,
)
from omnigent_factory.omnigent.tree import DEFAULT_MAX_NODES, TreeObservation, scan_tree
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.omnigent import OMNIGENT_EFFECT_KINDS, SessionMatch, TreeScan

ELICITATION_NOT_PENDING = "elicitation-not-pending"
NONCE_LABEL = "factory.dispatch"
_TURN_ITEM_TYPES = frozenset(
    {"message", "function_call", "function_call_output", "reasoning", "custom_tool_call"}
)


def effect_marker(effect_id: str) -> str:
    return f"[factory-effect {effect_id}]"


def text_digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class OmnigentConfig:
    agent_id: str
    host_id: str
    repository: str
    project_id: str | None = None
    max_tree_nodes: int = DEFAULT_MAX_NODES
    #: Policy caches propagate across replicas with a 30 s TTL; add margin.
    policy_barrier_us: int = 35_000_000
    #: Default branch the CEL safety rule protects from direct pushes.
    default_branch: str = "main"
    #: Optional *additional* operator CEL rule, attached as ``factory-cel-operator``.
    #: The fixed ``factory-cel`` deny rule is always attached and verified regardless.
    cel_expression: str | None = None
    cel_reason: str = pol.CEL_REASON


def _message_texts(item: Mapping[str, Any]) -> list[str]:
    data = item.get("data") if isinstance(item.get("data"), dict) else item
    if not isinstance(data, Mapping):
        return []
    content = data.get("content")
    texts: list[str] = []
    if isinstance(content, str):
        texts.append(content)
    elif isinstance(content, list):
        texts.extend(
            str(part["text"])
            for part in content
            if isinstance(part, dict) and isinstance(part.get("text"), str)
        )
    return texts


def _is_user_message(item: Mapping[str, Any]) -> bool:
    data = item.get("data") if isinstance(item.get("data"), dict) else item
    return (
        item.get("type") == "message" and isinstance(data, Mapping) and data.get("role") == "user"
    )


def _json(value: object) -> JsonValue:
    if value is None or isinstance(value, str | int | bool):
        return value
    if isinstance(value, float):
        return str(value)
    if isinstance(value, list | tuple | frozenset | set):
        return [_json(v) for v in value]
    if isinstance(value, Mapping):
        return {str(k): _json(v) for k, v in value.items()}
    return str(value)


class OmnigentExecutionAdapter:
    def __init__(
        self,
        *,
        rest: OmnigentRest,
        config: OmnigentConfig,
        directory: DispatchDirectory,
        ledger: OwnItemLedger,
        workspaces: Workspaces,
        provisioner: CredentialProvisioner,
        identity: BotIdentity,
        clock: Clock,
        broker_socket: Path,
        helper_command: str | None = None,
    ) -> None:
        self.rest = rest
        self.config = config
        self.directory = directory
        self.ledger = ledger
        self.workspaces = workspaces
        self.provisioner = provisioner
        self.identity = identity
        self.clock = clock
        self.broker_socket = broker_socket
        self.helper_command = helper_command
        self._known_nodes: dict[str, set[str]] = {}
        # Required safety policy, compiled now so a bad deployment input fails at wiring.
        self._cel: tuple[pol.PolicySpec, ...] = (pol.factory_cel_policy(config.default_branch),)
        if config.cel_expression is not None:
            self._cel += (pol.operator_cel_policy(config.cel_expression, config.cel_reason),)

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return OMNIGENT_EFFECT_KINDS

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        handlers = {
            EffectKind.CREATE_SESSION: self._create,
            EffectKind.PREPARE_SESSION: self._prepare,
            EffectKind.SEND_MESSAGE: self._send,
            EffectKind.RESOLVE_ELICITATION: self._resolve,
            EffectKind.INTERRUPT_TREE: self._interrupt,
            EffectKind.SCAN_TREE: self._scan,
            EffectKind.RECONCILE_SESSION: self._reconcile,
            EffectKind.REPLACE_COST_POLICY: self._replace_cost,
        }
        handler = handlers.get(effect.kind)
        if handler is None:
            return DefinitiveFailure(f"{effect.kind} not handled by the Omnigent adapter")
        return await handler(effect)

    # ================================================================ observer port

    async def _nonce_rows(self, nonce: str) -> list[dict[str, Any]] | RetryableReadFailure:
        """Raw root rows labelled with ``nonce`` (all pages, archived included).

        The full row is kept adapter-side so adoption can verify the complete recorded
        tuple (agent, host, project, workspace, branch); ``SessionMatch`` carries less.
        """
        try:
            rows = await self.rest.paginate(
                "/v1/sessions",
                {"kind": "any", "include_archived": "true", "visibility": "all", "order": "asc"},
            )
        except OmnigentReadError as exc:
            return RetryableReadFailure(f"nonce search incomplete: {exc.reason}")
        return [
            row
            for row in rows
            if as_map(row.get("labels")).get(NONCE_LABEL) == nonce
            and row.get("parent_session_id") is None
            and isinstance(row.get("id"), str)
        ]

    async def find_by_nonce(self, nonce: str) -> list[SessionMatch] | RetryableReadFailure:
        rows = await self._nonce_rows(nonce)
        if isinstance(rows, RetryableReadFailure):
            return rows
        return [
            SessionMatch(
                root_id=row["id"],
                nonce=nonce,
                workspace=row.get("workspace"),
                branch=row.get("git_branch"),
                agent_id=row.get("agent_id"),
            )
            for row in rows
        ]

    async def observe_tree(self, root_id: str) -> TreeObservation:
        known = self._known_nodes.setdefault(root_id, {root_id})
        obs = await scan_tree(
            self.rest, root_id, known_ids=tuple(known), max_nodes=self.config.max_tree_nodes
        )
        known.update(obs.nodes)
        return obs

    async def scan_tree(self, root_id: str) -> TreeScan:
        return (await self.observe_tree(root_id)).to_scan()

    # ================================================================ create / adopt

    async def _spec(self, effect: EffectIntent) -> StageSpec | None:
        sid = effect.preconditions.session_id
        return None if sid is None else await self.directory.stage_spec(sid)

    async def _create(self, effect: EffectIntent) -> AdapterOutcome:
        spec = await self._spec(effect)
        if spec is None or effect.args.get("nonce") != spec.nonce:
            return DefinitiveFailure("stage-spec-mismatch")
        git: dict[str, Any]
        base_oid: str | None = None
        try:
            if spec.bind_worktree is not None:
                verified = await asyncio.to_thread(
                    self.workspaces.verify_worktree, spec.bind_worktree, spec.branch
                )
                workspace = str(verified.path)
                git = {"branch_name": spec.branch, "existing_worktree": True}
            else:
                orphan = await asyncio.to_thread(self.workspaces.find_branch_worktree, spec.branch)
                exists = await asyncio.to_thread(self.workspaces.branch_exists, spec.branch)
                if orphan is not None or exists:
                    return DefinitiveFailure(
                        f"branch-already-exists: {spec.branch}"
                        + (f" (worktree {orphan})" if orphan else "")
                    )
                await asyncio.to_thread(self.workspaces.ensure_source_clone)
                base = spec.base_branch or "main"
                try:
                    base_oid = await asyncio.to_thread(self.workspaces.fetch_base, base)
                except WorktreeError as exc:
                    return RetryableReadFailure(f"source fetch failed: {exc}")
                workspace = str(self.workspaces.source_clone)
                git = {"branch_name": spec.branch, "base_branch": f"origin/{base}"}
        except WorktreeError as exc:
            return DefinitiveFailure(f"workspace-rejected: {exc}")
        body: dict[str, Any] = {
            "agent_id": self.config.agent_id,
            "host_type": "external",
            "host_id": self.config.host_id,
            "workspace": workspace,
            "git": git,
            "title": spec.title,
            "labels": {
                NONCE_LABEL: spec.nonce,
                "factory.parcel": spec.parcel_id,
                "factory.stage": spec.kind.value,
                "factory.attempt": str(spec.attempt),
            },
            "initial_items": [],
        }
        if self.config.project_id is not None:
            body["project_id"] = self.config.project_id
        resp = await self.rest.post_json("/v1/sessions", body)
        cls = classify_write(resp)
        if cls == WriteClass.DEFINITIVE:
            return DefinitiveFailure(f"create rejected: HTTP {resp.status} {resp.error_code or ''}")
        if cls == WriteClass.AMBIGUOUS or resp.body is None:
            return AmbiguousWrite(f"create outcome unknown: {resp.status or resp.error}")
        root = resp.body.get("id") or resp.body.get("session_id")
        if not isinstance(root, str) or not root:
            return AmbiguousWrite("create acknowledged without a session id")
        self._known_nodes[root] = {root}
        return Ack(
            remote_id=root,
            detail={
                "nonce": spec.nonce,
                "workspace": _json(resp.body.get("workspace")),
                "branch": _json(resp.body.get("git_branch")),
                "base_oid": base_oid,
            },
        )

    def _identity_problem(self, row: Mapping[str, Any], spec: StageSpec) -> str | None:
        """Agent/host/project/branch of a listed or snapshotted root vs the recorded tuple."""
        if row.get("agent_id") != self.config.agent_id:
            return "agent mismatch"
        if row.get("host_id") != self.config.host_id:
            return "host mismatch"
        if row.get("project_id") != self.config.project_id:
            return "project mismatch"
        if row.get("git_branch") != spec.branch:
            return "branch mismatch"
        return None

    def _verified_match(self, row: Mapping[str, Any], spec: StageSpec) -> bool:
        if self._identity_problem(row, spec) is not None:
            return False
        workspace = row.get("workspace")
        if not isinstance(workspace, str):
            return False
        try:
            verified = self.workspaces.verify_worktree(workspace, spec.branch)
        except WorktreeError:
            return False
        return spec.bind_worktree is None or verified.path == spec.bind_worktree.resolve()

    async def _reconcile(self, effect: EffectIntent) -> AdapterOutcome:
        effect_id = effect.args.get("effect_id")
        if isinstance(effect_id, str):
            return await self._reconcile_send(effect_id)
        spec = await self._spec(effect)
        nonce = effect.args.get("nonce")
        if spec is None or nonce != spec.nonce:
            return RetryableReadFailure("no persisted dispatch tuple for this nonce")
        found = await self._nonce_rows(spec.nonce)
        if isinstance(found, RetryableReadFailure):
            return found
        verified = [r for r in found if await asyncio.to_thread(self._verified_match, r, spec)]
        detail: dict[str, JsonValue] = {
            "kind": "adoption",
            "nonce": spec.nonce,
            "matches": len(found),
            "verified": len(verified),
            "root_id": str(verified[0]["id"]) if len(found) == 1 and len(verified) == 1 else None,
        }
        if not found:
            orphan = await asyncio.to_thread(self.workspaces.find_branch_worktree, spec.branch)
            detail["orphan_worktree"] = str(orphan) if orphan is not None else None
            detail["branch_exists"] = await asyncio.to_thread(
                self.workspaces.branch_exists, spec.branch
            )
        return Ack(detail=detail)

    # ================================================================ prepare

    def _unexpected_turn(self, snap: Mapping[str, Any]) -> str | None:
        if snap.get("status") not in ("idle", None):
            return f"status {snap.get('status')}"
        if snap.get("active_response_id"):
            return "active response"
        if snap.get("pending_inputs") or snap.get("pending_elicitations"):
            return "pending input or prompt"
        items = snap.get("items") or []
        if any(isinstance(i, dict) and i.get("type") in _TURN_ITEM_TYPES for i in items):
            return "conversation items present"
        return None

    def _check_root(self, snap: Mapping[str, Any], spec: StageSpec) -> str | None:
        labels = as_map(snap.get("labels"))
        if labels.get(NONCE_LABEL) != spec.nonce:
            return "nonce label mismatch"
        problem = self._identity_problem(snap, spec)
        if problem is not None:
            return problem
        if snap.get("parent_session_id") is not None:
            return "not a root session"
        return None

    async def _prepare(self, effect: EffectIntent) -> AdapterOutcome:
        spec = await self._spec(effect)
        root = effect.args.get("root_id")
        if spec is None or not isinstance(root, str):
            return DefinitiveFailure("stage-spec-missing")
        try:
            snap = await self.rest.get_json(f"/v1/sessions/{root}")
        except OmnigentReadError as exc:
            return RetryableReadFailure(f"root snapshot: {exc.reason}")
        problem = self._check_root(snap, spec)
        if problem is not None:
            return self._prepared(root, ok=False, reason=problem)
        turn = self._unexpected_turn(snap)
        if turn is not None:
            return self._prepared(root, ok=True, unexpected=True, reason=turn)
        try:
            verified = await asyncio.to_thread(
                self.workspaces.verify_worktree, str(snap.get("workspace") or ""), spec.branch
            )
            if spec.bind_worktree is not None and verified.path != spec.bind_worktree.resolve():
                return self._prepared(root, ok=False, reason="bound workspace differs")
            capability = await self.provisioner.provision(spec.session_id)
            await asyncio.to_thread(self._wire, verified, spec, capability.path)
        except WorktreeError as exc:
            return self._prepared(root, ok=False, reason=f"workspace: {exc}")
        cost = pol.cost_policy(
            spec.policy_generation,
            pol.cost_threshold_usd(_cost(snap), spec.granted_us),
        )
        wanted = (*self._static_policies(spec), cost)
        try:
            for policy in wanted:
                await pol.ensure_policy(self.rest, root, policy)
            await pol.verify_policies(self.rest, root, wanted)
        except pol.PolicyError as exc:
            if exc.conflict:
                return self._prepared(root, ok=False, reason=exc.reason)
            return RetryableReadFailure(f"policy preparation incomplete: {exc.reason}")
        try:
            again = await self.rest.get_json(f"/v1/sessions/{root}")
        except OmnigentReadError as exc:
            return RetryableReadFailure(f"root re-read: {exc.reason}")
        turn = self._unexpected_turn(again)
        if turn is not None:
            return self._prepared(root, ok=True, unexpected=True, reason=turn)
        return self._prepared(
            root,
            ok=True,
            workspace=str(verified.path),
            head_oid=verified.head_oid,
            capability_id=capability.capability_id,
        )

    def _wire(self, verified: VerifiedWorktree, spec: StageSpec, cap_path: Path) -> None:
        wiring = StageWiring(spec.session_id, cap_path, self.broker_socket, self.config.repository)
        self.workspaces.configure(
            verified, wiring, self.identity, helper_command=self.helper_command
        )

    async def upgrade_static_policies(self, session_id: str) -> bool:
        """Boot: bring a live session's factory-owned static policies to the current version.

        ``factory-github`` / ``factory-cel`` are deterministic and daemon-owned; a session
        prepared by an older daemon keeps its old parameters (e.g. the shell surface that
        raised human prompts) and would fail the next policy verification. A same-name
        policy with other parameters is replaced; cost generations are untouched.
        Returns whether anything changed. Raises :class:`PolicyError` on failure.
        """
        spec = await self.directory.stage_spec(session_id)
        if spec is None or spec.root_id is None:
            return False
        root = spec.root_id
        changed = False
        rows = await pol.list_session_policies(self.rest, root)
        for wanted in self._static_policies(spec):
            same = [r for r in rows if r.get("name") == wanted.name]
            if any(wanted.matches(r) for r in same):
                continue
            for row in same:
                if isinstance(row.get("id"), str):
                    await pol.delete_policy(self.rest, root, str(row["id"]))
            await pol.ensure_policy(self.rest, root, wanted)
            changed = True
        rows = await pol.list_session_policies(self.rest, root)
        for wanted in self._static_policies(spec):
            if not any(wanted.matches(r) for r in rows):
                raise pol.PolicyError(f"policy {wanted.name} upgrade unverified")
        return changed

    def _static_policies(self, spec: StageSpec) -> tuple[pol.PolicySpec, ...]:
        return (pol.github_policy(spec.kind, self.config.repository, spec.branch), *self._cel)

    def _prepared(
        self, root: str, *, ok: bool, unexpected: bool = False, reason: str = "", **extra: str
    ) -> Ack:
        detail: dict[str, JsonValue] = {
            "ok": ok,
            "unexpected_turn": unexpected,
            "reason": reason,
            **extra,
        }
        if ok and not unexpected:
            detail["policy_ready_at_us"] = self.clock.now_utc_us() + self.config.policy_barrier_us
        return Ack(remote_id=root, detail=detail)

    # ================================================================ messages

    async def _send(self, effect: EffectIntent) -> AdapterOutcome:
        spec = await self._spec(effect)
        if spec is None or spec.root_id is None:
            return DefinitiveFailure("no adopted root for this session")
        text = await self.directory.message_text(effect)
        if not text:
            return DefinitiveFailure("no rendered message text")
        full = f"{text}\n\n{effect_marker(effect.effect_id)}"
        await self.ledger.record_intent(
            OwnSend(effect.effect_id, spec.session_id, spec.root_id, "message", text_digest(full))
        )
        resp = await self.rest.post_json(
            f"/v1/sessions/{spec.root_id}/events",
            {
                "type": "message",
                "data": {"role": "user", "content": [{"type": "input_text", "text": full}]},
            },
        )
        cls = classify_write(resp)
        if cls == WriteClass.DEFINITIVE:
            return DefinitiveFailure(f"message rejected: HTTP {resp.status}")
        if cls == WriteClass.AMBIGUOUS or resp.body is None:
            return AmbiguousWrite(f"message outcome unknown: {resp.status or resp.error}")
        if resp.body.get("denied") is True:
            return DefinitiveFailure("message denied by input policy")
        item = resp.body.get("item_id")
        if not isinstance(item, str) or not item:
            return AmbiguousWrite("message accepted without an item id")
        await self.ledger.record_item(effect.effect_id, item)
        return Ack(remote_id=item, detail={"node_id": spec.root_id})

    async def _reconcile_send(self, effect_id: str) -> AdapterOutcome:
        send = await self.ledger.lookup(effect_id)
        if send is None:
            return RetryableReadFailure("no recorded own-send intent for this effect")
        if send.kind == "resolve":
            spec = await self.directory.stage_spec(send.session_id)
            root = spec.root_id if spec is not None and spec.root_id else send.node_id
            obs = await self.observe_tree(root)
            holder = obs.node_with_elicitation(send.elicitation_id)
            if holder is None and not obs.complete:
                return RetryableReadFailure("tree scan incomplete")
            state = "still_pending" if holder is not None else "gone"
            return Ack(detail={"kind": "resolve", "state": state, "effect_id": effect_id})
        marker = effect_marker(effect_id)
        try:
            items = await self.rest.paginate(f"/v1/sessions/{send.node_id}/items", {"order": "asc"})
            snap = await self.rest.get_json(f"/v1/sessions/{send.node_id}")
        except OmnigentReadError as exc:
            return RetryableReadFailure(f"history incomplete: {exc.reason}")
        exact = [
            i
            for i in items
            if _is_user_message(i)
            and isinstance(i.get("id"), str)
            and any(marker in t and text_digest(t) == send.text_sha256 for t in _message_texts(i))
        ]
        if len(exact) == 1:
            item_id = str(exact[0]["id"])
            await self.ledger.record_item(effect_id, item_id)
            return Ack(
                remote_id=item_id,
                detail={"kind": "message", "state": "delivered", "item_id": item_id},
            )
        if len(exact) > 1:
            return Ack(detail={"kind": "message", "state": "ambiguous", "effect_id": effect_id})
        pending = [
            p
            for p in snap.get("pending_inputs") or ()
            if isinstance(p, dict) and any(marker in t for t in _message_texts(p))
        ]
        if pending:
            return Ack(
                detail={
                    "kind": "message",
                    "state": "pending_input",
                    "pending_id": _json(pending[0].get("pending_id")),
                }
            )
        return Ack(detail={"kind": "message", "state": "absent", "effect_id": effect_id})

    # ================================================================ elicitations

    async def _resolve(self, effect: EffectIntent) -> AdapterOutcome:
        spec = await self._spec(effect)
        eid = effect.args.get("elicitation_id")
        if spec is None or spec.root_id is None or not isinstance(eid, str):
            return DefinitiveFailure("no adopted root or elicitation id")
        content = await self.directory.elicitation_content(effect)
        if content is None:
            return DefinitiveFailure("no recorded answer for this elicitation")
        obs = await self.observe_tree(spec.root_id)
        holder = obs.node_with_elicitation(eid)
        if holder is None:
            if not obs.complete:
                return RetryableReadFailure("tree scan incomplete; prompt location unknown")
            return DefinitiveFailure(ELICITATION_NOT_PENDING)
        event = obs.owned_elicitations()[eid][1]
        problem = check_form_answer(event, content)
        if problem is not None:
            return DefinitiveFailure(f"answer does not fit the prompt schema: {problem}")
        await self.ledger.record_intent(
            OwnSend(
                effect.effect_id, spec.session_id, holder.node_id, "resolve", elicitation_id=eid
            )
        )
        resp = await self.rest.post_json(
            f"/v1/sessions/{holder.node_id}/elicitations/{eid}/resolve",
            {"action": "accept", "content": dict(content)},
        )
        cls = classify_write(resp)
        if cls == WriteClass.OK:
            await self.ledger.record_item(effect.effect_id, eid)
            return Ack(remote_id=eid, detail={"node_id": holder.node_id})
        if cls == WriteClass.DEFINITIVE and resp.status != 404:
            return DefinitiveFailure(f"resolve rejected: HTTP {resp.status}")
        return AmbiguousWrite(f"resolve outcome unknown: {resp.status or resp.error}")

    # ================================================================ interrupt / scan

    async def _interrupt(self, effect: EffectIntent) -> AdapterOutcome:
        root = effect.args.get("root_id")
        if not isinstance(root, str):
            return DefinitiveFailure("no root to interrupt")
        obs = await self.observe_tree(root)
        targets = [
            root,
            *sorted(
                n.node_id for n in obs.nodes.values() if n.node_id != root and (n.busy or n.parked)
            ),
        ]
        interrupted: list[JsonValue] = []
        failed: list[JsonValue] = []
        for node in targets:
            resp = await self.rest.post_json(
                f"/v1/sessions/{node}/events", {"type": "interrupt", "data": {}}
            )
            (interrupted if classify_write(resp) == WriteClass.OK else failed).append(node)
        return Ack(
            remote_id=root,
            detail={
                "interrupted": interrupted,
                "failed": failed,
                "scan_complete": obs.complete,
                "stop_evidence": False,
            },
        )

    async def _scan(self, effect: EffectIntent) -> AdapterOutcome:
        root = effect.args.get("root_id")
        if not isinstance(root, str):
            return DefinitiveFailure("no root to scan")
        obs = await self.observe_tree(root)
        return Ack(remote_id=root, detail=scan_detail(obs))

    # ================================================================ cost grant

    async def _replace_cost(self, effect: EffectIntent) -> AdapterOutcome:
        spec = await self._spec(effect)
        generation, granted = effect.args.get("generation"), effect.args.get("granted_us")
        if spec is None or spec.root_id is None:
            return DefinitiveFailure("no adopted root for this session")
        if not isinstance(generation, int) or not isinstance(granted, int):
            return DefinitiveFailure("malformed cost policy intent")
        try:
            snap = await self.rest.get_json(f"/v1/sessions/{spec.root_id}")
        except OmnigentReadError as exc:
            return RetryableReadFailure(f"spend read failed: {exc.reason}")
        spent = _cost(snap)
        threshold = pol.cost_threshold_usd(spent, granted)
        new = pol.cost_policy(generation, threshold)
        try:
            pid = await pol.replace_cost_policy(
                self.rest, spec.root_id, new, keep=self._static_policies(spec)
            )
        except pol.PolicyError as exc:
            if exc.conflict:
                return DefinitiveFailure(exc.reason)
            return RetryableReadFailure(f"cost policy replacement incomplete: {exc.reason}")
        return Ack(
            remote_id=pid,
            detail={
                "grant_id": _json(effect.args.get("grant_id")),
                "generation": generation,
                "threshold_usd": str(threshold),
                "spend_known": spent is not None,
                "ready_at_us": self.clock.now_utc_us() + self.config.policy_barrier_us,
            },
        )


def _cost(snap: Mapping[str, Any]) -> float | None:
    value = snap.get("total_cost_usd")
    return float(value) if isinstance(value, int | float) else None


def scan_detail(obs: TreeObservation) -> dict[str, JsonValue]:
    return {
        "complete": obs.complete,
        "busy": obs.busy,
        "pending_waiter": obs.pending_waiter,
        "node_ids": [_json(n) for n in sorted(obs.nodes)],
        "errors": [_json(e) for e in obs.errors],
        "pending_elicitations": [
            {"node_id": owner, "elicitation_id": eid}
            for eid, (owner, _event) in sorted(obs.owned_elicitations().items())
        ],
    }


def check_form_answer(event: Mapping[str, Any], content: Mapping[str, FormValue]) -> str | None:
    """Content must fit the prompt's flat MCP ``requestedSchema``.

    Delegates to the pinned server-side validator
    (``omnigent.tools._elicitation_schema.validate_content_against_schema``): only named
    keys, MCP primitive values matching each property's declared type, enum members,
    value bounds, and every ``required`` field. Empty content is accepted only when the
    schema requires nothing.
    """
    for key, raw in content.items():
        value: object = raw  # runtime data from the directory; re-validate the wire shape
        if isinstance(value, list):
            if not all(isinstance(v, str) for v in value):
                return f"{key}: list values must be strings"
        elif value is not None and not isinstance(value, str | int | float | bool):
            return f"{key}: nested values are not allowed"
    params = as_map(event.get("params"))
    schema = params.get("requestedSchema")
    if not isinstance(schema, dict):
        return None if not content else "prompt requests no form content"
    if not content:
        required = as_list(schema.get("required"))
        return f"missing required fields {required}" if required else None
    if validate_content_against_schema(dict(content), schema) is None:
        return "content does not match requested field names, types, enums or required fields"
    return None
