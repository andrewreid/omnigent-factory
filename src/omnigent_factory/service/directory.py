"""Durable dispatch snapshots, versioned message rendering, and GitHub publications."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path
from typing import Any

from omnigent_factory.core.codec import parcel_from_json
from omnigent_factory.core.contract_view import (
    ContractViewError,
    parcel_marker,
    render_contract_section,
)
from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    EffectIntent,
    EffectKind,
    ExecutionContext,
)
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.protocol import ParsedResult
from omnigent_factory.core.types import (
    MICROS_PER_MINUTE,
    ApprovalKind,
    Contract,
    Decision,
    IssueSnapshot,
    Parcel,
    SessionKind,
    StageSession,
    issue_session_title,
)
from omnigent_factory.github.adapter import ParcelBinding, TriageFields
from omnigent_factory.omnigent.directory import FormValue, StageSpec
from omnigent_factory.ports.adapter import EffectAdapter
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.db import StoreWorker

_TOKEN = re.compile(
    r"(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----)"
)


#: GitHub's issue-comment body limit minus room for the appended effect marker.
_CANONICAL_PUBLICATION_LIMIT = 65_536 - 256


class ServiceDispatchDirectory:
    """Production ``DispatchDirectory`` backed by projections plus immutable files."""

    def __init__(self, db: StoreWorker, config: ServiceConfig) -> None:
        self.db = db
        self.config = config
        self.root = config.state_dir / "dispatches"
        self.results = config.state_dir / "results"

    async def stage_spec(self, session_id: str) -> StageSpec | None:
        existing = self._read(self.root / f"{_safe(session_id)}.json")
        parcel = await self._parcel_for_session(session_id)
        if parcel is None:
            return None
        session = parcel.session(session_id)
        if session is None:
            return None
        if existing is None:
            existing = await self._make_snapshot(parcel, session)
            self._write_once(self.root / f"{_safe(session_id)}.json", existing)
        return StageSpec(
            session_id=session.session_id,
            parcel_id=parcel.parcel_id,
            kind=session.kind,
            attempt=session.attempt,
            nonce=session.nonce,
            branch=str(existing["branch"]),
            title=str(existing["title"]),
            base_branch=(
                None
                if existing.get("bind_worktree")
                else str(existing.get("base_branch") or self.config.default_branch)
            ),
            bind_worktree=(
                Path(str(existing["bind_worktree"])) if existing.get("bind_worktree") else None
            ),
            root_id=session.root_id,
            grant_id=session.grant.grant_id,
            granted_us=session.grant.remaining_us,
            policy_generation=session.grant.policy_generation,
            root_nonce=_root_nonce(parcel, session),
        )

    async def _make_snapshot(self, parcel: Parcel, session: StageSession) -> dict[str, object]:
        issue = parcel.issue_number or 0
        branch = f"factory/issue-{issue or _safe(parcel.parcel_id)}"
        previous = await self._previous_worktree(parcel, session, branch)
        template_name = _FIRST_TEMPLATES[session.kind]
        if session.kind == SessionKind.TRIAGE and self.latest_triage(parcel) is not None:
            template_name = _RETRIAGE_TEMPLATE  # a re-run on owner feedback
        auth = parcel.authorization(session.authorization_id)
        if session.kind == SessionKind.BUILD and auth is not None and auth.rework:
            # Owner feedback on the built work, or a merge conflict with the base branch.
            template_name = _CONFLICT_TEMPLATE if auth.conflict else _REWORK_TEMPLATE
        template = _template(template_name)
        evidence = await self.issue_evidence(parcel)
        return {
            "version": 1,
            "session_id": session.session_id,
            "parcel_id": parcel.parcel_id,
            "nonce": session.nonce,
            "revision": session.revision,
            "branch": branch,
            "base_branch": self.config.default_branch,
            "bind_worktree": previous,
            # The issue session's title (Omnigent keeps it for every later run).
            "title": issue_session_title(issue, evidence.title if evidence is not None else ""),
            "template": template_name,
            "template_sha256": hashlib.sha256(template.encode()).hexdigest(),
            "untrusted_boundary": _new_boundary(),
            "guidance": (
                self.config.triage_guidance
                if session.kind == SessionKind.TRIAGE
                else self.config.engineering_guidance
            ),
        }

    async def _previous_worktree(
        self, parcel: Parcel, session: StageSession, branch: str
    ) -> str | None:
        for old in reversed(parcel.sessions):
            if old.session_id == session.session_id:
                continue
            data = self._read(self.root / f"{_safe(old.session_id)}.json")
            if data and data.get("branch") == branch and isinstance(data.get("workspace"), str):
                return str(data["workspace"])
        return None

    def dispatch_snapshot(self, session_id: str) -> dict[str, Any] | None:
        """The persisted dispatch snapshot (branch, workspace, template) of a session."""
        return self._read(self.root / f"{_safe(session_id)}.json")

    async def record_create(self, session_id: str, ack: Ack) -> None:
        path = self.root / f"{_safe(session_id)}.json"
        data = self._read(path)
        if data is None:
            return
        workspace = ack.detail.get("workspace")
        branch = ack.detail.get("branch")
        if isinstance(workspace, str) and workspace:
            data["workspace"] = workspace
        if isinstance(branch, str) and branch:
            data["branch"] = branch
        base_oid = ack.detail.get("base_oid")
        if isinstance(base_oid, str):
            data["base_oid"] = base_oid
        _atomic_json(path, data)

    async def message_text(self, effect: EffectIntent) -> str | None:
        sid = effect.preconditions.session_id
        if sid is None:
            return None
        parcel = await self._parcel_for_session(sid)
        spec = await self.stage_spec(sid)
        if parcel is None or spec is None:
            return None
        snapshot = self._read(self.root / f"{_safe(sid)}.json")
        if snapshot is None:
            return None
        boundary = snapshot.get("untrusted_boundary")
        if not isinstance(boundary, str) or not boundary:
            boundary = _new_boundary()
            snapshot["untrusted_boundary"] = boundary
            _atomic_json(self.root / f"{_safe(sid)}.json", snapshot)
        purpose = str(effect.args.get("purpose") or "first")
        if purpose == "checkpoint_cleanup":
            return _template("checkpoint-v2.txt").format(grant_id=spec.grant_id, run_id=sid)
        if purpose == "continuation":
            return _template("continuation-v2.txt").format(grant_id=spec.grant_id)
        if purpose == "answer_relay":
            decision = parcel.decision(str(effect.args.get("decision_id") or ""))
            answer = await self.decision_answer(decision) if decision is not None else None
            if decision is None or answer is None:
                return None
            return _template("answer-v1.txt").format(
                decision_id=decision.decision_id, answer=answer
            )
        if purpose == "feedback":
            if spec.kind == SessionKind.BUILD:
                return _template("build-feedback-v2.txt").format(run_id=sid)
            if spec.kind == SessionKind.TRIAGE:
                return _template("triage-comment-v1.txt").format(run_id=sid)
            return _template("feedback-v3.txt").format(revision=parcel.revision, run_id=sid)
        if purpose == "readiness_wake":
            if effect.args.get("wake") == "conflict":
                return _template(_CONFLICT_WAKE_TEMPLATE).format(
                    pr_number=int(str(effect.args.get("pr_number") or 0)),
                    head_sha=str(effect.args.get("head_sha") or ""),
                    base_ref=str(effect.args.get("base_ref") or self.config.default_branch),
                    run_id=sid,
                )
            # Findings only (the check wake spent or not needed): ask for an outcome each.
            findings = effect.args.get("wake") == "findings"
            name = "readiness-findings-wake-v1.txt" if findings else "readiness-wake-v8.txt"
            return _template(name).format(
                pr_number=int(str(effect.args.get("pr_number") or 0)),
                head_sha=str(effect.args.get("head_sha") or ""),
                reason=str(effect.args.get("reason") or "")[:500],
                run_id=sid,
            )
        if purpose == "operator_note":
            note = str(effect.args.get("text") or "").strip()
            if not note:
                return None
            return _template("operator-v2.txt").format(note=note, run_id=sid)
        # The dispatch snapshot pins the first message's template bytes (restart-stable).
        try:
            template = _template(str(snapshot.get("template") or ""))
        except FileNotFoundError:
            return None  # pinned to a template this release no longer ships
        if hashlib.sha256(template.encode()).hexdigest() != snapshot.get("template_sha256"):
            return None
        # A short pointer: the factory tools are the source of truth for the issue, plan,
        # feedback and status (no task data is copied into the conversation).
        common: dict[str, object] = {
            "repository": self.config.repository,
            "issue_number": parcel.issue_number or 0,
            "run_id": sid,
            "session_id": spec.root_id or "",
            "revision": parcel.revision,
            "granted_minutes": max(1, spec.granted_us // MICROS_PER_MINUTE),
            "handoff": self._handoff(parcel, sid),
        }
        if spec.kind == SessionKind.BUILD:
            _authority_text, authority_hash = _authority(parcel)
            common.update(
                branch=spec.branch,
                gh_wrapper=self.config.wrapper_bin_dir / "gh",
                capability_file=self.config.capability_dir / f"{_safe(sid)}.cap",
                plan_hash=authority_hash,
                pr_number=parcel.pr_number or 0,
                base_ref=self.config.default_branch,
            )
        return template.format(**common)

    def _handoff(self, parcel: Parcel, run_id: str) -> str:
        """Stored summary for the first run in a replacement issue session.

        Only when this run created a root that replaced an earlier one: what the earlier
        conversation knew, as authoritative store fields. Tools remain the source of truth.
        """
        issue = parcel.issue_session
        if issue is None or issue.created_by != run_id or issue.generation <= 1:
            return ""
        lines = ["This is a new issue session replacing an earlier one. Stored summary:"]
        stages = [s.kind.value for s in parcel.sessions if s.session_id != run_id]
        if stages:
            lines.append(f"- earlier runs: {', '.join(stages)}")
        triage = self.latest_triage(parcel)
        if triage is not None:
            lines.append(
                f"- triage: {triage.get('recommendation')}, priority {triage.get('priority')},"
                f" size {triage.get('size')}"
            )
        contract = parcel.current_contract
        if contract is not None:
            lines.append(f"- current plan: revision {contract.revision}, hash {contract.full_hash}")
        if parcel.readiness is not None:
            lines.append(f"- PR #{parcel.readiness.pr_number} at {parcel.readiness.head_sha[:12]}")
        elif parcel.pr_number is not None:
            lines.append(f"- PR #{parcel.pr_number}")
        lines.append("Check factory_get_status before continuing.")
        return "\n".join(lines)

    def latest_triage(self, parcel: Parcel) -> dict[str, Any] | None:
        """The most recent accepted triage result of the parcel, if any."""
        for session in reversed(parcel.sessions):
            if session.kind != SessionKind.TRIAGE:
                continue
            stored = self.latest_result(session.session_id)
            record = stored.get("factory_result") if stored is not None else None
            body = record.get("result") if isinstance(record, dict) else None
            if isinstance(body, dict) and body.get("kind") == "triage":
                return body
        return None

    async def elicitation_content(self, effect: EffectIntent) -> dict[str, FormValue] | None:
        sid = effect.preconditions.session_id
        if sid is None:
            return None
        parcel = await self._parcel_for_session(sid)
        if parcel is None:
            return None
        decision = parcel.decision(str(effect.args.get("decision_id") or ""))
        answer = await self.decision_answer(decision) if decision is not None else None
        if answer is None:
            return None
        try:
            value: Any = json.loads(answer)
        except ValueError:
            return {"answer": answer}
        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            return {"answer": answer}
        if any(
            (item is not None and not isinstance(item, str | int | float | bool | list))
            or (isinstance(item, list) and not all(isinstance(v, str) for v in item))
            for item in value.values()
        ):
            return None
        return value

    def save_result(
        self, session_id: str, slot: str, parsed: ParsedResult
    ) -> dict[str, Any] | None:
        """Store an accepted result for publication; returns the previous ``latest``.

        Written before its reducer event commits (under the parcel lock, so no effect can
        read it in between). A rejected event restores the previous ``latest``
        (:meth:`restore_latest`); a crash in between leaves at most an unreferenced file.
        """
        payload: dict[str, object] = {
            "item_id": slot,
            "factory_result": parsed.result.model_dump(mode="json"),
            "contract_canonical": (
                parsed.contract_canonical.decode()
                if parsed.contract_canonical is not None
                else None
            ),
        }
        latest = self.results / f"{_safe(session_id)}-latest.json"
        previous = self._read(latest)
        _atomic_json(self.results / f"{_safe(session_id)}-{_safe(slot)}.json", payload)
        _atomic_json(latest, payload)
        return previous

    def restore_latest(self, session_id: str, previous: dict[str, Any] | None) -> None:
        latest = self.results / f"{_safe(session_id)}-latest.json"
        if previous is None:
            latest.unlink(missing_ok=True)
        else:
            _atomic_json(latest, previous)

    def plan_result_for(self, session_id: str, canonical: str) -> dict[str, Any] | None:
        """The accepted plan result whose canonical contract is exactly ``canonical``."""
        prefix = f"{_safe(session_id)}-"
        if not self.results.is_dir():
            return None
        for path in sorted(self.results.glob(f"{prefix}*.json")):
            if path.name == f"{prefix}latest.json":
                continue
            data = self._read(path)
            if data is None or data.get("contract_canonical") != canonical:
                continue
            record = data.get("factory_result")
            body = record.get("result") if isinstance(record, dict) else None
            if isinstance(body, dict):
                return body
        return None

    def latest_result(self, session_id: str) -> dict[str, Any] | None:
        return self._read(self.results / f"{_safe(session_id)}-latest.json")

    async def _parcel_for_session(self, session_id: str) -> Parcel | None:
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT p.aggregate_json FROM parcels p JOIN stage_sessions s "
                "ON s.parcel_id = p.parcel_id WHERE s.session_id = ?",
                (session_id,),
            )
        )
        if not rows:
            return None
        return parcel_from_json(str(rows[0][0]))

    async def issue_evidence(self, parcel: Parcel) -> IssueSnapshot | None:
        """The freshest verified read of the issue (title/body) recorded for the parcel."""
        parcel_id = parcel.parcel_id
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT payload_json FROM events WHERE parcel_id = ? "
                "AND payload_json LIKE '%\"evidence\":{%' ORDER BY sequence DESC LIMIT 1",
                (parcel_id,),
            )
        )
        if not rows:
            return None
        from omnigent_factory.core.codec import event_from_json  # noqa: PLC0415

        return event_from_json(str(rows[0][0])).evidence

    async def issue_labels(self, parcel: Parcel) -> list[str] | None:
        """Labels from the newest issue payload among the parcel's deliveries (None: none)."""
        for payload in await self._delivery_payloads(parcel, None, newest_first=True):
            issue = payload.get("issue")
            labels = issue.get("labels") if isinstance(issue, dict) else None
            if isinstance(labels, list):
                return [
                    str(label["name"])
                    for label in labels
                    if isinstance(label, dict) and isinstance(label.get("name"), str)
                ]
        return None

    async def decision_answer(self, decision: Decision) -> str | None:
        """The owner's answer: the ``/decide`` text, else the plain comment that answered."""
        if decision.answer is not None:
            return decision.answer
        event_id = decision.answer_event_id
        if not event_id:
            return None
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT d.body, r.comments_json FROM events e "
                "JOIN deliveries d ON e.delivery_guid = d.delivery_guid "
                "LEFT JOIN pr_review_comments r ON r.delivery_guid = d.delivery_guid "
                "WHERE e.event_id = ?",
                (event_id,),
            )
        )
        found = _feedback_text(rows[0][0], rows[0][1]) if rows else None
        return found[2] if found is not None else None

    async def owner_comments(
        self, parcel: Parcel, *, since_us: int = 0, kinds: tuple[EventKind, ...] = ()
    ) -> list[dict[str, Any]]:
        """Owner comment texts carried by the parcel's control events, oldest first: issue
        comments, and on the parcel's PR conversation comments and reviews (with their
        inline comments)."""
        wanted = kinds or (
            EventKind.PLAN_FEEDBACK,
            EventKind.DECIDE,
            EventKind.REQUEST_PLAN,
            EventKind.REQUEST_REPLAN,
            EventKind.APPROVE_PLAN,
            EventKind.CONTINUE,
        )
        parcel_id = parcel.parcel_id
        kinds_json = json.dumps([k.value for k in wanted])
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT e.event_id, e.kind, e.source_time_us, d.body, e.sequence, "
                "r.comments_json, d.delivery_guid FROM events e "
                "JOIN deliveries d ON e.delivery_guid = d.delivery_guid "
                "LEFT JOIN pr_review_comments r ON r.delivery_guid = d.delivery_guid "
                "WHERE e.parcel_id = ? AND e.accepted = 1 "
                "AND e.kind IN (SELECT value FROM json_each(?)) "
                "AND e.source_time_us >= ? ORDER BY e.sequence",
                (parcel_id, kinds_json, since_us),
            )
        )
        comments: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            if row[6] in seen:
                continue  # one comment per delivery (an operator re-application repeats it)
            seen.add(row[6])
            found = _feedback_text(row[3], row[5])
            if found is not None:
                source, pr_number, text = found
                comments.append(
                    {
                        "event_id": str(row[0]),
                        "kind": str(row[1]),
                        "at_us": int(row[2]),
                        "sequence": int(row[4]),
                        "source": source,
                        "pr_number": pr_number,
                        "text": text,
                    }
                )
        return comments

    async def _delivery_payloads(
        self, parcel: Parcel, kind: str | None, *, newest_first: bool = False
    ) -> list[dict[str, Any]]:
        parcel_id = parcel.parcel_id
        sql = _NEWEST_PAYLOADS if newest_first else _OLDEST_PAYLOADS
        rows = await self.db.call(lambda store: store.query(sql, (parcel_id, kind, kind)))
        payloads: list[dict[str, Any]] = []
        for row in rows:
            try:
                value = json.loads(bytes(row[0]))
            except (ValueError, TypeError):
                continue
            if isinstance(value, dict):
                payloads.append(value)
        return payloads

    def _read(self, path: Path) -> dict[str, Any] | None:
        if not path.is_file():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None

    def _write_once(self, path: Path, value: dict[str, object]) -> None:
        if path.exists():
            return
        _atomic_json(path, value)


class RecordingOmnigentAdapter:
    """Record create adoption data while preserving the adapter outcome contract."""

    def __init__(self, delegate: EffectAdapter, directory: ServiceDispatchDirectory) -> None:
        self.delegate = delegate
        self.directory = directory

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return self.delegate.handled_kinds

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        outcome = await self.delegate.execute(effect, ctx)
        sid = effect.preconditions.session_id
        if effect.kind == EffectKind.CREATE_SESSION and sid and isinstance(outcome, Ack):
            await self.directory.record_create(sid, outcome)
        return outcome


class ServiceParcelResolver:
    """Supplies GitHub context (issue number) that core effect args deliberately omit."""

    def __init__(self, db: StoreWorker) -> None:
        self.db = db

    async def __call__(self, parcel_id: str) -> ParcelBinding | None:
        parcel = await self.db.call(lambda store: store.load_parcel(parcel_id))
        if parcel is None or parcel.issue_number is None:
            return None
        return ParcelBinding(issue_number=parcel.issue_number)


class PublicationRenderer:
    def __init__(self, directory: ServiceDispatchDirectory, config: ServiceConfig) -> None:
        self.directory = directory
        self.config = config

    async def triage_fields(self, effect: EffectIntent) -> TriageFields | None:
        """Priority/Size/labels of the stored triage result a PUBLISH_TRIAGE publishes."""
        body = self._stored_result(effect)
        if body is None or body.get("kind") != "triage":
            return None
        labels = body.get("labels")
        return TriageFields(
            priority=str(body.get("priority") or ""),
            size=str(body.get("size") or ""),
            labels=tuple(str(label) for label in labels) if isinstance(labels, list) else (),
        )

    async def cross_vendor_review(self, effect: EffectIntent) -> bool:
        """Molly's opposite-vendor review of the PR head, as reported in ``build_ready``.

        Clean means: accepted verdict, reviewer vendor differs from the implementer, the
        reviewed head is the PR head the effect verifies, no finding left unresolved.
        """
        body = self._stored_result(effect)
        if body is None or body.get("kind") != "build_ready":
            return False
        # The head the accepted review attested; the GitHub read decides whether it
        # still covers a newer PR head (base-branch sync only).
        head = effect.args.get("reviewed_head") or effect.args.get("head_sha")
        review = body.get("review")
        if not isinstance(review, dict) or not isinstance(head, str):
            return False
        implementer = str(review.get("implementation_vendor") or "").strip().lower()
        reviewer = str(review.get("review_vendor") or "").strip().lower()
        findings = body.get("findings")
        return (
            review.get("accepted") is True
            and bool(implementer)
            and bool(reviewer)
            and implementer != reviewer
            and review.get("reviewed_head") == head
            and body.get("head_sha") == head
            and body.get("pr_number") == effect.args.get("pr_number")
            and all(
                isinstance(f, dict) and f.get("disposition") != "unresolved"
                for f in (findings if isinstance(findings, list) else [])
            )
        )

    def _stored_result(self, effect: EffectIntent) -> dict[str, Any] | None:
        sid = str(effect.args.get("session_id") or effect.preconditions.session_id or "")
        if not sid:
            return None
        result = self.directory.latest_result(sid)
        record = result.get("factory_result") if result is not None else None
        body = record.get("result") if isinstance(record, dict) else None
        return body if isinstance(body, dict) else None

    async def __call__(self, effect: EffectIntent) -> str | None:
        if effect.parcel_id is None:
            return None
        parcel_id = effect.parcel_id
        parcel = await self.directory.db.call(lambda store: store.load_parcel(parcel_id or ""))
        if parcel is None:
            return None
        if effect.kind == EffectKind.PUBLISH_CONTRACT:
            contract = parcel.contract(str(effect.args.get("contract_id") or ""))
            if contract is None:
                return None
            plan = self.directory.plan_result_for(contract.source_session_id, contract.canonical)
            return render_contract_comment(parcel, contract, plan, self.config)
        if (
            effect.kind == EffectKind.PUBLISH_REPORT and effect.args.get("report") == "ready"
        ) or effect.kind == EffectKind.EDIT_REPORT:
            build = (
                self.directory.latest_result(parcel.readiness.session_id)
                if parcel.readiness is not None
                else None
            )
            record = build.get("factory_result") if isinstance(build, dict) else None
            result = record.get("result") if isinstance(record, dict) else None
            text = _ready_text(
                effect.args,
                result if isinstance(result, dict) else None,
                parcel.readiness.checks_summary if parcel.readiness is not None else "",
                self.config.repository,
                review_bot_name(self.config),
            )
        elif effect.kind in {EffectKind.PUBLISH_TRIAGE, EffectKind.PUBLISH_REPORT}:
            body = self._stored_result(effect)
            if body is None:
                return None
            text = _public_result(body, agent_display_name(self.config))
        elif effect.kind == EffectKind.POST_COMMENT and effect.args.get("template") == "decision":
            text = _decision_text(effect.args)
        elif (
            effect.kind == EffectKind.POST_COMMENT
            and effect.args.get("template") == "ready-blocked"
            and isinstance(effect.args.get("findings"), list)
        ):
            text = _findings_blocked_text(effect.args, review_bot_name(self.config))
        elif effect.kind == EffectKind.POST_COMMENT:
            template = str(effect.args.get("template") or "status")
            text = _status_text(template, effect.args, agent_display_name(self.config))
        else:
            return None
        return _safe_publication(text, self.config)


def _findings_blocked_text(args: Mapping[str, Any], bot_name: str = "Review bot") -> str:
    """Needs you for review-bot findings: the reason, then each open thread (severity,
    path, title, link), so the owner sees what is left without opening the PR."""
    findings = [f for f in args.get("findings") or [] if isinstance(f, dict)]
    head = str(args.get("head_sha") or "")[:7]
    count = len(findings)
    noun = "finding" if count == 1 else "findings"
    lines = [
        f"PR #{args.get('pr_number')} isn't ready at `{head}`: {args.get('reason')}. "
        "I have no automatic fix attempt left, so I need your call.",
        "",
    ]
    if args.get("further_round"):
        lines.append(
            f"{bot_name} has {count} open {noun} on `{head}`, a further review round "
            "after a fix commit:"
        )
    else:
        lines.append(f"{bot_name} has {count} open {noun} on `{head}`:")
    lines.append("")
    for f in findings:
        badge = f"**{f.get('severity')}** " if f.get("severity") else ""
        path = str(f.get("path") or "").replace("`", "'")
        path = f"`{path}`" if path else "(no file)"
        title = " ".join(str(f.get("title") or "").replace("`", "'").split())
        url = str(f.get("url") or "")
        line = f"- {badge}{path}: {title or 'untitled'}"
        lines.append(f"{line} ([thread]({url}))" if url else line)
    return "\n".join(lines)


def _decision_text(args: Mapping[str, Any]) -> str:
    """The agent's question as its own reply: the owner answers by replying to it.

    Answering in Omnigent or with ``/decide`` still works, but is never advertised.
    """
    summary = str(args.get("summary") or "").strip()
    return summary or "I have a question before I can go on."


_OMNIGENT_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


def omnigent_link(base_url: str, session_id: str | None) -> str | None:
    """``{omnigent_base_url}/c/{session_id}``: Omnigent's share link (normal Omnigent auth)."""
    if not session_id or _OMNIGENT_ID.fullmatch(session_id) is None:
        return None
    return f"{base_url.rstrip('/')}/c/{session_id}"


def review_bot_name(config: ServiceConfig) -> str:
    """The review bot's name for owner-facing text: "@codex" reads "Codex"."""
    name = config.review_bot_mention.strip().lstrip("@") or config.review_bot_login.removesuffix(
        "[bot]"
    )
    name = " ".join(name.split())
    return name[:1].upper() + name[1:] if name else "Review bot"


def _ready_text(
    args: Mapping[str, Any],
    result: dict[str, Any] | None,
    checks_summary: str,
    repository: str,
    bot_name: str = "Review bot",
) -> str:
    """Ready report: what changed (the agent's summary, as written), then the factory's
    lines from the evidence the Ready decision used: PR, CI, the review bot (all kept
    current in place while the card stays in Ready on this head), the cross-vendor
    review and findings."""
    number = args.get("pr_number")
    head = str(args.get("head_sha") or "")
    url = f"https://github.com/{repository}/pull/{number}"
    red = str(args.get("red_checks") or "")
    lines = [f"### PR #{number} is ready for review", ""]
    if result is not None and result.get("head_sha") == head and result.get("summary"):
        lines += [str(result["summary"]).strip(), ""]  # the agent's Markdown, as written
    lines.append(f"**PR:** {url} (head `{head[:12]}`)")
    summary = str(args.get("checks_summary") or checks_summary)
    lines.append(f"**CI:** {summary or 'required checks green'}")
    bot = str(args.get("review_bot") or "")
    if bot:
        lines.append(f"**Review bot:** {bot_name}: {bot}")
    if "red_checks" in args:
        # Parked with red required checks: the agent attributes them to causes outside
        # the change (its summary above says why); merging waits for them.
        names = ", ".join(f"`{n}`" for n in red.split("; ") if n) or "see the PR checks"
        lines.append(f"**Required check red:** {names} (cause: see the summary above)")
    review = result.get("review") if result is not None else None
    if isinstance(review, dict):
        lines.append(
            f"**Cross-vendor review:** {review.get('implementation_vendor')} → "
            f"{review.get('review_vendor')}: "
            f"{'clean' if review.get('accepted') is True else 'not accepted'}"
            f" at `{str(review.get('reviewed_head') or '')[:12]}`"
        )
    findings = result.get("findings") if result is not None else None
    if isinstance(findings, list) and findings:
        lines += ["", "**Findings and outcomes:**"]
        for f in findings[:20]:
            if not isinstance(f, dict):
                continue
            # One list item each (no internal finding label): its evidence stays on one line.
            note = " ".join(str(f.get("evidence") or "").split())[:300]
            lines.append(
                f"- {f.get('severity')} ({f.get('source')}): {f.get('disposition')}"
                + (f" — {note}" if note else "")
            )
    else:
        lines.append("**Findings:** none reported")
    return "\n".join(lines)


def _authority(parcel: Parcel) -> tuple[str, str]:
    approval = parcel.current_approval
    if approval is not None and approval.kind == ApprovalKind.SKIP:
        value = approval.snapshot_canonical or ""
        return value, approval.full_hash
    contract = parcel.current_contract
    if contract is None:
        return "", "missing"
    return contract.canonical, contract.full_hash


def render_contract_comment(
    parcel: Parcel,
    contract: Contract,
    plan: dict[str, Any] | None,
    config: ServiceConfig,
) -> str | None:
    """Readable plan comment: parcel marker, deterministic contract section, context.

    The contract section is :func:`render_contract_section` of the stored canonical bytes
    (hash-bound, never neutralised or truncated afterwards). Approach and risks are the
    agent's free-form context, shown outside the approved section, neutralised and
    truncated to fit. An unrenderable or oversized contract section is refused.
    """
    try:
        section = render_contract_section(contract.canonical)
    except ContractViewError:
        return None
    # The heading and context sit outside the hash-bound section: only the section between
    # its markers (and the parcel marker's hash) is verified, so they never change it.
    heading = (
        f"{parcel_marker(parcel.issue_number or 0, contract.prefix)}\n"
        f"### Plan for #{parcel.issue_number or 0} (size {contract.size.value})"
        f" · hash `{contract.prefix}`"
    )
    blocked = _credential_block(
        contract.canonical + "\n" + json.dumps(plan or {}, ensure_ascii=False), config
    )
    if blocked is not None:
        return blocked
    fixed = len(heading) + len(section) + 64
    if fixed > _CANONICAL_PUBLICATION_LIMIT:
        return None
    context = _neutralise(_plan_context(plan))
    budget = min(_CANONICAL_PUBLICATION_LIMIT - fixed, 30_000)
    if len(context) > budget:
        note = "\n\n[context truncated]"
        context = context[: max(budget - len(note), 0)].rstrip() + note
    parts = [heading, section, "---", context] if context else [heading, section]
    return "\n\n".join(parts)


def _plan_context(plan: dict[str, Any] | None) -> str:
    if plan is None:
        return ""
    lines = ["#### Context (not part of the approved contract)"]
    if plan.get("approach"):
        lines += ["", "**Approach**", "", str(plan["approach"]).strip()]
    risks = _bullets(plan.get("risks"))
    if risks:
        lines += ["", "**Risks**", "", risks]
    return "\n".join(lines) if len(lines) > 1 else ""


#: Status comments: only what the owner must read or act on (each one notifies him).
#: Informational status goes to the card's "Factory note" field instead (reducer).
#: Plain sentences, no internal ids or how-to lines (a plain reply is enough).
_STATUS_TEXT = {
    "checkpoint": "I've used the time I was given and I'm wrapping up. "
    "`/continue` (or e.g. `/continue 2h`) gives me more.",
    "ready-blocked": "PR #{pr_number} isn't ready at `{head_sha}`: {reason}. "
    "I have no automatic fix attempt left, so I need your call.",
    "create-rejected": "Omnigent refused to create the session ({reason}), so this is blocked.",
    "adoption-ambiguous": "I couldn't tell which Omnigent session is mine ({matches} matches), "
    "so this is blocked for operator review.",
    "decision": "I have a question for you (impact {impact}).",
    "stop-unverified": "A stop couldn't be verified, so this is blocked until the session is "
    "confirmed idle.",
    "restart-exhausted": "The session stopped and couldn't be restarted automatically, so this "
    "is blocked.",
}


def agent_display_name(config: ServiceConfig) -> str:
    """The factory agent's name for owner-facing text (config ``omnigent_agent_name``)."""
    name = " ".join(config.omnigent_agent_name.split())
    return name[:1].upper() + name[1:] if name else "The factory agent"


def _status_text(template: str, args: Mapping[str, Any], agent: str = "The factory agent") -> str:
    fallback = f"Status: {template.replace('-', ' ')}."
    pattern = _STATUS_TEXT.get(template)
    if pattern is None:
        return fallback
    values = {key: str(value).replace("_", " ") for key, value in args.items()}
    values.update({key: str(value) for key, value in args.items() if key.endswith("_id")})
    values["agent"] = agent
    try:
        return pattern.format(**values)
    except (KeyError, IndexError, ValueError):
        return fallback


def _neutralise(text: str) -> str:
    """Model text published as the bot: no mentions or hidden HTML comments.

    An HTML comment could mimic an effect or contract marker, so it is broken while
    staying legible. Everything else (code fences included) is the agent's Markdown.
    """
    text = re.sub(r"(?<![\w@])@(?=[A-Za-z0-9])", "@\u200b", text)
    return text.replace("<!--", "&lt;!--")


def _bullets(items: object) -> str:
    return "\n".join(f"- {item}" for item in items) if isinstance(items, list) and items else ""


def _public_result(result: dict[str, Any], agent: str = "The factory agent") -> str:
    kind = str(result.get("kind") or "result")
    if kind == "triage":
        recommendation = str(result.get("recommendation") or "").replace("_", " ")
        lines = [
            "### Triage",
            "",
            str(result.get("summary", "")).strip(),
            "",
            f"**Recommendation:** {recommendation}",
            f"**Priority:** {result.get('priority')} · **Size:** {result.get('size')}",
        ]
        if result.get("duplicate_issue"):
            lines.append(f"**Duplicate of:** #{result.get('duplicate_issue')}")
        labels = result.get("labels")
        if isinstance(labels, list) and labels:
            lines.append("**Suggested labels:** " + ", ".join(f"`{label}`" for label in labels))
        missing = _bullets(result.get("missing_information"))
        if missing:
            lines += ["", "**Missing information:**", missing]
        related = _related_lines(result.get("related"))
        if related:
            lines += ["", "**Related:**", *related]
        return "\n".join(lines)
    if kind == "plan":
        contract = result.get("contract")
        contract = contract if isinstance(contract, dict) else {}
        criteria = contract.get("acceptance_criteria")
        lines = [
            "### Plan",
            "",
            f"**Goal:** {contract.get('goal', '')}",
            f"**Approach:** {result.get('approach', '')}",
        ]
        if isinstance(criteria, list) and criteria:
            lines += ["", "**Acceptance criteria:**"]
            lines += [
                f"- {item.get('criterion', '')}" for item in criteria if isinstance(item, dict)
            ]
        risks = _bullets(result.get("risks"))
        if risks:
            lines += ["", "**Risks:**", risks]
        return "\n".join(lines)
    if kind == "blocked":
        lines = [f"### {agent} is blocked", "", str(result.get("reason", "")).strip()]
        done = _bullets(result.get("done"))
        if done:
            lines += ["", "**Done so far:**", done]
        return "\n".join(lines)
    if kind == "checkpoint":
        lines = ["### Checkpoint"]
        for title, key in (("Done", "done"), ("Remaining", "remaining"), ("Risks", "risks")):
            section = _bullets(result.get(key))
            if section:
                lines += ["", f"**{title}:**", section]
        return "\n".join(lines)
    if kind == "build_ready":
        return (
            f"{str(result.get('summary', '')).strip()}\n\n"
            f"PR #{result.get('pr_number')} at `{result.get('head_sha')}`; "
            f"release readiness: {result.get('release_readiness')}."
        )
    return "Report recorded."


#: How each related issue reads in the triage comment: "- Overlaps #12: note".
_RELATION_TEXT = {
    "duplicate": "Duplicate of",
    "overlaps": "Overlaps",
    "conflicts": "Conflicts with",
    "depends_on": "Depends on",
    "blocks": "Blocks",
    "supersedes": "Supersedes",
}


def _related_lines(items: object) -> list[str]:
    """One plain list item per related issue (``#N`` links it; no internal ids)."""
    lines: list[str] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or not isinstance(item.get("issue"), int):
            continue
        verb = _RELATION_TEXT.get(str(item.get("relation")), "Related to")
        note = " ".join(str(item.get("note") or "").split())
        lines.append(f"- {verb} #{item['issue']}" + (f": {note}" if note else ""))
    return lines


def _safe_publication(text: str, config: ServiceConfig) -> str:
    """Free-form publication only: block credentials, neutralise mentions, bound length.

    The agent's Markdown is kept as written (paragraphs, lists, headings, code).
    """
    blocked = _credential_block(text, config)
    if blocked is not None:
        return blocked
    # GitHub expands mentions even when their text originated in an untrusted issue.
    # Break the trigger while retaining legible attribution, then bound bot output.
    text = _neutralise(text)
    limit = 8_000
    if len(text) > limit:
        text = text[: limit - 30].rstrip() + "\n\n[truncated]"
    return text


def _credential_block(text: str, config: ServiceConfig) -> str | None:
    fingerprints: list[str] = []
    for path in (
        config.resolved_webhook_secret_file,
        config.resolved_omnigent_token_file,
        config.resolved_mcp_token_file,
    ):
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if len(value) >= 8:
            fingerprints.append(value)
    if _TOKEN.search(text) or any(value in text for value in fingerprints):
        return "Publication blocked: the result contained credential-like material."
    return None


_PAYLOADS = (
    "SELECT d.body FROM deliveries d JOIN events e ON e.delivery_guid = d.delivery_guid "
    "WHERE e.parcel_id = ? AND (? IS NULL OR e.kind = ?) "
)
_NEWEST_PAYLOADS = _PAYLOADS + "ORDER BY e.sequence DESC LIMIT 50"
_OLDEST_PAYLOADS = _PAYLOADS + "ORDER BY e.sequence ASC LIMIT 50"


def _root_nonce(parcel: Parcel, session: StageSession) -> str | None:
    """The creation label of the issue session a reused run executes in."""
    issue = parcel.issue_session
    if (
        issue is not None
        and session.root_id is not None
        and issue.root_id == session.root_id
        and issue.created_by != session.session_id
    ):
        return issue.nonce
    return None


def _feedback_text(raw: object, review_comments: object) -> tuple[str, int | None, str] | None:
    """(source, PR number, text) of an owner comment/review delivery body.

    ``source`` is ``issue`` (issue comment), ``pr_comment`` (PR conversation comment) or
    ``pr_review`` (a review: its body, then each inline comment as ``path:line: text``).
    """
    if not isinstance(raw, bytes | bytearray | memoryview | str):
        return None
    try:
        payload = json.loads(raw if isinstance(raw, str) else bytes(raw))
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    review = payload.get("review")
    pr = payload.get("pull_request")
    if isinstance(review, dict) and isinstance(pr, dict):
        number = pr.get("number") if isinstance(pr.get("number"), int) else None
        state = str(review.get("state") or "").lower().replace("_", " ")
        lines = [f"PR #{number} review ({state})"]
        if isinstance(review.get("body"), str) and review["body"].strip():
            lines.append(review["body"])
        for c in _json_list(review_comments):
            where = f"{c.get('path') or '?'}:{c.get('line')}" if c.get("line") else c.get("path")
            lines.append(f"- {where or 'PR'}: {c.get('body') or ''}")
        return "pr_review", number, "\n".join(lines)
    comment = payload.get("comment")
    text = comment.get("body") if isinstance(comment, dict) else None
    if not isinstance(text, str):
        return None
    issue = payload.get("issue")
    if isinstance(issue, dict) and "pull_request" in issue:
        number = issue.get("number") if isinstance(issue.get("number"), int) else None
        return "pr_comment", number, text
    return "issue", None, text


def _json_list(raw: object) -> list[dict[str, Any]]:
    if not isinstance(raw, str):
        return []
    try:
        value = json.loads(raw)
    except ValueError:
        return []
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def untrusted_block(text: str, boundary: str, label: str = "UNTRUSTED DATA") -> str:
    """``text`` framed by an unguessable boundary it cannot reproduce."""
    return f"BEGIN {label} {boundary}\n{_untrusted(text, boundary)}\nEND {label} {boundary}"


def new_boundary() -> str:
    return _new_boundary()


def _untrusted(text: str, boundary: str) -> str:
    """Prevent untrusted payloads from reproducing their durable random frame."""
    return text.replace(boundary, "[escaped factory data boundary]")


def _new_boundary() -> str:
    return f"FACTORY_DATA_{secrets.token_hex(16)}"


#: First-message template of each stage run (the dispatch snapshot pins name and bytes).
_FIRST_TEMPLATES = {
    SessionKind.TRIAGE: "triage-v6.txt",
    SessionKind.PLAN: "plan-v6.txt",
    SessionKind.BUILD: "build-v8.txt",
}
_RETRIAGE_TEMPLATE = "triage-feedback-v4.txt"
_REWORK_TEMPLATE = "build-rework-v5.txt"
#: A rework run, or a wake of the waiting build run, for a merge conflict with the base.
_CONFLICT_TEMPLATE = "build-conflict-v1.txt"
_CONFLICT_WAKE_TEMPLATE = "readiness-conflict-wake-v1.txt"


def _template(name: str) -> str:
    return (files("omnigent_factory.service") / "templates" / name).read_text(encoding="utf-8")


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _safe(value: str) -> str:
    return "".join(char if char.isalnum() or char in "-_" else "_" for char in value)[:128]
