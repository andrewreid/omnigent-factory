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
    escape_inline,
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
    IssueSnapshot,
    Parcel,
    SessionKind,
    StageSession,
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
        template_name = f"{session.kind.value}-v2.txt"
        template = _template(template_name)
        evidence = await self.issue_evidence(parcel)
        issue_title = " ".join((evidence.title if evidence is not None else "").split())
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
            "title": f"#{issue} · {issue_title}"[:200] if issue_title else f"#{issue}",
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
        template_name = str(snapshot.get("template") or "")
        template = _template(template_name)
        boundary = snapshot.get("untrusted_boundary")
        if not isinstance(boundary, str) or not boundary:
            boundary = _new_boundary()
            snapshot["untrusted_boundary"] = boundary
            _atomic_json(self.root / f"{_safe(sid)}.json", snapshot)
        purpose = str(effect.args.get("purpose") or "first")
        if purpose == "checkpoint_cleanup":
            return _template("checkpoint-v2.txt").format(grant_id=spec.grant_id, run_id=sid)
        if purpose == "continuation":
            return _template("continuation-v1.txt").format(grant_id=spec.grant_id)
        if purpose == "answer_relay":
            decision = parcel.decision(str(effect.args.get("decision_id") or ""))
            if decision is None or decision.answer is None:
                return None
            return _template("answer-v1.txt").format(
                decision_id=decision.decision_id, answer=decision.answer
            )
        if purpose == "feedback":
            return _template("feedback-v2.txt").format(revision=parcel.revision, run_id=sid)
        if purpose == "readiness_wake":
            return _template("readiness-wake-v2.txt").format(
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
        if decision is None or decision.answer is None:
            return None
        try:
            value: Any = json.loads(decision.answer)
        except ValueError:
            return {"answer": decision.answer}
        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            return {"answer": decision.answer}
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

    async def owner_comments(
        self, parcel: Parcel, *, since_us: int = 0, kinds: tuple[EventKind, ...] = ()
    ) -> list[dict[str, Any]]:
        """Owner comment texts carried by the parcel's control events, oldest first."""
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
                "SELECT e.event_id, e.kind, e.source_time_us, d.body FROM events e "
                "JOIN deliveries d ON e.delivery_guid = d.delivery_guid "
                "WHERE e.parcel_id = ? AND e.accepted = 1 "
                "AND e.kind IN (SELECT value FROM json_each(?)) "
                "AND e.source_time_us >= ? ORDER BY e.sequence",
                (parcel_id, kinds_json, since_us),
            )
        )
        comments: list[dict[str, Any]] = []
        for row in rows:
            text = _comment_body(row[3])
            if text is not None:
                comments.append(
                    {
                        "event_id": str(row[0]),
                        "kind": str(row[1]),
                        "at_us": int(row[2]),
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
            link = self._stage_link(parcel, contract.source_session_id)
            return render_contract_comment(parcel, contract, plan, self.config, link)
        if effect.kind == EffectKind.PUBLISH_REPORT and effect.args.get("report") == "ready":
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
            )
        elif effect.kind in {EffectKind.PUBLISH_TRIAGE, EffectKind.PUBLISH_REPORT}:
            body = self._stored_result(effect)
            if body is None:
                return None
            text = _public_result(body)
        elif effect.kind == EffectKind.POST_COMMENT and effect.args.get("template") == "decision":
            return _safe_publication(self._decision_text(effect), self.config)
        elif effect.kind == EffectKind.POST_COMMENT:
            template = str(effect.args.get("template") or "status")
            text = _status_text(template, effect.args)
        else:
            return None
        sid = effect.args.get("session_id") or effect.preconditions.session_id
        if effect.args.get("report") == "ready" and parcel.readiness is not None:
            sid = parcel.readiness.session_id
        link = self._stage_link(parcel, str(sid) if sid else None)
        if link is not None:
            text = f"{text}\n\n[Open in Omnigent]({link})"
        return _safe_publication(text, self.config)

    def _stage_link(self, parcel: Parcel, session_id: str | None) -> str | None:
        """Deeplink to the stage's root Omnigent session (the named one, else current)."""
        session = parcel.session(session_id) if session_id else parcel.current_session
        if session is None:
            session = parcel.current_session
        return omnigent_link(self.config.omnigent_base_url, session.root_id if session else None)

    def _decision_text(self, effect: EffectIntent) -> str:
        """What Molly is asking, where to answer it, and the ``/decide`` fallback."""
        args = effect.args
        decision_id = str(args.get("decision_id") or "")
        summary = str(args.get("summary") or "").strip()
        node = omnigent_link(self.config.omnigent_base_url, _text_or_none(args.get("node_id")))
        root = omnigent_link(self.config.omnigent_base_url, _text_or_none(args.get("root_id")))
        lines = [f"**Factory: the agent is waiting for an answer** (decision `{decision_id}`)."]
        if summary:
            lines += ["", *(f"> {line}" for line in summary.splitlines())]
        where: list[str] = []
        if node is not None:
            where.append(f"[open the prompt in Omnigent]({node})")
        if root is not None and root != node:
            where.append(f"[issue session]({root})")
        answer = "Answer it in Omnigent" + (f" ({', '.join(where)})" if where else "")
        lines += [
            "",
            f"{answer}, or comment `/decide {decision_id} <answer>`. The card returns to "
            "Working once the prompt is answered.",
        ]
        return "\n".join(lines)


_OMNIGENT_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


def omnigent_link(base_url: str, session_id: str | None) -> str | None:
    """``{omnigent_base_url}/c/{session_id}``: Omnigent's share link (normal Omnigent auth)."""
    if not session_id or _OMNIGENT_ID.fullmatch(session_id) is None:
        return None
    return f"{base_url.rstrip('/')}/c/{session_id}"


def _text_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _ready_text(
    args: Mapping[str, Any],
    result: dict[str, Any] | None,
    checks_summary: str,
    repository: str,
) -> str:
    """Ready report: what changed, CI, the cross-vendor review, findings and next step."""
    number = args.get("pr_number")
    head = str(args.get("head_sha") or "")
    url = f"https://github.com/{repository}/pull/{number}"
    lines = [f"### Factory: PR #{number} is Ready", ""]
    if result is not None and result.get("head_sha") == head and result.get("summary"):
        lines += [" ".join(str(result["summary"]).split())[:1200], ""]
    lines.append(f"**PR:** {url} (head `{head[:12]}`)")
    lines.append(f"**CI:** {checks_summary or 'required checks green'}")
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
            note = " ".join(str(f.get("evidence") or "").split())[:300]
            lines.append(
                f"- `{f.get('id')}` {f.get('severity')} ({f.get('source')}): "
                f"{f.get('disposition')}" + (f" — {note}" if note else "")
            )
    else:
        lines.append("**Findings:** none reported")
    lines += ["", f"**Next:** review and merge PR #{number}."]
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
    session_link: str | None = None,
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
    heading = (
        f"{parcel_marker(parcel.issue_number or 0, contract.prefix)}\n"
        f"### Plan for #{parcel.issue_number or 0} (size {contract.size.value})"
        f" · hash `{contract.prefix}`\n\n"
        "The approved contract is the section below; approving binds to exactly this text "
        f"(hash `{contract.prefix}`)."
    )
    respond = (
        "**How to respond:** drag the card to Building to approve this exact plan (or "
        f"comment `/approve {contract.prefix}`), or reply with feedback for a revision."
    )
    if session_link is not None:
        # Outside the hash-bound section: the link never affects the approval target.
        respond += f"\n\n[Open in Omnigent]({session_link})"
    open_ids = [d.decision_id for d in parcel.open_decisions]
    if open_ids:
        respond = (
            "**Open decisions** (answer in Omnigent or `/decide <id> <answer>`; approval "
            "waits for them):\n\n"
            + "\n".join(f"- `{escape_inline(i)}`" for i in open_ids)
            + "\n\n"
            + respond
        )
    blocked = _credential_block(
        contract.canonical + "\n" + json.dumps(plan or {}, ensure_ascii=False), config
    )
    if blocked is not None:
        return blocked
    fixed = len(heading) + len(section) + len(respond) + 64
    if fixed > _CANONICAL_PUBLICATION_LIMIT:
        return None
    context = _neutralise(_plan_context(plan))
    budget = min(_CANONICAL_PUBLICATION_LIMIT - fixed, 30_000)
    if len(context) > budget:
        note = "\n\n[context truncated]"
        context = context[: max(budget - len(note), 0)].rstrip() + note
    parts = (
        [heading, section, "---", context, respond]
        if context
        else [
            heading,
            section,
            "---",
            respond,
        ]
    )
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


_STATUS_TEXT = {
    "checkpoint": "Factory checkpoint: the granted time is used up and Molly is wrapping up. "
    "Comment `/continue` (optionally with a duration, e.g. `/continue 2h`) to grant more.",
    "queued": "Factory: approval recorded; the build is queued behind earlier approvals.",
    "approval-acknowledged": "Factory: approval recorded; the build starts when capacity allows.",
    "stopped": "Factory: stopped by the owner. Nothing runs until a new stage control.",
    "rework-unsupported": "Factory: rework on a Ready parcel is not supported yet.",
    "approval-invalidated": "Factory: the approval is no longer valid ({reason}). "
    "Approve the current plan again to continue.",
    "pr-closed-unmerged": "Factory: PR #{pr_number} was closed without merging; "
    "the parcel needs an owner decision.",
    "ready-invalidated": "Factory: the parcel is no longer Ready ({reason}).",
    "ready-blocked": "Factory: PR #{pr_number} at `{head_sha}` is not Ready: {reason}. "
    "No automatic fix attempt remains; the parcel needs an owner decision.",
    "create-rejected": "Factory: Omnigent refused to create the session ({reason}); "
    "the parcel is Blocked.",
    "adoption-ambiguous": "Factory: could not tell which Omnigent session is ours "
    "({matches} matches); the parcel is Blocked for operator review.",
    "decision": "Factory: Molly needs a decision (`{decision_id}`, impact {impact}). "
    "Answer it in Omnigent or comment `/decide {decision_id} <answer>`.",
    "decision-resolved-externally": "Factory: decision `{decision_id}` was resolved "
    "outside the factory.",
    "result-invalid": "Factory status: the stage result was invalid; the parcel is Blocked.",
    "stop-unverified": "Factory: a stop could not be verified; the parcel is Blocked until "
    "the session tree is confirmed idle.",
    "restart-exhausted": "Factory: the session stopped and could not be restarted "
    "automatically; the parcel is Blocked.",
    "queue-entry-invalid": "Factory: a queued build was dropped because its approval is "
    "no longer valid.",
    "control-rejected": "Factory: that command was not accepted ({reason}).",
}


_CONTROL_NAMES = {
    "WaivePlan": "start building (plan waived)",
    "ApprovePlan": "approve the plan",
    "RequestPlan": "start a plan",
    "RequestReplan": "start a replan",
    "RequestTriage": "start triage",
    "Continue": "continue",
    "Decide": "record that answer",
}
_REFUSAL_REASONS = {
    "open-decisions": "{n} open question(s) from an earlier session ({ids}). Answer in "
    "Omnigent or comment `/decide <id> <answer>`, then try again.",
    "revision-in-flight": "a plan revision is still in progress; wait for the updated plan.",
    "waiver-stage-invalid": "the card can skip the plan only from Inbox or Triaged, and not "
    "while another build or approval is active.",
    "approval-stage-invalid": "approval is only possible from Scoped while no build is live.",
    "plan-not-approvable": "the current plan is not approvable yet (open decisions or a "
    "pending revision).",
    "no-published-contract": "there is no published plan to approve.",
    "hash-does-not-identify-latest": "that hash does not identify the latest plan.",
    "decision-not-open": "that decision is not open (it may already be answered in Omnigent).",
    "decision-ambiguous": "more than one question is open; name it with `/decide <id> <answer>`.",
    "parcel-not-eligible": "the issue is not eligible (closed, assigned to a person, or not "
    "on the board).",
    "not-at-checkpoint": "nothing is waiting at a checkpoint.",
}


def _refusal_text(args: Mapping[str, Any]) -> str:
    """A refused owner control in plain language, with the next step and any card rollback."""
    control = _CONTROL_NAMES.get(str(args.get("control") or ""), "do that")
    reason = str(args.get("reason") or "")
    ids = [i for i in str(args.get("open_decisions") or "").split(",") if i]
    pattern = _REFUSAL_REASONS.get(reason)
    why = (
        pattern.format(n=len(ids), ids=", ".join(f"`{i}`" for i in ids) or "none recorded")
        if pattern is not None
        else f"the request was not valid now ({reason.replace('-', ' ')})."
    )
    text = f"Factory: couldn't {control}: {why}"
    rolled = args.get("rolled_back_to")
    if isinstance(rolled, str) and rolled:
        text += f" The card was moved back to {rolled}; drag it again when ready."
    return text


def _status_text(template: str, args: Mapping[str, Any]) -> str:
    if template == "control-rejected":
        return _refusal_text(args)
    fallback = f"Factory status: {template.replace('-', ' ')}."
    pattern = _STATUS_TEXT.get(template)
    if pattern is None:
        return fallback
    values = {key: str(value).replace("_", " ") for key, value in args.items()}
    values.update({key: str(value) for key, value in args.items() if key.endswith("_id")})
    try:
        return pattern.format(**values)
    except (KeyError, IndexError, ValueError):
        return fallback


def _neutralise(text: str) -> str:
    """Model text published as the bot: no mentions, fences or hidden HTML comments.

    A fence could mimic the canonical ``parcel-contract`` block and an HTML comment could
    mimic an effect marker, so both are broken while staying legible.
    """
    text = re.sub(r"(?<![\w@])@(?=[A-Za-z0-9])", "@\u200b", text)
    text = text.replace("```", "`\u200b``").replace("~~~", "~\u200b~~")
    return text.replace("<!--", "&lt;!--")


def _bullets(items: object) -> str:
    return "\n".join(f"- {item}" for item in items) if isinstance(items, list) and items else ""


def _public_result(result: dict[str, Any]) -> str:
    kind = str(result.get("kind") or "result")
    if kind == "triage":
        recommendation = str(result.get("recommendation") or "").replace("_", " ")
        lines = [
            "### Factory triage",
            "",
            str(result.get("summary", "")),
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
        lines += ["", "Move the card to Scoped to request a plan, or Building to waive it."]
        return "\n".join(lines)
    if kind == "plan":
        contract = result.get("contract")
        contract = contract if isinstance(contract, dict) else {}
        criteria = contract.get("acceptance_criteria")
        lines = [
            "### Factory plan (informational)",
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
        lines = [
            "### Factory: Molly stopped and needs the owner",
            "",
            f"> {' '.join(str(result.get('reason', '')).split())}",
        ]
        done = _bullets(result.get("done"))
        if done:
            lines += ["", "**Done so far:**", done]
        lines += [
            "",
            "The card is Blocked. Fix the cause, then resume this session "
            "(`omnigent-factory resume <parcel> --message ...`) or give a new stage control.",
        ]
        return "\n".join(lines)
    if kind == "checkpoint":
        lines = ["### Factory checkpoint"]
        for title, key in (("Done", "done"), ("Remaining", "remaining"), ("Risks", "risks")):
            section = _bullets(result.get(key))
            if section:
                lines += ["", f"**{title}:**", section]
        return "\n".join(lines)
    if kind == "build_ready":
        return (
            f"Factory build report: {result.get('summary', '')}\n\n"
            f"PR #{result.get('pr_number')} at `{result.get('head_sha')}`; "
            f"release readiness: {result.get('release_readiness')}."
        )
    return "Factory report recorded."


def _safe_publication(text: str, config: ServiceConfig) -> str:
    """Free-form publication only: block credentials, neutralise mentions, bound length."""
    blocked = _credential_block(text, config)
    if blocked is not None:
        return blocked
    # GitHub expands mentions even when their text originated in an untrusted issue.
    # Break the trigger while retaining legible attribution, then bound bot output.
    text = _neutralise(text)
    limit = 8_000
    if len(text) > limit:
        text = text[: limit - 30].rstrip() + "\n\n[factory output truncated]"
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
        return (
            "Factory publication blocked: the result contained credential-like material. "
            "Provide a redacted result before continuing."
        )
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


def _comment_body(raw: object) -> str | None:
    if not isinstance(raw, bytes | bytearray | memoryview | str):
        return None
    try:
        payload = json.loads(raw if isinstance(raw, str) else bytes(raw))
    except (ValueError, TypeError):
        return None
    comment = payload.get("comment") if isinstance(payload, dict) else None
    text = comment.get("body") if isinstance(comment, dict) else None
    return text if isinstance(text, str) else None


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
