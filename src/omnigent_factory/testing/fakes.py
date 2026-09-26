"""In-memory adapter fakes conforming to the :mod:`omnigent_factory.ports` protocols.

Each fake records every executed intent in ``calls`` and returns scripted outcomes:
push outcomes with :meth:`_ScriptedAdapter.script` (per effect kind, FIFO); otherwise the
default is :class:`~omnigent_factory.core.effects.Ack`.
"""

from __future__ import annotations

import itertools
from collections import defaultdict, deque
from dataclasses import dataclass, field

from omnigent_factory.core.effects import (
    Ack,
    AdapterOutcome,
    CredentialProfile,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    RetryableReadFailure,
)
from omnigent_factory.core.types import IssueSnapshot
from omnigent_factory.ports.credentials import CREDENTIAL_EFFECT_KINDS, TokenGrant, TokenRefusal
from omnigent_factory.ports.github import (
    GITHUB_EFFECT_KINDS,
    ContractPublication,
    IssueRef,
    PullRequestEvidence,
)
from omnigent_factory.ports.omnigent import OMNIGENT_EFFECT_KINDS, SessionMatch, TreeScan
from omnigent_factory.ports.scheduler import SCHEDULER_EFFECT_KINDS


class FakeClock:
    """Deterministic clock. ``advance`` moves both UTC and monotonic time."""

    def __init__(self, start_us: int = 1_800_000_000_000_000, monotonic_us: int = 0) -> None:
        self._now = start_us
        self._mono = monotonic_us

    def now_utc_us(self) -> int:
        return self._now

    def monotonic_us(self) -> int:
        return self._mono

    def advance(self, us: int) -> None:
        if us < 0:
            raise ValueError("time does not go backwards")
        self._now += us
        self._mono += us

    def reboot(self) -> None:
        """Simulate a new process boot: monotonic time restarts, UTC continues."""
        self._mono = 0


@dataclass
class _ScriptedAdapter:
    kinds: frozenset[EffectKind]
    calls: list[tuple[EffectIntent, ExecutionContext]] = field(default_factory=list)
    _scripts: dict[EffectKind, deque[AdapterOutcome]] = field(
        default_factory=lambda: defaultdict(deque)
    )
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    @property
    def handled_kinds(self) -> frozenset[EffectKind]:
        return self.kinds

    def script(self, kind: EffectKind, *outcomes: AdapterOutcome) -> None:
        if kind not in self.kinds:
            raise ValueError(f"{kind} not handled by this fake")
        self._scripts[kind].extend(outcomes)

    def _next(self, effect: EffectIntent) -> AdapterOutcome:
        queue = self._scripts[effect.kind]
        if queue:
            return queue.popleft()
        return Ack(remote_id=f"{effect.kind.value}-{next(self._ids)}")

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        if effect.kind not in self.kinds:
            raise ValueError(f"{effect.kind} not handled by this fake")
        self.calls.append((effect, ctx))
        return self._next(effect)

    def executed(self, kind: EffectKind) -> list[EffectIntent]:
        return [e for e, _ in self.calls if e.kind == kind]


@dataclass
class FakeGitHub(_ScriptedAdapter):
    """Implements :class:`~omnigent_factory.ports.github.GitHubAdapter`."""

    kinds: frozenset[EffectKind] = GITHUB_EFFECT_KINDS
    snapshots: dict[str, IssueSnapshot | RetryableReadFailure] = field(default_factory=dict)
    prs: dict[tuple[str, int], PullRequestEvidence] = field(default_factory=dict)
    publications: dict[str, ContractPublication] = field(default_factory=dict)

    async def issue_snapshot(self, ref: IssueRef) -> IssueSnapshot | RetryableReadFailure:
        snap = self.snapshots.get(ref.parcel_id)
        if snap is None:
            return RetryableReadFailure("no snapshot scripted")
        return snap

    async def pull_request(
        self, repo_id: str, pr_number: int
    ) -> PullRequestEvidence | RetryableReadFailure:
        pr = self.prs.get((repo_id, pr_number))
        return pr if pr is not None else RetryableReadFailure("no PR scripted")

    async def find_contract_publication(
        self, ref: IssueRef, effect_id: str
    ) -> ContractPublication | RetryableReadFailure | None:
        return self.publications.get(effect_id)


@dataclass
class FakeOmnigent(_ScriptedAdapter):
    """Implements :class:`~omnigent_factory.ports.omnigent.OmnigentAdapter`."""

    kinds: frozenset[EffectKind] = OMNIGENT_EFFECT_KINDS
    sessions: list[SessionMatch] = field(default_factory=list)
    trees: dict[str, TreeScan] = field(default_factory=dict)

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        outcome = await super().execute(effect, ctx)
        if effect.kind == EffectKind.CREATE_SESSION and isinstance(outcome, Ack):
            nonce = effect.args.get("nonce")
            if isinstance(nonce, str) and outcome.remote_id is not None:
                self.sessions.append(SessionMatch(outcome.remote_id, nonce, None, None, None))
        return outcome

    async def find_by_nonce(self, nonce: str) -> list[SessionMatch] | RetryableReadFailure:
        return [s for s in self.sessions if s.nonce == nonce]

    async def scan_tree(self, root_id: str) -> TreeScan:
        return self.trees.get(
            root_id,
            TreeScan(
                root_id,
                complete=True,
                busy=False,
                pending_waiter=False,
                node_ids=frozenset({root_id}),
            ),
        )


@dataclass
class FakeCredentialBroker(_ScriptedAdapter):
    """Implements :class:`~omnigent_factory.ports.credentials.CredentialBroker`.

    Issuance is denied by default; ``ENABLE_ISSUANCE`` / ``DISABLE_ISSUANCE`` toggle it.
    """

    kinds: frozenset[EffectKind] = CREDENTIAL_EFFECT_KINDS
    enabled: dict[str, CredentialProfile] = field(default_factory=dict)
    repository: str = "SA-Ambulance/timesheets"
    issued: list[tuple[str, CredentialProfile]] = field(default_factory=list)

    async def execute(self, effect: EffectIntent, ctx: ExecutionContext) -> AdapterOutcome:
        outcome = await super().execute(effect, ctx)
        sid = effect.preconditions.session_id
        if isinstance(outcome, Ack) and sid is not None:
            if effect.kind == EffectKind.ENABLE_ISSUANCE:
                profile = effect.args.get("profile", CredentialProfile.READ_ONLY.value)
                self.enabled[sid] = CredentialProfile(str(profile))
            else:
                self.enabled.pop(sid, None)
        return outcome

    def issuance_enabled(self, session_id: str) -> bool:
        return session_id in self.enabled

    async def request_token(
        self, session_id: str, capability_secret: str, repository: str
    ) -> TokenGrant | TokenRefusal:
        profile = self.enabled.get(session_id)
        if profile is None:
            return TokenRefusal("issuance disabled")
        if repository != self.repository:
            return TokenRefusal("wrong repository")
        if not capability_secret:
            return TokenRefusal("missing capability")
        self.issued.append((session_id, profile))
        return TokenGrant(f"fake-token-{len(self.issued)}", profile, repository, 0)


@dataclass
class FakeScheduler(_ScriptedAdapter):
    """Records ``ARM_TIMER`` / ``WAKE_SCHEDULER`` intents."""

    kinds: frozenset[EffectKind] = SCHEDULER_EFFECT_KINDS
