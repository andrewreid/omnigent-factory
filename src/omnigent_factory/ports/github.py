"""GitHub port (implemented by Task 2).

Effect kinds handled by the GitHub adapter: ``MOVE_CARD``, ``SET_BOT``, ``POST_COMMENT``,
``PUBLISH_CONTRACT``, ``PUBLISH_TRIAGE``, ``PUBLISH_REPORT``, ``ENSURE_PROJECT_ITEM``,
``FETCH_PR_EVIDENCE``, ``RECONCILE_PARCEL`` (see :data:`GITHUB_EFFECT_KINDS`).

Every bot-authored write carries a unique effect marker (the ``effect_id``) so a lost
acknowledgement can be adopted by listing comments; a marker posted by another actor is
not ours. Board writes carry the expected source value.

Board-write contract (reviews r1 F2, r2 F2): the reducer records each ``MOVE_CARD`` as a
pending move (source and target column) and gates all work-bearing effects and new trees
while any is pending. Only the executor (``Provenance.ADAPTER``) resolves it:

* after a successful write, read back and emit
  ``ColumnObserved(stage=<exact target>, daemon_effect_id=<effect_id>)``;
* on a definitive failure/cancellation emit ``EffectCancelled`` for it; on an ambiguous
  write emit ``EffectUnknown`` and later ``EffectReconciled`` (``delivered`` with the
  project item ID, or proven absent).

Absence reconciles the source column as a fresh observation (leftward => safety). A
webhook echo naming the effect is never an acknowledgement (``core.admission``).

Ambiguous message/elicitation writes: report ``EffectUnknown``; resolve only with an
executor ``MessageAck`` (issued effect, exact session, real item ID) or
``EffectReconciled``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.core.events import ChecksState
from omnigent_factory.core.types import IssueSnapshot
from omnigent_factory.ports.adapter import EffectAdapter

GITHUB_EFFECT_KINDS = frozenset(
    {
        EffectKind.MOVE_CARD,
        EffectKind.SET_BOT,
        EffectKind.POST_COMMENT,
        EffectKind.PUBLISH_CONTRACT,
        EffectKind.PUBLISH_TRIAGE,
        EffectKind.PUBLISH_REPORT,
        EffectKind.ENSURE_PROJECT_ITEM,
        EffectKind.FETCH_PR_EVIDENCE,
        EffectKind.RECONCILE_PARCEL,
    }
)


@dataclass(frozen=True, slots=True)
class IssueRef:
    repo_id: str
    issue_number: int
    parcel_id: str


@dataclass(frozen=True, slots=True)
class PullRequestEvidence:
    """Fresh PR facts used to verify a build-ready attestation (§8, §7.2)."""

    pr_number: int
    head_sha: str
    open: bool
    merged: bool
    bot_authored: bool
    parcel_branch: bool
    closes_issue: bool
    checks: ChecksState  # automated required checks, human-review-gate excluded
    review_accepted: bool  # independent opposite-vendor review of this head chain
    findings_dispositioned: bool

    @property
    def verified(self) -> bool:
        return (
            self.open
            and self.bot_authored
            and self.parcel_branch
            and self.closes_issue
            and self.checks == ChecksState.GREEN
            and self.review_accepted
            and self.findings_dispositioned
        )


@dataclass(frozen=True, slots=True)
class ContractPublication:
    """A bot comment found by marker, with the exact contract bytes it contains."""

    comment_id: str
    author_is_bot: bool
    canonical: str | None
    posted_at_us: int


@runtime_checkable
class GitHubReader(Protocol):
    """Fresh reads used as reducer evidence. Reads never mutate GitHub."""

    async def issue_snapshot(self, ref: IssueRef) -> IssueSnapshot | RetryableReadFailure: ...

    async def pull_request(
        self, repo_id: str, pr_number: int
    ) -> PullRequestEvidence | RetryableReadFailure: ...

    async def find_contract_publication(
        self, ref: IssueRef, effect_id: str
    ) -> ContractPublication | RetryableReadFailure | None: ...


@runtime_checkable
class GitHubAdapter(GitHubReader, EffectAdapter, Protocol):
    """Combined GitHub reader + effect executor."""
