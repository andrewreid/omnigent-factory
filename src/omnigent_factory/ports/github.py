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

Status identity (T2 F3): the Status column is read and written by *option ID*, never by
option name. Renaming an option therefore cannot silently change a parcel's stage; an
unknown option ID reads as "no known stage". :data:`STATUS_OPTION_IDS` preserves the live
board's IDs (the setup renderer keeps them on migration). Column display names come from
host config (``status_names``); :data:`DEFAULT_STATUS_NAMES` is only its default.

PR linkage (T2 F1): :meth:`GitHubReader.pull_request` receives the parcel's
:class:`IssueRef`; ``closes_issue`` is true only when GitHub's own closing-issue
references for that PR contain exactly this parcel's issue node.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from omnigent_factory.core.effects import EffectKind, RetryableReadFailure
from omnigent_factory.core.events import ChecksState
from omnigent_factory.core.types import IssueSnapshot, Stage
from omnigent_factory.ports.adapter import EffectAdapter

GITHUB_EFFECT_KINDS = frozenset(
    {
        EffectKind.MOVE_CARD,
        EffectKind.SET_BOT,
        EffectKind.SET_NOTE,
        EffectKind.REACT_COMMENT,
        EffectKind.POST_COMMENT,
        EffectKind.PUBLISH_CONTRACT,
        EffectKind.PUBLISH_TRIAGE,
        EffectKind.PUBLISH_REPORT,
        EffectKind.ENSURE_PROJECT_ITEM,
        EffectKind.FETCH_PR_EVIDENCE,
        EffectKind.RECONCILE_PARCEL,
    }
)


#: Project 5 Status field node ID.
STATUS_FIELD_NODE_ID = "PVTSSF_lADOEanNes4BkJhbzhi7I9w"

#: Live Status option IDs per stage (project 5). Preserved across board migrations.
STATUS_OPTION_IDS: Mapping[Stage, str] = MappingProxyType(
    {
        Stage.INBOX: "915abb46",
        Stage.TRIAGED: "43889573",
        Stage.SCOPED: "3a7f779a",
        Stage.BUILDING: "ba3c85dd",
        Stage.READY: "6df89cbb",
        Stage.DONE: "4980e49d",
    }
)

#: Default Status option display names per stage; host config ``status_names`` overrides
#: them. Only the setup renderer and ``doctor`` read names; logic never does.
DEFAULT_STATUS_NAMES: Mapping[Stage, str] = MappingProxyType(
    {stage: stage.value for stage in STATUS_OPTION_IDS}
)


def stage_for_option(
    option_id: object, options: Mapping[Stage, str] = STATUS_OPTION_IDS
) -> Stage | None:
    """The stage whose Status option has exactly ``option_id`` (names are never read)."""
    if not isinstance(option_id, str) or not option_id:
        return None
    matches = [stage for stage, known in options.items() if known == option_id]
    return matches[0] if len(matches) == 1 else None


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
    checks_summary: str = ""
    #: When the configured review bot can still respond to ``head_sha``: the source time
    #: of the latest trigger it has not answered (a push/PR open or an explicit re-ping),
    #: 0 when it already answered this head and nothing re-pinged it, None when unknown.
    review_bot_pending_since_us: int | None = None

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
    """A bot comment found by effect marker: its rendered contract section and hash marker.

    Nothing is parsed back as JSON; the section is compared with the deterministic
    rendering of the stored contract (``core.contract_view``).
    """

    comment_id: str
    author_is_bot: bool
    contract_section: str | None
    marker_hash: str | None
    posted_at_us: int


@runtime_checkable
class GitHubReader(Protocol):
    """Fresh reads used as reducer evidence. Reads never mutate GitHub."""

    async def issue_snapshot(self, ref: IssueRef) -> IssueSnapshot | RetryableReadFailure: ...

    async def pull_request(
        self, ref: IssueRef, pr_number: int
    ) -> PullRequestEvidence | RetryableReadFailure:
        """Fresh PR evidence judged against the parcel issue ``ref`` (closing linkage)."""
        ...

    async def find_contract_publication(
        self, ref: IssueRef, effect_id: str
    ) -> ContractPublication | RetryableReadFailure | None: ...


@runtime_checkable
class GitHubAdapter(GitHubReader, EffectAdapter, Protocol):
    """Combined GitHub reader + effect executor."""
