"""Owner-facing refusal notes explain the stable audit reason in plain English."""

from __future__ import annotations

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.projection import NOTE_MAX
from omnigent_factory.core.reducer import _REFUSAL_NOTES
from omnigent_factory.core.types import IssueSnapshot
from omnigent_factory.testing.builders import snapshot
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def test_assigned_issue_refusal_names_the_assignee_and_keeps_the_reason_code() -> None:
    h = Harness()
    r = h.send(
        P,
        ev.RequestTriage(),
        evidence=snapshot(human_assigned=True, human_assignees=("andrewreid",)),
    )

    assert not r.audit.accepted and r.audit.reason == "parcel-not-eligible"
    assert h.p().note == (
        "Not triaged: #1 is assigned to @andrewreid. Unassign it to let the factory work on it."
    )


def test_closed_issue_refusal_explains_how_to_fix_it() -> None:
    h = Harness()
    r = h.send(P, ev.RequestTriage(), evidence=snapshot(open=False))

    assert not r.audit.accepted and r.audit.reason == "parcel-not-eligible"
    assert h.p().note == ("Not triaged: #1 is closed. Reopen it to let the factory work on it.")


@pytest.mark.parametrize(
    ("evidence", "message"),
    [
        (
            snapshot(identity_resolved=False),
            "Not triaged: #1 could not be verified as an issue in this repository.",
        ),
        (
            snapshot(repo_matches=False),
            "Not triaged: #1 belongs to another repository.",
        ),
        (
            snapshot(content_type="PullRequest"),
            "Not triaged: #1 is a pull request, not an issue.",
        ),
    ],
)
def test_other_ineligibility_causes_are_specific(evidence: IssueSnapshot, message: str) -> None:
    h = Harness()
    r = h.send(P, ev.RequestTriage(), evidence=evidence)

    assert not r.audit.accepted and r.audit.reason == "parcel-not-eligible"
    assert h.p().note == message


def test_generic_refusal_is_plain_while_the_audit_code_stays_stable() -> None:
    h = Harness()
    h.eligible()
    r = h.send(P, ev.Continue())

    assert not r.audit.accepted and r.audit.reason == "nothing-to-continue"
    assert h.p().note == "Cannot continue: this issue has no current run."


def test_every_static_explained_reason_has_a_short_plain_message() -> None:
    expected = {
        "control-not-fresh-after-barrier",
        "grant-duration-out-of-bounds",
        "triage-not-allowed-from-stage",
        "epic-plan-not-posted",
        "epic-plan-revision-pending",
        "open-decisions",
        "hash-does-not-identify-latest",
        "epic-plan-not-posted-before-control",
        "plan-not-allowed-from-ready",
        "plan-not-allowed-from-stage",
        "no-published-contract",
        "approval-stage-invalid",
        "waiver-without-fresh-snapshot",
        "revision-in-flight",
        "waiver-stage-invalid",
        "decision-ambiguous",
        "decision-not-open",
        "nothing-to-continue",
        "not-at-checkpoint",
        "continue-cannot-clear-hard-fence",
        "session-retired",
        "stage-authority-invalid",
        "tree-not-quiescent",
        "rework-only-from-ready",
        "hard-fence-present",
        "continuation-preflight-failed",
        "card-in-ready",
    }
    assert set(_REFUSAL_NOTES) == expected
    assert all(
        0 < len(note) <= NOTE_MAX and "Command refused" not in note
        for note in _REFUSAL_NOTES.values()
    )
