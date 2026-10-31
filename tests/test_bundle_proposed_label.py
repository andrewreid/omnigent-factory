"""``factory:bundle-proposed``: on every member of an open bundle proposal while its card
is in Triage or Planning, off once no open proposal names it or the card left; an owner's
removal sticks for the proposals open then. Durable through ``SET_BUNDLE_PROPOSED``."""

from __future__ import annotations

import json

from omnigent_factory.core import events as ev
from omnigent_factory.core.admission import ADMISSION
from omnigent_factory.core.effects import VERIFIABLE_WRITE_KINDS, EffectKind
from omnigent_factory.core.events import EventKind, Provenance
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.protocol import BUNDLE_PROPOSED_LABEL
from omnigent_factory.core.reducer import TransitionResult
from omnigent_factory.core.types import Stage, Via
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.ports.github import GITHUB_EFFECT_KINDS
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def _observe(h: Harness, *keys: str, pid: str = P) -> TransitionResult:
    return h.send(
        pid, ev.BundleProposalsObserved(proposal_keys=keys), provenance=Provenance.ADAPTER
    )


def _labels(r: TransitionResult) -> list[str]:
    return [str(e.args["action"]) for e in Harness.of(r, EffectKind.SET_BUNDLE_PROPOSED)]


def _triaged(h: Harness, pid: str = P) -> None:
    h.triage(pid)
    assert h.p(pid).stage == Stage.TRIAGED


def test_label_kind_is_a_durable_automatically_recovered_github_write() -> None:
    assert BUNDLE_PROPOSED_LABEL == "factory:bundle-proposed"
    assert EffectKind.SET_BUNDLE_PROPOSED in VERIFIABLE_WRITE_KINDS
    assert EffectKind.SET_BUNDLE_PROPOSED in GITHUB_EFFECT_KINDS
    assert ADMISSION[EventKind.BUNDLE_PROPOSALS_OBSERVED].provenances == frozenset(
        {Provenance.ADAPTER}
    )


def test_open_proposal_adds_the_label_once_and_closing_it_removes_it() -> None:
    h = Harness()
    _triaged(h)
    r = _observe(h, "k1")
    assert r.audit.accepted and _labels(r) == ["add"]
    assert h.p().bundle_proposed_label and h.p().bundle_proposals == ("k1",)
    # The same set again is no change (idempotent: nothing written, nothing emitted).
    again = _observe(h, "k1")
    assert not again.audit.accepted and again.audit.reason == "bundle-proposals-unchanged"
    assert _labels(again) == []
    # Accepted, refused, stale or superseded: the service reports no open proposal.
    gone = _observe(h)
    assert _labels(gone) == ["remove"] and not h.p().bundle_proposed_label


def test_an_issue_in_several_proposals_keeps_the_label_until_none_remain() -> None:
    h = Harness()
    _triaged(h)
    assert _labels(_observe(h, "k1", "k2")) == ["add"]
    assert _labels(_observe(h, "k2")) == []  # still named by k2
    assert h.p().bundle_proposed_label
    assert _labels(_observe(h)) == ["remove"]


def test_card_outside_triage_or_planning_gets_no_label() -> None:
    h = Harness()
    h.eligible()
    assert h.p().stage in (None, Stage.INBOX)
    r = _observe(h, "k1")
    assert r.audit.accepted and _labels(r) == [] and not h.p().bundle_proposed_label


def test_planning_keeps_the_label_and_leaving_for_building_removes_it() -> None:
    h = Harness()
    _triaged(h)
    _observe(h, "k1")
    r = h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert h.p().stage == Stage.SCOPED and _labels(r) == []
    h.create_ok()
    h.publish_plan()
    approved = h.approve()
    assert h.p().stage == Stage.BUILDING
    assert _labels(approved) == ["remove"] and not h.p().bundle_proposed_label


def test_owner_removal_is_not_fought_for_that_proposal() -> None:
    h = Harness()
    _triaged(h)
    [add] = Harness.of(_observe(h, "k1"), EffectKind.SET_BUNDLE_PROPOSED)
    r = h.send(P, ev.BundleProposedRemoved())
    assert r.audit.accepted and _labels(r) == []  # the label is already gone
    assert not h.p().bundle_proposed_label and h.p().bundle_proposed_removed == ("k1",)
    # A pending or retried add is superseded: never re-added.
    assert effect_still_valid(h.p(), add) == "bundle-proposed-superseded"
    # Re-planning or another unchanged sync does not bring it back for k1.
    assert _labels(_observe(h, "k1")) == []
    # A later proposal naming the issue may add it again.
    r2 = _observe(h, "k1", "k9")
    assert _labels(r2) == ["add"] and h.p().bundle_proposed_label
    # The removal record is dropped with its proposal (bounded).
    _observe(h, "k9")
    assert h.p().bundle_proposed_removed == ()


def test_non_owner_or_unlabelled_removal_is_ignored() -> None:
    h = Harness()
    _triaged(h)
    r = h.send(P, ev.BundleProposedRemoved())
    assert not r.audit.accepted and r.audit.reason == "bundle-proposed-not-added"
    _observe(h, "k1")
    r = h.send(P, ev.BundleProposedRemoved(), actor=OTHER_USER_ID)
    assert not r.audit.accepted and h.p().bundle_proposed_label


def test_remove_effect_is_superseded_when_the_label_is_wanted_again() -> None:
    h = Harness()
    _triaged(h)
    _observe(h, "k1")
    [remove] = Harness.of(_observe(h), EffectKind.SET_BUNDLE_PROPOSED)
    _observe(h, "k2")
    assert h.p().bundle_proposed_label
    assert effect_still_valid(h.p(), remove) == "bundle-proposed-superseded"


# ------------------------------------------------------------------ webhook

IDENTITY = DeliveryIdentity(
    app_id=900,
    installation_id=901,
    organization_id=296340858,
    project_node_id="PVT_x",
    status_field_node_id="PVTSSF_x",
    repository_id=1278047766,
    repository_node_id="R_x",
    repository_full_name="example/repo",
    owner_ids=frozenset({OWNER_ID}),
    bot_user_id=777,
    automation_user_ids=frozenset({888}),
)


def _unlabel(name: str, sender: dict[str, object]) -> tuple[str, ...]:
    data = {
        "action": "unlabeled",
        "installation": {"id": 901, "app_id": 900},
        "organization": {"id": 296340858},
        "repository": {"id": 1278047766, "node_id": "R_x", "full_name": "example/repo"},
        "sender": sender,
        "issue": {"id": 10, "node_id": "I_1", "number": 12},
        "label": {"name": name},
    }
    normalized = DeliveryNormalizer(IDENTITY).normalize(
        raw_body=json.dumps(data).encode(),
        event_name="issues",
        delivery_guid="delivery-1",
        delivery_time_us=2_000_000_000_000_000,
        provenance=Provenance.WEBHOOK,
    )
    return tuple(type(e.body).__name__ for e in normalized.events)


def test_only_an_owners_unlabel_is_a_removal_event() -> None:
    owner = {"id": OWNER_ID, "type": "User"}
    assert _unlabel(BUNDLE_PROPOSED_LABEL, owner) == ("BundleProposedRemoved",)
    assert _unlabel(BUNDLE_PROPOSED_LABEL, {"id": 777, "type": "Bot"}) == ()  # the factory
    assert _unlabel(BUNDLE_PROPOSED_LABEL, {"id": 222, "type": "User"}) == ()
    assert _unlabel("factory:close-candidate", owner) == ("CloseCandidateRemoved",)
