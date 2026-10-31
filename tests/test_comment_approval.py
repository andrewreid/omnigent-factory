"""An owner's plain comment that clearly approves the posted plan acts as ``/approve``.

Covers the conservative text rules (``core.approval_text``), the webhook marking, and
the reducer applying it exactly like ``/approve`` (checks, refusal notes, reactions) only
in Scoped on a published plan; anything else stays plan feedback.
"""

from __future__ import annotations

import json

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.approval_text import ApprovalText, classify_approval
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.events import Provenance
from omnigent_factory.core.reducer import TransitionResult, effective_event
from omnigent_factory.core.types import ApprovalKind, Stage, Via
from omnigent_factory.github.webhook import DeliveryIdentity, DeliveryNormalizer
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"

# ------------------------------------------------------------------ text rules


@pytest.mark.parametrize(
    "text",
    [
        "approved",
        "Approved.",
        "APPROVED!",
        "approve",
        "Approve 👍",
        "LGTM",
        "lgtm 🚀",
        "Looks good",
        "looks good to me!",
        "Ship it",
        "Go ahead.",
        "Plan approved",
        "plan is approved",
        "I approve",
        "  approved \n",
        "\n\napproved\n\n",
        "Approved, thanks!",
        "Plan approved per conversation in-session.",
        "Approved: as discussed in our chat",
        "go ahead with the plan now",
        "Approved - thank you",
        "LGTM \U0001f44d\U0001f3fd",
        "approved \u2705\U0001f680!",
        "Approved \u2714\ufe0f",
        "Looks good to me, cheers",
    ],
)
def test_allowlisted_comments_are_approvals(text: str) -> None:
    assert classify_approval(text) == ApprovalText()


@pytest.mark.parametrize(
    ("text", "handle"),
    [
        ("Approved 3f2a9c1b7d4e", "3f2a9c1b7d4e"),
        ("3f2a9c1b7d4e approved", "3f2a9c1b7d4e"),
        ("plan 3F2A9C1B7D4E5F60 approved.", "3f2a9c1b7d4e5f60"),
        ("Approve hash 3f2a9c1b7d4e, thanks", "3f2a9c1b7d4e"),
        ("LGTM 3f2a9c1b7d4e", "3f2a9c1b7d4e"),
        ("approve plan 3f2a9c1b7d4e", "3f2a9c1b7d4e"),
    ],
)
def test_a_named_hash_with_an_approval_word_is_a_hash_approval(text: str, handle: str) -> None:
    assert classify_approval(text) == ApprovalText(handle)


@pytest.mark.parametrize(
    "text",
    [
        # change requests, conditions and refusals
        "Approved, but drop step 3",
        "approved except the migration",
        "Looks good, however rename the flag",
        "LGTM, please add a test",
        "approve once CI is green",
        "approved if the API stays stable",
        "Ship it after the release",
        "go ahead instead with option B",
        "not approved",
        "Approved — no wait",
        "looks good but change the name",
        # questions
        "approved?",
        "Looks good? I wonder about the cache",
        "LGTM 3f2a9c1b7d4e?",
        # longer prose / not an approval phrase at the start
        "Approved per our conversation earlier today in the planning session with the team",
        "I think this plan mostly works",
        "Thanks for the plan",
        "Please keep the API stable",
        "The plan approved yesterday is wrong",
        "",
        # hash without an approval word, or two hashes
        "3f2a9c1b7d4e",
        "approved 3f2a9c1b7d4e or 0123456789ab",
        "approved 3f2a9c1b7d4e 0123456789ab",
        "3f2a9c1b7d4e approved pending CI",
        "hash approved",
    ],
)
def test_ambiguous_or_change_requesting_comments_stay_feedback(text: str) -> None:
    assert classify_approval(text) is None


@pytest.mark.parametrize(
    "text",
    [
        # trailing words outside the positive allowlist: conditions and refusals
        "Approved pending CI",
        "Approved, don't merge",
        "approved dont merge",
        "Approved, merge later",
        "approved for now, after CI",
        "LGTM modulo nits",
        "approved, ok",
        "approved 2",
        # code, quotes, Markdown and HTML fail closed before any normalisation
        "```\napproved\n```",
        "```approved```",
        "`approved`",
        "Approved `rm -rf`",
        "> approved",
        "> Plan approved per conversation in-session.",
        "**Approved**",
        "_approved_",
        "~~approved~~",
        '"approved"',
        "<b>approved</b>",
        "<!-- approved -->",
        "# approved",
        "- approved",
        "* approved",
        # smart apostrophes/quotes and other Unicode punctuation are rejected, not mapped
        "Approved, don\u2019t merge",
        "Approved \u2018as is\u2019",
        "\u201capproved\u201d",
        "Approved \u2014 no wait",
        "approved\u2026",
        "approved\uff01",
        # multi-line: approve, then change
        "Approved\nbut please drop step 3",
        "approved\n\nAlso rename the flag",
        "LGTM\r\nwait, one thing",
        "Plan approved per conversation in-session.\nChange the cache TTL.",
        # Unicode lookalikes (Cyrillic a/e/o, fullwidth, mathematical bold)
        "\u0430pproved",
        "approv\u0435d",
        "l\u0433tm",
        "\uff41pproved",
        "\U0001d41a\U0001d429\U0001d429\U0001d42b\U0001d428\U0001d42f\U0001d41e\U0001d41d",
        # zero-width and other invisible characters
        "appro\u200bved",
        "approved\u200b",
        "\ufeffapproved",
        "approved\u2060 merge",
        "app\u00adroved",
        "approved\tthanks",
        # trailing links
        "Approved https://example.com/plan",
        "approved [plan](https://example.com)",
        "approved <https://example.com>",
        "approved see example.com",
        "approved www.example.com",
        # emoji outside the allowlist or not at the end
        "approved \U0001f44e",
        "\U0001f44d approved",
        "approved \U0001f44d merge",
        "approved \U0001f914",
    ],
)
def test_adversarial_comments_fail_closed(text: str) -> None:
    assert classify_approval(text) is None


# ------------------------------------------------------------------ webhook marking

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


def _comment(body: str, sender: int = OWNER_ID, *, pr: bool = False) -> tuple[ev.Event, ...]:
    issue: dict[str, object] = {"id": 10, "node_id": "I_1", "number": 12}
    if pr:
        issue["pull_request"] = {"url": "x"}
    data = {
        "action": "created",
        "installation": {"id": 901, "app_id": 900},
        "organization": {"id": 296340858},
        "repository": {"id": 1278047766, "node_id": "R_x", "full_name": "example/repo"},
        "sender": {"id": sender},
        "issue": issue,
        "comment": {"id": 44, "body": body, "created_at": "2026-09-25T01:02:03Z"},
    }
    return (
        DeliveryNormalizer(IDENTITY)
        .normalize(
            raw_body=json.dumps(data).encode(),
            event_name="issue_comment",
            delivery_guid="delivery-1",
            delivery_time_us=2_000_000_000_000_000,
            provenance=Provenance.WEBHOOK,
        )
        .events
    )


def test_webhook_marks_an_approving_owner_comment() -> None:
    (event,) = _comment("Plan approved per conversation in-session.")
    assert isinstance(event.body, ev.PlanFeedback)
    assert event.body.approves and event.body.approve_hash is None
    (hashed,) = _comment("Approved 3f2a9c1b7d4e")
    assert isinstance(hashed.body, ev.PlanFeedback) and hashed.body.approve_hash == "3f2a9c1b7d4e"


def test_webhook_leaves_feedback_and_non_owner_comments_as_before() -> None:
    (event,) = _comment("Approved, but please rename the flag")
    assert isinstance(event.body, ev.PlanFeedback) and not event.body.approves
    assert _comment("approved", sender=222) == ()
    # A PR conversation comment is never an approval.
    (pr,) = _comment("approved", pr=True)
    assert isinstance(pr.body, ev.PlanFeedback) and not pr.body.approves


# ------------------------------------------------------------------ reducer

_COMMENT_IDS = iter(range(9000, 10000))


def _say(
    h: Harness, *, approves: bool = True, approve_hash: str | None = None, actor: int = OWNER_ID
) -> TransitionResult:
    cid = next(_COMMENT_IDS)
    return h.send(
        P,
        ev.PlanFeedback(text_digest=f"d{cid}", approves=approves, approve_hash=approve_hash),
        event_id=f"github:comment:{cid}:created",
        actor=actor,
    )


def _reactions(r: TransitionResult) -> list[str]:
    return [str(e.args["content"]) for e in Harness.of(r, EffectKind.REACT_COMMENT)]


def _latest_hash(h: Harness) -> str:
    contract = h.p().current_contract
    assert contract is not None and contract.published
    return contract.full_hash


def test_approving_comment_on_a_published_plan_approves_like_the_command() -> None:
    h = Harness()
    h.plan_published()
    revision = h.p().revision
    r = _say(h)
    assert r.audit.accepted
    approval = h.p().current_approval
    assert approval is not None and approval.kind == ApprovalKind.PLAN
    contract = h.p().current_contract
    assert contract is not None and approval.full_hash == contract.full_hash
    assert h.p().revision == revision and not h.p().revision_pending  # not feedback
    assert _reactions(r) == ["+1"]
    assert Harness.of(r, EffectKind.MOVE_CARD)  # to Building, as /approve
    assert r.audit.kind == ev.EventKind.APPROVE_PLAN.value


def test_1457_wording_approves() -> None:
    (event,) = _comment("Plan approved per conversation in-session.")
    h = Harness()
    h.plan_published()
    body = event.body
    assert isinstance(body, ev.PlanFeedback)
    r = _say(h, approves=body.approves, approve_hash=body.approve_hash)
    assert r.audit.accepted
    assert h.p().current_approval is not None


def test_hash_approval_must_name_the_latest_plan() -> None:
    h = Harness()
    h.plan_published()
    digest = _latest_hash(h)
    r = _say(h, approve_hash=digest[:12])
    assert r.audit.accepted and _reactions(r) == ["+1"]
    assert h.p().current_approval is not None


def test_hash_naming_no_plan_stays_feedback() -> None:
    h = Harness()
    h.plan_published()
    revision = h.p().revision
    r = _say(h, approve_hash="0123456789ab")  # a commit SHA, say
    assert r.audit.accepted
    assert h.p().current_approval is None
    assert h.p().revision == revision + 1 and h.p().revision_pending
    assert _reactions(r) == ["eyes"]


def test_stale_plan_hash_is_refused_like_the_command() -> None:
    h = Harness()
    h.plan_published(goal="First plan")
    old = _latest_hash(h)
    _say(h, approves=False)  # feedback: revise
    h.publish_plan(goal="Second plan")
    assert _latest_hash(h) != old
    r = _say(h, approve_hash=old[:12])
    assert not r.audit.accepted
    assert r.audit.reason == "hash-does-not-identify-latest"
    assert _reactions(r) == ["confused"]
    assert h.p().current_approval is None


def test_revision_pending_refuses_with_the_specific_note_not_feedback() -> None:
    h = Harness()
    h.plan_published()
    _say(h, approves=False)  # plain feedback: the plan is being revised
    revision = h.p().revision
    feedback = tuple(h.p().revision_feedback)
    assert h.p().revision_pending
    r = _say(h)
    assert not r.audit.accepted
    assert r.audit.reason == "plan-not-approvable"
    assert h.p().note == "Cannot approve: the plan is being revised after your answer/feedback"
    assert _reactions(r) == ["confused"]
    # Not silently taken as another revision either.
    assert h.p().revision == revision and tuple(h.p().revision_feedback) == feedback


def test_after_approval_the_card_left_planning_so_comments_are_feedback() -> None:
    h = Harness()
    h.plan_published()
    h.approve()
    # The card moved to Building: elsewhere than Scoped the comment is plain feedback.
    assert h.p().stage == Stage.BUILDING
    event = h.f(P).make(ev.PlanFeedback(text_digest="x", approves=True))
    assert effective_event(h.p(), event) is event


def test_no_published_plan_or_other_column_stays_feedback() -> None:
    h = Harness()
    h.eligible()
    event = h.f(P).make(ev.PlanFeedback(text_digest="x", approves=True))
    assert effective_event(h.p(), event) is event  # Inbox, no plan
    h.send(P, ev.RequestPlan(via=Via.DRAG))
    assert h.p().stage == Stage.SCOPED and h.p().current_contract is None
    assert effective_event(h.p(), event) is event  # plan still running


def test_non_owner_comment_is_ignored_as_today() -> None:
    h = Harness()
    h.plan_published()
    r = _say(h, actor=OTHER_USER_ID)
    assert not r.audit.accepted
    assert h.p().current_approval is None
    assert _reactions(r) == []


def test_plain_feedback_is_unchanged() -> None:
    h = Harness()
    h.plan_published()
    revision = h.p().revision
    r = _say(h, approves=False)
    assert r.audit.accepted and _reactions(r) == ["eyes"]
    assert h.p().revision == revision + 1 and h.p().current_approval is None
