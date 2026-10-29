from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.preconditions import effect_still_valid
from omnigent_factory.core.types import Lifecycle, Parcel, Via, issue_session_title
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"


def _active_parcel() -> Parcel:
    h = Harness()
    h.eligible()
    h.send(P, ev.RequestTriage(via=Via.DRAG))
    h.create_ok()
    return h.p()


def _effect(
    parcel: Parcel,
    kind: EffectKind,
    args: dict[str, str | int | bool],
) -> EffectIntent:
    session = parcel.current_session
    assert session is not None
    return EffectIntent(
        effect_id=f"ef-{kind.value}",
        kind=kind,
        parcel_id=parcel.parcel_id,
        target=session.session_id,
        preconditions=Preconditions(
            parcel_version=parcel.version,
            eligibility_epoch=parcel.eligibility_epoch,
            session_id=session.session_id,
            authorization_id=session.authorization_id,
            grant_id=session.grant.grant_id,
        ),
        args=args,
    )


def test_stale_feedback_eyes_do_not_return_after_thumbs_up() -> None:
    parcel = replace(_active_parcel(), unread_feedback=("github:comment:42:created",))
    eyes = _effect(parcel, EffectKind.REACT_COMMENT, {"comment_id": 42, "content": "eyes"})
    assert effect_still_valid(parcel, eyes) is None
    assert effect_still_valid(replace(parcel, unread_feedback=()), eyes) == "reaction-superseded"


def test_old_interrupt_cannot_stop_a_new_run_on_the_shared_root() -> None:
    parcel = _active_parcel()
    old = parcel.current_session
    assert old is not None and old.root_id is not None
    interrupt = _effect(parcel, EffectKind.INTERRUPT_TREE, {"root_id": old.root_id})
    assert effect_still_valid(parcel, interrupt) is None
    newer = replace(old, session_id="ss-new", lifecycle=Lifecycle.ACTIVE)
    advanced = replace(
        parcel,
        sessions=(*parcel.sessions, newer),
        current_session_id=newer.session_id,
    )
    assert effect_still_valid(advanced, interrupt) == "interrupt-superseded"


def test_cost_policy_retry_is_scoped_to_the_current_grant() -> None:
    parcel = _active_parcel()
    session = parcel.current_session
    assert session is not None
    policy = _effect(
        parcel,
        EffectKind.REPLACE_COST_POLICY,
        {
            "grant_id": session.grant.grant_id,
            "generation": session.grant.policy_generation,
            "granted_us": session.grant.duration_us,
        },
    )
    assert effect_still_valid(parcel, policy) is None
    newer_grant = replace(
        session.grant,
        grant_id="gr-new",
        policy_generation=session.grant.policy_generation + 1,
    )
    advanced = replace(
        parcel,
        sessions=tuple(
            replace(candidate, grant=newer_grant)
            if candidate.session_id == session.session_id
            else candidate
            for candidate in parcel.sessions
        ),
    )
    assert effect_still_valid(advanced, policy) == "cost-policy-superseded"


def test_rename_retry_cannot_restore_an_older_issue_title() -> None:
    parcel = _active_parcel()
    issue = parcel.issue_session
    assert issue is not None
    rename = _effect(
        parcel,
        EffectKind.RENAME_SESSION,
        {
            "root_id": issue.root_id,
            "title": issue_session_title(parcel.issue_number, parcel.issue_title),
        },
    )
    assert effect_still_valid(parcel, rename) is None
    advanced = replace(
        parcel,
        issue_title="New title",
        issue_session=replace(issue, title="New title"),
    )
    assert effect_still_valid(advanced, rename) == "rename-superseded"


def test_policy_verification_retry_is_scoped_to_the_pending_current_grant() -> None:
    parcel = _active_parcel()
    session = parcel.current_session
    assert session is not None and session.root_id is not None
    pending = replace(session, policy_ready=False)
    parcel = replace(
        parcel,
        sessions=tuple(
            pending if candidate.session_id == session.session_id else candidate
            for candidate in parcel.sessions
        ),
    )
    verify = _effect(
        parcel,
        EffectKind.VERIFY_POLICIES,
        {"root_id": pending.root_id, "not_before_us": 0, "reconcile": False},
    )
    assert effect_still_valid(parcel, verify) is None
    settled = replace(
        parcel,
        sessions=tuple(
            replace(candidate, policy_ready=True)
            if candidate.session_id == pending.session_id
            else candidate
            for candidate in parcel.sessions
        ),
    )
    assert effect_still_valid(settled, verify) == "policy-verification-superseded"
