"""Follow-ups: issues a run files join the source issue's epic with the dependencies the
agent names (idempotent, add-only), the bot-opened backstop and its exclusions, and epic
autopilot's hold with the owner's per-issue and per-epic overrides."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core.effects import RetryableReadFailure
from omnigent_factory.core.events import EventKind
from omnigent_factory.core.protocol import Correlation, ResultError, validate_result
from omnigent_factory.core.types import IssueLinks, LinkedIssue
from omnigent_factory.github.client import GitHubAPIError, GitHubRejected, GraphQLErrors
from omnigent_factory.ports.github import IssueRef, LabelEvent
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.follow_ups import (
    BUILD_FOLLOW_UPS_LABEL,
    FOLLOW_UP_LABEL,
    FollowUps,
    referenced_issues,
)
from omnigent_factory.service.links import NativeLinks
from omnigent_factory.store.sqlite import FollowUpRow, SqliteStore
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, REPO_ID
from tests.service.test_epic_autopilot import (
    BOT,
    EPIC,
    S1,
    S2,
    FakeGates,
    fresh,
    history,
    on,
    plan_dict,
    rig,
)
from tests.test_epic_autopilot_reducer import claimed

pytestmark = pytest.mark.asyncio

BOT_LOGIN = "molly-omnigent-factory[bot]"


# ------------------------------------------------------------------ reporting (schema)


def _build(**over: Any) -> dict[str, Any]:
    return {
        "kind": "build_ready",
        "pr_number": 7,
        "branch": "factory/issue-747",
        "head_sha": "a" * 40,
        "summary": "done",
        "verification": [
            {"command": "pytest", "file_set": ["tests"], "outcome": "passed", "evidence": "ok"}
        ],
        "review": {
            "implementation_vendor": "anthropic",
            "review_vendor": "openai",
            "reviewed_head": "a" * 40,
            "artifact_reference": "r",
            "artifact_sha256": "b" * 64,
            "accepted": True,
        },
        "findings": [],
        "remediation_batches_used": 0,
        "targeted_rechecks_used": 0,
        "release_readiness": "ready",
        **over,
    }


CORR = Correlation(
    parcel_id="I_747",
    stage_session_id="ss_1",
    dispatch_nonce="n",
    revision=1,
    stage="build",
    issue_number=747,
)


async def test_build_and_blocked_results_carry_follow_ups_and_bad_lists_are_refused():
    parsed = validate_result(_build(follow_ups=[{"issue": 904, "blocks": [882, 884]}]), CORR)
    dumped = parsed.result.result.model_dump(mode="json")
    assert dumped["follow_ups"] == [{"issue": 904, "blocks": [882, 884]}]
    blocked = {"kind": "blocked", "reason": "x", "done": [], "follow_ups": [{"issue": 905}]}
    assert validate_result(blocked, CORR).result.result.model_dump()["follow_ups"] == [
        {"issue": 905, "blocks": []}
    ]
    for bad, message in (
        ([{"issue": 904}, {"issue": 904}], "each issue at most once"),
        ([{"issue": 747}], "not this issue itself"),
        ([{"issue": 904, "blocks": [904]}], "cannot block itself"),
        ([{"issue": 904, "blocks": [882, 882]}], "at most once"),
    ):
        with pytest.raises(ResultError, match=message):
            validate_result(_build(follow_ups=bad), CORR)


# ------------------------------------------------------------------ GitHub fake


@dataclass
class FakeGitHubClient:
    """Issues by number (parent, labels, blockers) behind GraphQL and the label REST
    calls; records every write."""

    issues: dict[int, dict[str, Any]]
    repo_labels: set[str] = field(default_factory=set)
    writes: list[tuple[str, Any]] = field(default_factory=list)
    fail: set[str] = field(default_factory=set)
    #: Mutation name -> the error GitHub answers it with (counted in ``refused``).
    refuse: dict[str, GitHubAPIError] = field(default_factory=dict)
    refused: int = 0

    def _by_id(self, node: object) -> dict[str, Any]:
        return next(i for i in self.issues.values() if i["id"] == node)

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if query.lstrip().startswith("mutation"):
            name = "addSubIssue" if "addSubIssue" in query else "addBlockedBy"
            if name in self.fail:
                raise GitHubAPIError(f"{name} failed")
            if name in self.refuse:
                self.refused += 1
                raise self.refuse[name]
            if name == "addSubIssue":
                parent, sub = self._by_id(variables["parent"]), self._by_id(variables["sub"])
                sub["parent"] = parent["number"]
                self.writes.append((name, (parent["number"], sub["number"])))
            else:
                blocked, blocking = (
                    self._by_id(variables["issue"]),
                    self._by_id(variables["blocking"]),
                )
                blocked["blocked_by"].append(blocking["number"])
                self.writes.append((name, (blocked["number"], blocking["number"])))
            return {}
        found = self.issues.get(int(variables["number"]))
        if found is None:
            return {"repository": {"issue": None}}
        parent = found.get("parent")
        return {
            "repository": {
                "issue": {
                    "id": found["id"],
                    "number": found["number"],
                    "repository": {"id": REPO_ID},
                    "issueType": None,
                    "parent": None
                    if parent is None
                    else {"number": parent, "repository": {"id": REPO_ID}},
                    "labels": {"nodes": [{"name": n} for n in found["labels"]]},
                    "blockedBy": {
                        "totalCount": len(found["blocked_by"]),
                        "nodes": [
                            {"number": n, "repository": {"id": REPO_ID}}
                            for n in found["blocked_by"]
                        ],
                    },
                }
            }
        }

    async def request(
        self, method: str, path: str, *, json_body: Any = None, expected: Any = None
    ) -> Any:
        if method == "GET" and "/labels/" in path:
            if path.rsplit("/", 1)[1] not in self.repo_labels:
                raise GitHubRejected(404, method, path)
            return SimpleNamespace(status_code=200)
        if "label" in self.fail:
            raise GitHubAPIError("label failed")
        if path.endswith("/labels") and "/issues/" not in path:
            self.repo_labels.add(json_body["name"])
            self.writes.append(("createLabel", json_body["name"]))
        else:
            number = int(path.split("/issues/")[1].split("/", maxsplit=1)[0])
            self.issues[number]["labels"].extend(json_body["labels"])
            self.writes.append(("addLabel", (number, tuple(json_body["labels"]))))
        return SimpleNamespace(status_code=200)


def gh_issue(number: int, *, parent: int | None = None, labels: tuple[str, ...] = ()):
    return {
        "id": f"I_{number}",
        "number": number,
        "parent": parent,
        "labels": list(labels),
        "blocked_by": [],
    }


def tracker(service: Any, client: FakeGitHubClient, events: Any = None) -> FollowUps:
    async def no_cards() -> list[Any]:
        return []

    native = NativeLinks(service, client, lambda _sid: None, no_cards)  # type: ignore[arg-type]

    async def read_events(ref: IssueRef) -> Any:
        if events is None:
            return ()
        return events(ref) if callable(events) else events

    return FollowUps(service, native, read_events)


async def row(service: Any, number: int) -> FollowUpRow | None:
    repo_id = service.config.repo_id
    found: FollowUpRow | None = await service.db.call(
        lambda store: store.follow_up(repo_id, number)
    )
    return found


def result_with(*items: dict[str, Any]) -> dict[str, Any]:
    return {"kind": "build_ready", "follow_ups": list(items)}


# ------------------------------------------------------------------ linking (2)


async def test_a_reported_follow_up_gets_its_label_epic_and_blocked_by_links_once(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    caplog.set_level("INFO")
    async with rig(on(service_config), history()) as r:
        client = FakeGitHubClient(
            {
                878: gh_issue(878),
                747: gh_issue(747, parent=878),
                882: gh_issue(882, parent=878),
                884: gh_issue(884, parent=878),
                904: gh_issue(904),
            }
        )
        follow = tracker(r.service, client)
        follow.schedule_result(747, result_with({"issue": 904, "blocks": [882, 884]}))
        await follow.drain()
        assert client.writes == [
            ("addBlockedBy", (882, 904)),
            ("addBlockedBy", (884, 904)),
            ("createLabel", FOLLOW_UP_LABEL),
            ("addLabel", (904, (FOLLOW_UP_LABEL,))),
            ("addSubIssue", (878, 904)),
        ]
        stored = await row(r.service, 904)
        assert stored is not None and stored.source_issue == 747 and stored.pending() == ()
        assert stored.hold == "held" and stored.origin == "reported"
        for text in ("follow-up labelled issue=#904", "follow-up added to epic issue=#904"):
            assert text in caplog.text
        # Retry and a second report: nothing done twice.
        assert await follow.retry_pending() == 0
        await follow.record_reported(747, result_with({"issue": 904, "blocks": [882]}))
        assert len(client.writes) == 5


async def test_linking_survives_a_failure_and_restart_without_redoing_or_undoing_the_owner(
    service_config: ServiceConfig,
):
    async with rig(on(service_config), history()) as r:
        client = FakeGitHubClient(
            {878: gh_issue(878), 747: gh_issue(747, parent=878), 904: gh_issue(904)},
            repo_labels={FOLLOW_UP_LABEL},
            fail={"addSubIssue"},
        )
        follow = tracker(r.service, client)
        await follow.record_reported(747, result_with({"issue": 904}))
        assert client.writes == [("addLabel", (904, (FOLLOW_UP_LABEL,)))]
        pending = await row(r.service, 904)
        assert pending is not None and pending.pending() == ("parent",)
        # The owner takes the label off before the retry: the restarted factory adds the
        # parent it still owes, and never puts the label back.
        client.issues[904]["labels"].clear()
        await follow.on_issues_delivery(label_payload("unlabeled", 904, FOLLOW_UP_LABEL, OWNER_ID))
        client.fail.clear()
        restarted = tracker(r.service, client)
        assert await restarted.retry_pending() == 1
        assert client.writes[1:] == [("addSubIssue", (878, 904))]
        assert client.issues[904]["labels"] == []
        # An owner-set parent elsewhere is kept: a later follow-up of it is not moved.
        client.issues[905] = gh_issue(905, parent=999)
        await restarted.record_reported(747, result_with({"issue": 905}))
        assert ("addSubIssue", (878, 905)) not in client.writes


#: GitHub's answer to ``addSubIssue`` on an epic already at its 100 sub-issues.
EPIC_FULL = GraphQLErrors((("UNPROCESSABLE", "Parent cannot have more than 100 sub-issues"),))


async def test_a_parent_github_refuses_for_good_is_given_up_once_and_stays_given_up(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture
):
    # timesheets #1113/#1115/#1118/#1123: epic #878 was full; the factory retried each
    # parent forever, logging only "GitHub GraphQL request returned errors".
    caplog.set_level("INFO")
    async with rig(on(service_config), history()) as r:
        client = FakeGitHubClient(
            {878: gh_issue(878), 1020: gh_issue(1020, parent=878), 1113: gh_issue(1113)},
            repo_labels={FOLLOW_UP_LABEL},
            refuse={"addSubIssue": EPIC_FULL},
        )
        follow = tracker(r.service, client)
        await follow.record_reported(1020, result_with({"issue": 1113}))
        assert client.refused == 1
        stored = await row(r.service, 1113)
        assert stored is not None and stored.pending() == ()
        assert {"parent", "gave-up:parent"} <= stored.done
        given_up = [m for m in caplog.messages if "follow-up parent given up" in m]
        assert given_up == [
            "follow-up parent given up issue=#1113 epic=#878 source=#1020 reason=GitHub "
            "GraphQL request returned errors: UNPROCESSABLE: Parent cannot have more than "
            "100 sub-issues"
        ]
        # Retries and a restart never ask again.
        assert await follow.retry_pending() == 0
        assert await tracker(r.service, client).retry_pending() == 0
        assert client.refused == 1
        assert not [m for m in caplog.messages if "follow-up parent failed" in m]


async def test_a_transient_parent_error_is_retried_with_a_bounded_backoff(
    service_config: ServiceConfig, caplog: pytest.LogCaptureFixture, monkeypatch: Any
):
    caplog.set_level("INFO")
    now = [1000.0]
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "time", lambda: now[0])
    async with rig(on(service_config), history()) as r:
        client = FakeGitHubClient(
            {878: gh_issue(878), 747: gh_issue(747, parent=878), 904: gh_issue(904)},
            repo_labels={FOLLOW_UP_LABEL},
            refuse={
                "addSubIssue": GraphQLErrors(
                    ((None, "Something went wrong while executing your query."),)
                )
            },
        )
        follow = tracker(r.service, client)
        await follow.record_reported(747, result_with({"issue": 904}))
        assert client.refused == 1
        assert "Something went wrong" in caplog.text  # the message, not just "errors"
        await follow.retry_pending()  # inside the first minute: not asked again
        assert client.refused == 1
        delays = []
        for _ in range(9):
            due = follow._backoff[904][0]
            delays.append(due - now[0])
            now[0] = due
            await follow.retry_pending()
        assert delays == [60, 120, 240, 480, 960, 1920, 3600, 3600, 3600]
        assert client.refused == 10
        stored = await row(r.service, 904)
        assert stored is not None and stored.pending() == ("parent",)
        client.refuse.clear()
        now[0] = follow._backoff[904][0]
        assert await follow.retry_pending() == 1
        assert 904 not in follow._backoff
        assert ("addSubIssue", (878, 904)) in client.writes


async def test_a_follow_up_of_an_issue_outside_any_epic_is_labelled_only(
    service_config: ServiceConfig,
):
    async with rig(on(service_config), history()) as r:
        client = FakeGitHubClient({747: gh_issue(747), 904: gh_issue(904)})
        follow = tracker(r.service, client)
        await follow.record_reported(747, result_with({"issue": 904}))
        assert [name for name, _ in client.writes] == ["createLabel", "addLabel"]
        stored = await row(r.service, 904)
        assert stored is not None and stored.pending() == ()


# ------------------------------------------------------------------ backstop (3)


def opened_payload(
    number: int, body: str, *, sender: str = BOT_LOGIN, sender_id: int = BOT
) -> bytes:
    return json.dumps(
        {
            "action": "opened",
            "issue": {"number": number, "node_id": f"I_{number}", "body": body},
            "sender": {"login": sender, "id": sender_id, "type": "Bot"},
        }
    ).encode()


def label_payload(action: str, number: int, label: str, actor: int, at: str = "") -> bytes:
    return json.dumps(
        {
            "action": action,
            "label": {"name": label},
            "issue": {
                "number": number,
                "node_id": f"I_{number}",
                "updated_at": at or "2026-10-11T00:00:00Z",
            },
            "sender": {"id": actor, "type": "Bot" if actor == BOT else "User"},
        }
    ).encode()


async def test_body_references():
    repo = "SA-Ambulance/timesheets"
    body = (
        "Found while building #2 (see https://github.com/SA-Ambulance/timesheets/issues/3); "
        "fix before #882. Not other/repo#5, not &#39; nor a#6"
    )
    assert referenced_issues(body, repo) == {2, 3, 882}


async def test_an_issue_the_bot_opens_during_a_run_is_a_follow_up_of_that_run(
    service_config: ServiceConfig,
):
    h = history()
    claimed(h)  # #2 (S1) has an open run
    async with rig(on(service_config), h) as r:
        client = FakeGitHubClient(
            {1: gh_issue(1), 2: gh_issue(2, parent=1), 30: gh_issue(30)},
            repo_labels={FOLLOW_UP_LABEL},
        )
        follow = tracker(r.service, client)
        await follow.on_issues_delivery(
            opened_payload(30, "Out of scope for #2; fix before #3 builds.")
        )
        await follow.drain()
        stored = await row(r.service, 30)
        assert stored is not None and stored.origin == "detected" and stored.source_issue == 2
        assert stored.blocks == ()  # never inferred from the body
        assert client.writes == [
            ("addLabel", (30, (FOLLOW_UP_LABEL,))),
            ("addSubIssue", (1, 30)),
        ]
        # Reported later: blocks join, nothing done twice.
        client.issues[3] = gh_issue(3, parent=1)
        await follow.record_reported(2, result_with({"issue": 30, "blocks": [3]}))
        assert client.writes[2:] == [("addBlockedBy", (3, 30))]
        assert await follow.retry_pending() == 0
        assert (await row(r.service, 30)).origin == "reported"  # type: ignore[union-attr]


async def test_the_backstop_excludes_gates_other_authors_idle_and_ambiguous_issues(
    service_config: ServiceConfig, tmp_path: Path
):
    gate = {"key": "tenant", "title": "Set up the tenant", "steps": "do it", "blocks": [3]}
    h = history()
    claimed(h)  # #2 has an open run; #3 has none
    async with rig(on(service_config), h, plan=plan_dict(gates=[gate]), gates=FakeGates()) as r:
        await r.autopilot.run_once()
        epic = await r.parcel(EPIC)
        assert [g.number for g in epic.autopilot_gates] == [40]
        client = FakeGitHubClient({})
        follow = tracker(r.service, client)
        # Epic autopilot's own human gate: its marker, or (marker-less) its known number.
        marked = "Do it before #2.\n\n<!-- factory-gate epic=1 key=tenant -->"
        assert await follow.detect(json.loads(opened_payload(41, marked))) is None
        assert await follow.detect(json.loads(opened_payload(40, "Blocks #2."))) is None
        # Someone else's issue, an issue naming only idle issues, or none.
        other = opened_payload(42, "about #2", sender="someone", sender_id=OTHER_USER_ID)
        assert await follow.detect(json.loads(other)) is None
        assert await follow.detect(json.loads(opened_payload(43, "before #3"))) is None
        assert await follow.detect(json.loads(opened_payload(44, "no reference"))) is None
        assert await follow.detect(json.loads(opened_payload(45, "during #2"))) == 2
        await follow.drain()
        for n in (40, 41, 42, 43, 44):
            assert await row(r.service, n) is None
    h2 = history()
    claimed(h2)
    claimed(h2, S2)  # #2 and #3 both have open runs
    async with rig(on(fresh(service_config, tmp_path, "two")), h2) as r:
        follow = tracker(r.service, FakeGitHubClient({}))
        assert await follow.detect(json.loads(opened_payload(46, "#2 and #3"))) is None


# ------------------------------------------------------------------ autopilot policy (4)


def blocking_links(*numbers: int) -> IssueLinks:
    return IssueLinks(
        parent=LinkedIssue(1, True), blocking=tuple(LinkedIssue(n, True) for n in numbers)
    )


async def test_autopilot_holds_a_follow_up_that_blocks_nothing_in_the_epic(
    service_config: ServiceConfig, tmp_path: Path
):
    async with rig(on(service_config), history()) as r:
        r.labels[S1] = [FOLLOW_UP_LABEL]  # #2 comes first in the plan
        assert await r.autopilot.run_once() == "#1: started #3"
        assert (await r.parcel(S1)).autopilot_claim is None
        note = (await r.parcel(EPIC)).epic_note
        assert note == "Epic · 0/2 done · autopilot: planning #3, 1 follow-up held"
    # A recorded follow-up is held from the moment it is reported, label or not.
    async with rig(on(fresh(service_config, tmp_path, "rec")), history()) as r:
        await record(r.service, 2, source=3)
        assert await r.autopilot.run_once() == "#1: started #3"


async def test_autopilot_builds_a_follow_up_that_blocks_another_open_sub_issue(
    service_config: ServiceConfig, tmp_path: Path
):
    h = history()
    async with rig(on(service_config), h, sub_reads={S1: blocking_links(3)}) as r:
        await _reread(r, S1, blocking_links(3))
        r.labels[S1] = [FOLLOW_UP_LABEL]
        assert await r.autopilot.run_once() == "#1: started #2"
    # Blocking only an issue outside the epic (or a closed one) does not count.
    async with rig(on(fresh(service_config, tmp_path, "out")), history()) as r:
        await _reread(r, S1, blocking_links(500))
        r.labels[S1] = [FOLLOW_UP_LABEL]
        assert await r.autopilot.run_once() == "#1: started #3"


async def test_the_owner_removing_the_label_releases_one_follow_up_and_a_bot_does_not(
    service_config: ServiceConfig, tmp_path: Path
):
    async with rig(on(service_config), history()) as r:
        await record(r.service, 2, source=3)
        follow = tracker(r.service, FakeGitHubClient({}))
        await follow.on_issues_delivery(label_payload("unlabeled", 2, FOLLOW_UP_LABEL, BOT))
        await follow.on_issues_delivery(
            label_payload("unlabeled", 2, FOLLOW_UP_LABEL, OTHER_USER_ID)
        )
        assert await r.autopilot.run_once() == "#1: started #3"  # still held
    async with rig(on(fresh(service_config, tmp_path, "owner")), history()) as r:
        await record(r.service, 2, source=3)
        r.labels[S1] = []
        follow = tracker(r.service, FakeGitHubClient({}))
        await follow.on_issues_delivery(label_payload("unlabeled", 2, FOLLOW_UP_LABEL, OWNER_ID))
        assert await r.autopilot.run_once() == "#1: started #2"


async def test_a_lost_owner_label_removal_is_recovered_from_the_timeline(
    service_config: ServiceConfig,
):
    async with rig(on(service_config), history()) as r:
        await record(r.service, 2, source=3)
        bot_removal = (
            LabelEvent("LE_1", FOLLOW_UP_LABEL, True, BOT, 10),
            LabelEvent("LE_2", FOLLOW_UP_LABEL, False, BOT, 20),
        )
        follow = tracker(r.service, FakeGitHubClient({}), events=bot_removal)
        assert await follow.check_card(S1, ()) is True
        assert (await row(r.service, 2)).hold == "held"  # type: ignore[union-attr]
        owner_removal = (*bot_removal, LabelEvent("LE_3", FOLLOW_UP_LABEL, False, OWNER_ID, 30))
        follow = tracker(r.service, FakeGitHubClient({}), events=owner_removal)
        assert await follow.check_card(S1, ()) is True
        assert (await row(r.service, 2)).hold == "released"  # type: ignore[union-attr]
        assert await r.autopilot.run_once() == "#1: started #2"
        # A failed read keeps the card for the next diff.
        failing = tracker(r.service, FakeGitHubClient({}), events=RetryableReadFailure("x"))
        assert await failing.check_card(S1, (FOLLOW_UP_LABEL,)) is False


async def test_factory_build_follow_ups_on_the_epic_releases_all_and_removal_restores(
    service_config: ServiceConfig, tmp_path: Path
):
    async with rig(on(service_config), history()) as r:
        await record(r.service, 2, source=3)
        follow = tracker(r.service, FakeGitHubClient({}))
        # Only the owner's label counts.
        await follow.on_issues_delivery(label_payload("labeled", 1, BUILD_FOLLOW_UPS_LABEL, BOT))
        await follow.on_issues_delivery(
            label_payload("labeled", 1, BUILD_FOLLOW_UPS_LABEL, OWNER_ID, "2026-10-11T00:01:00Z")
        )
        assert await r.autopilot.run_once() == "#1: started #2"
    async with rig(on(fresh(service_config, tmp_path, "off")), history()) as r:
        await record(r.service, 2, source=3)
        follow = tracker(r.service, FakeGitHubClient({}))
        for action, at in (("labeled", "00:01"), ("unlabeled", "00:02")):
            await follow.on_issues_delivery(
                label_payload(action, 1, BUILD_FOLLOW_UPS_LABEL, OWNER_ID, f"2026-10-11T{at}:00Z")
            )
        # A bot removing it again changes nothing either way.
        await follow.on_issues_delivery(label_payload("unlabeled", 1, BUILD_FOLLOW_UPS_LABEL, BOT))
        assert await r.autopilot.run_once() == "#1: started #3"
    # A lost webhook: the epic card shows the label, the owner's timeline event decides.
    async with rig(on(fresh(service_config, tmp_path, "lost")), history()) as r:
        await record(r.service, 2, source=3)
        events = (LabelEvent("LE_9", BUILD_FOLLOW_UPS_LABEL, True, OWNER_ID, 50),)
        follow = tracker(r.service, FakeGitHubClient({}), events=events)
        assert await follow.check_card(EPIC, (BUILD_FOLLOW_UPS_LABEL,)) is True
        assert await r.autopilot.run_once() == "#1: started #2"


async def test_held_follow_ups_are_counted_in_the_epic_note(service_config: ServiceConfig):
    async with rig(on(service_config), history()) as r:
        await record(r.service, 2, source=3)
        await record(r.service, 3, source=2)
        assert await r.autopilot.run_once() == "#1: nothing to start"
        assert await r.count(EventKind.AUTOPILOT_PLAN) == 0
        note = (await r.parcel(EPIC)).epic_note
        assert note == "Epic · 0/2 done · autopilot: 2 follow-ups held"


# ------------------------------------------------------------------ instructions (1)


async def test_build_instructions_no_longer_ask_the_run_to_open_issues():
    """The factory files follow-ups itself now (``service.deferred``): current templates
    ask for ``deferred``; older ones stay for runs pinned to them."""
    from omnigent_factory.service import directory as d

    root = Path(d.__file__).parent / "templates"
    current = (
        d._FIRST_TEMPLATES[d.SessionKind.BUILD],
        d._REWORK_TEMPLATE,
        d._READINESS_WAKE_TEMPLATE,
        d._FINDINGS_WAKE_TEMPLATE,
    )
    for name in current:
        text = " ".join((root / name).read_text("utf-8").split())
        assert "deferred" in text and "Never open issues" in text, name
        assert "Every issue you open goes in follow_ups" not in text, name
    for old in ("build-v11.txt", "build-rework-v8.txt"):  # pinned in-flight runs keep theirs
        assert "Every issue you open goes in follow_ups" in (root / old).read_text("utf-8")


# ------------------------------------------------------------------ helpers


async def record(service: Any, number: int, *, source: int) -> None:
    await service.db.call(
        lambda store: SqliteStore.record_follow_up(
            store,
            service.config.repo_id,
            number,
            source_issue=source,
            origin="reported",
        )
    )


async def _reread(r: Any, pid: str, links: IssueLinks) -> None:
    """The sub-issue's stored read shows ``links`` (as a per-issue read would)."""
    from omnigent_factory.core import events as ev
    from omnigent_factory.core.events import Event, Provenance
    from omnigent_factory.testing.builders import snapshot

    now = r.clock.now_utc_us()
    parcel = await r.parcel(pid)
    evidence = snapshot(read_at_us=now, links=links)
    r.github.snapshots[pid] = evidence
    result = await r.service.apply_event(
        Event(
            event_id=f"test-read:{pid}:{now}",
            repo_id=r.config.repo_id,
            parcel_id=pid,
            source_time_us=now,
            provenance=Provenance.ADAPTER,
            body=ev.GitHubSnapshot(),
            issue_number=parcel.issue_number,
            evidence=evidence,
        )
    )
    assert result.accepted, result.reason


# ------------------------------------------------------------------ wiring


async def test_an_issues_webhook_reaches_follow_ups_and_a_board_diff_checks_card_labels(
    service_config: ServiceConfig,
):
    from dataclasses import replace as dc_replace

    from omnigent_factory.github.webhook import DeliveryNormalizer
    from omnigent_factory.service.board_diff import DiffResult
    from omnigent_factory.service.github_delivery import GitHubDeliveryProcessor
    from omnigent_factory.store.sqlite import DeliveryRecord
    from tests.service.test_webhook_mapping import IDENTITY, _common

    async with rig(on(service_config), history()) as r:
        seen: list[Any] = []

        class Follow:
            async def on_issues_delivery(self, body: bytes) -> None:
                seen.append(json.loads(body)["action"])

            async def check_card(self, parcel_id: str, labels: tuple[str, ...]) -> bool:
                seen.append((parcel_id, labels))
                return False  # a failed read: the card's digest is not stored

            async def retry_pending(self) -> int:
                seen.append("retry")
                return 0

        processor = GitHubDeliveryProcessor(
            r.service,
            DeliveryNormalizer(dc_replace(IDENTITY, repository_node_id=service_config.repo_id)),
            r.github,  # type: ignore[arg-type]
            r.clock,
        )
        processor.follow_ups = Follow()
        payload = _common(OTHER_USER_ID)
        payload["repository"] = {
            "id": IDENTITY.repository_id,
            "node_id": service_config.repo_id,
            "full_name": IDENTITY.repository_full_name,
        }
        payload["issue"] = {"number": 2, "node_id": S1, "body": "x"}
        body = json.dumps({**payload, "action": "opened"}).encode()
        record = DeliveryRecord("d-open", "issues", body, {})
        await r.service.db.call(lambda store: store.append_delivery(record))
        await processor.process(record)
        assert seen == ["opened"]

        class Diff:
            async def run_once(self) -> DiffResult:
                return DiffResult(changed={S1: "dg"}, labels={S1: (FOLLOW_UP_LABEL,)})

        r.service.board_diff = Diff()
        r.service.follow_ups = Follow()
        await r.service._board_diff_tick(0.0, frozenset())
        assert seen[1:] == [(S1, (FOLLOW_UP_LABEL,)), "retry"]
        digests = await r.service.db.call(lambda store: store.board_digests())
        assert S1 not in digests
