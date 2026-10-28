"""Issue bundling in the service: the bundle rules, proposals (once per member set, owner
ticks only, lost ticks recovered from the edit history), ``/bundle``, the synthetic bundle
issue (exactly once, never the members' parent), member holds, ejection, merge (members
done even without their close webhook), cancel and ``/unbundle``, the switch, and the
members left out of auto-build, ranking, auto-triage and autopilot."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from functools import partial
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core import report_lines as rl
from omnigent_factory.core.events import Event, Provenance
from omnigent_factory.core.types import (
    AutopilotClaim,
    BotState,
    Decision,
    DecisionImpact,
    DecisionStatus,
    Hold,
    IssueLinks,
    IssueSnapshot,
    LinkedIssue,
    Parcel,
    Size,
    Stage,
)
from omnigent_factory.ports.github import IssueRef
from omnigent_factory.service.auto_build import mark_blocker
from omnigent_factory.service.auto_triage import known_to_factory
from omnigent_factory.service.bundles import (
    BUNDLE_LABEL,
    Bundles,
    MemberFacts,
    bundle_marker,
    parse_proposals,
    proposal_marker,
    validate_bundle,
)
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store import bundles as bs
from omnigent_factory.testing.builders import OTHER_USER_ID, OWNER_ID, snapshot
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from omnigent_factory.testing.harness import Harness
from tests.service.test_deferred import BOT, REPO, FakeRepo, iso
from tests.service.test_epic_autopilot import seed
from tests.service.test_follow_ups import gh_issue

#: Harness parcels: issue numbers 1, 2, 3 in creation order.
P1, P2, P3 = "I_m1", "I_m2", "I_m3"
NUMBERS = {P1: 1, P2: 2, P3: 3}


def triage(size: str = "S", priority: str = "P2", **over: Any) -> dict[str, Any]:
    return {"kind": "triage", "size": size, "priority": priority, "related": [], **over}


@dataclass
class FakeDirectory:
    """Stored triage results and issue reads per issue number."""

    triage: dict[int, dict[str, Any] | None] = field(default_factory=dict)
    evidence: dict[int, IssueSnapshot] = field(default_factory=dict)

    def latest_triage(self, parcel: Parcel) -> dict[str, Any] | None:
        return self.triage.get(parcel.issue_number or 0, triage())

    async def issue_evidence(self, parcel: Parcel) -> IssueSnapshot | None:
        n = parcel.issue_number or 0
        return self.evidence.get(n) or snapshot(title=f"Issue {n}", body=f"Body of #{n}")


@dataclass
class BundleRepo(FakeRepo):
    """Adds comment posts and reactions, PRs, and node ids on issue responses."""

    next_comment: int = 900
    reactions: list[tuple[int, str]] = field(default_factory=list)
    prs: dict[int, dict[str, Any]] = field(default_factory=dict)
    #: The next comment post lands on GitHub but its response is lost.
    lose_comment: bool = False

    def _rest(self, issue: dict[str, Any]) -> dict[str, Any]:
        return {**super()._rest(issue), "node_id": issue["id"]}

    async def get_json(self, path: str) -> Any:
        if path.startswith(f"{REPO}/pulls/"):
            return dict(self.prs[int(path.rsplit("/", 1)[1])])
        return await super().get_json(path)

    async def request(
        self, method: str, path: str, *, json_body: Any = None, expected: Any = None
    ) -> Any:
        if method == "POST" and path.endswith("/reactions"):
            self.reactions.append((int(path.split("/")[-2]), json_body["content"]))
            return SimpleNamespace(status_code=201)
        if method == "POST" and path.endswith("/comments"):
            issue = int(path.split("/issues/")[1].split("/", maxsplit=1)[0])
            cid = self.next_comment
            self.next_comment += 1
            self.comments[cid] = {
                "id": cid,
                "issue": issue,
                "node_id": f"IC_{cid}",
                "body": json_body["body"],
                "user": {"id": BOT},
            }
            self.writes.append(("comment", (issue, cid)))
            if self.lose_comment:
                self.lose_comment = False
                from omnigent_factory.github.client import AmbiguousRequest

                raise AmbiguousRequest("GitHub request failed: ReadTimeout")
            return SimpleNamespace(
                status_code=201, json=lambda: {"id": cid, "node_id": f"IC_{cid}"}
            )
        response = await super().request(method, path, json_body=json_body, expected=expected)
        if method == "POST" and path == f"{REPO}/issues":
            number = self.next_number - 1
            self.issues[number]["id"] = f"I_{number}"
            return SimpleNamespace(
                status_code=201, json=lambda: {"number": number, "node_id": f"I_{number}"}
            )
        return response

    def comments_on(self, issue: int) -> list[dict[str, Any]]:
        return [c for c in self.comments.values() if c["issue"] == issue]


@dataclass
class Rig:
    service: FactoryService
    repo: BundleRepo
    directory: FakeDirectory
    bundles: Bundles
    clock: FakeClock
    h: Harness
    reads: dict[int, IssueSnapshot] = field(default_factory=dict)

    async def parcel(self, number: int) -> Parcel:
        rows = await self.service.db.call(
            lambda store: store.query(
                "SELECT parcel_id FROM parcels WHERE issue_number = ?", (number,)
            )
        )
        found = await self.service.db.call(lambda store: store.load_parcel(str(rows[0][0])))
        assert found is not None
        return found

    async def state(self) -> list[bs.Bundle]:
        return await self.service.db.call(
            partial(bs.bundles_of, repo_id=self.service.config.repo_id)
        )

    async def proposals(self) -> list[bs.Proposal]:
        return await self.service.db.call(
            partial(bs.proposals_of, repo_id=self.service.config.repo_id)
        )

    def new(self) -> Bundles:
        """The same database and GitHub after a restart (nothing in memory carried)."""
        return make(self.service, self.repo, self.directory, self.reads)

    async def apply(self, number: int, body: ev.EventBody, **kw: Any) -> Any:
        p = await self.parcel(number)
        self.clock.advance(1_000_000)
        return await self.service.apply_event(
            Event(
                event_id=f"t:{number}:{self.clock.now_utc_us()}",
                repo_id=self.service.config.repo_id,
                parcel_id=p.parcel_id,
                source_time_us=self.clock.now_utc_us(),
                provenance=kw.pop("provenance", Provenance.ADAPTER),
                body=body,
                issue_number=number,
                **kw,
            )
        )


def make(
    service: FactoryService,
    repo: BundleRepo,
    directory: FakeDirectory,
    reads: dict[int, IssueSnapshot],
) -> Bundles:
    async def read(ref: IssueRef) -> IssueSnapshot:
        base = reads.get(ref.issue_number) or snapshot(title=f"Issue {ref.issue_number}")
        return replace(base, read_at_us=service.clock.now_utc_us())

    return Bundles(service, repo, directory, str, snapshot=read)


def history(sizes: dict[str, Size] | None = None, *, plan: tuple[str, ...] = ()) -> Harness:
    h = Harness()
    for pid in (P1, P2, P3):
        h.triage(pid, size=(sizes or {}).get(pid, Size.S))
    for pid in plan:
        h.plan_published(pid)
    return h


def on(config: ServiceConfig, **over: Any) -> ServiceConfig:
    return config.model_copy(update={"bundling": True, **over})


@asynccontextmanager
async def rig(
    config: ServiceConfig, h: Harness | None = None, *, parent: int | None = 40
) -> AsyncIterator[Rig]:
    h = h or history()
    seed(config, h)
    clock = FakeClock()
    clock.advance(10**9)  # after the seeded history
    service = FactoryService(
        config, adapters=(FakeGitHub(), FakeOmnigent(), FakeCredentialBroker()), clock=clock
    )
    await service.start()
    try:
        await service.operator_command("unpause", {})
        repo = BundleRepo(
            {
                1: gh_issue(1, parent=parent),
                2: gh_issue(2, parent=parent),
                3: gh_issue(3, parent=parent),
                40: gh_issue(40),
            }
        )
        directory = FakeDirectory()
        reads: dict[int, IssueSnapshot] = {}
        bundles = make(service, repo, directory, reads)
        service.bundles = bundles
        yield Rig(service, repo, directory, bundles, clock, h, reads)
    finally:
        await service.stop()


def proposal(*members: int, theme: str = "Export tidy-up") -> dict[str, Any]:
    return {
        "members": list(members),
        "theme": theme,
        "reason": "Both change api/src/export.ts and share its tests.",
        "shared_area": "api/src/export.ts",
    }


async def propose(r: Rig, *members: int) -> bs.Proposal:
    found, problems = await r.bundles.check_proposals([proposal(*members)], {})
    assert problems == []
    await r.bundles.record_proposals("rk_1", found)
    await r.bundles.post_pending()
    [p] = [p for p in await r.proposals() if set(p.members) == set(members)]
    return p


def edited(r: Rig, comment_id: int, after: str, *, sender: int = OWNER_ID) -> bytes:
    c = r.repo.comments[comment_id]
    before = c["body"]
    c["body"] = after
    return json.dumps(
        {
            "action": "edited",
            "issue": {"number": c["issue"]},
            "comment": {
                "id": comment_id,
                "body": after,
                "user": {"id": BOT},
                "updated_at": iso(r.clock.now_utc_us()),
            },
            "changes": {"body": {"from": before}},
            "sender": {"id": sender},
        }
    ).encode()


def ticked(body: str) -> str:
    return body.replace("- [ ] Build these together", "- [x] Build these together", 1)


def command(issue: int, text: str, comment_id: int = 777, sender: int = OWNER_ID) -> bytes:
    return json.dumps(
        {
            "action": "created",
            "issue": {"number": issue},
            "comment": {"id": comment_id, "body": text, "user": {"id": sender}},
            "sender": {"id": sender},
        }
    ).encode()


async def confirmed(r: Rig, *members: int) -> bs.Bundle:
    p = await propose(r, *members)
    assert p.comment_id is not None
    body = r.repo.comments[p.comment_id]["body"]
    assert await r.bundles.on_comment_delivery(edited(r, p.comment_id, ticked(body))) == 1
    [b] = [b for b in await r.state() if b.bundle_key == p.proposal_key]
    return b


# ------------------------------------------------------------------ the rules


def facts(h: Harness, pid: str, **over: Any) -> MemberFacts:
    values: dict[str, Any] = {
        "number": NUMBERS[pid],
        "parcel": h.p(pid),
        "labels": frozenset(),
        "triage": triage(),
        "priority": "P2",
    }
    values.update(over)
    return MemberFacts(**values)


def check(h: Harness, members: tuple[int, ...] = (1, 2), **per: dict[str, Any]) -> list[str]:
    by_pid = {P1: per.get("a", {}), P2: per.get("b", {}), P3: per.get("c", {})}
    found = {NUMBERS[pid]: facts(h, pid, **over) for pid, over in by_pid.items()}
    return validate_bundle(members, found, max_members=3, max_points=4)


def test_a_valid_bundle_breaks_no_rule():
    h = history()
    assert check(h) == [] and check(h, (1, 2, 3)) == []


@pytest.mark.parametrize(
    ("members", "per", "expected"),
    [
        ((1,), {}, "a bundle has 2 to 3 members (1 given)"),
        ((1, 2, 3, 4), {}, "a bundle has 2 to 3 members (4 given)"),
        ((1, 1), {}, "each member at most once"),
        ((1, 2), {"a": {"triage": triage("M")}, "b": {"triage": triage("M")}}, "6 size points"),
        ((1, 2), {"a": {"triage": triage("L")}}, "#1 is size L or unsized"),
        ((1, 2), {"a": {"triage": None}}, "#1 is not in Triage or Planning with a triage"),
        ((1, 2), {"a": {"priority": "P0"}}, "#1 is P0"),
        ((1, 2), {"a": {"labels": frozenset({"factory:skip"})}}, "#1 is labelled factory:skip"),
        ((1, 2), {"b": {"labels": frozenset({"needs-review"})}}, "#2 is labelled needs-review"),
        ((1, 2), {"a": {"labels": None}}, "#1: its labels could not be read"),
        (
            (1, 2),
            {"a": {"parent": 40, "parent_autopilot": True}},
            "epic #40, which is on autopilot",
        ),
        ((1, 2), {"a": {"parent": 40}, "b": {"parent": 41}}, "members of different epics"),
        ((1, 2), {"a": {"bundled": 9}}, "#1 is already in bundle #9"),
        ((1, 2), {"a": {"queued": True}}, "#1: work on it is running or queued"),
        ((1, 2), {"a": {"open_dependencies": (77,)}}, "#1 depends on open #77"),
    ],
)
def test_each_rule_is_checked(members, per, expected):
    problems = check(history(), members, **per)
    assert any(expected in p for p in problems), problems


def test_parcel_state_rules():
    h = history()
    p = h.p(P1)
    unsized = replace(p, size=None)
    assert "#1 is size L or unsized: only S and M issues are bundled" in check(
        h, a={"parcel": unsized, "triage": triage(size=None)}
    )
    epic = replace(p, links=IssueLinks(sub_total=2))
    assert "#1 is an epic" in check(h, a={"parcel": epic})
    inbox = replace(p, stage=Stage.INBOX)
    assert any("not in Triage or Planning" in x for x in check(h, a={"parcel": inbox}))
    building = replace(p, stage=Stage.BUILDING)
    assert "#1 is already in Building" in check(h, a={"parcel": building})
    assigned = replace(p, eligible=False)
    assert any("assigned to a person" in x for x in check(h, a={"parcel": assigned}))
    blocked = replace(p, links=IssueLinks(blocked_by=(LinkedIssue(50, True),)))
    assert "#1 is blocked by open #50" in check(h, a={"parcel": blocked})
    inside = replace(p, links=IssueLinks(blocked_by=(LinkedIssue(2, True),)))
    assert check(h, a={"parcel": inside}) == []  # a blocker inside the bundle is fine
    asked = replace(
        p,
        decisions=(Decision("d1", "s", "e", 1, DecisionImpact.UNKNOWN, DecisionStatus.OPEN),),
    )
    assert any("needs you" in x for x in check(h, a={"parcel": asked}))
    needs_you = replace(p, bot=BotState.NEEDS_YOU)
    assert any("needs you" in x for x in check(h, a={"parcel": needs_you}))


def test_the_member_ceiling_is_twelve_whatever_the_config():
    h = history()
    found = {n: facts(h, P1, number=n) for n in range(1, 14)}
    problems = validate_bundle(tuple(found), found, max_members=20, max_points=99)
    assert "a bundle has 2 to 12 members (13 given)" in problems
    explicit = validate_bundle(tuple(found), found, max_members=20, max_points=99, explicit=True)
    assert "a bundle has 2 to 12 members (13 given)" in explicit


def test_an_owner_bundle_ignores_the_size_rules_only():
    h = history()
    sized = {"L": triage("L"), "M": triage("M"), "unsized": triage(size=None)}
    found = {
        n: facts(
            h,
            P1,
            number=n,
            triage=sized[["L", "M", "unsized"][n % 3]],
            parcel=replace(h.p(P1), size=None),
        )
        for n in range(1, 9)
    }
    members = tuple(found)
    # A suggestion: too many members, L and unsized members, too many points.
    suggested = validate_bundle(members, found, max_members=3, max_points=4)
    assert "a bundle has 2 to 3 members (8 given)" in suggested
    assert any("size L or unsized" in p for p in suggested)
    assert any("size points" in p for p in suggested)
    # The owner's /bundle: up to its own maximum, any size.
    assert validate_bundle(members, found, max_members=8, max_points=4, explicit=True) == []
    over = validate_bundle(members, found, max_members=7, max_points=4, explicit=True)
    assert over == ["a bundle has 2 to 7 members (8 given)"]
    assert validate_bundle((1,), found, max_members=8, max_points=4, explicit=True) == [
        "a bundle has 2 to 8 members (1 given)"
    ]
    # Every protective rule still refuses it.
    p = h.p(P1)
    for over_facts, expected in (
        (
            {"parcel": replace(p, autopilot_claim=AutopilotClaim(40, "ep", 1, 1))},
            "#1: epic autopilot drives it",
        ),
        (
            {"parcel": replace(p, links=IssueLinks(blocked_by=(LinkedIssue(50, True),)))},
            "#1 is blocked by open #50",
        ),
        ({"triage": None}, "#1 is not in Triage or Planning with a triage result"),
        ({"open_dependencies": (77,)}, "#1 depends on open #77"),
        ({"priority": "P0"}, "#1 is P0"),
        ({"labels": frozenset({"factory:skip"})}, "#1 is labelled factory:skip"),
        ({"bundled": 9}, "#1 is already in bundle #9"),
        ({"parcel": replace(p, eligible=False)}, "#1 is closed, assigned"),
        ({"parcel": replace(p, links=IssueLinks(sub_total=2))}, "#1 is an epic"),
    ):
        refused = {**found, 1: replace(found[1], **over_facts)}
        problems = validate_bundle(members, refused, max_members=8, max_points=4, explicit=True)
        assert any(expected in x for x in problems), (expected, problems)


def test_an_owner_bundle_may_take_sub_issues_of_an_autopilot_epic_a_proposal_never():
    h = history()
    piloted = {"parent": 40, "parent_autopilot": True}
    found = {NUMBERS[pid]: facts(h, pid, **piloted) for pid in (P1, P2)}
    assert validate_bundle((1, 2), found, max_members=3, max_points=4, explicit=True) == []
    suggested = validate_bundle((1, 2), found, max_members=3, max_points=4)
    assert "#1 is a sub-issue of epic #40, which is on autopilot" in suggested
    # One autopilot drives right now is refused either way (the reducer would not hold it).
    driven = replace(h.p(P1), autopilot_claim=AutopilotClaim(40, "ep", 1, 1))
    found[1] = replace(found[1], parcel=driven)
    for explicit in (True, False):
        problems = validate_bundle((1, 2), found, max_members=3, max_points=4, explicit=explicit)
        assert "#1: epic autopilot drives it" in problems
    # A claim the owner took over (released) no longer drives it.
    released = replace(driven, autopilot_claim=AutopilotClaim(40, "ep", 1, 1, released="stop"))
    found[1] = replace(found[1], parcel=released)
    assert validate_bundle((1, 2), found, max_members=3, max_points=4, explicit=True) == []


def test_a_member_the_reducer_would_not_hold_is_refused():
    h = history()
    stuck = replace(h.p(P1), holds=frozenset({Hold.RESULT_INVALID}))
    assert "#1: it needs you" in check(h, a={"parcel": stuck})
    # Already refused by another rule: that rule's reason only.
    assert "#1: it needs you" not in check(h, a={"parcel": stuck, "priority": "P0"})


def test_proposal_shape_errors():
    _ok, problems = parse_proposals([{"members": [1, "2"], "theme": "t", "reason": "r"}])
    assert problems == ["bundles[1]: members must be issue numbers"]
    _ok, problems = parse_proposals([{"members": [1, 2], "theme": "", "reason": ""}])
    assert any("theme is required" in p for p in problems)
    assert any("reason is required" in p and "area of code" in p for p in problems)
    _ok, problems = parse_proposals([proposal(1, 2), proposal(2, 3)])
    assert problems == ["#2 is in bundles[1] and bundles[2]"]
    assert parse_proposals("x")[1] == ["bundles: a list of at most 5"]


# ------------------------------------------------------------------ proposals


@pytest.mark.asyncio
async def test_a_proposal_gets_one_comment_on_its_lowest_member(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        p = await propose(r, 2, 1)
        assert p.anchor_issue == 1 and p.state == "posted" and p.members == (1, 2)
        [comment] = r.repo.comments_on(1)
        assert proposal_marker(p.proposal_key) in comment["body"]
        assert rl.checked(comment["body"], rl.BUNDLE) == {p.action_id: False}
        assert "- [ ] Build these together as one PR" in comment["body"]
        # The same member set proposed again (another ranking): no second comment.
        await propose(r, 1, 2)
        await r.bundles.post_pending()
        assert len(r.repo.comments_on(1)) == 1 and len(await r.proposals()) == 1


@pytest.mark.asyncio
async def test_a_lost_post_is_found_by_its_marker(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        found, _ = await r.bundles.check_proposals([proposal(1, 2)], {})
        await r.bundles.record_proposals("rk_1", found)
        r.repo.lose_comment = True
        await r.bundles.post_pending()
        assert [p.state for p in await r.proposals()] == ["posting"]
        await r.new().post_pending()
        [p] = await r.proposals()
        assert p.state == "posted" and len(r.repo.comments_on(1)) == 1


@pytest.mark.asyncio
async def test_ranking_proposals_are_validated(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        r.directory.triage[1] = triage("L")
        _found, problems = await r.bundles.check_proposals([proposal(1, 2)], {2: "P0"})
        assert any("#1 is size L" in p for p in problems)
        assert any("#2 is P0" in p for p in problems)
        assert all(p.startswith("bundles[1] (#1 and #2): ") for p in problems)


@pytest.mark.asyncio
async def test_a_member_turning_ineligible_makes_the_proposal_stale(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        r.repo.issues[2]["labels"].append("factory:skip")
        await r.bundles.catch_up()
        [p] = await r.proposals()
        assert p.state == "stale" and "labelled factory:skip" in p.detail
        assert p.comment_id is not None
        body = r.repo.comments[p.comment_id]["body"]
        assert rl.checked(body, rl.BUNDLE) == {}  # no box left to tick
        assert "→ stale: #2 is labelled factory:skip" in body
        # A tick (of the old text) does nothing now.
        assert await r.bundles.on_comment_delivery(edited(r, p.comment_id, ticked(body))) == 0
        assert await r.state() == []


# ------------------------------------------------------------------ confirming


@pytest.mark.asyncio
async def test_only_an_owner_tick_confirms(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        assert p.comment_id is not None
        body = r.repo.comments[p.comment_id]["body"]
        delivery = edited(r, p.comment_id, ticked(body), sender=OTHER_USER_ID)
        assert await r.bundles.on_comment_delivery(delivery) == 0
        assert await r.state() == []
        # The bot's own edit (sender = the bot) is no tick either.
        delivery = edited(r, p.comment_id, ticked(body), sender=BOT)
        assert await r.bundles.on_comment_delivery(delivery) == 0
        assert await r.state() == []


@pytest.mark.asyncio
async def test_a_tick_creates_one_bundle_issue_holds_members_and_plans(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        b = await confirmed(r, 1, 2)
        assert b.state == "open" and b.issue_number == 100 and b.via == "webhook"
        issue = r.repo.issues[100]
        assert issue["title"] == "Bundle: Export tidy-up (#1, #2)"
        assert bundle_marker(b.bundle_key) in issue["body"]
        assert "`Closes #1`, `Closes #2`" in issue["body"]
        assert BUNDLE_LABEL in issue["labels"]
        # Never the members' parent: the bundle joins their shared epic instead.
        subs = [w for w in r.repo.writes if w[0] == "addSubIssue"]
        assert subs == [("addSubIssue", (40, 100))]
        for n in (1, 2):
            m = await r.parcel(n)
            assert Hold.BUNDLED in m.holds and m.bundled_into == 100
            assert m.note == "In bundle #100"
        bundle = await r.parcel(100)
        assert bundle.stage == Stage.SCOPED and bundle.revision_pending
        # The proposal says what came of it.
        [p] = await r.proposals()
        assert p.state == "created" and p.detail == "#100"
        assert p.comment_id is not None
        assert "→ bundled into #100" in r.repo.comments[p.comment_id]["body"]


@pytest.mark.asyncio
async def test_bundle_issue_creation_is_idempotent_across_a_restart(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        assert p.comment_id is not None
        body = r.repo.comments[p.comment_id]["body"]
        r.repo.lose_create = True  # the create lands, its response is lost
        await r.bundles.on_comment_delivery(edited(r, p.comment_id, ticked(body)))
        [b] = await r.state()
        assert b.state == "creating" and r.repo.creates == 1
        restarted = r.new()
        await restarted.sync()
        await restarted.sync()
        [b] = await r.state()
        assert b.state == "open" and b.issue_number == 100 and r.repo.creates == 1
        # The webhook delivered again changes nothing.
        again = edited(r, p.comment_id, ticked(body))
        assert await restarted.on_comment_delivery(again) == 0
        assert r.repo.creates == 1 and len(await r.state()) == 1


@pytest.mark.asyncio
async def test_a_lost_tick_is_recovered_only_from_an_owner_edit(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        assert p.comment_id is not None
        body = ticked(r.repo.comments[p.comment_id]["body"])
        r.repo.comments[p.comment_id]["body"] = body  # ticked; the webhook never came
        r.repo.edits[f"IC_{p.comment_id}"] = [
            {"editedAt": iso(r.clock.now_utc_us()), "diff": body, "editor": {"databaseId": 1}}
        ]
        await r.bundles.catch_up()
        assert await r.state() == []  # edited by someone else: ignored
        r.repo.edits[f"IC_{p.comment_id}"] = [
            {
                "editedAt": iso(r.clock.now_utc_us()),
                "diff": body,
                "editor": {"databaseId": OWNER_ID},
            }
        ]
        await r.bundles.catch_up()
        [b] = await r.state()
        assert b.via == "recovered" and b.state == "open"


@pytest.mark.asyncio
async def test_bundle_command_creates_a_bundle(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        assert await r.bundles.on_comment_delivery(command(2, "/bundle #3 Tidy exports")) == 1
        [b] = await r.state()
        assert b.source == "command" and [m.issue_number for m in b.members] == [2, 3]
        assert r.repo.issues[100]["title"] == "Bundle: Tidy exports (#2, #3)"
        assert (777, "+1") in r.repo.reactions
        for n in (2, 3):
            assert Hold.BUNDLED in (await r.parcel(n)).holds
        # A redelivery of the same command: nothing new.
        assert await r.bundles.on_comment_delivery(command(2, "/bundle #3 Tidy exports")) == 0
        assert r.repo.creates == 1


@pytest.mark.asyncio
async def test_bundle_command_is_refused_with_its_reasons(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        r.directory.triage[3] = None
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #3")) == 0
        assert await r.state() == [] and (777, "confused") in r.repo.reactions
        [reply] = r.repo.comments_on(1)
        assert "#3 is not in Triage or Planning with a triage result" in reply["body"]
        # Not an owner: nothing at all.
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #2", 778, 4242)) == 0
        assert await r.state() == []


@pytest.mark.asyncio
async def test_a_member_cannot_be_in_two_bundles(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        assert await r.bundles.on_comment_delivery(command(3, "/bundle #2", 778)) == 0
        [reply] = r.repo.comments_on(3)
        assert "#2 is already in bundle #100" in reply["body"]


def five_issues() -> Harness:
    h = Harness()
    for pid in (P1, P2, P3, "I_m4", "I_m5"):
        h.triage(pid, size=Size.M)
    return h


@pytest.mark.asyncio
async def test_bundle_command_takes_any_size_up_to_its_own_maximum(service_config: ServiceConfig):
    async with rig(on(service_config), five_issues()) as r:
        for n in (4, 5):
            r.repo.issues[n] = gh_issue(n, parent=40)
        r.directory.triage.update({n: triage("M") for n in (1, 2, 4, 5)})
        r.directory.triage[3] = triage("L")
        # Five members (above bundle_max_members 3), an L, 12 size points (above 4).
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #2 #3 #4 #5 Tidy")) == 1
        [b] = await r.state()
        assert b.source == "command"
        assert [m.issue_number for m in b.members] == [1, 2, 3, 4, 5]
        for n in (1, 2, 3, 4, 5):
            assert Hold.BUNDLED in (await r.parcel(n)).holds
        view = await r.bundles.members_view(await r.parcel(100), "B")
        assert [m["number"] for m in view] == [1, 2, 3, 4, 5]
        assert view[2]["triage"]["size"] == "L"


@pytest.mark.asyncio
async def test_bundle_command_is_refused_above_its_maximum(service_config: ServiceConfig):
    config = on(service_config, bundle_command_max_members=4)
    async with rig(config, five_issues()) as r:
        for n in (4, 5):
            r.repo.issues[n] = gh_issue(n, parent=40)
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #2 #3 #4 #5")) == 0
        assert await r.state() == []
        [reply] = r.repo.comments_on(1)
        assert "a bundle has 2 to 4 members (5 given)" in reply["body"]
        assert "size" not in reply["body"]


@pytest.mark.asyncio
async def test_proposals_keep_the_suggestion_limits(service_config: ServiceConfig):
    config = on(service_config, bundle_max_members=2, bundle_command_max_members=12)
    async with rig(config) as r:
        _found, problems = await r.bundles.check_proposals([proposal(1, 2, 3)], {})
        assert any("a bundle has 2 to 2 members (3 given)" in p for p in problems), problems
        r.directory.triage.update({1: triage("M"), 2: triage("M")})
        _found, problems = await r.bundles.check_proposals([proposal(1, 2)], {})
        assert any("6 size points (S = 1, M = 3): at most 4" in p for p in problems), problems
        r.directory.triage[2] = triage("L")
        _found, problems = await r.bundles.check_proposals([proposal(1, 2)], {})
        assert any("#2 is size L or unsized" in p for p in problems), problems
        text, _field = await r.bundles.ranking_text()
        assert "bundles of 2 to 2 related small issues" in text and "at most 4 points" in text


@pytest.mark.asyncio
async def test_a_ticked_proposal_keeps_the_suggestion_rules(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        assert p.comment_id is not None
        r.directory.triage[2] = triage("L")  # it grew before the owner ticked
        body = r.repo.comments[p.comment_id]["body"]
        assert await r.bundles.on_comment_delivery(edited(r, p.comment_id, ticked(body))) == 1
        assert await r.state() == []
        [refused] = await r.proposals()
        assert refused.state == "refused" and "#2 is size L or unsized" in refused.detail


def depends_on(*issues: int) -> dict[str, Any]:
    return triage(related=[{"issue": n, "relation": "depends_on", "note": "x"} for n in issues])


@pytest.mark.asyncio
async def test_a_depends_on_naming_the_members_own_epic_is_no_dependency(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        # #1's triage listed its parent epic #40 ("Parent tracking epic"): never an open
        # dependency, on the proposal path and the /bundle path alike.
        r.directory.triage[1] = depends_on(40)
        _found, problems = await r.bundles.check_proposals([proposal(1, 2)], {})
        assert problems == []
        r.directory.triage[1] = depends_on(40, 77)  # a real open dependency still refuses
        _found, problems = await r.bundles.check_proposals([proposal(1, 2)], {})
        assert problems == ["bundles[1] (#1 and #2): #1 depends on open #77"]
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #2")) == 0
        [reply] = r.repo.comments_on(1)
        assert "#1 depends on open #77" in reply["body"] and "#40" not in reply["body"]
        r.directory.triage[1] = depends_on(40)
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #2", 778)) == 1


def autopilot_epic(*, drive: str | None = None) -> Harness:
    """Epic #1 on autopilot with sub-issues #2 (S1) and #3 (S2) triaged; ``drive``: the
    sub-issue autopilot claimed (its plan posted, awaiting approval)."""
    from tests.test_epic_autopilot_reducer import S1, S2, claimed, on_autopilot

    h = Harness()
    on_autopilot(h)
    driven = {"#2": S1, "#3": S2}.get(drive or "")
    for pid in (S1, S2):
        if pid == driven:
            claimed(h, pid)  # planned straight from the inbox, as autopilot does
        else:
            h.triage(pid, size=Size.S)
    return h


@pytest.mark.asyncio
async def test_an_owner_bundle_takes_sub_issues_of_an_autopilot_epic_and_says_so(
    service_config: ServiceConfig,
):
    async with rig(on(service_config), autopilot_epic(), parent=1) as r:
        # A proposal (and its tick) still refuses them.
        _found, problems = await r.bundles.check_proposals([proposal(2, 3)], {})
        assert "bundles[1] (#2 and #3): #2 is a sub-issue of epic #1, which is on autopilot" in (
            problems
        )
        assert await r.bundles.on_comment_delivery(command(2, "/bundle #3 Tidy")) == 1
        [b] = await r.state()
        assert b.issue_number == 100 and b.held == (2, 3)
        for n in (2, 3):
            assert Hold.BUNDLED in (await r.parcel(n)).holds
        [reply] = r.repo.comments_on(2)
        assert reply["body"] == (
            "Bundled #2 and #3 in bundle #100, built as one pull request.\n\n"
            "#2 and #3 are sub-issues of epic #1, which is on autopilot: the bundle builds "
            "them now, not autopilot. Autopilot goes on with the rest of #1; a part that "
            "depends on them waits until the bundle's pull request closes them."
        )
        for bad in ("Factory", "Next:", "/approve", "\n> "):
            assert bad not in reply["body"]


@pytest.mark.asyncio
async def test_an_owner_bundle_without_autopilot_members_only_reacts(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        assert await r.bundles.on_comment_delivery(command(1, "/bundle #2")) == 1
        assert r.repo.comments_on(1) == [] and (777, "+1") in r.repo.reactions


@pytest.mark.asyncio
async def test_an_owner_bundle_refuses_a_sub_issue_autopilot_drives_now(
    service_config: ServiceConfig,
):
    async with rig(on(service_config), autopilot_epic(drive="#2"), parent=1) as r:
        assert (await r.parcel(2)).autopilot_claim is not None
        assert await r.bundles.on_comment_delivery(command(2, "/bundle #3")) == 0
        assert await r.state() == []
        [reply] = r.repo.comments_on(2)
        assert "#2: epic autopilot drives it" in reply["body"]
        assert "which is on autopilot" not in reply["body"]


@pytest.mark.asyncio
async def test_a_tick_of_a_proposal_still_refuses_autopilot_epic_members(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        assert p.comment_id is not None
        body = r.repo.comments[p.comment_id]["body"]
        # Epic #40 was put on autopilot before the owner ticked.
        r.bundles.facts = _piloted(r.bundles.facts)  # type: ignore[method-assign]
        assert await r.bundles.on_comment_delivery(edited(r, p.comment_id, ticked(body))) == 1
        assert await r.state() == []
        [refused] = await r.proposals()
        assert refused.state == "refused"
        assert refused.detail == "#1 is a sub-issue of epic #40, which is on autopilot"


def _piloted(read: Any) -> Any:
    """The members' epic #40 is on autopilot (the facts as read, with that one change)."""

    async def facts(*args: Any, **kw: Any) -> dict[int, MemberFacts]:
        found = await read(*args, **kw)
        return {n: replace(f, parent_autopilot=f.parent == 40) for n, f in found.items()}

    return facts


# ------------------------------------------------------------------ bundle runs


@pytest.mark.asyncio
async def test_a_bundle_run_sees_its_members_and_their_triage(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        bundle = await r.parcel(100)
        view = await r.bundles.members_view(bundle, "FACTORY_DATA_x")
        assert view is not None and [m["number"] for m in view] == [1, 2]
        assert view[0]["triage"]["size"] == "S" and "Body of #1" in view[0]["text"]
        assert await r.bundles.members_for(bundle) == frozenset({1, 2})
        assert await r.bundles.members_for(await r.parcel(1)) is None
        closes = await r.bundles.required_closes(bundle.parcel_id)
        assert [n for n, _ in closes] == [1, 2]
        assert await r.bundles.required_closes((await r.parcel(1)).parcel_id) == ()


@pytest.mark.asyncio
async def test_ejection_returns_the_member_to_triage(service_config: ServiceConfig):
    async with rig(on(service_config), history(plan=(P2,))) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        bundle = await r.parcel(100)
        ejected = await r.bundles.record_ejections(
            bundle, {"ejected": [{"issue": 2, "reason": "needs a schema decision"}]}
        )
        assert ejected == [2]
        m = await r.parcel(2)
        assert Hold.BUNDLED not in m.holds and m.stage == Stage.TRIAGED
        assert m.note == "Ejected from bundle #100: needs a schema decision"
        assert "~~#2~~ ejected: needs a schema decision" in r.repo.issues[100]["body"]
        assert "`Closes #1`)" in r.repo.issues[100]["body"]
        assert await r.bundles.members_for(bundle) == frozenset({1})
        # One member left: the bundle carries on.
        [b] = await r.state()
        assert b.state == "open" and Hold.BUNDLED in (await r.parcel(1)).holds


@pytest.mark.asyncio
async def test_merge_completes_members_without_their_close_webhook(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        await r.apply(100, ev.PRObserved(pr_number=7, open=True, parcel_branch=True))
        await r.apply(100, ev.PRObserved(pr_number=7, open=False, merged=True, parcel_branch=True))
        r.repo.prs[7] = {"merged": True}
        # GitHub closed both members; no close webhook reached the factory.
        r.reads[1] = snapshot(open=False)
        r.reads[2] = snapshot(open=False)
        await r.bundles.catch_up()
        [b] = await r.state()
        assert b.state == "merged"
        assert [m.state for m in b.members] == ["done", "done"]
        for n in (1, 2):
            assert Hold.COMPLETED in (await r.parcel(n)).holds


@pytest.mark.asyncio
async def test_closing_the_bundle_issue_unmerged_releases_every_member(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        await r.bundles.on_issues_delivery(
            json.dumps({"action": "closed", "issue": {"number": 100}}).encode()
        )
        [b] = await r.state()
        assert b.state == "cancelled"
        for n in (1, 2):
            m = await r.parcel(n)
            assert Hold.BUNDLED not in m.holds and m.note == "Released from bundle #100"
            assert m.stage == Stage.TRIAGED  # back where it was


@pytest.mark.asyncio
async def test_unbundle_on_the_bundle_issue_releases_every_member(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        assert await r.bundles.on_comment_delivery(command(100, "/unbundle", 779)) == 1
        assert (779, "+1") in r.repo.reactions
        [b] = await r.state()
        assert b.state == "cancelled"
        assert [Hold.BUNDLED in (await r.parcel(n)).holds for n in (1, 2)] == [False, False]


@pytest.mark.asyncio
async def test_assigning_the_bundle_issue_releases_and_a_member_alone_is_ejected(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2 #3"))
        await r.bundles.on_issues_delivery(
            json.dumps(
                {"action": "labeled", "issue": {"number": 3}, "label": {"name": "factory:skip"}}
            ).encode()
        )
        [b] = await r.state()
        assert b.member(3) is not None and b.member(3).state == "ejected"  # type: ignore[union-attr]
        assert Hold.BUNDLED not in (await r.parcel(3)).holds
        await r.bundles.on_issues_delivery(
            json.dumps(
                {"action": "assigned", "issue": {"number": 100}, "assignee": {"type": "User"}}
            ).encode()
        )
        [b] = await r.state()
        assert b.state == "cancelled"
        assert [Hold.BUNDLED in (await r.parcel(n)).holds for n in (1, 2)] == [False, False]


# ------------------------------------------------------------------ the switch


@pytest.mark.asyncio
async def test_off_stops_proposals_and_creation_but_not_confirmed_bundles(
    service_config: ServiceConfig,
):
    async with rig(on(service_config)) as r:
        pending = await propose(r, 2, 3)
        await r.bundles.on_comment_delivery(command(1, "/bundle #2", 780))
        status = await r.bundles.command({"action": "off"})
        assert status["enabled"] is False and status["enabled_source"] == "operator"
        assert status["active_bundles"] == 1 and status["members_held"] == 2
        _found, problems = await r.bundles.check_proposals([proposal(1, 3)], {})
        assert problems == ["bundles: bundling is off; leave bundles out"]
        assert await r.bundles.ranking_text() == ("", "")
        # A tick of the pending proposal is inert; /bundle is refused.
        assert pending.comment_id is not None
        body = r.repo.comments[pending.comment_id]["body"]
        assert await r.bundles.on_comment_delivery(edited(r, pending.comment_id, ticked(body))) == 0
        assert await r.bundles.on_comment_delivery(command(3, "/bundle #2", 781)) == 0
        assert len(await r.state()) == 1
        # The confirmed bundle carries on: an ejection still applies.
        bundle = await r.parcel(100)
        assert await r.bundles.record_ejections(bundle, {"ejected": [{"issue": 2, "reason": "x"}]})
        listing = await r.bundles.command({"action": "list"})
        assert listing["bundles"][0]["members"]["#2"] == "ejected: x"  # type: ignore[index]


@pytest.mark.asyncio
async def test_the_config_switch_and_the_operator_override(service_config: ServiceConfig):
    async with rig(service_config) as r:
        assert (await r.bundles.enabled()) == (False, "config")
        assert await r.bundles.ranking_text() == ("", "")
        await r.bundles.command({"action": "on"})
        assert (await r.bundles.enabled()) == (True, "operator")
        text, field_ = await r.bundles.ranking_text()
        assert "2 to 3 related small issues" in text and "at most 4 points" in text
        assert "bundles" in field_


# ------------------------------------------------------------------ members left alone


@pytest.mark.asyncio
async def test_members_are_left_out_of_auto_build_auto_triage_and_autopilot(
    service_config: ServiceConfig,
):
    async with rig(on(service_config), history(plan=(P1,))) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2"))
        m = await r.parcel(1)
        assert mark_blocker(m) == "in bundle #100: built with it"
        assert known_to_factory(m)
        m = await r.parcel(2)  # triaged, its run settled
        from omnigent_factory.service.autopilot import EpicAutopilotService, Sub

        pilot = EpicAutopilotService(
            r.service,
            lambda ref: None,  # type: ignore[arg-type,return-value]
            lambda: None,  # type: ignore[arg-type,return-value]
            labels=lambda p: _labels(),  # type: ignore[arg-type]
            plans=lambda ap: None,  # type: ignore[arg-type,return-value]
        )
        member = replace(m, links=IssueLinks(parent=LinkedIssue(40, True)))
        assert await pilot._candidate_blocker(Sub(1, member, None, 0), set()) == ""
        freed = replace(member, holds=member.holds - {Hold.BUNDLED})
        assert await pilot._candidate_blocker(Sub(1, freed, None, 0), set()) is None


async def _labels() -> list[str]:
    return []


@pytest.mark.asyncio
async def test_members_are_not_ranked(service_config: ServiceConfig):
    from tests.service.test_ranking import card, item, ranking_config, started
    from tests.service.test_ranking import rig as ranking_rig

    class Held:
        async def held_numbers(self) -> frozenset[int]:
            return frozenset({2})

        async def ranking_text(self) -> tuple[str, str]:
            return "", ""

        async def check_proposals(self, raw: Any, priorities: Any) -> tuple[list[Any], list[str]]:
            return [], ["bundles: bundling is off; leave bundles out"]

    async with ranking_rig(ranking_config(service_config)) as rr:
        rr.ranker.bundles = Held()
        await started(rr, card(1), card(2), card(3))
        from omnigent_factory.service.ranking import RankingToolError

        with pytest.raises(RankingToolError, match="bundling is off"):
            await rr.submit(
                {"ranking": [item(1), item(3)], "summary": "t", "bundles": [proposal(1, 3)]}
            )
        # #2 is held in a bundle: not ranked, not required.
        receipt = await rr.submit({"ranking": [item(3), item(1)], "summary": "s"})
        assert receipt["ranked"] == 2


@pytest.mark.asyncio
async def test_ranking_records_accepted_proposals_and_names_them(service_config: ServiceConfig):
    from tests.service.test_ranking import card, item, ranking_config, started
    from tests.service.test_ranking import rig as ranking_rig

    recorded: list[Any] = []

    class Taking:
        async def held_numbers(self) -> frozenset[int]:
            return frozenset()

        async def ranking_text(self) -> tuple[str, str]:
            return "Bundling is on.\n", ", any bundles"

        async def check_proposals(self, raw: Any, priorities: Any) -> tuple[list[Any], list[str]]:
            return parse_proposals(raw)

        async def record_proposals(self, run_id: str, proposals: Any) -> None:
            recorded.extend(proposals)

    async with ranking_rig(ranking_config(service_config)) as rr:
        rr.ranker.bundles = Taking()
        await started(rr, card(1), card(2))
        receipt = await rr.submit(
            {"ranking": [item(1), item(2)], "summary": "s", "bundles": [proposal(1, 2)]}
        )
        assert receipt["bundles_proposed"] == 1
        assert [p.members for p in recorded] == [(1, 2)]
        assert await rr.ranker.run_once() == "completed"
        [update] = rr.board.updates.values()
        assert "**Bundles proposed**" in update and "#1, #2: Export tidy-up" in update


# ------------------------------------------------------------------ attribution


@dataclass
class Attributing:
    """A bundle issue #9 with members #1 and #2 (``source_for``/``member_sources``)."""

    async def source_for(self, parcel: Parcel, member: object) -> int | None:
        return member if member in (1, 2) else parcel.issue_number  # type: ignore[return-value]

    async def member_sources(self, parcel: Parcel) -> list[int]:
        return [1, 2]


@pytest.mark.asyncio
async def test_a_split_names_its_member_as_the_source(service_config: ServiceConfig):
    from omnigent_factory.service.links import NativeLinks
    from omnigent_factory.service.splits import Splits
    from omnigent_factory.store import discipline as ds

    async with rig(on(service_config)) as r:

        async def no_cards() -> list[Any]:
            return []

        links = NativeLinks(r.service, r.repo, lambda _sid: None, no_cards)  # type: ignore[arg-type]
        splits = Splits(r.service, links, str)
        splits.bundles = Attributing()
        bundle = replace(await r.parcel(3), issue_number=9)
        item_ = {"title": "Later bit", "body": "b", "reason": "owner said so", "member": 1}
        own = {"title": "Bundle bit", "body": "b", "reason": "owner said so"}
        await splits.record_result(bundle, "s1", {"split_out": [item_, own]})
        rows = await r.service.db.call(partial(ds.splits, repo_id=r.service.config.repo_id))
        assert {(s.source_issue, s.title) for s in rows} == {(1, "Later bit"), (9, "Bundle bit")}
        # The member's epic: the split joins #40 (member #1's parent).
        assert ("addSubIssue", (40, 100)) in r.repo.writes
        section = await splits.section(bundle, "s1")
        assert "Later bit" in section and "Bundle bit" in section


@pytest.mark.asyncio
async def test_a_deferred_finding_names_its_member_as_the_source(service_config: ServiceConfig):
    from omnigent_factory.service.deferred import Deferrals
    from omnigent_factory.service.review_rounds import ReviewRounds
    from omnigent_factory.store import discipline as ds
    from tests.service.test_follow_ups import tracker

    async with rig(on(service_config)) as r:
        deferred = Deferrals(
            r.service, r.repo, tracker(r.service, r.repo), ReviewRounds(r.service), str
        )
        deferred.bundles = Attributing()
        bundle = replace(await r.parcel(3), issue_number=9, pr_number=7)
        finding = {
            "severity": "P2",
            "path": "api/a.ts",
            "title": "Leak",
            "reason": "outside",
            "member": 2,
        }
        await deferred.record_result(
            bundle, "s1", {"kind": "build_ready", "pr_number": 7, "deferred": [finding]}
        )
        mine = await r.service.db.call(
            partial(ds.deferred_items, repo_id=r.service.config.repo_id, source_issue=2)
        )
        assert [i.title for i in mine] == ["Leak"]
        # Filed as the member's follow-up issue, still a box in the bundle's Ready report.
        assert any(w[0] == "createIssue" and "(#2)" in w[1][1] for w in r.repo.writes)
        section = await deferred.report_section(bundle)
        assert "Fix now: P2 `api/a.ts` Leak (in #" in section


# ------------------------------------------------------------------ status, doctor, CLI


@pytest.mark.asyncio
async def test_status_comments_show_the_bundle_and_each_member_its_bundle(
    service_config: ServiceConfig,
):
    from omnigent_factory.service.status_comment import IssueView, SubIssue, _sub_line, render_issue

    async with rig(on(service_config)) as r:
        await r.bundles.on_comment_delivery(command(1, "/bundle #2 #3 Tidy"))
        bundle = await r.parcel(100)
        await r.bundles.record_ejections(bundle, {"ejected": [{"issue": 3, "reason": "big"}]})
        text = await r.bundles.section(bundle)
        assert text.startswith("Bundle (open) of 2 issue(s): Tidy")
        assert "- #1 Add export button — in the bundle" in text
        assert "- #3 Add export button — ejected: big" in text
        member = await r.bundles.section(await r.parcel(1))
        assert member.startswith("Bundled into #100")
        assert await r.bundles.section(await r.parcel(3)) == ""
        view = IssueView(issue=1, stage="Triage", bot=BotState.IDLE, bundle=member)
        assert "#### Bundle\n\nBundled into #100" in render_issue(view, [], "Triage · idle")
    sub = SubIssue(number=1, title="Issue 1", open=True, stage="Triage", bundle=100)
    assert _sub_line(sub).endswith("Triage · in bundle #100")


@pytest.mark.asyncio
async def test_doctor_reports_bundling_and_its_counts(service_config: ServiceConfig):
    from omnigent_factory.service.doctor import DoctorReport, _check_bundling

    async with rig(on(service_config)) as r:
        await propose(r, 2, 3)
        await r.bundles.on_comment_delivery(command(1, "/bundle #2", 790))
    report = DoctorReport()
    _check_bundling(on(service_config), report)
    assert report.checks["bundling"] == (
        "on (config); suggested at most 3 members, 4 points; /bundle at most 8 members; "
        "1 proposals pending, 1 active bundles, 2 members held"
    )


@pytest.mark.asyncio
async def test_operator_command_and_cli(service_config: ServiceConfig):
    from omnigent_factory.cli import build_parser

    for action in ("status", "on", "off", "list", "propose-now"):
        args = build_parser().parse_args(["bundles", action])
        assert args.command == "bundles" and args.bundles_command == action
    async with rig(service_config) as r:
        status = await r.service.operator_command("bundles", {"action": "status"})
        assert status["enabled"] is False and status["proposals_pending"] == 0
        with pytest.raises(ValueError, match="bundling is off"):
            await r.service.operator_command("bundles", {"action": "propose-now"})
        with pytest.raises(ValueError, match="status, on, off, list or propose-now"):
            await r.service.operator_command("bundles", {"action": "grant"})


def test_the_switch_and_caps_are_hot_reloadable_and_bounded(service_config: ServiceConfig):
    from pydantic import ValidationError

    from omnigent_factory.service.config import HOT_RELOAD_KEYS

    assert {
        "bundling",
        "bundle_max_members",
        "bundle_command_max_members",
        "bundle_max_points",
    } <= HOT_RELOAD_KEYS
    assert (service_config.bundling, service_config.bundle_max_members) == (False, 3)
    assert service_config.bundle_command_max_members == 8
    assert service_config.bundle_max_points == 4
    raw = service_config.model_dump()
    assert ServiceConfig.model_validate({**raw, "bundle_max_members": 8}).bundle_max_members == 8
    widest = {**raw, "bundle_max_members": 12, "bundle_command_max_members": 12}
    assert ServiceConfig.model_validate(widest).bundle_command_max_members == 12
    with pytest.raises(ValidationError, match="must not exceed bundle_command_max_members"):
        ServiceConfig.model_validate({**raw, "bundle_max_members": 9})
    for bad in (
        {"bundle_command_max_members": 13},
        {"bundle_command_max_members": 1},
        {"bundle_max_members": 13},
        {"bundle_max_members": 1},
    ):
        with pytest.raises(ValidationError):
            ServiceConfig.model_validate({**raw, **bad})


def test_the_setup_template_documents_the_bundle_limits(
    service_config: ServiceConfig, tmp_path: Any
):
    import tomllib

    from omnigent_factory.service.setup import OperationsRenderer

    sample = OperationsRenderer(tmp_path / "config.toml").render(service_config)
    raw = tomllib.loads(sample["config.example.toml"])["service"]
    keys = ("bundling", "bundle_max_members", "bundle_command_max_members", "bundle_max_points")
    assert {k: raw[k] for k in keys} == {
        "bundling": False,
        "bundle_max_members": 3,
        "bundle_command_max_members": 8,
        "bundle_max_points": 4,
    }
    parsed = ServiceConfig.model_validate({**service_config.model_dump(), **raw})
    assert parsed.bundle_command_max_members == 8


def test_the_needs_you_comment_names_the_missing_member_closes():
    from omnigent_factory.service.directory import needs_you_text

    base = {"pr_number": 7, "review_missing": False, "closes_issue": False, "issue": 9}
    text = needs_you_text({**base, "missing_closes": "#12, #13"})
    assert "PR #7 isn't ready: the PR doesn't close #12, #13" in text
    assert "the PR doesn't close #9" in needs_you_text(base)
