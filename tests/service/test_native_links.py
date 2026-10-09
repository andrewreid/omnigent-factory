"""GitHub-native links in the service: writing the links triage reports (idempotent,
same repository, add-only), epic notes and issue types, and re-reads on link webhooks."""

from __future__ import annotations

import json
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectIntent, EffectKind, Preconditions, RetryClass
from omnigent_factory.core.types import IssueLinks, LinkedIssue, Stage
from omnigent_factory.ports.github import RankingCard
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.links import NativeLinks, epic_note, requested_links
from omnigent_factory.testing.builders import EventFactory, snapshot
from omnigent_factory.testing.harness import Harness
from tests.service.test_auto_build import rig

REPO = "R_kgDOTC12Fg"


class FakeClient:
    """GraphQL only: issues by number, their blockers and type; records mutations."""

    def __init__(self, issues: dict[int, dict[str, Any]], *, types: list[str] = ()) -> None:  # type: ignore[assignment]
        self.issues = issues
        self.types = list(types)
        self.mutations: list[tuple[str, dict[str, object]]] = []

    async def graphql(self, query: str, variables: dict[str, object]) -> dict[str, Any]:
        if query.lstrip().startswith("mutation"):
            name = "addBlockedBy" if "addBlockedBy" in query else "updateIssueIssueType"
            self.mutations.append((name, dict(variables)))
            if name == "addBlockedBy":
                blocked = next(i for i in self.issues.values() if i["id"] == variables["issue"])
                blocking = next(i for i in self.issues.values() if i["id"] == variables["blocking"])
                blocked["blocked_by"].append(blocking["number"])
            return {}
        if "issueTypes" in query:
            return {
                "repository": {
                    "issueTypes": {"nodes": [{"id": f"IT_{n}", "name": n} for n in self.types]}
                }
            }
        issue = self.issues.get(int(variables["number"]))  # type: ignore[arg-type]
        if issue is None:
            return {"repository": {"issue": None}}
        return {
            "repository": {
                "issue": {
                    "id": issue["id"],
                    "number": issue["number"],
                    "repository": {"id": issue.get("repo", REPO)},
                    "issueType": {"name": issue["type"]} if issue.get("type") else None,
                    "blockedBy": {
                        "totalCount": len(issue["blocked_by"]),
                        "nodes": [
                            {"number": n, "repository": {"id": REPO}} for n in issue["blocked_by"]
                        ],
                    },
                }
            }
        }


def issue(number: int, *, blocked_by: tuple[int, ...] = (), **kw: object) -> dict[str, Any]:
    return {"id": f"I_{number}", "number": number, "blocked_by": list(blocked_by), **kw}


def links(service: Any, client: FakeClient, cards: list[RankingCard] | None = None) -> NativeLinks:
    async def read() -> list[RankingCard]:
        return list(cards or [])

    return NativeLinks(service, client, lambda _sid: None, read)  # type: ignore[arg-type]


def card(
    number: int, *, rank: float | None = None, linked: IssueLinks | None = None
) -> RankingCard:
    return RankingCard(
        node_id=f"I_n{number}",
        item_id=f"PVTI_{number}",
        number=number,
        title=f"Issue {number}",
        stage=Stage.TRIAGED,
        created_at_us=1,
        rank=rank,
        priority=None,
        links=linked if linked is not None else IssueLinks(),
    )


# ------------------------------------------------------------------ what triage asks for


def test_depends_on_and_blocks_become_blocked_by_pairs_and_nothing_else():
    result = {
        "result": {
            "kind": "triage",
            "related": [
                {"issue": 823, "relation": "depends_on", "note": "x"},
                {"issue": 825, "relation": "blocks", "note": "x"},
                {"issue": 12, "relation": "overlaps", "note": "x"},
                {"issue": 824, "relation": "depends_on", "note": "self"},
            ],
        }
    }
    assert requested_links(result, 824) == [(824, 823), (825, 824)]
    epic = {
        "result": {
            "kind": "epic_triage",
            "links": [
                {"issue": 824, "blocked_by": 823, "reason": "x"},
                {"issue": 824, "blocked_by": 823, "reason": "dup"},
            ],
        }
    }
    assert requested_links(epic, 821) == [(824, 823)]


# ------------------------------------------------------------------ writes


@pytest.mark.asyncio
async def test_a_link_is_created_once_never_duplicated_and_only_within_the_repository(
    service_config: ServiceConfig,
):
    async with rig(service_config, Harness()) as r:
        client = FakeClient(
            {
                823: issue(823),
                824: issue(824),
                825: issue(825, blocked_by=(824,)),
                900: issue(900, repo="R_other"),
            }
        )
        native = links(r.service, client)
        assert await native.link(824, 823, source=824) is True
        assert client.mutations == [("addBlockedBy", {"issue": "I_824", "blocking": "I_823"})]
        # Idempotent: an existing link (made now or before) is skipped.
        assert await native.link(824, 823, source=824) is False
        assert await native.link(825, 824, source=824) is False
        # Another repository's issue (or none) is never linked.
        assert await native.link(900, 824, source=824) is False
        assert await native.link(824, 999, source=824) is False
        assert len(client.mutations) == 1
        assert not any("remove" in name.lower() for name, _ in client.mutations)


@pytest.mark.asyncio
async def test_write_reads_the_published_result_of_the_triage_run(service_config: ServiceConfig):
    h = Harness()
    h.eligible("I_src")
    async with rig(service_config, h) as r:
        client = FakeClient({1: issue(1), 823: issue(823)})
        stored = {
            "factory_result": {
                "result": {
                    "kind": "triage",
                    "related": [{"issue": 823, "relation": "depends_on", "note": "x"}],
                }
            }
        }

        async def read() -> list[RankingCard]:
            return []

        native = NativeLinks(r.service, client, lambda _sid: stored, read)  # type: ignore[arg-type]
        effect = EffectIntent(
            effect_id="ef_1",
            kind=EffectKind.PUBLISH_TRIAGE,
            parcel_id="I_src",
            target="I_src",
            args={"session_id": "ss_1"},
            preconditions=Preconditions(parcel_version=1, eligibility_epoch=0),
            retry_class=RetryClass.ADOPTABLE_WRITE,
            dedupe_key="d",
        )
        assert await native.write(effect) == 1
        assert client.mutations == [("addBlockedBy", {"issue": "I_1", "blocking": "I_823"})]


# ------------------------------------------------------------------ epics


def test_epic_note_counts_done_and_names_the_best_ranked_unblocked_sub_issue():
    parent = LinkedIssue(821, True)
    epic = card(821, linked=IssueLinks(sub_total=8, sub_completed=1))
    blocked = card(
        823, rank=1.0, linked=IssueLinks(parent=parent, blocked_by=(LinkedIssue(822, True),))
    )
    free = card(824, rank=2.0, linked=IssueLinks(parent=parent))
    unranked = card(825, linked=IssueLinks(parent=parent))
    other = card(9, rank=0.5)
    assert epic_note(epic, [epic, blocked, free, unranked, other]) == (
        "Epic · 1/8 done · next: #824"
    )
    assert epic_note(epic, [epic, blocked]) == "Epic · 1/8 done"
    assert epic_note(other, [other]) == ""


@pytest.mark.asyncio
async def test_epic_pass_writes_the_note_only_on_change_and_types_untyped_epics(
    service_config: ServiceConfig,
):
    h = Harness()
    for pid in ("I_n821", "I_n5"):
        h.eligible(pid)
    async with rig(service_config, h) as r:
        epic = card(821, linked=IssueLinks(sub_total=2, sub_completed=0))
        child = card(824, linked=IssueLinks(parent=LinkedIssue(821, True)))
        typed = card(5, linked=IssueLinks(sub_total=1))
        client = FakeClient({821: issue(821), 5: issue(5, type="Feature")}, types=["Task", "Epic"])
        native = links(r.service, client, [epic, child, typed])
        assert await native.epic_pass() == 2
        p = await r.parcel("I_n821")
        assert p.epic_note == "Epic · 0/2 done · next: #824"
        assert (await r.parcel("I_n5")).epic_note == "Epic · 0/1 done"
        assert await native.epic_pass() == 0  # unchanged: nothing applied
        # The untyped epic got the Epic type; the owner's Feature type was left alone.
        assert client.mutations == [("updateIssueIssueType", {"issue": "I_821", "type": "IT_Epic"})]


@pytest.mark.asyncio
async def test_without_an_epic_issue_type_typing_is_a_no_op(service_config: ServiceConfig):
    h = Harness()
    h.eligible("I_n821")
    async with rig(service_config, h) as r:
        client = FakeClient({821: issue(821)}, types=["Task", "Bug", "Feature"])
        native = links(r.service, client, [card(821, linked=IssueLinks(sub_total=3))])
        await native.epic_pass()
        assert client.mutations == []


# ------------------------------------------------------------------ re-reads


@pytest.mark.asyncio
async def test_link_webhooks_and_a_closed_blocker_re_read_the_known_issues(
    service_config: ServiceConfig,
):
    h = Harness()
    for number in (823, 824):
        h.factories[f"I_{number}"] = EventFactory(f"I_{number}", issue_number=number)
    h.eligible("I_824")
    f = h.f("I_823")
    h.apply(
        f.make(
            ev.GitHubSnapshot(),
            evidence=snapshot(
                read_at_us=f.now,
                links=IssueLinks(blocking=(LinkedIssue(824, True),), parent=LinkedIssue(821, True)),
            ),
        )
    )
    async with rig(service_config, h) as r:
        reread: list[str] = []

        async def record(parcel_id: str) -> None:
            reread.append(parcel_id)

        r.service.reread = record  # type: ignore[method-assign]
        native = links(r.service, FakeClient({}))
        repo = f"https://api.github.com/repos/{service_config.repository}"
        payload = {
            "action": "blocked_by_added",
            "blocked_issue": {"node_id": "I_824", "repository_url": repo},
            "blocking_issue": {"node_id": "I_unknown", "repository_url": repo},
        }
        assert await native.on_link_delivery("issue_dependencies", json.dumps(payload)) == 1
        other = {
            "action": "sub_issue_added",
            "parent_issue": {"node_id": "I_824", "repository_url": ".../repos/x/y"},
        }
        assert await native.on_link_delivery("sub_issues", json.dumps(other)) == 0
        assert reread == ["I_824"]
        # #823 closed: what it blocks (#824, known) is re-read; its parent is unknown.
        assert await native.on_issue_state("I_823") == 1
        assert reread == ["I_824", "I_824"]


# ------------------------------------------------------------------ webhooks


@pytest.mark.asyncio
async def test_a_link_webhook_is_handed_to_native_links_and_retired(
    service_config: ServiceConfig,
):
    from dataclasses import replace as dc_replace

    from omnigent_factory.github.webhook import DeliveryNormalizer
    from omnigent_factory.service.github_delivery import GitHubDeliveryProcessor
    from omnigent_factory.store.sqlite import DeliveryRecord
    from tests.service.test_main_conflict import OWNER
    from tests.service.test_webhook_mapping import IDENTITY, _common

    async with rig(service_config, Harness()) as r:
        seen: list[tuple[str, bytes]] = []

        class Native:
            async def on_link_delivery(self, name: str, body: bytes) -> int:
                seen.append((name, body))
                return 0

        processor = GitHubDeliveryProcessor(
            r.service,
            DeliveryNormalizer(dc_replace(IDENTITY, repository_node_id=service_config.repo_id)),
            r.github,  # type: ignore[arg-type]
            r.clock,
        )
        processor.native_links = Native()
        payload = _common(OWNER)
        payload["repository"] = {
            "id": IDENTITY.repository_id,
            "node_id": service_config.repo_id,
            "full_name": IDENTITY.repository_full_name,
        }
        body = json.dumps({**payload, "action": "blocked_by_added"}).encode()
        record = DeliveryRecord("d-dep", "issue_dependencies", body, {})
        await r.service.db.call(lambda store: store.append_delivery(record))
        await processor.process(record)
        assert seen == [("issue_dependencies", body)]
        rows = await r.service.db.call(
            lambda store: store.query("SELECT status FROM deliveries WHERE delivery_guid = 'd-dep'")
        )
        assert rows[0][0] != "pending"


@pytest.mark.asyncio
async def test_each_board_diff_runs_the_epic_pass(service_config: ServiceConfig):
    from omnigent_factory.service.board_diff import DiffResult

    async with rig(service_config, Harness()) as r:
        passes: list[int] = []

        class Diff:
            async def run_once(self) -> DiffResult:
                return DiffResult()

        class Native:
            async def epic_pass(self) -> int:
                passes.append(1)
                return 0

        r.service.board_diff = Diff()
        r.service.native_links = Native()
        await r.service._board_diff_tick(0.0, frozenset())
        assert passes == [1]
