"""A bundle PR is Ready only when its closing references include the bundle issue AND
every member still in the bundle (an all-of check for bundle parcels only); the read says
which ``Closes`` lines are missing."""

from __future__ import annotations

import httpx
import pytest

from omnigent_factory.ports.github import PullRequestEvidence

from .test_adapter import PARCEL_REF, adapter, closing_refs
from .test_status_and_linkage import pr_handler


async def evidence(
    graphql: list[httpx.Response], members: tuple[tuple[int, str], ...] | None
) -> PullRequestEvidence:
    seen: list[dict] = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(pr_handler(graphql, seen))) as h:
        gh = adapter(h)
        if members is not None:

            async def required(parcel_id: str) -> tuple[tuple[int, str], ...]:
                assert parcel_id == PARCEL_REF.parcel_id
                return members

            gh.bundle_members = required
        result = await gh.pull_request(PARCEL_REF, 7)
    assert isinstance(result, PullRequestEvidence)
    return result


MEMBERS = ((13, "I_13"), (14, "I_14"))


@pytest.mark.asyncio
async def test_a_bundle_pr_must_close_every_member_too():
    found = await evidence([closing_refs("I_1"), closing_refs("I_1", "I_13")], MEMBERS)
    assert found.closes_issue is False and found.missing_closes == (14,)
    assert found.verified is False


@pytest.mark.asyncio
async def test_a_bundle_pr_closing_all_of_them_closes_the_issue():
    refs = closing_refs("I_1", "I_13", "I_14")
    found = await evidence([refs, refs], MEMBERS)
    assert found.closes_issue is True and found.missing_closes == ()


@pytest.mark.asyncio
async def test_the_bundle_issue_itself_is_named_when_missing():
    refs = closing_refs("I_13", "I_14")
    found = await evidence([refs, refs], MEMBERS)
    assert found.closes_issue is False and found.missing_closes == (12,)


@pytest.mark.asyncio
async def test_an_ordinary_pr_needs_only_its_own_issue():
    assert (await evidence([closing_refs("I_1")], None)).closes_issue is True
    assert (await evidence([closing_refs("I_1")], ())).missing_closes == ()
