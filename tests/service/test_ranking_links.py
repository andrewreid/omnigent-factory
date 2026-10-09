"""Ranking respects native links: a blocker ranks above what it blocks, sub-issues of one
parent stay together, and an owner pin is kept even when it forces a violation."""

from __future__ import annotations

import logging

import pytest

from omnigent_factory.core.types import IssueLinks, LinkedIssue
from omnigent_factory.service.ranking import link_order, validate_submission
from tests.service.test_ranking import card, item


def blocked_by(*numbers: int, parent: int | None = None) -> IssueLinks:
    return IssueLinks(
        blocked_by=tuple(LinkedIssue(n, True) for n in numbers),
        parent=LinkedIssue(parent, True) if parent is not None else None,
    )


def test_a_blocker_moves_above_what_it_blocks_and_closed_blockers_do_not_count():
    cards = {
        1: card(1, links=blocked_by(3)),
        2: card(2),
        3: card(3),
        4: card(4, links=IssueLinks(blocked_by=(LinkedIssue(2, False),))),
    }
    order, messages = link_order([1, 4, 2, 3], cards, {})
    # #1 waits for #3; #4's blocker (#2) is closed, so it keeps its place.
    assert order == [4, 2, 3, 1]
    assert messages == []


def test_sub_issues_of_one_parent_are_kept_together_unless_a_blocker_says_otherwise():
    cards = {
        10: card(10, links=blocked_by(parent=99)),
        20: card(20),
        11: card(11, links=blocked_by(parent=99)),
        12: card(12, links=blocked_by(30, parent=99)),
        30: card(30),
    }
    order, _ = link_order([10, 20, 11, 12, 30], cards, {})
    assert order[:2] == [10, 11]  # siblings adjacent
    assert order.index(30) < order.index(12)  # the blocker rule wins over adjacency


def test_a_cycle_does_not_lose_issues():
    cards = {1: card(1, links=blocked_by(2)), 2: card(2, links=blocked_by(1))}
    order, _ = link_order([1, 2], cards, {})
    assert sorted(order) == [1, 2]


def test_an_owner_pin_is_kept_and_its_blocker_is_pulled_up_where_possible():
    cards = {1: card(1), 2: card(2), 3: card(3), 9: card(9, links=blocked_by(3))}
    order, messages = link_order([1, 2, 3], cards, {9: 2.0})
    assert order[0] == 3 and messages == []  # rank 1 (above the pin at 2)
    # Pinned at 1: nothing can go above it; the pin stays and it is reported.
    order, messages = link_order([1, 2, 3], cards, {9: 1.0})
    assert messages and "#3" in messages[0] and "#9" in messages[0]


def test_a_submission_is_reordered_by_links_and_a_forced_violation_is_logged(
    caplog: pytest.LogCaptureFixture,
):
    triage = {
        1: card(1, links=blocked_by(2)),
        2: card(2),
        9: card(9, links=blocked_by(2)),
    }
    raw = {"ranking": [item(1), item(2)], "summary": "s"}
    with caplog.at_level(logging.WARNING):
        sub = validate_submission(raw, triage, {9: 1.0}, ())
    assert [n for n, _ in sub.order] == [2, 1]
    assert sub.ranks == {9: 1.0, 2: 2.0, 1: 3.0}
    assert "owner pin keeps it so" in caplog.text
