"""The unconfirmed-create search budget has one source of truth (the reducer's search
schedule); the directory's tombstone check reads it instead of its own constant."""

from __future__ import annotations

from types import SimpleNamespace

from omnigent_factory.core import reducer
from omnigent_factory.core.types import Lifecycle
from omnigent_factory.service import directory


def test_budget_is_derived_from_the_search_schedule() -> None:
    # One immediate search plus one per delay; ten minutes or more in all.
    delays = reducer._CREATE_SEARCH_DELAYS_US
    assert len(delays) + 1 == reducer.CREATE_SEARCH_BUDGET
    assert reducer.CREATE_SEARCH_BUDGET == 4  # today's schedule
    assert directory.CREATE_SEARCH_BUDGET is reducer.CREATE_SEARCH_BUDGET


def _run(searches: int) -> SimpleNamespace:
    return SimpleNamespace(
        execution_closed=True,
        lifecycle=Lifecycle.RETIRED,
        create_unconfirmed=True,
        create_searches=searches,
        create_match_seen=False,
        root_id=None,
    )


def test_directory_concludes_an_empty_create_exactly_at_the_budget() -> None:
    budget = reducer.CREATE_SEARCH_BUDGET
    assert not directory._concluded_empty_create(_run(budget - 1))  # type: ignore[arg-type]
    assert directory._concluded_empty_create(_run(budget))  # type: ignore[arg-type]
