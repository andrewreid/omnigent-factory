"""A finished parcel shows Bot = Idle whatever holds it was left with (#677 after merge)."""

from __future__ import annotations

from dataclasses import replace

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import EffectKind
from omnigent_factory.core.projection import project_bot
from omnigent_factory.core.types import BotState, Hold, Lifecycle, Stage
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
STALE = frozenset({Hold.CHECKS_FAILED, Hold.REWORK_CONTROL_REQUIRED, Hold.SAFETY})


def _finished(h: Harness, **changes: object):
    p = h.p()
    retired = tuple(replace(s, lifecycle=Lifecycle.RETIRED) for s in p.sessions)
    return replace(p, stage=Stage.DONE, eligible=False, sessions=retired, holds=STALE, **changes)


def test_finished_parcel_is_idle_despite_leftover_holds():
    h = Harness()
    h.to_building()
    done = _finished(h)
    assert project_bot(done) == BotState.IDLE
    assert project_bot(replace(done, stage=Stage.BUILDING)) == BotState.IDLE  # closed issue
    assert project_bot(replace(done, eligible=True)) == BotState.IDLE  # Done column


def test_unfinished_or_still_draining_parcels_keep_their_signal():
    h = Harness()
    h.to_building()
    done = _finished(h)
    live = replace(done, eligible=True, stage=Stage.BUILDING)
    assert project_bot(live) == BotState.NEEDS_YOU  # open work: holds still apply
    draining = replace(
        done, sessions=tuple(replace(s, lifecycle=Lifecycle.DRAINING) for s in done.sessions)
    )
    assert project_bot(draining) != BotState.IDLE  # never Idle before the tree is quiescent


def test_closing_a_parcel_settles_the_card_to_idle_with_one_write():
    h = Harness()
    b = h.to_building()
    h.send(P, ev.Closed())
    assert h.p().bot != BotState.IDLE  # the stop drain is still running
    r = h.quiesce(P, b.session_id)
    assert h.p().bot == BotState.IDLE
    assert [e.args["bot"] for e in Harness.of(r, EffectKind.SET_BOT)] == ["Idle"]
