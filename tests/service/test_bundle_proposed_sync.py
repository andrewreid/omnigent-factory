"""The bundling service converges ``factory:bundle-proposed`` through the reducer: each
reconcile tick reports the open proposals naming each member (bounded per pass, so
existing proposals are backfilled gradually); accepted or stale proposals take it off."""

from __future__ import annotations

import pytest

from omnigent_factory.core.effects import EffectKind
from omnigent_factory.service.config import ServiceConfig
from tests.service.test_bundles import Rig, confirmed, on, propose, rig

pytestmark = pytest.mark.asyncio


async def _label_effects(r: Rig, number: int) -> list[str]:
    parcel = await r.parcel(number)
    rows = await r.service.db.call(
        lambda store: store.query(
            "SELECT json_extract(payload_json, '$.args.action') FROM effects "
            "WHERE parcel_id = ? AND kind = ? ORDER BY rowid",
            (parcel.parcel_id, EffectKind.SET_BUNDLE_PROPOSED.value),
        )
    )
    return [str(row[0]) for row in rows]


async def test_members_of_a_posted_proposal_are_labelled_once(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        p = await propose(r, 1, 2)
        assert await r.bundles.sync_proposed_labels() == 2
        for n in (1, 2):
            m = await r.parcel(n)
            assert m.bundle_proposals == (p.proposal_key,) and m.bundle_proposed_label
            assert await _label_effects(r, n) == ["add"]
        assert (await r.parcel(3)).bundle_proposals == ()
        # Converged: another pass changes nothing and emits nothing.
        assert await r.bundles.sync_proposed_labels() == 0
        assert await _label_effects(r, 1) == ["add"]


async def test_backfill_is_bounded_per_pass(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        await propose(r, 1, 2)
        assert await r.bundles.sync_proposed_labels(limit=1) == 1
        assert await r.bundles.sync_proposed_labels(limit=1) == 1
        assert await r.bundles.sync_proposed_labels(limit=1) == 0
        assert all([(await r.parcel(n)).bundle_proposed_label for n in (1, 2)])


async def test_an_accepted_proposal_removes_the_label(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        await propose(r, 1, 2)
        await r.bundles.sync_proposed_labels()
        await confirmed(r, 1, 2)  # the members are bundled now
        await r.bundles.tick()
        for n in (1, 2):
            m = await r.parcel(n)
            assert m.bundle_proposals == () and not m.bundle_proposed_label
            assert await _label_effects(r, n) == ["add", "remove"]


async def test_a_stale_proposal_removes_the_label(service_config: ServiceConfig):
    async with rig(on(service_config)) as r:
        await propose(r, 1, 2)
        await r.bundles.sync_proposed_labels()
        r.repo.issues[2]["labels"].append("factory:skip")
        await r.bundles.catch_up()
        [p] = await r.proposals()
        assert p.state == "stale"
        assert await r.bundles.sync_proposed_labels() == 2
        assert not (await r.parcel(1)).bundle_proposed_label
        assert await _label_effects(r, 1) == ["add", "remove"]
