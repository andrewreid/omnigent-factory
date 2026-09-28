"""#677 → Ready: Molly's cross-vendor review counts; ambiguous evidence reads are re-fetched."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from omnigent_factory.core import events as ev
from omnigent_factory.core.effects import Ack, EffectIntent, EffectKind, Preconditions
from omnigent_factory.core.types import BotState, Stage
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.directory import PublicationRenderer, ServiceDispatchDirectory
from omnigent_factory.service.runtime import FactoryService
from omnigent_factory.store.sqlite import SqliteStore
from omnigent_factory.testing.builders import result_candidate
from omnigent_factory.testing.fakes import (
    FakeClock,
    FakeCredentialBroker,
    FakeGitHub,
    FakeOmnigent,
)
from omnigent_factory.testing.harness import Harness

P = "I_parcel_1"
HEAD = "4950075d373b7db6c008a502da568ee73e0fdf03"
#: Molly's build_ready for #677 as saved (review block verbatim, trimmed text fields).
BUILD_READY = {
    "kind": "build_ready",
    "pr_number": 682,
    "branch": "factory/issue-677",
    "head_sha": HEAD,
    "summary": "Route guard proven in isolation.",
    "verification": [],
    "review": {
        "accepted": True,
        "artifact_reference": "node_modules/.molly/reviews/677/release-ce3388c-1.md",
        "artifact_sha256": "43eed0edd847d4e209675f74299657274aca518f4c0be54553cd2c6f8aa209ef",
        "implementation_vendor": "anthropic",
        "review_vendor": "openai",
        "reviewed_head": HEAD,
    },
    "findings": [
        {
            "id": "A1",
            "source": "codex",
            "severity": "ADVISORY",
            "disposition": "advisory",
            "evidence": "disabled-user cases pass with the guard removed",
        }
    ],
    "remediation_batches_used": 0,
    "targeted_rechecks_used": 0,
    "release_readiness": "ready",
}


def _effect(args: dict[str, Any]) -> EffectIntent:
    return EffectIntent(
        effect_id="ef_fetch",
        kind=EffectKind.FETCH_PR_EVIDENCE,
        parcel_id=P,
        target=P,
        preconditions=Preconditions(1, 0),
        args=args,
    )


@pytest.mark.parametrize(
    ("change", "clean"),
    [
        ({}, True),
        ({"review": {**BUILD_READY["review"], "review_vendor": "Anthropic"}}, False),
        ({"review": {**BUILD_READY["review"], "accepted": False}}, False),
        ({"review": {**BUILD_READY["review"], "reviewed_head": "0" * 40}}, False),
        ({"findings": [{**BUILD_READY["findings"][0], "disposition": "unresolved"}]}, False),
    ],
)
@pytest.mark.asyncio
async def test_cross_vendor_review_is_read_from_the_build_ready_result(
    service_config: ServiceConfig, change: dict[str, Any], clean: bool
) -> None:
    results = service_config.state_dir / "results"
    results.mkdir(mode=0o700, exist_ok=True)
    payload = {"item_id": "i", "factory_result": {"result": {**BUILD_READY, **change}}}
    (results / "ss_build-latest.json").write_text(json.dumps(payload))
    renderer = PublicationRenderer(ServiceDispatchDirectory(None, service_config), service_config)  # type: ignore[arg-type]
    args = {"session_id": "ss_build", "pr_number": 682, "head_sha": HEAD}
    assert await renderer.cross_vendor_review(_effect(args)) is clean


def test_fresh_evidence_clears_ambiguous_reads_and_reaches_ready() -> None:
    h = Harness()
    b = h.to_building()
    h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=682,
            head_sha=HEAD,
        ),
    )
    for n in range(3):  # the live parcel held 11 of these
        h.send(
            P,
            ev.EffectUnknown(
                effect_id=f"ef_old{n}", effect_kind=EffectKind.FETCH_PR_EVIDENCE.value
            ),
        )
    h.send(
        P,
        ev.PRObserved(
            pr_number=682, head_sha=HEAD, open=True, bot_authored=True, parcel_branch=True
        ),
    )
    assert h.p().bot == BotState.BLOCKED
    h.send(
        P,
        ev.ReadinessEvidence(session_id=b.session_id, pr_number=682, head_sha=HEAD, verified=True),
    )
    h.quiesce(P, b.session_id)
    p = h.p()
    assert not p.unknown_effects and p.readiness.ready and p.stage == Stage.READY


@pytest.mark.asyncio
async def test_boot_requeues_unknown_reads_and_a_detail_less_ack_is_retried(
    service_config: ServiceConfig,
) -> None:
    h = Harness()
    b = h.to_building()
    r = h.send(
        P,
        result_candidate(
            b.session_id,
            b.root_id,
            b.revision,
            ev.ResultKind.BUILD_READY,
            pr_number=682,
            head_sha=HEAD,
        ),
    )
    [fetch] = Harness.of(r, EffectKind.FETCH_PR_EVIDENCE)
    h.send(
        P,
        ev.PRObserved(
            pr_number=682, head_sha=HEAD, open=True, bot_authored=True, parcel_branch=True
        ),
    )
    h.quiesce(P, b.session_id)
    clock = FakeClock()
    store = SqliteStore.open(service_config.database_path, clock)
    store.ensure_repository(h.cfg)
    store.apply_event(h.f().make(ev.Unpause(), parcel_id=None, event_id="unpause"), h.cfg)
    for event, _ in h.log:
        store.apply_event(event, h.cfg)
    store.query("UPDATE effects SET state = 'unknown' WHERE effect_id = ?", (fetch.effect_id,))
    store.close()

    github = FakeGitHub()
    verified = {"verified": True, "remediation_exhausted": False}
    # The startup reconcile adds one catch-up read of the unverified head.
    github.script(
        EffectKind.FETCH_PR_EVIDENCE, Ack("682", {}), Ack("682", verified), Ack("682", verified)
    )
    service = FactoryService(
        service_config,
        adapters=(github, FakeOmnigent(), FakeCredentialBroker()),
        clock=clock,
    )
    await service.start()
    try:
        for _ in range(300):
            clock.advance(1_000_000)  # the retry of the detail-less ack comes due
            stored = await service.db.call(lambda s: s.get_effect(fetch.effect_id))
            if stored is not None and stored.state == "done":
                break
            await asyncio.sleep(0.01)
        stored = await service.db.call(lambda s: s.get_effect(fetch.effect_id))
        parcel = await service.db.call(lambda s: s.load_parcel(P))
        assert stored.state == "done" and len(github.executed(EffectKind.FETCH_PR_EVIDENCE)) >= 2
        assert not parcel.unknown_effects
        assert parcel.readiness is not None and parcel.readiness.verified
        assert parcel.stage == Stage.READY
    finally:
        await service.stop()
