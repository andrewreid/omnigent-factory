"""Broker issuance: default-deny, refresh, refusal, revocation, rotation (§6.1)."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from omnigent_factory.core.effects import (
    Ack,
    CredentialProfile,
    DefinitiveFailure,
    EffectIntent,
    EffectKind,
    ExecutionContext,
    Preconditions,
)
from omnigent_factory.credentials.broker import PROFILE_PERMISSIONS, LocalCredentialBroker
from omnigent_factory.credentials.capabilities import (
    CapabilityRegistry,
    read_capability_file,
)
from omnigent_factory.ports.credentials import CredentialBroker, TokenGrant, TokenRefusal
from omnigent_factory.testing.fakes import FakeClock
from tests.credentials.fakes import HOUR_US, FakeGate, FakeMinter

REPO = "SA-Ambulance/timesheets"
CTX = ExecutionContext(boot_id="boot", lease_epoch=1, parcel_version=1, attempt=1)


def _intent(kind: EffectKind, sid: str, profile: str | None = None) -> EffectIntent:
    args = {"profile": profile} if profile is not None else {}
    return EffectIntent(
        effect_id=f"ef-{kind}-{sid}",
        kind=kind,
        parcel_id="P1",
        target=sid,
        preconditions=Preconditions(parcel_version=1, eligibility_epoch=0, session_id=sid),
        args=args,
    )


@pytest.fixture
def setup(tmp_path: Path):
    clock = FakeClock()
    minter = FakeMinter(clock)
    gate = FakeGate()
    caps = CapabilityRegistry(tmp_path / "caps", tmp_path / "broker.sock", REPO, volatile_ok=True)
    broker = LocalCredentialBroker(
        gate=gate, minter=minter, clock=clock, capabilities=caps, repository=REPO
    )
    return broker, gate, minter, clock


async def _enable(broker: LocalCredentialBroker, sid: str, profile: CredentialProfile) -> str:
    record = await broker.provision(sid)
    secret = read_capability_file(record.path).secret
    outcome = await broker.execute(_intent(EffectKind.ENABLE_ISSUANCE, sid, profile.value), CTX)
    assert isinstance(outcome, Ack)
    return secret


def test_broker_satisfies_port(setup) -> None:
    broker, *_ = setup
    assert isinstance(broker, CredentialBroker)
    assert broker.handled_kinds == {EffectKind.ENABLE_ISSUANCE, EffectKind.DISABLE_ISSUANCE}


@pytest.mark.asyncio
async def test_issuance_denied_by_default_even_with_valid_capability(setup) -> None:
    broker, gate, minter, _ = setup
    record = await broker.provision("S1")
    gate.open("S1", CredentialProfile.BUILD)
    secret = read_capability_file(record.path).secret
    result = await broker.request_token("S1", secret, REPO)
    assert result == TokenRefusal("issuance-disabled")
    assert minter.minted == []


@pytest.mark.asyncio
async def test_capability_file_is_private_and_secret_not_in_hash(setup, tmp_path: Path) -> None:
    broker, *_ = setup
    record = await broker.provision("S1")
    assert stat.S_IMODE(record.path.stat().st_mode) == 0o600
    assert stat.S_IMODE(record.path.parent.stat().st_mode) == 0o700
    secret = read_capability_file(record.path).secret
    assert secret not in record.secret_sha256
    os.chmod(record.path, 0o644)
    with pytest.raises(Exception, match="not private"):
        read_capability_file(record.path)


@pytest.mark.asyncio
async def test_profile_permissions_are_fixed_and_requested_exactly(setup) -> None:
    broker, gate, minter, _ = setup
    secret = await _enable(broker, "PLAN", CredentialProfile.READ_ONLY)
    gate.open("PLAN", CredentialProfile.READ_ONLY)
    grant = await broker.request_token("PLAN", secret, REPO)
    assert isinstance(grant, TokenGrant)
    assert grant.profile == CredentialProfile.READ_ONLY
    assert minter.minted[-1][1] == PROFILE_PERMISSIONS[CredentialProfile.READ_ONLY]
    assert all(v == "read" for v in minter.minted[-1][1].values())
    build = PROFILE_PERMISSIONS[CredentialProfile.BUILD]
    assert build["contents"] == "write" and "workflows" not in build
    assert "administration" not in build and "organization_projects" not in build


@pytest.mark.asyncio
async def test_refusals_wrong_repo_bad_secret_and_closed_gate(setup) -> None:
    broker, gate, minter, _ = setup
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    assert await broker.request_token("S1", secret, "other/repo") == TokenRefusal(
        "wrong-repository"
    )
    assert await broker.request_token("S1", "guess", REPO) == TokenRefusal("invalid-capability")
    assert await broker.request_token("S2", secret, REPO) == TokenRefusal("invalid-capability")
    gate.close("S1", "fenced:safety")
    assert await broker.request_token("S1", secret, REPO) == TokenRefusal("fenced:safety")
    gate.open("S1", CredentialProfile.READ_ONLY)
    assert await broker.request_token("S1", secret, REPO) == TokenRefusal("profile-mismatch")
    assert minter.minted == []


@pytest.mark.asyncio
async def test_warm_cache_still_rechecks_gate_and_refreshes_before_expiry(setup) -> None:
    broker, gate, minter, clock = setup
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    first = await broker.request_token("S1", secret, REPO)
    second = await broker.request_token("S1", secret, REPO)
    assert isinstance(first, TokenGrant) and isinstance(second, TokenGrant)
    assert first.token == second.token and len(minter.minted) == 1
    # Warm cache is not authority: a fence refuses the very next call.
    gate.close("S1", "stopped")
    assert await broker.request_token("S1", secret, REPO) == TokenRefusal("stopped")
    gate.open("S1", CredentialProfile.BUILD)
    # Refresh shortly before expiry (default 5 min margin).
    clock.advance(HOUR_US - 4 * 60 * 1_000_000)
    third = await broker.request_token("S1", secret, REPO)
    assert isinstance(third, TokenGrant) and third.token != first.token
    assert len(minter.minted) == 2


@pytest.mark.asyncio
async def test_disable_refuses_and_revokes_cached_token(setup) -> None:
    broker, gate, minter, _ = setup
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    grant = await broker.request_token("S1", secret, REPO)
    assert isinstance(grant, TokenGrant)
    outcome = await broker.execute(_intent(EffectKind.DISABLE_ISSUANCE, "S1"), CTX)
    assert isinstance(outcome, Ack)
    assert minter.revoked == [grant.token]
    assert not broker.issuance_enabled("S1")
    assert await broker.request_token("S1", secret, REPO) == TokenRefusal("issuance-disabled")


@pytest.mark.asyncio
async def test_broader_minted_token_is_revoked_and_refused(setup) -> None:
    broker, gate, minter, _ = setup
    minter.broaden = True
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    result = await broker.request_token("S1", secret, REPO)
    assert result == TokenRefusal("minted-token-broader-than-profile")
    assert minter.revoked == [minter.minted[0][0]]


@pytest.mark.asyncio
async def test_fence_landing_during_mint_revokes_and_refuses(setup) -> None:
    broker, gate, minter, _ = setup
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    minter.on_mint = lambda: gate.close("S1", "fenced:revoked")
    result = await broker.request_token("S1", secret, REPO)
    assert result == TokenRefusal("fenced:revoked")
    assert minter.revoked == [minter.minted[0][0]]


@pytest.mark.asyncio
async def test_mint_failure_is_refusal_not_owner_fallback(setup) -> None:
    broker, gate, minter, _ = setup
    minter.fail = "installation suspended"
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    result = await broker.request_token("S1", secret, REPO)
    assert isinstance(result, TokenRefusal) and "installation suspended" in result.reason


@pytest.mark.asyncio
async def test_rotation_invalidates_previous_capability(setup) -> None:
    broker, gate, _, _ = setup
    old = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    rotated = await broker.provision("S1")
    new = read_capability_file(rotated.path).secret
    assert rotated.generation == 2
    assert await broker.request_token("S1", old, REPO) == TokenRefusal("invalid-capability")
    assert isinstance(await broker.request_token("S1", new, REPO), TokenGrant)
    await broker.retire("S1")
    assert not rotated.path.exists()
    assert await broker.request_token("S1", new, REPO) == TokenRefusal("invalid-capability")


@pytest.mark.asyncio
async def test_enable_requires_provisioned_capability_and_known_profile(setup) -> None:
    broker, *_ = setup
    missing = await broker.execute(_intent(EffectKind.ENABLE_ISSUANCE, "S9", "build"), CTX)
    assert isinstance(missing, DefinitiveFailure)
    await broker.provision("S9")
    bogus = await broker.execute(_intent(EffectKind.ENABLE_ISSUANCE, "S9", "admin"), CTX)
    assert isinstance(bogus, DefinitiveFailure)
    assert not broker.issuance_enabled("S9")


@pytest.mark.asyncio
async def test_audit_never_contains_token_or_secret(setup) -> None:
    broker, gate, _, _ = setup
    secret = await _enable(broker, "S1", CredentialProfile.BUILD)
    gate.open("S1", CredentialProfile.BUILD)
    grant = await broker.request_token("S1", secret, REPO)
    assert isinstance(grant, TokenGrant)
    await broker.request_token("S1", "bad", REPO)
    flat = repr(broker.audit.entries)
    assert grant.token not in flat and secret not in flat
