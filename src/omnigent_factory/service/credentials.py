"""Store-backed execution gate and GitHub App token-minter bridge."""

from __future__ import annotations

from collections.abc import Mapping

from omnigent_factory.core.effects import profile_for
from omnigent_factory.core.predicates import (
    authority_ok,
    board_pending,
    message_uncertain,
    work_allowed,
)
from omnigent_factory.core.types import Lifecycle, Parcel
from omnigent_factory.credentials.broker import (
    PROFILE_PERMISSIONS,
    GateDecision,
    MintedToken,
    MintFailure,
)
from omnigent_factory.github.auth import InstallationTokenService
from omnigent_factory.ports.clock import Clock
from omnigent_factory.ports.credentials import TokenRefusal
from omnigent_factory.service.db import StoreWorker


class StoreExecutionGate:
    """Re-evaluate core authority from the current aggregate on every token request."""

    def __init__(self, db: StoreWorker, clock: Clock) -> None:
        self.db = db
        self.clock = clock

    async def token_gate(self, session_id: str) -> GateDecision:
        parcel = await self._parcel(session_id)
        if parcel is None:
            return GateDecision(False, "unknown-session")
        session = parcel.session(session_id)
        if session is None or not session.prepared or session.execution_closed:
            return GateDecision(False, "session-not-prepared")
        profile = profile_for(session.kind)
        if work_allowed(parcel, session):
            return GateDecision(True, profile=profile)
        deadline = session.grant.grace_deadline_us
        cleanup = (
            session.session_id == parcel.current_session_id
            and session.lifecycle == Lifecycle.CHECKPOINT_GRACE
            and not session.fences
            and deadline is not None
            and self.clock.now_utc_us() < deadline
            and authority_ok(parcel, session)
            and not message_uncertain(parcel, session)
            and not board_pending(parcel)
        )
        if cleanup:
            return GateDecision(True, "checkpoint-cleanup", profile)
        return GateDecision(False, "core-work-gate-closed", profile)

    async def _parcel(self, session_id: str) -> Parcel | None:
        rows = await self.db.call(
            lambda store: store.query(
                "SELECT p.parcel_id FROM parcels p JOIN stage_sessions s "
                "ON s.parcel_id = p.parcel_id WHERE s.session_id = ?",
                (session_id,),
            )
        )
        if len(rows) != 1:
            return None
        return await self.db.call(lambda store: store.load_parcel(str(rows[0][0])))


class AppInstallationTokenMinter:
    """Adapt the App installation service to the broker's fixed-permission protocol."""

    def __init__(self, service: InstallationTokenService) -> None:
        self.service = service

    async def mint(
        self, repository: str, permissions: Mapping[str, str]
    ) -> MintedToken | MintFailure:
        profile = next(
            (
                candidate
                for candidate, fixed in PROFILE_PERMISSIONS.items()
                if dict(fixed) == dict(permissions)
            ),
            None,
        )
        if profile is None:
            return MintFailure("permission profile is not factory-approved")
        result = await self.service.mint(repository, profile)
        if isinstance(result, TokenRefusal):
            return MintFailure(result.reason)
        return MintedToken(
            result.token,
            result.expires_at_us,
            dict(PROFILE_PERMISSIONS[profile]),
            (result.repository,),
        )

    async def revoke(self, token: str) -> bool:
        return await self.service.revoke(token)
