"""Store-backed execution gate and GitHub App token-minter bridge."""

from __future__ import annotations

from collections.abc import Mapping

from omnigent_factory.core.effects import CredentialProfile, profile_for
from omnigent_factory.core.predicates import (
    authority_ok,
    board_pending,
    message_uncertain,
    work_allowed,
)
from omnigent_factory.core.types import MICROS_PER_MINUTE, Lifecycle, Parcel
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
from omnigent_factory.store import ranking as rs


class StoreExecutionGate:
    """Re-evaluate core authority from the current aggregate on every token request."""

    def __init__(self, db: StoreWorker, clock: Clock) -> None:
        self.db = db
        self.clock = clock

    async def token_gate(self, session_id: str) -> GateDecision:
        parcel = await self._parcel(session_id)
        if parcel is None:
            return await self._ranking_gate(session_id)
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

    async def _ranking_gate(self, run_id: str) -> GateDecision:
        run = await self.db.call(lambda store: rs.get_run(store, run_id))
        if run is None:
            return GateDecision(False, "unknown-session")
        now = self.clock.now_utc_us()
        # A run is durable authority; the broker separately requires verified policies
        # and an enabled capability. Every issuance, including cache hits, checks here.
        ready = (
            run.state in ("sending", "running")
            and run.root_id is not None
            and run.policy_ready_at_us is not None
            and now >= run.policy_ready_at_us
            and run.finished_at_us is None
            and run.archived_at_us is None
        )
        deadline = (
            run.created_at_us + 15 * MICROS_PER_MINUTE
            if run.state == "sending"
            else (run.started_at_us or run.created_at_us) + 30 * MICROS_PER_MINUTE
        )
        return GateDecision(
            ready and now < deadline, "ranking-work-gate", CredentialProfile.READ_ONLY
        )

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
