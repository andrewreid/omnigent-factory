"""GitHub App JWT creation and least-privilege installation-token minting."""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from omnigent_factory.core.effects import CredentialProfile
from omnigent_factory.ports.credentials import TokenGrant, TokenRefusal

_READ_ONLY_PERMISSIONS: Mapping[str, str] = {
    "actions": "read",
    "checks": "read",
    "contents": "read",
    "issues": "read",
    "metadata": "read",
    "pull_requests": "read",
    "statuses": "read",
}
_BUILD_PERMISSIONS: Mapping[str, str] = {
    **_READ_ONLY_PERMISSIONS,
    "actions": "write",
    "contents": "write",
    "issues": "write",
    "pull_requests": "write",
    "workflows": "write",
}
_DAEMON_PERMISSIONS: Mapping[str, str] = {
    **_READ_ONLY_PERMISSIONS,
    "issues": "write",
    "pull_requests": "write",
    "organization_projects": "write",
}


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


@dataclass(frozen=True, slots=True)
class AppAuthenticator:
    app_id: int
    private_key_pem: bytes
    now: Callable[[], float] = time.time

    def jwt(self) -> str:
        """Return a short-lived RS256 App JWT with clock-skew allowance."""
        issued = int(self.now())
        header = _b64url(b'{"alg":"RS256","typ":"JWT"}')
        payload = _b64url(
            json.dumps(
                {"iat": issued - 60, "exp": issued + 9 * 60, "iss": str(self.app_id)},
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        unsigned = f"{header}.{payload}".encode("ascii")
        key = serialization.load_pem_private_key(self.private_key_pem, password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ValueError("GitHub App private key must be RSA")
        signature = key.sign(unsigned, padding.PKCS1v15(), hashes.SHA256())
        return f"{unsigned.decode()}.{_b64url(signature)}"


@dataclass(frozen=True, slots=True)
class DaemonToken:
    """Daemon installation credential with explicit write permissions, not a stage grant."""

    token: str
    repository: str
    expires_at_us: int
    permissions: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class InstallationTokenService:
    """Mint tokens constrained to one configured installation repository."""

    client: httpx.AsyncClient
    authenticator: AppAuthenticator
    installation_id: int
    repository: str
    api_url: str = "https://api.github.com"

    async def mint(
        self, requested_repository: str, profile: CredentialProfile
    ) -> TokenGrant | TokenRefusal:
        permissions = (
            _BUILD_PERMISSIONS if profile == CredentialProfile.BUILD else _READ_ONLY_PERMISSIONS
        )
        return await self._mint(requested_repository, profile, permissions)

    async def mint_daemon(self) -> DaemonToken | TokenRefusal:
        """Mint the daemon's board/comment credential without stage contents-write."""
        result = await self._mint(self.repository, CredentialProfile.READ_ONLY, _DAEMON_PERMISSIONS)
        if isinstance(result, TokenRefusal):
            return result
        return DaemonToken(
            result.token,
            result.repository,
            result.expires_at_us,
            dict(_DAEMON_PERMISSIONS),
        )

    async def _mint(
        self,
        requested_repository: str,
        profile: CredentialProfile,
        permissions: Mapping[str, str],
    ) -> TokenGrant | TokenRefusal:
        if requested_repository != self.repository:
            return TokenRefusal("repository is outside the configured installation scope")
        owner, separator, repo = self.repository.partition("/")
        if not separator or not owner or not repo:
            return TokenRefusal("configured repository must be owner/name")
        try:
            response = await self.client.post(
                f"{self.api_url}/app/installations/{self.installation_id}/access_tokens",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {self.authenticator.jwt()}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                json={"repositories": [repo], "permissions": dict(permissions)},
            )
        except httpx.HTTPError as exc:
            return TokenRefusal(f"installation token request failed: {type(exc).__name__}")
        if response.status_code != 201:
            return TokenRefusal(f"installation token request rejected: HTTP {response.status_code}")
        data: Any = response.json()
        token = data.get("token") if isinstance(data, dict) else None
        expires_at = data.get("expires_at") if isinstance(data, dict) else None
        if not isinstance(token, str) or not isinstance(expires_at, str):
            return TokenRefusal("installation token response was malformed")
        try:
            expiry_us = int(
                datetime.fromisoformat(expires_at.replace("Z", "+00:00")).timestamp() * 1e6
            )
        except ValueError:
            return TokenRefusal("installation token expiry was malformed")
        returned_permissions = data.get("permissions")
        if not isinstance(returned_permissions, dict):
            return TokenRefusal("installation token response omitted permission scope")
        if returned_permissions != dict(permissions):
            return TokenRefusal("installation token permissions differ from the requested scope")
        returned_repositories = data.get("repositories")
        if not isinstance(returned_repositories, list):
            return TokenRefusal("installation token response omitted repository scope")
        names = {item.get("full_name") for item in returned_repositories if isinstance(item, dict)}
        if names != {self.repository}:
            return TokenRefusal("installation token repository scope differs from request")
        return TokenGrant(token, profile, self.repository, expiry_us)

    async def revoke(self, token: str) -> bool:
        """Best-effort revocation of a cached installation token."""
        try:
            response = await self.client.delete(
                f"{self.api_url}/installation/token",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
        except httpx.HTTPError:
            return False
        return response.status_code == 204
