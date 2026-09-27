"""Read-only deployment diagnostics for the production composition."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import socket
import stat
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import httpx

from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubClient
from omnigent_factory.omnigent.rest import FileTokenAuth, OmnigentReadError, OmnigentRest
from omnigent_factory.ports.credentials import TokenRefusal
from omnigent_factory.service.composition import _private_file
from omnigent_factory.service.config import ServiceConfig


@dataclass(slots=True)
class DoctorReport:
    ok: bool = True
    checks: dict[str, str] = field(default_factory=dict)
    resolved: dict[str, str | int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def pass_check(self, name: str, detail: str = "ok") -> None:
        self.checks[name] = detail

    def fail(self, name: str, detail: str) -> None:
        self.ok = False
        self.checks[name] = "failed"
        self.errors.append(f"{name}: {detail}")

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


async def run_doctor(
    config: ServiceConfig,
    *,
    github_transport: httpx.AsyncBaseTransport | None = None,
    omnigent_transport: httpx.AsyncBaseTransport | None = None,
) -> DoctorReport:
    """Perform live reads only; print-worthy IDs are returned under ``resolved``."""
    report = DoctorReport()
    try:
        key = _private_file(config.resolved_app_private_key_file)
        _private_file(config.resolved_webhook_secret_file)
        _private_file(config.resolved_omnigent_token_file)
        if config.app_env.exists():
            _private_file(config.app_env)
        report.pass_check("secrets", "all required files are owned by this user and mode 0600")
    except (OSError, ValueError, RuntimeError) as exc:
        report.fail("secrets", str(exc))
        return report

    github_http = httpx.AsyncClient(transport=github_transport, timeout=15.0)
    omnigent = OmnigentRest(
        config.omnigent_base_url,
        auth=FileTokenAuth(config.resolved_omnigent_token_file),
        transport=omnigent_transport,
    )
    tokens: InstallationTokenService | None = None
    minted_token: str | None = None
    try:
        auth = AppAuthenticator(config.github_app_id, key)
        jwt = auth.jwt()
        report.pass_check("github_app_jwt", "RS256 App JWT generated")
        tokens = InstallationTokenService(
            github_http,
            auth,
            config.github_installation_id,
            config.repository,
            config.github_api_url,
        )
        daemon = await tokens.mint_daemon()
        if isinstance(daemon, TokenRefusal):
            report.fail("github_installation_token", daemon.reason)
        else:
            minted_token = daemon.token
            report.pass_check("github_installation_token", "repository-scoped token minted")
            client = GitHubClient(github_http, daemon.token, api_url=config.github_api_url)
            await _check_github(config, client, jwt, report)
        await _check_omnigent(config, omnigent, report)
    except (httpx.HTTPError, ValueError, RuntimeError) as exc:
        report.fail("live_checks", f"{type(exc).__name__}: {exc}")
    finally:
        if tokens is not None and minted_token is not None:
            if await tokens.revoke(minted_token):
                report.pass_check("github_token_revoke", "diagnostic token revoked")
            else:
                report.fail("github_token_revoke", "diagnostic token revocation failed")
        await omnigent.aclose()
        await github_http.aclose()

    await asyncio.to_thread(_check_clone, config, report)
    _check_socket(config, report)
    await asyncio.to_thread(_check_bind, config, report)
    return report


async def _check_github(
    config: ServiceConfig, client: GitHubClient, jwt: str, report: DoctorReport
) -> None:
    del jwt
    repositories = await client.get_json("/installation/repositories?per_page=100")
    rows = repositories.get("repositories") if isinstance(repositories, dict) else None
    if not isinstance(rows, list):
        report.fail("github_repository_scope", "installation repositories response malformed")
        return
    names = [row.get("full_name") for row in rows if isinstance(row, dict)]
    total = repositories.get("total_count") if isinstance(repositories, dict) else None
    if names != [config.repository] or total != 1:
        report.fail("github_repository_scope", f"token can see {names!r}, expected one repository")
        return
    repository = rows[0]
    repo_id = repository.get("id")
    repo_node = repository.get("node_id")
    owner = repository.get("owner")
    owner_id = owner.get("id") if isinstance(owner, dict) else None
    if (
        not isinstance(repo_id, int)
        or not isinstance(repo_node, str)
        or not isinstance(owner_id, int)
    ):
        report.fail("github_repository_identity", "repository identity fields are missing")
        return
    report.resolved.update(
        repository_database_id=repo_id, repo_id=repo_node, organization_id=owner_id
    )
    if config.repository_database_id not in (None, repo_id) or config.repo_id != repo_node:
        report.fail("github_repository_identity", "configured repository IDs differ from GitHub")
    else:
        report.pass_check("github_repository_identity")
    factory = await client.default_branch_config(config.repository)
    report.pass_check(
        "factory_yml",
        "parsed; checkpoint blocks "
        + json.dumps(factory.checkpoints.block_hours.model_dump(), sort_keys=True),
    )
    await _check_project(config, client, report)


async def _check_project(config: ServiceConfig, client: GitHubClient, report: DoctorReport) -> None:
    query = """
    query($id: ID!) { node(id: $id) { ... on ProjectV2 { id fields(first: 100) {
      nodes { ... on ProjectV2SingleSelectField { id name options { id name } } }
    } } } }
    """
    data = await client.graphql(query, {"id": config.project_node_id})
    node = data.get("node")
    fields = node.get("fields") if isinstance(node, dict) else None
    rows = fields.get("nodes") if isinstance(fields, dict) else None
    if not isinstance(rows, list):
        report.fail("github_project", "project fields unavailable")
        return
    by_id = {row.get("id"): row for row in rows if isinstance(row, dict)}
    for name, field_id, expected in (
        ("Status", config.status_field_node_id, config.status_options),
        ("Bot", config.bot_field_node_id, config.bot_options),
    ):
        field_value = by_id.get(field_id)
        options = field_value.get("options") if isinstance(field_value, dict) else None
        observed = {
            str(option.get("name")): str(option.get("id"))
            for option in options or []
            if isinstance(option, dict)
        }
        if not isinstance(field_value, dict) or field_value.get("name") != name:
            report.fail("github_project", f"{name} field ID does not match")
            return
        if observed != expected:
            report.fail("github_project", f"{name} option IDs differ from configuration")
            return
    report.pass_check("github_project", "project and live field/option IDs match")


async def _check_omnigent(config: ServiceConfig, rest: OmnigentRest, report: DoctorReport) -> None:
    try:
        reached = False
        for singular, path, configured_id, configured_name in (
            ("host", "/v1/hosts", config.omnigent_host_id, config.omnigent_host_name),
            ("agent", "/v1/agents", config.omnigent_agent_id, config.omnigent_agent_name),
            ("project", "/v1/projects", config.omnigent_project_id, config.omnigent_project_name),
        ):
            rows = await _identity_rows(rest, path)
            reached = True
            matches = [
                row
                for row in rows
                if row.get("id") == configured_id
                or (configured_id is None and row.get("name") == configured_name)
            ]
            if len(matches) != 1 or not isinstance(matches[0].get("id"), str):
                report.fail(
                    f"omnigent_{singular}", "configured identity is not uniquely resolvable"
                )
                continue
            resolved = str(matches[0]["id"])
            report.resolved[f"omnigent_{singular}_id"] = resolved
            report.pass_check(f"omnigent_{singular}", resolved)
        if reached:
            report.pass_check("omnigent_reachable", "authenticated API reads succeeded")
    except OmnigentReadError as exc:
        report.fail("omnigent_auth", exc.reason)


async def _identity_rows(rest: OmnigentRest, path: str) -> list[dict[str, Any]]:
    """Rows keyed by ``id``, accepting both live list shapes.

    ``/v1/agents`` and ``/v1/projects`` return the cursor envelope ``{data, has_more}``;
    the live ``/v1/hosts`` returns ``{hosts: [...]}`` with rows keyed by ``host_id``.
    """
    body = await rest.get_json(path)
    if "data" in body:
        rows = await rest.paginate(path) if body.get("has_more") else body["data"]
    else:
        rows = body.get(path.rsplit("/", 1)[-1])
    if not isinstance(rows, list):
        raise OmnigentReadError(f"GET {path}: missing row list")
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if "id" not in row and "host_id" in row:
            normalized.append({**row, "id": row["host_id"]})
        else:
            normalized.append(row)
    return normalized


def _check_clone(config: ServiceConfig, report: DoctorReport) -> None:
    if (
        not config.real_gh_path.is_absolute()
        or not config.real_gh_path.is_file()
        or not os.access(config.real_gh_path, os.X_OK)
    ):
        report.fail("gh_binary", "configured real gh path is not an executable file")
    else:
        report.pass_check("gh_binary", str(config.real_gh_path))
    if not (config.source_clone / ".git").is_dir():
        report.fail("build_clone", "dedicated ordinary clone is absent")
        return
    commands = (
        ("remote", "get-url", "origin"),
        ("status", "--porcelain=v1"),
        ("branch", "--show-current"),
    )
    outputs: list[str] = []
    for args in commands:
        proc = subprocess.run(  # noqa: S603 - fixed read-only git argv
            ("git", "-C", str(config.source_clone), *args),  # noqa: S607
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"},
        )
        if proc.returncode:
            report.fail("build_clone", proc.stderr.strip() or "git inspection failed")
            return
        outputs.append(proc.stdout.strip())
    expected = f"https://github.com/{config.repository}.git"
    if outputs[0] != expected or outputs[1]:
        report.fail("build_clone", "origin differs or clone is dirty")
    else:
        report.pass_check("build_clone", f"clean on {outputs[2] or '(detached)'}")


def _check_socket(config: ServiceConfig, report: DoctorReport) -> None:
    parent = config.broker_socket.parent
    if parent.exists() and stat.S_IMODE(parent.stat().st_mode) != 0o700:
        report.fail("broker_socket", "socket parent is accessible by group/other")
    elif (
        config.broker_socket.exists() and stat.S_IMODE(config.broker_socket.stat().st_mode) != 0o600
    ):
        report.fail("broker_socket", "existing socket is not mode 0600")
    else:
        report.pass_check("broker_socket", "path is absent or private")


def _check_bind(config: ServiceConfig, report: DoctorReport) -> None:
    try:
        socket.getaddrinfo(config.bind_host, config.bind_port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        report.fail("bind_address", str(exc))
        return
    try:
        address = ipaddress.ip_address(config.bind_host)
    except ValueError:
        address = None
    if address is not None and not address.is_loopback:
        try:
            routes = Path("/proc/net/fib_trie").read_text(encoding="utf-8")
        except OSError as exc:
            report.fail("bind_address", f"cannot inspect local addresses: {exc}")
            return
        if config.bind_host not in routes:
            report.fail("bind_address", "address is not assigned to this host")
            return
    report.pass_check("bind_address", f"resolves to {config.bind_host}:{config.bind_port}")
