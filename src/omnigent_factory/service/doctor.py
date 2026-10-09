"""Read-only deployment diagnostics for the production composition."""

from __future__ import annotations

import asyncio
import importlib.metadata
import io
import ipaddress
import os
import socket
import stat
import subprocess
import tarfile
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import httpx
import yaml

from omnigent_factory.github.auth import AppAuthenticator, InstallationTokenService
from omnigent_factory.github.client import GitHubAPIError, GitHubClient
from omnigent_factory.omnigent.rest import (
    OmnigentReadError,
    OmnigentRest,
    WriteClass,
    classify_write,
)
from omnigent_factory.ports.credentials import TokenRefusal
from omnigent_factory.service.composition import _private_file
from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.mcp import TOOL_NAMES
from omnigent_factory.service.omnigent_auth import expiry, expiry_message, omnigent_auth


@dataclass(slots=True)
class DoctorReport:
    ok: bool = True
    checks: dict[str, str] = field(default_factory=dict)
    resolved: dict[str, str | int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    #: Advisory findings; never change ``ok``.
    warnings: list[str] = field(default_factory=list)

    def pass_check(self, name: str, detail: str = "ok") -> None:
        self.checks[name] = detail

    def warn(self, name: str, detail: str) -> None:
        self.checks[name] = f"warning: {detail}"
        self.warnings.append(f"{name}: {detail}")

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
    mcp_transport: httpx.AsyncBaseTransport | None = None,
    live: bool = False,
) -> DoctorReport:
    """Perform live reads only; print-worthy IDs are returned under ``resolved``.

    ``live`` (opt-in) also creates a throwaway session for the configured agent in the
    configured project and archives it: server-side create failures (e.g. an agent env
    variable the server cannot resolve) only show up then.
    """
    report = DoctorReport()
    try:
        key = _private_file(config.resolved_app_private_key_file)
        _private_file(config.resolved_webhook_secret_file)
        if config.omnigent_cli_store is None:
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
        auth=omnigent_auth(config),
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
            await _check_push_events(config, github_http, jwt, report)
        await _check_omnigent(config, omnigent, report)
        probe = await _check_live_session(config, omnigent, report) if live else None
        await _check_agent_factory_tools(config, omnigent, report, session_id=probe)
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

    _check_login_expiry(config, report)
    await asyncio.to_thread(_check_clone, config, report)
    _check_socket(config, report)
    await asyncio.to_thread(_check_bind, config, report)
    _check_mcp_token(config, report)
    _check_caller_policy(report)
    await _check_mcp_listener(config, report, transport=mcp_transport)
    return report


def _check_mcp_token(config: ServiceConfig, report: DoctorReport) -> None:
    path = config.resolved_mcp_token_file
    try:
        token = _private_file(path)
    except (OSError, RuntimeError) as exc:
        report.fail("mcp_token", f"{exc} (create it with `omnigent-factory setup mcp-token`)")
        return
    if len(token.strip()) < 32:
        report.fail("mcp_token", "token file is too short")
        return
    report.pass_check("mcp_token", f"{path} is private (mode 0600)")


def _check_caller_policy(report: DoctorReport) -> None:
    """Dry compile + exercise the per-session caller-identity CEL policy."""
    from omnigent.policies.builtins.cel import cel_policy  # noqa: PLC0415

    from omnigent_factory.omnigent.policies import caller_policy  # noqa: PLC0415

    try:
        spec = caller_policy("conv_doctor_probe")
        check = cel_policy(**spec.factory_params)
    except (ValueError, ImportError) as exc:
        report.fail("caller_policy", f"identity policy does not compile: {exc}")
        return

    def verdict(name: str, arguments: object) -> str | None:
        out = check({"type": "tool_call", "data": {"name": name, "arguments": arguments}})
        return str(out.get("result")) if isinstance(out, dict) else None

    expected = [
        (verdict("factory__factory_get_status", {"session_id": "conv_doctor_probe"}), "ALLOW"),
        (verdict("factory__factory_get_status", {"session_id": "conv_other"}), "DENY"),
        (verdict("mcp__factory__factory_submit_result", {}), "DENY"),
        (verdict("factory_get_plan", "not-a-map"), "DENY"),
        (verdict("sys_os_shell", {"command": "ls"}), "ALLOW"),
    ]
    if any(got != want for got, want in expected):
        report.fail("caller_policy", f"identity policy verdicts wrong: {expected}")
        return
    report.pass_check("caller_policy", "compiles; binds factory tools to the calling session")


async def _check_mcp_listener(
    config: ServiceConfig,
    report: DoctorReport,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> None:
    """The loopback /mcp listener answers (401 without a token proves the gate is up).

    Advisory: doctor also runs before the daemon is first started.
    """
    url = f"http://127.0.0.1:{config.mcp_port}/mcp/"
    try:
        async with httpx.AsyncClient(transport=transport, timeout=5.0) as client:
            resp = await client.post(url, json={})
    except httpx.HTTPError as exc:
        report.warn(
            "mcp_listener", f"{url} unreachable ({type(exc).__name__}); daemon not running?"
        )
        return
    if resp.status_code in (401, 503):
        report.pass_check("mcp_listener", f"{url} is up and requires the bearer token")
    else:
        report.fail("mcp_listener", f"{url} answered HTTP {resp.status_code} without a token")


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
    await _check_project(config, client, report)


async def _check_push_events(
    config: ServiceConfig, http: httpx.AsyncClient, jwt: str, report: DoctorReport
) -> None:
    """Warn when the App installation does not receive ``push``: a move of the default
    branch then re-checks open bot PRs for merge conflicts only at the periodic reads.

    Skipped silently whenever the installation's subscribed events cannot be read.
    """
    try:
        response = await http.get(
            f"{config.github_api_url}/app/installations/{config.github_installation_id}",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {jwt}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        data: Any = response.json() if response.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        return
    events = data.get("events") if isinstance(data, dict) else None
    if not isinstance(events, list) or not all(isinstance(e, str) for e in events):
        return
    if "push" in events:
        report.pass_check("github_push_events", "installation receives push")
        return
    report.warn(
        "github_push_events",
        "the App installation is not subscribed to push: tick Push under Subscribe to events "
        "in the GitHub App settings (and accept the change on the installation), so a push "
        "to the default branch re-checks open bot PRs for merge conflicts",
    )


async def _check_project(config: ServiceConfig, client: GitHubClient, report: DoctorReport) -> None:
    query = """
    query($id: ID!) { node(id: $id) { ... on ProjectV2 { id fields(first: 100) {
      nodes {
        ... on ProjectV2SingleSelectField { id name options { id name } }
        ... on ProjectV2Field { id name dataType }
      }
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
    observed: dict[str, list[tuple[str, str]]] = {}
    for name, field_id in (
        ("Status", config.status_field_node_id),
        ("Bot", config.bot_field_node_id),
    ):
        field_value = by_id.get(field_id)
        if not isinstance(field_value, dict) or field_value.get("name") != name:
            report.fail("github_project", f"{name} field ID does not match")
            return
        options = field_value.get("options")
        observed[name] = [
            (str(option.get("name")), str(option.get("id")))
            for option in options or []
            if isinstance(option, dict)
        ]
    if dict(observed["Bot"]) != config.bot_options:
        report.fail("github_project", "Bot option IDs differ from configuration")
        return
    # Status is keyed by option ID; names are display only (checked separately below).
    live_status = {option_id: name for name, option_id in observed["Status"]}
    configured = set(config.status_options.values())
    if set(live_status) != configured:
        missing = sorted(configured - set(live_status))
        extra = sorted(set(live_status) - configured)
        report.fail(
            "github_project",
            f"Status option IDs differ from configuration (missing {missing}, unexpected {extra})",
        )
        return
    _check_status_names(config, live_status, report)
    note = by_id.get(config.note_field_node_id)
    if (
        not isinstance(note, dict)
        or note.get("name") != "Factory note"
        or note.get("dataType") != "TEXT"
    ):
        report.fail("github_project", "Factory note text field ID does not match")
        return
    report.pass_check("github_project", "project and live field/option IDs match")
    ranking_set_up = config.ranking or bool(config.rank_field_node_id)
    if ranking_set_up:
        _check_rank_field(config, by_id, report)
    if ranking_set_up and config.ranking_status_update:
        await _check_status_updates(config, client, report)


def _check_rank_field(config: ServiceConfig, by_id: dict[Any, Any], report: DoctorReport) -> None:
    """The triage ranking's "Rank" NUMBER field (``rank_field_node_id``)."""
    if not config.rank_field_node_id:
        report.warn(
            "github_rank_field",
            "rank_field_node_id is not set: triage ranking cannot run (create the Rank "
            "NUMBER field, see `setup render`, and record its node ID)",
        )
        return
    rank = by_id.get(config.rank_field_node_id)
    if not isinstance(rank, dict) or rank.get("name") != "Rank" or rank.get("dataType") != "NUMBER":
        report.fail("github_rank_field", "Rank number field ID does not match")
        return
    report.pass_check("github_rank_field", "Rank number field ID matches")


async def _check_status_updates(
    config: ServiceConfig, client: GitHubClient, report: DoctorReport
) -> None:
    """Advisory: the App can read project status updates (the ranking posts one; without
    access it posts no summary and logs it)."""
    query = """
    query($id: ID!) { node(id: $id) { ... on ProjectV2 {
      statusUpdates(first: 1) { nodes { id } }
    } } }
    """
    try:
        data = await client.graphql(query, {"id": config.project_node_id})
    except GitHubAPIError as exc:
        report.warn("github_status_updates", f"project status updates unavailable: {exc}")
        return
    node = data.get("node")
    if not isinstance(node, dict) or not isinstance(node.get("statusUpdates"), dict):
        report.warn("github_status_updates", "project status updates unavailable to the App")
        return
    report.pass_check(
        "github_status_updates", "status updates readable (creation is checked on first use)"
    )


def _check_status_names(
    config: ServiceConfig, live_status: dict[str, str], report: DoctorReport
) -> None:
    """Advisory: the configured column names match the live names of the configured IDs."""
    mismatches = [
        f"{stage} option {option_id} is {live_status[option_id]!r} on the board but "
        f"{config.status_names[stage]!r} in status_names"
        for stage, option_id in config.status_options.items()
        if live_status[option_id] != config.status_names[stage]
    ]
    if mismatches:
        report.warn("status_names", "; ".join(mismatches))
    else:
        names = ", ".join(config.status_names[stage] for stage in config.status_options)
        report.pass_check("status_names", f"configured names match the board: {names}")


def _check_login_expiry(config: ServiceConfig, report: DoctorReport) -> None:
    found = expiry_message(config)
    info = expiry(config)
    if found is None:
        detail = "no expiry known" if info is None or info.expires_at is None else "ok"
        if info is not None:
            detail = f"{info.source}{', refreshable' if info.refreshable else ''}: {detail}"
        report.pass_check("omnigent_login", detail)
        return
    level, message = found
    if level == "error":
        report.fail("omnigent_login", message)
    else:
        report.warn("omnigent_login", message)


async def _check_server_version(rest: OmnigentRest, report: DoctorReport) -> None:
    """Warn (never fail) when the server differs from the pinned client version.

    The server exposes only its package version (``/api/version``), not a commit, so a
    same-version build from another commit cannot be detected here.
    """
    try:
        body = await rest.get_json("/api/version")
    except OmnigentReadError as exc:
        report.warn("omnigent_version", f"server version unreadable: {exc.reason}")
        return
    server = body.get("version") if isinstance(body, dict) else None
    try:
        pinned = importlib.metadata.version("omnigent")
    except importlib.metadata.PackageNotFoundError:
        pinned = None
    commit = _pinned_commit()
    if not isinstance(server, str) or pinned is None:
        report.warn("omnigent_version", f"cannot compare server={server!r} pinned={pinned!r}")
    elif server != pinned:
        report.warn(
            "omnigent_version",
            f"server {server} differs from the pinned client {pinned} (commit {commit})",
        )
    else:
        report.pass_check("omnigent_version", f"{server} (pinned commit {commit})")


def _pinned_commit() -> str:
    try:
        from omnigent import _build_info  # noqa: PLC0415
    except ImportError:
        return "unknown"
    return str(getattr(_build_info, "COMMIT_SHA", "unknown"))[:12]


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
            await _check_server_version(rest, report)
    except OmnigentReadError as exc:
        report.fail("omnigent_auth", exc.reason)


async def _check_live_session(
    config: ServiceConfig, rest: OmnigentRest, report: DoctorReport
) -> str | None:
    """Create a session exactly as a stage run would (no message, no git branch) and
    archive it again; returns the session (its agent bundle stays readable)."""
    agent = report.resolved.get("omnigent_agent_id")
    host = report.resolved.get("omnigent_host_id")
    if not agent or not host:
        report.fail("live_session", "agent or host is not resolved")
        return None
    body: dict[str, Any] = {
        "agent_id": agent,
        "host_type": "external",
        "host_id": host,
        "workspace": str(config.source_clone),
        "title": "factory doctor probe (safe to delete)",
        "labels": {"factory.doctor": "probe"},
        "initial_items": [],
    }
    project = report.resolved.get("omnigent_project_id")
    if project:
        body["project_id"] = project
    created = await rest.post_json("/v1/sessions", body)
    root = (created.body or {}).get("id") or (created.body or {}).get("session_id")
    if classify_write(created) != WriteClass.OK or not isinstance(root, str) or not root:
        detail = created.error_code or _error_message(created.body) or created.error or ""
        report.fail("live_session", f"session create failed: HTTP {created.status} {detail}")
        return None
    archived = await rest.patch_json(f"/v1/sessions/{root}", {"archived": True})
    if classify_write(archived) != WriteClass.OK:
        report.fail(
            "live_session",
            f"created {root} but could not archive it: HTTP {archived.status}",
        )
        return root
    report.pass_check("live_session", f"created and archived {root}")
    return root


#: Prefixes an agent allowlist may put before a factory tool's name.
_TOOL_PREFIXES = ("mcp__factory__", "factory__")


def _bare_tool(tool: str) -> str:
    for prefix in _TOOL_PREFIXES:
        tool = tool.removeprefix(prefix)
    return tool


def missing_factory_tools(
    servers: Iterable[tuple[str, str | None, Sequence[str] | None]],
    mcp_port: int,
    served: Sequence[str] = TOOL_NAMES,
) -> list[str] | None:
    """The served factory tools the agent cannot call, from its MCP servers as
    (name, url, tool allowlist or None for every tool); None when it declares no factory
    server. The factory server is the one named ``factory``, or whose URL is the factory
    listener (``FACTORY_MCP_URL`` or the loopback ``mcp_port``)."""
    listener = (f"127.0.0.1:{mcp_port}", f"localhost:{mcp_port}", "FACTORY_MCP_URL")
    allowed: set[str] = set()
    found = False
    for name, url, tools in servers:
        if name != "factory" and not any(mark in (url or "") for mark in listener):
            continue
        found = True
        if tools is None:
            return []
        allowed.update(_bare_tool(tool) for tool in tools)
    if not found:
        return None
    return [tool for tool in served if tool not in allowed]


def _bundle_mcp_servers(bundle: bytes) -> list[tuple[str, str | None, list[str] | None]]:
    """The MCP servers an agent bundle (``.tar.gz``) declares, read without extracting
    or validating the rest of the spec: inline ``type: mcp`` entries under
    ``config.yaml``'s ``tools:`` and ``tools/mcp/*.yaml`` sidecars, as (name, url as
    written, ``tools`` allowlist; an absent or empty list allows every tool)."""
    servers: list[tuple[str, str | None, list[str] | None]] = []
    config: Any = None
    with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:*") as tar:
        for member in tar.getmembers():
            path = member.name.removeprefix("./")
            sidecar = path.startswith("tools/mcp/") and path.endswith(".yaml")
            if not member.isfile() or (path != "config.yaml" and not sidecar):
                continue
            handle = tar.extractfile(member)
            raw = yaml.safe_load(handle.read()) if handle is not None else None
            if path == "config.yaml":
                config = raw
            elif isinstance(raw, dict) and raw.get("name") is not None:
                servers.append((str(raw["name"]), _text(raw.get("url")), _allowlist(raw)))
    if not isinstance(config, dict):
        raise ValueError("the bundle has no config.yaml mapping")
    tools = config.get("tools")
    for key, value in tools.items() if isinstance(tools, dict) else ():
        if isinstance(value, dict) and str(value.get("type", "")) == "mcp":
            servers.append((str(key), _text(value.get("url")), _allowlist(value)))
    return servers


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _allowlist(raw: dict[str, Any]) -> list[str] | None:
    """Omnigent's per-server ``tools:`` rule: a non-empty list restricts, else all."""
    tools = raw.get("tools")
    return [str(t) for t in tools] if isinstance(tools, list) and tools else None


async def _agent_session(rest: OmnigentRest, agent_id: str) -> str | None:
    """The newest session of the configured agent (the agent bundle is read through a
    session), archived ones included."""
    rows = await rest.paginate(
        "/v1/sessions",
        {"kind": "default", "include_archived": "true", "visibility": "all", "order": "desc"},
        until=lambda row: row.get("agent_id") == agent_id,
    )
    for row in rows:
        if row.get("agent_id") == agent_id and isinstance(row.get("id"), str):
            return str(row["id"])
    return None


async def _check_agent_factory_tools(
    config: ServiceConfig,
    rest: OmnigentRest,
    report: DoctorReport,
    *,
    session_id: str | None = None,
) -> None:
    """Every tool the factory MCP server serves is one the configured agent may call (an
    agent allowlist without e.g. ``factory_submit_ranking`` fails ranking silently)."""
    name = "agent_factory_tools"
    agent = report.resolved.get("omnigent_agent_id")
    if not agent:
        return  # the agent check already failed
    try:
        root = session_id or await _agent_session(rest, str(agent))
        if root is None:
            report.warn(
                name,
                "agent bundle unreadable: no session of the configured agent to read it "
                "through (run `doctor --live`)",
            )
            return
        bundle = await rest.get_bytes(f"/v1/sessions/{root}/agent/contents")
        servers = await asyncio.to_thread(_bundle_mcp_servers, bundle)
    except OmnigentReadError as exc:
        report.warn(name, f"agent bundle unreadable: {exc.reason}")
        return
    except Exception as exc:  # a bundle this client cannot parse
        report.warn(name, f"agent bundle unreadable: {type(exc).__name__}: {exc}"[:300])
        return
    missing = missing_factory_tools(servers, config.mcp_port)
    if missing is None:
        report.fail(name, "the agent declares no factory MCP server; it can call no factory tool")
    elif missing:
        report.fail(
            name,
            "the agent's factory MCP allowlist lacks "
            + ", ".join(missing)
            + " (add them to the agent's `tools:` list for the factory server)",
        )
    else:
        report.pass_check(name, f"the agent can call all {len(TOOL_NAMES)} factory tools")


def _error_message(body: object) -> str:
    err = body.get("error") if isinstance(body, dict) else None
    message = err.get("message") if isinstance(err, dict) else None
    return str(message)[:300] if message else ""


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
