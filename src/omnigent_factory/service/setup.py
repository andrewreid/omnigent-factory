"""Pure render/validate operations; applying infrastructure remains an owner action."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.interfaces import SetupRenderer


def _toml_string(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


class OperationsRenderer:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path

    def render(self, config: ServiceConfig) -> Mapping[str, str]:
        executable = "%h/.local/share/omnigent-factory/venv/bin/omnigent-factory"
        unit = f"""[Unit]
Description=Omnigent factory daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={executable} serve --config {self.config_path}
WorkingDirectory=%h/.local/state/omnigent-factory
Restart=on-failure
RestartSec=5
UMask=0077
RuntimeDirectory=omnigent-factory
RuntimeDirectoryMode=0700
StateDirectory=omnigent-factory
StateDirectoryMode=0700
NoNewPrivileges=true
TimeoutStopSec=30

[Install]
WantedBy=default.target
"""
        ingress = f"""# Ingress contract

Route `factory.reid.ee` through the owner's Cloudflare/Kubernetes ingress to the
coder host at `{config.bind_host}:{config.bind_port}`. Expose only
`POST /webhooks/github`. Do not publish `/healthz` or the private operator socket.

TLS, hostname routing, source controls, and request-size policy belong to the
owner-managed ingress. Never route the credential or operator Unix sockets.
"""
        config_example = f'''# Owner-reviewed host trust configuration.
[service]
state_dir = "{_toml_string(config.state_dir)}"
runtime_dir = "%t/omnigent-factory"
bind_host = "127.0.0.1"  # set to this host's LAN address for ingress
bind_port = 8787
mcp_port = 8787  # loopback-only /mcp listener (127.0.0.1); Rosie's FACTORY_MCP_URL
repo_id = "{_toml_string(config.repo_id)}"
repository = "SA-Ambulance/timesheets"
owners = [114979]
# repository_database_id = 123 # pin the value printed by doctor
# organization_id = 123        # pin the value printed by doctor
project_node_id = "PVT_kwDOEanNes4BkJhb"
status_field_node_id = "PVTSSF_lADOEanNes4BkJhbzhi7I9w"
github_app_id = 5085812
github_installation_id = 165144097
github_bot_login = "molly-omnigent-factory[bot]"
github_bot_user_id = 334191208
app_env = "~/.config/omnigent-factory/app.env"
secrets_dir = "~/.config/omnigent-factory/secrets"
app_private_key_file = "~/.config/omnigent-factory/secrets/app.pem"
webhook_secret_file = "~/.config/omnigent-factory/secrets/webhook_secret"
omnigent_token_file = "~/.config/omnigent-factory/secrets/omnigent-token"
omnigent_base_url = "https://omnigent.reid.ee"
omnigent_host_name = "coder"
omnigent_agent_name = "Rosie"
omnigent_project_name = "Timesheets"
# Pin the IDs printed by `omnigent-factory doctor` before `serve`.
omnigent_host_id = ""
omnigent_agent_id = "rosie"  # the factory agent new issue sessions run
omnigent_project_id = ""
source_clone = "~/.local/share/omnigent-factory/clones/timesheets"
worktree_root = "~/.local/share/omnigent-factory/worktrees/timesheets"
wrapper_bin_dir = "~/.local/share/omnigent-factory/bin"
gh_config_dir = "~/.local/state/omnigent-factory/gh"
real_gh_path = "/usr/bin/gh"
# Factory policy: this file is the single source of truth (no repository factory.yml).
# These keys are hot-reloadable with `omnigent-factory reload` (or SIGHUP).
max_building = 1
max_open_bot_prs = 3
checkpoint_grace_minutes = 15
cost_backstop_usd_per_hour = 35
independent_reviewer_ids = []
# triage_guidance = "Classify work with the repo's area:* labels."
# engineering_guidance = "Engineering conventions live in AGENTS.md."

[service.checkpoint_block_hours]
S = 2
M = 4
L = 6

[service.status_options]
Inbox = "915abb46"
Triaged = "43889573"
Scoped = "3a7f779a"
Building = "ba3c85dd"
Ready = "6df89cbb"
Done = "4980e49d"

[service.bot_options]
Working = "18ff4a7c"
"Needs you" = "b1767cfa"
Checkpoint = "b85b9520"
Blocked = "d28564d7"
Idle = "cd0936de"
'''
        return {
            "omnigent-factory.service": unit,
            "INGRESS.md": ingress,
            "config.example.toml": config_example,
        }

    def validate(self, config: ServiceConfig) -> tuple[str, ...]:
        errors = list(config.validate_paths())
        if config.bind_host.strip("[]") in {"", "0.0.0.0", "::"}:  # noqa: S104 - detects, never binds
            errors.append("bind_host must be a specific address, not a wildcard")
        return tuple(errors)


class CompositeSetupRenderer:
    """Combine Task-2 artifacts with service artifacts without coupling packages."""

    def __init__(self, *renderers: SetupRenderer) -> None:
        self.renderers = renderers

    def render(self, config: ServiceConfig) -> Mapping[str, str]:
        output: dict[str, str] = {}
        for renderer in self.renderers:
            for name, content in renderer.render(config).items():
                if name in output:
                    raise ValueError(f"duplicate setup artifact: {name}")
                output[name] = content
        return output

    def validate(self, config: ServiceConfig) -> tuple[str, ...]:
        return tuple(error for renderer in self.renderers for error in renderer.validate(config))


def write_rendered(artifacts: Mapping[str, str], output_dir: Path) -> None:
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name, content in artifacts.items():
        path = output_dir / name
        if path.parent != output_dir or Path(name).name != name:
            raise ValueError(f"artifact name must be a basename: {name}")
        path.write_text(content, encoding="utf-8")
