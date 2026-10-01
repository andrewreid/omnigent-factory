"""Host-trusted daemon configuration and local permission checks."""

from __future__ import annotations

import os
import stat
import tomllib
from collections.abc import Mapping
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from omnigent_factory.core.types import MICROS_PER_MINUTE, Size, TrustedConfig
from omnigent_factory.ports.github import (
    DEFAULT_STATUS_NAMES,
    STATUS_FIELD_NODE_ID,
    STATUS_OPTION_IDS,
)


class ConfigError(RuntimeError):
    """Configuration is unsafe or invalid."""


#: Keys ``omnigent-factory reload`` applies to the running daemon; any other change
#: needs a restart.
HOT_RELOAD_KEYS = frozenset(
    {
        "max_building",
        "max_open_bot_prs",
        "checkpoint_block_hours",
        "checkpoint_grace_minutes",
        "cost_backstop_usd_per_hour",
        "review_bot_grace_minutes",
        "review_bot_login",
        "review_bot_mention",
        "independent_reviewer_ids",
        "triage_guidance",
        "engineering_guidance",
        "status_names",
    }
)


class ServiceConfig(BaseModel):
    """Configuration whose defaults keep the HTTP listener private to the host."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    state_dir: Path = Field(default_factory=lambda: Path.home() / ".local/state/omnigent-factory")
    bind_host: str = "127.0.0.1"
    bind_port: int = Field(default=8787, ge=1, le=65535)
    #: The factory MCP endpoint (``/mcp``) is served only on a 127.0.0.1 listener on this
    #: port (the same socket as ``bind_host`` when that is loopback on the same port).
    mcp_port: int = Field(default=8787, ge=1, le=65535)
    #: Bearer token for ``/mcp`` (default ``<secrets_dir>/mcp-token``, mode 0600; created
    #: by ``omnigent-factory setup mcp-token``).
    mcp_token_file: Path | None = None
    repo_id: str
    owners: frozenset[int]
    repository: str = "SA-Ambulance/timesheets"
    repository_database_id: int | None = None
    organization_id: int | None = None
    project_node_id: str = "PVT_kwDOEanNes4BkJhb"
    status_field_node_id: str = STATUS_FIELD_NODE_ID
    status_options: dict[str, str] = Field(
        default_factory=lambda: {stage.value: option for stage, option in STATUS_OPTION_IDS.items()}
    )
    #: Status column display names keyed like ``status_options`` (stage identity). Only
    #: ``doctor`` and the setup renderer read them; omitted stages keep their defaults.
    status_names: dict[str, str] = Field(
        default_factory=lambda: {stage.value: name for stage, name in DEFAULT_STATUS_NAMES.items()}
    )
    bot_field_node_id: str = "PVTSSF_lADOEanNes4BkJhbzhjgMaM"
    bot_options: dict[str, str] = Field(
        default_factory=lambda: {
            "Working": "18ff4a7c",
            "Needs you": "b1767cfa",
            "Checkpoint": "b85b9520",
            "Blocked": "d28564d7",
            "Queued": "13cc68fd",
            "Idle": "cd0936de",
        }
    )
    #: Projects v2 TEXT field "Factory note": the card's latest status reason.
    note_field_node_id: str = "PVTF_lADOEanNes4BkJhbzhjww0c"
    github_app_id: int = 5_085_812
    github_installation_id: int = 165_144_097
    github_bot_login: str = "molly-omnigent-factory[bot]"
    github_bot_user_id: int = 334_191_208
    github_api_url: str = "https://api.github.com"
    app_env: Path = Field(default_factory=lambda: Path.home() / ".config/omnigent-factory/app.env")
    app_private_key_file: Path | None = None
    webhook_secret_file: Path | None = None
    omnigent_token_file: Path | None = None
    #: The owner's Omnigent CLI credential store (refreshable login); preferred when set.
    omnigent_cli_store: Path | None = None
    omnigent_base_url: str = "https://omnigent.reid.ee"
    omnigent_host_id: str | None = None
    omnigent_host_name: str = "coder"
    #: The factory agent every new issue session runs (``doctor`` checks it exists).
    omnigent_agent_id: str | None = "rosie"
    omnigent_agent_name: str = "Rosie"
    omnigent_project_id: str | None = None
    omnigent_project_name: str = "Timesheets"
    source_clone: Path = Field(
        default_factory=lambda: Path.home() / ".local/share/omnigent-factory/clones/timesheets"
    )
    worktree_root: Path = Field(
        default_factory=lambda: Path.home() / ".local/share/omnigent-factory/worktrees/timesheets"
    )
    runtime_dir: Path | None = None
    broker_socket_name: str = "credentials.sock"
    wrapper_bin_dir: Path = Field(
        default_factory=lambda: Path.home() / ".local/share/omnigent-factory/bin"
    )
    gh_config_dir: Path = Field(
        default_factory=lambda: Path.home() / ".local/state/omnigent-factory/gh"
    )
    real_gh_path: Path = Path("/usr/bin/gh")
    default_branch: str = "main"
    required_checks: tuple[tuple[str, int], ...] = ()
    # Factory policy. This host file is the single source of truth (the target repository
    # carries no factory config); every key below is hot-reloadable (``reload``).
    independent_reviewer_ids: frozenset[int] = frozenset()
    triage_guidance: str = "Follow repository-local instructions and minimise assumptions."
    engineering_guidance: str = "Follow repository-local engineering and test conventions."
    max_building: int = Field(default=1, ge=1)
    max_open_bot_prs: int = Field(default=3, ge=1)
    checkpoint_block_hours: dict[str, int] = Field(default_factory=lambda: {"S": 2, "M": 4, "L": 6})
    checkpoint_grace_minutes: int = Field(default=15, ge=1, le=120)
    cost_backstop_usd_per_hour: int = Field(default=35, ge=1, le=1000)
    #: Minutes the review bot gets to comment on a PR head before the card can be Ready;
    #: applies only while it can still respond (it has not answered the head, or was
    #: re-pinged since its last answer).
    review_bot_grace_minutes: int = Field(default=10, ge=0, le=120)
    #: The review bot's GitHub login ("" = unknown: the grace always applies).
    review_bot_login: str = "chatgpt-codex-connector[bot]"
    #: The mention that asks the review bot for a (re-)review.
    review_bot_mention: str = "@codex"
    github_config: Path | None = None
    secrets_dir: Path = Field(
        default_factory=lambda: Path.home() / ".config/omnigent-factory/secrets"
    )
    webhook_max_bytes: int = Field(default=2 * 1024 * 1024, ge=1)
    operator_socket_name: str = "operator.sock"
    reconcile_interval_seconds: float = Field(default=120.0, gt=0)
    clock_interval_seconds: float = Field(default=1.0, gt=0)
    effect_poll_seconds: float = Field(default=0.05, gt=0)
    #: Longest the delivery loop sleeps with nothing due. A committed webhook delivery or
    #: an operator release wakes it at once; this only bounds a missed wake-up.
    delivery_idle_poll_seconds: float = Field(default=5.0, gt=0)
    #: Days a processed delivery keeps its body/headers when no event references it.
    delivery_body_retention_days: float = Field(default=14.0, gt=0)
    background_error_backoff_seconds: float = Field(default=0.05, gt=0)
    background_failure_limit: int = Field(default=10, ge=1)
    delivery_retry_backoff_seconds: float = Field(default=1.0, gt=0)
    delivery_retry_max_backoff_seconds: float = Field(default=300.0, gt=0)
    delivery_resolution_backoff_seconds: float = Field(default=900.0, gt=0)
    delivery_resolution_max_attempts: int = Field(default=3, ge=1, le=20)
    operator_timeout_seconds: float = Field(default=5.0, gt=0)
    shutdown_timeout_seconds: float = Field(default=5.0, gt=0)
    observation_interval_seconds: float = Field(default=5.0, gt=0)

    @field_validator("owners")
    @classmethod
    def owners_not_empty(cls, value: frozenset[int]) -> frozenset[int]:
        if not value or any(owner <= 0 for owner in value):
            raise ValueError("owners must contain positive numeric GitHub IDs")
        return value

    @field_validator("operator_socket_name", "broker_socket_name")
    @classmethod
    def socket_is_basename(cls, value: str) -> str:
        if not value or Path(value).name != value:
            raise ValueError("socket name must be a basename")
        return value

    @field_validator("omnigent_host_id", "omnigent_agent_id", "omnigent_project_id", mode="before")
    @classmethod
    def blank_omnigent_id_is_unset(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("status_names")
    @classmethod
    def status_names_complete(cls, value: dict[str, str]) -> dict[str, str]:
        unknown = set(value) - {stage.value for stage in DEFAULT_STATUS_NAMES}
        if unknown:
            raise ValueError(f"status_names has unknown stages: {sorted(unknown)}")
        names = {stage.value: name for stage, name in DEFAULT_STATUS_NAMES.items()} | value
        if any(not name.strip() for name in names.values()) or len(set(names.values())) != len(
            names
        ):
            raise ValueError("status_names must be unique and non-blank")
        return names

    @model_validator(mode="after")
    def trusted_identities_are_consistent(self) -> ServiceConfig:
        if self.github_bot_user_id in self.owners:
            raise ValueError("the factory bot cannot be an owner")
        if self.independent_reviewer_ids & (self.owners | {self.github_bot_user_id}):
            raise ValueError("independent reviewers cannot include owners or the factory bot")
        if not self.triage_guidance.strip() or not self.engineering_guidance.strip():
            raise ValueError("repository guidance must not be blank")
        expected = {stage.value for stage in STATUS_OPTION_IDS}
        if set(self.status_options) != expected or len(set(self.status_options.values())) != len(
            self.status_options
        ):
            raise ValueError("status_options must map every unique live Status option id")
        expected_bots = {"Working", "Needs you", "Checkpoint", "Blocked", "Queued", "Idle"}
        if set(self.bot_options) != expected_bots or len(set(self.bot_options.values())) != len(
            self.bot_options
        ):
            raise ValueError("bot_options must map every unique Bot option id")
        if set(self.checkpoint_block_hours) != {"S", "M", "L"}:
            raise ValueError("checkpoint_block_hours must contain S, M and L")
        blocks = [self.checkpoint_block_hours[key] for key in ("S", "M", "L")]
        if any(not 1 <= value <= 24 for value in blocks) or blocks != sorted(blocks):
            raise ValueError("checkpoint block hours must be 1..24 and ordered S <= M <= L")
        return self

    @property
    def database_path(self) -> Path:
        return self.state_dir / "factory.sqlite3"

    @property
    def operator_socket(self) -> Path:
        return self.effective_runtime_dir / self.operator_socket_name

    @property
    def broker_socket(self) -> Path:
        return self.effective_runtime_dir / self.broker_socket_name

    @property
    def capability_dir(self) -> Path:
        return self.effective_runtime_dir / "capabilities"

    @property
    def effective_runtime_dir(self) -> Path:
        return self.runtime_dir or self.state_dir / "runtime"

    @property
    def resolved_app_private_key_file(self) -> Path:
        return self.app_private_key_file or self.secrets_dir / "app.pem"

    @property
    def resolved_webhook_secret_file(self) -> Path:
        return self.webhook_secret_file or self.secrets_dir / "webhook_secret"

    @property
    def resolved_mcp_token_file(self) -> Path:
        return self.mcp_token_file or self.secrets_dir / "mcp-token"

    @property
    def resolved_omnigent_token_file(self) -> Path:
        return self.omnigent_token_file or self.secrets_dir / "omnigent-token"

    @property
    def trusted(self) -> TrustedConfig:
        return TrustedConfig(
            repo_id=self.repo_id,
            owners=self.owners,
            max_building=self.max_building,
            max_open_bot_prs=self.max_open_bot_prs,
            block_hours={Size(key): value for key, value in self.checkpoint_block_hours.items()},
            grace_us=self.checkpoint_grace_minutes * MICROS_PER_MINUTE,
            cost_usd_per_hour_micros=self.cost_backstop_usd_per_hour * 1_000_000,
            review_grace_us=self.review_bot_grace_minutes * MICROS_PER_MINUTE,
        )

    def prepare_private_directories(self) -> None:
        _private_directory(self.state_dir, create=True)
        _private_directory(self.effective_runtime_dir, create=True)
        _private_directory(self.secrets_dir, create=False)
        if self.secrets_dir.exists():
            for entry in self.secrets_dir.iterdir():
                _private_secret_file(entry)

    def validate_paths(self) -> list[str]:
        errors: list[str] = []
        for path, required in (
            (self.github_config, False),
            (self.app_env, False),
        ):
            if path is not None and not path.is_file():
                errors.append(f"configuration file does not exist: {path}")
            elif required and path is None:
                errors.append("required configuration path is missing")
        try:
            if self.state_dir.exists():
                _private_directory(self.state_dir, create=False)
            if self.secrets_dir.exists():
                _private_directory(self.secrets_dir, create=False)
                for entry in self.secrets_dir.iterdir():
                    _private_secret_file(entry)
            if self.effective_runtime_dir.exists():
                _private_directory(self.effective_runtime_dir, create=False)
        except ConfigError as exc:
            errors.append(str(exc))
        return errors


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat(follow_symlinks=False).st_mode)


def _private_directory(path: Path, *, create: bool) -> None:
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not path.exists():
        return
    if path.is_symlink() or not path.is_dir():
        raise ConfigError(f"private directory is not a real directory: {path}")
    mode = _mode(path)
    if mode != 0o700:
        raise ConfigError(f"private directory must be mode 0700: {path} is {mode:04o}")
    if path.stat(follow_symlinks=False).st_uid != os.getuid():
        raise ConfigError(f"private directory must be owned by the daemon user: {path}")


def _private_secret_file(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise ConfigError(f"secret must be a regular file: {path}")
    mode = _mode(path)
    if mode != 0o600:
        raise ConfigError(f"secret file must be mode 0600: {path} is {mode:04o}")
    if path.stat(follow_symlinks=False).st_uid != os.getuid():
        raise ConfigError(f"secret file must be owned by the daemon user: {path}")


def load_config(path: str | Path) -> ServiceConfig:
    config_path = Path(path).expanduser().resolve()
    with config_path.open("rb") as stream:
        raw = tomllib.load(stream)
    section = raw.get("service", raw)
    if not isinstance(section, dict):
        raise ConfigError("configuration root must be a table")
    # Resolve configured paths relative to the configuration file, not the caller's cwd.
    for key in (
        "state_dir",
        "github_config",
        "secrets_dir",
        "app_env",
        "app_private_key_file",
        "webhook_secret_file",
        "omnigent_token_file",
        "mcp_token_file",
        "omnigent_cli_store",
        "source_clone",
        "worktree_root",
        "runtime_dir",
        "wrapper_bin_dir",
        "gh_config_dir",
        "real_gh_path",
    ):
        value = section.get(key)
        if isinstance(value, str):
            runtime = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
            expanded = Path(
                os.path.expandvars(value.replace("%h", str(Path.home())).replace("%t", runtime))
            ).expanduser()
            section[key] = expanded if expanded.is_absolute() else config_path.parent / expanded
    env_path = section.get("app_env")
    if env_path is None:
        env_path = Path.home() / ".config/omnigent-factory/app.env"
    env = load_app_env(Path(env_path))
    aliases: Mapping[str, str] = {
        "FACTORY_APP_ID": "github_app_id",
        "FACTORY_INSTALLATION_ID": "github_installation_id",
        "FACTORY_BOT_LOGIN": "github_bot_login",
        "FACTORY_BOT_USER_ID": "github_bot_user_id",
    }
    for env_name, field_name in aliases.items():
        if env_name in env and field_name not in section:
            env_value: object = env[env_name]
            if field_name.endswith("_id"):
                try:
                    env_value = int(str(env_value))
                except ValueError as exc:
                    raise ConfigError(f"{env_name} must be an integer") from exc
            section[field_name] = env_value
    return ServiceConfig.model_validate(section)


def load_app_env(path: Path) -> dict[str, str]:
    """Read the owner's mode-0600 ``app.env`` identity file without changing process env."""
    if not path.exists():
        return {}
    _private_secret_file(path)
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or not key.replace("_", "a").isalnum():
            raise ConfigError(f"invalid app.env entry at line {number}")
        if key in values:
            raise ConfigError(f"duplicate app.env key: {key}")
        values[key] = value.strip().strip('"').strip("'")
    return values
