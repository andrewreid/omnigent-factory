"""Host-trusted daemon configuration and local permission checks."""

from __future__ import annotations

import os
import stat
import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from omnigent_factory.core.types import TrustedConfig


class ConfigError(RuntimeError):
    """Configuration is unsafe or invalid."""


class ServiceConfig(BaseModel):
    """Configuration whose defaults keep the HTTP listener private to the host."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    state_dir: Path = Field(default_factory=lambda: Path.home() / ".local/state/omnigent-factory")
    bind_host: str = "127.0.0.1"
    bind_port: int = Field(default=8787, ge=1, le=65535)
    repo_id: str
    owners: frozenset[int]
    max_building: int = Field(default=1, ge=1)
    max_open_bot_prs: int = Field(default=3, ge=1)
    repository_config: Path | None = None
    github_config: Path | None = None
    secrets_dir: Path = Field(
        default_factory=lambda: Path.home() / ".config/omnigent-factory/secrets"
    )
    webhook_max_bytes: int = Field(default=2 * 1024 * 1024, ge=1)
    operator_socket_name: str = "operator.sock"
    reconcile_interval_seconds: float = Field(default=120.0, gt=0)
    clock_interval_seconds: float = Field(default=1.0, gt=0)
    effect_poll_seconds: float = Field(default=0.05, gt=0)
    background_error_backoff_seconds: float = Field(default=0.05, gt=0)
    background_failure_limit: int = Field(default=10, ge=1)
    delivery_retry_backoff_seconds: float = Field(default=1.0, gt=0)
    delivery_retry_max_backoff_seconds: float = Field(default=300.0, gt=0)
    operator_timeout_seconds: float = Field(default=5.0, gt=0)
    shutdown_timeout_seconds: float = Field(default=5.0, gt=0)

    @field_validator("owners")
    @classmethod
    def owners_not_empty(cls, value: frozenset[int]) -> frozenset[int]:
        if not value or any(owner <= 0 for owner in value):
            raise ValueError("owners must contain positive numeric GitHub IDs")
        return value

    @field_validator("operator_socket_name")
    @classmethod
    def socket_is_basename(cls, value: str) -> str:
        if not value or Path(value).name != value:
            raise ValueError("operator_socket_name must be a basename")
        return value

    @property
    def database_path(self) -> Path:
        return self.state_dir / "factory.sqlite3"

    @property
    def operator_socket(self) -> Path:
        return self.state_dir / self.operator_socket_name

    @property
    def trusted(self) -> TrustedConfig:
        return TrustedConfig(
            repo_id=self.repo_id,
            owners=self.owners,
            max_building=self.max_building,
            max_open_bot_prs=self.max_open_bot_prs,
        )

    def prepare_private_directories(self) -> None:
        _private_directory(self.state_dir, create=True)
        _private_directory(self.secrets_dir, create=False)
        if self.secrets_dir.exists():
            for entry in self.secrets_dir.iterdir():
                _private_secret_file(entry)

    def validate_paths(self) -> list[str]:
        errors: list[str] = []
        for path, required in (
            (self.repository_config, False),
            (self.github_config, False),
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
    for key in ("state_dir", "repository_config", "github_config", "secrets_dir"):
        value = section.get(key)
        if isinstance(value, str):
            expanded = Path(os.path.expandvars(value)).expanduser()
            section[key] = expanded if expanded.is_absolute() else config_path.parent / expanded
    return ServiceConfig.model_validate(section)
