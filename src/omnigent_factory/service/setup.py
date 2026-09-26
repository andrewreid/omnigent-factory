"""Pure render/validate operations; applying infrastructure remains an owner action."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from omnigent_factory.service.config import ServiceConfig
from omnigent_factory.service.interfaces import SetupRenderer


class OperationsRenderer:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path

    def render(self, config: ServiceConfig) -> Mapping[str, str]:
        unit = f"""[Unit]
Description=Omnigent Factory
After=network-online.target

[Service]
Type=simple
ExecStart=%h/.local/bin/omnigent-factory serve --config {self.config_path}
Restart=on-failure
RestartSec=5
UMask=0077
NoNewPrivileges=true

[Install]
WantedBy=default.target
"""
        ingress = f"""# Ingress contract

Route `factory.reid.ee` through the owner's Cloudflare/Kubernetes ingress to the
coder host at `{config.bind_host}:{config.bind_port}`. Expose only
`POST /webhooks/github`. Do not publish `/healthz` or the private operator socket.

The daemon deliberately binds to loopback by default; TLS, hostname routing, source
controls, and request-size policy belong to the owner-managed ingress.
"""
        return {
            "omnigent-factory.service": unit,
            "INGRESS.md": ingress,
        }

    def validate(self, config: ServiceConfig) -> tuple[str, ...]:
        errors = list(config.validate_paths())
        if config.bind_host != "127.0.0.1":
            errors.append("bind_host should remain 127.0.0.1 behind the owner ingress")
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
