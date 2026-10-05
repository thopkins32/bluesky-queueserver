"""Validated QueueServer V2 process configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlparse

import yaml
from pydantic import AfterValidator, Field, field_validator, model_validator

from .contracts import NonBlankText, StrictModel

_RESERVED_WORKER_ARGUMENTS = {
    "--ipc-fd",
    "--worker-instance-uid",
    "--worker-lock-path",
    "--instrument-id",
}


def _canonical_path(value: Path) -> Path:
    return value.expanduser().resolve(strict=False)


CanonicalPath = Annotated[Path, Field(strict=False), AfterValidator(_canonical_path)]


class WorkerLaunchConfig(StrictModel):
    command: Annotated[list[NonBlankText], Field(min_length=1)]
    startup_timeout_seconds: Annotated[float, Field(gt=0)] = 10.0
    heartbeat_interval_seconds: Annotated[float, Field(gt=0)] = 1.0
    heartbeat_timeout_seconds: Annotated[float, Field(gt=0)] = 5.0

    @field_validator("command")
    @classmethod
    def _command_excludes_controller_arguments(cls, value: list[str]) -> list[str]:
        for argument in value:
            option = argument.split("=", 1)[0]
            if option in _RESERVED_WORKER_ARGUMENTS:
                raise ValueError(f"{option} is supplied only by the controller")
        return value

    @model_validator(mode="after")
    def _heartbeat_timeout_exceeds_interval(self) -> WorkerLaunchConfig:
        if self.heartbeat_timeout_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("heartbeat_timeout_seconds must exceed heartbeat_interval_seconds")
        return self


class OidcConfig(StrictModel):
    issuer: NonBlankText
    audience: NonBlankText
    jwks_url: NonBlankText | None = None
    jwks_path: CanonicalPath | None = None
    http_timeout_seconds: Annotated[float, Field(gt=0)] = 5.0
    cache_seconds: Annotated[int, Field(ge=1)] = 300
    clock_skew_seconds: Annotated[int, Field(ge=0, le=30)] = 30

    @field_validator("jwks_url")
    @classmethod
    def _jwks_url_is_https(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("jwks_url must be an HTTPS URL without embedded credentials")
        return value

    @model_validator(mode="after")
    def _has_exactly_one_jwks_source(self) -> OidcConfig:
        if (self.jwks_url is None) == (self.jwks_path is None):
            raise ValueError("exactly one of jwks_url or jwks_path is required")
        return self


class TlsConfig(StrictModel):
    certificate_path: CanonicalPath
    private_key_path: CanonicalPath


class ControllerConfig(StrictModel):
    instrument_id: NonBlankText
    database_path: CanonicalPath
    worker: WorkerLaunchConfig
    oidc: OidcConfig
    tls: TlsConfig
    offline_simulator: bool = False

    @model_validator(mode="after")
    def _local_jwks_is_offline_only(self) -> ControllerConfig:
        if self.oidc.jwks_path is not None and not self.offline_simulator:
            raise ValueError("jwks_path requires offline_simulator=true")
        return self


class WorkerRuntimeConfig(StrictModel):
    provider: Literal["simulated-count", "profile"]
    environment_lock_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    startup_directory: CanonicalPath | None = None
    adapter_path: CanonicalPath | None = None

    @model_validator(mode="after")
    def _paths_match_provider(self) -> WorkerRuntimeConfig:
        paths = (self.startup_directory, self.adapter_path)
        if self.provider == "simulated-count" and any(path is not None for path in paths):
            raise ValueError("simulated-count provider does not accept profile paths")
        if self.provider == "profile" and any(path is None for path in paths):
            raise ValueError("profile provider requires startup_directory and adapter_path")
        return self


def _load_yaml_mapping(path: str | Path) -> dict[str, object]:
    config_path = Path(path).expanduser().resolve(strict=True)
    parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if type(parsed) is not dict or any(type(key) is not str for key in parsed):
        raise ValueError(f"configuration {config_path} must contain one YAML mapping")
    return parsed


def load_controller_config(path: str | Path) -> ControllerConfig:
    return ControllerConfig.model_validate(_load_yaml_mapping(path))


def load_worker_runtime_config(path: str | Path) -> WorkerRuntimeConfig:
    return WorkerRuntimeConfig.model_validate(_load_yaml_mapping(path))
