"""Reviewed operation registration SDK."""

from __future__ import annotations

import hashlib
import inspect
import re
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass
from importlib import metadata
from types import MappingProxyType
from typing import Any, TypeVar

from pydantic import BaseModel

from ..contracts import (
    WORKER_PROTOCOL_VERSION,
    AuthorizationScope,
    OperationDescriptor,
    OrphanPolicy,
    StrictModel,
    WorkerCatalog,
    canonical_json,
    make_operation_descriptor,
    validate_operation_request,
    validate_operation_result,
)

RequestModel = TypeVar("RequestModel", bound=StrictModel)
ResultModel = TypeVar("ResultModel", bound=StrictModel)


@dataclass(frozen=True)
class OperationContext:
    run_engine: object
    devices: Mapping[str, object]
    profile: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "devices", MappingProxyType(dict(self.devices)))
        if self.profile is not None:
            object.__setattr__(self, "profile", MappingProxyType(dict(self.profile)))


@dataclass(frozen=True)
class PreparedOperation:
    plan: Generator[object, object, object]
    build_result: Callable[[tuple[str, ...]], BaseModel | Mapping[str, object]]

    def __post_init__(self) -> None:
        if not isinstance(self.plan, Generator):
            raise TypeError("prepared operation plan must be a generator")
        if not callable(self.build_result):
            raise TypeError("prepared operation build_result must be callable")


OperationHandler = Callable[[StrictModel, OperationContext], PreparedOperation]


@dataclass(frozen=True)
class RegisteredOperation:
    descriptor: OperationDescriptor
    request_model: type[StrictModel]
    result_model: type[StrictModel]
    handler: OperationHandler


class OperationRegistry:
    def __init__(self) -> None:
        self._operations: dict[tuple[str, str], RegisteredOperation] = {}

    def register(
        self,
        *,
        operation_id: str,
        operation_version: str,
        request_model: type[RequestModel],
        result_model: type[ResultModel],
        required_scope: AuthorizationScope,
        orphan_policy: OrphanPolicy,
    ) -> Callable[[Callable[[RequestModel, OperationContext], PreparedOperation]], OperationHandler]:
        descriptor = make_operation_descriptor(
            operation_id=operation_id,
            operation_version=operation_version,
            request_model=request_model,
            result_model=result_model,
            required_scope=required_scope,
            orphan_policy=orphan_policy,
        )
        if orphan_policy is not OrphanPolicy.REQUEST_STOP:
            raise ValueError(f"unsupported orphan policy {orphan_policy!r}")
        identity = (descriptor.operation_id, descriptor.operation_version)

        def decorator(handler: Callable[[RequestModel, OperationContext], PreparedOperation]) -> OperationHandler:
            if identity in self._operations:
                raise ValueError(f"operation {identity[0]!r} version {identity[1]!r} is already registered")
            self._validate_handler_signature(handler)
            registered = RegisteredOperation(
                descriptor=descriptor,
                request_model=request_model,
                result_model=result_model,
                handler=handler,
            )
            self._operations[identity] = registered
            return handler

        return decorator

    @property
    def descriptors(self) -> tuple[OperationDescriptor, ...]:
        return tuple(item.descriptor for item in self._operations.values())

    def catalog(
        self, *, protocol_version: str, worker_revision: str, provenance: dict[str, object]
    ) -> WorkerCatalog:
        if not self._operations:
            raise RuntimeError("worker operation catalog is empty")
        return WorkerCatalog(
            protocol_version=protocol_version,
            worker_revision=worker_revision,
            worker_provenance=provenance,
            operations=list(self.descriptors),
        )

    def prepare(
        self,
        *,
        operation_id: str,
        operation_version: str,
        parameters: object,
        context: OperationContext,
    ) -> tuple[RegisteredOperation, PreparedOperation]:
        registered = self.get(operation_id, operation_version)
        validated = validate_operation_request(
            registered.descriptor,
            operation_id=operation_id,
            operation_version=operation_version,
            parameters=parameters,
        )
        request = registered.request_model.model_validate(validated)
        prepared = registered.handler(request, context)
        if not isinstance(prepared, PreparedOperation):
            raise TypeError("operation handler must return PreparedOperation")
        return registered, prepared

    def validate_result(
        self,
        registered: RegisteredOperation,
        result: BaseModel | Mapping[str, object],
    ) -> dict[str, object]:
        raw = result.model_dump(mode="json") if isinstance(result, BaseModel) else dict(result)
        validated = registered.result_model.model_validate(raw).model_dump(mode="json")
        return validate_operation_result(registered.descriptor, validated)

    def get(self, operation_id: str, operation_version: str) -> RegisteredOperation:
        try:
            return self._operations[(operation_id, operation_version)]
        except KeyError as exc:
            raise KeyError(f"operation {operation_id!r} version {operation_version!r} is not registered") from exc

    @staticmethod
    def _validate_handler_signature(handler: Callable[..., Any]) -> None:
        signature = inspect.signature(handler)
        parameters = list(signature.parameters.values())
        if len(parameters) != 2 or any(
            parameter.kind not in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
            for parameter in parameters
        ):
            raise TypeError("operation handler must accept exactly (request, context)")


def build_worker_catalog(
    registry: OperationRegistry,
    *,
    environment_lock_sha256: str,
    startup_hashes: Mapping[str, str] | None = None,
    adapter_sha256: str | None = None,
) -> WorkerCatalog:
    if not re.fullmatch(r"[0-9a-f]{64}", environment_lock_sha256):
        raise ValueError("environment_lock_sha256 must be 64 lowercase hexadecimal characters")
    if startup_hashes is None and adapter_sha256 is not None:
        raise ValueError("adapter provenance requires profile startup provenance")
    if startup_hashes is not None:
        if adapter_sha256 is None or not re.fullmatch(r"[0-9a-f]{64}", adapter_sha256):
            raise ValueError("profile adapter SHA-256 is required")
        if any(not re.fullmatch(r"[0-9a-f]{64}", digest) for digest in startup_hashes.values()):
            raise ValueError("profile source SHA-256 values must be lowercase hexadecimal")

    descriptors = [descriptor.model_dump(mode="json") for descriptor in registry.descriptors]
    if not descriptors:
        raise RuntimeError("worker operation catalog is empty")
    provenance = {
        "distribution_version": metadata.version("bluesky-queueserver"),
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "operations": descriptors,
        "bluesky_version": metadata.version("bluesky"),
        "ophyd_version": metadata.version("ophyd"),
        "environment_lock_sha256": environment_lock_sha256,
        "startup_source_sha256": None if startup_hashes is None else dict(sorted(startup_hashes.items())),
        "adapter_source_sha256": adapter_sha256,
    }
    revision = f"sha256:{hashlib.sha256(canonical_json(provenance).encode()).hexdigest()}"
    return registry.catalog(
        protocol_version=WORKER_PROTOCOL_VERSION,
        worker_revision=revision,
        provenance=provenance,
    )
