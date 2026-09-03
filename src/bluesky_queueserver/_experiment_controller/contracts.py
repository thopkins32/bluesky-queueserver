from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError, ValidationError

WORKER_PROTOCOL_VERSION = 1
SIMULATED_COUNT_OPERATION_ID = "simulated-count"
SIMULATED_COUNT_OPERATION_VERSION = "1"
SIMULATED_WORKER_REVISION = "prototype-simulator-v1"


class OperationState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class OperationDescriptor:
    operation_id: str
    operation_version: str
    parameter_schema: Mapping[str, object]


@dataclass(frozen=True)
class WorkerCatalog:
    worker_revision: str
    operations: tuple[OperationDescriptor, ...]


@dataclass(frozen=True)
class ControlLease:
    lease_uid: str
    subject: str
    issued_at: float
    expires_at: float


@dataclass(frozen=True)
class OperationRecord:
    operation_uid: str
    operation_id: str
    operation_version: str
    parameters: Mapping[str, object]
    worker_revision: str
    submitted_by: str
    state: OperationState
    queue_sequence: int
    run_uids: tuple[str, ...]
    error_message: str | None
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class QueueSnapshot:
    revision: int
    operations: tuple[OperationRecord, ...]
    dispatch_block_reason: str | None


@dataclass(frozen=True)
class ControllerEvent:
    event_id: int
    occurred_at: float
    actor: str
    event_type: str
    operation_uid: str | None
    payload: Mapping[str, object]


@dataclass(frozen=True)
class WorkerExecutionResult:
    state: OperationState
    run_uids: tuple[str, ...]
    error_message: str | None


class ControllerError(Exception):
    pass


class StorageVersionError(ControllerError):
    def __init__(self, *, expected_version: int, current_version: int):
        self.expected_version = expected_version
        self.current_version = current_version
        super().__init__(f"Unsupported controller storage version {current_version}; expected {expected_version}")


class RevisionConflictError(ControllerError):
    def __init__(self, *, expected_revision: int, current_revision: int):
        self.expected_revision = expected_revision
        self.current_revision = current_revision
        super().__init__(
            f"Queue revision conflict: expected {expected_revision}, current revision is {current_revision}"
        )


class LeaseConflictError(ControllerError):
    pass


class LeaseExpiredError(ControllerError):
    pass


class DispatchBlockedError(ControllerError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Dispatch is blocked: {reason}")


class OperationValidationError(ControllerError):
    pass


class WorkerProtocolError(ControllerError):
    pass


class WorkerTransportError(ControllerError):
    pass


def validate_operation_parameters(
    descriptor: OperationDescriptor, parameters: Mapping[str, object]
) -> dict[str, object]:
    """Return a JSON-compatible parameter object validated against an operation descriptor."""

    if not isinstance(parameters, Mapping):
        raise OperationValidationError("Operation parameters must be a JSON object")

    try:
        encoded = json.dumps(parameters, allow_nan=False)
        normalized = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise OperationValidationError(f"Operation parameters are not JSON serializable: {exc}") from exc

    if not isinstance(normalized, dict):
        raise OperationValidationError("Operation parameters must be a JSON object")

    schema = descriptor.parameter_schema
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        raise OperationValidationError("Operation descriptor must define a closed JSON-object schema")

    try:
        Draft7Validator.check_schema(schema)
        Draft7Validator(schema).validate(normalized)
    except SchemaError as exc:
        raise OperationValidationError(f"Invalid operation parameter schema: {exc.message}") from exc
    except ValidationError as exc:
        raise OperationValidationError(f"Invalid operation parameters: {exc.message}") from exc

    return normalized
