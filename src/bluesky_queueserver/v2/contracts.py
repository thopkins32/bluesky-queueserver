"""Strict public and private QueueServer V2 data contracts."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID, uuid4

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    field_validator,
    model_validator,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_UTC_MICROS = 253_402_300_799_999_999
API_VERSION = "2"
WORKER_PROTOCOL_VERSION = "2"


class OperationState(StrEnum):
    SUBMITTED = "submitted"
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ABORTED = "aborted"
    INTERRUPTED = "interrupted"
    UNKNOWN = "unknown"


class AttemptState(StrEnum):
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABORTED = "aborted"
    INTERRUPTED = "interrupted"
    UNKNOWN = "unknown"


class QueueExecutionState(StrEnum):
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    STOPPED = "stopped"
    BLOCKED = "blocked"


class QueueExecutionPolicy(StrEnum):
    STOP_ON_NON_SUCCESS = "stop_on_non_success"


class OrphanPolicy(StrEnum):
    REQUEST_STOP = "request-stop"


class AuthorizationScope(StrEnum):
    READ = "queueserver:read"
    CONTROL = "queueserver:control"
    ADMIN = "queueserver:admin"


def effective_authorization_scopes(scopes: Iterable[str]) -> frozenset[AuthorizationScope]:
    recognized = {scope for value in scopes if (scope := _scope_or_none(value)) is not None}
    if AuthorizationScope.ADMIN in recognized:
        return frozenset(AuthorizationScope)
    if AuthorizationScope.CONTROL in recognized:
        recognized.add(AuthorizationScope.READ)
    return frozenset(recognized)


def _scope_or_none(value: str) -> AuthorizationScope | None:
    try:
        return AuthorizationScope(value)
    except ValueError:
        return None


def scope_allows(scopes: Iterable[str], required: AuthorizationScope) -> bool:
    return required in effective_authorization_scopes(scopes)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)


def _validate_uuid4_string(value: str) -> str:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("must be a canonical UUID4 string") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise ValueError("must be a canonical UUID4 string")
    return value


def uuid4_string() -> str:
    return str(uuid4())


def format_utc_micros(value: int) -> str:
    return (_EPOCH + timedelta(microseconds=value)).isoformat(timespec="microseconds").replace("+00:00", "Z")


UUID4String = Annotated[str, AfterValidator(_validate_uuid4_string)]
OperationUid = UUID4String
AttemptUid = UUID4String
QueueExecutionUid = UUID4String
LeaseUid = UUID4String
WorkerInstanceUid = UUID4String
MessageUid = UUID4String
CorrelationUid = UUID4String
UtcMicros = Annotated[int, Field(ge=0, le=_MAX_UTC_MICROS)]
ApiTimestamp = Annotated[UtcMicros, PlainSerializer(format_utc_micros, return_type=str, when_used="json")]

JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
JsonObject = dict[str, object]


class ContractValidationError(ValueError):
    """A public value does not satisfy the registered operation contract."""


def require_json_value(value: object, *, path: str = "$") -> None:
    value_type = type(value)
    if value is None or value_type in {str, bool, int}:
        return
    if value_type is float:
        if not math.isfinite(value):
            raise ContractValidationError(f"{path} must contain only finite JSON numbers")
        return
    if value_type is list:
        for index, item in enumerate(value):
            require_json_value(item, path=f"{path}[{index}]")
        return
    if value_type is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise ContractValidationError(f"{path} must contain only string object keys")
            require_json_value(item, path=f"{path}.{key}")
        return
    raise ContractValidationError(f"{path} contains non-JSON value of type {value_type.__name__}")


def require_json_object(value: object, *, label: str) -> JsonObject:
    if type(value) is not dict:
        raise ContractValidationError(f"{label} must be a JSON object")
    require_json_value(value)
    return value


def canonical_json(value: object) -> str:
    require_json_value(value)
    return json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _validate_closed_schema(schema: JsonObject) -> JsonObject:
    require_json_object(schema, label="schema")
    if schema.get("$schema") != JSON_SCHEMA_DIALECT:
        raise ValueError(f"schema must declare {JSON_SCHEMA_DIALECT}")
    if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
        raise ValueError("operation schemas must describe closed JSON objects")
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ValueError(f"invalid JSON Schema: {exc.message}") from exc
    return schema


def model_json_schema(model: type[BaseModel]) -> JsonObject:
    if not isinstance(model, type) or not issubclass(model, BaseModel):
        raise TypeError("request and result models must be Pydantic models")
    required_config = {"extra": "forbid", "frozen": True, "strict": True}
    if any(model.model_config.get(key) != expected for key, expected in required_config.items()):
        raise TypeError("request and result models must be frozen, strict, and forbid extra fields")
    schema = {"$schema": JSON_SCHEMA_DIALECT, **model.model_json_schema(mode="validation")}
    return _validate_closed_schema(schema)


class OperationDescriptor(StrictModel):
    operation_id: Annotated[str, Field(min_length=1)]
    operation_version: Annotated[str, Field(min_length=1)]
    request_schema: JsonObject
    result_schema: JsonObject
    required_scope: AuthorizationScope
    orphan_policy: OrphanPolicy

    @field_validator("request_schema", "result_schema")
    @classmethod
    def _schemas_are_closed(cls, value: JsonObject) -> JsonObject:
        return _validate_closed_schema(value)


class WorkerCatalog(StrictModel):
    protocol_version: Annotated[str, Field(min_length=1)]
    worker_revision: Annotated[str, Field(min_length=1)]
    worker_provenance: JsonObject
    operations: Annotated[list[OperationDescriptor], Field(min_length=1)]

    @field_validator("worker_provenance")
    @classmethod
    def _provenance_is_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="worker_provenance")

    @model_validator(mode="after")
    def _operation_versions_are_unique(self) -> WorkerCatalog:
        identities = [(item.operation_id, item.operation_version) for item in self.operations]
        if len(identities) != len(set(identities)):
            raise ValueError("worker catalog contains a duplicate operation ID and version")
        return self


def make_operation_descriptor(
    *,
    operation_id: str,
    operation_version: str,
    request_model: type[BaseModel],
    result_model: type[BaseModel],
    required_scope: AuthorizationScope,
    orphan_policy: OrphanPolicy,
) -> OperationDescriptor:
    return OperationDescriptor(
        operation_id=operation_id,
        operation_version=operation_version,
        request_schema=model_json_schema(request_model),
        result_schema=model_json_schema(result_model),
        required_scope=required_scope,
        orphan_policy=orphan_policy,
    )


def descriptor_fingerprint(descriptor: OperationDescriptor) -> str:
    digest = hashlib.sha256(canonical_json(descriptor.model_dump(mode="json")).encode()).hexdigest()
    return f"sha256:{digest}"


def _validate_schema_instance(*, schema: JsonObject, value: object, label: str) -> JsonObject:
    json_object = require_json_object(value, label=label)
    try:
        Draft202012Validator(schema).validate(json_object)
    except JsonSchemaValidationError as exc:
        location = ".".join(str(item) for item in exc.absolute_path)
        suffix = f" at {location}" if location else ""
        raise ContractValidationError(
            f"{label} does not match its registered schema{suffix}: {exc.message}"
        ) from exc
    return json_object


def validate_operation_request(
    descriptor: OperationDescriptor,
    *,
    operation_id: str,
    operation_version: str,
    parameters: object,
) -> JsonObject:
    if operation_id != descriptor.operation_id:
        raise ContractValidationError(f"unknown operation ID {operation_id!r}")
    if operation_version != descriptor.operation_version:
        raise ContractValidationError(
            f"unsupported version {operation_version!r} for operation {descriptor.operation_id!r}"
        )
    return _validate_schema_instance(schema=descriptor.request_schema, value=parameters, label="parameters")


def validate_operation_result(descriptor: OperationDescriptor, result: object) -> JsonObject:
    return _validate_schema_instance(schema=descriptor.result_schema, value=result, label="result")


SIMULATED_COUNT_OPERATION_ID = "simulated-count"
SIMULATED_COUNT_OPERATION_VERSION = "1"


class SimulatedCountRequest(StrictModel):
    detectors: Annotated[list[Literal["det"]], Field(min_length=1, max_length=1)]
    num: Annotated[int, Field(ge=1, le=10)] = 1
    delay: Annotated[float, Field(ge=0, le=10)] = 0.0

    @field_validator("detectors")
    @classmethod
    def _detector_is_exactly_one_unique_id(cls, value: list[Literal["det"]]) -> list[Literal["det"]]:
        if len(value) != 1 or len(set(value)) != len(value):
            raise ValueError("detectors must contain exactly one unique detector ID")
        return value


class SimulatedCountResult(StrictModel):
    run_uids: list[str]


SIMULATED_COUNT_DESCRIPTOR = make_operation_descriptor(
    operation_id=SIMULATED_COUNT_OPERATION_ID,
    operation_version=SIMULATED_COUNT_OPERATION_VERSION,
    request_model=SimulatedCountRequest,
    result_model=SimulatedCountResult,
    required_scope=AuthorizationScope.CONTROL,
    orphan_policy=OrphanPolicy.REQUEST_STOP,
)


def _nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("must not be blank")
    return value


NonBlankText = Annotated[str, Field(min_length=1), AfterValidator(_nonblank)]
ReasonText = Annotated[str, Field(min_length=1, max_length=1000), AfterValidator(_nonblank)]
QueueRevision = Annotated[int, Field(ge=0)]


class ActorKind(StrEnum):
    PRINCIPAL = "principal"
    SCHEDULER = "scheduler"
    SYSTEM = "system"


class LeaseRequest(StrictModel):
    ttl_seconds: Annotated[int, Field(ge=5, le=3600)] = 300


class OperationSubmission(StrictModel):
    operation_id: NonBlankText
    operation_version: NonBlankText
    parameters: JsonObject

    @field_validator("parameters")
    @classmethod
    def _parameters_are_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="parameters")


class QueueReorder(StrictModel):
    operation_uids: list[OperationUid]

    @field_validator("operation_uids")
    @classmethod
    def _operation_uids_are_unique(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("operation_uids must not contain duplicates")
        return value


class QueueExecutionStart(StrictModel):
    policy: Annotated[QueueExecutionPolicy, Field(strict=False)] = QueueExecutionPolicy.STOP_ON_NON_SUCCESS


class MutationReason(StrictModel):
    reason: ReasonText


class RecoveryAcknowledgement(StrictModel):
    note: ReasonText


class ControlLeaseView(StrictModel):
    holder: NonBlankText | None
    expires_at: ApiTimestamp | None


class OperationView(StrictModel):
    operation_uid: OperationUid
    operation_id: NonBlankText
    operation_version: NonBlankText
    parameters: JsonObject
    descriptor_fingerprint: NonBlankText
    state: OperationState
    submitted_by: NonBlankText
    submitted_at: ApiTimestamp
    updated_at: ApiTimestamp
    queue_position: Annotated[int, Field(ge=0)] | None = None
    queue_execution_uid: QueueExecutionUid | None = None
    attempt_uid: AttemptUid | None = None
    replaced_by: OperationUid | None = None
    result: JsonObject | None = None

    @field_validator("parameters")
    @classmethod
    def _parameters_are_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="parameters")

    @field_validator("result")
    @classmethod
    def _result_is_json(cls, value: JsonObject | None) -> JsonObject | None:
        return None if value is None else require_json_object(value, label="result")


class QueueSnapshot(StrictModel):
    revision: QueueRevision
    operations: list[OperationView]
    active_execution_uid: QueueExecutionUid | None = None
    dispatch_block_uid: UUID4String | None = None


class QueueExecutionView(StrictModel):
    queue_execution_uid: QueueExecutionUid
    state: QueueExecutionState
    policy: QueueExecutionPolicy
    initiated_by: NonBlankText
    starting_revision: QueueRevision
    admitted_operation_uids: list[OperationUid]
    active_attempt_uid: AttemptUid | None
    dispatch_block_uid: UUID4String | None
    created_at: ApiTimestamp
    updated_at: ApiTimestamp
    stop_requested_at: ApiTimestamp | None = None
    completed_at: ApiTimestamp | None = None


class AttemptView(StrictModel):
    attempt_uid: AttemptUid
    operation_uid: OperationUid
    queue_execution_uid: QueueExecutionUid
    worker_instance_uid: WorkerInstanceUid
    worker_revision: NonBlankText
    worker_provenance: JsonObject
    state: AttemptState
    created_at: ApiTimestamp
    started_at: ApiTimestamp | None = None
    completed_at: ApiTimestamp | None = None
    stop_requested_at: ApiTimestamp | None = None
    stop_acknowledged_at: ApiTimestamp | None = None
    run_uids: list[str]
    diagnostic: str | None = None
    cleanup_completed: bool | None = None

    @field_validator("worker_provenance")
    @classmethod
    def _worker_provenance_is_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="worker_provenance")


class ControllerEvent(StrictModel):
    event_id: Annotated[int, Field(ge=1)]
    timestamp: ApiTimestamp
    actor_kind: ActorKind
    actor_id: NonBlankText
    event_type: NonBlankText
    operation_uid: OperationUid | None = None
    queue_execution_uid: QueueExecutionUid | None = None
    attempt_uid: AttemptUid | None = None
    worker_instance_uid: WorkerInstanceUid | None = None
    queue_revision: QueueRevision | None = None
    payload: JsonObject

    @field_validator("payload")
    @classmethod
    def _payload_is_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="payload")


class CatalogView(StrictModel):
    protocol_version: NonBlankText
    worker_revision: NonBlankText
    worker_provenance: JsonObject
    operations: list[OperationDescriptor]

    @field_validator("worker_provenance")
    @classmethod
    def _worker_provenance_is_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="worker_provenance")


class HealthView(StrictModel):
    status: Literal["ok"] = "ok"
    package_version: NonBlankText
    worker_revision: str | None = None
    environment_lock_sha256: str | None = None
    source_sha256: JsonObject | None = None

    @field_validator("source_sha256")
    @classmethod
    def _source_hashes_are_json(cls, value: JsonObject | None) -> JsonObject | None:
        return None if value is None else require_json_object(value, label="source_sha256")


class ReadinessView(StrictModel):
    ready: bool
    controller_lock: Literal["held", "unavailable"]
    storage: Literal["ready", "unavailable"]
    worker: Literal["ready", "not_ready"]
    fencing: Literal["clear", "blocked"]
    dispatch: Literal["ready", "blocked"]


class ErrorBody(StrictModel):
    code: NonBlankText
    message: NonBlankText
    details: JsonObject | None = None
    request_id: NonBlankText

    @field_validator("details")
    @classmethod
    def _details_are_json(cls, value: JsonObject | None) -> JsonObject | None:
        return None if value is None else require_json_object(value, label="details")


class ErrorResponse(StrictModel):
    error: ErrorBody
