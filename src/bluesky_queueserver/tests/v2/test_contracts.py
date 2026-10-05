import json

import pytest
from pydantic import ValidationError

from bluesky_queueserver.v2.config import (
    ControllerConfig,
    OidcConfig,
    WorkerLaunchConfig,
    WorkerRuntimeConfig,
    load_controller_config,
    load_worker_runtime_config,
)
from bluesky_queueserver.v2.contracts import (
    JSON_SCHEMA_DIALECT,
    SIMULATED_COUNT_DESCRIPTOR,
    SIMULATED_COUNT_OPERATION_ID,
    SIMULATED_COUNT_OPERATION_VERSION,
    ApiTimestamp,
    AttemptState,
    AuthorizationScope,
    ContractValidationError,
    ControlLeaseView,
    ErrorBody,
    ErrorResponse,
    HealthView,
    LeaseRequest,
    MutationReason,
    OperationDescriptor,
    OperationState,
    OperationSubmission,
    OperationUid,
    OrphanPolicy,
    QueueExecutionPolicy,
    QueueExecutionStart,
    QueueExecutionState,
    QueueReorder,
    ReadinessView,
    RecoveryAcknowledgement,
    SimulatedCountRequest,
    SimulatedCountResult,
    StrictModel,
    WorkerCatalog,
    descriptor_fingerprint,
    effective_authorization_scopes,
    make_operation_descriptor,
    scope_allows,
    uuid4_string,
    validate_operation_request,
    validate_operation_result,
)


class IdentityRecord(StrictModel):
    operation_uid: OperationUid
    created_at: ApiTimestamp


class CatalogRequest(StrictModel):
    values: list[int]


class CatalogResult(StrictModel):
    total: int


def make_catalog_descriptor() -> OperationDescriptor:
    return make_operation_descriptor(
        operation_id="catalog-test",
        operation_version="1",
        request_model=CatalogRequest,
        result_model=CatalogResult,
        required_scope=AuthorizationScope.CONTROL,
        orphan_policy=OrphanPolicy.REQUEST_STOP,
    )


def test_lifecycle_values_are_complete():
    assert {state.value for state in OperationState} == {
        "submitted",
        "queued",
        "claimed",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "aborted",
        "interrupted",
        "unknown",
    }
    assert {state.value for state in AttemptState} == {
        "claimed",
        "running",
        "succeeded",
        "failed",
        "aborted",
        "interrupted",
        "unknown",
    }
    assert {state.value for state in QueueExecutionState} == {
        "running",
        "stopping",
        "completed",
        "stopped",
        "blocked",
    }
    assert [policy.value for policy in QueueExecutionPolicy] == ["stop_on_non_success"]
    assert [policy.value for policy in OrphanPolicy] == ["request-stop"]


def test_identity_and_timestamp_contract_is_strict_and_frozen():
    record = IdentityRecord(operation_uid=uuid4_string(), created_at=1)

    assert json.loads(record.model_dump_json())["created_at"] == "1970-01-01T00:00:00.000001Z"

    with pytest.raises(ValidationError):
        IdentityRecord(operation_uid="not-a-uuid", created_at=1)
    with pytest.raises(ValidationError):
        IdentityRecord(operation_uid=uuid4_string(), created_at="1")
    with pytest.raises(ValidationError):
        IdentityRecord(operation_uid=uuid4_string(), created_at=1, extra=True)
    with pytest.raises(ValidationError):
        record.created_at = 2


def test_operation_descriptor_uses_closed_2020_12_schemas():
    descriptor = make_catalog_descriptor()

    assert set(OperationDescriptor.model_fields) == {
        "operation_id",
        "operation_version",
        "request_schema",
        "result_schema",
        "required_scope",
        "orphan_policy",
    }
    assert descriptor.request_schema["$schema"] == JSON_SCHEMA_DIALECT
    assert descriptor.request_schema["additionalProperties"] is False
    assert descriptor.result_schema["additionalProperties"] is False
    assert descriptor_fingerprint(descriptor).startswith("sha256:")


def test_worker_catalog_keeps_operations_as_json_list():
    catalog = WorkerCatalog(
        protocol_version="2",
        worker_revision="sha256:" + "1" * 64,
        worker_provenance={"distribution_version": "test"},
        operations=[make_catalog_descriptor()],
    )

    payload = json.loads(catalog.model_dump_json())
    assert isinstance(payload["operations"], list)
    assert set(WorkerCatalog.model_fields) == {
        "protocol_version",
        "worker_revision",
        "worker_provenance",
        "operations",
    }


def test_descriptor_validates_identity_request_and_result_independently():
    descriptor = make_catalog_descriptor()

    assert validate_operation_request(
        descriptor,
        operation_id="catalog-test",
        operation_version="1",
        parameters={"values": [1, 2]},
    ) == {"values": [1, 2]}
    assert validate_operation_result(descriptor, {"total": 3}) == {"total": 3}

    invalid_requests = [
        {"operation_id": "other", "operation_version": "1", "parameters": {"values": [1]}},
        {"operation_id": "catalog-test", "operation_version": "2", "parameters": {"values": [1]}},
        {"operation_id": "catalog-test", "operation_version": "1", "parameters": {"values": [1], "x": 2}},
        {"operation_id": "catalog-test", "operation_version": "1", "parameters": {"values": (1,)}},
        {"operation_id": "catalog-test", "operation_version": "1", "parameters": {"values": [float("nan")]}},
    ]
    for request in invalid_requests:
        with pytest.raises(ContractValidationError):
            validate_operation_request(descriptor, **request)

    with pytest.raises(ContractValidationError):
        validate_operation_result(descriptor, {"total": 3, "extra": True})
    with pytest.raises(ContractValidationError):
        validate_operation_result(descriptor, {"total": float("inf")})


def test_worker_catalog_rejects_duplicate_operation_versions():
    descriptor = make_catalog_descriptor()
    with pytest.raises(ValidationError):
        WorkerCatalog(
            protocol_version="2",
            worker_revision="revision",
            worker_provenance={},
            operations=[descriptor, descriptor],
        )


def test_simulated_count_contract_is_bounded_and_closed():
    request = SimulatedCountRequest.model_validate_json('{"detectors":["det"],"num":10,"delay":10.0}')
    result = SimulatedCountResult(run_uids=[uuid4_string()])

    assert request.detectors == ["det"]
    assert SIMULATED_COUNT_DESCRIPTOR.operation_id == SIMULATED_COUNT_OPERATION_ID == "simulated-count"
    assert SIMULATED_COUNT_DESCRIPTOR.operation_version == SIMULATED_COUNT_OPERATION_VERSION == "1"
    assert SIMULATED_COUNT_DESCRIPTOR.required_scope is AuthorizationScope.CONTROL
    assert SIMULATED_COUNT_DESCRIPTOR.orphan_policy is OrphanPolicy.REQUEST_STOP
    assert validate_operation_result(
        SIMULATED_COUNT_DESCRIPTOR, result.model_dump(mode="json")
    ) == result.model_dump(mode="json")

    invalid_requests = [
        {"detectors": []},
        {"detectors": ["det", "det"]},
        {"detectors": ["other"]},
        {"detectors": ("det",)},
        {"detectors": ["det"], "num": 0},
        {"detectors": ["det"], "num": 11},
        {"detectors": ["det"], "delay": -0.1},
        {"detectors": ["det"], "delay": 10.1},
        {"detectors": ["det"], "unexpected": True},
    ]
    for parameters in invalid_requests:
        with pytest.raises(ValidationError):
            SimulatedCountRequest.model_validate(parameters)


def test_simulated_count_schema_accepts_only_json_lists():
    parameters = {"detectors": ["det"], "num": 1, "delay": 0.0}
    assert (
        validate_operation_request(
            SIMULATED_COUNT_DESCRIPTOR,
            operation_id=SIMULATED_COUNT_OPERATION_ID,
            operation_version=SIMULATED_COUNT_OPERATION_VERSION,
            parameters=parameters,
        )
        == parameters
    )

    with pytest.raises(ContractValidationError):
        validate_operation_request(
            SIMULATED_COUNT_DESCRIPTOR,
            operation_id=SIMULATED_COUNT_OPERATION_ID,
            operation_version=SIMULATED_COUNT_OPERATION_VERSION,
            parameters={"detectors": ("det",)},
        )


def test_api_mutation_inputs_reject_untrusted_identity_and_invalid_bounds():
    assert LeaseRequest().ttl_seconds == 300
    assert (
        QueueExecutionStart.model_validate_json('{"policy":"stop_on_non_success"}').policy
        is QueueExecutionPolicy.STOP_ON_NON_SUCCESS
    )
    submission = OperationSubmission.model_validate_json(
        '{"operation_id":"simulated-count","operation_version":"1","parameters":{"detectors":["det"]}}'
    )
    assert submission.parameters == {"detectors": ["det"]}

    for ttl in (4, 3601, "300"):
        with pytest.raises(ValidationError):
            LeaseRequest(ttl_seconds=ttl)
    with pytest.raises(ValidationError):
        OperationSubmission(
            operation_id="simulated-count",
            operation_version="1",
            parameters={"detectors": ["det"]},
            subject="mallory",
        )
    with pytest.raises(ValidationError):
        QueueReorder(operation_uids=[uuid4_string(), uuid4_string(), "not-a-uuid"])
    duplicate_uid = uuid4_string()
    with pytest.raises(ValidationError):
        QueueReorder(operation_uids=[duplicate_uid, duplicate_uid])
    for value in ("", "   ", "x" * 1001):
        with pytest.raises(ValidationError):
            MutationReason(reason=value)
        with pytest.raises(ValidationError):
            RecoveryAcknowledgement(note=value)


def test_api_views_serialize_timestamps_without_internal_lease_identity():
    lease = ControlLeaseView(holder="operator", expires_at=1)
    payload = json.loads(lease.model_dump_json())

    assert payload == {"holder": "operator", "expires_at": "1970-01-01T00:00:00.000001Z"}
    assert "lease_uid" not in ControlLeaseView.model_fields

    health = HealthView(package_version="2.0.0")
    readiness = ReadinessView(
        ready=False,
        controller_lock="held",
        storage="ready",
        worker="not_ready",
        fencing="blocked",
        dispatch="blocked",
    )
    assert health.model_dump() == {
        "status": "ok",
        "package_version": "2.0.0",
        "worker_revision": None,
        "environment_lock_sha256": None,
        "source_sha256": None,
    }
    assert set(readiness.model_dump()) == {"ready", "controller_lock", "storage", "worker", "fencing", "dispatch"}


def test_error_response_is_strict_and_json_safe():
    response = ErrorResponse(
        error=ErrorBody(code="conflict", message="state changed", details={"revision": 2}, request_id="request")
    )
    assert response.error.details == {"revision": 2}
    with pytest.raises(ValidationError):
        ErrorBody(code="conflict", message="state changed", details={"bad": float("nan")}, request_id="request")


def test_controller_config_loads_canonical_offline_simulator_paths(tmp_path):
    config_path = tmp_path / "controller.yml"
    config_path.write_text(
        f"""
instrument_id: simulator
database_path: {tmp_path / "state.sqlite"}
worker:
  command: [qserver-v2-worker, --config, {tmp_path / "worker.yml"}]
oidc:
  issuer: https://issuer.example
  audience: queueserver
  jwks_path: {tmp_path / "jwks.json"}
tls:
  certificate_path: {tmp_path / "server.crt"}
  private_key_path: {tmp_path / "server.key"}
offline_simulator: true
""",
        encoding="utf-8",
    )

    config = load_controller_config(config_path)

    assert config.database_path == (tmp_path / "state.sqlite").resolve()
    assert config.worker.startup_timeout_seconds == 10
    assert config.worker.heartbeat_interval_seconds == 1
    assert config.worker.heartbeat_timeout_seconds == 5
    assert config.oidc.http_timeout_seconds == 5
    assert config.oidc.cache_seconds == 300
    assert config.oidc.clock_skew_seconds == 30


def test_controller_config_rejects_unsafe_jwks_and_worker_command(tmp_path):
    oidc = OidcConfig(
        issuer="https://issuer.example",
        audience="queueserver",
        jwks_path=tmp_path / "jwks.json",
    )
    with pytest.raises(ValidationError):
        ControllerConfig(
            instrument_id="simulator",
            database_path=tmp_path / "state.sqlite",
            worker=WorkerLaunchConfig(command=["qserver-v2-worker"]),
            oidc=oidc,
            tls={"certificate_path": tmp_path / "server.crt", "private_key_path": tmp_path / "server.key"},
        )
    with pytest.raises(ValidationError):
        OidcConfig(issuer="issuer", audience="queueserver", jwks_url="http://issuer.example/jwks")
    with pytest.raises(ValidationError):
        OidcConfig(
            issuer="issuer",
            audience="queueserver",
            jwks_url="https://issuer.example/jwks",
            jwks_path=tmp_path / "jwks.json",
        )
    with pytest.raises(ValidationError):
        WorkerLaunchConfig(command=["qserver-v2-worker", "--ipc-fd=4"])
    with pytest.raises(ValidationError):
        WorkerLaunchConfig(command="qserver-v2-worker")


def test_worker_runtime_config_enforces_provider_boundary(tmp_path):
    config_path = tmp_path / "worker.yml"
    config_path.write_text(
        "provider: simulated-count\nenvironment_lock_sha256: " + "a" * 64 + "\n",
        encoding="utf-8",
    )
    config = load_worker_runtime_config(config_path)
    assert config.provider == "simulated-count"

    with pytest.raises(ValidationError):
        WorkerRuntimeConfig(
            provider="simulated-count",
            environment_lock_sha256="a" * 64,
            startup_directory=tmp_path,
            adapter_path=tmp_path / "adapter.py",
        )
    with pytest.raises(ValidationError):
        WorkerRuntimeConfig(provider="profile", environment_lock_sha256="a" * 64)
    with pytest.raises(ValidationError):
        WorkerRuntimeConfig(provider="simulated-count", environment_lock_sha256="A" * 64)
    with pytest.raises(ValidationError):
        WorkerRuntimeConfig(provider="simulated-count", environment_lock_sha256="a" * 64, ipc_fd=3)


def test_authorization_scope_hierarchy_is_fixed():
    assert effective_authorization_scopes(["queueserver:read", "unrelated"]) == {AuthorizationScope.READ}
    assert effective_authorization_scopes(["queueserver:control"]) == {
        AuthorizationScope.READ,
        AuthorizationScope.CONTROL,
    }
    assert effective_authorization_scopes(["queueserver:admin"]) == set(AuthorizationScope)
    assert scope_allows(["queueserver:admin"], AuthorizationScope.CONTROL)
    assert not scope_allows(["queueserver:read"], AuthorizationScope.CONTROL)
