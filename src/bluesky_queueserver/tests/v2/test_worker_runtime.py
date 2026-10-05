import asyncio
import struct
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from bluesky_queueserver.v2.contracts import (
    AttemptState,
    AuthorizationScope,
    OperationState,
    OperationSubmission,
    OrphanPolicy,
    StrictModel,
)
from bluesky_queueserver.v2.controller import ControllerService, queue_etag
from bluesky_queueserver.v2.storage import SQLiteStore
from bluesky_queueserver.v2.worker.profile import ProfileLoadError, load_profile
from bluesky_queueserver.v2.worker.runtime import WorkerAuthorityError, WorkerAuthorityLock
from bluesky_queueserver.v2.worker.sdk import (
    OperationContext,
    OperationRegistry,
    PreparedOperation,
    build_worker_catalog,
)
from bluesky_queueserver.v2.worker.simulated_count import register_simulated_count, simulated_devices
from bluesky_queueserver.v2.worker_protocol import (
    MAX_FRAME_BYTES,
    FrameTooLargeError,
    MessageType,
    SubprocessWorkerGateway,
    WorkerMessage,
    WorkerProtocolError,
    decode_frame,
    encode_frame,
    new_message,
    read_frame,
    validate_payload,
)


class Request(StrictModel):
    value: int


class Result(StrictModel):
    value: int


def example_plan():
    yield "message"


def register_example(registry):
    @registry.register(
        operation_id="example",
        operation_version="1",
        request_model=Request,
        result_model=Result,
        required_scope=AuthorizationScope.CONTROL,
        orphan_policy=OrphanPolicy.REQUEST_STOP,
    )
    def handler(request, context):
        return PreparedOperation(
            plan=example_plan(),
            build_result=lambda run_uids: Result(value=request.value),
        )

    return handler


def test_registry_generates_catalog_and_validates_both_sides():
    registry = OperationRegistry()
    register_example(registry)
    context = OperationContext(run_engine=object(), devices={"det": object()})

    registered, prepared = registry.prepare(
        operation_id="example",
        operation_version="1",
        parameters={"value": 5},
        context=context,
    )
    assert next(prepared.plan) == "message"
    result = prepared.build_result(())
    assert registry.validate_result(registered, result) == {"value": 5}

    catalog = registry.catalog(protocol_version="2", worker_revision="revision", provenance={"source": "test"})
    assert [(item.operation_id, item.operation_version) for item in catalog.operations] == [("example", "1")]
    assert catalog.operations[0].request_schema["additionalProperties"] is False

    with pytest.raises(ValueError):
        registry.prepare(
            operation_id="example",
            operation_version="1",
            parameters={"value": 5, "extra": True},
            context=context,
        )
    with pytest.raises(ValidationError):
        registry.validate_result(registered, {"value": "5"})


def test_registry_rejects_duplicate_open_or_invalid_registrations():
    registry = OperationRegistry()
    register_example(registry)
    with pytest.raises(ValueError, match="already registered"):
        register_example(registry)

    class UnstrictRequest(BaseModel):
        value: int

    with pytest.raises(TypeError, match="frozen, strict"):
        OperationRegistry().register(
            operation_id="bad",
            operation_version="1",
            request_model=UnstrictRequest,
            result_model=Result,
            required_scope=AuthorizationScope.CONTROL,
            orphan_policy=OrphanPolicy.REQUEST_STOP,
        )

    class OpenRequest(StrictModel):
        model_config = ConfigDict(extra="allow", frozen=True, strict=True)
        value: int

    with pytest.raises(TypeError, match="forbid extra"):
        OperationRegistry().register(
            operation_id="bad",
            operation_version="1",
            request_model=OpenRequest,
            result_model=Result,
            required_scope=AuthorizationScope.CONTROL,
            orphan_policy=OrphanPolicy.REQUEST_STOP,
        )

    with pytest.raises(TypeError, match="exactly"):
        OperationRegistry().register(
            operation_id="bad-signature",
            operation_version="1",
            request_model=Request,
            result_model=Result,
            required_scope=AuthorizationScope.CONTROL,
            orphan_policy=OrphanPolicy.REQUEST_STOP,
        )(lambda request: None)


def test_operation_context_exposes_read_only_namespaces():
    context = OperationContext(run_engine=object(), devices={"det": 1}, profile={"plan": 2})
    with pytest.raises(TypeError):
        context.devices["other"] = 3
    with pytest.raises(TypeError):
        context.profile["other"] = 3


def test_profile_loader_executes_top_level_files_in_lexical_order(tmp_path):
    startup = tmp_path / "startup"
    startup.mkdir()
    (startup / "10-second.ipy").write_text('order.append("second")\n', encoding="utf-8")
    (startup / "00-first.py").write_text(
        """
from bluesky_queueserver.v2.contracts import StrictModel
order = ["first"]
class ProfileRequest(StrictModel):
    value: int
class ProfileResult(StrictModel):
    value: int
def profile_plan():
    yield "profile-message"
""",
        encoding="utf-8",
    )
    nested = startup / "nested"
    nested.mkdir()
    (nested / "05-ignored.py").write_text('raise RuntimeError("must not execute")\n', encoding="utf-8")
    adapter = tmp_path / "adapter.py"
    adapter.write_text(
        """
from bluesky_queueserver.v2.contracts import AuthorizationScope, OrphanPolicy
from bluesky_queueserver.v2.worker.sdk import PreparedOperation
order.append("adapter")
def register_operations(registry, profile):
    @registry.register(
        operation_id="profile-op",
        operation_version="1",
        request_model=ProfileRequest,
        result_model=ProfileResult,
        required_scope=AuthorizationScope.CONTROL,
        orphan_policy=OrphanPolicy.REQUEST_STOP,
    )
    def prepare(request, context):
        return PreparedOperation(
            plan=profile_plan(),
            build_result=lambda run_uids: ProfileResult(value=request.value),
        )
""",
        encoding="utf-8",
    )
    registry = OperationRegistry()

    loaded = load_profile(startup_directory=startup, adapter_path=adapter, registry=registry)

    assert loaded.namespace["order"] == ["first", "second", "adapter"]
    assert list(loaded.startup_hashes) == [
        str((startup / "00-first.py").resolve()),
        str((startup / "10-second.ipy").resolve()),
    ]
    assert len(loaded.adapter_sha256) == 64
    assert [(item.operation_id, item.operation_version) for item in registry.descriptors] == [("profile-op", "1")]


def test_profile_loader_requires_exact_registration_entry_point(tmp_path):
    startup = tmp_path / "startup"
    startup.mkdir()
    adapter = tmp_path / "adapter.py"
    adapter.write_text("value = 1\n", encoding="utf-8")

    with pytest.raises(ProfileLoadError, match="register_operations"):
        load_profile(startup_directory=startup, adapter_path=adapter, registry=OperationRegistry())


def test_native_simulator_registers_only_reviewed_operation():
    registry = OperationRegistry()
    register_simulated_count(registry)

    assert [(item.operation_id, item.operation_version) for item in registry.descriptors] == [
        ("simulated-count", "1")
    ]
    assert set(simulated_devices()) == {"det"}


def test_worker_revision_covers_contract_environment_and_sources():
    registry = OperationRegistry()
    register_example(registry)

    native = build_worker_catalog(registry, environment_lock_sha256="a" * 64)
    same = build_worker_catalog(registry, environment_lock_sha256="a" * 64)
    changed = build_worker_catalog(registry, environment_lock_sha256="b" * 64)

    assert native.worker_revision == same.worker_revision
    assert native.worker_revision != changed.worker_revision
    assert native.worker_revision.startswith("sha256:")
    assert native.worker_provenance["startup_source_sha256"] is None
    assert native.worker_provenance["adapter_source_sha256"] is None
    assert native.worker_provenance["operations"] == [native.operations[0].model_dump(mode="json")]


def test_protocol_frame_round_trip_and_exact_envelope():
    worker_uid = __import__("uuid").uuid4()
    attempt_uid = __import__("uuid").uuid4()
    message = new_message(
        message_type=MessageType.EXECUTE_REQUEST,
        worker_instance_uid=str(worker_uid),
        attempt_uid=str(attempt_uid),
        payload={"operation_id": "example", "operation_version": "1", "parameters": {"value": 1}},
    )
    frame = encode_frame(message)

    assert struct.unpack(">I", frame[:4])[0] == len(frame) - 4
    assert decode_frame(frame[4:]) == message
    assert message.correlation_id is None

    with pytest.raises(ValidationError):
        WorkerMessage.model_validate_json(message.model_dump_json(exclude={"message_id"}))
    with pytest.raises(WorkerProtocolError):
        decode_frame(message.model_dump_json().replace('"attempt_uid":"', '"attempt_uid":null,"old":"').encode())


def test_protocol_rejects_oversized_length_before_body_allocation():
    async def scenario():
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack(">I", MAX_FRAME_BYTES + 1))
        with pytest.raises(FrameTooLargeError):
            await read_frame(reader)

    asyncio.run(scenario())


def test_protocol_enforces_attempt_and_correlation_semantics():
    worker_uid = str(__import__("uuid").uuid4())
    attempt_uid = str(__import__("uuid").uuid4())
    request = new_message(
        message_type=MessageType.SAFE_STOP_REQUEST,
        worker_instance_uid=worker_uid,
        attempt_uid=attempt_uid,
        payload={},
    )
    response = new_message(
        message_type=MessageType.SAFE_STOP_ACKNOWLEDGED,
        worker_instance_uid=worker_uid,
        attempt_uid=attempt_uid,
        correlation_id=request.message_id,
        payload={},
    )
    assert validate_payload(response).model_dump() == {}

    with pytest.raises(ValidationError, match="attempt_uid"):
        WorkerMessage(
            protocol_version="2",
            message_type=MessageType.PING,
            message_id=str(__import__("uuid").uuid4()),
            correlation_id=None,
            worker_instance_uid=worker_uid,
            attempt_uid=attempt_uid,
            payload={"sent_at": 1},
        )
    with pytest.raises(ValidationError, match="correlate"):
        WorkerMessage(
            protocol_version="2",
            message_type=MessageType.PONG,
            message_id=str(__import__("uuid").uuid4()),
            correlation_id=None,
            worker_instance_uid=worker_uid,
            attempt_uid=None,
            payload={"sent_at": 1},
        )


async def wait_for_operation_state(store, operation_uid, state):
    cursor = 0
    while True:
        events = await store.wait_for_events(after=cursor, timeout=5)
        assert events, f"timed out waiting for operation {state.value}"
        for event in events:
            cursor = event.event_id
            if event.operation_uid == operation_uid and event.event_type == f"operation.{state.value}":
                return
            if event.operation_uid == operation_uid and event.event_type in {
                "operation.failed",
                "operation.aborted",
                "operation.interrupted",
                "operation.unknown",
            }:
                raise AssertionError(f"unexpected terminal event: {event.model_dump(mode='json')}")


def test_real_worker_runs_simulated_count_and_stays_ping_responsive(tmp_path):
    async def scenario():
        worker_config = tmp_path / "worker.yml"
        worker_config.write_text(
            "provider: simulated-count\nenvironment_lock_sha256: " + "a" * 64 + "\n",
            encoding="utf-8",
        )
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        gateway = SubprocessWorkerGateway(
            command=[
                sys.executable,
                "-m",
                "bluesky_queueserver.v2.worker.runtime",
                "--config",
                str(worker_config),
            ],
            worker_lock_path=store.worker_lock_path,
            instrument_id="instrument",
        )
        service = ControllerService(store, worker=gateway)
        await service.open()
        try:
            assert [(item.operation_id, item.operation_version) for item in gateway.catalog.operations] == [
                ("simulated-count", "1")
            ]
            competing_lock = WorkerAuthorityLock(store.worker_lock_path, str(uuid4()))
            with pytest.raises(WorkerAuthorityError, match="already held"):
                competing_lock.acquire()
            health = service.health()
            assert health.worker_revision == gateway.catalog.worker_revision
            assert health.environment_lock_sha256 == "a" * 64
            assert health.source_sha256 == {
                "startup_source_sha256": None,
                "adapter_source_sha256": None,
            }
            await service.acquire_control_lease(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
            )
            operation, revision = await service.submit_operation(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(0),
                submission=OperationSubmission(
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"], "num": 3, "delay": 0.2},
                ),
            )
            await service.start_queue_execution(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(revision),
            )
            await wait_for_operation_state(store, operation.operation_uid, OperationState.RUNNING)
            await gateway.ping()
            await wait_for_operation_state(store, operation.operation_uid, OperationState.SUCCEEDED)
            await service.wait_scheduler_idle()
            completed = await store.get_operation(operation.operation_uid)
            assert completed.state is OperationState.SUCCEEDED
            assert len(completed.result["run_uids"]) == 1
        finally:
            await service.close()

        probe = WorkerAuthorityLock(store.worker_lock_path, str(uuid4()))
        probe.acquire()
        probe.release()

    asyncio.run(scenario())


def make_real_service(tmp_path):
    worker_config = tmp_path / "worker.yml"
    worker_config.write_text(
        "provider: simulated-count\nenvironment_lock_sha256: " + "a" * 64 + "\n",
        encoding="utf-8",
    )
    store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
    gateway = SubprocessWorkerGateway(
        command=[
            sys.executable,
            "-m",
            "bluesky_queueserver.v2.worker.runtime",
            "--config",
            str(worker_config),
        ],
        worker_lock_path=store.worker_lock_path,
        instrument_id="instrument",
    )
    return store, gateway, ControllerService(store, worker=gateway)


@pytest.mark.parametrize("stop_after_current", [False, True])
def test_real_worker_safe_stop_aborts_at_checkpoint(tmp_path, stop_after_current):
    async def scenario():
        store, gateway, service = make_real_service(tmp_path)
        await service.open()
        try:
            await service.acquire_control_lease(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
            )
            operation, revision = await service.submit_operation(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(0),
                submission=OperationSubmission(
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"], "num": 10, "delay": 0.2},
                ),
            )
            execution, _ = await service.start_queue_execution(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(revision),
            )
            await wait_for_operation_state(store, operation.operation_uid, OperationState.RUNNING)
            revision = await store.current_revision()
            if stop_after_current:
                _, revision = await service.stop_queue_execution(
                    principal="operator",
                    scopes=[AuthorizationScope.CONTROL],
                    if_match=queue_etag(revision),
                    queue_execution_uid=execution.queue_execution_uid,
                )
            attempt = await store.get_active_attempt()
            assert attempt is not None
            await service.safe_stop_attempt(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(revision),
                attempt_uid=attempt.attempt_uid,
            )
            await wait_for_operation_state(
                store,
                operation.operation_uid,
                OperationState.ABORTED,
            )
            await service.wait_scheduler_idle()
            terminal = await store.get_attempt(attempt.attempt_uid)
            assert terminal.state is AttemptState.ABORTED
            assert terminal.cleanup_completed is True
            assert terminal.stop_acknowledged_at is not None
            assert (await store.get_queue_execution(execution.queue_execution_uid)).state.value == "stopped"
        finally:
            await service.close()

    asyncio.run(scenario())


def test_controller_socket_loss_requests_orphan_stop_and_releases_worker_lock(tmp_path):
    async def scenario():
        store, gateway, service = make_real_service(tmp_path)
        await service.open()
        try:
            await service.acquire_control_lease(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
            )
            operation, revision = await service.submit_operation(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(0),
                submission=OperationSubmission(
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"], "num": 10, "delay": 0.2},
                ),
            )
            await service.start_queue_execution(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(revision),
            )
            await wait_for_operation_state(store, operation.operation_uid, OperationState.RUNNING)
            exit_code = await gateway.disconnect()
            assert exit_code == 2
            await wait_for_operation_state(store, operation.operation_uid, OperationState.UNKNOWN)
            await service.wait_scheduler_idle()
            probe = WorkerAuthorityLock(store.worker_lock_path, str(uuid4()))
            probe.acquire()
            probe.release()
        finally:
            await service.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "mode",
    [
        "timeout",
        "eof",
        "exit",
        "malformed",
        "oversized",
        "wrong-protocol",
        "wrong-worker",
        "wrong-attempt",
        "wrong-correlation",
        "invalid-result",
    ],
)
def test_post_claim_protocol_fault_becomes_unknown_once(tmp_path, mode):
    async def scenario():
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        gateway = SubprocessWorkerGateway(
            command=[sys.executable, str(Path(__file__).with_name("fault_worker.py")), "--mode", mode],
            worker_lock_path=store.worker_lock_path,
            instrument_id="instrument",
            heartbeat_interval_seconds=0.05,
            heartbeat_timeout_seconds=0.2,
        )
        service = ControllerService(store, worker=gateway, protocol_timeout_seconds=0.5)
        await service.open()
        try:
            await service.acquire_control_lease(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
            )
            revision = 0
            operations = []
            for num in (1, 2):
                operation, revision = await service.submit_operation(
                    principal="operator",
                    scopes=[AuthorizationScope.CONTROL],
                    if_match=queue_etag(revision),
                    submission=OperationSubmission(
                        operation_id="simulated-count",
                        operation_version="1",
                        parameters={"detectors": ["det"], "num": num},
                    ),
                )
                operations.append(operation)
            execution, _ = await service.start_queue_execution(
                principal="operator",
                scopes=[AuthorizationScope.CONTROL],
                if_match=queue_etag(revision),
            )
            await wait_for_operation_state(store, operations[0].operation_uid, OperationState.UNKNOWN)
            await service.wait_scheduler_idle()

            attempt = await store.get_active_attempt()
            assert attempt is None
            assert (await store.get_operation(operations[0].operation_uid)).state is OperationState.UNKNOWN
            assert (await store.get_queue_execution(execution.queue_execution_uid)).state.value == "blocked"
            snapshot = await store.queue_snapshot()
            assert snapshot.dispatch_block_uid is not None
            assert [item.operation_uid for item in snapshot.operations] == [operations[1].operation_uid]
            terminal_events = [
                event
                for event in await store.list_events()
                if event.operation_uid == operations[0].operation_uid and event.event_type == "operation.unknown"
            ]
            assert len(terminal_events) == 1
        finally:
            await service.close()

    asyncio.run(scenario())
