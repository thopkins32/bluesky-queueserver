import asyncio
import sys
from pathlib import Path

import pytest

from bluesky_queueserver._experiment_controller import ExperimentController, SubprocessWorkerClient
from bluesky_queueserver._experiment_controller.contracts import (
    SIMULATED_COUNT_OPERATION_ID,
    SIMULATED_COUNT_OPERATION_VERSION,
    SIMULATED_WORKER_REVISION,
    DispatchBlockedError,
    LeaseExpiredError,
    OperationDescriptor,
    OperationState,
    OperationValidationError,
    RevisionConflictError,
    WorkerCatalog,
    WorkerExecutionResult,
    WorkerTransportError,
)

_PARAMETER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {"num": {"type": "integer", "minimum": 1, "maximum": 10}},
    "additionalProperties": False,
}


def _catalog() -> WorkerCatalog:
    return WorkerCatalog(
        worker_revision=SIMULATED_WORKER_REVISION,
        operations=(
            OperationDescriptor(
                operation_id=SIMULATED_COUNT_OPERATION_ID,
                operation_version=SIMULATED_COUNT_OPERATION_VERSION,
                parameter_schema=_PARAMETER_SCHEMA,
            ),
        ),
    )


class _SuccessWorker:
    def __init__(self):
        self.executed_operation_uids: list[str] = []
        self.closed = False

    async def open(self) -> WorkerCatalog:
        return _catalog()

    async def execute(self, operation):
        self.executed_operation_uids.append(operation.operation_uid)
        return WorkerExecutionResult(
            state=OperationState.SUCCEEDED,
            run_uids=(f"run-{operation.operation_uid}",),
            error_message=None,
        )

    async def close(self) -> None:
        self.closed = True


class _FailingWorker(_SuccessWorker):
    async def execute(self, operation):
        self.executed_operation_uids.append(operation.operation_uid)
        return WorkerExecutionResult(
            state=OperationState.FAILED,
            run_uids=(),
            error_message="simulated operation failure",
        )


class _TransportLosingWorker(_SuccessWorker):
    async def execute(self, operation):
        self.executed_operation_uids.append(operation.operation_uid)
        raise WorkerTransportError("simulated worker transport loss")


class _Clock:
    def __init__(self, value: float):
        self.value = value

    def __call__(self) -> float:
        return self.value


def _subprocess_worker() -> SubprocessWorkerClient:
    return SubprocessWorkerClient(
        (
            sys.executable,
            "-u",
            "-m",
            "bluesky_queueserver._experiment_controller.simulator",
        ),
        response_timeout=30.0,
    )


def test_subprocess_execution_and_file_backed_persistence(tmp_path: Path):
    async def scenario():
        database_path = tmp_path / "controller.sqlite"
        controller = ExperimentController(database_path, _subprocess_worker())
        await controller.open()
        try:
            lease = await controller.acquire_lease("operator", ttl_seconds=60.0)
            queued = await controller.submit_operation(
                "operator",
                lease.lease_uid,
                0,
                SIMULATED_COUNT_OPERATION_ID,
                SIMULATED_COUNT_OPERATION_VERSION,
                {"num": 1},
            )
            final = await controller.dispatch_next("operator", lease.lease_uid, 1)
            assert final is not None
            assert final.operation_uid == queued.operation_uid
            assert final.state is OperationState.SUCCEEDED
            assert final.run_uids

            snapshot = await controller.queue_snapshot()
            events = await controller.events()
            assert snapshot.revision == 3
            assert snapshot.operations == (final,)
            assert [event.event_type for event in events] == [
                "lease.acquired",
                "operation.submitted",
                "operation.started",
                "operation.succeeded",
            ]
            assert [event.event_id for event in events] == sorted(event.event_id for event in events)
            assert await controller.events(events[1].event_id) == events[2:]
        finally:
            await controller.close()

        reopened = ExperimentController(database_path, _subprocess_worker())
        await reopened.open()
        try:
            assert await reopened.queue_snapshot() == snapshot
            assert await reopened.events() == events
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_stale_revision_rejected_without_duplicate_operation(tmp_path: Path):
    async def scenario():
        worker = _SuccessWorker()
        controller = ExperimentController(tmp_path / "controller.sqlite", worker)
        await controller.open()
        try:
            lease = await controller.acquire_lease("operator", ttl_seconds=60.0)
            await controller.submit_operation(
                "operator",
                lease.lease_uid,
                0,
                SIMULATED_COUNT_OPERATION_ID,
                SIMULATED_COUNT_OPERATION_VERSION,
                {"num": 1},
            )

            with pytest.raises(RevisionConflictError) as exc_info:
                await controller.submit_operation(
                    "operator",
                    lease.lease_uid,
                    0,
                    SIMULATED_COUNT_OPERATION_ID,
                    SIMULATED_COUNT_OPERATION_VERSION,
                    {"num": 2},
                )

            assert exc_info.value.expected_revision == 0
            assert exc_info.value.current_revision == 1
            snapshot = await controller.queue_snapshot()
            assert snapshot.revision == 1
            assert len(snapshot.operations) == 1
            assert snapshot.operations[0].parameters == {"num": 1}
            assert [event.event_type for event in await controller.events()] == [
                "lease.acquired",
                "operation.submitted",
            ]
        finally:
            await controller.close()
        assert worker.closed

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("operation_id", "operation_version", "parameters"),
    [
        ("unknown-operation", SIMULATED_COUNT_OPERATION_VERSION, {}),
        (SIMULATED_COUNT_OPERATION_ID, "unknown-version", {}),
        (SIMULATED_COUNT_OPERATION_ID, SIMULATED_COUNT_OPERATION_VERSION, {"num": 0}),
        (SIMULATED_COUNT_OPERATION_ID, SIMULATED_COUNT_OPERATION_VERSION, []),
        (SIMULATED_COUNT_OPERATION_ID, SIMULATED_COUNT_OPERATION_VERSION, {"extra": True}),
    ],
)
def test_invalid_operation_never_mutates_queue(
    tmp_path: Path,
    operation_id: str,
    operation_version: str,
    parameters: object,
):
    async def scenario():
        worker = _SuccessWorker()
        controller = ExperimentController(tmp_path / "controller.sqlite", worker)
        await controller.open()
        try:
            lease = await controller.acquire_lease("operator", ttl_seconds=60.0)
            snapshot_before = await controller.queue_snapshot()
            events_before = await controller.events()

            with pytest.raises(OperationValidationError):
                await controller.submit_operation(
                    "operator",
                    lease.lease_uid,
                    snapshot_before.revision,
                    operation_id,
                    operation_version,
                    parameters,
                )

            assert await controller.queue_snapshot() == snapshot_before
            assert await controller.events() == events_before
            assert worker.executed_operation_uids == []
        finally:
            await controller.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("worker_type", "expected_state", "expected_block_reason", "expected_event_type"),
    [
        (_FailingWorker, OperationState.FAILED, "operation_failed", "operation.failed"),
        (
            _TransportLosingWorker,
            OperationState.UNKNOWN,
            "worker_transport_failure",
            "operation.unknown",
        ),
    ],
)
def test_failure_blocks_dispatch_until_explicit_recovery(
    tmp_path: Path,
    worker_type,
    expected_state: OperationState,
    expected_block_reason: str,
    expected_event_type: str,
):
    async def scenario():
        worker = worker_type()
        controller = ExperimentController(tmp_path / "controller.sqlite", worker)
        await controller.open()
        try:
            lease = await controller.acquire_lease("operator", ttl_seconds=60.0)
            first = await controller.submit_operation(
                "operator",
                lease.lease_uid,
                0,
                SIMULATED_COUNT_OPERATION_ID,
                SIMULATED_COUNT_OPERATION_VERSION,
                {"num": 1},
            )
            terminal = await controller.dispatch_next("operator", lease.lease_uid, 1)
            assert terminal is not None
            assert terminal.operation_uid == first.operation_uid
            assert terminal.state is expected_state
            assert worker.executed_operation_uids == [first.operation_uid]

            blocked = await controller.queue_snapshot()
            assert blocked.revision == 3
            assert blocked.dispatch_block_reason == expected_block_reason
            with pytest.raises(DispatchBlockedError):
                await controller.dispatch_next("operator", lease.lease_uid, blocked.revision)
            assert worker.executed_operation_uids == [first.operation_uid]

            events_before_ack = await controller.events()
            with pytest.raises(ValueError):
                await controller.acknowledge_dispatch_block("operator", lease.lease_uid, blocked.revision, "")
            with pytest.raises(RevisionConflictError) as exc_info:
                await controller.acknowledge_dispatch_block(
                    "operator", lease.lease_uid, blocked.revision - 1, "reviewed"
                )
            assert exc_info.value.expected_revision == blocked.revision - 1
            assert exc_info.value.current_revision == blocked.revision
            assert await controller.queue_snapshot() == blocked
            assert await controller.events() == events_before_ack

            recovered = await controller.acknowledge_dispatch_block(
                "operator", lease.lease_uid, blocked.revision, "reviewed"
            )
            assert recovered.revision == blocked.revision + 1
            assert recovered.dispatch_block_reason is None
            assert recovered.operations == blocked.operations

            second = await controller.submit_operation(
                "operator",
                lease.lease_uid,
                recovered.revision,
                SIMULATED_COUNT_OPERATION_ID,
                SIMULATED_COUNT_OPERATION_VERSION,
                {"num": 2},
            )
            after_submit = await controller.queue_snapshot()
            second_terminal = await controller.dispatch_next("operator", lease.lease_uid, after_submit.revision)
            assert second_terminal is not None
            assert second_terminal.operation_uid == second.operation_uid
            assert worker.executed_operation_uids == [first.operation_uid, second.operation_uid]
            assert worker.executed_operation_uids.count(first.operation_uid) == 1

            final_snapshot = await controller.queue_snapshot()
            persisted_first = next(
                operation
                for operation in final_snapshot.operations
                if operation.operation_uid == first.operation_uid
            )
            assert persisted_first == terminal
            assert expected_event_type in [event.event_type for event in await controller.events()]
        finally:
            await controller.close()
        assert worker.closed

    asyncio.run(scenario())


def test_expired_lease_cannot_mutate_persisted_operation(tmp_path: Path):
    async def scenario():
        clock = _Clock(100.0)
        worker = _SuccessWorker()
        controller = ExperimentController(tmp_path / "controller.sqlite", worker, clock=clock)
        await controller.open()
        try:
            lease = await controller.acquire_lease("operator", ttl_seconds=10.0)
            first = await controller.submit_operation(
                "operator",
                lease.lease_uid,
                0,
                SIMULATED_COUNT_OPERATION_ID,
                SIMULATED_COUNT_OPERATION_VERSION,
                {"num": 1},
            )
            terminal = await controller.dispatch_next("operator", lease.lease_uid, 1)
            assert terminal is not None and terminal.operation_uid == first.operation_uid

            snapshot_before = await controller.queue_snapshot()
            events_before = await controller.events()
            clock.value = 110.0
            with pytest.raises(LeaseExpiredError):
                await controller.submit_operation(
                    "operator",
                    lease.lease_uid,
                    snapshot_before.revision,
                    SIMULATED_COUNT_OPERATION_ID,
                    SIMULATED_COUNT_OPERATION_VERSION,
                    {"num": 2},
                )
            with pytest.raises(LeaseExpiredError):
                await controller.submit_operation(
                    "another-operator",
                    lease.lease_uid,
                    snapshot_before.revision,
                    SIMULATED_COUNT_OPERATION_ID,
                    SIMULATED_COUNT_OPERATION_VERSION,
                    {"num": 2},
                )

            assert await controller.queue_snapshot() == snapshot_before
            assert await controller.events() == events_before
            assert snapshot_before.operations == (terminal,)
        finally:
            await controller.close()
        assert worker.closed

    asyncio.run(scenario())
