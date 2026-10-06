import asyncio
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from bluesky_queueserver.v2.contracts import (
    SIMULATED_COUNT_DESCRIPTOR,
    AttemptState,
    AuthorizationScope,
    OperationState,
    QueueExecutionState,
    RecoveryAcknowledgement,
    WorkerCatalog,
)
from bluesky_queueserver.v2.controller import ControllerService, queue_etag
from bluesky_queueserver.v2.storage import IdempotencyRequest, SQLiteStore, StateConflictError
from bluesky_queueserver.v2.worker.runtime import WorkerAuthorityLock
from bluesky_queueserver.v2.worker_protocol import SubprocessWorkerGateway


def run(coroutine):
    return asyncio.run(coroutine)


async def acknowledge_recovery(service, *, revision, note, key):
    if_match = queue_etag(revision)
    acknowledgement = RecoveryAcknowledgement(note=note)
    request = IdempotencyRequest(
        principal="operator",
        method="POST",
        target="/api/v2/recovery/acknowledge",
        key=key,
        body=acknowledgement.model_dump(mode="json"),
        if_match=if_match,
    )
    return await service.acknowledge_recovery(
        request,
        principal="operator",
        scopes=[AuthorizationScope.CONTROL],
        if_match=if_match,
        acknowledgement=acknowledgement,
        timestamp=service.store.now_micros(),
    )


class CountingWorker:
    def __init__(self, lock_path):
        self.worker_instance_uid = str(uuid4())
        self.lock_path = lock_path
        self.pid = None
        self.catalog = WorkerCatalog(
            protocol_version="2",
            worker_revision="sha256:" + "2" * 64,
            worker_provenance={"provider": "replacement"},
            operations=[SIMULATED_COUNT_DESCRIPTOR],
        )
        self.start_count = 0

    async def start(self):
        self.start_count += 1

    async def start_execution(self, *, attempt, descriptor, parameters):
        raise AssertionError("recovery must not resume or dispatch queued work")

    async def request_safe_stop(self, *, attempt_uid):
        raise AssertionError("replacement worker has no active attempt")

    async def close(self):
        return 0

    async def disconnect(self):
        return 0


@pytest.mark.parametrize("attempt_state", [AttemptState.CLAIMED, AttemptState.RUNNING])
def test_restart_requires_worker_fence_and_acknowledgement(tmp_path, attempt_state):
    async def scenario():
        database_path = tmp_path / "state.sqlite"
        old_lock = WorkerAuthorityLock(f"{database_path}.worker.lock", str(uuid4()))
        old_lock.acquire()
        old_worker_uid = str(uuid4())
        old_store = SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 100)
        await old_store.open()
        await old_store.acquire_control_lease(principal="operator")
        catalog = WorkerCatalog(
            protocol_version="2",
            worker_revision="sha256:" + "1" * 64,
            worker_provenance={"provider": "old"},
            operations=[SIMULATED_COUNT_DESCRIPTOR],
        )
        await old_store.register_ready_worker(
            worker_instance_uid=old_worker_uid,
            catalog=catalog,
            lock_path=old_store.worker_lock_path,
            pid=None,
        )
        first, revision = await old_store.submit_operation(
            principal="operator",
            expected_revision=0,
            descriptor=SIMULATED_COUNT_DESCRIPTOR,
            operation_id="simulated-count",
            operation_version="1",
            parameters={"detectors": ["det"]},
        )
        second, revision = await old_store.submit_operation(
            principal="operator",
            expected_revision=revision,
            descriptor=SIMULATED_COUNT_DESCRIPTOR,
            operation_id="simulated-count",
            operation_version="1",
            parameters={"detectors": ["det"], "num": 2},
        )
        execution, _ = await old_store.start_queue_execution(
            principal="operator",
            expected_revision=revision,
        )
        dispatch = await old_store.claim_next_attempt(worker_instance_uid=old_worker_uid)
        assert dispatch is not None
        if attempt_state is AttemptState.RUNNING:
            await old_store.mark_attempt_running(attempt_uid=dispatch.attempt.attempt_uid)
        await old_store.close()

        replacement = CountingWorker(Path(f"{database_path}.worker.lock"))
        service = ControllerService(
            SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 200),
            worker=replacement,
        )
        await service.open()
        try:
            assert not service.dispatch_ready
            assert replacement.start_count == 0
            recovered = await service.store.get_attempt(dispatch.attempt.attempt_uid)
            assert recovered.state is AttemptState.UNKNOWN
            block = await service.store.get_active_dispatch_block()
            assert block is not None
            assert block.requires_fence
            assert block.fenced_at is None
            revision = await service.store.current_revision()

            with pytest.raises(StateConflictError, match="authority has not ended"):
                await acknowledge_recovery(
                    service,
                    revision=revision,
                    note="investigated",
                    key=f"recovery-held-{attempt_state.value}",
                )
            assert replacement.start_count == 0

            old_lock.release()
            await acknowledge_recovery(
                service,
                revision=revision,
                note="old worker lock released",
                key=f"recovery-released-{attempt_state.value}",
            )
            revision = await service.store.current_revision()
            assert revision > 0
            assert replacement.start_count == 1
            assert service.dispatch_ready
            assert await service.store.get_active_dispatch_block() is None
            assert (
                await service.store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.STOPPED
            assert [item.operation_uid for item in (await service.store.queue_snapshot()).operations] == [
                second.operation_uid
            ]
            assert (await service.store.get_operation(first.operation_uid)).state.value == "unknown"
        finally:
            old_lock.release()
            await service.close()

    run(scenario())


async def wait_for_operation_state(store, operation_uid, state):
    cursor = 0
    while True:
        events = await store.wait_for_events(after=cursor, timeout=5)
        assert events, f"timed out waiting for operation {state.value}"
        for event in events:
            cursor = event.event_id
            if event.operation_uid == operation_uid and event.event_type == f"operation.{state.value}":
                return


def test_graceful_shutdown_commits_interrupted_after_worker_fence(tmp_path):
    async def scenario():
        database_path = tmp_path / "state.sqlite"
        worker_config = tmp_path / "worker.yml"
        worker_config.write_text(
            "provider: simulated-count\nenvironment_lock_sha256: " + "a" * 64 + "\n",
            encoding="utf-8",
        )
        store = SQLiteStore(database_path, instrument_id="instrument")
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
        await store.acquire_control_lease(principal="operator")
        operation, revision = await store.submit_operation(
            principal="operator",
            expected_revision=0,
            descriptor=SIMULATED_COUNT_DESCRIPTOR,
            operation_id="simulated-count",
            operation_version="1",
            parameters={"detectors": ["det"], "num": 10, "delay": 0.2},
        )
        await store.start_queue_execution(
            principal="operator",
            expected_revision=revision,
        )
        service.wake_scheduler()
        await wait_for_operation_state(store, operation.operation_uid, OperationState.RUNNING)
        attempt = await store.get_active_attempt()
        assert attempt is not None

        await service.close()

        async with SQLiteStore(database_path, instrument_id="instrument", allow_initialize=False) as reopened:
            terminal = await reopened.get_attempt(attempt.attempt_uid)
            block = await reopened.get_active_dispatch_block()
            assert terminal.state is AttemptState.INTERRUPTED
            assert terminal.cleanup_completed is True
            assert block is not None
            assert block.kind == "interrupted"
            assert block.requires_fence
            assert block.fenced_at is not None
            event_types = [event.event_type for event in await reopened.list_events()]
            assert event_types.index("controller.shutdown_requested") < event_types.index("operation.interrupted")
            assert event_types.index("operation.interrupted") < event_types.index("worker.fenced")

    run(scenario())


def test_clean_failure_requires_acknowledgement_but_not_fence_evidence(tmp_path):
    async def scenario():
        database_path = tmp_path / "state.sqlite"
        old_store = SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 100)
        await old_store.open()
        await old_store.acquire_control_lease(principal="operator")
        old_worker_uid = str(uuid4())
        catalog = WorkerCatalog(
            protocol_version="2",
            worker_revision="sha256:" + "1" * 64,
            worker_provenance={"provider": "old"},
            operations=[SIMULATED_COUNT_DESCRIPTOR],
        )
        await old_store.register_ready_worker(
            worker_instance_uid=old_worker_uid,
            catalog=catalog,
            lock_path=old_store.worker_lock_path,
            pid=None,
        )
        operation, revision = await old_store.submit_operation(
            principal="operator",
            expected_revision=0,
            descriptor=SIMULATED_COUNT_DESCRIPTOR,
            operation_id="simulated-count",
            operation_version="1",
            parameters={"detectors": ["det"]},
        )
        execution, _ = await old_store.start_queue_execution(
            principal="operator",
            expected_revision=revision,
        )
        dispatch = await old_store.claim_next_attempt(worker_instance_uid=old_worker_uid)
        await old_store.mark_attempt_running(attempt_uid=dispatch.attempt.attempt_uid)
        await old_store.complete_attempt(
            attempt_uid=dispatch.attempt.attempt_uid,
            state=AttemptState.FAILED,
            result=None,
            run_uids=(),
            diagnostic="proved failure",
            cleanup_completed=True,
        )
        await old_store.close()

        replacement = CountingWorker(Path(f"{database_path}.worker.lock"))
        service = ControllerService(
            SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 200),
            worker=replacement,
        )
        await service.open()
        try:
            block = await service.store.get_active_dispatch_block()
            assert block is not None
            assert not block.requires_fence
            assert replacement.start_count == 0
            revision = await service.store.current_revision()
            await acknowledge_recovery(
                service,
                revision=revision,
                note="failure reviewed",
                key="recovery-clean-failure",
            )
            assert replacement.start_count == 1
            assert (
                await service.store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.STOPPED
            assert (await service.store.get_operation(operation.operation_uid)).state.value == "failed"
        finally:
            await service.close()

    run(scenario())
