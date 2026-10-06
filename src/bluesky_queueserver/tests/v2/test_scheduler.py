import asyncio
from uuid import uuid4

import pytest

from bluesky_queueserver.v2.contracts import (
    SIMULATED_COUNT_DESCRIPTOR,
    AttemptState,
    AuthorizationScope,
    OperationState,
    QueueExecutionState,
    WorkerCatalog,
)
from bluesky_queueserver.v2.controller import (
    AuthorityFileLock,
    ControllerAlreadyRunningError,
    ControllerAuthorityError,
    ControllerService,
    InvalidPreconditionError,
    MissingPreconditionError,
    WorkerCompletion,
    parse_queue_etag,
    queue_etag,
)
from bluesky_queueserver.v2.storage import IdempotencyRequest, SQLiteStore, StateConflictError


def run(coroutine):
    return asyncio.run(coroutine)


async def submit_simulated(store, *, revision, num=1, delay=0.0):
    return await store.submit_operation(
        principal="operator",
        expected_revision=revision,
        descriptor=SIMULATED_COUNT_DESCRIPTOR,
        operation_id="simulated-count",
        operation_version="1",
        parameters={"detectors": ["det"], "num": num, "delay": delay},
    )


async def safe_stop(service, *, attempt_uid, revision, key):
    if_match = queue_etag(revision)
    request = IdempotencyRequest(
        principal="operator",
        method="POST",
        target=f"/api/v2/attempts/{attempt_uid}/safe-stop",
        key=key,
        body={},
        if_match=if_match,
    )
    return await service.safe_stop_attempt(
        request,
        principal="operator",
        scopes=[AuthorizationScope.CONTROL],
        if_match=if_match,
        attempt_uid=attempt_uid,
        timestamp=service.store.now_micros(),
    )


def test_controller_holds_one_authority_lock_and_scheduler(tmp_path):
    async def scenario():
        first = ControllerService(SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument"))
        second = ControllerService(SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument"))
        await first.open()
        try:
            assert first.holds_authority
            assert first.scheduler_running
            with pytest.raises(ControllerAlreadyRunningError):
                await second.open()
            assert not second.holds_authority
        finally:
            await first.close()

        await second.open()
        assert second.holds_authority
        await second.close()

    run(scenario())


def test_authority_lock_rejects_unsafe_parent_and_symlink(tmp_path):
    insecure = tmp_path / "insecure"
    insecure.mkdir(mode=0o777)
    insecure.chmod(0o777)
    try:
        with pytest.raises(ControllerAuthorityError, match="group- or world-writable"):
            AuthorityFileLock(insecure / "controller.lock").acquire()
    finally:
        insecure.chmod(0o700)

    target = tmp_path / "target.lock"
    target.touch(mode=0o600)
    alias = tmp_path / "alias.lock"
    alias.symlink_to(target)
    with pytest.raises(ControllerAuthorityError, match="cannot safely open"):
        AuthorityFileLock(alias).acquire()


class BarrierWorker:
    def __init__(self, lock_path):
        self.worker_instance_uid = str(uuid4())
        self.lock_path = lock_path
        self.pid = None
        self.catalog = WorkerCatalog(
            protocol_version="2",
            worker_revision="sha256:" + "1" * 64,
            worker_provenance={"provider": "test"},
            operations=[SIMULATED_COUNT_DESCRIPTOR],
        )
        self.started = asyncio.Queue()
        self.completions = {}
        self.stop_requests = []

    async def start(self):
        return None

    async def start_execution(self, *, attempt, descriptor, parameters):
        completion = asyncio.get_running_loop().create_future()
        self.completions[attempt.attempt_uid] = completion
        await self.started.put((attempt, descriptor, parameters, completion))
        return completion

    async def request_safe_stop(self, *, attempt_uid):
        self.stop_requests.append(attempt_uid)
        completion = self.completions[attempt_uid]
        if not completion.done():
            completion.set_result(
                WorkerCompletion(
                    state=AttemptState.ABORTED,
                    result=None,
                    run_uids=(),
                    diagnostic="safe stop",
                    cleanup_completed=True,
                    stop_acknowledged=True,
                )
            )

    async def close(self):
        return 0


def test_safe_stop_before_worker_contact_aborts_without_execute(tmp_path):
    async def scenario():
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        worker = BarrierWorker(store.worker_lock_path)
        service = ControllerService(store, worker=worker)
        await service.open()
        try:
            await store.acquire_control_lease(principal="operator")
            operation, revision = await submit_simulated(store, revision=0)
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            dispatch = await store.claim_next_attempt(worker_instance_uid=worker.worker_instance_uid)
            assert dispatch is not None

            await safe_stop(
                service,
                attempt_uid=dispatch.attempt.attempt_uid,
                revision=await store.current_revision(),
                key="safe-stop-before-contact",
            )
            await service.wait_scheduler_idle()

            stopped = await store.get_attempt(dispatch.attempt.attempt_uid)
            assert stopped.state is AttemptState.ABORTED
            assert worker.started.empty()
            assert worker.stop_requests == []
            assert (await store.get_operation(operation.operation_uid)).state is OperationState.ABORTED
            assert (
                await store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.STOPPED
        finally:
            await service.close()

    run(scenario())


def test_safe_stop_running_attempt_is_correlated_once_and_stops_batch(tmp_path):
    async def scenario():
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        worker = BarrierWorker(store.worker_lock_path)
        service = ControllerService(store, worker=worker)
        await service.open()
        try:
            await store.acquire_control_lease(principal="operator")
            revision = 0
            operations = []
            for num in (1, 2):
                operation, revision = await submit_simulated(store, revision=revision, num=num)
                operations.append(operation)
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            service.wake_scheduler()
            attempt, _, _, _ = await worker.started.get()
            await wait_for_event(store, "operation.running")

            await safe_stop(
                service,
                attempt_uid=attempt.attempt_uid,
                revision=await store.current_revision(),
                key="safe-stop-running",
            )
            acknowledged = await store.get_attempt(attempt.attempt_uid)
            assert acknowledged.stop_acknowledged_at is not None
            await service.wait_scheduler_idle()

            terminal = await store.get_attempt(attempt.attempt_uid)
            assert terminal.state is AttemptState.ABORTED
            assert worker.stop_requests == [attempt.attempt_uid]
            assert (
                await store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.STOPPED
            assert [item.operation_uid for item in (await store.queue_snapshot()).operations] == [
                operations[1].operation_uid
            ]

            revision = await store.current_revision()
            with pytest.raises(StateConflictError, match="terminal"):
                await safe_stop(
                    service,
                    attempt_uid=attempt.attempt_uid,
                    revision=revision,
                    key="safe-stop-terminal",
                )
            assert worker.stop_requests == [attempt.attempt_uid]
            assert await store.current_revision() == revision
        finally:
            await service.close()

    run(scenario())


def test_scheduler_dispatches_admitted_operations_in_fifo_order(tmp_path):
    async def scenario():
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        worker = BarrierWorker(store.worker_lock_path)
        service = ControllerService(store, worker=worker)
        await service.open()
        try:
            await store.acquire_control_lease(principal="operator")
            operation_uids = []
            revision = 0
            for num in (1, 2, 3):
                operation, revision = await store.submit_operation(
                    principal="operator",
                    expected_revision=revision,
                    descriptor=SIMULATED_COUNT_DESCRIPTOR,
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"], "num": num},
                )
                operation_uids.append(operation.operation_uid)
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            service.wake_scheduler()

            observed_uids = []
            for _ in operation_uids:
                attempt, descriptor, parameters, completion = await worker.started.get()
                observed_uids.append(attempt.operation_uid)
                assert descriptor is SIMULATED_COUNT_DESCRIPTOR
                assert parameters["detectors"] == ["det"]
                completion.set_result(
                    WorkerCompletion(
                        state=AttemptState.SUCCEEDED,
                        result={"run_uids": [str(uuid4())]},
                        run_uids=(str(uuid4()),),
                        diagnostic=None,
                        cleanup_completed=True,
                    )
                )

            await service.wait_scheduler_idle()
            assert observed_uids == operation_uids
            assert (
                await store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.COMPLETED
            states = [(await store.get_operation(uid)).state for uid in operation_uids]
            assert states == [OperationState.SUCCEEDED] * 3
        finally:
            await service.close()

    run(scenario())


class MutableClock:
    def __init__(self, value=0):
        self.value = value

    def __call__(self):
        return self.value


def test_queue_etag_parser_rejects_missing_malformed_and_overflow():
    assert parse_queue_etag('"qrev-0"') == 0
    assert parse_queue_etag('"qrev-42"') == 42
    with pytest.raises(MissingPreconditionError):
        parse_queue_etag(None)
    for value in ("qrev-1", 'W/"qrev-1"', '"qrev-01"', '"qrev-9223372036854775808"'):
        with pytest.raises(InvalidPreconditionError):
            parse_queue_etag(value)


async def wait_for_event(store, event_type, *, after=0):
    cursor = after
    while True:
        events = await store.wait_for_events(after=cursor, timeout=1)
        assert events, f"timed out waiting for {event_type}"
        for event in events:
            cursor = event.event_id
            if event.event_type == event_type:
                return event


def test_scheduler_serializes_live_edits_and_continues_after_lease_expiry(tmp_path):
    async def scenario():
        clock = MutableClock()
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument", clock=clock)
        worker = BarrierWorker(store.worker_lock_path)
        service = ControllerService(store, worker=worker)
        await service.open()
        try:
            await store.acquire_control_lease(principal="operator", ttl_seconds=5)
            submitted = []
            revision = 0
            for num in (1, 2, 3):
                operation, revision = await submit_simulated(store, revision=revision, num=num)
                submitted.append(operation)
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            service.wake_scheduler()

            first_attempt, _, _, first_completion = await worker.started.get()
            await wait_for_event(store, "operation.running")
            revision = await store.current_revision()

            fourth, revision = await submit_simulated(store, revision=revision, num=4)
            _, revision = await store.cancel_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=submitted[1].operation_uid,
            )
            replacement, revision = await store.replace_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=submitted[2].operation_uid,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 5},
            )
            snapshot = await store.reorder_queue(
                principal="operator",
                expected_revision=revision,
                operation_uids=[replacement.operation_uid, fourth.operation_uid],
            )
            with pytest.raises(StateConflictError, match="not queued"):
                await store.cancel_operation(
                    principal="operator",
                    expected_revision=snapshot.revision,
                    operation_uid=submitted[0].operation_uid,
                )
            service.wake_scheduler()

            clock.value = 5_000_000
            assert await store.expire_control_lease()
            service.wake_scheduler()
            first_uid = str(uuid4())
            first_completion.set_result(
                WorkerCompletion(
                    state=AttemptState.SUCCEEDED,
                    result={"run_uids": [first_uid]},
                    run_uids=(first_uid,),
                    diagnostic=None,
                    cleanup_completed=True,
                )
            )

            observed = [first_attempt.operation_uid]
            for expected_uid in (replacement.operation_uid, fourth.operation_uid):
                attempt, _, _, completion = await worker.started.get()
                observed.append(attempt.operation_uid)
                assert attempt.operation_uid == expected_uid
                run_uid = str(uuid4())
                completion.set_result(
                    WorkerCompletion(
                        state=AttemptState.SUCCEEDED,
                        result={"run_uids": [run_uid]},
                        run_uids=(run_uid,),
                        diagnostic=None,
                        cleanup_completed=True,
                    )
                )

            await service.wait_scheduler_idle()
            assert observed == [submitted[0].operation_uid, replacement.operation_uid, fourth.operation_uid]
            assert (
                await store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.COMPLETED
            assert (await store.get_operation(submitted[1].operation_uid)).state is OperationState.CANCELLED
            assert (await store.get_operation(submitted[2].operation_uid)).state is OperationState.CANCELLED
        finally:
            await service.close()

    run(scenario())


@pytest.mark.parametrize("terminal_state", [AttemptState.FAILED, AttemptState.INTERRUPTED, AttemptState.UNKNOWN])
def test_scheduler_blocks_on_first_non_success_without_retry(tmp_path, terminal_state):
    async def scenario():
        store = SQLiteStore(tmp_path / f"{terminal_state.value}.sqlite", instrument_id="instrument")
        worker = BarrierWorker(store.worker_lock_path)
        service = ControllerService(store, worker=worker)
        await service.open()
        try:
            await store.acquire_control_lease(principal="operator")
            revision = 0
            operation_uids = []
            for num in (1, 2):
                operation, revision = await submit_simulated(store, revision=revision, num=num)
                operation_uids.append(operation.operation_uid)
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            service.wake_scheduler()
            attempt, _, _, completion = await worker.started.get()
            completion.set_result(
                WorkerCompletion(
                    state=terminal_state,
                    result=None,
                    run_uids=(),
                    diagnostic=f"proved {terminal_state.value}",
                    cleanup_completed=terminal_state is not AttemptState.UNKNOWN,
                )
            )
            await service.wait_scheduler_idle()

            assert attempt.operation_uid == operation_uids[0]
            assert (
                await store.get_queue_execution(execution.queue_execution_uid)
            ).state is QueueExecutionState.BLOCKED
            snapshot = await store.queue_snapshot()
            assert snapshot.dispatch_block_uid is not None
            assert [item.operation_uid for item in snapshot.operations] == [operation_uids[1]]
            assert worker.started.empty()
        finally:
            await service.close()

    run(scenario())


def test_stop_after_current_leaves_remaining_work_queued(tmp_path):
    async def scenario():
        store = SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument")
        worker = BarrierWorker(store.worker_lock_path)
        service = ControllerService(store, worker=worker)
        await service.open()
        try:
            await store.acquire_control_lease(principal="operator")
            revision = 0
            operations = []
            for num in (1, 2):
                operation, revision = await submit_simulated(store, revision=revision, num=num)
                operations.append(operation)
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            service.wake_scheduler()
            _, _, _, completion = await worker.started.get()
            await wait_for_event(store, "operation.running")
            revision = await store.current_revision()
            stopping, _ = await store.stop_queue_execution(
                principal="operator",
                expected_revision=revision,
                queue_execution_uid=execution.queue_execution_uid,
            )
            service.wake_scheduler()
            assert stopping.state is QueueExecutionState.STOPPING

            run_uid = str(uuid4())
            completion.set_result(
                WorkerCompletion(
                    state=AttemptState.SUCCEEDED,
                    result={"run_uids": [run_uid]},
                    run_uids=(run_uid,),
                    diagnostic=None,
                    cleanup_completed=True,
                )
            )
            await service.wait_scheduler_idle()

            stopped = await store.get_queue_execution(execution.queue_execution_uid)
            assert stopped.state is QueueExecutionState.STOPPED
            assert [item.operation_uid for item in (await store.queue_snapshot()).operations] == [
                operations[1].operation_uid
            ]
            assert worker.started.empty()
        finally:
            await service.close()

    run(scenario())
