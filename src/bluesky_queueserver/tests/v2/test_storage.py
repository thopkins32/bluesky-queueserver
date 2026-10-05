import asyncio
import sqlite3
from uuid import uuid4

import pytest

from bluesky_queueserver.v2 import storage
from bluesky_queueserver.v2.contracts import (
    SIMULATED_COUNT_DESCRIPTOR,
    ActorKind,
    AttemptState,
    AuthorizationScope,
    OperationState,
    OrphanPolicy,
    QueueExecutionPolicy,
    QueueExecutionState,
    SimulatedCountResult,
    StrictModel,
    WorkerCatalog,
    make_operation_descriptor,
)
from bluesky_queueserver.v2.storage import (
    EVENT_TYPES,
    MIGRATIONS,
    SCHEMA_VERSION,
    SQLITE_APPLICATION_ID,
    SQLITE_BUSY_TIMEOUT_MILLISECONDS,
    CatalogCompatibilityError,
    IdempotencyConflictError,
    IdempotencyRequest,
    LeaseConflictError,
    LeaseExpiredError,
    LeaseOwnershipError,
    QueueValidationError,
    RevisionConflictError,
    SQLiteStore,
    StateConflictError,
    StorageConfigurationError,
    StorageIdentityError,
    StoragePathError,
    StorageVersionError,
    StoredHttpResponse,
    filesystem_type_from_mountinfo,
)


def run(coroutine):
    return asyncio.run(coroutine)


def test_store_opens_one_hardened_local_connection(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def scenario():
        async with SQLiteStore(database_path, instrument_id="test") as store:
            settings = await store.pragma_settings()
            assert store.database_path == database_path.resolve()
            assert store.controller_lock_path == tmp_path / "state.sqlite.controller.lock"
            assert store.worker_lock_path == tmp_path / "state.sqlite.worker.lock"
            assert settings == {
                "application_id": SQLITE_APPLICATION_ID,
                "busy_timeout": SQLITE_BUSY_TIMEOUT_MILLISECONDS,
                "foreign_keys": 1,
                "journal_mode": "wal",
                "synchronous": 2,
            }
            assert await store.schema_info() == (SCHEMA_VERSION, MIGRATIONS[-1].checksum)

    run(scenario())


def test_store_canonicalizes_symlinked_parent(tmp_path):
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real_parent, target_is_directory=True)

    store = SQLiteStore(alias / "state.sqlite", instrument_id="test")

    assert store.database_path == real_parent / "state.sqlite"
    assert store.controller_lock_path == real_parent / "state.sqlite.controller.lock"


def test_store_rejects_unrelated_or_wrong_application_id(tmp_path):
    unrelated_path = tmp_path / "unrelated.sqlite"
    connection = sqlite3.connect(unrelated_path)
    connection.execute("CREATE TABLE unrelated(value TEXT)")
    connection.commit()
    connection.close()

    with pytest.raises(StorageIdentityError, match="no QueueServer V2 application ID"):
        run(SQLiteStore(unrelated_path, instrument_id="test").open())

    wrong_path = tmp_path / "wrong.sqlite"
    connection = sqlite3.connect(wrong_path)
    connection.execute("PRAGMA application_id=42")
    connection.commit()
    connection.close()

    with pytest.raises(StorageIdentityError, match="application ID 42"):
        run(SQLiteStore(wrong_path, instrument_id="test").open())


def test_store_refuses_remote_or_unidentifiable_filesystems(tmp_path, monkeypatch):
    mountinfo = "36 25 0:32 / / rw,relatime - nfs server:/export rw\n"
    assert filesystem_type_from_mountinfo(tmp_path, mountinfo) == "nfs"

    monkeypatch.setattr(storage, "filesystem_type_from_mountinfo", lambda path, mountinfo: "nfs")
    with pytest.raises(StoragePathError, match="cannot use nfs"):
        run(SQLiteStore(tmp_path / "state.sqlite", instrument_id="test").open())

    with pytest.raises(StoragePathError, match="unidentifiable"):
        filesystem_type_from_mountinfo(tmp_path, "malformed")


def test_store_requires_existing_database_parent(tmp_path):
    with pytest.raises(StoragePathError, match="parent does not exist"):
        SQLiteStore(tmp_path / "missing" / "state.sqlite", instrument_id="test")


def test_store_refuses_missing_or_mismatched_migration_history(tmp_path):
    uninitialized_path = tmp_path / "uninitialized.sqlite"
    with pytest.raises(StorageVersionError, match="does not exist"):
        run(SQLiteStore(uninitialized_path, instrument_id="test", allow_initialize=False).open())

    database_path = tmp_path / "state.sqlite"

    async def initialize():
        async with SQLiteStore(database_path, instrument_id="test"):
            pass

    run(initialize())
    connection = sqlite3.connect(database_path)
    connection.execute("UPDATE schema_migrations SET checksum='sha256:wrong' WHERE version=1")
    connection.commit()
    connection.close()

    with pytest.raises(StorageVersionError, match="checksum does not match"):
        run(SQLiteStore(database_path, instrument_id="test").open())


def test_store_refuses_user_version_mismatch(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def initialize():
        async with SQLiteStore(database_path, instrument_id="test"):
            pass

    run(initialize())
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA user_version=2")
    connection.close()

    with pytest.raises(StorageVersionError, match="does not match migration version"):
        run(SQLiteStore(database_path, instrument_id="test").open())


def test_schema_contains_all_authority_tables_and_canonical_metadata(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def initialize():
        async with SQLiteStore(database_path, instrument_id="instrument"):
            pass

    run(initialize())
    connection = sqlite3.connect(database_path)
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    metadata = connection.execute(
        "SELECT queue_revision, database_path, controller_lock_path, worker_lock_path, instrument_id "
        "FROM controller_metadata WHERE singleton=1"
    ).fetchone()
    foreign_key_errors = connection.execute("PRAGMA foreign_key_check").fetchall()
    connection.close()

    assert tables == {
        "schema_migrations",
        "controller_metadata",
        "control_lease",
        "operations",
        "queue_entries",
        "queue_executions",
        "queue_execution_admissions",
        "execution_attempts",
        "worker_instances",
        "dispatch_blocks",
        "controller_events",
        "idempotency_records",
    }
    assert metadata == (
        0,
        str(database_path.resolve()),
        f"{database_path.resolve()}.controller.lock",
        f"{database_path.resolve()}.worker.lock",
        "instrument",
    )
    assert foreign_key_errors == []


def test_store_rejects_alternate_authority_metadata(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def initialize():
        async with SQLiteStore(database_path, instrument_id="instrument"):
            pass

    run(initialize())
    connection = sqlite3.connect(database_path)
    connection.execute("UPDATE controller_metadata SET controller_lock_path='/tmp/alternate.lock'")
    connection.commit()
    connection.close()

    with pytest.raises(StorageConfigurationError, match="authority metadata"):
        run(SQLiteStore(database_path, instrument_id="instrument").open())


def test_partial_uniqueness_and_history_triggers_fail_closed(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def initialize():
        async with SQLiteStore(database_path, instrument_id="instrument"):
            pass

    run(initialize())
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA foreign_keys=ON")
    operation_uids = [str(uuid4()), str(uuid4())]
    execution_uids = [str(uuid4()), str(uuid4())]
    worker_uids = [str(uuid4()), str(uuid4())]
    for operation_uid in operation_uids:
        connection.execute(
            """
            INSERT INTO operations(
                operation_uid, operation_id, operation_version, parameters_json,
                descriptor_fingerprint, submitted_by, state, submitted_at, updated_at
            ) VALUES (?, 'simulated-count', '1', '{}', 'sha256:test', 'operator', 'queued', 1, 1)
            """,
            (operation_uid,),
        )
    connection.execute(
        "INSERT INTO queue_entries(operation_uid, position, enqueued_at) VALUES (?, 0, 1)",
        (operation_uids[0],),
    )
    connection.execute(
        """
        INSERT INTO queue_executions(
            queue_execution_uid, state, policy, initiated_by, starting_revision, created_at, updated_at
        ) VALUES (?, 'running', 'stop_on_non_success', 'operator', 0, 1, 1)
        """,
        (execution_uids[0],),
    )
    connection.execute(
        """
        INSERT INTO worker_instances(
            worker_instance_uid, worker_revision, worker_provenance_json, catalog_json,
            state, lock_path, started_at
        ) VALUES (?, 'revision', '{}', '{}', 'ready', '/tmp/worker.lock', 1)
        """,
        (worker_uids[0],),
    )
    connection.execute(
        """
        INSERT INTO execution_attempts(
            attempt_uid, operation_uid, queue_execution_uid, worker_instance_uid,
            worker_revision, worker_provenance_json, execute_message_uid,
            scheduler_authorization, state, created_at
        ) VALUES (?, ?, ?, ?, 'revision', '{}', ?, 'queue-execution:test', 'claimed', 1)
        """,
        (str(uuid4()), operation_uids[0], execution_uids[0], worker_uids[0], str(uuid4())),
    )
    connection.commit()

    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO queue_entries(operation_uid, position, enqueued_at) VALUES (?, 0, 1)",
            (operation_uids[1],),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO queue_executions(
                queue_execution_uid, state, policy, initiated_by, starting_revision, created_at, updated_at
            ) VALUES (?, 'running', 'stop_on_non_success', 'operator', 0, 1, 1)
            """,
            (execution_uids[1],),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO worker_instances(
                worker_instance_uid, worker_revision, worker_provenance_json, catalog_json,
                state, lock_path, started_at
            ) VALUES (?, 'revision', '{}', '{}', 'ready', '/tmp/worker.lock', 1)
            """,
            (worker_uids[1],),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO execution_attempts(
                attempt_uid, operation_uid, queue_execution_uid, worker_instance_uid,
                worker_revision, worker_provenance_json, execute_message_uid,
                scheduler_authorization, state, created_at
            ) VALUES (?, ?, ?, ?, 'revision', '{}', ?, 'queue-execution:test', 'claimed', 1)
            """,
            (str(uuid4()), operation_uids[1], execution_uids[0], worker_uids[0], str(uuid4())),
        )
    connection.rollback()
    connection.close()


def test_submission_replacement_reorder_and_cancel_preserve_history(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def scenario():
        async with SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 100) as store:
            await store.acquire_control_lease(principal="operator")
            first, revision = await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 1, "delay": 0.0},
            )
            second, revision = await store.submit_operation(
                principal="operator",
                expected_revision=revision,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 2, "delay": 0.0},
            )
            replacement, revision = await store.replace_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=first.operation_uid,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 3, "delay": 0.0},
            )
            old = await store.get_operation(first.operation_uid)
            snapshot = await store.queue_snapshot()

            assert old.state is OperationState.CANCELLED
            assert old.replaced_by == replacement.operation_uid
            assert old.parameters["detectors"] == ("det",)
            assert replacement.operation_uid != first.operation_uid
            assert [record.operation_uid for record in snapshot.operations] == [
                replacement.operation_uid,
                second.operation_uid,
            ]

            snapshot = await store.reorder_queue(
                principal="operator",
                expected_revision=revision,
                operation_uids=[second.operation_uid, replacement.operation_uid],
            )
            revision = snapshot.revision
            assert [record.operation_uid for record in snapshot.operations] == [
                second.operation_uid,
                replacement.operation_uid,
            ]

            cancelled, revision = await store.cancel_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=second.operation_uid,
            )
            assert cancelled.state is OperationState.CANCELLED
            assert [record.operation_uid for record in (await store.queue_snapshot()).operations] == [
                replacement.operation_uid
            ]
            assert revision == 5

            with pytest.raises(StateConflictError):
                await store.cancel_operation(
                    principal="operator",
                    expected_revision=revision,
                    operation_uid=first.operation_uid,
                )
            with pytest.raises(QueueValidationError):
                await store.reorder_queue(
                    principal="operator",
                    expected_revision=revision,
                    operation_uids=[],
                )
            with pytest.raises(RevisionConflictError):
                await store.cancel_operation(
                    principal="operator",
                    expected_revision=revision - 1,
                    operation_uid=replacement.operation_uid,
                )
            assert await store.current_revision() == revision

    run(scenario())


def test_invalid_submission_rolls_back_without_queue_mutation(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            await store.acquire_control_lease(principal="operator")
            with pytest.raises(ValueError):
                await store.submit_operation(
                    principal="operator",
                    expected_revision=0,
                    descriptor=SIMULATED_COUNT_DESCRIPTOR,
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["unreviewed"]},
                )
            assert await store.queue_snapshot() == storage.QueueRecord(revision=0, operations=())

    run(scenario())


def test_queue_execution_admits_live_edits_and_stops_without_new_admissions(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument", clock=lambda: 100) as store:
            await store.acquire_control_lease(principal="operator")
            first, revision = await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"]},
            )
            execution, revision = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
                policy=QueueExecutionPolicy.STOP_ON_NON_SUCCESS,
            )
            assert execution.state is QueueExecutionState.RUNNING
            assert execution.admitted_operation_uids == (first.operation_uid,)

            second, revision = await store.submit_operation(
                principal="operator",
                expected_revision=revision,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 2},
            )
            replacement, revision = await store.replace_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=second.operation_uid,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 3},
            )
            execution = await store.get_queue_execution(execution.queue_execution_uid)
            assert execution.admitted_operation_uids == (
                first.operation_uid,
                second.operation_uid,
                replacement.operation_uid,
            )

            with pytest.raises(StateConflictError):
                await store.start_queue_execution(principal="operator", expected_revision=revision)
            assert await store.current_revision() == revision

            execution, revision = await store.stop_queue_execution(
                principal="operator",
                expected_revision=revision,
                queue_execution_uid=execution.queue_execution_uid,
            )
            assert execution.state is QueueExecutionState.STOPPED

            third, revision = await store.submit_operation(
                principal="operator",
                expected_revision=revision,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"], "num": 4},
            )
            execution = await store.get_queue_execution(execution.queue_execution_uid)
            assert third.operation_uid not in execution.admitted_operation_uids

    run(scenario())


def test_running_execution_completes_when_last_admitted_item_is_cancelled(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument", clock=lambda: 100) as store:
            await store.acquire_control_lease(principal="operator")
            operation, revision = await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"]},
            )
            execution, revision = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            _, revision = await store.cancel_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=operation.operation_uid,
            )

            execution = await store.get_queue_execution(execution.queue_execution_uid)
            assert execution.state is QueueExecutionState.COMPLETED
            assert execution.completed_at == 100
            assert revision == 3

    run(scenario())


def test_queue_execution_rejects_empty_queue(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            await store.acquire_control_lease(principal="operator")
            with pytest.raises(StateConflictError, match="empty queue"):
                await store.start_queue_execution(principal="operator", expected_revision=0)
            assert await store.current_revision() == 0

    run(scenario())


def test_external_mutations_increment_once_and_emit_matching_events(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def scenario():
        async with SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 100) as store:
            await store.acquire_control_lease(principal="operator")
            operation, revision = await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"]},
            )
            _, revision = await store.cancel_operation(
                principal="operator",
                expected_revision=revision,
                operation_uid=operation.operation_uid,
            )
            assert revision == 2

    run(scenario())
    connection = sqlite3.connect(database_path)
    event_revisions = [
        row[0]
        for row in connection.execute(
            "SELECT DISTINCT queue_revision FROM controller_events "
            "WHERE queue_revision IS NOT NULL ORDER BY queue_revision"
        )
    ]
    stored_revision = connection.execute(
        "SELECT queue_revision FROM controller_metadata WHERE singleton=1"
    ).fetchone()[0]
    connection.close()

    assert stored_revision == 2
    assert event_revisions == [1, 2]


def test_event_insert_failure_rolls_back_domain_and_revision(tmp_path, monkeypatch):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            await store.acquire_control_lease(principal="operator")

            async def fail_event(*args, **kwargs):
                raise RuntimeError("injected event failure")

            monkeypatch.setattr(store, "_insert_event_on", fail_event)
            with pytest.raises(RuntimeError, match="injected event failure"):
                await store.submit_operation(
                    principal="operator",
                    expected_revision=0,
                    descriptor=SIMULATED_COUNT_DESCRIPTOR,
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"]},
                )
            assert await store.queue_snapshot() == storage.QueueRecord(revision=0, operations=())

    run(scenario())


def test_idempotent_mutation_replays_original_response_after_revision_advances(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument", clock=lambda: 100) as store:
            request = IdempotencyRequest(
                principal="operator",
                method="POST",
                target="/api/v2/operations",
                key="request-1",
                body={"operation": [1]},
                if_match='"qrev-0"',
            )
            calls = 0

            async def mutate(connection):
                nonlocal calls
                calls += 1
                revision = await store._increment_revision_on(connection)
                await store._insert_event_on(
                    connection,
                    timestamp=100,
                    actor_kind=ActorKind.PRINCIPAL,
                    actor_id="operator",
                    event_type="queue.reordered",
                    queue_revision=revision,
                    payload={},
                )
                return StoredHttpResponse(status=201, body={"revision": revision, "items": [1]}, etag='"qrev-1"')

            first = await store.run_idempotent_mutation(request, mutate)

            async def must_not_run(connection):
                raise AssertionError("idempotent replay executed the mutation")

            replay = await store.run_idempotent_mutation(request, must_not_run)
            assert not first.replayed
            assert replay.replayed
            assert replay.response == first.response
            assert calls == 1
            assert await store.current_revision() == 1

            conflict = IdempotencyRequest(
                principal="operator",
                method="POST",
                target="/api/v2/operations",
                key="request-1",
                body={"operation": [2]},
                if_match='"qrev-0"',
            )
            with pytest.raises(IdempotencyConflictError):
                await store.run_idempotent_mutation(conflict, must_not_run)

    run(scenario())


def test_invalid_idempotency_response_rolls_back_action(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def scenario():
        async with SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 100) as store:
            await store.acquire_control_lease(principal="operator")
            events_before = await store.list_events()
            request = IdempotencyRequest(
                principal="operator",
                method="POST",
                target="/api/v2/operations",
                key="request-2",
                body={},
                if_match='"qrev-0"',
            )

            async def mutate(connection):
                validated = storage.validate_operation_request(
                    SIMULATED_COUNT_DESCRIPTOR,
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"]},
                )
                await store._submit_operation_on(
                    connection,
                    principal="operator",
                    expected_revision=0,
                    descriptor=SIMULATED_COUNT_DESCRIPTOR,
                    parameters_json=storage.canonical_json(validated),
                    fingerprint=storage.descriptor_fingerprint(SIMULATED_COUNT_DESCRIPTOR),
                    operation_uid=str(uuid4()),
                    timestamp=100,
                )
                return StoredHttpResponse(status=201, body={"bad": float("nan")}, etag='"qrev-1"')

            with pytest.raises(ValueError):
                await store.run_idempotent_mutation(request, mutate)
            assert await store.queue_snapshot() == storage.QueueRecord(revision=0, operations=())
            assert await store.list_events() == events_before

    run(scenario())
    connection = sqlite3.connect(database_path)
    assert connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM idempotency_records").fetchone()[0] == 0
    connection.close()


def test_idempotency_key_format_is_bounded():
    for key in ("", "space key", "x" * 129):
        with pytest.raises(ValueError, match="Idempotency-Key"):
            IdempotencyRequest(
                principal="operator",
                method="POST",
                target="/api/v2/operations",
                key=key,
                body={},
                if_match='"qrev-0"',
            )


def test_controller_events_are_typed_append_only_and_cursor_ordered(tmp_path):
    database_path = tmp_path / "state.sqlite"

    async def scenario():
        async with SQLiteStore(database_path, instrument_id="instrument", clock=lambda: 123) as store:
            await store.acquire_control_lease(principal="operator")
            operation, _ = await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"]},
            )
            events = await store.list_events(after=1, limit=100)
            assert [event.event_type for event in events] == ["operation.submitted", "operation.queued"]
            assert [event.event_id for event in events] == [2, 3]
            assert all(event.timestamp == 123 for event in events)
            assert all(event.actor_kind is ActorKind.PRINCIPAL for event in events)
            assert all(event.actor_id == "operator" for event in events)
            assert all(event.operation_uid == operation.operation_uid for event in events)
            assert [event.event_id for event in await store.list_events(after=2)] == [3]
            assert await store.wait_for_events(after=3, timeout=0.001) == ()

    run(scenario())
    connection = sqlite3.connect(database_path)
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute("UPDATE controller_events SET event_type='operation.failed' WHERE event_id=1")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute("DELETE FROM controller_events WHERE event_id=1")
    connection.close()


def test_event_type_contract_covers_every_required_transition():
    assert {
        "lease.acquired",
        "lease.renewed",
        "lease.released",
        "lease.overridden",
        "lease.expired",
        "operation.submitted",
        "operation.queued",
        "operation.cancelled",
        "operation.replaced",
        "operation.claimed",
        "operation.running",
        "operation.succeeded",
        "operation.failed",
        "operation.aborted",
        "operation.interrupted",
        "operation.unknown",
        "queue_execution.started",
        "queue_execution.stop_requested",
        "queue_execution.completed",
        "queue_execution.stopped",
        "queue_execution.blocked",
        "queue_execution.admitted",
        "attempt.stop_requested",
        "attempt.stop_acknowledged",
        "worker.started",
        "worker.ready",
        "worker.exited",
        "worker.fenced",
        "recovery.acknowledged",
    } <= EVENT_TYPES


def test_control_lease_is_principal_owned_audited_and_revision_independent(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            lease = await store.acquire_control_lease(principal="alice", ttl_seconds=5, now=1_000_000)
            assert lease.subject == "alice"
            assert lease.expires_at == 6_000_000
            assert await store.current_revision() == 0

            with pytest.raises(LeaseConflictError):
                await store.acquire_control_lease(principal="alice", ttl_seconds=5, now=2_000_000)
            with pytest.raises(LeaseOwnershipError):
                await store.renew_control_lease(principal="bob", ttl_seconds=5, now=2_000_000)
            with pytest.raises(LeaseOwnershipError):
                await store.release_control_lease(principal="bob", now=2_000_000)

            renewed = await store.renew_control_lease(principal="alice", ttl_seconds=10, now=2_000_000)
            assert renewed.lease_uid == lease.lease_uid
            assert renewed.expires_at == 12_000_000
            assert not await store.expire_control_lease(now=11_999_999)
            assert await store.expire_control_lease(now=12_000_000)
            assert not await store.expire_control_lease(now=12_000_001)
            assert await store.get_control_lease() is None
            assert await store.current_revision() == 0

            event_types = [event.event_type for event in await store.list_events()]
            assert event_types == ["lease.acquired", "lease.renewed", "lease.expired"]

    run(scenario())


def test_admin_override_takes_lease_for_administrator(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            await store.acquire_control_lease(principal="alice", now=0)
            lease = await store.override_control_lease(
                administrator="admin",
                reason="operator handoff",
                ttl_seconds=5,
                now=1,
            )
            assert lease.subject == "admin"
            assert (await store.get_control_lease()).subject == "admin"
            event = (await store.list_events())[-1]
            assert event.event_type == "lease.overridden"
            assert event.payload == {
                "previous_holder": "alice",
                "reason": "operator handoff",
                "expires_at": 5_000_001,
            }

    run(scenario())


def test_expired_lease_is_recorded_before_renewal_fails(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            await store.acquire_control_lease(principal="alice", ttl_seconds=5, now=0)
            with pytest.raises(LeaseExpiredError):
                await store.renew_control_lease(principal="alice", ttl_seconds=5, now=5_000_000)
            assert await store.get_control_lease() is None
            assert [event.event_type for event in await store.list_events()] == [
                "lease.acquired",
                "lease.expired",
            ]

    run(scenario())


def test_claim_is_durable_before_running_and_never_double_claims(tmp_path):
    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument", clock=lambda: 100) as store:
            await store.acquire_control_lease(principal="operator")
            worker_instance_uid = str(uuid4())
            catalog = WorkerCatalog(
                protocol_version="2",
                worker_revision="sha256:" + "1" * 64,
                worker_provenance={"provider": "test"},
                operations=[SIMULATED_COUNT_DESCRIPTOR],
            )
            await store.register_ready_worker(
                worker_instance_uid=worker_instance_uid,
                catalog=catalog,
                lock_path=store.worker_lock_path,
                pid=None,
            )
            operation_uids = []
            revision = 0
            for num in (1, 2):
                operation, revision = await store.submit_operation(
                    principal="operator",
                    expected_revision=revision,
                    descriptor=SIMULATED_COUNT_DESCRIPTOR,
                    operation_id="simulated-count",
                    operation_version="1",
                    parameters={"detectors": ["det"], "num": num},
                )
                operation_uids.append(operation.operation_uid)
            execution, revision = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )

            dispatch = await store.claim_next_attempt(worker_instance_uid=worker_instance_uid)
            assert dispatch is not None
            assert dispatch.operation.operation_uid == operation_uids[0]
            assert dispatch.attempt.state is AttemptState.CLAIMED
            assert dispatch.attempt.queue_execution_uid == execution.queue_execution_uid
            assert dispatch.attempt.worker_revision == catalog.worker_revision
            assert dispatch.attempt.scheduler_authorization == f"queue-execution:{execution.queue_execution_uid}"
            assert await store.claim_next_attempt(worker_instance_uid=worker_instance_uid) is None

            running = await store.mark_attempt_running(attempt_uid=dispatch.attempt.attempt_uid)
            assert running.state is AttemptState.RUNNING
            completed = await store.complete_attempt(
                attempt_uid=dispatch.attempt.attempt_uid,
                state=AttemptState.SUCCEEDED,
                result={"run_uids": [str(uuid4())]},
                run_uids=(str(uuid4()),),
                diagnostic=None,
                cleanup_completed=True,
            )
            assert completed.state is AttemptState.SUCCEEDED

            second = await store.claim_next_attempt(worker_instance_uid=worker_instance_uid)
            assert second is not None
            assert second.operation.operation_uid == operation_uids[1]
            assert second.attempt.attempt_uid != dispatch.attempt.attempt_uid
            assert await store.current_revision() == revision + 4

    run(scenario())


def test_worker_catalog_requires_matching_nonterminal_schema(tmp_path):
    class ChangedRequest(StrictModel):
        detectors: list[str]

    async def scenario():
        async with SQLiteStore(tmp_path / "state.sqlite", instrument_id="instrument") as store:
            await store.acquire_control_lease(principal="operator")
            await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"]},
            )
            compatible = WorkerCatalog(
                protocol_version="2",
                worker_revision="sha256:" + "1" * 64,
                worker_provenance={"revision": 1},
                operations=[SIMULATED_COUNT_DESCRIPTOR],
            )
            worker = await store.register_ready_worker(
                worker_instance_uid=str(uuid4()),
                catalog=compatible,
                lock_path=store.worker_lock_path,
                pid=None,
            )
            await store.mark_worker_exited(
                worker_instance_uid=worker.worker_instance_uid,
                exit_code=0,
                faulted=False,
            )

            changed_descriptor = make_operation_descriptor(
                operation_id="simulated-count",
                operation_version="1",
                request_model=ChangedRequest,
                result_model=SimulatedCountResult,
                required_scope=AuthorizationScope.CONTROL,
                orphan_policy=OrphanPolicy.REQUEST_STOP,
            )
            incompatible = WorkerCatalog(
                protocol_version="2",
                worker_revision="sha256:" + "2" * 64,
                worker_provenance={"revision": 2},
                operations=[changed_descriptor],
            )
            with pytest.raises(CatalogCompatibilityError, match="changed the schema"):
                await store.register_ready_worker(
                    worker_instance_uid=str(uuid4()),
                    catalog=incompatible,
                    lock_path=store.worker_lock_path,
                    pid=None,
                )
            assert await store.get_active_worker() is None

    run(scenario())
