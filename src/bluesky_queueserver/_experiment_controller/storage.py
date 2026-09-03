from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

from .contracts import (
    ControlLease,
    ControllerError,
    ControllerEvent,
    DispatchBlockedError,
    LeaseConflictError,
    LeaseExpiredError,
    OperationRecord,
    OperationState,
    QueueSnapshot,
    RevisionConflictError,
    StorageVersionError,
)

_SCHEMA_VERSION = 1
_BUSY_TIMEOUT_MILLISECONDS = 5_000


class SQLiteControllerStore:
    """Local SQLite persistence for the experiment-controller prototype."""

    def __init__(self, database_path: Path):
        self._database_path = Path(database_path)
        self._connection: sqlite3.Connection | None = None

    def open(self) -> None:
        if self._connection is not None:
            raise RuntimeError("Controller store is already open")

        connection = sqlite3.connect(
            str(self._database_path),
            isolation_level=None,
            timeout=_BUSY_TIMEOUT_MILLISECONDS / 1_000,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MILLISECONDS}")
            connection.execute("PRAGMA journal_mode=WAL")
            self._initialize_schema(connection)
        except BaseException:
            connection.close()
            raise

        self._connection = connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def acquire_lease(self, *, subject: str, issued_at: float, expires_at: float) -> ControlLease:
        connection = self._require_connection()
        with self._transaction(connection):
            row = connection.execute(
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1"
            ).fetchone()
            if row is not None and float(row["expires_at"]) > issued_at:
                if row["subject"] != subject:
                    raise LeaseConflictError(f"Control lease is held by {row['subject']!r}")
                lease_uid = str(row["lease_uid"])
            else:
                lease_uid = str(uuid.uuid4())

            connection.execute(
                """
                INSERT INTO control_lease(singleton, lease_uid, subject, issued_at, expires_at)
                VALUES (1, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    lease_uid=excluded.lease_uid,
                    subject=excluded.subject,
                    issued_at=excluded.issued_at,
                    expires_at=excluded.expires_at
                """,
                (lease_uid, subject, issued_at, expires_at),
            )
            self._insert_event(
                connection,
                occurred_at=issued_at,
                actor=subject,
                event_type="lease.acquired",
                operation_uid=None,
                payload={"lease_uid": lease_uid, "expires_at": expires_at},
            )

        return ControlLease(
            lease_uid=lease_uid,
            subject=subject,
            issued_at=issued_at,
            expires_at=expires_at,
        )

    def validate_mutation(
        self,
        *,
        subject: str,
        lease_uid: str,
        expected_revision: int,
        now: float,
    ) -> None:
        connection = self._require_connection()
        with self._read_transaction(connection):
            self._validate_authority(connection, subject=subject, lease_uid=lease_uid, now=now)
            self._validate_revision(connection, expected_revision=expected_revision)

    def submit_operation(
        self,
        *,
        subject: str,
        lease_uid: str,
        expected_revision: int,
        operation_id: str,
        operation_version: str,
        parameters: Mapping[str, object],
        worker_revision: str,
        now: float,
    ) -> OperationRecord:
        connection = self._require_connection()
        with self._transaction(connection):
            self._validate_authority(connection, subject=subject, lease_uid=lease_uid, now=now)
            self._validate_revision(connection, expected_revision=expected_revision)

            sequence_row = connection.execute(
                "SELECT COALESCE(MAX(queue_sequence), 0) + 1 AS next_sequence FROM operations"
            ).fetchone()
            queue_sequence = int(sequence_row["next_sequence"])
            operation_uid = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO operations(
                    operation_uid,
                    operation_id,
                    operation_version,
                    parameters_json,
                    worker_revision,
                    submitted_by,
                    state,
                    queue_sequence,
                    run_uids_json,
                    error_message,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    operation_uid,
                    operation_id,
                    operation_version,
                    self._json_dumps(parameters),
                    worker_revision,
                    subject,
                    OperationState.QUEUED.value,
                    queue_sequence,
                    self._json_dumps([]),
                    now,
                    now,
                ),
            )
            self._increment_revision(connection)
            self._insert_event(
                connection,
                occurred_at=now,
                actor=subject,
                event_type="operation.submitted",
                operation_uid=operation_uid,
                payload={
                    "operation_id": operation_id,
                    "operation_version": operation_version,
                    "queue_sequence": queue_sequence,
                    "worker_revision": worker_revision,
                },
            )
            row = self._operation_row(connection, operation_uid)

        return self._row_to_operation(row)

    def claim_next_operation(
        self,
        *,
        subject: str,
        lease_uid: str,
        expected_revision: int,
        now: float,
    ) -> OperationRecord | None:
        connection = self._require_connection()
        with self._transaction(connection):
            self._validate_authority(connection, subject=subject, lease_uid=lease_uid, now=now)
            self._validate_revision(connection, expected_revision=expected_revision)
            metadata = self._metadata_row(connection)
            if metadata["dispatch_block_reason"] is not None:
                raise DispatchBlockedError(str(metadata["dispatch_block_reason"]))

            row = connection.execute(
                """
                SELECT * FROM operations
                WHERE state=?
                ORDER BY queue_sequence
                LIMIT 1
                """,
                (OperationState.QUEUED.value,),
            ).fetchone()
            if row is None:
                return None

            operation_uid = str(row["operation_uid"])
            connection.execute(
                "UPDATE operations SET state=?, updated_at=? WHERE operation_uid=?",
                (OperationState.RUNNING.value, now, operation_uid),
            )
            self._increment_revision(connection)
            self._insert_event(
                connection,
                occurred_at=now,
                actor=subject,
                event_type="operation.started",
                operation_uid=operation_uid,
                payload={"queue_sequence": int(row["queue_sequence"])},
            )
            claimed_row = self._operation_row(connection, operation_uid)

        return self._row_to_operation(claimed_row)

    def complete_operation(
        self,
        *,
        operation_uid: str,
        actor: str,
        state: OperationState,
        run_uids: Sequence[str],
        error_message: str | None,
        now: float,
    ) -> OperationRecord:
        if state not in (OperationState.SUCCEEDED, OperationState.FAILED):
            raise ValueError(f"Unsupported terminal operation state {state!r}")

        connection = self._require_connection()
        with self._transaction(connection):
            current_row = self._operation_row(connection, operation_uid)
            if current_row["state"] != OperationState.RUNNING.value:
                raise ControllerError(
                    f"Operation {operation_uid!r} cannot transition from {current_row['state']!r} "
                    f"to {state.value!r}"
                )

            connection.execute(
                """
                UPDATE operations
                SET state=?, run_uids_json=?, error_message=?, updated_at=?
                WHERE operation_uid=?
                """,
                (state.value, self._json_dumps(list(run_uids)), error_message, now, operation_uid),
            )
            if state is OperationState.FAILED:
                connection.execute(
                    """
                    UPDATE controller_metadata
                    SET queue_revision=queue_revision + 1, dispatch_block_reason='operation_failed'
                    WHERE singleton=1
                    """
                )
            else:
                self._increment_revision(connection)
            self._insert_event(
                connection,
                occurred_at=now,
                actor=actor,
                event_type=f"operation.{state.value}",
                operation_uid=operation_uid,
                payload={"run_uids": list(run_uids), "error_message": error_message},
            )
            completed_row = self._operation_row(connection, operation_uid)

        return self._row_to_operation(completed_row)

    def mark_operation_unknown(
        self,
        *,
        operation_uid: str,
        actor: str,
        error_message: str,
        now: float,
    ) -> OperationRecord:
        connection = self._require_connection()
        with self._transaction(connection):
            current_row = self._operation_row(connection, operation_uid)
            if current_row["state"] != OperationState.RUNNING.value:
                raise ControllerError(
                    f"Operation {operation_uid!r} cannot transition from {current_row['state']!r} to 'unknown'"
                )

            connection.execute(
                """
                UPDATE operations
                SET state=?, run_uids_json=?, error_message=?, updated_at=?
                WHERE operation_uid=?
                """,
                (
                    OperationState.UNKNOWN.value,
                    self._json_dumps([]),
                    error_message,
                    now,
                    operation_uid,
                ),
            )
            connection.execute(
                """
                UPDATE controller_metadata
                SET queue_revision=queue_revision + 1, dispatch_block_reason='worker_transport_failure'
                WHERE singleton=1
                """
            )
            self._insert_event(
                connection,
                occurred_at=now,
                actor=actor,
                event_type="operation.unknown",
                operation_uid=operation_uid,
                payload={"error_message": error_message},
            )
            unknown_row = self._operation_row(connection, operation_uid)

        return self._row_to_operation(unknown_row)

    def acknowledge_dispatch_block(
        self,
        *,
        subject: str,
        lease_uid: str,
        expected_revision: int,
        note: str,
        now: float,
    ) -> QueueSnapshot:
        if not note.strip():
            raise ValueError("Recovery acknowledgement note must not be empty")

        connection = self._require_connection()
        with self._transaction(connection):
            self._validate_authority(connection, subject=subject, lease_uid=lease_uid, now=now)
            self._validate_revision(connection, expected_revision=expected_revision)
            metadata = self._metadata_row(connection)
            previous_reason = metadata["dispatch_block_reason"]
            if previous_reason is None:
                raise DispatchBlockedError("dispatch_not_blocked")

            connection.execute(
                """
                UPDATE controller_metadata
                SET queue_revision=queue_revision + 1, dispatch_block_reason=NULL
                WHERE singleton=1
                """
            )
            self._insert_event(
                connection,
                occurred_at=now,
                actor=subject,
                event_type="dispatch.recovery_acknowledged",
                operation_uid=None,
                payload={"previous_block_reason": str(previous_reason), "note": note},
            )

        return self.queue_snapshot()

    def queue_snapshot(self) -> QueueSnapshot:
        connection = self._require_connection()
        with self._read_transaction(connection):
            metadata = self._metadata_row(connection)
            rows = connection.execute("SELECT * FROM operations ORDER BY queue_sequence").fetchall()
            operations = tuple(self._row_to_operation(row) for row in rows)
            return QueueSnapshot(
                revision=int(metadata["queue_revision"]),
                operations=operations,
                dispatch_block_reason=metadata["dispatch_block_reason"],
            )

    def events(self, *, after_event_id: int = 0) -> list[ControllerEvent]:
        connection = self._require_connection()
        rows = connection.execute(
            """
            SELECT event_id, occurred_at, actor, event_type, operation_uid, payload_json
            FROM events
            WHERE event_id > ?
            ORDER BY event_id
            """,
            (after_event_id,),
        ).fetchall()
        return [
            ControllerEvent(
                event_id=int(row["event_id"]),
                occurred_at=float(row["occurred_at"]),
                actor=str(row["actor"]),
                event_type=str(row["event_type"]),
                operation_uid=row["operation_uid"],
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]

    @staticmethod
    def _initialize_schema(connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS controller_metadata(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL,
                    queue_revision INTEGER NOT NULL,
                    dispatch_block_reason TEXT
                )
                """
            )
            metadata = connection.execute(
                "SELECT schema_version FROM controller_metadata WHERE singleton=1"
            ).fetchone()
            if metadata is None:
                connection.execute(
                    """
                    INSERT INTO controller_metadata(
                        singleton, schema_version, queue_revision, dispatch_block_reason
                    ) VALUES (1, ?, 0, NULL)
                    """,
                    (_SCHEMA_VERSION,),
                )
            elif int(metadata["schema_version"]) != _SCHEMA_VERSION:
                raise StorageVersionError(
                    expected_version=_SCHEMA_VERSION,
                    current_version=int(metadata["schema_version"]),
                )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS control_lease(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    lease_uid TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    issued_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS operations(
                    operation_uid TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL,
                    operation_version TEXT NOT NULL,
                    parameters_json TEXT NOT NULL,
                    worker_revision TEXT NOT NULL,
                    submitted_by TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('queued', 'running', 'succeeded', 'failed', 'unknown')),
                    queue_sequence INTEGER NOT NULL UNIQUE,
                    run_uids_json TEXT NOT NULL,
                    error_message TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS events(
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at REAL NOT NULL,
                    actor TEXT NOT NULL,
                    event_type TEXT NOT NULL CHECK(event_type IN (
                        'lease.acquired',
                        'operation.submitted',
                        'operation.started',
                        'operation.succeeded',
                        'operation.failed',
                        'operation.unknown',
                        'dispatch.recovery_acknowledged'
                    )),
                    operation_uid TEXT REFERENCES operations(operation_uid),
                    payload_json TEXT NOT NULL
                )
                """
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    @contextmanager
    def _transaction(self, connection: sqlite3.Connection) -> Iterator[None]:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()

    @contextmanager
    def _read_transaction(self, connection: sqlite3.Connection) -> Iterator[None]:
        connection.execute("BEGIN")
        try:
            yield
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Controller store is not open")
        return self._connection

    @staticmethod
    def _metadata_row(connection: sqlite3.Connection) -> sqlite3.Row:
        row = connection.execute(
            "SELECT queue_revision, dispatch_block_reason FROM controller_metadata WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise ControllerError("Controller metadata is missing")
        return row

    @staticmethod
    def _operation_row(connection: sqlite3.Connection, operation_uid: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM operations WHERE operation_uid=?", (operation_uid,)).fetchone()
        if row is None:
            raise ControllerError(f"Operation {operation_uid!r} does not exist")
        return row

    @staticmethod
    def _validate_authority(
        connection: sqlite3.Connection,
        *,
        subject: str,
        lease_uid: str,
        now: float,
    ) -> None:
        row = connection.execute(
            "SELECT lease_uid, subject, expires_at FROM control_lease WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise LeaseConflictError("No control lease is active")
        if float(row["expires_at"]) <= now:
            raise LeaseExpiredError("Control lease has expired")
        if row["subject"] != subject or row["lease_uid"] != lease_uid:
            raise LeaseConflictError("Control lease does not match the caller")

    @classmethod
    def _validate_revision(cls, connection: sqlite3.Connection, *, expected_revision: int) -> None:
        current_revision = int(cls._metadata_row(connection)["queue_revision"])
        if current_revision != expected_revision:
            raise RevisionConflictError(
                expected_revision=expected_revision,
                current_revision=current_revision,
            )

    @staticmethod
    def _increment_revision(connection: sqlite3.Connection) -> None:
        connection.execute("UPDATE controller_metadata SET queue_revision=queue_revision + 1 WHERE singleton=1")

    @classmethod
    def _insert_event(
        cls,
        connection: sqlite3.Connection,
        *,
        occurred_at: float,
        actor: str,
        event_type: str,
        operation_uid: str | None,
        payload: Mapping[str, object],
    ) -> None:
        connection.execute(
            """
            INSERT INTO events(occurred_at, actor, event_type, operation_uid, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (occurred_at, actor, event_type, operation_uid, cls._json_dumps(payload)),
        )

    @staticmethod
    def _json_dumps(value: object) -> str:
        return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _row_to_operation(row: sqlite3.Row) -> OperationRecord:
        return OperationRecord(
            operation_uid=str(row["operation_uid"]),
            operation_id=str(row["operation_id"]),
            operation_version=str(row["operation_version"]),
            parameters=json.loads(row["parameters_json"]),
            worker_revision=str(row["worker_revision"]),
            submitted_by=str(row["submitted_by"]),
            state=OperationState(row["state"]),
            queue_sequence=int(row["queue_sequence"]),
            run_uids=tuple(json.loads(row["run_uids_json"])),
            error_message=row["error_message"],
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )
