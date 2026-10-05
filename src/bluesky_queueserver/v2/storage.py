"""Transactional SQLite authority for QueueServer V2."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from uuid import uuid4

import aiosqlite

from .contracts import (
    ActorKind,
    AttemptState,
    ControllerEvent,
    OperationDescriptor,
    OperationState,
    QueueExecutionPolicy,
    QueueExecutionState,
    WorkerCatalog,
    canonical_json,
    descriptor_fingerprint,
    require_json_object,
    validate_operation_request,
)

SQLITE_APPLICATION_ID = 1_364_416_050  # 0x51535632, ASCII "QSV2"
SQLITE_BUSY_TIMEOUT_MILLISECONDS = 5_000

_REMOTE_FILESYSTEMS = frozenset(
    {
        "9p",
        "ceph",
        "cifs",
        "fuse.9p",
        "fuse.ceph",
        "fuse.glusterfs",
        "fuse.sshfs",
        "glusterfs",
        "nfs",
        "nfs4",
        "smb",
        "smb2",
        "smb3",
        "smbfs",
    }
)
EVENT_TYPES = frozenset(
    {
        "attempt.stop_acknowledged",
        "attempt.stop_requested",
        "controller.shutdown_requested",
        "dispatch.blocked",
        "lease.acquired",
        "lease.expired",
        "lease.overridden",
        "lease.released",
        "lease.renewed",
        "operation.aborted",
        "operation.cancelled",
        "operation.claimed",
        "operation.failed",
        "operation.interrupted",
        "operation.queued",
        "operation.replaced",
        "operation.running",
        "operation.submitted",
        "operation.succeeded",
        "operation.unknown",
        "queue.reordered",
        "queue_execution.admitted",
        "queue_execution.blocked",
        "queue_execution.completed",
        "queue_execution.started",
        "queue_execution.stop_requested",
        "queue_execution.stopped",
        "recovery.acknowledged",
        "restore.requires_review",
        "worker.exited",
        "worker.fenced",
        "worker.ready",
        "worker.started",
    }
)
_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")


class StorageError(RuntimeError):
    """Base error for the V2 SQLite authority."""


class StoragePathError(StorageError):
    """The database path cannot provide local single-host authority."""


class StorageIdentityError(StorageError):
    """The SQLite file is not a QueueServer V2 authority database."""


class StorageVersionError(StorageError):
    """The database schema cannot be used by this controller."""


class StorageConfigurationError(StorageError):
    """Persisted authority metadata disagrees with process configuration."""


class RevisionConflictError(StorageError):
    def __init__(self, *, expected_revision: int, current_revision: int):
        self.expected_revision = expected_revision
        self.current_revision = current_revision
        super().__init__(f"queue revision {expected_revision} is stale; current revision is {current_revision}")


class RecordNotFoundError(StorageError):
    """A requested durable record does not exist."""


class StateConflictError(StorageError):
    """A requested transition is invalid for the current durable state."""


class QueueValidationError(StorageError):
    """A requested queue ordering is not the complete pending queue."""


class LeaseConflictError(StorageError):
    """A lease exists or is not owned by the caller."""


class LeaseOwnershipError(StorageError):
    """The caller does not own an active control lease."""


class LeaseExpiredError(StorageError):
    """The control lease has expired."""


class CatalogCompatibilityError(StorageError):
    """A worker catalog cannot execute persisted nonterminal operations."""


def _freeze_json(value: object) -> object:
    if type(value) is dict:
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_freeze_json(item) for item in value)
    return value


def thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


@dataclass(frozen=True)
class OperationRecord:
    operation_uid: str
    operation_id: str
    operation_version: str
    parameters: Mapping[str, object]
    descriptor_fingerprint: str
    submitted_by: str
    state: OperationState
    submitted_at: int
    updated_at: int
    queue_position: int | None
    result: Mapping[str, object] | None
    replaced_by: str | None


class IdempotencyConflictError(StorageError):
    """An idempotency key was reused for a different request."""


@dataclass(frozen=True)
class IdempotencyRequest:
    principal: str
    method: str
    target: str
    key: str
    body: object
    if_match: str | None

    def __post_init__(self) -> None:
        if not self.principal.strip():
            raise ValueError("idempotency principal must not be blank")
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", self.key):
            raise ValueError("invalid Idempotency-Key")
        if not self.method or self.method != self.method.upper():
            raise ValueError("HTTP method must be uppercase")
        if not self.target.startswith("/"):
            raise ValueError("request target must start with '/'")
        canonical_json(self.body)

    @property
    def request_hash(self) -> str:
        request = {
            "target": self.target,
            "body": self.body,
            "if_match": self.if_match,
        }
        return f"sha256:{hashlib.sha256(canonical_json(request).encode()).hexdigest()}"


@dataclass(frozen=True)
class StoredHttpResponse:
    status: int
    body: Mapping[str, object]
    etag: str | None


@dataclass(frozen=True)
class IdempotencyResult:
    response: StoredHttpResponse
    replayed: bool


@dataclass(frozen=True)
class QueueRecord:
    revision: int
    operations: tuple[OperationRecord, ...]
    active_execution_uid: str | None = None
    dispatch_block_uid: str | None = None


@dataclass(frozen=True)
class QueueExecutionRecord:
    queue_execution_uid: str
    state: QueueExecutionState
    policy: QueueExecutionPolicy
    initiated_by: str
    starting_revision: int
    admitted_operation_uids: tuple[str, ...]
    created_at: int
    updated_at: int
    stop_requested_at: int | None
    completed_at: int | None


@dataclass(frozen=True)
class ControlLeaseRecord:
    lease_uid: str
    subject: str
    issued_at: int
    expires_at: int


@dataclass(frozen=True)
class WorkerInstanceRecord:
    worker_instance_uid: str
    worker_revision: str
    worker_provenance: Mapping[str, object]
    catalog: WorkerCatalog
    state: str
    lock_path: str
    pid: int | None
    started_at: int
    ready_at: int | None
    exited_at: int | None
    exit_code: int | None


@dataclass(frozen=True)
class AttemptRecord:
    attempt_uid: str
    operation_uid: str
    queue_execution_uid: str
    worker_instance_uid: str
    worker_revision: str
    worker_provenance: Mapping[str, object]
    execute_message_uid: str
    scheduler_authorization: str
    state: AttemptState
    created_at: int
    started_at: int | None
    completed_at: int | None
    stop_requested_at: int | None
    stop_acknowledged_at: int | None
    result: Mapping[str, object] | None
    run_uids: tuple[str, ...]
    diagnostic: str | None
    cleanup_completed: bool | None


@dataclass(frozen=True)
class DispatchRecord:
    attempt: AttemptRecord
    operation: OperationRecord


@dataclass(frozen=True)
class StopRequestRecord:
    attempt: AttemptRecord
    queue_revision: int
    contact_worker: bool


@dataclass(frozen=True)
class DispatchBlockRecord:
    dispatch_block_uid: str
    kind: str
    reason: str
    queue_execution_uid: str | None
    attempt_uid: str | None
    requires_fence: bool
    fence_evidence: Mapping[str, object] | None
    created_at: int
    fenced_at: int | None
    acknowledged_at: int | None
    acknowledged_by: str | None
    acknowledgement_note: str | None


def canonical_database_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    try:
        parent = candidate.parent.resolve(strict=True)
    except OSError as exc:
        raise StoragePathError(f"database parent does not exist: {candidate.parent}") from exc
    if not parent.is_dir():
        raise StoragePathError(f"database parent is not a directory: {parent}")
    canonical = candidate.resolve(strict=True) if candidate.exists() else parent / candidate.name
    if canonical.exists() and not canonical.is_file():
        raise StoragePathError(f"database path is not a regular file: {canonical}")
    return canonical


def _unescape_mount_field(value: str) -> str:
    return _MOUNT_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), value)


def filesystem_type_from_mountinfo(path: Path, mountinfo: str) -> str:
    canonical = path.resolve(strict=True)
    matches: list[tuple[int, str]] = []
    for line in mountinfo.splitlines():
        before, separator, after = line.partition(" - ")
        if not separator:
            continue
        fields = before.split()
        trailing = after.split()
        if len(fields) < 5 or not trailing:
            continue
        mount_point = Path(_unescape_mount_field(fields[4]))
        try:
            canonical.relative_to(mount_point)
        except ValueError:
            continue
        matches.append((len(mount_point.parts), trailing[0].lower()))
    if not matches:
        raise StoragePathError(f"filesystem type for {canonical} is unidentifiable")
    return max(matches)[1]


def assert_local_filesystem(path: Path) -> str:
    if sys.platform != "linux":
        raise StoragePathError("QueueServer V2 SQLite storage requires Linux mount information")
    try:
        mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
    except OSError as exc:
        raise StoragePathError("cannot read /proc/self/mountinfo") from exc
    filesystem_type = filesystem_type_from_mountinfo(path, mountinfo)
    if filesystem_type in _REMOTE_FILESYSTEMS:
        raise StoragePathError(f"SQLite authority database cannot use {filesystem_type} storage")
    return filesystem_type


@dataclass(frozen=True)
class Migration:
    version: int
    statements: tuple[str, ...]

    @property
    def checksum(self) -> str:
        payload = "\n;\n".join(statement.strip() for statement in self.statements).encode()
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"


_MIGRATION_1_STATEMENTS = (
    """
    CREATE TABLE schema_migrations(
        version INTEGER PRIMARY KEY CHECK(version > 0),
        checksum TEXT NOT NULL,
        applied_at INTEGER NOT NULL CHECK(applied_at >= 0)
    )
    """,
    """
    CREATE TABLE operations(
        operation_uid TEXT PRIMARY KEY,
        operation_id TEXT NOT NULL,
        operation_version TEXT NOT NULL,
        parameters_json TEXT NOT NULL,
        descriptor_fingerprint TEXT NOT NULL,
        submitted_by TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN (
            'submitted', 'queued', 'claimed', 'running', 'succeeded', 'failed',
            'cancelled', 'aborted', 'interrupted', 'unknown'
        )),
        submitted_at INTEGER NOT NULL CHECK(submitted_at >= 0),
        updated_at INTEGER NOT NULL CHECK(updated_at >= submitted_at),
        result_json TEXT,
        replaced_by TEXT UNIQUE REFERENCES operations(operation_uid)
    )
    """,
    """
    CREATE TABLE queue_entries(
        operation_uid TEXT PRIMARY KEY REFERENCES operations(operation_uid),
        position INTEGER NOT NULL UNIQUE CHECK(position >= 0),
        enqueued_at INTEGER NOT NULL CHECK(enqueued_at >= 0)
    )
    """,
    """
    CREATE TABLE queue_executions(
        queue_execution_uid TEXT PRIMARY KEY,
        state TEXT NOT NULL CHECK(state IN ('running', 'stopping', 'completed', 'stopped', 'blocked')),
        policy TEXT NOT NULL CHECK(policy = 'stop_on_non_success'),
        initiated_by TEXT NOT NULL,
        starting_revision INTEGER NOT NULL CHECK(starting_revision >= 0),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        updated_at INTEGER NOT NULL CHECK(updated_at >= created_at),
        stop_requested_at INTEGER,
        completed_at INTEGER
    )
    """,
    """
    CREATE TABLE queue_execution_admissions(
        queue_execution_uid TEXT NOT NULL REFERENCES queue_executions(queue_execution_uid),
        operation_uid TEXT NOT NULL REFERENCES operations(operation_uid),
        admitted_at INTEGER NOT NULL CHECK(admitted_at >= 0),
        admission_revision INTEGER NOT NULL CHECK(admission_revision >= 0),
        admission_order INTEGER NOT NULL CHECK(admission_order >= 0),
        PRIMARY KEY(queue_execution_uid, operation_uid),
        UNIQUE(queue_execution_uid, admission_order)
    )
    """,
    """
    CREATE TABLE worker_instances(
        worker_instance_uid TEXT PRIMARY KEY,
        worker_revision TEXT NOT NULL,
        worker_provenance_json TEXT NOT NULL,
        catalog_json TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('starting', 'ready', 'stopping', 'exited', 'faulted', 'fenced')),
        lock_path TEXT NOT NULL,
        pid INTEGER,
        started_at INTEGER NOT NULL CHECK(started_at >= 0),
        ready_at INTEGER,
        exited_at INTEGER,
        exit_code INTEGER
    )
    """,
    """
    CREATE TABLE execution_attempts(
        attempt_uid TEXT PRIMARY KEY,
        operation_uid TEXT NOT NULL REFERENCES operations(operation_uid),
        queue_execution_uid TEXT NOT NULL REFERENCES queue_executions(queue_execution_uid),
        worker_instance_uid TEXT NOT NULL REFERENCES worker_instances(worker_instance_uid),
        worker_revision TEXT NOT NULL,
        worker_provenance_json TEXT NOT NULL,
        execute_message_uid TEXT NOT NULL,
        scheduler_authorization TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN (
            'claimed', 'running', 'succeeded', 'failed', 'aborted', 'interrupted', 'unknown'
        )),
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        started_at INTEGER,
        completed_at INTEGER,
        stop_requested_at INTEGER,
        stop_acknowledged_at INTEGER,
        result_json TEXT,
        run_uids_json TEXT NOT NULL DEFAULT '[]',
        diagnostic TEXT,
        cleanup_completed INTEGER CHECK(cleanup_completed IN (0, 1))
    )
    """,
    """
    CREATE TABLE dispatch_blocks(
        dispatch_block_uid TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK(kind IN ('failed', 'interrupted', 'unknown', 'restore.requires_review')),
        reason TEXT NOT NULL,
        queue_execution_uid TEXT REFERENCES queue_executions(queue_execution_uid),
        attempt_uid TEXT REFERENCES execution_attempts(attempt_uid),
        requires_fence INTEGER NOT NULL CHECK(requires_fence IN (0, 1)),
        fence_evidence_json TEXT,
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        fenced_at INTEGER,
        acknowledged_at INTEGER,
        acknowledged_by TEXT,
        acknowledgement_note TEXT
    )
    """,
    """
    CREATE TABLE controller_metadata(
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        queue_revision INTEGER NOT NULL CHECK(queue_revision >= 0),
        database_path TEXT NOT NULL,
        controller_lock_path TEXT NOT NULL,
        worker_lock_path TEXT NOT NULL,
        instrument_id TEXT NOT NULL,
        active_dispatch_block_uid TEXT REFERENCES dispatch_blocks(dispatch_block_uid)
    )
    """,
    """
    CREATE TABLE control_lease(
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        lease_uid TEXT NOT NULL UNIQUE,
        subject TEXT NOT NULL,
        issued_at INTEGER NOT NULL CHECK(issued_at >= 0),
        expires_at INTEGER NOT NULL CHECK(expires_at > issued_at)
    )
    """,
    """
    CREATE TABLE controller_events(
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL CHECK(timestamp >= 0),
        actor_kind TEXT NOT NULL CHECK(actor_kind IN ('principal', 'scheduler', 'system')),
        actor_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        operation_uid TEXT REFERENCES operations(operation_uid),
        queue_execution_uid TEXT REFERENCES queue_executions(queue_execution_uid),
        attempt_uid TEXT REFERENCES execution_attempts(attempt_uid),
        worker_instance_uid TEXT REFERENCES worker_instances(worker_instance_uid),
        queue_revision INTEGER CHECK(queue_revision >= 0),
        payload_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE idempotency_records(
        principal TEXT NOT NULL,
        method TEXT NOT NULL,
        target TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        response_status INTEGER NOT NULL,
        response_body_json TEXT NOT NULL,
        response_etag TEXT,
        created_at INTEGER NOT NULL CHECK(created_at >= 0),
        PRIMARY KEY(principal, method, target, idempotency_key)
    )
    """,
    """
    CREATE UNIQUE INDEX one_active_queue_execution
    ON queue_executions((1)) WHERE state IN ('running', 'stopping', 'blocked')
    """,
    """
    CREATE UNIQUE INDEX one_active_worker_instance
    ON worker_instances((1)) WHERE state IN ('starting', 'ready', 'stopping')
    """,
    """
    CREATE UNIQUE INDEX one_active_execution_attempt
    ON execution_attempts((1)) WHERE state IN ('claimed', 'running')
    """,
    """
    CREATE UNIQUE INDEX one_active_dispatch_block
    ON dispatch_blocks((1)) WHERE acknowledged_at IS NULL
    """,
    """
    CREATE TRIGGER operations_immutable_identity
    BEFORE UPDATE OF operation_uid, operation_id, operation_version, parameters_json,
                     descriptor_fingerprint, submitted_by, submitted_at
    ON operations
    BEGIN
        SELECT RAISE(ABORT, 'operation submitted identity is immutable');
    END
    """,
    """
    CREATE TRIGGER admissions_append_only_update
    BEFORE UPDATE ON queue_execution_admissions
    BEGIN
        SELECT RAISE(ABORT, 'queue execution admissions are append-only');
    END
    """,
    """
    CREATE TRIGGER admissions_append_only_delete
    BEFORE DELETE ON queue_execution_admissions
    BEGIN
        SELECT RAISE(ABORT, 'queue execution admissions are append-only');
    END
    """,
    """
    CREATE TRIGGER attempts_identity_immutable
    BEFORE UPDATE OF attempt_uid, operation_uid, queue_execution_uid, worker_instance_uid,
                     worker_revision, worker_provenance_json, execute_message_uid,
                     scheduler_authorization, created_at
    ON execution_attempts
    BEGIN
        SELECT RAISE(ABORT, 'execution attempt identity and provenance are immutable');
    END
    """,
    """
    CREATE TRIGGER attempts_history_preserved
    BEFORE DELETE ON execution_attempts
    BEGIN
        SELECT RAISE(ABORT, 'execution attempt history cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER dispatch_block_identity_immutable
    BEFORE UPDATE OF dispatch_block_uid, kind, reason, queue_execution_uid, attempt_uid,
                     requires_fence, created_at
    ON dispatch_blocks
    BEGIN
        SELECT RAISE(ABORT, 'dispatch block identity is immutable');
    END
    """,
    """
    CREATE TRIGGER dispatch_block_history_preserved
    BEFORE DELETE ON dispatch_blocks
    BEGIN
        SELECT RAISE(ABORT, 'dispatch block history cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER controller_events_append_only_update
    BEFORE UPDATE ON controller_events
    BEGIN
        SELECT RAISE(ABORT, 'controller events are append-only');
    END
    """,
    """
    CREATE TRIGGER controller_events_append_only_delete
    BEFORE DELETE ON controller_events
    BEGIN
        SELECT RAISE(ABORT, 'controller events are append-only');
    END
    """,
)

MIGRATIONS = (Migration(version=1, statements=_MIGRATION_1_STATEMENTS),)
SCHEMA_VERSION = MIGRATIONS[-1].version


class ReentrantAsyncLock:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task[object] | None = None
        self._depth = 0

    async def __aenter__(self) -> ReentrantAsyncLock:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("database lock requires an asyncio task")
        if self._owner is task:
            self._depth += 1
            return self
        await self._lock.acquire()
        self._owner = task
        self._depth = 1
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        task = asyncio.current_task()
        if task is not self._owner:
            raise RuntimeError("database lock released by a different task")
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            self._lock.release()


class SQLiteStore:
    """One asynchronous connection to a local QueueServer V2 authority database."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        instrument_id: str,
        allow_initialize: bool = True,
        allow_migrate: bool = False,
        clock: Callable[[], int] | None = None,
    ):
        if not instrument_id.strip():
            raise ValueError("instrument_id must not be blank")
        self.database_path = canonical_database_path(database_path)
        self.controller_lock_path = Path(f"{self.database_path}.controller.lock")
        self.worker_lock_path = Path(f"{self.database_path}.worker.lock")
        self.instrument_id = instrument_id
        self._allow_initialize = allow_initialize
        self._allow_migrate = allow_migrate
        self._clock = clock or (lambda: time.time_ns() // 1_000)
        self._connection: aiosqlite.Connection | None = None
        self._transaction_lock = ReentrantAsyncLock()
        self._event_condition = asyncio.Condition()
        self._transaction_connection: ContextVar[aiosqlite.Connection | None] = ContextVar(
            f"queueserver_v2_transaction_{id(self)}",
            default=None,
        )
        self._transaction_has_event = False

    @property
    def is_open(self) -> bool:
        return self._connection is not None

    async def __aenter__(self) -> SQLiteStore:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.close()

    async def open(self) -> None:
        if self._connection is not None:
            raise RuntimeError("SQLite store is already open")
        if not self._allow_initialize and not self.database_path.exists():
            raise StorageVersionError(f"database does not exist: {self.database_path}")
        assert_local_filesystem(self.database_path.parent)
        connection = await aiosqlite.connect(
            self.database_path,
            isolation_level=None,
            timeout=SQLITE_BUSY_TIMEOUT_MILLISECONDS / 1_000,
        )
        connection.row_factory = sqlite3.Row
        try:
            await connection.execute("PRAGMA foreign_keys=ON")
            await connection.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MILLISECONDS}")
            application_id = int((await self._fetchone_on(connection, "PRAGMA application_id"))[0])
            tables = await self._fetchall_on(
                connection,
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            empty_database = not tables
            if application_id == 0:
                if not empty_database or not self._allow_initialize:
                    raise StorageIdentityError(
                        f"database {self.database_path} has no QueueServer V2 application ID"
                    )
                await connection.execute(f"PRAGMA application_id={SQLITE_APPLICATION_ID}")
            elif application_id != SQLITE_APPLICATION_ID:
                raise StorageIdentityError(
                    f"database {self.database_path} has application ID {application_id}, "
                    f"expected {SQLITE_APPLICATION_ID}"
                )
            journal_mode = str((await self._fetchone_on(connection, "PRAGMA journal_mode=WAL"))[0]).lower()
            if journal_mode != "wal":
                raise StorageError(f"SQLite refused WAL mode for {self.database_path}: {journal_mode}")
            await connection.execute("PRAGMA synchronous=FULL")
            await self._prepare_schema(connection, empty_database=empty_database)
            await self._verify_metadata(connection)
        except BaseException:
            await connection.close()
            raise
        self._connection = connection

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def pragma_settings(self) -> dict[str, int | str]:
        async with self._transaction_lock:
            connection = self._require_connection()
            return {
                "application_id": int((await self._fetchone_on(connection, "PRAGMA application_id"))[0]),
                "busy_timeout": int((await self._fetchone_on(connection, "PRAGMA busy_timeout"))[0]),
                "foreign_keys": int((await self._fetchone_on(connection, "PRAGMA foreign_keys"))[0]),
                "journal_mode": str((await self._fetchone_on(connection, "PRAGMA journal_mode"))[0]).lower(),
                "synchronous": int((await self._fetchone_on(connection, "PRAGMA synchronous"))[0]),
            }

    async def schema_info(self) -> tuple[int, str]:
        async with self._transaction_lock:
            connection = self._require_connection()
            row = await self._fetchone_on(
                connection,
                "SELECT version, checksum FROM schema_migrations ORDER BY version DESC LIMIT 1",
            )
            return int(row["version"]), str(row["checksum"])

    async def _prepare_schema(self, connection: aiosqlite.Connection, *, empty_database: bool) -> None:
        migration_table = await self._fetchall_on(
            connection,
            "SELECT name FROM sqlite_schema WHERE type='table' AND name='schema_migrations'",
        )
        if not migration_table:
            if not empty_database:
                raise StorageVersionError("QueueServer V2 migration history is missing")
            if not self._allow_initialize:
                raise StorageVersionError("database schema is uninitialized")
            for migration in MIGRATIONS:
                await self._apply_migration(connection, migration)
            return

        rows = await self._fetchall_on(
            connection,
            "SELECT version, checksum FROM schema_migrations ORDER BY version",
        )
        versions = [int(row["version"]) for row in rows]
        if versions != list(range(1, len(versions) + 1)):
            raise StorageVersionError(f"database migration history is not contiguous: {versions}")
        expected = {migration.version: migration for migration in MIGRATIONS}
        for row in rows:
            version = int(row["version"])
            migration = expected.get(version)
            if migration is None:
                raise StorageVersionError(
                    f"database schema version {version} is newer than supported version {SCHEMA_VERSION}"
                )
            if str(row["checksum"]) != migration.checksum:
                raise StorageVersionError(f"database migration {version} checksum does not match")
        current_version = versions[-1] if versions else 0
        user_version = int((await self._fetchone_on(connection, "PRAGMA user_version"))[0])
        if user_version != current_version:
            raise StorageVersionError(
                f"database user_version {user_version} does not match migration version {current_version}"
            )
        if current_version < SCHEMA_VERSION:
            if not self._allow_migrate:
                raise StorageVersionError(
                    f"database schema version {current_version} is older than required version {SCHEMA_VERSION}"
                )
            for migration in MIGRATIONS[current_version:]:
                await self._apply_migration(connection, migration)

    async def _apply_migration(self, connection: aiosqlite.Connection, migration: Migration) -> None:
        await connection.execute("BEGIN IMMEDIATE")
        try:
            for statement in migration.statements:
                await connection.execute(statement)
            if migration.version == 1:
                await connection.execute(
                    """
                    INSERT INTO controller_metadata(
                        singleton, queue_revision, database_path, controller_lock_path,
                        worker_lock_path, instrument_id, active_dispatch_block_uid
                    ) VALUES (1, 0, ?, ?, ?, ?, NULL)
                    """,
                    (
                        str(self.database_path),
                        str(self.controller_lock_path),
                        str(self.worker_lock_path),
                        self.instrument_id,
                    ),
                )
            await connection.execute(
                "INSERT INTO schema_migrations(version, checksum, applied_at) VALUES (?, ?, ?)",
                (migration.version, migration.checksum, self._clock()),
            )
            await connection.execute(f"PRAGMA user_version={migration.version}")
        except BaseException:
            await connection.rollback()
            raise
        else:
            await connection.commit()

    async def _verify_metadata(self, connection: aiosqlite.Connection) -> None:
        row = await self._fetchone_on(
            connection,
            """
            SELECT database_path, controller_lock_path, worker_lock_path, instrument_id
            FROM controller_metadata WHERE singleton=1
            """,
        )
        expected = (
            str(self.database_path),
            str(self.controller_lock_path),
            str(self.worker_lock_path),
            self.instrument_id,
        )
        actual = tuple(
            str(row[key])
            for key in (
                "database_path",
                "controller_lock_path",
                "worker_lock_path",
                "instrument_id",
            )
        )
        if actual != expected:
            raise StorageConfigurationError(
                "database authority metadata does not match the canonical paths and instrument ID"
            )

    async def mark_restore_requires_review(
        self,
        *,
        fence_evidence: Mapping[str, object],
        now: int | None = None,
    ) -> tuple[DispatchBlockRecord, int]:
        evidence_json = canonical_json(dict(fence_evidence))
        timestamp = self._timestamp(now)
        dispatch_block_uid = str(uuid4())
        async with self.transaction() as connection:
            new_revision = await self._increment_revision_on(connection)
            attempts = await self._fetchall_on(
                connection,
                "SELECT * FROM execution_attempts WHERE state IN ('claimed', 'running')",
            )
            for attempt in attempts:
                await connection.execute(
                    """
                    UPDATE execution_attempts
                    SET state='unknown', completed_at=?, diagnostic=?, cleanup_completed=0
                    WHERE attempt_uid=?
                    """,
                    (timestamp, "database restored with a nonterminal attempt", attempt["attempt_uid"]),
                )
                await connection.execute(
                    "UPDATE operations SET state='unknown', updated_at=? WHERE operation_uid=?",
                    (timestamp, attempt["operation_uid"]),
                )
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.SYSTEM,
                    actor_id="restore",
                    event_type="operation.unknown",
                    queue_revision=new_revision,
                    operation_uid=str(attempt["operation_uid"]),
                    queue_execution_uid=str(attempt["queue_execution_uid"]),
                    attempt_uid=str(attempt["attempt_uid"]),
                    worker_instance_uid=str(attempt["worker_instance_uid"]),
                    payload={"reason": "database restored", "cleanup_completed": False},
                )
            await connection.execute(
                """
                UPDATE queue_executions
                SET state='stopped', updated_at=?, completed_at=?
                WHERE state IN ('running', 'stopping', 'blocked')
                """,
                (timestamp, timestamp),
            )
            await connection.execute(
                """
                UPDATE dispatch_blocks
                SET acknowledged_at=?, acknowledged_by='system',
                    acknowledgement_note='superseded by restore review'
                WHERE acknowledged_at IS NULL
                """,
                (timestamp,),
            )
            await connection.execute(
                """
                UPDATE worker_instances SET state='fenced', exited_at=?
                WHERE state IN ('starting', 'ready', 'stopping')
                """,
                (timestamp,),
            )
            await connection.execute(
                """
                INSERT INTO dispatch_blocks(
                    dispatch_block_uid, kind, reason, requires_fence,
                    fence_evidence_json, created_at, fenced_at
                ) VALUES (?, 'restore.requires_review', 'restored state requires operator review', 1, ?, ?, ?)
                """,
                (dispatch_block_uid, evidence_json, timestamp, timestamp),
            )
            await connection.execute(
                "UPDATE controller_metadata SET active_dispatch_block_uid=? WHERE singleton=1",
                (dispatch_block_uid,),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SYSTEM,
                actor_id="restore",
                event_type="restore.requires_review",
                queue_revision=new_revision,
                payload={"dispatch_block_uid": dispatch_block_uid, "fence_evidence": dict(fence_evidence)},
            )
            block = await self._dispatch_block_row_on(connection, dispatch_block_uid)
        return self._row_to_dispatch_block(block), new_revision

    async def get_active_dispatch_block(self) -> DispatchBlockRecord | None:
        async with self._transaction_lock:
            rows = await self._fetchall_on(
                self._require_connection(),
                "SELECT * FROM dispatch_blocks WHERE acknowledged_at IS NULL",
            )
            return None if not rows else self._row_to_dispatch_block(rows[0])

    async def recover_startup(self, *, now: int | None = None) -> DispatchBlockRecord | None:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            blocks = await self._fetchall_on(
                connection,
                "SELECT * FROM dispatch_blocks WHERE acknowledged_at IS NULL",
            )
            attempts = await self._fetchall_on(
                connection,
                "SELECT * FROM execution_attempts WHERE state IN ('claimed', 'running')",
            )
            if blocks and attempts:
                raise StorageError("active dispatch block and nonterminal attempt coexist")
            if attempts:
                attempt = attempts[0]
                new_revision = await self._increment_revision_on(connection)
                diagnostic = "controller restarted with a nonterminal attempt"
                await connection.execute(
                    """
                    UPDATE execution_attempts
                    SET state='unknown', completed_at=?, diagnostic=?, cleanup_completed=0
                    WHERE attempt_uid=?
                    """,
                    (timestamp, diagnostic, attempt["attempt_uid"]),
                )
                await connection.execute(
                    "UPDATE operations SET state='unknown', updated_at=? WHERE operation_uid=?",
                    (timestamp, attempt["operation_uid"]),
                )
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.SYSTEM,
                    actor_id="startup-recovery",
                    event_type="operation.unknown",
                    queue_revision=new_revision,
                    operation_uid=str(attempt["operation_uid"]),
                    queue_execution_uid=str(attempt["queue_execution_uid"]),
                    attempt_uid=str(attempt["attempt_uid"]),
                    worker_instance_uid=str(attempt["worker_instance_uid"]),
                    payload={"reason": diagnostic, "cleanup_completed": False, "run_uids": []},
                )
                await self._block_dispatch_on(
                    connection,
                    attempt_row=attempt,
                    kind="unknown",
                    reason=diagnostic,
                    requires_fence=True,
                    timestamp=timestamp,
                    queue_revision=new_revision,
                )
                blocks = await self._fetchall_on(
                    connection,
                    "SELECT * FROM dispatch_blocks WHERE acknowledged_at IS NULL",
                )
            return None if not blocks else self._row_to_dispatch_block(blocks[0])

    async def record_worker_fenced(
        self,
        *,
        evidence: Mapping[str, object],
        now: int | None = None,
    ) -> DispatchBlockRecord | None:
        evidence_json = canonical_json(dict(evidence))
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            blocks = await self._fetchall_on(
                connection,
                "SELECT * FROM dispatch_blocks WHERE acknowledged_at IS NULL",
            )
            active_workers = await self._fetchall_on(
                connection,
                """
                SELECT worker_instance_uid FROM worker_instances
                WHERE state IN ('starting', 'ready', 'stopping')
                """,
            )
            block = None if not blocks else blocks[0]
            if block is not None and block["fenced_at"] is not None:
                return self._row_to_dispatch_block(block)
            if block is None and not active_workers:
                return None
            new_revision = await self._increment_revision_on(connection)
            if block is not None:
                await connection.execute(
                    "UPDATE dispatch_blocks SET fence_evidence_json=?, fenced_at=? WHERE dispatch_block_uid=?",
                    (evidence_json, timestamp, block["dispatch_block_uid"]),
                )
            for worker in active_workers:
                await connection.execute(
                    "UPDATE worker_instances SET state='fenced', exited_at=? WHERE worker_instance_uid=?",
                    (timestamp, worker["worker_instance_uid"]),
                )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SYSTEM,
                actor_id="controller",
                event_type="worker.fenced",
                queue_revision=new_revision,
                worker_instance_uid=(
                    None if not active_workers else str(active_workers[0]["worker_instance_uid"])
                ),
                payload=dict(evidence),
            )
            if block is None:
                return None
            updated = await self._dispatch_block_row_on(connection, str(block["dispatch_block_uid"]))
        return self._row_to_dispatch_block(updated)

    async def acknowledge_recovery(
        self,
        *,
        principal: str,
        expected_revision: int,
        note: str,
        fence_evidence: Mapping[str, object] | None = None,
        now: int | None = None,
    ) -> int:
        if not note.strip() or len(note) > 1000:
            raise ValueError("recovery note must be nonblank and at most 1000 characters")
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            blocks = await self._fetchall_on(
                connection,
                "SELECT * FROM dispatch_blocks WHERE acknowledged_at IS NULL",
            )
            if not blocks:
                raise StateConflictError("no recovery acknowledgement is required")
            block = blocks[0]
            needs_fence = bool(block["requires_fence"]) and block["fenced_at"] is None
            if needs_fence and fence_evidence is None:
                raise StateConflictError("prior worker authority has not been fenced")
            new_revision = await self._increment_revision_on(connection)
            if fence_evidence is not None and block["fenced_at"] is None:
                evidence_json = canonical_json(dict(fence_evidence))
                await connection.execute(
                    "UPDATE dispatch_blocks SET fence_evidence_json=?, fenced_at=? WHERE dispatch_block_uid=?",
                    (evidence_json, timestamp, block["dispatch_block_uid"]),
                )
                active_workers = await self._fetchall_on(
                    connection,
                    """
                    SELECT worker_instance_uid FROM worker_instances
                    WHERE state IN ('starting', 'ready', 'stopping')
                    """,
                )
                for worker in active_workers:
                    await connection.execute(
                        "UPDATE worker_instances SET state='fenced', exited_at=? WHERE worker_instance_uid=?",
                        (timestamp, worker["worker_instance_uid"]),
                    )
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.SYSTEM,
                    actor_id="controller",
                    event_type="worker.fenced",
                    queue_revision=new_revision,
                    worker_instance_uid=(
                        None if not active_workers else str(active_workers[0]["worker_instance_uid"])
                    ),
                    payload=dict(fence_evidence),
                )
            await connection.execute(
                """
                UPDATE dispatch_blocks
                SET acknowledged_at=?, acknowledged_by=?, acknowledgement_note=?
                WHERE dispatch_block_uid=?
                """,
                (timestamp, principal, note, block["dispatch_block_uid"]),
            )
            await connection.execute(
                "UPDATE controller_metadata SET active_dispatch_block_uid=NULL WHERE singleton=1"
            )
            if block["queue_execution_uid"] is not None:
                await connection.execute(
                    """
                    UPDATE queue_executions SET state='stopped', updated_at=?, completed_at=?
                    WHERE queue_execution_uid=? AND state='blocked'
                    """,
                    (timestamp, timestamp, block["queue_execution_uid"]),
                )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="recovery.acknowledged",
                queue_revision=new_revision,
                queue_execution_uid=(
                    None if block["queue_execution_uid"] is None else str(block["queue_execution_uid"])
                ),
                attempt_uid=None if block["attempt_uid"] is None else str(block["attempt_uid"]),
                payload={"dispatch_block_uid": str(block["dispatch_block_uid"]), "note": note},
            )
        return new_revision

    async def _dispatch_block_row_on(
        self,
        connection: aiosqlite.Connection,
        dispatch_block_uid: str,
    ) -> sqlite3.Row:
        rows = await self._fetchall_on(
            connection,
            "SELECT * FROM dispatch_blocks WHERE dispatch_block_uid=?",
            (dispatch_block_uid,),
        )
        if not rows:
            raise RecordNotFoundError(f"dispatch block {dispatch_block_uid} does not exist")
        return rows[0]

    @staticmethod
    def _row_to_dispatch_block(row: sqlite3.Row) -> DispatchBlockRecord:
        evidence = (
            None
            if row["fence_evidence_json"] is None
            else _freeze_json(json.loads(str(row["fence_evidence_json"])))
        )
        if evidence is not None and not isinstance(evidence, Mapping):
            raise StorageError("stored fence evidence is not a JSON object")
        return DispatchBlockRecord(
            dispatch_block_uid=str(row["dispatch_block_uid"]),
            kind=str(row["kind"]),
            reason=str(row["reason"]),
            queue_execution_uid=(None if row["queue_execution_uid"] is None else str(row["queue_execution_uid"])),
            attempt_uid=None if row["attempt_uid"] is None else str(row["attempt_uid"]),
            requires_fence=bool(row["requires_fence"]),
            fence_evidence=evidence,
            created_at=int(row["created_at"]),
            fenced_at=None if row["fenced_at"] is None else int(row["fenced_at"]),
            acknowledged_at=None if row["acknowledged_at"] is None else int(row["acknowledged_at"]),
            acknowledged_by=None if row["acknowledged_by"] is None else str(row["acknowledged_by"]),
            acknowledgement_note=(
                None if row["acknowledgement_note"] is None else str(row["acknowledgement_note"])
            ),
        )

    async def register_ready_worker(
        self,
        *,
        worker_instance_uid: str,
        catalog: WorkerCatalog,
        lock_path: str | Path,
        pid: int | None,
        now: int | None = None,
    ) -> WorkerInstanceRecord:
        timestamp = self._timestamp(now)
        canonical_lock_path = Path(lock_path).resolve(strict=False)
        if canonical_lock_path != self.worker_lock_path:
            raise StorageConfigurationError("worker lock path does not match controller authority metadata")
        catalog_json = canonical_json(catalog.model_dump(mode="json"))
        provenance_json = canonical_json(catalog.worker_provenance)
        async with self.transaction() as connection:
            active = await self._fetchall_on(
                connection,
                """
                SELECT worker_instance_uid FROM worker_instances
                WHERE state IN ('starting', 'ready', 'stopping')
                """,
            )
            if active:
                raise StateConflictError("a worker instance is already active")
            await self._validate_catalog_compatibility_on(connection, catalog)
            await connection.execute(
                """
                INSERT INTO worker_instances(
                    worker_instance_uid, worker_revision, worker_provenance_json, catalog_json,
                    state, lock_path, pid, started_at, ready_at
                ) VALUES (?, ?, ?, ?, 'ready', ?, ?, ?, ?)
                """,
                (
                    worker_instance_uid,
                    catalog.worker_revision,
                    provenance_json,
                    catalog_json,
                    str(canonical_lock_path),
                    pid,
                    timestamp,
                    timestamp,
                ),
            )
            for event_type in ("worker.started", "worker.ready"):
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.SYSTEM,
                    actor_id="controller",
                    event_type=event_type,
                    queue_revision=None,
                    worker_instance_uid=worker_instance_uid,
                    payload={
                        "worker_revision": catalog.worker_revision,
                        "worker_provenance": catalog.worker_provenance,
                    },
                )
            row = await self._worker_instance_row_on(connection, worker_instance_uid)
        return self._row_to_worker_instance(row)

    async def mark_worker_exited(
        self,
        *,
        worker_instance_uid: str,
        exit_code: int | None,
        faulted: bool,
        now: int | None = None,
    ) -> WorkerInstanceRecord:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            row = await self._worker_instance_row_on(connection, worker_instance_uid)
            if row["state"] not in {"starting", "ready", "stopping"}:
                raise StateConflictError(f"worker instance {worker_instance_uid} is not active")
            state = "faulted" if faulted else "exited"
            await connection.execute(
                """
                UPDATE worker_instances SET state=?, exited_at=?, exit_code=?
                WHERE worker_instance_uid=?
                """,
                (state, timestamp, exit_code, worker_instance_uid),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SYSTEM,
                actor_id="controller",
                event_type="worker.exited",
                queue_revision=None,
                worker_instance_uid=worker_instance_uid,
                payload={"state": state, "exit_code": exit_code},
            )
            updated = await self._worker_instance_row_on(connection, worker_instance_uid)
        return self._row_to_worker_instance(updated)

    async def _validate_catalog_compatibility_on(
        self,
        connection: aiosqlite.Connection,
        catalog: WorkerCatalog,
    ) -> None:
        descriptors = {
            (descriptor.operation_id, descriptor.operation_version): descriptor
            for descriptor in catalog.operations
        }
        rows = await self._fetchall_on(
            connection,
            """
            SELECT operation_id, operation_version, descriptor_fingerprint
            FROM operations
            WHERE state IN ('submitted', 'queued', 'claimed', 'running')
            """,
        )
        for row in rows:
            identity = (str(row["operation_id"]), str(row["operation_version"]))
            descriptor = descriptors.get(identity)
            if descriptor is None:
                raise CatalogCompatibilityError(
                    f"worker catalog is missing nonterminal operation {identity[0]!r} version {identity[1]!r}"
                )
            if descriptor_fingerprint(descriptor) != str(row["descriptor_fingerprint"]):
                raise CatalogCompatibilityError(
                    f"worker catalog changed the schema for {identity[0]!r} version {identity[1]!r}"
                )

    async def get_active_worker(self) -> WorkerInstanceRecord | None:
        async with self._transaction_lock:
            rows = await self._fetchall_on(
                self._require_connection(),
                "SELECT * FROM worker_instances WHERE state IN ('starting', 'ready', 'stopping')",
            )
            return None if not rows else self._row_to_worker_instance(rows[0])

    async def get_control_lease(self) -> ControlLeaseRecord | None:
        async with self._transaction_lock:
            rows = await self._fetchall_on(
                self._require_connection(),
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1",
            )
            return None if not rows else self._row_to_lease(rows[0])

    async def acquire_control_lease(
        self,
        *,
        principal: str,
        ttl_seconds: int = 300,
        now: int | None = None,
    ) -> ControlLeaseRecord:
        self._validate_principal(principal)
        self._validate_lease_ttl(ttl_seconds)
        timestamp = self._timestamp(now)
        expires_at = timestamp + ttl_seconds * 1_000_000
        lease_uid = str(uuid4())
        async with self.transaction() as connection:
            rows = await self._fetchall_on(
                connection,
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1",
            )
            if rows and int(rows[0]["expires_at"]) > timestamp:
                raise LeaseConflictError(f"control lease is held by {rows[0]['subject']!r}")
            if rows:
                await self._expire_lease_on(connection, row=rows[0], timestamp=timestamp)
            await connection.execute(
                """
                INSERT INTO control_lease(singleton, lease_uid, subject, issued_at, expires_at)
                VALUES (1, ?, ?, ?, ?)
                """,
                (lease_uid, principal, timestamp, expires_at),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="lease.acquired",
                queue_revision=None,
                payload={"expires_at": expires_at},
            )
        return ControlLeaseRecord(
            lease_uid=lease_uid,
            subject=principal,
            issued_at=timestamp,
            expires_at=expires_at,
        )

    async def renew_control_lease(
        self,
        *,
        principal: str,
        ttl_seconds: int = 300,
        now: int | None = None,
    ) -> ControlLeaseRecord:
        self._validate_principal(principal)
        self._validate_lease_ttl(ttl_seconds)
        timestamp = self._timestamp(now)
        expires_at = timestamp + ttl_seconds * 1_000_000
        expired = False
        lease: ControlLeaseRecord | None = None
        async with self.transaction() as connection:
            rows = await self._fetchall_on(
                connection,
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1",
            )
            if not rows:
                raise LeaseOwnershipError("no control lease is active")
            row = rows[0]
            if int(row["expires_at"]) <= timestamp:
                await self._expire_lease_on(connection, row=row, timestamp=timestamp)
                expired = True
            elif str(row["subject"]) != principal:
                raise LeaseOwnershipError("control lease belongs to another principal")
            else:
                await connection.execute(
                    "UPDATE control_lease SET expires_at=? WHERE singleton=1",
                    (expires_at,),
                )
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.PRINCIPAL,
                    actor_id=principal,
                    event_type="lease.renewed",
                    queue_revision=None,
                    payload={"expires_at": expires_at},
                )
                lease = ControlLeaseRecord(
                    lease_uid=str(row["lease_uid"]),
                    subject=principal,
                    issued_at=int(row["issued_at"]),
                    expires_at=expires_at,
                )
        if expired:
            raise LeaseExpiredError("control lease has expired")
        assert lease is not None
        return lease

    async def release_control_lease(self, *, principal: str, now: int | None = None) -> None:
        self._validate_principal(principal)
        timestamp = self._timestamp(now)
        expired = False
        async with self.transaction() as connection:
            rows = await self._fetchall_on(
                connection,
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1",
            )
            if not rows:
                raise LeaseOwnershipError("no control lease is active")
            row = rows[0]
            if int(row["expires_at"]) <= timestamp:
                await self._expire_lease_on(connection, row=row, timestamp=timestamp)
                expired = True
            elif str(row["subject"]) != principal:
                raise LeaseOwnershipError("control lease belongs to another principal")
            else:
                await connection.execute("DELETE FROM control_lease WHERE singleton=1")
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.PRINCIPAL,
                    actor_id=principal,
                    event_type="lease.released",
                    queue_revision=None,
                    payload={},
                )
        if expired:
            raise LeaseExpiredError("control lease has expired")

    async def override_control_lease(
        self,
        *,
        administrator: str,
        reason: str,
        ttl_seconds: int = 300,
        now: int | None = None,
    ) -> ControlLeaseRecord:
        self._validate_principal(administrator)
        if not reason.strip() or len(reason) > 1000:
            raise ValueError("override reason must be nonblank and at most 1000 characters")
        self._validate_lease_ttl(ttl_seconds)
        timestamp = self._timestamp(now)
        expires_at = timestamp + ttl_seconds * 1_000_000
        lease_uid = str(uuid4())
        async with self.transaction() as connection:
            rows = await self._fetchall_on(
                connection,
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1",
            )
            previous_holder = None if not rows else str(rows[0]["subject"])
            if rows:
                await connection.execute("DELETE FROM control_lease WHERE singleton=1")
            await connection.execute(
                """
                INSERT INTO control_lease(singleton, lease_uid, subject, issued_at, expires_at)
                VALUES (1, ?, ?, ?, ?)
                """,
                (lease_uid, administrator, timestamp, expires_at),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=administrator,
                event_type="lease.overridden",
                queue_revision=None,
                payload={"previous_holder": previous_holder, "reason": reason, "expires_at": expires_at},
            )
        return ControlLeaseRecord(
            lease_uid=lease_uid,
            subject=administrator,
            issued_at=timestamp,
            expires_at=expires_at,
        )

    async def expire_control_lease(self, *, now: int | None = None) -> bool:
        timestamp = self._timestamp(now)
        expired = False
        async with self.transaction() as connection:
            rows = await self._fetchall_on(
                connection,
                "SELECT lease_uid, subject, issued_at, expires_at FROM control_lease WHERE singleton=1",
            )
            if rows and int(rows[0]["expires_at"]) <= timestamp:
                await self._expire_lease_on(connection, row=rows[0], timestamp=timestamp)
                expired = True
        return expired

    async def _expire_lease_on(
        self,
        connection: aiosqlite.Connection,
        *,
        row: sqlite3.Row,
        timestamp: int,
    ) -> None:
        await connection.execute("DELETE FROM control_lease WHERE singleton=1")
        await self._insert_event_on(
            connection,
            timestamp=timestamp,
            actor_kind=ActorKind.SYSTEM,
            actor_id="controller",
            event_type="lease.expired",
            queue_revision=None,
            payload={"holder": str(row["subject"]), "expired_at": int(row["expires_at"])},
        )

    @staticmethod
    def _row_to_lease(row: sqlite3.Row) -> ControlLeaseRecord:
        return ControlLeaseRecord(
            lease_uid=str(row["lease_uid"]),
            subject=str(row["subject"]),
            issued_at=int(row["issued_at"]),
            expires_at=int(row["expires_at"]),
        )

    async def _worker_instance_row_on(
        self,
        connection: aiosqlite.Connection,
        worker_instance_uid: str,
    ) -> sqlite3.Row:
        rows = await self._fetchall_on(
            connection,
            "SELECT * FROM worker_instances WHERE worker_instance_uid=?",
            (worker_instance_uid,),
        )
        if not rows:
            raise RecordNotFoundError(f"worker instance {worker_instance_uid} does not exist")
        return rows[0]

    @staticmethod
    def _row_to_worker_instance(row: sqlite3.Row) -> WorkerInstanceRecord:
        provenance = _freeze_json(json.loads(str(row["worker_provenance_json"])))
        if not isinstance(provenance, Mapping):
            raise StorageError("stored worker provenance is not a JSON object")
        return WorkerInstanceRecord(
            worker_instance_uid=str(row["worker_instance_uid"]),
            worker_revision=str(row["worker_revision"]),
            worker_provenance=provenance,
            catalog=WorkerCatalog.model_validate_json(str(row["catalog_json"])),
            state=str(row["state"]),
            lock_path=str(row["lock_path"]),
            pid=None if row["pid"] is None else int(row["pid"]),
            started_at=int(row["started_at"]),
            ready_at=None if row["ready_at"] is None else int(row["ready_at"]),
            exited_at=None if row["exited_at"] is None else int(row["exited_at"]),
            exit_code=None if row["exit_code"] is None else int(row["exit_code"]),
        )

    def now_micros(self) -> int:
        return self._timestamp(None)

    @staticmethod
    def _validate_lease_ttl(ttl_seconds: int) -> None:
        if type(ttl_seconds) is not int or not 5 <= ttl_seconds <= 3600:
            raise ValueError("lease TTL must be an integer between 5 and 3600 seconds")

    async def _validate_active_lease_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        timestamp: int,
    ) -> None:
        rows = await self._fetchall_on(
            connection,
            "SELECT subject, expires_at FROM control_lease WHERE singleton=1",
        )
        if not rows:
            raise LeaseOwnershipError("no control lease is active")
        row = rows[0]
        if int(row["expires_at"]) <= timestamp:
            raise LeaseExpiredError("control lease has expired")
        if str(row["subject"]) != principal:
            raise LeaseOwnershipError("control lease belongs to another principal")

    async def current_revision(self) -> int:
        async with self._transaction_lock:
            row = await self._fetchone_on(
                self._require_connection(),
                "SELECT queue_revision FROM controller_metadata WHERE singleton=1",
            )
            return int(row["queue_revision"])

    async def get_operation(self, operation_uid: str) -> OperationRecord:
        async with self._transaction_lock:
            row = await self._operation_row_on(self._require_connection(), operation_uid)
            return self._row_to_operation(row)

    async def queue_snapshot(self) -> QueueRecord:
        async with self._transaction_lock:
            connection = self._require_connection()
            metadata = await self._fetchone_on(
                connection,
                "SELECT queue_revision, active_dispatch_block_uid FROM controller_metadata WHERE singleton=1",
            )
            rows = await self._fetchall_on(
                connection,
                """
                SELECT operations.*, queue_entries.position AS queue_position
                FROM queue_entries
                JOIN operations USING(operation_uid)
                ORDER BY queue_entries.position
                """,
            )
            active_execution = await self._fetchall_on(
                connection,
                """
                SELECT queue_execution_uid FROM queue_executions
                WHERE state IN ('running', 'stopping', 'blocked')
                """,
            )
            return QueueRecord(
                revision=int(metadata["queue_revision"]),
                operations=tuple(self._row_to_operation(row) for row in rows),
                active_execution_uid=(
                    None if not active_execution else str(active_execution[0]["queue_execution_uid"])
                ),
                dispatch_block_uid=(
                    None
                    if metadata["active_dispatch_block_uid"] is None
                    else str(metadata["active_dispatch_block_uid"])
                ),
            )

    async def submit_operation(
        self,
        *,
        principal: str,
        expected_revision: int,
        descriptor: OperationDescriptor,
        operation_id: str,
        operation_version: str,
        parameters: object,
        now: int | None = None,
    ) -> tuple[OperationRecord, int]:
        validated = validate_operation_request(
            descriptor,
            operation_id=operation_id,
            operation_version=operation_version,
            parameters=parameters,
        )
        parameters_json = canonical_json(validated)
        fingerprint = descriptor_fingerprint(descriptor)
        operation_uid = str(uuid4())
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            position = int(
                (
                    await self._fetchone_on(
                        connection,
                        "SELECT COALESCE(MAX(position), -1) + 1 AS next_position FROM queue_entries",
                    )
                )["next_position"]
            )
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                """
                INSERT INTO operations(
                    operation_uid, operation_id, operation_version, parameters_json,
                    descriptor_fingerprint, submitted_by, state, submitted_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation_uid,
                    descriptor.operation_id,
                    descriptor.operation_version,
                    parameters_json,
                    fingerprint,
                    principal,
                    OperationState.SUBMITTED.value,
                    timestamp,
                    timestamp,
                ),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="operation.submitted",
                queue_revision=new_revision,
                operation_uid=operation_uid,
                payload={
                    "operation_id": descriptor.operation_id,
                    "operation_version": descriptor.operation_version,
                    "descriptor_fingerprint": fingerprint,
                },
            )
            await connection.execute(
                "UPDATE operations SET state=? WHERE operation_uid=?",
                (OperationState.QUEUED.value, operation_uid),
            )
            await connection.execute(
                "INSERT INTO queue_entries(operation_uid, position, enqueued_at) VALUES (?, ?, ?)",
                (operation_uid, position, timestamp),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="operation.queued",
                queue_revision=new_revision,
                operation_uid=operation_uid,
                payload={"position": position},
            )
            await self._admit_if_running_on(
                connection,
                operation_uid=operation_uid,
                actor_id=principal,
                timestamp=timestamp,
                queue_revision=new_revision,
            )
            row = await self._operation_row_on(connection, operation_uid)
        return self._row_to_operation(row), new_revision

    async def cancel_operation(
        self,
        *,
        principal: str,
        expected_revision: int,
        operation_uid: str,
        now: int | None = None,
    ) -> tuple[OperationRecord, int]:
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            row = await self._operation_row_on(connection, operation_uid)
            if row["state"] != OperationState.QUEUED.value or row["queue_position"] is None:
                raise StateConflictError(f"operation {operation_uid} is not queued")
            await connection.execute("DELETE FROM queue_entries WHERE operation_uid=?", (operation_uid,))
            await connection.execute(
                "UPDATE operations SET state=?, updated_at=? WHERE operation_uid=?",
                (OperationState.CANCELLED.value, timestamp, operation_uid),
            )
            await self._compact_queue_positions_on(connection)
            new_revision = await self._increment_revision_on(connection)
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="operation.cancelled",
                queue_revision=new_revision,
                operation_uid=operation_uid,
                payload={},
            )
            await self._maybe_finish_execution_on(
                connection,
                timestamp=timestamp,
                queue_revision=new_revision,
            )
            updated = await self._operation_row_on(connection, operation_uid)
        return self._row_to_operation(updated), new_revision

    async def replace_operation(
        self,
        *,
        principal: str,
        expected_revision: int,
        operation_uid: str,
        descriptor: OperationDescriptor,
        operation_id: str,
        operation_version: str,
        parameters: object,
        now: int | None = None,
    ) -> tuple[OperationRecord, int]:
        validated = validate_operation_request(
            descriptor,
            operation_id=operation_id,
            operation_version=operation_version,
            parameters=parameters,
        )
        parameters_json = canonical_json(validated)
        fingerprint = descriptor_fingerprint(descriptor)
        replacement_uid = str(uuid4())
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            row = await self._operation_row_on(connection, operation_uid)
            if row["state"] != OperationState.QUEUED.value or row["queue_position"] is None:
                raise StateConflictError(f"operation {operation_uid} is not queued")
            position = int(row["queue_position"])
            new_revision = await self._increment_revision_on(connection)
            await connection.execute("DELETE FROM queue_entries WHERE operation_uid=?", (operation_uid,))
            await connection.execute(
                """
                INSERT INTO operations(
                    operation_uid, operation_id, operation_version, parameters_json,
                    descriptor_fingerprint, submitted_by, state, submitted_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    replacement_uid,
                    descriptor.operation_id,
                    descriptor.operation_version,
                    parameters_json,
                    fingerprint,
                    principal,
                    OperationState.QUEUED.value,
                    timestamp,
                    timestamp,
                ),
            )
            await connection.execute(
                "INSERT INTO queue_entries(operation_uid, position, enqueued_at) VALUES (?, ?, ?)",
                (replacement_uid, position, timestamp),
            )
            await connection.execute(
                "UPDATE operations SET state=?, updated_at=?, replaced_by=? WHERE operation_uid=?",
                (OperationState.CANCELLED.value, timestamp, replacement_uid, operation_uid),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="operation.replaced",
                queue_revision=new_revision,
                operation_uid=operation_uid,
                payload={"replacement_operation_uid": replacement_uid, "position": position},
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="operation.submitted",
                queue_revision=new_revision,
                operation_uid=replacement_uid,
                payload={
                    "operation_id": descriptor.operation_id,
                    "operation_version": descriptor.operation_version,
                    "descriptor_fingerprint": fingerprint,
                    "replaces": operation_uid,
                },
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="operation.queued",
                queue_revision=new_revision,
                operation_uid=replacement_uid,
                payload={"position": position},
            )
            await self._admit_if_running_on(
                connection,
                operation_uid=replacement_uid,
                actor_id=principal,
                timestamp=timestamp,
                queue_revision=new_revision,
            )
            replacement = await self._operation_row_on(connection, replacement_uid)
        return self._row_to_operation(replacement), new_revision

    async def reorder_queue(
        self,
        *,
        principal: str,
        expected_revision: int,
        operation_uids: list[str],
        now: int | None = None,
    ) -> QueueRecord:
        if len(operation_uids) != len(set(operation_uids)):
            raise QueueValidationError("queue reorder contains duplicate operation UIDs")
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            current_rows = await self._fetchall_on(
                connection,
                "SELECT operation_uid FROM queue_entries ORDER BY position",
            )
            current = [str(row["operation_uid"]) for row in current_rows]
            if len(operation_uids) != len(current) or set(operation_uids) != set(current):
                raise QueueValidationError(
                    "queue reorder must contain every currently queued operation exactly once"
                )
            offset = len(current) + 1
            for index, operation_uid in enumerate(operation_uids):
                await connection.execute(
                    "UPDATE queue_entries SET position=? WHERE operation_uid=?",
                    (offset + index, operation_uid),
                )
            for index, operation_uid in enumerate(operation_uids):
                await connection.execute(
                    "UPDATE queue_entries SET position=? WHERE operation_uid=?",
                    (index, operation_uid),
                )
            new_revision = await self._increment_revision_on(connection)
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="queue.reordered",
                queue_revision=new_revision,
                payload={"operation_uids": operation_uids},
            )
        return await self.queue_snapshot()

    async def get_queue_execution(self, queue_execution_uid: str) -> QueueExecutionRecord:
        async with self._transaction_lock:
            return await self._queue_execution_record_on(
                self._require_connection(),
                queue_execution_uid,
            )

    async def start_queue_execution(
        self,
        *,
        principal: str,
        expected_revision: int,
        policy: QueueExecutionPolicy = QueueExecutionPolicy.STOP_ON_NON_SUCCESS,
        now: int | None = None,
    ) -> tuple[QueueExecutionRecord, int]:
        timestamp = self._timestamp(now)
        queue_execution_uid = str(uuid4())
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            active = await self._fetchall_on(
                connection,
                """
                SELECT queue_execution_uid FROM queue_executions
                WHERE state IN ('running', 'stopping', 'blocked')
                """,
            )
            if active:
                raise StateConflictError("a queue execution is already active")
            queued = await self._fetchall_on(
                connection,
                "SELECT operation_uid FROM queue_entries ORDER BY position",
            )
            if not queued:
                raise StateConflictError("cannot start a queue execution for an empty queue")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                """
                INSERT INTO queue_executions(
                    queue_execution_uid, state, policy, initiated_by,
                    starting_revision, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    queue_execution_uid,
                    QueueExecutionState.RUNNING.value,
                    policy.value,
                    principal,
                    expected_revision,
                    timestamp,
                    timestamp,
                ),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="queue_execution.started",
                queue_revision=new_revision,
                queue_execution_uid=queue_execution_uid,
                payload={"policy": policy.value, "starting_revision": expected_revision},
            )
            for admission_order, row in enumerate(queued):
                operation_uid = str(row["operation_uid"])
                await connection.execute(
                    """
                    INSERT INTO queue_execution_admissions(
                        queue_execution_uid, operation_uid, admitted_at,
                        admission_revision, admission_order
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (queue_execution_uid, operation_uid, timestamp, new_revision, admission_order),
                )
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.PRINCIPAL,
                    actor_id=principal,
                    event_type="queue_execution.admitted",
                    queue_revision=new_revision,
                    operation_uid=operation_uid,
                    queue_execution_uid=queue_execution_uid,
                    payload={"admission_order": admission_order},
                )
            record = await self._queue_execution_record_on(connection, queue_execution_uid)
        return record, new_revision

    async def stop_queue_execution(
        self,
        *,
        principal: str,
        expected_revision: int,
        queue_execution_uid: str,
        now: int | None = None,
    ) -> tuple[QueueExecutionRecord, int]:
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            row = await self._queue_execution_row_on(connection, queue_execution_uid)
            if row["state"] != QueueExecutionState.RUNNING.value:
                raise StateConflictError(f"queue execution {queue_execution_uid} is not running")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                """
                UPDATE queue_executions
                SET state=?, stop_requested_at=?, updated_at=?
                WHERE queue_execution_uid=?
                """,
                (QueueExecutionState.STOPPING.value, timestamp, timestamp, queue_execution_uid),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="queue_execution.stop_requested",
                queue_revision=new_revision,
                queue_execution_uid=queue_execution_uid,
                payload={},
            )
            await self._maybe_finish_execution_on(
                connection,
                timestamp=timestamp,
                queue_revision=new_revision,
            )
            record = await self._queue_execution_record_on(connection, queue_execution_uid)
        return record, new_revision

    async def _admit_if_running_on(
        self,
        connection: aiosqlite.Connection,
        *,
        actor_id: str,
        operation_uid: str,
        timestamp: int,
        queue_revision: int,
    ) -> None:
        executions = await self._fetchall_on(
            connection,
            "SELECT queue_execution_uid FROM queue_executions WHERE state='running'",
        )
        if not executions:
            return
        queue_execution_uid = str(executions[0]["queue_execution_uid"])
        admission_order = int(
            (
                await self._fetchone_on(
                    connection,
                    """
                    SELECT COALESCE(MAX(admission_order), -1) + 1 AS next_order
                    FROM queue_execution_admissions WHERE queue_execution_uid=?
                    """,
                    (queue_execution_uid,),
                )
            )["next_order"]
        )
        await connection.execute(
            """
            INSERT INTO queue_execution_admissions(
                queue_execution_uid, operation_uid, admitted_at, admission_revision, admission_order
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (queue_execution_uid, operation_uid, timestamp, queue_revision, admission_order),
        )
        await self._insert_event_on(
            connection,
            timestamp=timestamp,
            actor_kind=ActorKind.PRINCIPAL,
            actor_id=actor_id,
            event_type="queue_execution.admitted",
            queue_revision=queue_revision,
            operation_uid=operation_uid,
            queue_execution_uid=queue_execution_uid,
            payload={"admission_order": admission_order},
        )

    async def _maybe_finish_execution_on(
        self,
        connection: aiosqlite.Connection,
        *,
        timestamp: int,
        queue_revision: int,
    ) -> None:
        executions = await self._fetchall_on(
            connection,
            """
            SELECT * FROM queue_executions
            WHERE state IN ('running', 'stopping')
            """,
        )
        if not executions:
            return
        execution = executions[0]
        queue_execution_uid = str(execution["queue_execution_uid"])
        active_attempts = await self._fetchall_on(
            connection,
            """
            SELECT attempt_uid FROM execution_attempts
            WHERE queue_execution_uid=? AND state IN ('claimed', 'running')
            """,
            (queue_execution_uid,),
        )
        if active_attempts:
            return
        if execution["state"] == QueueExecutionState.RUNNING.value:
            admitted_queued = await self._fetchall_on(
                connection,
                """
                SELECT admissions.operation_uid
                FROM queue_execution_admissions AS admissions
                JOIN operations USING(operation_uid)
                JOIN queue_entries USING(operation_uid)
                WHERE admissions.queue_execution_uid=? AND operations.state='queued'
                """,
                (queue_execution_uid,),
            )
            if admitted_queued:
                return
            terminal_state = QueueExecutionState.COMPLETED
            event_type = "queue_execution.completed"
        else:
            terminal_state = QueueExecutionState.STOPPED
            event_type = "queue_execution.stopped"
        await connection.execute(
            """
            UPDATE queue_executions SET state=?, updated_at=?, completed_at=?
            WHERE queue_execution_uid=?
            """,
            (terminal_state.value, timestamp, timestamp, queue_execution_uid),
        )
        await self._insert_event_on(
            connection,
            timestamp=timestamp,
            actor_kind=ActorKind.SCHEDULER,
            actor_id="scheduler",
            event_type=event_type,
            queue_revision=queue_revision,
            queue_execution_uid=queue_execution_uid,
            payload={},
        )

    async def _queue_execution_row_on(
        self,
        connection: aiosqlite.Connection,
        queue_execution_uid: str,
    ) -> sqlite3.Row:
        rows = await self._fetchall_on(
            connection,
            "SELECT * FROM queue_executions WHERE queue_execution_uid=?",
            (queue_execution_uid,),
        )
        if not rows:
            raise RecordNotFoundError(f"queue execution {queue_execution_uid} does not exist")
        return rows[0]

    async def _queue_execution_record_on(
        self,
        connection: aiosqlite.Connection,
        queue_execution_uid: str,
    ) -> QueueExecutionRecord:
        row = await self._queue_execution_row_on(connection, queue_execution_uid)
        admissions = await self._fetchall_on(
            connection,
            """
            SELECT operation_uid FROM queue_execution_admissions
            WHERE queue_execution_uid=? ORDER BY admission_order
            """,
            (queue_execution_uid,),
        )
        return QueueExecutionRecord(
            queue_execution_uid=str(row["queue_execution_uid"]),
            state=QueueExecutionState(str(row["state"])),
            policy=QueueExecutionPolicy(str(row["policy"])),
            initiated_by=str(row["initiated_by"]),
            starting_revision=int(row["starting_revision"]),
            admitted_operation_uids=tuple(str(item["operation_uid"]) for item in admissions),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            stop_requested_at=None if row["stop_requested_at"] is None else int(row["stop_requested_at"]),
            completed_at=None if row["completed_at"] is None else int(row["completed_at"]),
        )

    async def list_events(self, *, after: int = 0, limit: int = 100) -> tuple[ControllerEvent, ...]:
        if type(after) is not int or after < 0:
            raise ValueError("event cursor must be a nonnegative integer")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("event limit must be between 1 and 1000")
        async with self._transaction_lock:
            rows = await self._fetchall_on(
                self._require_connection(),
                "SELECT * FROM controller_events WHERE event_id>? ORDER BY event_id LIMIT ?",
                (after, limit),
            )
        return tuple(self._row_to_event(row) for row in rows)

    async def wait_for_events(
        self,
        *,
        after: int,
        limit: int = 100,
        timeout: float | None = None,
    ) -> tuple[ControllerEvent, ...]:
        async with self._event_condition:
            events = await self.list_events(after=after, limit=limit)
            if events:
                return events
            try:
                await asyncio.wait_for(self._event_condition.wait(), timeout=timeout)
            except TimeoutError:
                return ()
        return await self.list_events(after=after, limit=limit)

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> ControllerEvent:
        payload = json.loads(str(row["payload_json"]))
        if type(payload) is not dict:
            raise StorageError("stored controller event payload is not a JSON object")
        return ControllerEvent(
            event_id=int(row["event_id"]),
            timestamp=int(row["timestamp"]),
            actor_kind=ActorKind(str(row["actor_kind"])),
            actor_id=str(row["actor_id"]),
            event_type=str(row["event_type"]),
            operation_uid=None if row["operation_uid"] is None else str(row["operation_uid"]),
            queue_execution_uid=(None if row["queue_execution_uid"] is None else str(row["queue_execution_uid"])),
            attempt_uid=None if row["attempt_uid"] is None else str(row["attempt_uid"]),
            worker_instance_uid=(None if row["worker_instance_uid"] is None else str(row["worker_instance_uid"])),
            queue_revision=None if row["queue_revision"] is None else int(row["queue_revision"]),
            payload=payload,
        )

    async def get_active_attempt(self) -> AttemptRecord | None:
        async with self._transaction_lock:
            rows = await self._fetchall_on(
                self._require_connection(),
                "SELECT * FROM execution_attempts WHERE state IN ('claimed', 'running')",
            )
            return None if not rows else self._row_to_attempt(rows[0])

    async def mark_controller_shutdown_requested(
        self,
        *,
        attempt_uid: str,
        now: int | None = None,
    ) -> int:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            attempt = await self._attempt_row_on(connection, attempt_uid)
            if attempt["state"] not in {AttemptState.CLAIMED.value, AttemptState.RUNNING.value}:
                raise StateConflictError(f"attempt {attempt_uid} is already terminal")
            execution = await self._queue_execution_row_on(connection, str(attempt["queue_execution_uid"]))
            if execution["state"] not in {
                QueueExecutionState.RUNNING.value,
                QueueExecutionState.STOPPING.value,
            }:
                raise StateConflictError("active attempt has no stoppable queue execution")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                """
                UPDATE queue_executions
                SET state='stopping', stop_requested_at=COALESCE(stop_requested_at, ?), updated_at=?
                WHERE queue_execution_uid=?
                """,
                (timestamp, timestamp, attempt["queue_execution_uid"]),
            )
            await connection.execute(
                """
                UPDATE execution_attempts
                SET stop_requested_at=COALESCE(stop_requested_at, ?)
                WHERE attempt_uid=?
                """,
                (timestamp, attempt_uid),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SYSTEM,
                actor_id="controller",
                event_type="controller.shutdown_requested",
                queue_revision=new_revision,
                operation_uid=str(attempt["operation_uid"]),
                queue_execution_uid=str(attempt["queue_execution_uid"]),
                attempt_uid=attempt_uid,
                worker_instance_uid=str(attempt["worker_instance_uid"]),
                payload={},
            )
        return new_revision

    async def request_attempt_stop(
        self,
        *,
        principal: str,
        expected_revision: int,
        attempt_uid: str,
        now: int | None = None,
    ) -> StopRequestRecord:
        timestamp = self._timestamp(now)
        await self.expire_control_lease(now=timestamp)
        async with self.transaction() as connection:
            self._validate_principal(principal)
            await self._validate_active_lease_on(connection, principal=principal, timestamp=timestamp)
            await self._validate_revision_on(connection, expected_revision)
            attempt = await self._attempt_row_on(connection, attempt_uid)
            if attempt["state"] not in {AttemptState.CLAIMED.value, AttemptState.RUNNING.value}:
                raise StateConflictError(f"attempt {attempt_uid} is already terminal")
            execution = await self._queue_execution_row_on(connection, str(attempt["queue_execution_uid"]))
            if execution["state"] not in {
                QueueExecutionState.RUNNING.value,
                QueueExecutionState.STOPPING.value,
            }:
                raise StateConflictError(
                    f"queue execution {attempt['queue_execution_uid']} cannot accept a safe stop"
                )
            if attempt["stop_requested_at"] is not None:
                raise StateConflictError(f"safe stop was already requested for attempt {attempt_uid}")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                """
                UPDATE queue_executions
                SET state='stopping', stop_requested_at=COALESCE(stop_requested_at, ?), updated_at=?
                WHERE queue_execution_uid=?
                """,
                (timestamp, timestamp, attempt["queue_execution_uid"]),
            )
            await connection.execute(
                "UPDATE execution_attempts SET stop_requested_at=? WHERE attempt_uid=?",
                (timestamp, attempt_uid),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.PRINCIPAL,
                actor_id=principal,
                event_type="attempt.stop_requested",
                queue_revision=new_revision,
                operation_uid=str(attempt["operation_uid"]),
                queue_execution_uid=str(attempt["queue_execution_uid"]),
                attempt_uid=attempt_uid,
                worker_instance_uid=str(attempt["worker_instance_uid"]),
                payload={},
            )
            contact_worker = attempt["state"] == AttemptState.RUNNING.value
            if not contact_worker:
                await connection.execute(
                    """
                    UPDATE execution_attempts
                    SET state='aborted', completed_at=?, cleanup_completed=1
                    WHERE attempt_uid=?
                    """,
                    (timestamp, attempt_uid),
                )
                await connection.execute(
                    "UPDATE operations SET state='aborted', updated_at=? WHERE operation_uid=?",
                    (timestamp, attempt["operation_uid"]),
                )
                await self._insert_event_on(
                    connection,
                    timestamp=timestamp,
                    actor_kind=ActorKind.PRINCIPAL,
                    actor_id=principal,
                    event_type="operation.aborted",
                    queue_revision=new_revision,
                    operation_uid=str(attempt["operation_uid"]),
                    queue_execution_uid=str(attempt["queue_execution_uid"]),
                    attempt_uid=attempt_uid,
                    worker_instance_uid=str(attempt["worker_instance_uid"]),
                    payload={"before_worker_contact": True, "run_uids": []},
                )
                await self._maybe_finish_execution_on(
                    connection,
                    timestamp=timestamp,
                    queue_revision=new_revision,
                )
            updated = await self._attempt_row_on(connection, attempt_uid)
        return StopRequestRecord(
            attempt=self._row_to_attempt(updated),
            queue_revision=new_revision,
            contact_worker=contact_worker,
        )

    async def acknowledge_attempt_stop(
        self,
        *,
        attempt_uid: str,
        now: int | None = None,
    ) -> tuple[AttemptRecord, int]:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            attempt = await self._attempt_row_on(connection, attempt_uid)
            if attempt["state"] != AttemptState.RUNNING.value or attempt["stop_requested_at"] is None:
                raise StateConflictError(f"attempt {attempt_uid} has no active safe-stop request")
            if attempt["stop_acknowledged_at"] is not None:
                raise StateConflictError(f"safe stop was already acknowledged for attempt {attempt_uid}")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                "UPDATE execution_attempts SET stop_acknowledged_at=? WHERE attempt_uid=?",
                (timestamp, attempt_uid),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SCHEDULER,
                actor_id=str(attempt["scheduler_authorization"]),
                event_type="attempt.stop_acknowledged",
                queue_revision=new_revision,
                operation_uid=str(attempt["operation_uid"]),
                queue_execution_uid=str(attempt["queue_execution_uid"]),
                attempt_uid=attempt_uid,
                worker_instance_uid=str(attempt["worker_instance_uid"]),
                payload={},
            )
            updated = await self._attempt_row_on(connection, attempt_uid)
        return self._row_to_attempt(updated), new_revision

    async def claim_next_attempt(
        self,
        *,
        worker_instance_uid: str,
        now: int | None = None,
    ) -> DispatchRecord | None:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            metadata = await self._fetchone_on(
                connection,
                "SELECT active_dispatch_block_uid FROM controller_metadata WHERE singleton=1",
            )
            if metadata["active_dispatch_block_uid"] is not None:
                return None
            worker = await self._worker_instance_row_on(connection, worker_instance_uid)
            if worker["state"] != "ready":
                raise StateConflictError(f"worker instance {worker_instance_uid} is not ready")
            active_attempt = await self._fetchall_on(
                connection,
                "SELECT attempt_uid FROM execution_attempts WHERE state IN ('claimed', 'running')",
            )
            if active_attempt:
                return None
            executions = await self._fetchall_on(
                connection,
                "SELECT queue_execution_uid FROM queue_executions WHERE state='running'",
            )
            if not executions:
                return None
            queue_execution_uid = str(executions[0]["queue_execution_uid"])
            operations = await self._fetchall_on(
                connection,
                """
                SELECT operations.*, queue_entries.position AS queue_position
                FROM queue_entries
                JOIN operations USING(operation_uid)
                JOIN queue_execution_admissions AS admissions USING(operation_uid)
                WHERE admissions.queue_execution_uid=? AND operations.state='queued'
                ORDER BY queue_entries.position
                LIMIT 1
                """,
                (queue_execution_uid,),
            )
            if not operations:
                return None
            operation_row = operations[0]
            operation_uid = str(operation_row["operation_uid"])
            attempt_uid = str(uuid4())
            execute_message_uid = str(uuid4())
            scheduler_authorization = f"queue-execution:{queue_execution_uid}"
            new_revision = await self._increment_revision_on(connection)
            await connection.execute("DELETE FROM queue_entries WHERE operation_uid=?", (operation_uid,))
            await self._compact_queue_positions_on(connection)
            await connection.execute(
                "UPDATE operations SET state=?, updated_at=? WHERE operation_uid=?",
                (OperationState.CLAIMED.value, timestamp, operation_uid),
            )
            await connection.execute(
                """
                INSERT INTO execution_attempts(
                    attempt_uid, operation_uid, queue_execution_uid, worker_instance_uid,
                    worker_revision, worker_provenance_json, execute_message_uid,
                    scheduler_authorization, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_uid,
                    operation_uid,
                    queue_execution_uid,
                    worker_instance_uid,
                    str(worker["worker_revision"]),
                    str(worker["worker_provenance_json"]),
                    execute_message_uid,
                    scheduler_authorization,
                    AttemptState.CLAIMED.value,
                    timestamp,
                ),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SCHEDULER,
                actor_id=scheduler_authorization,
                event_type="operation.claimed",
                queue_revision=new_revision,
                operation_uid=operation_uid,
                queue_execution_uid=queue_execution_uid,
                attempt_uid=attempt_uid,
                worker_instance_uid=worker_instance_uid,
                payload={"execute_message_uid": execute_message_uid},
            )
            attempt_row = await self._attempt_row_on(connection, attempt_uid)
            operation_row = await self._operation_row_on(connection, operation_uid)
        return DispatchRecord(
            attempt=self._row_to_attempt(attempt_row),
            operation=self._row_to_operation(operation_row),
        )

    async def mark_attempt_running(
        self,
        *,
        attempt_uid: str,
        now: int | None = None,
    ) -> AttemptRecord:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            row = await self._attempt_row_on(connection, attempt_uid)
            if row["state"] != AttemptState.CLAIMED.value:
                raise StateConflictError(f"attempt {attempt_uid} is not claimed")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                "UPDATE execution_attempts SET state=?, started_at=? WHERE attempt_uid=?",
                (AttemptState.RUNNING.value, timestamp, attempt_uid),
            )
            await connection.execute(
                "UPDATE operations SET state=?, updated_at=? WHERE operation_uid=?",
                (OperationState.RUNNING.value, timestamp, row["operation_uid"]),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SCHEDULER,
                actor_id=str(row["scheduler_authorization"]),
                event_type="operation.running",
                queue_revision=new_revision,
                operation_uid=str(row["operation_uid"]),
                queue_execution_uid=str(row["queue_execution_uid"]),
                attempt_uid=attempt_uid,
                worker_instance_uid=str(row["worker_instance_uid"]),
                payload={},
            )
            updated = await self._attempt_row_on(connection, attempt_uid)
        return self._row_to_attempt(updated)

    async def complete_attempt(
        self,
        *,
        attempt_uid: str,
        state: AttemptState,
        result: Mapping[str, object] | None,
        run_uids: tuple[str, ...],
        diagnostic: str | None,
        cleanup_completed: bool,
        fence_evidence: Mapping[str, object] | None = None,
        now: int | None = None,
    ) -> AttemptRecord:
        if state not in {
            AttemptState.SUCCEEDED,
            AttemptState.FAILED,
            AttemptState.ABORTED,
            AttemptState.INTERRUPTED,
            AttemptState.UNKNOWN,
        }:
            raise ValueError(f"attempt completion state must be terminal, not {state.value}")
        if any(type(uid) is not str or not uid for uid in run_uids):
            raise ValueError("run_uids must contain nonempty strings")
        result_json = None if result is None else canonical_json(dict(result))
        run_uids_json = canonical_json(list(run_uids))
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            row = await self._attempt_row_on(connection, attempt_uid)
            if row["state"] not in {AttemptState.CLAIMED.value, AttemptState.RUNNING.value}:
                raise StateConflictError(f"attempt {attempt_uid} is already terminal")
            new_revision = await self._increment_revision_on(connection)
            await connection.execute(
                """
                UPDATE execution_attempts
                SET state=?, completed_at=?, result_json=?, run_uids_json=?,
                    diagnostic=?, cleanup_completed=?
                WHERE attempt_uid=?
                """,
                (
                    state.value,
                    timestamp,
                    result_json,
                    run_uids_json,
                    diagnostic,
                    int(cleanup_completed),
                    attempt_uid,
                ),
            )
            await connection.execute(
                "UPDATE operations SET state=?, result_json=?, updated_at=? WHERE operation_uid=?",
                (state.value, result_json, timestamp, row["operation_uid"]),
            )
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SCHEDULER,
                actor_id=str(row["scheduler_authorization"]),
                event_type=f"operation.{state.value}",
                queue_revision=new_revision,
                operation_uid=str(row["operation_uid"]),
                queue_execution_uid=str(row["queue_execution_uid"]),
                attempt_uid=attempt_uid,
                worker_instance_uid=str(row["worker_instance_uid"]),
                payload={
                    "run_uids": list(run_uids),
                    "diagnostic": diagnostic,
                    "cleanup_completed": cleanup_completed,
                },
            )
            if state in {AttemptState.FAILED, AttemptState.INTERRUPTED, AttemptState.UNKNOWN}:
                await self._block_dispatch_on(
                    connection,
                    attempt_row=row,
                    kind=state.value,
                    reason=diagnostic or f"attempt ended {state.value}",
                    requires_fence=state in {AttemptState.INTERRUPTED, AttemptState.UNKNOWN},
                    timestamp=timestamp,
                    queue_revision=new_revision,
                    fence_evidence=fence_evidence,
                )
            else:
                if state is AttemptState.ABORTED:
                    await connection.execute(
                        """
                        UPDATE queue_executions
                        SET state='stopping', stop_requested_at=COALESCE(stop_requested_at, ?), updated_at=?
                        WHERE queue_execution_uid=? AND state='running'
                        """,
                        (timestamp, timestamp, row["queue_execution_uid"]),
                    )
                await self._maybe_finish_execution_on(
                    connection,
                    timestamp=timestamp,
                    queue_revision=new_revision,
                )
            updated = await self._attempt_row_on(connection, attempt_uid)
        return self._row_to_attempt(updated)

    async def get_attempt(self, attempt_uid: str) -> AttemptRecord:
        async with self._transaction_lock:
            row = await self._attempt_row_on(self._require_connection(), attempt_uid)
            return self._row_to_attempt(row)

    async def get_latest_attempt_for_operation(self, operation_uid: str) -> AttemptRecord | None:
        async with self._transaction_lock:
            rows = await self._fetchall_on(
                self._require_connection(),
                """
                SELECT * FROM execution_attempts
                WHERE operation_uid=? ORDER BY created_at DESC, rowid DESC LIMIT 1
                """,
                (operation_uid,),
            )
            return None if not rows else self._row_to_attempt(rows[0])

    async def _block_dispatch_on(
        self,
        connection: aiosqlite.Connection,
        *,
        attempt_row: sqlite3.Row,
        kind: str,
        reason: str,
        requires_fence: bool,
        fence_evidence: Mapping[str, object] | None = None,
        timestamp: int,
        queue_revision: int,
    ) -> str:
        dispatch_block_uid = str(uuid4())
        evidence_json = None if fence_evidence is None else canonical_json(dict(fence_evidence))
        await connection.execute(
            """
            INSERT INTO dispatch_blocks(
                dispatch_block_uid, kind, reason, queue_execution_uid, attempt_uid,
                requires_fence, fence_evidence_json, created_at, fenced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dispatch_block_uid,
                kind,
                reason,
                attempt_row["queue_execution_uid"],
                attempt_row["attempt_uid"],
                int(requires_fence),
                evidence_json,
                timestamp,
                timestamp if fence_evidence is not None else None,
            ),
        )
        await connection.execute(
            "UPDATE controller_metadata SET active_dispatch_block_uid=? WHERE singleton=1",
            (dispatch_block_uid,),
        )
        await connection.execute(
            "UPDATE queue_executions SET state='blocked', updated_at=? WHERE queue_execution_uid=?",
            (timestamp, attempt_row["queue_execution_uid"]),
        )
        for event_type in ("dispatch.blocked", "queue_execution.blocked"):
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SCHEDULER,
                actor_id=str(attempt_row["scheduler_authorization"]),
                event_type=event_type,
                queue_revision=queue_revision,
                operation_uid=str(attempt_row["operation_uid"]),
                queue_execution_uid=str(attempt_row["queue_execution_uid"]),
                attempt_uid=str(attempt_row["attempt_uid"]),
                worker_instance_uid=str(attempt_row["worker_instance_uid"]),
                payload={
                    "dispatch_block_uid": dispatch_block_uid,
                    "kind": kind,
                    "reason": reason,
                    "requires_fence": requires_fence,
                    "fence_evidence": None if fence_evidence is None else dict(fence_evidence),
                },
            )
        if fence_evidence is not None:
            await self._insert_event_on(
                connection,
                timestamp=timestamp,
                actor_kind=ActorKind.SYSTEM,
                actor_id="controller",
                event_type="worker.fenced",
                queue_revision=queue_revision,
                worker_instance_uid=str(attempt_row["worker_instance_uid"]),
                payload=dict(fence_evidence),
            )
        return dispatch_block_uid

    async def _attempt_row_on(self, connection: aiosqlite.Connection, attempt_uid: str) -> sqlite3.Row:
        rows = await self._fetchall_on(
            connection,
            "SELECT * FROM execution_attempts WHERE attempt_uid=?",
            (attempt_uid,),
        )
        if not rows:
            raise RecordNotFoundError(f"attempt {attempt_uid} does not exist")
        return rows[0]

    @staticmethod
    def _row_to_attempt(row: sqlite3.Row) -> AttemptRecord:
        provenance = _freeze_json(json.loads(str(row["worker_provenance_json"])))
        result = None if row["result_json"] is None else _freeze_json(json.loads(str(row["result_json"])))
        run_uids = tuple(json.loads(str(row["run_uids_json"])))
        if not isinstance(provenance, Mapping) or (result is not None and not isinstance(result, Mapping)):
            raise StorageError("stored attempt JSON has an invalid shape")
        return AttemptRecord(
            attempt_uid=str(row["attempt_uid"]),
            operation_uid=str(row["operation_uid"]),
            queue_execution_uid=str(row["queue_execution_uid"]),
            worker_instance_uid=str(row["worker_instance_uid"]),
            worker_revision=str(row["worker_revision"]),
            worker_provenance=provenance,
            execute_message_uid=str(row["execute_message_uid"]),
            scheduler_authorization=str(row["scheduler_authorization"]),
            state=AttemptState(str(row["state"])),
            created_at=int(row["created_at"]),
            started_at=None if row["started_at"] is None else int(row["started_at"]),
            completed_at=None if row["completed_at"] is None else int(row["completed_at"]),
            stop_requested_at=None if row["stop_requested_at"] is None else int(row["stop_requested_at"]),
            stop_acknowledged_at=(
                None if row["stop_acknowledged_at"] is None else int(row["stop_acknowledged_at"])
            ),
            result=result,
            run_uids=run_uids,
            diagnostic=None if row["diagnostic"] is None else str(row["diagnostic"]),
            cleanup_completed=(None if row["cleanup_completed"] is None else bool(row["cleanup_completed"])),
        )

    async def run_idempotent_mutation(
        self,
        request: IdempotencyRequest,
        action: Callable[[aiosqlite.Connection], Awaitable[StoredHttpResponse]],
        *,
        now: int | None = None,
    ) -> IdempotencyResult:
        timestamp = self._timestamp(now)
        async with self.transaction() as connection:
            rows = await self._fetchall_on(
                connection,
                """
                SELECT request_hash, response_status, response_body_json, response_etag
                FROM idempotency_records
                WHERE principal=? AND method=? AND target=? AND idempotency_key=?
                """,
                (request.principal, request.method, request.target, request.key),
            )
            if rows:
                row = rows[0]
                if str(row["request_hash"]) != request.request_hash:
                    raise IdempotencyConflictError("idempotency key was already used for a different request")
                body = json.loads(str(row["response_body_json"]))
                if type(body) is not dict:
                    raise StorageError("stored idempotency response is not a JSON object")
                return IdempotencyResult(
                    response=StoredHttpResponse(
                        status=int(row["response_status"]),
                        body=body,
                        etag=None if row["response_etag"] is None else str(row["response_etag"]),
                    ),
                    replayed=True,
                )

            response = await action(connection)
            if type(response.status) is not int or not 200 <= response.status < 300:
                raise ValueError("only successful HTTP mutation responses may be persisted")
            response_body = require_json_object(response.body, label="idempotency response body")
            await connection.execute(
                """
                INSERT INTO idempotency_records(
                    principal, method, target, idempotency_key, request_hash,
                    response_status, response_body_json, response_etag, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.principal,
                    request.method,
                    request.target,
                    request.key,
                    request.request_hash,
                    response.status,
                    canonical_json(response_body),
                    response.etag,
                    timestamp,
                ),
            )
            return IdempotencyResult(response=response, replayed=False)

    async def _validate_revision_on(self, connection: aiosqlite.Connection, expected_revision: int) -> None:
        row = await self._fetchone_on(
            connection,
            "SELECT queue_revision FROM controller_metadata WHERE singleton=1",
        )
        current_revision = int(row["queue_revision"])
        if expected_revision != current_revision:
            raise RevisionConflictError(
                expected_revision=expected_revision,
                current_revision=current_revision,
            )

    async def _increment_revision_on(self, connection: aiosqlite.Connection) -> int:
        row = await self._fetchone_on(
            connection,
            "SELECT queue_revision FROM controller_metadata WHERE singleton=1",
        )
        current_revision = int(row["queue_revision"])
        if current_revision >= 9_223_372_036_854_775_807:
            raise StorageError("queue revision is exhausted")
        new_revision = current_revision + 1
        await connection.execute(
            "UPDATE controller_metadata SET queue_revision=? WHERE singleton=1",
            (new_revision,),
        )
        return new_revision

    async def _compact_queue_positions_on(self, connection: aiosqlite.Connection) -> None:
        rows = await self._fetchall_on(
            connection,
            "SELECT operation_uid FROM queue_entries ORDER BY position",
        )
        if not rows:
            return
        offset = len(rows) + int(
            (
                await self._fetchone_on(
                    connection,
                    "SELECT COALESCE(MAX(position), -1) AS maximum FROM queue_entries",
                )
            )["maximum"]
        )
        for index, row in enumerate(rows):
            await connection.execute(
                "UPDATE queue_entries SET position=? WHERE operation_uid=?",
                (offset + index + 1, row["operation_uid"]),
            )
        for index, row in enumerate(rows):
            await connection.execute(
                "UPDATE queue_entries SET position=? WHERE operation_uid=?",
                (index, row["operation_uid"]),
            )

    async def _operation_row_on(self, connection: aiosqlite.Connection, operation_uid: str) -> sqlite3.Row:
        rows = await self._fetchall_on(
            connection,
            """
            SELECT operations.*, queue_entries.position AS queue_position
            FROM operations
            LEFT JOIN queue_entries USING(operation_uid)
            WHERE operations.operation_uid=?
            """,
            (operation_uid,),
        )
        if not rows:
            raise RecordNotFoundError(f"operation {operation_uid} does not exist")
        return rows[0]

    async def _insert_event_on(
        self,
        connection: aiosqlite.Connection,
        *,
        timestamp: int,
        actor_kind: ActorKind,
        actor_id: str,
        event_type: str,
        queue_revision: int | None,
        payload: Mapping[str, object],
        operation_uid: str | None = None,
        queue_execution_uid: str | None = None,
        attempt_uid: str | None = None,
        worker_instance_uid: str | None = None,
    ) -> None:
        if event_type not in EVENT_TYPES:
            raise ValueError(f"unsupported controller event type {event_type!r}")
        if not actor_id.strip():
            raise ValueError("event actor_id must not be blank")
        await connection.execute(
            """
            INSERT INTO controller_events(
                timestamp, actor_kind, actor_id, event_type, operation_uid,
                queue_execution_uid, attempt_uid, worker_instance_uid, queue_revision, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                actor_kind.value,
                actor_id,
                event_type,
                operation_uid,
                queue_execution_uid,
                attempt_uid,
                worker_instance_uid,
                queue_revision,
                canonical_json(dict(payload)),
            ),
        )
        self._transaction_has_event = True

    @staticmethod
    def _validate_principal(principal: str) -> None:
        if not principal.strip():
            raise ValueError("principal must not be blank")

    def _timestamp(self, value: int | None) -> int:
        timestamp = self._clock() if value is None else value
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("timestamp must be a nonnegative integer UTC microsecond value")
        return timestamp

    @staticmethod
    def _row_to_operation(row: sqlite3.Row) -> OperationRecord:
        parameters = _freeze_json(json.loads(str(row["parameters_json"])))
        result = None if row["result_json"] is None else _freeze_json(json.loads(str(row["result_json"])))
        if not isinstance(parameters, Mapping) or (result is not None and not isinstance(result, Mapping)):
            raise StorageError("stored operation JSON is not an object")
        return OperationRecord(
            operation_uid=str(row["operation_uid"]),
            operation_id=str(row["operation_id"]),
            operation_version=str(row["operation_version"]),
            parameters=parameters,
            descriptor_fingerprint=str(row["descriptor_fingerprint"]),
            submitted_by=str(row["submitted_by"]),
            state=OperationState(str(row["state"])),
            submitted_at=int(row["submitted_at"]),
            updated_at=int(row["updated_at"]),
            queue_position=None if row["queue_position"] is None else int(row["queue_position"]),
            result=result,
            replaced_by=None if row["replaced_by"] is None else str(row["replaced_by"]),
        )

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        current = self._transaction_connection.get()
        if current is not None:
            yield current
            return

        notify_events = False
        async with self._transaction_lock:
            connection = self._require_connection()
            token = self._transaction_connection.set(connection)
            self._transaction_has_event = False
            await connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                await connection.rollback()
                self._transaction_has_event = False
                raise
            else:
                await connection.commit()
                notify_events = self._transaction_has_event
                self._transaction_has_event = False
            finally:
                self._transaction_connection.reset(token)
        if notify_events:
            async with self._event_condition:
                self._event_condition.notify_all()

    def _require_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("SQLite store is not open")
        return self._connection

    @staticmethod
    async def _fetchone_on(connection: aiosqlite.Connection, sql: str, parameters: tuple[object, ...] = ()):
        cursor = await connection.execute(sql, parameters)
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        if row is None:
            raise StorageError(f"SQLite query returned no row: {sql}")
        return row

    @staticmethod
    async def _fetchall_on(
        connection: aiosqlite.Connection, sql: str, parameters: tuple[object, ...] = ()
    ) -> list[sqlite3.Row]:
        cursor = await connection.execute(sql, parameters)
        try:
            return await cursor.fetchall()
        finally:
            await cursor.close()
