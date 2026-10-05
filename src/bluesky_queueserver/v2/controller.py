"""QueueServer V2 controller service and scheduler."""

from __future__ import annotations

import asyncio
import errno
import fcntl
import json
import os
import re
import stat
from collections.abc import Awaitable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import uuid4

import aiosqlite

from .._version import __version__
from .contracts import (
    AttemptState,
    AttemptView,
    AuthorizationScope,
    CatalogView,
    ControlLeaseView,
    HealthView,
    OperationDescriptor,
    OperationSubmission,
    OperationView,
    QueueExecutionPolicy,
    QueueExecutionView,
    QueueReorder,
    QueueSnapshot,
    ReadinessView,
    RecoveryAcknowledgement,
    WorkerCatalog,
    canonical_json,
    descriptor_fingerprint,
    scope_allows,
    validate_operation_request,
    validate_operation_result,
)
from .storage import (
    AttemptRecord,
    DispatchRecord,
    IdempotencyRequest,
    IdempotencyResult,
    LeaseExpiredError,
    OperationRecord,
    QueueExecutionRecord,
    QueueRecord,
    SQLiteStore,
    StateConflictError,
    StoredHttpResponse,
    thaw_json,
)


class AuthorizationError(RuntimeError):
    """The authenticated principal lacks a required QueueServer scope."""


class MissingPreconditionError(RuntimeError):
    """A required If-Match queue revision is absent."""


class InvalidPreconditionError(RuntimeError):
    """An If-Match value is not a QueueServer V2 revision ETag."""


class ControllerAuthorityError(RuntimeError):
    """The process cannot safely hold controller authority."""


class ControllerAlreadyRunningError(ControllerAuthorityError):
    """Another process already holds the controller authority lock."""


def queue_etag(revision: int) -> str:
    return f'"qrev-{revision}"'


def parse_queue_etag(value: str | None) -> int:
    if value is None:
        raise MissingPreconditionError("If-Match is required")
    match = re.fullmatch(r'"qrev-(0|[1-9][0-9]*)"', value)
    if match is None:
        raise InvalidPreconditionError("If-Match must be a quoted qrev revision")
    revision = int(match.group(1))
    if revision > 9_223_372_036_854_775_807:
        raise InvalidPreconditionError("If-Match queue revision is out of range")
    return revision


class AuthorityFileLock:
    """Process-lifetime exclusive authority represented by a hardened flock."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        if self._fd is not None:
            raise RuntimeError(f"authority lock is already held: {self.path}")
        parent_stat = self.path.parent.stat()
        self._validate_owner_and_mode(parent_stat, label="authority lock parent")
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise ControllerAuthorityError(f"cannot safely open authority lock {self.path}: {exc}") from exc
        try:
            lock_stat = os.fstat(fd)
            if not stat.S_ISREG(lock_stat.st_mode):
                raise ControllerAuthorityError(f"authority lock is not a regular file: {self.path}")
            self._validate_owner_and_mode(lock_stat, label="authority lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise ControllerAlreadyRunningError(
                        f"controller authority is already held: {self.path}"
                    ) from exc
                raise ControllerAuthorityError(f"cannot lock controller authority {self.path}: {exc}") from exc
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
            os.fsync(fd)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    @staticmethod
    def _validate_owner_and_mode(metadata: os.stat_result, *, label: str) -> None:
        if metadata.st_uid != os.geteuid():
            raise ControllerAuthorityError(f"{label} is not owned by the service user")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ControllerAuthorityError(f"{label} is group- or world-writable")


def probe_worker_authority(path: str | Path, *, observed_at: int) -> dict[str, object] | None:
    probe = AuthorityFileLock(path)
    try:
        probe.acquire()
    except ControllerAlreadyRunningError:
        return None
    try:
        return {
            "method": "exclusive-flock",
            "lock_path": str(Path(path).resolve(strict=False)),
            "observed_at": observed_at,
        }
    finally:
        probe.release()


@dataclass(frozen=True)
class WorkerCompletion:
    state: AttemptState
    result: Mapping[str, object] | None
    run_uids: tuple[str, ...]
    diagnostic: str | None
    cleanup_completed: bool
    stop_acknowledged: bool = False


class WorkerGateway(Protocol):
    worker_instance_uid: str
    catalog: WorkerCatalog
    lock_path: Path
    pid: int | None

    async def start(self) -> None: ...

    async def start_execution(
        self,
        *,
        attempt: AttemptRecord,
        descriptor: OperationDescriptor,
        parameters: dict[str, object],
    ) -> Awaitable[WorkerCompletion]: ...

    async def request_safe_stop(self, *, attempt_uid: str) -> None: ...

    async def close(self) -> int | None: ...

    async def disconnect(self) -> int | None: ...


class ControllerService:
    """Own the V2 authority and one event-driven scheduler task."""

    def __init__(
        self,
        store: SQLiteStore,
        *,
        worker: WorkerGateway | None = None,
        protocol_timeout_seconds: float = 10.0,
    ):
        if protocol_timeout_seconds <= 0:
            raise ValueError("protocol_timeout_seconds must be positive")
        self.store = store
        self.worker = worker
        self._protocol_timeout_seconds = protocol_timeout_seconds
        self._authority_lock = AuthorityFileLock(store.controller_lock_path)
        self._scheduler_wakeup = asyncio.Event()
        self._scheduler_idle = asyncio.Event()
        self._scheduler_idle.set()
        self._scheduler_task: asyncio.Task[None] | None = None
        self._scheduler_error: Exception | None = None
        self._attempt_locks: dict[str, asyncio.Lock] = {}
        self._closing = False
        self._worker_started = False
        self._dispatch_ready = False
        self._shutdown_fence_event = asyncio.Event()
        self._shutdown_fence_evidence: Mapping[str, object] | None = None

    @property
    def holds_authority(self) -> bool:
        return self._authority_lock.held

    @property
    def scheduler_running(self) -> bool:
        return self._scheduler_task is not None and not self._scheduler_task.done()

    @property
    def scheduler_error(self) -> Exception | None:
        return self._scheduler_error

    @property
    def dispatch_ready(self) -> bool:
        return self._dispatch_ready

    async def open(self) -> None:
        if self._scheduler_task is not None:
            raise RuntimeError("controller service is already open")
        self._authority_lock.acquire()
        try:
            await self.store.open()
            recovery_block = await self.store.recover_startup()
            if recovery_block is not None and recovery_block.fenced_at is None:
                evidence = probe_worker_authority(
                    self.store.worker_lock_path,
                    observed_at=self.store.now_micros(),
                )
                if evidence is not None:
                    recovery_block = await self.store.record_worker_fenced(evidence=evidence)
            if recovery_block is None:
                active_worker = await self.store.get_active_worker()
                can_launch = True
                if active_worker is not None:
                    evidence = probe_worker_authority(
                        self.store.worker_lock_path,
                        observed_at=self.store.now_micros(),
                    )
                    can_launch = evidence is not None
                    if evidence is not None:
                        await self.store.record_worker_fenced(evidence=evidence)
                if can_launch:
                    await self._start_worker()
            self._closing = False
            self._scheduler_error = None
            self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="queueserver-v2-scheduler")
            self._scheduler_wakeup.set()
        except BaseException:
            if self.worker is not None and self._worker_started:
                await self.worker.close()
                self._worker_started = False
            await self.store.close()
            self._authority_lock.release()
            raise

    async def close(self) -> None:
        if not self.holds_authority:
            return
        self._closing = True
        self._dispatch_ready = False
        active_attempt = await self.store.get_active_attempt()
        if active_attempt is not None:
            lock = self._attempt_locks.setdefault(active_attempt.attempt_uid, asyncio.Lock())
            async with lock:
                current = await self.store.get_attempt(active_attempt.attempt_uid)
                if current.state in {AttemptState.CLAIMED, AttemptState.RUNNING}:
                    await self.store.mark_controller_shutdown_requested(attempt_uid=current.attempt_uid)
                    active_attempt = current
                else:
                    active_attempt = None

        exit_code = None
        if self.worker is not None and self._worker_started:
            exit_code = await self.worker.close()
            self._worker_started = False
            active_worker = await self.store.get_active_worker()
            if active_worker is not None:
                await self.store.mark_worker_exited(
                    worker_instance_uid=active_worker.worker_instance_uid,
                    exit_code=exit_code,
                    faulted=False,
                )

        if active_attempt is not None:
            evidence = probe_worker_authority(
                self.store.worker_lock_path,
                observed_at=self.store.now_micros(),
            )
            if evidence is not None:
                self._shutdown_fence_evidence = evidence
                self._shutdown_fence_event.set()
                current = await self.store.get_active_attempt()
                if current is not None and current.state is AttemptState.CLAIMED:
                    await self.store.complete_attempt(
                        attempt_uid=current.attempt_uid,
                        state=AttemptState.INTERRUPTED,
                        result=None,
                        run_uids=current.run_uids,
                        diagnostic="controller shutdown before worker acceptance",
                        cleanup_completed=True,
                        fence_evidence=evidence,
                    )

        self._scheduler_wakeup.set()
        if self._scheduler_task is not None:
            if active_attempt is not None and self._shutdown_fence_evidence is None:
                self._scheduler_task.cancel()
            await asyncio.gather(self._scheduler_task, return_exceptions=True)
            self._scheduler_task = None
        await self.store.close()
        self._authority_lock.release()

    async def _start_worker(self) -> None:
        if self.worker is None or self._worker_started:
            self._dispatch_ready = self._worker_started
            return
        await self.worker.start()
        await self.store.register_ready_worker(
            worker_instance_uid=self.worker.worker_instance_uid,
            catalog=self.worker.catalog,
            lock_path=self.worker.lock_path,
            pid=self.worker.pid,
        )
        self._worker_started = True
        self._dispatch_ready = True

    def wake_scheduler(self) -> None:
        self._require_open()
        self._scheduler_wakeup.set()

    async def wait_scheduler_idle(self) -> None:
        await self._scheduler_idle.wait()
        if self._scheduler_error is not None:
            raise self._scheduler_error

    def health(self) -> HealthView:
        if self.worker is None or not self._worker_started:
            return HealthView(package_version=__version__)
        provenance = self.worker.catalog.worker_provenance
        environment_lock = provenance.get("environment_lock_sha256")
        source_hashes = {
            "startup_source_sha256": provenance.get("startup_source_sha256"),
            "adapter_source_sha256": provenance.get("adapter_source_sha256"),
        }
        return HealthView(
            package_version=__version__,
            worker_revision=self.worker.catalog.worker_revision,
            environment_lock_sha256=(environment_lock if isinstance(environment_lock, str) else None),
            source_sha256=source_hashes,
        )

    async def readiness(self) -> ReadinessView:
        block = await self.store.get_active_dispatch_block() if self.store.is_open else None
        fencing_clear = block is None or not block.requires_fence or block.fenced_at is not None
        return ReadinessView(
            ready=self.holds_authority and self.store.is_open and self._dispatch_ready and fencing_clear,
            controller_lock="held" if self.holds_authority else "unavailable",
            storage="ready" if self.store.is_open else "unavailable",
            worker="ready" if self._worker_started else "not_ready",
            fencing="clear" if fencing_clear else "blocked",
            dispatch="ready" if self._dispatch_ready and block is None else "blocked",
        )

    def catalog_view(self) -> CatalogView:
        self._require_open()
        if self.worker is None or not self._worker_started:
            raise RuntimeError("worker catalog is unavailable")
        catalog = self.worker.catalog
        return CatalogView(
            protocol_version=catalog.protocol_version,
            worker_revision=catalog.worker_revision,
            worker_provenance=catalog.worker_provenance,
            operations=catalog.operations,
        )

    async def control_lease(self) -> ControlLeaseView:
        self._require_open()
        lease = await self.store.get_control_lease()
        if lease is None:
            return ControlLeaseView(holder=None, expires_at=None)
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def queue(self) -> QueueRecord:
        self._require_open()
        return await self.store.queue_snapshot()

    async def queue_view(self) -> QueueSnapshot:
        snapshot = await self.queue()
        operations = [await self._operation_view(record) for record in snapshot.operations]
        return QueueSnapshot(
            revision=snapshot.revision,
            operations=operations,
            active_execution_uid=snapshot.active_execution_uid,
            dispatch_block_uid=snapshot.dispatch_block_uid,
        )

    async def _queue_view_on(
        self,
        connection: aiosqlite.Connection,
        snapshot: QueueRecord | None = None,
    ) -> QueueSnapshot:
        snapshot = snapshot or await self.store._queue_snapshot_on(connection)
        operations = [await self._operation_view_on(connection, record) for record in snapshot.operations]
        return QueueSnapshot(
            revision=snapshot.revision,
            operations=operations,
            active_execution_uid=snapshot.active_execution_uid,
            dispatch_block_uid=snapshot.dispatch_block_uid,
        )

    async def operation_view(self, operation_uid: str) -> OperationView:
        return await self._operation_view(await self.store.get_operation(operation_uid))

    async def _operation_view_on(
        self,
        connection: aiosqlite.Connection,
        record: OperationRecord,
    ) -> OperationView:
        attempt = await self.store._latest_attempt_for_operation_on(connection, record.operation_uid)
        return self._operation_view_from_records(record, attempt)

    async def queue_execution_view(self, queue_execution_uid: str) -> QueueExecutionView:
        record = await self.store.get_queue_execution(queue_execution_uid)
        active_attempt = await self.store.get_active_attempt()
        block = await self.store.get_active_dispatch_block()
        return QueueExecutionView(
            queue_execution_uid=record.queue_execution_uid,
            state=record.state,
            policy=record.policy,
            initiated_by=record.initiated_by,
            starting_revision=record.starting_revision,
            admitted_operation_uids=list(record.admitted_operation_uids),
            active_attempt_uid=(
                active_attempt.attempt_uid
                if active_attempt is not None and active_attempt.queue_execution_uid == queue_execution_uid
                else None
            ),
            dispatch_block_uid=(
                block.dispatch_block_uid
                if block is not None and block.queue_execution_uid == queue_execution_uid
                else None
            ),
            created_at=record.created_at,
            updated_at=record.updated_at,
            stop_requested_at=record.stop_requested_at,
            completed_at=record.completed_at,
        )

    async def _queue_execution_view_on(
        self,
        connection: aiosqlite.Connection,
        queue_execution_uid: str,
    ) -> QueueExecutionView:
        record = await self.store._queue_execution_record_on(connection, queue_execution_uid)
        active_attempt = await self.store._active_attempt_on(connection)
        block = await self.store._active_dispatch_block_on(connection)
        return QueueExecutionView(
            queue_execution_uid=record.queue_execution_uid,
            state=record.state,
            policy=record.policy,
            initiated_by=record.initiated_by,
            starting_revision=record.starting_revision,
            admitted_operation_uids=list(record.admitted_operation_uids),
            active_attempt_uid=(
                active_attempt.attempt_uid
                if active_attempt is not None and active_attempt.queue_execution_uid == queue_execution_uid
                else None
            ),
            dispatch_block_uid=(
                block.dispatch_block_uid
                if block is not None and block.queue_execution_uid == queue_execution_uid
                else None
            ),
            created_at=record.created_at,
            updated_at=record.updated_at,
            stop_requested_at=record.stop_requested_at,
            completed_at=record.completed_at,
        )

    async def attempt_view(self, attempt_uid: str) -> AttemptView:
        return self._attempt_view(await self.store.get_attempt(attempt_uid))

    async def _attempt_view_on(
        self,
        connection: aiosqlite.Connection,
        attempt_uid: str,
    ) -> AttemptView:
        return self._attempt_view(await self.store._attempt_record_on(connection, attempt_uid))

    async def events(self, *, after: int = 0, limit: int = 100):
        return await self.store.list_events(after=after, limit=limit)

    async def wait_for_events(self, *, after: int, limit: int = 100, timeout: float | None = None):
        return await self.store.wait_for_events(after=after, limit=limit, timeout=timeout)

    async def acquire_control_lease(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        ttl_seconds: int = 300,
    ) -> ControlLeaseView:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        lease = await self.store.acquire_control_lease(principal=principal, ttl_seconds=ttl_seconds)
        self._scheduler_wakeup.set()
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def _acquire_control_lease_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        ttl_seconds: int,
        timestamp: int,
    ) -> ControlLeaseView:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        lease = await self.store._acquire_control_lease_on(
            connection,
            principal=principal,
            ttl_seconds=ttl_seconds,
            timestamp=timestamp,
            lease_uid=str(uuid4()),
        )
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def renew_control_lease(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        ttl_seconds: int = 300,
    ) -> ControlLeaseView:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        lease = await self.store.renew_control_lease(principal=principal, ttl_seconds=ttl_seconds)
        self._scheduler_wakeup.set()
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def _renew_control_lease_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        ttl_seconds: int,
        timestamp: int,
    ) -> ControlLeaseView:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        lease, expired = await self.store._renew_control_lease_on(
            connection,
            principal=principal,
            ttl_seconds=ttl_seconds,
            timestamp=timestamp,
        )
        if expired:
            raise LeaseExpiredError("control lease has expired")
        assert lease is not None
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def release_control_lease(self, *, principal: str, scopes: Iterable[str]) -> None:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        await self.store.release_control_lease(principal=principal)
        self._scheduler_wakeup.set()

    async def _release_control_lease_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        timestamp: int,
    ) -> None:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        expired = await self.store._release_control_lease_on(
            connection,
            principal=principal,
            timestamp=timestamp,
        )
        if expired:
            raise LeaseExpiredError("control lease has expired")

    async def override_control_lease(
        self,
        *,
        administrator: str,
        scopes: Iterable[str],
        reason: str,
        ttl_seconds: int = 300,
    ) -> ControlLeaseView:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.ADMIN)
        lease = await self.store.override_control_lease(
            administrator=administrator,
            reason=reason,
            ttl_seconds=ttl_seconds,
        )
        self._scheduler_wakeup.set()
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def _override_control_lease_on(
        self,
        connection: aiosqlite.Connection,
        *,
        administrator: str,
        scopes: Iterable[str],
        reason: str,
        ttl_seconds: int,
        timestamp: int,
    ) -> ControlLeaseView:
        self._require_open()
        self._require_scope(scopes, AuthorizationScope.ADMIN)
        lease = await self.store._override_control_lease_on(
            connection,
            administrator=administrator,
            reason=reason,
            ttl_seconds=ttl_seconds,
            timestamp=timestamp,
            lease_uid=str(uuid4()),
        )
        return ControlLeaseView(holder=lease.subject, expires_at=lease.expires_at)

    async def check_lease_expiry(self, *, now: int | None = None) -> bool:
        self._require_open()
        expired = await self.store.expire_control_lease(now=now)
        if expired:
            self._scheduler_wakeup.set()
        return expired

    async def submit_operation(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        submission: OperationSubmission,
    ) -> tuple[OperationRecord, int]:
        expected_revision, descriptor, validated = self._prepare_operation_mutation(
            scopes=scopes,
            if_match=if_match,
            submission=submission,
        )
        result = await self.store.submit_operation(
            principal=principal,
            expected_revision=expected_revision,
            descriptor=descriptor,
            operation_id=submission.operation_id,
            operation_version=submission.operation_version,
            parameters=validated,
        )
        self._scheduler_wakeup.set()
        return result

    async def _submit_operation_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        submission: OperationSubmission,
        timestamp: int,
    ) -> tuple[OperationRecord, int]:
        expected_revision, descriptor, validated = self._prepare_operation_mutation(
            scopes=scopes,
            if_match=if_match,
            submission=submission,
        )
        return await self.store._submit_operation_on(
            connection,
            principal=principal,
            expected_revision=expected_revision,
            descriptor=descriptor,
            parameters_json=canonical_json(validated),
            fingerprint=descriptor_fingerprint(descriptor),
            operation_uid=str(uuid4()),
            timestamp=timestamp,
        )

    async def cancel_operation(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        operation_uid: str,
    ) -> tuple[OperationRecord, int]:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        result = await self.store.cancel_operation(
            principal=principal,
            expected_revision=expected_revision,
            operation_uid=operation_uid,
        )
        self._scheduler_wakeup.set()
        return result

    async def _cancel_operation_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        operation_uid: str,
        timestamp: int,
    ) -> tuple[OperationRecord, int]:
        return await self.store._cancel_operation_on(
            connection,
            principal=principal,
            expected_revision=self._prepare_control_mutation(scopes=scopes, if_match=if_match),
            operation_uid=operation_uid,
            timestamp=timestamp,
        )

    async def replace_operation(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        operation_uid: str,
        submission: OperationSubmission,
    ) -> tuple[OperationRecord, int]:
        expected_revision, descriptor, validated = self._prepare_operation_mutation(
            scopes=scopes,
            if_match=if_match,
            submission=submission,
        )
        result = await self.store.replace_operation(
            principal=principal,
            expected_revision=expected_revision,
            operation_uid=operation_uid,
            descriptor=descriptor,
            operation_id=submission.operation_id,
            operation_version=submission.operation_version,
            parameters=validated,
        )
        self._scheduler_wakeup.set()
        return result

    async def _replace_operation_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        operation_uid: str,
        submission: OperationSubmission,
        timestamp: int,
    ) -> tuple[OperationRecord, int]:
        expected_revision, descriptor, validated = self._prepare_operation_mutation(
            scopes=scopes,
            if_match=if_match,
            submission=submission,
        )
        return await self.store._replace_operation_on(
            connection,
            principal=principal,
            expected_revision=expected_revision,
            operation_uid=operation_uid,
            descriptor=descriptor,
            parameters_json=canonical_json(validated),
            fingerprint=descriptor_fingerprint(descriptor),
            replacement_uid=str(uuid4()),
            timestamp=timestamp,
        )

    async def reorder_queue(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        reorder: QueueReorder,
    ) -> QueueRecord:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        result = await self.store.reorder_queue(
            principal=principal,
            expected_revision=expected_revision,
            operation_uids=reorder.operation_uids,
        )
        self._scheduler_wakeup.set()
        return result

    async def _reorder_queue_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        reorder: QueueReorder,
        timestamp: int,
    ) -> QueueRecord:
        return await self.store._reorder_queue_on(
            connection,
            principal=principal,
            expected_revision=self._prepare_control_mutation(scopes=scopes, if_match=if_match),
            operation_uids=reorder.operation_uids,
            timestamp=timestamp,
        )

    async def start_queue_execution(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        policy: QueueExecutionPolicy = QueueExecutionPolicy.STOP_ON_NON_SUCCESS,
    ) -> tuple[QueueExecutionRecord, int]:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        if not self._dispatch_ready:
            raise RuntimeError("dispatch is not ready")
        result = await self.store.start_queue_execution(
            principal=principal,
            expected_revision=expected_revision,
            policy=policy,
        )
        self._scheduler_wakeup.set()
        return result

    async def _start_queue_execution_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        policy: QueueExecutionPolicy,
        timestamp: int,
    ) -> tuple[QueueExecutionRecord, int]:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        if not self._dispatch_ready:
            raise RuntimeError("dispatch is not ready")
        return await self.store._start_queue_execution_on(
            connection,
            principal=principal,
            expected_revision=expected_revision,
            policy=policy,
            timestamp=timestamp,
            queue_execution_uid=str(uuid4()),
        )

    async def stop_queue_execution(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        queue_execution_uid: str,
    ) -> tuple[QueueExecutionRecord, int]:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        result = await self.store.stop_queue_execution(
            principal=principal,
            expected_revision=expected_revision,
            queue_execution_uid=queue_execution_uid,
        )
        self._scheduler_wakeup.set()
        return result

    async def _stop_queue_execution_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        queue_execution_uid: str,
        timestamp: int,
    ) -> tuple[QueueExecutionRecord, int]:
        return await self.store._stop_queue_execution_on(
            connection,
            principal=principal,
            expected_revision=self._prepare_control_mutation(scopes=scopes, if_match=if_match),
            queue_execution_uid=queue_execution_uid,
            timestamp=timestamp,
        )

    async def safe_stop_attempt(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        attempt_uid: str,
    ) -> tuple[AttemptRecord, int]:
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        lock = self._attempt_locks.setdefault(attempt_uid, asyncio.Lock())
        async with lock:
            stop = await self.store.request_attempt_stop(
                principal=principal,
                expected_revision=parse_queue_etag(if_match),
                attempt_uid=attempt_uid,
            )
            if not stop.contact_worker:
                self._attempt_locks.pop(attempt_uid, None)
                self._scheduler_wakeup.set()
                return stop.attempt, stop.queue_revision
            if self.worker is None:
                completed = await self.store.complete_attempt(
                    attempt_uid=attempt_uid,
                    state=AttemptState.UNKNOWN,
                    result=None,
                    run_uids=stop.attempt.run_uids,
                    diagnostic="worker is unavailable during safe stop",
                    cleanup_completed=False,
                )
                await self._fence_worker_after_ambiguous_outcome()
                return completed, await self.store.current_revision()
            try:
                await asyncio.wait_for(
                    self.worker.request_safe_stop(attempt_uid=attempt_uid),
                    timeout=self._protocol_timeout_seconds,
                )
            except Exception as exc:
                completed = await self.store.complete_attempt(
                    attempt_uid=attempt_uid,
                    state=AttemptState.UNKNOWN,
                    result=None,
                    run_uids=stop.attempt.run_uids,
                    diagnostic=f"safe-stop protocol failed: {exc}",
                    cleanup_completed=False,
                )
                await self._fence_worker_after_ambiguous_outcome()
                return completed, await self.store.current_revision()
            acknowledged, revision = await self.store.acknowledge_attempt_stop(attempt_uid=attempt_uid)
            return acknowledged, revision

    async def run_idempotent_safe_stop(
        self,
        request: IdempotencyRequest,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        attempt_uid: str,
        timestamp: int,
    ) -> IdempotencyResult:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        await self.store.expire_control_lease(now=timestamp)
        lock = self._attempt_locks.setdefault(attempt_uid, asyncio.Lock())
        terminal = False

        async with lock:

            async def action(connection: aiosqlite.Connection) -> StoredHttpResponse:
                nonlocal terminal
                stop = await self.store._request_attempt_stop_on(
                    connection,
                    principal=principal,
                    expected_revision=expected_revision,
                    attempt_uid=attempt_uid,
                    timestamp=timestamp,
                )
                revision = stop.queue_revision
                if not stop.contact_worker:
                    terminal = True
                elif self.worker is None:
                    await self.store._complete_attempt_on(
                        connection,
                        attempt_uid=attempt_uid,
                        state=AttemptState.UNKNOWN,
                        result=None,
                        run_uids=stop.attempt.run_uids,
                        diagnostic="worker is unavailable during safe stop",
                        cleanup_completed=False,
                        fence_evidence=None,
                        timestamp=self.store.now_micros(),
                    )
                    await self._fence_worker_after_ambiguous_outcome_on(connection)
                    revision = await self.store._current_revision_on(connection)
                    terminal = True
                else:
                    try:
                        await asyncio.wait_for(
                            self.worker.request_safe_stop(attempt_uid=attempt_uid),
                            timeout=self._protocol_timeout_seconds,
                        )
                    except Exception as exc:
                        await self.store._complete_attempt_on(
                            connection,
                            attempt_uid=attempt_uid,
                            state=AttemptState.UNKNOWN,
                            result=None,
                            run_uids=stop.attempt.run_uids,
                            diagnostic=f"safe-stop protocol failed: {exc}",
                            cleanup_completed=False,
                            fence_evidence=None,
                            timestamp=self.store.now_micros(),
                        )
                        await self._fence_worker_after_ambiguous_outcome_on(connection)
                        revision = await self.store._current_revision_on(connection)
                        terminal = True
                    else:
                        _, revision = await self.store._acknowledge_attempt_stop_on(
                            connection,
                            attempt_uid=attempt_uid,
                            timestamp=self.store.now_micros(),
                        )
                view = await self._attempt_view_on(connection, attempt_uid)
                return StoredHttpResponse(
                    status=200,
                    body=json.loads(view.model_dump_json()),
                    etag=queue_etag(revision),
                )

            result = await self.store.run_idempotent_mutation(request, action, now=timestamp)

        if result.replayed or terminal:
            self._attempt_locks.pop(attempt_uid, None)
        if not result.replayed:
            self._scheduler_wakeup.set()
        return result

    async def acknowledge_recovery(
        self,
        *,
        principal: str,
        scopes: Iterable[str],
        if_match: str | None,
        acknowledgement: RecoveryAcknowledgement,
    ) -> int:
        expected_revision, fence_evidence = await self._prepare_recovery_acknowledgement(
            scopes=scopes,
            if_match=if_match,
        )
        revision = await self.store.acknowledge_recovery(
            principal=principal,
            expected_revision=expected_revision,
            note=acknowledgement.note,
            fence_evidence=fence_evidence,
        )
        await self._finish_recovery_acknowledgement()
        return revision

    async def _prepare_recovery_acknowledgement(
        self,
        *,
        scopes: Iterable[str],
        if_match: str | None,
    ) -> tuple[int, Mapping[str, object] | None]:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        block = await self.store.get_active_dispatch_block()
        active_worker = await self.store.get_active_worker()
        fence_evidence = None
        if block is not None and block.fenced_at is None and (block.requires_fence or active_worker is not None):
            fence_evidence = probe_worker_authority(
                self.store.worker_lock_path,
                observed_at=self.store.now_micros(),
            )
            if fence_evidence is None:
                raise StateConflictError("prior worker authority has not ended")
        return expected_revision, fence_evidence

    async def _acknowledge_recovery_on(
        self,
        connection: aiosqlite.Connection,
        *,
        principal: str,
        acknowledgement: RecoveryAcknowledgement,
        expected_revision: int,
        fence_evidence: Mapping[str, object] | None,
        timestamp: int,
    ) -> int:
        return await self.store._acknowledge_recovery_on(
            connection,
            principal=principal,
            expected_revision=expected_revision,
            note=acknowledgement.note,
            fence_evidence=fence_evidence,
            timestamp=timestamp,
        )

    async def _finish_recovery_acknowledgement(self) -> None:
        if not self._worker_started:
            await self._start_worker()
        else:
            self._dispatch_ready = True
        self._scheduler_wakeup.set()

    def _prepare_control_mutation(self, *, scopes: Iterable[str], if_match: str | None) -> int:
        self._require_scope(scopes, AuthorizationScope.CONTROL)
        return parse_queue_etag(if_match)

    def _prepare_operation_mutation(
        self,
        *,
        scopes: Iterable[str],
        if_match: str | None,
        submission: OperationSubmission,
    ) -> tuple[int, OperationDescriptor, dict[str, object]]:
        expected_revision = self._prepare_control_mutation(scopes=scopes, if_match=if_match)
        descriptor = self._lookup_descriptor(submission.operation_id, submission.operation_version)
        validated = validate_operation_request(
            descriptor,
            operation_id=submission.operation_id,
            operation_version=submission.operation_version,
            parameters=submission.parameters,
        )
        return expected_revision, descriptor, validated

    def _lookup_descriptor(self, operation_id: str, operation_version: str) -> OperationDescriptor:
        self._require_open()
        if self.worker is None or not self._worker_started:
            raise RuntimeError("worker catalog is unavailable")
        for descriptor in self.worker.catalog.operations:
            if descriptor.operation_id == operation_id and descriptor.operation_version == operation_version:
                return descriptor
        raise ValueError(f"operation {operation_id!r} version {operation_version!r} is not in the worker catalog")

    def _require_open(self) -> None:
        if not self.holds_authority or self._scheduler_task is None:
            raise RuntimeError("controller service is not open")

    @staticmethod
    def _require_scope(scopes: Iterable[str], required: AuthorizationScope) -> None:
        if not scope_allows(scopes, required):
            raise AuthorizationError(f"scope {required.value!r} is required")

    async def _scheduler_loop(self) -> None:
        while True:
            self._scheduler_wakeup.clear()
            if self._closing:
                return
            await self.store.expire_control_lease()
            self._scheduler_idle.clear()
            try:
                while await self._dispatch_once():
                    if self._closing:
                        return
            except Exception as exc:
                self._scheduler_error = exc
                self._scheduler_idle.set()
                return
            self._scheduler_idle.set()
            if self._scheduler_wakeup.is_set():
                continue
            lease = await self.store.get_control_lease()
            timeout = None
            if lease is not None:
                timeout = max(0.0, (lease.expires_at - self.store.now_micros()) / 1_000_000)
            try:
                await asyncio.wait_for(self._scheduler_wakeup.wait(), timeout=timeout)
            except TimeoutError:
                continue

    async def _dispatch_once(self) -> bool:
        worker = self.worker
        if worker is None or not self._dispatch_ready:
            return False
        dispatch = await self.store.claim_next_attempt(worker_instance_uid=worker.worker_instance_uid)
        if dispatch is None:
            return False
        attempt_uid = dispatch.attempt.attempt_uid
        lock = self._attempt_locks.setdefault(attempt_uid, asyncio.Lock())
        descriptor = self._descriptor_for(dispatch)
        parameters = thaw_json(dispatch.operation.parameters)
        if type(parameters) is not dict:
            raise RuntimeError("stored operation parameters are not a JSON object")
        validate_operation_request(
            descriptor,
            operation_id=dispatch.operation.operation_id,
            operation_version=dispatch.operation.operation_version,
            parameters=parameters,
        )
        completion_waiter: Awaitable[WorkerCompletion] | None = None
        async with lock:
            current = await self.store.get_attempt(attempt_uid)
            if self._closing:
                self._attempt_locks.pop(attempt_uid, None)
                return False
            if current.state is not AttemptState.CLAIMED:
                self._attempt_locks.pop(attempt_uid, None)
                return False
            try:
                completion_waiter = await asyncio.wait_for(
                    worker.start_execution(
                        attempt=dispatch.attempt,
                        descriptor=descriptor,
                        parameters=parameters,
                    ),
                    timeout=self._protocol_timeout_seconds,
                )
                await self.store.mark_attempt_running(attempt_uid=attempt_uid)
            except Exception as exc:
                await self.store.complete_attempt(
                    attempt_uid=attempt_uid,
                    state=AttemptState.UNKNOWN,
                    result=None,
                    run_uids=(),
                    diagnostic=f"execute acceptance failed: {exc}",
                    cleanup_completed=False,
                )
                self._attempt_locks.pop(attempt_uid, None)
                self._dispatch_ready = False
                await self._fence_worker_after_ambiguous_outcome()
                return False
        assert completion_waiter is not None
        try:
            completion = await completion_waiter
            completion = self._validated_completion(descriptor, completion)
        except Exception as exc:
            completion = WorkerCompletion(
                state=AttemptState.UNKNOWN,
                result=None,
                run_uids=(),
                diagnostic=str(exc),
                cleanup_completed=False,
            )
        fence_evidence = None
        if completion.state is AttemptState.INTERRUPTED and self._closing:
            await self._shutdown_fence_event.wait()
            fence_evidence = self._shutdown_fence_evidence
        async with lock:
            current = await self.store.get_attempt(attempt_uid)
            if current.state not in {AttemptState.CLAIMED, AttemptState.RUNNING}:
                self._attempt_locks.pop(attempt_uid, None)
                return False
            await self.store.complete_attempt(
                attempt_uid=attempt_uid,
                state=completion.state,
                result=completion.result,
                run_uids=completion.run_uids,
                diagnostic=completion.diagnostic,
                cleanup_completed=completion.cleanup_completed,
                fence_evidence=fence_evidence,
            )
        if completion.state in {AttemptState.FAILED, AttemptState.INTERRUPTED, AttemptState.UNKNOWN}:
            self._dispatch_ready = False
        if completion.state in {AttemptState.INTERRUPTED, AttemptState.UNKNOWN} and not self._closing:
            await self._fence_worker_after_ambiguous_outcome()
        self._attempt_locks.pop(attempt_uid, None)
        return completion.state is AttemptState.SUCCEEDED and not self._closing

    async def _fence_worker_after_ambiguous_outcome(self) -> None:
        worker = self.worker
        if worker is None:
            return
        if self._worker_started:
            try:
                await worker.disconnect()
            except Exception:
                return
            else:
                self._worker_started = False
        evidence = probe_worker_authority(
            self.store.worker_lock_path,
            observed_at=self.store.now_micros(),
        )
        if evidence is not None:
            await self.store.record_worker_fenced(evidence=evidence)

    async def _fence_worker_after_ambiguous_outcome_on(
        self,
        connection: aiosqlite.Connection,
    ) -> None:
        worker = self.worker
        if worker is None:
            return
        if self._worker_started:
            try:
                await worker.disconnect()
            except Exception:
                return
            else:
                self._worker_started = False
        evidence = probe_worker_authority(
            self.store.worker_lock_path,
            observed_at=self.store.now_micros(),
        )
        if evidence is not None:
            await self.store._record_worker_fenced_on(
                connection,
                evidence=evidence,
                timestamp=self.store.now_micros(),
            )

    async def _operation_view(self, record: OperationRecord) -> OperationView:
        attempt = await self.store.get_latest_attempt_for_operation(record.operation_uid)
        return self._operation_view_from_records(record, attempt)

    @staticmethod
    def _operation_view_from_records(record: OperationRecord, attempt: AttemptRecord | None) -> OperationView:
        parameters = thaw_json(record.parameters)
        result = None if record.result is None else thaw_json(record.result)
        assert isinstance(parameters, dict)
        assert result is None or isinstance(result, dict)
        return OperationView(
            operation_uid=record.operation_uid,
            operation_id=record.operation_id,
            operation_version=record.operation_version,
            parameters=parameters,
            descriptor_fingerprint=record.descriptor_fingerprint,
            state=record.state,
            submitted_by=record.submitted_by,
            submitted_at=record.submitted_at,
            updated_at=record.updated_at,
            queue_position=record.queue_position,
            queue_execution_uid=None if attempt is None else attempt.queue_execution_uid,
            attempt_uid=None if attempt is None else attempt.attempt_uid,
            replaced_by=record.replaced_by,
            result=result,
        )

    @staticmethod
    def _attempt_view(record: AttemptRecord) -> AttemptView:
        provenance = thaw_json(record.worker_provenance)
        result = None if record.result is None else thaw_json(record.result)
        assert isinstance(provenance, dict)
        assert result is None or isinstance(result, dict)
        return AttemptView(
            attempt_uid=record.attempt_uid,
            operation_uid=record.operation_uid,
            queue_execution_uid=record.queue_execution_uid,
            worker_instance_uid=record.worker_instance_uid,
            worker_revision=record.worker_revision,
            worker_provenance=provenance,
            state=record.state,
            created_at=record.created_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
            stop_requested_at=record.stop_requested_at,
            stop_acknowledged_at=record.stop_acknowledged_at,
            run_uids=list(record.run_uids),
            diagnostic=record.diagnostic,
            cleanup_completed=record.cleanup_completed,
        )

    def _descriptor_for(self, dispatch: DispatchRecord) -> OperationDescriptor:
        assert self.worker is not None
        for descriptor in self.worker.catalog.operations:
            if (
                descriptor.operation_id == dispatch.operation.operation_id
                and descriptor.operation_version == dispatch.operation.operation_version
            ):
                return descriptor
        raise RuntimeError(
            f"worker catalog no longer contains {dispatch.operation.operation_id!r} "
            f"version {dispatch.operation.operation_version!r}"
        )

    @staticmethod
    def _validated_completion(
        descriptor: OperationDescriptor,
        completion: WorkerCompletion,
    ) -> WorkerCompletion:
        if not completion.cleanup_completed:
            raise RuntimeError("worker did not prove RunEngine cleanup")
        if completion.state is AttemptState.SUCCEEDED:
            if completion.result is None:
                raise RuntimeError("successful worker completion omitted its result")
            validate_operation_result(descriptor, dict(completion.result))
        if completion.state is AttemptState.ABORTED and not completion.stop_acknowledged:
            raise RuntimeError("aborted completion lacks a matching stop acknowledgement")
        if completion.state in {AttemptState.CLAIMED, AttemptState.RUNNING}:
            raise RuntimeError("worker completion is not terminal")
        return completion
