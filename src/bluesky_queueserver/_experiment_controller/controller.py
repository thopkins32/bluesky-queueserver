from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from pathlib import Path

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError

from .contracts import (
    ControlLease,
    ControllerEvent,
    OperationDescriptor,
    OperationRecord,
    OperationState,
    OperationValidationError,
    QueueSnapshot,
    WorkerCatalog,
    WorkerProtocolError,
    WorkerTransportError,
    validate_operation_parameters,
)
from .storage import SQLiteControllerStore
from .worker import WorkerClient


class ExperimentController:
    """Coordinate durable FIFO operations with one private worker."""

    def __init__(
        self,
        database_path: Path,
        worker: WorkerClient,
        clock: Callable[[], float] = time.time,
    ):
        self._store = SQLiteControllerStore(database_path)
        self._worker = worker
        self._clock = clock
        self._catalog: WorkerCatalog | None = None
        self._operations: dict[tuple[str, str], OperationDescriptor] = {}
        self._dispatch_lock = asyncio.Lock()

    async def open(self) -> None:
        if self._catalog is not None:
            raise RuntimeError("Experiment controller is already open")

        self._store.open()
        try:
            catalog = await self._worker.open()
            operations = self._validate_catalog(catalog)
        except BaseException:
            try:
                await self._worker.close()
            finally:
                self._store.close()
            raise

        self._catalog = catalog
        self._operations = operations

    async def acquire_lease(self, subject: str, ttl_seconds: float) -> ControlLease:
        self._require_open()
        if not subject.strip():
            raise ValueError("Lease subject must not be empty")
        if not ttl_seconds > 0:
            raise ValueError("Lease TTL must be positive")

        now = self._clock()
        return self._store.acquire_lease(
            subject=subject,
            issued_at=now,
            expires_at=now + ttl_seconds,
        )

    async def queue_snapshot(self) -> QueueSnapshot:
        self._require_open()
        return self._store.queue_snapshot()

    async def events(self, after_event_id: int = 0) -> list[ControllerEvent]:
        self._require_open()
        return self._store.events(after_event_id=after_event_id)

    async def submit_operation(
        self,
        subject: str,
        lease_uid: str,
        expected_revision: int,
        operation_id: str,
        operation_version: str,
        parameters: Mapping[str, object],
    ) -> OperationRecord:
        catalog = self._require_open()
        now = self._clock()
        self._store.validate_mutation(
            subject=subject,
            lease_uid=lease_uid,
            expected_revision=expected_revision,
            now=now,
        )

        descriptor = self._operations.get((operation_id, operation_version))
        if descriptor is None:
            raise OperationValidationError(f"Unknown operation ID/version {operation_id!r}/{operation_version!r}")
        normalized_parameters = validate_operation_parameters(descriptor, parameters)

        return self._store.submit_operation(
            subject=subject,
            lease_uid=lease_uid,
            expected_revision=expected_revision,
            operation_id=operation_id,
            operation_version=operation_version,
            parameters=normalized_parameters,
            worker_revision=catalog.worker_revision,
            now=now,
        )

    async def dispatch_next(
        self,
        subject: str,
        lease_uid: str,
        expected_revision: int,
    ) -> OperationRecord | None:
        self._require_open()
        async with self._dispatch_lock:
            operation = self._store.claim_next_operation(
                subject=subject,
                lease_uid=lease_uid,
                expected_revision=expected_revision,
                now=self._clock(),
            )
            if operation is None:
                return None

            try:
                result = await self._worker.execute(operation)
            except WorkerTransportError as exc:
                return self._store.mark_operation_unknown(
                    operation_uid=operation.operation_uid,
                    actor=subject,
                    error_message=str(exc) or "Worker transport failed",
                    now=self._clock(),
                )

            try:
                final_state = OperationState(result.state)
            except ValueError as exc:
                raise WorkerProtocolError(f"Worker returned invalid final state {result.state!r}") from exc
            if final_state not in (OperationState.SUCCEEDED, OperationState.FAILED):
                raise WorkerProtocolError(f"Worker returned invalid final state {final_state.value!r}")

            return self._store.complete_operation(
                operation_uid=operation.operation_uid,
                actor=subject,
                state=final_state,
                run_uids=result.run_uids,
                error_message=result.error_message,
                now=self._clock(),
            )

    async def acknowledge_dispatch_block(
        self,
        subject: str,
        lease_uid: str,
        expected_revision: int,
        note: str,
    ) -> QueueSnapshot:
        self._require_open()
        if not note.strip():
            raise ValueError("Recovery acknowledgement note must not be empty")
        return self._store.acknowledge_dispatch_block(
            subject=subject,
            lease_uid=lease_uid,
            expected_revision=expected_revision,
            note=note,
            now=self._clock(),
        )

    async def close(self) -> None:
        if self._catalog is None:
            return
        try:
            await self._worker.close()
        finally:
            self._catalog = None
            self._operations = {}
            self._store.close()

    def _require_open(self) -> WorkerCatalog:
        if self._catalog is None:
            raise RuntimeError("Experiment controller is not open")
        return self._catalog

    @staticmethod
    def _validate_catalog(catalog: WorkerCatalog) -> dict[tuple[str, str], OperationDescriptor]:
        if not isinstance(catalog.worker_revision, str) or not catalog.worker_revision:
            raise WorkerProtocolError("Worker revision must be a non-empty string")

        operations: dict[tuple[str, str], OperationDescriptor] = {}
        for descriptor in catalog.operations:
            if not descriptor.operation_id or not descriptor.operation_version:
                raise WorkerProtocolError("Worker operation ID and version must be non-empty")
            key = (descriptor.operation_id, descriptor.operation_version)
            if key in operations:
                raise WorkerProtocolError(
                    f"Worker catalog contains duplicate operation ID/version {key[0]!r}/{key[1]!r}"
                )
            try:
                Draft7Validator.check_schema(descriptor.parameter_schema)
            except SchemaError as exc:
                raise WorkerProtocolError(f"Worker operation schema is invalid: {exc.message}") from exc
            if (
                descriptor.parameter_schema.get("type") != "object"
                or descriptor.parameter_schema.get("additionalProperties") is not False
            ):
                raise WorkerProtocolError("Worker operation schema must describe a closed JSON object")
            operations[key] = descriptor

        return operations
