from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Mapping, Sequence
from typing import Protocol

from jsonschema import Draft7Validator
from jsonschema.exceptions import SchemaError

from .contracts import (
    WORKER_PROTOCOL_VERSION,
    OperationDescriptor,
    OperationRecord,
    OperationState,
    WorkerCatalog,
    WorkerExecutionResult,
    WorkerProtocolError,
    WorkerTransportError,
)


class WorkerClient(Protocol):
    async def open(self) -> WorkerCatalog:
        """Start the worker and return its operation catalog."""

    async def execute(self, operation: OperationRecord) -> WorkerExecutionResult:
        """Execute one previously claimed operation."""

    async def close(self) -> None:
        """Release worker resources."""


class SubprocessWorkerClient:
    """Private newline-delimited JSON client for a local worker subprocess."""

    def __init__(self, argv: Sequence[str], response_timeout: float):
        if not argv:
            raise ValueError("Worker argv must not be empty")
        if response_timeout <= 0:
            raise ValueError("Worker response timeout must be positive")

        self._argv = tuple(argv)
        self._response_timeout = response_timeout
        self._process: asyncio.subprocess.Process | None = None
        self._request_lock = asyncio.Lock()
        self._usable = False

    async def open(self) -> WorkerCatalog:
        if self._process is not None:
            raise RuntimeError("Worker client is already open")

        self._process = await asyncio.create_subprocess_exec(
            *self._argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            shell=False,
        )
        self._usable = True
        try:
            result = await self._request(command="describe", payload={})
            return self._catalog_from_result(result)
        except BaseException:
            await self._terminate_process()
            raise

    async def execute(self, operation: OperationRecord) -> WorkerExecutionResult:
        result = await self._request(
            command="execute",
            payload={
                "operation_uid": operation.operation_uid,
                "operation_id": operation.operation_id,
                "operation_version": operation.operation_version,
                "parameters": operation.parameters,
            },
        )
        return self._execution_result_from_wire(result)

    async def close(self) -> None:
        process = self._process
        if process is None:
            return

        if process.returncode is None and self._usable:
            try:
                await self._request(command="shutdown", payload={})
                await asyncio.wait_for(process.wait(), timeout=self._response_timeout)
            except (WorkerProtocolError, WorkerTransportError, asyncio.TimeoutError):
                pass

        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=self._response_timeout)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

        if process.stdin is not None:
            process.stdin.close()
            try:
                await process.stdin.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass

        self._process = None
        self._usable = False

    async def _request(self, *, command: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        process = self._process
        if process is None or not self._usable:
            raise WorkerTransportError("Worker process is not available")
        if process.returncode is not None:
            self._usable = False
            raise WorkerTransportError(f"Worker process exited with status {process.returncode}")
        if process.stdin is None or process.stdout is None:
            self._usable = False
            raise WorkerTransportError("Worker process streams are not available")

        request_id = str(uuid.uuid4())
        request = {
            "protocol_version": WORKER_PROTOCOL_VERSION,
            "request_id": request_id,
            "command": command,
            "payload": payload,
        }
        try:
            frame = (json.dumps(request, allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise WorkerProtocolError(f"Worker request is not JSON serializable: {exc}") from exc

        async with self._request_lock:
            try:
                response_line = await asyncio.wait_for(
                    self._write_and_read(process, frame),
                    timeout=self._response_timeout,
                )
            except asyncio.TimeoutError as exc:
                self._usable = False
                raise WorkerTransportError("Timed out waiting for worker response") from exc
            except (BrokenPipeError, ConnectionResetError, asyncio.IncompleteReadError) as exc:
                self._usable = False
                raise WorkerTransportError("Worker transport closed unexpectedly") from exc

        if not response_line:
            self._usable = False
            raise WorkerTransportError("Worker transport reached EOF")

        try:
            response = json.loads(response_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._usable = False
            raise WorkerProtocolError("Worker returned malformed JSON") from exc

        try:
            return self._validate_response(response, request_id=request_id)
        except WorkerProtocolError:
            self._usable = False
            raise

    @staticmethod
    async def _write_and_read(process: asyncio.subprocess.Process, frame: bytes) -> bytes:
        assert process.stdin is not None
        assert process.stdout is not None
        process.stdin.write(frame)
        await process.stdin.drain()
        return await process.stdout.readline()

    @staticmethod
    def _validate_response(response: object, *, request_id: str) -> Mapping[str, object]:
        if not isinstance(response, dict):
            raise WorkerProtocolError("Worker response must be a JSON object")

        common_keys = {"protocol_version", "request_id", "ok"}
        ok = response.get("ok")
        expected_keys = common_keys | ({"result"} if ok is True else {"error"})
        if set(response) != expected_keys:
            raise WorkerProtocolError("Worker response has missing or unexpected keys")
        if (
            type(response["protocol_version"]) is not int
            or response["protocol_version"] != WORKER_PROTOCOL_VERSION
        ):
            raise WorkerProtocolError("Worker protocol version does not match")
        if response["request_id"] != request_id:
            raise WorkerProtocolError("Worker response request ID does not match")
        if type(ok) is not bool:
            raise WorkerProtocolError("Worker response 'ok' field must be boolean")
        if ok is False:
            error = response["error"]
            if not isinstance(error, str) or not error:
                raise WorkerProtocolError("Worker error response must contain a concise error string")
            raise WorkerProtocolError(error)

        result = response["result"]
        if not isinstance(result, dict):
            raise WorkerProtocolError("Worker result must be a JSON object")
        return result

    @staticmethod
    def _catalog_from_result(result: Mapping[str, object]) -> WorkerCatalog:
        if set(result) != {"worker_revision", "operations"}:
            raise WorkerProtocolError("Worker catalog has missing or unexpected keys")
        worker_revision = result["worker_revision"]
        operations = result["operations"]
        if not isinstance(worker_revision, str) or not worker_revision:
            raise WorkerProtocolError("Worker revision must be a non-empty string")
        if not isinstance(operations, list):
            raise WorkerProtocolError("Worker operations must be a list")

        descriptors = []
        for raw_descriptor in operations:
            if not isinstance(raw_descriptor, dict) or set(raw_descriptor) != {
                "operation_id",
                "operation_version",
                "parameter_schema",
            }:
                raise WorkerProtocolError("Worker operation descriptor has missing or unexpected keys")
            operation_id = raw_descriptor["operation_id"]
            operation_version = raw_descriptor["operation_version"]
            parameter_schema = raw_descriptor["parameter_schema"]
            if not isinstance(operation_id, str) or not operation_id:
                raise WorkerProtocolError("Worker operation ID must be a non-empty string")
            if not isinstance(operation_version, str) or not operation_version:
                raise WorkerProtocolError("Worker operation version must be a non-empty string")
            if not isinstance(parameter_schema, dict):
                raise WorkerProtocolError("Worker operation schema must be a JSON object")
            try:
                Draft7Validator.check_schema(parameter_schema)
            except SchemaError as exc:
                raise WorkerProtocolError(f"Worker operation schema is invalid: {exc.message}") from exc
            if (
                parameter_schema.get("type") != "object"
                or parameter_schema.get("additionalProperties") is not False
            ):
                raise WorkerProtocolError("Worker operation schema must describe a closed JSON object")
            descriptors.append(
                OperationDescriptor(
                    operation_id=operation_id,
                    operation_version=operation_version,
                    parameter_schema=parameter_schema,
                )
            )

        return WorkerCatalog(worker_revision=worker_revision, operations=tuple(descriptors))

    @staticmethod
    def _execution_result_from_wire(result: Mapping[str, object]) -> WorkerExecutionResult:
        if set(result) != {"state", "run_uids", "error_message"}:
            raise WorkerProtocolError("Worker execution result has missing or unexpected keys")

        state_value = result["state"]
        run_uids = result["run_uids"]
        error_message = result["error_message"]
        if state_value not in (OperationState.SUCCEEDED.value, OperationState.FAILED.value):
            raise WorkerProtocolError(f"Worker returned invalid final operation state {state_value!r}")
        if not isinstance(run_uids, list) or not all(isinstance(uid, str) and uid for uid in run_uids):
            raise WorkerProtocolError("Worker run UIDs must be non-empty strings")

        state = OperationState(state_value)
        if state is OperationState.SUCCEEDED and error_message is not None:
            raise WorkerProtocolError("Successful worker result must not contain an error")
        if state is OperationState.FAILED:
            if run_uids:
                raise WorkerProtocolError("Failed worker result must not contain run UIDs")
            if not isinstance(error_message, str) or not error_message:
                raise WorkerProtocolError("Failed worker result must contain an error")

        return WorkerExecutionResult(
            state=state,
            run_uids=tuple(run_uids),
            error_message=error_message,
        )

    async def _terminate_process(self) -> None:
        process = self._process
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=self._response_timeout)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        self._process = None
        self._usable = False
