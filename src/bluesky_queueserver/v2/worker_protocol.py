"""Correlated private controller-worker protocol."""

from __future__ import annotations

import asyncio
import socket
import struct
import time
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from pydantic import Field, ValidationError, field_validator, model_validator

if TYPE_CHECKING:
    from .storage import AttemptRecord

from .contracts import (
    WORKER_PROTOCOL_VERSION,
    AttemptState,
    AttemptUid,
    JsonObject,
    MessageUid,
    OperationDescriptor,
    StrictModel,
    WorkerCatalog,
    WorkerInstanceUid,
    canonical_json,
    require_json_object,
    uuid4_string,
)

MAX_FRAME_BYTES = 1_048_576


class WorkerProtocolError(RuntimeError):
    """A worker message violates the private protocol."""


class WorkerTransportError(RuntimeError):
    """The private worker transport failed."""


class FrameTooLargeError(WorkerProtocolError):
    """A frame exceeds the allocation limit."""


class MessageType(StrEnum):
    STARTUP_CATALOG = "startup.catalog"
    STARTUP_READY = "startup.ready"
    EXECUTE_REQUEST = "execute.request"
    EXECUTE_ACCEPTED = "execute.accepted"
    EXECUTION_COMPLETED = "execution.completed"
    PING = "ping"
    PONG = "pong"
    SAFE_STOP_REQUEST = "safe_stop.request"
    SAFE_STOP_ACKNOWLEDGED = "safe_stop.acknowledged"
    SHUTDOWN_REQUEST = "shutdown.request"
    SHUTDOWN_ACKNOWLEDGED = "shutdown.acknowledged"


_REQUEST_TYPES = {
    MessageType.EXECUTE_REQUEST,
    MessageType.PING,
    MessageType.SAFE_STOP_REQUEST,
    MessageType.SHUTDOWN_REQUEST,
}
_RESPONSE_TYPES = {
    MessageType.EXECUTE_ACCEPTED,
    MessageType.EXECUTION_COMPLETED,
    MessageType.PONG,
    MessageType.SAFE_STOP_ACKNOWLEDGED,
    MessageType.SHUTDOWN_ACKNOWLEDGED,
}
_IDLE_TYPES = {
    MessageType.STARTUP_CATALOG,
    MessageType.STARTUP_READY,
    MessageType.PING,
    MessageType.PONG,
}
_ATTEMPT_TYPES = {
    MessageType.EXECUTE_REQUEST,
    MessageType.EXECUTE_ACCEPTED,
    MessageType.EXECUTION_COMPLETED,
    MessageType.SAFE_STOP_REQUEST,
    MessageType.SAFE_STOP_ACKNOWLEDGED,
}


class WorkerMessage(StrictModel):
    protocol_version: Annotated[str, Field(pattern=r"^2$")]
    message_type: Annotated[MessageType, Field(strict=False)]
    message_id: MessageUid
    correlation_id: MessageUid | None
    worker_instance_uid: WorkerInstanceUid
    attempt_uid: AttemptUid | None
    payload: JsonObject

    @field_validator("payload")
    @classmethod
    def _payload_is_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="worker payload")

    @model_validator(mode="after")
    def _correlation_and_attempt_match_message_type(self) -> WorkerMessage:
        if self.message_type in _REQUEST_TYPES and self.correlation_id is not None:
            raise ValueError("request messages must have null correlation_id")
        if self.message_type in _RESPONSE_TYPES and self.correlation_id is None:
            raise ValueError("response messages must correlate to a request")
        if self.message_type in _IDLE_TYPES and self.attempt_uid is not None:
            raise ValueError("startup and idle liveness messages must have null attempt_uid")
        if self.message_type in _ATTEMPT_TYPES and self.attempt_uid is None:
            raise ValueError("execution and safe-stop messages require attempt_uid")
        return self


class CatalogPayload(StrictModel):
    catalog: WorkerCatalog


class ExecutePayload(StrictModel):
    operation_id: Annotated[str, Field(min_length=1)]
    operation_version: Annotated[str, Field(min_length=1)]
    parameters: JsonObject

    @field_validator("parameters")
    @classmethod
    def _parameters_are_json(cls, value: JsonObject) -> JsonObject:
        return require_json_object(value, label="parameters")


class CompletionPayload(StrictModel):
    state: Annotated[AttemptState, Field(strict=False)]
    result: JsonObject | None
    run_uids: list[str]
    diagnostic: str | None
    cleanup_completed: bool
    stop_acknowledged: bool

    @model_validator(mode="after")
    def _state_is_terminal(self) -> CompletionPayload:
        if self.state in {AttemptState.CLAIMED, AttemptState.RUNNING}:
            raise ValueError("execution completion state must be terminal")
        if any(not uid for uid in self.run_uids):
            raise ValueError("run_uids must contain nonempty strings")
        return self


class HeartbeatPayload(StrictModel):
    sent_at: Annotated[int, Field(ge=0)]


class EmptyPayload(StrictModel):
    pass


_PAYLOAD_MODELS: dict[MessageType, type[StrictModel]] = {
    MessageType.STARTUP_CATALOG: CatalogPayload,
    MessageType.STARTUP_READY: EmptyPayload,
    MessageType.EXECUTE_REQUEST: ExecutePayload,
    MessageType.EXECUTE_ACCEPTED: EmptyPayload,
    MessageType.EXECUTION_COMPLETED: CompletionPayload,
    MessageType.PING: HeartbeatPayload,
    MessageType.PONG: HeartbeatPayload,
    MessageType.SAFE_STOP_REQUEST: EmptyPayload,
    MessageType.SAFE_STOP_ACKNOWLEDGED: EmptyPayload,
    MessageType.SHUTDOWN_REQUEST: EmptyPayload,
    MessageType.SHUTDOWN_ACKNOWLEDGED: EmptyPayload,
}


def validate_payload(message: WorkerMessage) -> StrictModel:
    model = _PAYLOAD_MODELS[message.message_type]
    return model.model_validate_json(canonical_json(message.payload))


def new_message(
    *,
    message_type: MessageType,
    worker_instance_uid: str,
    attempt_uid: str | None,
    payload: JsonObject,
    correlation_id: str | None = None,
    message_id: str | None = None,
) -> WorkerMessage:
    message = WorkerMessage(
        protocol_version=WORKER_PROTOCOL_VERSION,
        message_type=message_type,
        message_id=message_id or uuid4_string(),
        correlation_id=correlation_id,
        worker_instance_uid=worker_instance_uid,
        attempt_uid=attempt_uid,
        payload=payload,
    )
    validate_payload(message)
    return message


def encode_frame(message: WorkerMessage) -> bytes:
    payload = message.model_dump_json().encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise FrameTooLargeError(f"worker frame exceeds {MAX_FRAME_BYTES} bytes")
    return struct.pack(">I", len(payload)) + payload


def decode_frame(payload: bytes) -> WorkerMessage:
    if len(payload) > MAX_FRAME_BYTES:
        raise FrameTooLargeError(f"worker frame exceeds {MAX_FRAME_BYTES} bytes")
    try:
        message = WorkerMessage.model_validate_json(payload)
        validate_payload(message)
    except (UnicodeDecodeError, ValidationError, ValueError) as exc:
        raise WorkerProtocolError(f"invalid worker message: {exc}") from exc
    return message


async def read_frame(reader: asyncio.StreamReader) -> WorkerMessage:
    try:
        header = await reader.readexactly(4)
        length = struct.unpack(">I", header)[0]
        if length > MAX_FRAME_BYTES:
            raise FrameTooLargeError(f"worker frame exceeds {MAX_FRAME_BYTES} bytes")
        payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise WorkerTransportError("worker transport reached EOF") from exc
    return decode_frame(payload)


async def write_frame(writer: asyncio.StreamWriter, message: WorkerMessage) -> None:
    writer.write(encode_frame(message))
    try:
        await writer.drain()
    except (BrokenPipeError, ConnectionResetError) as exc:
        raise WorkerTransportError("worker transport closed while writing") from exc


@dataclass(frozen=True)
class ProtocolCompletion:
    state: AttemptState
    result: Mapping[str, object] | None
    run_uids: tuple[str, ...]
    diagnostic: str | None
    cleanup_completed: bool
    stop_acknowledged: bool


class SubprocessWorkerGateway:
    """Controller-side client for one private Unix-socket worker process."""

    def __init__(
        self,
        *,
        command: Sequence[str],
        worker_lock_path: str | Path,
        instrument_id: str,
        startup_timeout_seconds: float = 10.0,
        heartbeat_interval_seconds: float = 1.0,
        heartbeat_timeout_seconds: float = 5.0,
    ):
        if not command:
            raise ValueError("worker command must not be empty")
        self._command = tuple(command)
        self.lock_path = Path(worker_lock_path).resolve(strict=False)
        self._instrument_id = instrument_id
        self._startup_timeout_seconds = startup_timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.worker_instance_uid = uuid4_string()
        self._catalog: WorkerCatalog | None = None
        self._process: asyncio.subprocess.Process | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[WorkerMessage]] = {}
        self._executions: dict[str, tuple[str, asyncio.Future[ProtocolCompletion]]] = {}
        self._fault: Exception | None = None

    @property
    def catalog(self) -> WorkerCatalog:
        if self._catalog is None:
            raise RuntimeError("worker catalog is unavailable before startup")
        return self._catalog

    @property
    def pid(self) -> int | None:
        return None if self._process is None else self._process.pid

    async def start(self) -> None:
        if self._process is not None:
            raise RuntimeError("worker gateway is already started")
        if self._catalog is not None or self._fault is not None:
            self.worker_instance_uid = uuid4_string()
            self._catalog = None
            self._fault = None
            self._pending.clear()
            self._executions.clear()
        parent_socket, child_socket = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            child_socket.set_inheritable(True)
            argv = (
                *self._command,
                "--ipc-fd",
                str(child_socket.fileno()),
                "--worker-instance-uid",
                self.worker_instance_uid,
                "--worker-lock-path",
                str(self.lock_path),
                "--instrument-id",
                self._instrument_id,
            )
            self._process = await asyncio.create_subprocess_exec(
                *argv,
                pass_fds=(child_socket.fileno(),),
                shell=False,
            )
        finally:
            child_socket.close()
        self._reader, self._writer = await asyncio.open_connection(sock=parent_socket)
        try:
            catalog_message = await asyncio.wait_for(
                read_frame(self._reader),
                timeout=self._startup_timeout_seconds,
            )
            self._validate_worker_identity(catalog_message)
            if catalog_message.message_type is not MessageType.STARTUP_CATALOG:
                raise WorkerProtocolError("worker must send startup.catalog first")
            catalog_payload = validate_payload(catalog_message)
            assert isinstance(catalog_payload, CatalogPayload)
            ready_message = await asyncio.wait_for(
                read_frame(self._reader),
                timeout=self._startup_timeout_seconds,
            )
            self._validate_worker_identity(ready_message)
            if ready_message.message_type is not MessageType.STARTUP_READY:
                raise WorkerProtocolError("worker must send startup.ready after its catalog")
            validate_payload(ready_message)
            self._catalog = catalog_payload.catalog
        except BaseException:
            await self._close_transport()
            raise
        self._reader_task = asyncio.create_task(self._reader_loop(), name="queueserver-v2-worker-reader")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="queueserver-v2-worker-heartbeat")

    async def start_execution(
        self,
        *,
        attempt: AttemptRecord,
        descriptor: OperationDescriptor,
        parameters: dict[str, object],
    ) -> Awaitable[ProtocolCompletion]:
        if attempt.attempt_uid in self._executions:
            raise WorkerProtocolError(f"attempt {attempt.attempt_uid} is already active")
        message = new_message(
            message_type=MessageType.EXECUTE_REQUEST,
            worker_instance_uid=self.worker_instance_uid,
            attempt_uid=attempt.attempt_uid,
            message_id=attempt.execute_message_uid,
            payload={
                "operation_id": descriptor.operation_id,
                "operation_version": descriptor.operation_version,
                "parameters": parameters,
            },
        )
        completion = asyncio.get_running_loop().create_future()
        self._executions[attempt.attempt_uid] = (message.message_id, completion)
        try:
            response = await self._request_message(message)
        except BaseException:
            self._executions.pop(attempt.attempt_uid, None)
            raise
        if (
            response.message_type is not MessageType.EXECUTE_ACCEPTED
            or response.attempt_uid != attempt.attempt_uid
        ):
            self._executions.pop(attempt.attempt_uid, None)
            raise WorkerProtocolError("worker did not return the correlated execute acceptance")
        return completion

    async def request_safe_stop(self, *, attempt_uid: str) -> None:
        message = new_message(
            message_type=MessageType.SAFE_STOP_REQUEST,
            worker_instance_uid=self.worker_instance_uid,
            attempt_uid=attempt_uid,
            payload={},
        )
        response = await self._request_message(message)
        if response.message_type is not MessageType.SAFE_STOP_ACKNOWLEDGED or response.attempt_uid != attempt_uid:
            raise WorkerProtocolError("worker did not return the correlated safe-stop acknowledgement")

    async def ping(self) -> None:
        response = await self._request_message(
            new_message(
                message_type=MessageType.PING,
                worker_instance_uid=self.worker_instance_uid,
                attempt_uid=None,
                payload={"sent_at": time.time_ns() // 1_000},
            )
        )
        if response.message_type is not MessageType.PONG or response.attempt_uid is not None:
            raise WorkerProtocolError("worker returned an invalid idle pong")

    async def disconnect(self) -> int | None:
        process = self._process
        if process is None:
            return None
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        writer, self._writer = self._writer, None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass
        await process.wait()
        if self._reader_task is not None:
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
            self._reader_task = None
        self._process = None
        return process.returncode

    async def close(self) -> int | None:
        process = self._process
        if process is None:
            return None
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            await asyncio.gather(self._heartbeat_task, return_exceptions=True)
            self._heartbeat_task = None
        if process.returncode is None and self._writer is not None and self._fault is None:
            active_attempt_uid = next(iter(self._executions), None)
            try:
                response = await self._request_message(
                    new_message(
                        message_type=MessageType.SHUTDOWN_REQUEST,
                        worker_instance_uid=self.worker_instance_uid,
                        attempt_uid=active_attempt_uid,
                        payload={},
                    )
                )
                if response.message_type is not MessageType.SHUTDOWN_ACKNOWLEDGED:
                    raise WorkerProtocolError("worker did not acknowledge shutdown")
            except (WorkerProtocolError, WorkerTransportError, TimeoutError):
                pass
        await self._close_transport()
        return process.returncode

    async def _request_message(self, message: WorkerMessage) -> WorkerMessage:
        if self._fault is not None:
            raise WorkerTransportError(str(self._fault)) from self._fault
        if self._writer is None:
            raise WorkerTransportError("worker transport is not open")
        response = asyncio.get_running_loop().create_future()
        self._pending[message.message_id] = response
        try:
            async with self._write_lock:
                await write_frame(self._writer, message)
            return await asyncio.wait_for(response, timeout=self._heartbeat_timeout_seconds)
        except TimeoutError as exc:
            raise WorkerTransportError(f"worker did not answer {message.message_type.value}") from exc
        finally:
            self._pending.pop(message.message_id, None)

    async def _reader_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                message = await read_frame(self._reader)
                self._validate_worker_identity(message)
                if message.message_type is MessageType.PING:
                    await self._send(
                        new_message(
                            message_type=MessageType.PONG,
                            worker_instance_uid=self.worker_instance_uid,
                            attempt_uid=None,
                            correlation_id=message.message_id,
                            payload=message.payload,
                        )
                    )
                    continue
                if message.message_type is MessageType.EXECUTION_COMPLETED:
                    self._deliver_completion(message)
                    continue
                if message.correlation_id is None or message.correlation_id not in self._pending:
                    raise WorkerProtocolError("worker response has an unknown correlation ID")
                future = self._pending[message.correlation_id]
                if not future.done():
                    future.set_result(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)

    def _deliver_completion(self, message: WorkerMessage) -> None:
        assert message.attempt_uid is not None
        execution = self._executions.get(message.attempt_uid)
        if execution is None:
            raise WorkerProtocolError("worker completion references no active attempt")
        execute_message_uid, future = execution
        if message.correlation_id != execute_message_uid:
            raise WorkerProtocolError("worker completion correlation ID does not match execute request")
        payload = validate_payload(message)
        assert isinstance(payload, CompletionPayload)
        self._executions.pop(message.attempt_uid, None)
        if not future.done():
            future.set_result(
                ProtocolCompletion(
                    state=payload.state,
                    result=payload.result,
                    run_uids=tuple(payload.run_uids),
                    diagnostic=payload.diagnostic,
                    cleanup_completed=payload.cleanup_completed,
                    stop_acknowledged=payload.stop_acknowledged,
                )
            )

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval_seconds)
                response = await self._request_message(
                    new_message(
                        message_type=MessageType.PING,
                        worker_instance_uid=self.worker_instance_uid,
                        attempt_uid=None,
                        payload={"sent_at": time.time_ns() // 1_000},
                    )
                )
                if response.message_type is not MessageType.PONG or response.attempt_uid is not None:
                    raise WorkerProtocolError("worker returned an invalid idle pong")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)

    async def _send(self, message: WorkerMessage) -> None:
        if self._writer is None:
            raise WorkerTransportError("worker transport is not open")
        async with self._write_lock:
            await write_frame(self._writer, message)

    def _validate_worker_identity(self, message: WorkerMessage) -> None:
        if message.worker_instance_uid != self.worker_instance_uid:
            raise WorkerProtocolError("worker instance identity does not match")

    def _fail(self, exc: Exception) -> None:
        if self._fault is None:
            self._fault = exc
        for future in (*self._pending.values(), *(item[1] for item in self._executions.values())):
            if not future.done():
                future.set_exception(exc)

    async def _close_transport(self) -> None:
        writer, self._writer = self._writer, None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass
        process = self._process
        if process is not None and process.returncode is None:
            await process.wait()
        if self._reader_task is not None:
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
            self._reader_task = None
        self._reader = None
        self._process = None
