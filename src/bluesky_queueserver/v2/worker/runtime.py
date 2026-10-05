"""QueueServer V2 worker process runtime."""

from __future__ import annotations

import argparse
import asyncio
import errno
import fcntl
import os
import socket
import stat
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from bluesky import RunEngine
from bluesky.utils import RunEngineInterrupted

from ..config import WorkerRuntimeConfig, load_worker_runtime_config
from ..contracts import AttemptState, WorkerCatalog
from ..worker_protocol import (
    CompletionPayload,
    ExecutePayload,
    MessageType,
    WorkerMessage,
    WorkerProtocolError,
    WorkerTransportError,
    new_message,
    read_frame,
    validate_payload,
    write_frame,
)
from .profile import LoadedProfile, load_profile
from .sdk import OperationContext, OperationRegistry, build_worker_catalog
from .simulated_count import register_simulated_count, simulated_devices


class WorkerAuthorityError(RuntimeError):
    """The worker cannot safely hold its exclusive authority lock."""


class WorkerAuthorityLock:
    def __init__(self, path: str | Path, worker_instance_uid: str):
        self.path = Path(path).resolve(strict=False)
        self.worker_instance_uid = worker_instance_uid
        self._fd: int | None = None

    def acquire(self) -> None:
        parent = self.path.parent.stat()
        self._validate_owner_and_mode(parent, "worker lock parent")
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise WorkerAuthorityError(f"cannot safely open worker lock {self.path}: {exc}") from exc
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode):
                raise WorkerAuthorityError(f"worker lock is not a regular file: {self.path}")
            self._validate_owner_and_mode(metadata, "worker lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise WorkerAuthorityError(f"worker authority is already held: {self.path}") from exc
                raise WorkerAuthorityError(f"cannot lock worker authority {self.path}: {exc}") from exc
            os.ftruncate(fd, 0)
            os.write(fd, f"{self.worker_instance_uid} {os.getpid()}\n".encode())
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
    def _validate_owner_and_mode(metadata: os.stat_result, label: str) -> None:
        if metadata.st_uid != os.geteuid():
            raise WorkerAuthorityError(f"{label} is not owned by the service user")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise WorkerAuthorityError(f"{label} is group- or world-writable")


@dataclass
class ActiveExecution:
    attempt_uid: str
    execute_message_uid: str
    task: asyncio.Task[CompletionPayload]
    started: asyncio.Event
    accepted_sent: asyncio.Event
    stop_requested: bool = False
    stop_acknowledged: bool = False


class WorkerRuntime:
    def __init__(
        self,
        *,
        config: WorkerRuntimeConfig,
        ipc_fd: int,
        worker_instance_uid: str,
        worker_lock_path: str | Path,
        instrument_id: str,
        heartbeat_interval_seconds: float = 1.0,
        heartbeat_timeout_seconds: float = 5.0,
    ):
        self.config = config
        self.ipc_fd = ipc_fd
        self.worker_instance_uid = worker_instance_uid
        self.instrument_id = instrument_id
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._authority_lock = WorkerAuthorityLock(worker_lock_path, worker_instance_uid)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._write_lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="qserver-v2-run-engine")
        self._run_engine: RunEngine | None = None
        self._registry: OperationRegistry | None = None
        self._context: OperationContext | None = None
        self._catalog: WorkerCatalog | None = None
        self._active: ActiveExecution | None = None
        self._accepting = True
        self._controller_lost = False
        self._shutdown_requested = False
        self._last_controller_traffic = time.monotonic()
        self._ping_ids: set[str] = set()

    async def run(self) -> int:
        self._authority_lock.acquire()
        try:
            await self._initialize_execution_environment()
            ipc_socket = socket.socket(fileno=self.ipc_fd)
            ipc_socket.setblocking(False)
            self._reader, self._writer = await asyncio.open_connection(sock=ipc_socket)
            await self._send(
                new_message(
                    message_type=MessageType.STARTUP_CATALOG,
                    worker_instance_uid=self.worker_instance_uid,
                    attempt_uid=None,
                    payload={"catalog": self.catalog.model_dump(mode="json")},
                )
            )
            await self._send(
                new_message(
                    message_type=MessageType.STARTUP_READY,
                    worker_instance_uid=self.worker_instance_uid,
                    attempt_uid=None,
                    payload={},
                )
            )
            return await self._control_loop()
        finally:
            self._accepting = False
            if self._active is not None:
                self._request_pause()
                await asyncio.gather(self._active.task, return_exceptions=True)
            if self._writer is not None:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._authority_lock.release()

    @property
    def catalog(self) -> WorkerCatalog:
        if self._catalog is None:
            raise RuntimeError("worker runtime is not initialized")
        return self._catalog

    async def _initialize_execution_environment(self) -> None:
        loop = asyncio.get_running_loop()
        self._run_engine = await loop.run_in_executor(self._executor, lambda: RunEngine({}, context_managers=[]))
        registry = OperationRegistry()
        loaded_profile: LoadedProfile | None = None
        if self.config.provider == "simulated-count":
            register_simulated_count(registry)
            devices = simulated_devices()
            profile = None
        else:
            assert self.config.startup_directory is not None
            assert self.config.adapter_path is not None
            loaded_profile = load_profile(
                startup_directory=self.config.startup_directory,
                adapter_path=self.config.adapter_path,
                registry=registry,
            )
            devices = {}
            profile = loaded_profile.namespace
        self._registry = registry
        self._context = OperationContext(run_engine=self._run_engine, devices=devices, profile=profile)
        self._catalog = build_worker_catalog(
            registry,
            environment_lock_sha256=self.config.environment_lock_sha256,
            startup_hashes=None if loaded_profile is None else loaded_profile.startup_hashes,
            adapter_sha256=None if loaded_profile is None else loaded_profile.adapter_sha256,
        )

    async def _control_loop(self) -> int:
        assert self._reader is not None
        while True:
            remaining = self.heartbeat_timeout_seconds - (time.monotonic() - self._last_controller_traffic)
            if remaining <= 0:
                await self._handle_controller_loss()
                return 2
            try:
                message = await asyncio.wait_for(
                    read_frame(self._reader),
                    timeout=min(self.heartbeat_interval_seconds, remaining),
                )
            except TimeoutError:
                ping = new_message(
                    message_type=MessageType.PING,
                    worker_instance_uid=self.worker_instance_uid,
                    attempt_uid=None,
                    payload={"sent_at": time.time_ns() // 1_000},
                )
                self._ping_ids.add(ping.message_id)
                try:
                    await self._send(ping)
                except WorkerTransportError:
                    await self._handle_controller_loss()
                    return 2
                continue
            except (WorkerProtocolError, WorkerTransportError):
                await self._handle_controller_loss()
                return 2

            self._last_controller_traffic = time.monotonic()
            try:
                keep_running = await self._handle_message(message)
            except WorkerProtocolError:
                await self._handle_controller_loss()
                return 2
            if not keep_running:
                return 0

    async def _handle_message(self, message: WorkerMessage) -> bool:
        if message.worker_instance_uid != self.worker_instance_uid:
            raise WorkerProtocolError("controller used the wrong worker instance identity")
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
            return True
        if message.message_type is MessageType.PONG:
            if message.correlation_id not in self._ping_ids:
                raise WorkerProtocolError("controller pong has an unknown correlation ID")
            self._ping_ids.remove(message.correlation_id)
            return True
        if message.message_type is MessageType.EXECUTE_REQUEST:
            await self._handle_execute(message)
            return True
        if message.message_type is MessageType.SAFE_STOP_REQUEST:
            await self._handle_safe_stop(message)
            return True
        if message.message_type is MessageType.SHUTDOWN_REQUEST:
            await self._handle_shutdown(message)
            return False
        raise WorkerProtocolError(f"controller sent unsupported message {message.message_type.value}")

    async def _handle_execute(self, message: WorkerMessage) -> None:
        if not self._accepting or self._active is not None:
            raise WorkerProtocolError("worker cannot accept another execution")
        assert message.attempt_uid is not None
        payload = validate_payload(message)
        assert isinstance(payload, ExecutePayload)
        started = asyncio.Event()
        accepted_sent = asyncio.Event()
        task = asyncio.create_task(
            self._execute_and_report(
                message=message,
                payload=payload,
                started=started,
                accepted_sent=accepted_sent,
            ),
            name=f"queueserver-v2-attempt-{message.attempt_uid}",
        )
        self._active = ActiveExecution(
            attempt_uid=message.attempt_uid,
            execute_message_uid=message.message_id,
            task=task,
            started=started,
            accepted_sent=accepted_sent,
        )
        await started.wait()
        await self._send(
            new_message(
                message_type=MessageType.EXECUTE_ACCEPTED,
                worker_instance_uid=self.worker_instance_uid,
                attempt_uid=message.attempt_uid,
                correlation_id=message.message_id,
                payload={},
            )
        )
        accepted_sent.set()

    async def _handle_safe_stop(self, message: WorkerMessage) -> None:
        active = self._active
        if active is None or message.attempt_uid != active.attempt_uid:
            raise WorkerProtocolError("safe stop does not match the active attempt")
        if active.stop_requested:
            raise WorkerProtocolError("safe stop was already requested")
        active.stop_requested = True
        self._request_pause()
        active.stop_acknowledged = True
        await self._send(
            new_message(
                message_type=MessageType.SAFE_STOP_ACKNOWLEDGED,
                worker_instance_uid=self.worker_instance_uid,
                attempt_uid=active.attempt_uid,
                correlation_id=message.message_id,
                payload={},
            )
        )

    async def _handle_shutdown(self, message: WorkerMessage) -> None:
        self._accepting = False
        self._shutdown_requested = True
        active = self._active
        if active is not None:
            if message.attempt_uid != active.attempt_uid:
                raise WorkerProtocolError("shutdown attempt identity does not match")
            self._request_pause()
            await active.task
        elif message.attempt_uid is not None:
            raise WorkerProtocolError("idle shutdown must have null attempt_uid")
        await self._send(
            new_message(
                message_type=MessageType.SHUTDOWN_ACKNOWLEDGED,
                worker_instance_uid=self.worker_instance_uid,
                attempt_uid=message.attempt_uid,
                correlation_id=message.message_id,
                payload={},
            )
        )

    async def _execute_and_report(
        self,
        *,
        message: WorkerMessage,
        payload: ExecutePayload,
        started: asyncio.Event,
        accepted_sent: asyncio.Event,
    ) -> CompletionPayload:
        loop = asyncio.get_running_loop()
        completion = await loop.run_in_executor(
            self._executor,
            self._execute_sync,
            payload,
            started,
            loop,
        )
        await accepted_sent.wait()
        active = self._active
        if active is not None:
            completion = completion.model_copy(update={"stop_acknowledged": active.stop_acknowledged})
        if not self._controller_lost:
            await self._send(
                new_message(
                    message_type=MessageType.EXECUTION_COMPLETED,
                    worker_instance_uid=self.worker_instance_uid,
                    attempt_uid=message.attempt_uid,
                    correlation_id=message.message_id,
                    payload=completion.model_dump(mode="json"),
                )
            )
        self._active = None
        return completion

    def _execute_sync(
        self,
        payload: ExecutePayload,
        started: asyncio.Event,
        loop: asyncio.AbstractEventLoop,
    ) -> CompletionPayload:
        assert self._run_engine is not None
        assert self._registry is not None
        assert self._context is not None
        run_uids: list[str] = []

        def collect(name, document):
            if name == "start":
                run_uids.append(str(document["uid"]))

        token = self._run_engine.subscribe(collect)
        state = AttemptState.SUCCEEDED
        result = None
        diagnostic = None
        try:
            registered, prepared = self._registry.prepare(
                operation_id=payload.operation_id,
                operation_version=payload.operation_version,
                parameters=payload.parameters,
                context=self._context,
            )
            loop.call_soon_threadsafe(started.set)
            self._run_engine(prepared.plan)
            result = self._registry.validate_result(registered, prepared.build_result(tuple(run_uids)))
        except RunEngineInterrupted:
            if self._run_engine.state != "idle":
                self._run_engine.abort(reason="QueueServer V2 stop request")
            state = AttemptState.INTERRUPTED if self._shutdown_requested else AttemptState.ABORTED
            diagnostic = "controller shutdown" if self._shutdown_requested else "safe stop"
        except BaseException as exc:
            diagnostic = f"{type(exc).__name__}: {exc}"
            if self._run_engine.state != "idle":
                try:
                    self._run_engine.abort(reason="QueueServer V2 execution failure")
                except BaseException as cleanup_exc:
                    diagnostic = f"{diagnostic}; cleanup failed: {type(cleanup_exc).__name__}: {cleanup_exc}"
            state = AttemptState.FAILED if self._run_engine.state == "idle" else AttemptState.UNKNOWN
        finally:
            self._run_engine.unsubscribe(token)
            if not started.is_set():
                loop.call_soon_threadsafe(started.set)
        cleanup_completed = self._run_engine.state == "idle"
        if not cleanup_completed:
            state = AttemptState.UNKNOWN
            result = None
        return CompletionPayload(
            state=state,
            result=result,
            run_uids=run_uids,
            diagnostic=diagnostic,
            cleanup_completed=cleanup_completed,
            stop_acknowledged=False,
        )

    def _request_pause(self) -> None:
        if self._run_engine is not None and self._run_engine.state not in {"idle", "panicked"}:
            self._run_engine.request_pause(defer=True)

    async def _handle_controller_loss(self) -> None:
        self._controller_lost = True
        self._accepting = False
        if self._active is not None:
            self._request_pause()
            await asyncio.gather(self._active.task, return_exceptions=True)

    async def _send(self, message: WorkerMessage) -> None:
        if self._writer is None:
            raise WorkerTransportError("worker transport is not open")
        async with self._write_lock:
            await write_frame(self._writer, message)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qserver-v2-worker")
    parser.add_argument("--config", required=True)
    parser.add_argument("--ipc-fd", required=True, type=int)
    parser.add_argument("--worker-instance-uid", required=True)
    parser.add_argument("--worker-lock-path", required=True)
    parser.add_argument("--instrument-id", required=True)
    return parser


async def _run_from_args(args: argparse.Namespace) -> int:
    config = load_worker_runtime_config(args.config)
    runtime = WorkerRuntime(
        config=config,
        ipc_fd=args.ipc_fd,
        worker_instance_uid=args.worker_instance_uid,
        worker_lock_path=args.worker_lock_path,
        instrument_id=args.instrument_id,
    )
    return await runtime.run()


def main() -> int:
    args = _parser().parse_args()
    try:
        return asyncio.run(_run_from_args(args))
    except (WorkerAuthorityError, WorkerProtocolError, WorkerTransportError, ValueError) as exc:
        print(f"qserver-v2-worker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
