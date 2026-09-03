from __future__ import annotations

import json
import sys
from collections.abc import Mapping

from bluesky import RunEngine
from bluesky.callbacks.core import CallbackBase
from bluesky.plans import count
from ophyd.sim import hw

from .contracts import (
    SIMULATED_COUNT_OPERATION_ID,
    SIMULATED_COUNT_OPERATION_VERSION,
    SIMULATED_WORKER_REVISION,
    WORKER_PROTOCOL_VERSION,
    OperationDescriptor,
    OperationState,
    WorkerExecutionResult,
    validate_operation_parameters,
)

_SIMULATED_COUNT_PARAMETER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "num": {
            "type": "integer",
            "minimum": 1,
            "maximum": 10,
        }
    },
    "additionalProperties": False,
}


class _RunUidCollector(CallbackBase):
    def __init__(self) -> None:
        super().__init__()
        self.run_uids: list[str] = []
        self._open_run_uids: set[str] = set()

    def start(self, doc: Mapping[str, object]) -> None:
        uid = str(doc["uid"])
        self.run_uids.append(uid)
        self._open_run_uids.add(uid)

    def stop(self, doc: Mapping[str, object]) -> None:
        self._open_run_uids.discard(str(doc["run_start"]))


def _simulated_count_descriptor() -> OperationDescriptor:
    return OperationDescriptor(
        operation_id=SIMULATED_COUNT_OPERATION_ID,
        operation_version=SIMULATED_COUNT_OPERATION_VERSION,
        parameter_schema=_SIMULATED_COUNT_PARAMETER_SCHEMA,
    )


def _describe() -> dict[str, object]:
    descriptor = _simulated_count_descriptor()
    return {
        "worker_revision": SIMULATED_WORKER_REVISION,
        "operations": [
            {
                "operation_id": descriptor.operation_id,
                "operation_version": descriptor.operation_version,
                "parameter_schema": descriptor.parameter_schema,
            }
        ],
    }


def _execute(payload: Mapping[str, object]) -> WorkerExecutionResult:
    if set(payload) != {"operation_uid", "operation_id", "operation_version", "parameters"}:
        raise ValueError("Execute payload has missing or unexpected keys")
    if not isinstance(payload["operation_uid"], str) or not payload["operation_uid"]:
        raise ValueError("Operation UID must be a non-empty string")

    descriptor = _simulated_count_descriptor()
    if (
        payload["operation_id"] != descriptor.operation_id
        or payload["operation_version"] != descriptor.operation_version
    ):
        raise ValueError("Unknown operation ID or version")

    parameters = validate_operation_parameters(descriptor, payload["parameters"])
    detector = hw().__dict__["det"]
    run_engine = RunEngine({})
    collector = _RunUidCollector()
    subscription = run_engine.subscribe(collector)
    try:
        run_engine(count([detector], num=parameters.get("num", 1)))
    finally:
        run_engine.unsubscribe(subscription)

    return WorkerExecutionResult(
        state=OperationState.SUCCEEDED,
        run_uids=tuple(collector.run_uids),
        error_message=None,
    )


def _success_response(request_id: str, result: Mapping[str, object]) -> dict[str, object]:
    return {
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "request_id": request_id,
        "ok": True,
        "result": result,
    }


def _error_response(request_id: str, error: str) -> dict[str, object]:
    return {
        "protocol_version": WORKER_PROTOCOL_VERSION,
        "request_id": request_id,
        "ok": False,
        "error": error,
    }


def _handle_request(request: object) -> tuple[dict[str, object], bool]:
    request_id = request.get("request_id", "") if isinstance(request, dict) else ""
    try:
        if not isinstance(request, dict):
            raise ValueError("Request must be a JSON object")
        if set(request) != {"protocol_version", "request_id", "command", "payload"}:
            raise ValueError("Request has missing or unexpected keys")
        if type(request["protocol_version"]) is not int or request["protocol_version"] != WORKER_PROTOCOL_VERSION:
            raise ValueError("Unsupported worker protocol version")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("Request ID must be a non-empty string")
        if not isinstance(request["command"], str):
            raise ValueError("Command must be a string")
        if not isinstance(request["payload"], dict):
            raise ValueError("Payload must be a JSON object")

        command = request["command"]
        payload = request["payload"]
        if command == "describe":
            if payload:
                raise ValueError("Describe payload must be empty")
            return _success_response(request_id, _describe()), False
        if command == "shutdown":
            if payload:
                raise ValueError("Shutdown payload must be empty")
            return _success_response(request_id, {}), True
        if command == "execute":
            try:
                execution = _execute(payload)
            except Exception as exc:
                execution = WorkerExecutionResult(
                    state=OperationState.FAILED,
                    run_uids=(),
                    error_message=f"{type(exc).__name__}: {exc}",
                )
            return (
                _success_response(
                    request_id,
                    {
                        "state": execution.state.value,
                        "run_uids": list(execution.run_uids),
                        "error_message": execution.error_message,
                    },
                ),
                False,
            )
        raise ValueError(f"Unknown command {command!r}")
    except Exception as exc:
        return _error_response(str(request_id), f"{type(exc).__name__}: {exc}"), False


def main() -> int:
    for raw_line in sys.stdin:
        try:
            request = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            response = _error_response("", f"JSONDecodeError: {exc.msg}")
            should_shutdown = False
        else:
            response, should_shutdown = _handle_request(request)

        sys.stdout.write(json.dumps(response, allow_nan=False, separators=(",", ":")) + "\n")
        sys.stdout.flush()
        if should_shutdown:
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
