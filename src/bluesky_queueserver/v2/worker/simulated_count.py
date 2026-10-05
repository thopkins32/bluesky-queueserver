"""Native reviewed simulated-count operation."""

from __future__ import annotations

from bluesky.plans import count
from ophyd.sim import hw

from ..contracts import (
    AuthorizationScope,
    OrphanPolicy,
    SimulatedCountRequest,
    SimulatedCountResult,
)
from .sdk import OperationContext, OperationRegistry, PreparedOperation


def register_simulated_count(registry: OperationRegistry) -> None:
    @registry.register(
        operation_id="simulated-count",
        operation_version="1",
        request_model=SimulatedCountRequest,
        result_model=SimulatedCountResult,
        required_scope=AuthorizationScope.CONTROL,
        orphan_policy=OrphanPolicy.REQUEST_STOP,
    )
    def prepare(request: SimulatedCountRequest, context: OperationContext) -> PreparedOperation:
        detector = context.devices["det"]
        return PreparedOperation(
            plan=count([detector], num=request.num, delay=request.delay),
            build_result=lambda run_uids: SimulatedCountResult(run_uids=list(run_uids)),
        )


def simulated_devices() -> dict[str, object]:
    return {"det": hw().det}
