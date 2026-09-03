from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from .contracts import OperationState
from .controller import ExperimentController
from .worker import SubprocessWorkerClient


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the internal experiment-controller simulator prototype")
    parser.add_argument("--database", type=Path, help="Path to a local SQLite database")
    parser.add_argument("--num", type=int, default=1, help="Number of simulated count readings")
    return parser


async def _run_demo(database_path: Path, *, num: int) -> tuple[dict[str, object], bool]:
    worker = SubprocessWorkerClient(
        (
            sys.executable,
            "-u",
            "-m",
            "bluesky_queueserver._experiment_controller.simulator",
        ),
        response_timeout=30.0,
    )
    controller = ExperimentController(database_path, worker)
    await controller.open()
    try:
        lease = await controller.acquire_lease("demo-operator", ttl_seconds=60.0)
        initial = await controller.queue_snapshot()
        submitted = await controller.submit_operation(
            "demo-operator",
            lease.lease_uid,
            initial.revision,
            "simulated-count",
            "1",
            {"num": num},
        )
        before_dispatch = await controller.queue_snapshot()
        final = await controller.dispatch_next(
            "demo-operator",
            lease.lease_uid,
            before_dispatch.revision,
        )
        if final is None:
            raise RuntimeError("Submitted operation was not available for dispatch")

        final_snapshot = await controller.queue_snapshot()
        events = await controller.events()
        output = {
            "operation_uid": submitted.operation_uid,
            "state": final.state.value,
            "run_uids": list(final.run_uids),
            "queue_revision": final_snapshot.revision,
            "worker_revision": final.worker_revision,
            "event_ids": [event.event_id for event in events],
        }
        return output, final.state is OperationState.SUCCEEDED
    finally:
        await controller.close()


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.database is not None:
        output, succeeded = asyncio.run(_run_demo(arguments.database, num=arguments.num))
    else:
        with tempfile.TemporaryDirectory(prefix="experiment-controller-") as directory:
            output, succeeded = asyncio.run(_run_demo(Path(directory) / "controller.sqlite", num=arguments.num))

    print(json.dumps(output, separators=(",", ":"), sort_keys=True))
    return 0 if succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
