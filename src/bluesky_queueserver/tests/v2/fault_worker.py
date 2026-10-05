from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import struct
from uuid import uuid4

from bluesky_queueserver.v2.contracts import SIMULATED_COUNT_DESCRIPTOR, WorkerCatalog
from bluesky_queueserver.v2.worker.runtime import WorkerAuthorityLock
from bluesky_queueserver.v2.worker_protocol import MessageType, new_message, read_frame, write_frame


async def run(args):
    authority = WorkerAuthorityLock(args.worker_lock_path, args.worker_instance_uid)
    authority.acquire()
    sock = socket.socket(fileno=args.ipc_fd)
    sock.setblocking(False)
    reader, writer = await asyncio.open_connection(sock=sock)
    catalog = WorkerCatalog(
        protocol_version="2",
        worker_revision="sha256:" + "f" * 64,
        worker_provenance={"provider": "fault", "mode": args.mode},
        operations=[SIMULATED_COUNT_DESCRIPTOR],
    )
    try:
        await write_frame(
            writer,
            new_message(
                message_type=MessageType.STARTUP_CATALOG,
                worker_instance_uid=args.worker_instance_uid,
                attempt_uid=None,
                payload={"catalog": catalog.model_dump(mode="json")},
            ),
        )
        await write_frame(
            writer,
            new_message(
                message_type=MessageType.STARTUP_READY,
                worker_instance_uid=args.worker_instance_uid,
                attempt_uid=None,
                payload={},
            ),
        )
        while True:
            request = await read_frame(reader)
            if request.message_type is MessageType.PING:
                await write_frame(
                    writer,
                    new_message(
                        message_type=MessageType.PONG,
                        worker_instance_uid=args.worker_instance_uid,
                        attempt_uid=None,
                        correlation_id=request.message_id,
                        payload=request.payload,
                    ),
                )
                continue
            if request.message_type is MessageType.EXECUTE_REQUEST:
                break
        await write_frame(
            writer,
            new_message(
                message_type=MessageType.EXECUTE_ACCEPTED,
                worker_instance_uid=args.worker_instance_uid,
                attempt_uid=request.attempt_uid,
                correlation_id=request.message_id,
                payload={},
            ),
        )
        if args.mode == "eof":
            return 7
        if args.mode == "exit":
            os._exit(7)
        if args.mode == "timeout":
            await reader.read()
            return 7
        if args.mode == "malformed":
            payload = b"{not-json"
            writer.write(struct.pack(">I", len(payload)) + payload)
            await writer.drain()
        elif args.mode == "oversized":
            writer.write(struct.pack(">I", 1_048_577))
            await writer.drain()
        else:
            worker_uid = args.worker_instance_uid
            attempt_uid = request.attempt_uid
            correlation_id = request.message_id
            result = {"run_uids": [str(uuid4())]}
            if args.mode == "wrong-protocol":
                payload = {
                    "protocol_version": "999",
                    "message_type": "execution.completed",
                    "message_id": str(uuid4()),
                    "correlation_id": correlation_id,
                    "worker_instance_uid": worker_uid,
                    "attempt_uid": attempt_uid,
                    "payload": {
                        "state": "succeeded",
                        "result": result,
                        "run_uids": result["run_uids"],
                        "diagnostic": None,
                        "cleanup_completed": True,
                        "stop_acknowledged": False,
                    },
                }
                encoded = json.dumps(payload, separators=(",", ":")).encode()
                writer.write(struct.pack(">I", len(encoded)) + encoded)
                await writer.drain()
            else:
                if args.mode == "wrong-worker":
                    worker_uid = str(uuid4())
                elif args.mode == "wrong-attempt":
                    attempt_uid = str(uuid4())
                elif args.mode == "wrong-correlation":
                    correlation_id = str(uuid4())
                elif args.mode == "invalid-result":
                    result = {"unexpected": True}
                await write_frame(
                    writer,
                    new_message(
                        message_type=MessageType.EXECUTION_COMPLETED,
                        worker_instance_uid=worker_uid,
                        attempt_uid=attempt_uid,
                        correlation_id=correlation_id,
                        payload={
                            "state": "succeeded",
                            "result": result,
                            "run_uids": [str(uuid4())],
                            "diagnostic": None,
                            "cleanup_completed": True,
                            "stop_acknowledged": False,
                        },
                    ),
                )
        await reader.read()
        return 7
    finally:
        writer.close()
        await writer.wait_closed()
        authority.release()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--ipc-fd", required=True, type=int)
    parser.add_argument("--worker-instance-uid", required=True)
    parser.add_argument("--worker-lock-path", required=True)
    parser.add_argument("--instrument-id", required=True)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
