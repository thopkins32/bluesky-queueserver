"""QueueServer V2 controller command-line entry point."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import uvicorn

from .auth import OidcAuthenticator
from .config import ControllerConfig, load_controller_config
from .controller import ControllerService
from .storage import SQLiteStore
from .web import create_app
from .worker_protocol import SubprocessWorkerGateway


def build_service(config: ControllerConfig) -> tuple[ControllerService, OidcAuthenticator]:
    store = SQLiteStore(config.database_path, instrument_id=config.instrument_id)
    worker = SubprocessWorkerGateway(
        command=config.worker.command,
        worker_lock_path=store.worker_lock_path,
        instrument_id=config.instrument_id,
        startup_timeout_seconds=config.worker.startup_timeout_seconds,
        heartbeat_interval_seconds=config.worker.heartbeat_interval_seconds,
        heartbeat_timeout_seconds=config.worker.heartbeat_timeout_seconds,
    )
    return ControllerService(
        store,
        worker=worker,
        protocol_timeout_seconds=config.worker.heartbeat_timeout_seconds,
    ), OidcAuthenticator(config.oidc)


def _validate_tls_files(config: ControllerConfig) -> None:
    for label, path in (
        ("TLS certificate", config.tls.certificate_path),
        ("TLS private key", config.tls.private_key_path),
    ):
        if not path.is_file():
            raise ValueError(f"{label} is not a file: {path}")


async def serve(config: ControllerConfig, *, host: str, port: int) -> None:
    _validate_tls_files(config)
    service, authenticator = build_service(config)
    await service.open()
    try:
        application = create_app(service, authenticator)
        server = uvicorn.Server(
            uvicorn.Config(
                application,
                host=host,
                port=port,
                workers=1,
                ssl_certfile=str(config.tls.certificate_path),
                ssl_keyfile=str(config.tls.private_key_path),
            )
        )
        await server.serve()
    finally:
        await authenticator.close()
        await service.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="start-qserver-v2")
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    return parser


def main() -> int:
    args = _parser().parse_args()
    config = load_controller_config(Path(args.config))
    asyncio.run(serve(config, host=args.host, port=args.port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
