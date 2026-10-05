import asyncio
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from bluesky_queueserver.v2.admin import (
    backup_database,
    backup_manifest_path,
    check_database,
    migrate_database,
    restore_database,
)
from bluesky_queueserver.v2.config import load_controller_config, load_worker_runtime_config
from bluesky_queueserver.v2.contracts import SIMULATED_COUNT_DESCRIPTOR, AttemptState, WorkerCatalog
from bluesky_queueserver.v2.controller import AuthorityFileLock, ControllerAlreadyRunningError
from bluesky_queueserver.v2.storage import SQLITE_APPLICATION_ID, SQLiteStore

REPOSITORY_ROOT = Path(__file__).parents[4]


def run(coroutine):
    return asyncio.run(coroutine)


def write_config(tmp_path, database_path):
    config_path = tmp_path / "controller.yml"
    config_path.write_text(
        f"""
instrument_id: simulator
database_path: {database_path}
worker:
  command: [qserver-v2-worker, --config, {tmp_path / "worker.yml"}]
oidc:
  issuer: https://issuer.example
  audience: queueserver
  jwks_path: {tmp_path / "jwks.json"}
tls:
  certificate_path: {tmp_path / "server.crt"}
  private_key_path: {tmp_path / "server.key"}
offline_simulator: true
""",
        encoding="utf-8",
    )
    return load_controller_config(config_path)


def test_admin_initializes_checks_backs_up_and_restores_review_gate(tmp_path):
    database_path = tmp_path / "state.sqlite"
    config = write_config(tmp_path, database_path)
    migration = migrate_database(config)
    assert migration["schema_version"] == 1
    assert check_database(config)["application_id"] == SQLITE_APPLICATION_ID

    attempt_uid = None
    execution_uid = None

    async def create_nonterminal_state():
        nonlocal attempt_uid, execution_uid
        async with SQLiteStore(database_path, instrument_id="simulator") as store:
            await store.acquire_control_lease(principal="operator")
            catalog = WorkerCatalog(
                protocol_version="2",
                worker_revision="sha256:" + "1" * 64,
                worker_provenance={"provider": "test"},
                operations=[SIMULATED_COUNT_DESCRIPTOR],
            )
            worker_uid = str(uuid4())
            await store.register_ready_worker(
                worker_instance_uid=worker_uid,
                catalog=catalog,
                lock_path=store.worker_lock_path,
                pid=None,
            )
            operation, revision = await store.submit_operation(
                principal="operator",
                expected_revision=0,
                descriptor=SIMULATED_COUNT_DESCRIPTOR,
                operation_id="simulated-count",
                operation_version="1",
                parameters={"detectors": ["det"]},
            )
            execution, _ = await store.start_queue_execution(
                principal="operator",
                expected_revision=revision,
            )
            dispatch = await store.claim_next_attempt(worker_instance_uid=worker_uid)
            await store.mark_attempt_running(attempt_uid=dispatch.attempt.attempt_uid)
            attempt_uid = dispatch.attempt.attempt_uid
            execution_uid = execution.queue_execution_uid

    run(create_nonterminal_state())
    backup_path = tmp_path / "backup.sqlite"
    manifest = backup_database(config, backup_path)
    assert manifest["application_id"] == SQLITE_APPLICATION_ID
    assert manifest["worker_revision"] == "sha256:" + "1" * 64
    assert manifest["worker_provenance"] == {"provider": "test"}
    assert backup_manifest_path(backup_path).is_file()

    restored = restore_database(config, backup_path)
    assert restored["queue_revision"] > 0

    async def verify_restore():
        async with SQLiteStore(database_path, instrument_id="simulator", allow_initialize=False) as store:
            assert (await store.get_attempt(attempt_uid)).state is AttemptState.UNKNOWN
            assert (await store.get_queue_execution(execution_uid)).state.value == "stopped"
            block = await store.get_active_dispatch_block()
            assert block is not None
            assert block.kind == "restore.requires_review"
            assert block.fenced_at is not None

    run(verify_restore())


def test_admin_mutations_refuse_either_held_authority_lock(tmp_path):
    database_path = tmp_path / "state.sqlite"
    config = write_config(tmp_path, database_path)
    migrate_database(config)

    controller_lock = AuthorityFileLock(f"{database_path}.controller.lock")
    controller_lock.acquire()
    try:
        with pytest.raises(ControllerAlreadyRunningError):
            backup_database(config, tmp_path / "blocked-controller.sqlite")
    finally:
        controller_lock.release()

    worker_lock = AuthorityFileLock(f"{database_path}.worker.lock")
    worker_lock.acquire()
    try:
        with pytest.raises(ControllerAlreadyRunningError):
            backup_database(config, tmp_path / "blocked-worker.sqlite")
    finally:
        worker_lock.release()


def test_admin_check_rejects_wrong_application_id(tmp_path):
    database_path = tmp_path / "wrong.sqlite"
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA application_id=42")
    connection.close()
    config = write_config(tmp_path, database_path)

    with pytest.raises(Exception, match="application ID 42"):
        check_database(config)


def test_deployment_examples_validate_and_service_is_controller_only():
    controller = load_controller_config(REPOSITORY_ROOT / "deployment/v2/controller.example.yml")
    worker = load_worker_runtime_config(REPOSITORY_ROOT / "deployment/v2/worker.example.yml")
    unit = (REPOSITORY_ROOT / "deployment/v2/bluesky-queueserver-v2.service").read_text(encoding="utf-8")

    assert controller.instrument_id == "simulator"
    assert worker.provider == "simulated-count"
    assert "Restart=on-failure" in unit
    assert "KillMode=process" in unit
    assert "SendSIGKILL=no" in unit
    assert "TimeoutStopSec=infinity" in unit
    assert "qserver-v2-worker" not in next(line for line in unit.splitlines() if line.startswith("ExecStart="))
