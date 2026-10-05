"""Offline QueueServer V2 administration commands."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from .._version import __version__
from .config import ControllerConfig, load_controller_config
from .controller import AuthorityFileLock
from .storage import (
    MIGRATIONS,
    SCHEMA_VERSION,
    SQLITE_APPLICATION_ID,
    SQLiteStore,
    StorageIdentityError,
    StorageVersionError,
    assert_local_filesystem,
    canonical_database_path,
)


def _sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


@contextmanager
def offline_authority_locks(database_path: Path):
    controller_lock = AuthorityFileLock(f"{database_path}.controller.lock")
    worker_lock = AuthorityFileLock(f"{database_path}.worker.lock")
    controller_lock.acquire()
    try:
        worker_lock.acquire()
        try:
            yield
        finally:
            worker_lock.release()
    finally:
        controller_lock.release()


def inspect_database(
    database_path: str | Path,
    *,
    instrument_id: str | None = None,
    expected_database_path: str | Path | None = None,
) -> dict[str, object]:
    database_path = canonical_database_path(database_path)
    assert_local_filesystem(database_path.parent)
    if not database_path.is_file():
        raise StorageVersionError(f"database does not exist: {database_path}")
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
        if application_id != SQLITE_APPLICATION_ID:
            raise StorageIdentityError(
                f"database {database_path} has application ID {application_id}, expected {SQLITE_APPLICATION_ID}"
            )
        rows = connection.execute("SELECT version, checksum FROM schema_migrations ORDER BY version").fetchall()
        expected = [(migration.version, migration.checksum) for migration in MIGRATIONS]
        if rows != expected:
            raise StorageVersionError(f"migration history {rows!r} does not match {expected!r}")
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if user_version != SCHEMA_VERSION:
            raise StorageVersionError(f"database user_version {user_version} does not match {SCHEMA_VERSION}")
        metadata = connection.execute(
            "SELECT database_path, instrument_id, queue_revision FROM controller_metadata WHERE singleton=1"
        ).fetchone()
        if metadata is None:
            raise StorageVersionError("controller metadata is missing")
        expected_path = (
            database_path if expected_database_path is None else canonical_database_path(expected_database_path)
        )
        if str(metadata[0]) != str(expected_path):
            raise StorageVersionError("database canonical path does not match its metadata")
        if instrument_id is not None and str(metadata[1]) != instrument_id:
            raise StorageVersionError("database instrument ID does not match configuration")
        worker = connection.execute(
            """
            SELECT worker_revision, worker_provenance_json
            FROM worker_instances ORDER BY started_at DESC, rowid DESC LIMIT 1
            """
        ).fetchone()
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise StorageVersionError(f"SQLite integrity check failed: {integrity}")
        return {
            "application_id": application_id,
            "schema_version": user_version,
            "migration_checksum": rows[-1][1],
            "queue_revision": int(metadata[2]),
            "worker_revision": None if worker is None else str(worker[0]),
            "worker_provenance": None if worker is None else json.loads(str(worker[1])),
        }
    finally:
        connection.close()


def backup_manifest_path(backup_path: str | Path) -> Path:
    return Path(f"{Path(backup_path)}.manifest.json")


def _backup_locked(config: ControllerConfig, output_path: Path) -> dict[str, object]:
    source_path = canonical_database_path(config.database_path)
    output_path = canonical_database_path(output_path)
    if output_path == source_path:
        raise ValueError("backup path must differ from the authority database")
    if output_path.exists():
        raise FileExistsError(f"backup already exists: {output_path}")
    inspect_database(source_path, instrument_id=config.instrument_id)
    temporary = Path(tempfile.mkstemp(prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent)[1])
    try:
        source = sqlite3.connect(source_path)
        destination = sqlite3.connect(temporary)
        try:
            source.execute("PRAGMA wal_checkpoint(FULL)")
            source.backup(destination)
            destination.commit()
            destination.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            destination.close()
            source.close()
        info = inspect_database(
            temporary,
            instrument_id=config.instrument_id,
            expected_database_path=source_path,
        )
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, output_path)
        manifest = {
            "format": "queueserver-v2-backup-1",
            "created_at": time.time_ns() // 1_000,
            "source_database": str(source_path),
            "worker_revision": info["worker_revision"],
            "worker_provenance": info["worker_provenance"],
            "application_id": SQLITE_APPLICATION_ID,
            "schema_version": info["schema_version"],
            "migration_checksum": info["migration_checksum"],
            "package_version": __version__,
            "sha256": _sha256_file(output_path),
        }
        manifest_path = backup_manifest_path(output_path)
        temporary_manifest = Path(f"{manifest_path}.tmp")
        temporary_manifest.write_text(
            json.dumps(manifest, allow_nan=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with temporary_manifest.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary_manifest, manifest_path)
        directory_fd = os.open(output_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return manifest
    finally:
        temporary.unlink(missing_ok=True)


def backup_database(config: ControllerConfig, output_path: str | Path) -> dict[str, object]:
    database_path = canonical_database_path(config.database_path)
    with offline_authority_locks(database_path):
        return _backup_locked(config, Path(output_path))


def migrate_database(config: ControllerConfig, *, backup_path: str | Path | None = None) -> dict[str, object]:
    database_path = canonical_database_path(config.database_path)
    with offline_authority_locks(database_path):
        if database_path.exists() and database_path.stat().st_size:
            if backup_path is None:
                raise ValueError("--backup is required when migrating an existing database")
            _backup_locked(config, Path(backup_path))

        async def migrate():
            async with SQLiteStore(
                database_path,
                instrument_id=config.instrument_id,
                allow_initialize=True,
                allow_migrate=True,
            ) as store:
                version, checksum = await store.schema_info()
                return {"schema_version": version, "migration_checksum": checksum}

        return asyncio.run(migrate())


def restore_database(config: ControllerConfig, backup_path: str | Path) -> dict[str, object]:
    database_path = canonical_database_path(config.database_path)
    backup_path = canonical_database_path(backup_path)
    manifest_path = backup_manifest_path(backup_path)
    with offline_authority_locks(database_path):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageIdentityError(f"cannot read backup manifest {manifest_path}") from exc
        expected_keys = {
            "format",
            "created_at",
            "source_database",
            "application_id",
            "schema_version",
            "migration_checksum",
            "package_version",
            "worker_revision",
            "worker_provenance",
            "sha256",
        }
        if type(manifest) is not dict or set(manifest) != expected_keys:
            raise StorageIdentityError("backup manifest has unexpected fields")
        if manifest["format"] != "queueserver-v2-backup-1":
            raise StorageIdentityError("unsupported backup manifest format")
        if manifest["sha256"] != _sha256_file(backup_path):
            raise StorageIdentityError("backup SHA-256 does not match its manifest")
        if manifest["application_id"] != SQLITE_APPLICATION_ID:
            raise StorageIdentityError("backup manifest application ID does not match")
        if manifest["schema_version"] != SCHEMA_VERSION or manifest["package_version"] != __version__:
            raise StorageVersionError("backup schema or package version is incompatible")
        inspect_database(
            backup_path,
            instrument_id=config.instrument_id,
            expected_database_path=database_path,
        )

        temporary = Path(
            tempfile.mkstemp(prefix=f".{database_path.name}.", suffix=".restore", dir=database_path.parent)[1]
        )
        try:
            source = sqlite3.connect(backup_path)
            destination = sqlite3.connect(temporary)
            try:
                source.backup(destination)
                destination.commit()
                destination.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                destination.close()
                source.close()
            inspect_database(
                temporary,
                instrument_id=config.instrument_id,
                expected_database_path=database_path,
            )
            Path(f"{database_path}-wal").unlink(missing_ok=True)
            Path(f"{database_path}-shm").unlink(missing_ok=True)
            os.replace(temporary, database_path)
            with database_path.open("rb") as stream:
                os.fsync(stream.fileno())
            directory_fd = os.open(database_path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)

        async def mark_review():
            async with SQLiteStore(
                database_path,
                instrument_id=config.instrument_id,
                allow_initialize=False,
            ) as store:
                block, revision = await store.mark_restore_requires_review(
                    fence_evidence={
                        "method": "exclusive-controller-and-worker-flock",
                        "controller_lock_path": str(store.controller_lock_path),
                        "worker_lock_path": str(store.worker_lock_path),
                    }
                )
                return {"dispatch_block_uid": block.dispatch_block_uid, "queue_revision": revision}

        return asyncio.run(mark_review())


def check_database(config: ControllerConfig) -> dict[str, object]:
    return inspect_database(config.database_path, instrument_id=config.instrument_id)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qserver-v2-admin")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("check", "migrate", "backup", "restore"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--config", required=True)
        if command == "migrate":
            subparser.add_argument("--backup")
        elif command == "backup":
            subparser.add_argument("--output", required=True)
        elif command == "restore":
            subparser.add_argument("--backup", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    config = load_controller_config(args.config)
    if args.command == "check":
        result = check_database(config)
    elif args.command == "migrate":
        result = migrate_database(config, backup_path=args.backup)
    elif args.command == "backup":
        result = backup_database(config, args.output)
    else:
        result = restore_database(config, args.backup)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
