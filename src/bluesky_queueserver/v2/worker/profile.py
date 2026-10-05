"""Isolated deployment profile loader."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from IPython.core.interactiveshell import InteractiveShell

from .sdk import OperationRegistry


class ProfileLoadError(RuntimeError):
    """A trusted startup file or operation adapter could not be loaded."""


@dataclass(frozen=True)
class LoadedProfile:
    namespace: Mapping[str, object]
    startup_hashes: Mapping[str, str]
    adapter_sha256: str


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_profile(
    *,
    startup_directory: str | Path,
    adapter_path: str | Path,
    registry: OperationRegistry,
) -> LoadedProfile:
    startup_directory = Path(startup_directory).resolve(strict=True)
    adapter_path = Path(adapter_path).resolve(strict=True)
    if not startup_directory.is_dir():
        raise ProfileLoadError(f"startup directory is not a directory: {startup_directory}")
    if not adapter_path.is_file() or adapter_path.suffix != ".py":
        raise ProfileLoadError(f"operation adapter is not a Python file: {adapter_path}")

    namespace: dict[str, object] = {"__name__": "__main__"}
    shell = InteractiveShell(user_ns=namespace)
    startup_hashes: dict[str, str] = {}
    startup_files = sorted(
        (
            path
            for path in startup_directory.iterdir()
            if path.is_file() and path.suffix in {".py", ".ipy"} and path.resolve() != adapter_path
        ),
        key=lambda path: path.name,
    )
    try:
        for path in startup_files:
            startup_hashes[str(path.resolve())] = _sha256_file(path)
            if path.suffix == ".ipy":
                shell.safe_execfile_ipy(str(path), shell.user_ns)
            else:
                shell.safe_execfile(str(path), shell.user_ns, raise_exceptions=True)
        shell.user_ns.pop("register_operations", None)
        shell.safe_execfile(str(adapter_path), shell.user_ns, raise_exceptions=True)
    except BaseException as exc:
        raise ProfileLoadError(f"failed to execute trusted profile source: {exc}") from exc

    register_operations = shell.user_ns.get("register_operations")
    if not callable(register_operations):
        raise ProfileLoadError("adapter must define callable register_operations(registry, profile)")
    profile = MappingProxyType(dict(shell.user_ns))
    try:
        register_operations(registry, profile)
    except BaseException as exc:
        raise ProfileLoadError(f"adapter registration failed: {exc}") from exc
    if not registry.descriptors:
        raise ProfileLoadError("adapter registered no operations")
    return LoadedProfile(
        namespace=profile,
        startup_hashes=MappingProxyType(startup_hashes),
        adapter_sha256=_sha256_file(adapter_path),
    )
