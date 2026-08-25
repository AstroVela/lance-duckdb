#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

"""Generate deterministic provenance for a Vane wheel with static Lance."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlparse

FULL_SHA_RE = re.compile(r"[0-9a-f]{40}")
OBJECT_ID_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
PLATFORM_TAG_RE = re.compile(r"[A-Za-z0-9_.]+")
LANCE_REPOSITORY = "https://github.com/lance-format/lance-duckdb.git"
VANE_REPOSITORY = "https://github.com/AstroVela/vane.git"
FORBIDDEN_RUNTIME_ENV = (
    "LANCE_DUCKDB_EXTENSION",
    "LANCE_DUCKDB_TEST_ALLOW_UNSIGNED",
)
RUNTIME_PROBE = r"""
import json
import sys
from pathlib import Path

import vane
from vane import _native

prefix = Path(sys.prefix).resolve()
vane_path = Path(vane.__file__).resolve()
native_path = Path(_native.__file__).resolve()
for label, path in (("vane", vane_path), ("vane._native", native_path)):
    try:
        path.relative_to(prefix)
    except ValueError as exc:
        raise RuntimeError(
            f"{label} was not imported from the fresh environment: {path}"
        ) from exc

connection = vane.connect(
    ":memory:",
    config={
        "autoinstall_known_extensions": "false",
        "autoload_known_extensions": "false",
    },
)
try:
    before = connection.execute(
        "SELECT install_mode FROM duckdb_extensions() "
        "WHERE lower(extension_name) = 'lance'"
    ).fetchone()
    if before is None or str(before[0]).upper() != "STATICALLY_LINKED":
        raise RuntimeError(f"Lance is not statically linked into the wheel: {before!r}")
    connection.execute("LOAD lance")
    loaded, install_mode = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() "
        "WHERE lower(extension_name) = 'lance'"
    ).fetchone()
    if loaded is not True or str(install_mode).upper() != "STATICALLY_LINKED":
        raise RuntimeError(
            f"Lance did not load as a static extension: {(loaded, install_mode)!r}"
        )
    fork_version, source_id = connection.execute(
        "SELECT library_version, source_id FROM pragma_version()"
    ).fetchone()
finally:
    connection.close()

print(
    json.dumps(
        {
            "fork_version": fork_version,
            "install_mode": str(install_mode).upper(),
            "native_module": str(native_path),
            "source_id": source_id,
            "vane_module": str(vane_path),
        },
        sort_keys=True,
    )
)
"""


class ProvenanceError(RuntimeError):
    """Raised when artifact identity cannot be proven."""


def fail(message: str) -> NoReturn:
    raise ProvenanceError(message)


def run(command: list[str], *, cwd: Path) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def git(cwd: Path, *arguments: str) -> str:
    return run(["git", *arguments], cwd=cwd)


def require_clean_checkout(path: Path, label: str) -> None:
    status = git(path, "status", "--porcelain=v1")
    if status:
        fail(f"{label} checkout is not clean: {path}")


def canonical_github_remote(value: str) -> str:
    remote = value.strip()
    if remote.startswith("git@github.com:"):
        remote = "https://github.com/" + remote.removeprefix("git@github.com:")
    parsed = urlparse(remote)
    if parsed.scheme not in {"http", "https", "ssh"} or parsed.hostname != "github.com":
        fail(f"unsupported GitHub remote URL: {value!r}")
    path = parsed.path.removeprefix("/")
    if path.endswith(".git"):
        path = path[:-4]
    if not path or path.count("/") != 1:
        fail(f"invalid GitHub repository path: {value!r}")
    return f"https://github.com/{path}.git"


def require_official_repository(remote: str, expected: str, label: str) -> str:
    actual = canonical_github_remote(remote)
    if actual.casefold() != expected.casefold():
        fail(f"{label} checkout is not from the official repository: {actual}")
    return actual


def load_vane_revision(manifest_path: Path) -> str:
    try:
        with manifest_path.open("rb") as handle:
            manifest = tomllib.load(handle)
        vane = manifest["vane"]
        repository = vane["repository"]
        revision = vane["revision"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        fail(f"invalid Vane integration manifest {manifest_path}: {exc}")
    if repository != "AstroVela/vane":
        fail(f"Vane repository must be AstroVela/vane, got {repository!r}")
    if not isinstance(revision, str) or not FULL_SHA_RE.fullmatch(revision):
        fail(f"Vane revision must be a full lowercase SHA: {revision!r}")
    return revision


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def lexical_absolute_path(path: Path) -> Path:
    """Make a path absolute without resolving a virtualenv interpreter symlink."""

    return Path(os.path.abspath(path))


def wheel_platform_tag(wheel: Path) -> str:
    if wheel.suffix != ".whl":
        fail(f"expected a wheel filename, got {wheel.name!r}")
    fields = wheel.stem.split("-")
    if len(fields) < 5 or not PLATFORM_TAG_RE.fullmatch(fields[-1]):
        fail(f"cannot parse wheel platform tag from {wheel.name!r}")
    return fields[-1]


def probe_runtime(python: Path) -> dict[str, str]:
    if not python.is_file():
        fail(f"fresh-environment Python does not exist: {python}")
    leaked = [name for name in FORBIDDEN_RUNTIME_ENV if os.environ.get(name)]
    if leaked:
        fail(f"static-wheel probe forbids ambient variables: {', '.join(leaked)}")
    environment = os.environ.copy()
    for name in ("PYTHONHOME", "PYTHONPATH", "VIRTUAL_ENV"):
        environment.pop(name, None)
    with tempfile.TemporaryDirectory(prefix="vane-lance-provenance-") as temporary:
        result = subprocess.run(
            [str(python), "-I", "-c", RUNTIME_PROBE],
            cwd=temporary,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        fail(f"installed runtime returned invalid identity JSON: {exc}")
    if not isinstance(value, dict) or any(
        not isinstance(value.get(key), str)
        for key in (
            "fork_version",
            "install_mode",
            "native_module",
            "source_id",
            "vane_module",
        )
    ):
        fail(f"installed runtime returned incomplete identity: {value!r}")
    print(json.dumps(value, sort_keys=True), file=sys.stderr)
    return value


def vane_identity(vane_source: Path) -> tuple[str, str, str]:
    fork_revision = run(
        [sys.executable, "scripts/resolve_duckdb_fork_version.py", "--print-revision"],
        cwd=vane_source,
    )
    fork_version = run(
        [sys.executable, "scripts/resolve_duckdb_fork_version.py", "--print-version"],
        cwd=vane_source,
    )
    source_id = run(
        [sys.executable, "scripts/sync_duckdb_source_id.py", "--print"],
        cwd=vane_source,
    )
    if not OBJECT_ID_RE.fullmatch(fork_revision):
        fail(f"Vane returned an invalid DuckDB fork revision: {fork_revision!r}")
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+-vane\.[0-9a-f]{10}", fork_version):
        fail(f"Vane returned an invalid DuckDB fork version: {fork_version!r}")
    if not OBJECT_ID_RE.fullmatch(source_id):
        fail(f"Vane returned an invalid DuckDB SourceID: {source_id!r}")
    return fork_revision, fork_version, source_id


def write_atomic(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(contents)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def generate(args: argparse.Namespace) -> dict[str, str]:
    extension_root = args.extension_root.resolve()
    vane_source = args.vane_source.resolve()
    wheel = args.wheel.resolve(strict=True)
    manifest_path = args.manifest.resolve(strict=True)
    output = args.output.resolve()
    checksum_output = args.checksum_output.resolve()
    if output in {wheel, checksum_output} or checksum_output == wheel:
        fail("wheel, provenance, and checksum paths must be distinct")

    require_clean_checkout(extension_root, "lance-duckdb")
    require_clean_checkout(vane_source, "Vane")
    lance_commit = git(extension_root, "rev-parse", "HEAD^{commit}")
    lance_tree = git(extension_root, "rev-parse", "HEAD^{tree}")
    if not FULL_SHA_RE.fullmatch(lance_commit) or not FULL_SHA_RE.fullmatch(lance_tree):
        fail("lance-duckdb checkout returned an invalid Git identity")
    lance_repository = require_official_repository(
        git(extension_root, "remote", "get-url", "origin"),
        LANCE_REPOSITORY,
        "lance-duckdb",
    )

    vane_commit = load_vane_revision(manifest_path)
    actual_vane_commit = git(vane_source, "rev-parse", "HEAD^{commit}")
    if actual_vane_commit != vane_commit:
        fail(
            f"Vane checkout revision mismatch: expected {vane_commit}, got {actual_vane_commit}"
        )
    vane_repository = require_official_repository(
        git(vane_source, "remote", "get-url", "origin"),
        VANE_REPOSITORY,
        "Vane",
    )

    fork_revision, fork_version, source_id = vane_identity(vane_source)
    runtime = probe_runtime(lexical_absolute_path(args.python))
    if runtime["fork_version"] != fork_version:
        fail(
            "installed Vane fork version mismatch: "
            f"expected {fork_version!r}, got {runtime['fork_version']!r}"
        )
    if runtime["source_id"] != source_id[:10]:
        fail(
            "installed DuckDB SourceID mismatch: "
            f"expected {source_id[:10]!r}, got {runtime['source_id']!r}"
        )
    if runtime["install_mode"] != "STATICALLY_LINKED":
        fail(f"installed Lance mode is not static: {runtime['install_mode']!r}")

    wheel_hash = sha256_file(wheel)
    provenance = {
        "artifact_kind": "vane-ai-wheel-with-static-lance",
        "duckdb_fork_revision": fork_revision,
        "duckdb_fork_version": fork_version,
        "duckdb_source_id": source_id,
        "lance_commit": lance_commit,
        "lance_install_mode": runtime["install_mode"],
        "lance_repository": lance_repository,
        "lance_tree": lance_tree,
        "target_platform": wheel_platform_tag(wheel),
        "vane_commit": vane_commit,
        "vane_repository": vane_repository,
        "wheel_filename": wheel.name,
        "wheel_sha256": wheel_hash,
    }
    write_atomic(output, json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    write_atomic(checksum_output, f"{wheel_hash}  {wheel.name}\n")
    return provenance


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--vane-source", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checksum-output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    provenance = generate(parse_args())
    print(json.dumps(provenance, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ProvenanceError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
