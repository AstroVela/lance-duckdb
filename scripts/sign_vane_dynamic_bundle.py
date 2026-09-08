#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Sign bounded native data in an isolated, stdlib-only release job."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import stat
import subprocess
import tempfile
import tomllib
from pathlib import Path

EXTENSION_NAME = "lance"
MAX_ARTIFACT_BYTES = 384 * 1024 * 1024
KEY_FINGERPRINTS = {
    "production": "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb",
    "testpypi": "53779fb8f9c97e9dec9c66ff838839eb234d1a64d4b105671304820e627b5e32",
}


def child_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("VANE_PROVIDER_SIGNING_PRIVATE_KEY", None)
    # The exact stdlib signing utility invokes OpenSSL by name.
    environment["PATH"] = "/usr/bin:/bin"
    return environment


def _git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(
        ["/usr/bin/git", "-C", str(root), *arguments],
        text=True,
        env=child_environment(),
    ).strip()


def committed_manifest(root: Path, profile: str) -> dict:
    filename = (
        "vane-extension-release.toml"
        if profile == "production"
        else "vane-extension.toml"
    )
    manifest = tomllib.loads(_git(root, "show", f"HEAD:{filename}"))
    vane = manifest.get("vane", {})
    if (
        manifest.get("schema_version") != 2
        or manifest.get("name") != EXTENSION_NAME
        or not isinstance(vane, dict)
        or vane.get("repository") != "AstroVela/vane"
        or not isinstance(vane.get("revision"), str)
        or re.fullmatch(r"[0-9a-f]{40}", vane["revision"]) is None
    ):
        raise ValueError("signing requires the committed exact official Vane manifest")
    return vane


def artifact_identity(path: Path) -> tuple[int, bytes, bytes]:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or not 512 < metadata.st_size <= MAX_ARTIFACT_BYTES
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise ValueError(
            "signer artifact must be bounded regular non-symlink native data"
        )
    digest = hashlib.sha256()
    with path.open("rb") as source:
        remaining = metadata.st_size - 256
        while remaining:
            chunk = source.read(min(remaining, 1024 * 1024))
            if not chunk:
                raise ValueError("native data changed size while being inspected")
            digest.update(chunk)
            remaining -= len(chunk)
        signature = source.read(256)
        if len(signature) != 256 or source.read(1):
            raise ValueError("native data changed size while being inspected")
    return metadata.st_size, digest.digest(), signature


def require_artifact(path: Path) -> tuple[int, bytes]:
    size, digest, signature = artifact_identity(path)
    if signature != b"\0" * 256:
        raise ValueError("signer input must contain the unsigned DuckDB signature slot")
    return size, digest


def require_signed_artifact(path: Path, prepared_identity: tuple[int, bytes]) -> None:
    size, digest, signature = artifact_identity(path)
    if (size, digest) != prepared_identity or signature == b"\0" * 256:
        raise ValueError("signing must change only the final 256-byte signature slot")


def require_key_fingerprint(contents: bytearray, profile: str) -> None:
    if not 0 < len(contents) <= 64 * 1024:
        raise ValueError("signing key must be present and bounded")
    result = subprocess.run(
        ["/usr/bin/openssl", "pkey", "-pubout", "-outform", "DER", "-passin", "pass:"],
        input=contents,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
        env=child_environment(),
    )
    if (
        result.returncode
        or hashlib.sha256(result.stdout).hexdigest() != KEY_FINGERPRINTS[profile]
    ):
        # Do not disclose OpenSSL diagnostics, input contents, or private paths.
        raise ValueError("signing key does not match the selected native trust root")


def sign_bundle(arguments, contents: bytearray) -> None:
    manifest = committed_manifest(arguments.extension_root, arguments.profile)
    if (
        _git(arguments.vane_source, "rev-parse", "HEAD") != manifest["revision"]
        or _git(arguments.vane_source, "remote", "get-url", "origin").removesuffix(
            ".git"
        )
        != "https://github.com/AstroVela/vane"
        or _git(arguments.vane_source, "status", "--porcelain", "--untracked-files=no")
    ):
        raise ValueError(
            "signing utility must come from the clean exact official Vane checkout"
        )
    artifact = (
        arguments.input_directory / "artifacts" / f"{EXTENSION_NAME}.duckdb_extension"
    )
    # Snapshot immutable input identity before running the signing utility; do
    # not allow simultaneous edits of the input and output to mask a replacement.
    prepared_identity = require_artifact(artifact)
    require_key_fingerprint(contents, arguments.profile)
    output = arguments.output_directory
    if output.is_symlink() or any(parent.is_symlink() for parent in output.parents):
        raise ValueError("signed output directory must not use symlinks")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("signed output directory must be empty and not a symlink")
    runner_temp = Path(os.environ["RUNNER_TEMP"]).resolve()
    incoming = arguments.input_directory.resolve()
    if any(
        directory == runner_temp or directory in runner_temp.parents
        for directory in (incoming, output.resolve())
    ):
        raise ValueError(
            "private signing files must be outside downloaded and uploaded data"
        )
    with tempfile.TemporaryDirectory(
        prefix="vane-provider-signing-", dir=runner_temp
    ) as temporary:
        private_key = Path(temporary) / "private.pem"
        handle = os.open(private_key, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(handle, "wb") as destination:
                destination.write(contents)
                destination.flush()
                os.fsync(destination.fileno())
            subprocess.run(
                [
                    "/usr/bin/python3",
                    "-I",
                    "-S",
                    str(
                        arguments.vane_source / "scripts/sign_test_dynamic_extension.py"
                    ),
                    "--private-key",
                    str(private_key),
                    str(artifact),
                    str(output / artifact.name),
                ],
                check=True,
                env=child_environment(),
            )
        finally:
            if private_key.exists():
                with private_key.open("r+b", buffering=0) as destination:
                    destination.write(b"\0" * private_key.stat().st_size)
                    os.fsync(destination.fileno())
                private_key.unlink()
    require_signed_artifact(output / artifact.name, prepared_identity)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("manifest", "sign"))
    parser.add_argument("--profile", choices=tuple(KEY_FINGERPRINTS), required=True)
    parser.add_argument("--extension-root", type=Path, required=True)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--vane-source", type=Path)
    parser.add_argument("--input-directory", type=Path)
    parser.add_argument("--output-directory", type=Path)
    arguments = parser.parse_args(argv)
    # Remove the key from the environment before Git/OpenSSL/child Python run.
    contents = bytearray(
        os.environ.pop("VANE_PROVIDER_SIGNING_PRIVATE_KEY", "").encode()
    )
    try:
        if arguments.command == "manifest":
            if arguments.github_output is None:
                parser.error("manifest requires --github-output")
            manifest = committed_manifest(arguments.extension_root, arguments.profile)
            with arguments.github_output.open("a", encoding="utf-8") as output:
                output.write("vane_repository=AstroVela/vane\n")
                output.write(f"vane_revision={manifest['revision']}\n")
        else:
            if any(
                value is None
                for value in (
                    arguments.vane_source,
                    arguments.input_directory,
                    arguments.output_directory,
                )
            ):
                parser.error(
                    "sign requires exact source, input, and output directories"
                )
            sign_bundle(arguments, contents)
    finally:
        contents[:] = b"\0" * len(contents)
        contents.clear()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
