#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Seed checked source downloads, then delegate to the pinned Vane bootstrap."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VANE_DEPENDENCY_BASELINE = "44819aa2a6c10e56065e2b0330e7d6c89d1d2574"
THRIFT_ARCHIVE = "thrift-0.23.0.tar.gz"
# Copied from ports/thrift/portfile.cmake at the exact SDK dependency baseline.
THRIFT_SHA512 = (
    "a57c6fa645852f22ca10380621facc193393b19d1d760e113baa0f964365839043"
    "f2b527bd8cd3c03808380e9f09e9a8f707f8abbd931c51632e9d5181a459cf"
)
THRIFT_MIRROR = f"https://sources.buildroot.net/thrift/{THRIFT_ARCHIVE}"
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024


def cache_archive(url: str, destination: Path, expected_sha512: str) -> None:
    if (
        destination.is_file()
        and destination.stat().st_size <= MAX_ARCHIVE_BYTES
        and hashlib.sha512(destination.read_bytes()).hexdigest() == expected_sha512
    ):
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        prefix=".checked-source-", dir=destination.parent
    )
    try:
        digest = hashlib.sha512()
        total = 0
        with os.fdopen(handle, "wb") as output:
            with urllib.request.urlopen(url, timeout=60) as response:
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > MAX_ARCHIVE_BYTES:
                        raise ValueError("source archive exceeds its bounded size")
                    digest.update(chunk)
                    output.write(chunk)
        if digest.hexdigest() != expected_sha512:
            raise ValueError("source archive differs from its pinned SHA-512")
        os.replace(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--vane-source", type=Path, required=True)
    parser.add_argument("--vcpkg-root", type=Path, required=True)
    paths, _remaining = parser.parse_known_args(arguments)
    environment = os.environ.copy()
    downloads = Path(
        environment.get(
            "VCPKG_DOWNLOADS", paths.vcpkg_root.parent / ".cache/vcpkg-downloads"
        )
    ).resolve()
    environment["VCPKG_DOWNLOADS"] = downloads.as_posix()
    baseline = json.loads(
        (paths.vane_source / "vcpkg.json").read_text(encoding="utf-8")
    )["builtin-baseline"]
    if baseline == VANE_DEPENDENCY_BASELINE:
        try:
            cache_archive(THRIFT_MIRROR, downloads / THRIFT_ARCHIVE, THRIFT_SHA512)
        except (urllib.error.URLError, TimeoutError) as error:
            # The canonical upstream download remains available to vcpkg if the
            # optional mirror cannot be reached. Hash failures always stop here.
            print(
                f"Source mirror unavailable; using vcpkg download: {error}",
                file=sys.stderr,
            )
    subprocess.run(
        [
            sys.executable,
            "-I",
            str(ROOT / "vane-extension-ci-tools/scripts/vane_extension.py"),
            *arguments,
        ],
        check=True,
        env=environment,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
