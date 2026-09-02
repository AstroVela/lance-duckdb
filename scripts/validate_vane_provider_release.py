#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Validate one immutable Lance provider-wheel candidate set."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from email.parser import BytesParser
from email.policy import default
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, parse_wheel_filename
from packaging.version import Version

EXPECTED_INTERPRETERS = frozenset({"cp310", "cp311", "cp312", "cp313", "cp314"})
EXPECTED_PLATFORM = "manylinux_2_28_x86_64"
PROVIDER_DISTRIBUTION = "vane-extension-lance"
TESTPYPI_JSON_BASE = "https://test.pypi.org/pypi"
MAX_METADATA_BYTES = 1024 * 1024
MAX_TESTPYPI_FILE_BYTES = 100_000_000


class ReleaseValidationError(RuntimeError):
    """Raised when Lance distributions violate the release contract."""


@dataclass(frozen=True)
class WheelRecord:
    """Validated filename and dependency metadata for one provider wheel."""

    path: Path
    distribution_name: str
    version: str
    interpreter: str
    requirements: tuple[Requirement, ...]


def _canonical_version(value: str, description: str) -> str:
    parsed = Version(value)
    if str(parsed) != value:
        raise ReleaseValidationError(
            f"{description} must use canonical PEP 440 spelling: {parsed}"
        )
    if parsed.local is not None:
        raise ReleaseValidationError(f"{description} must not contain a local version")
    return value


def _read_metadata(path: Path):
    if path.stat().st_size > MAX_TESTPYPI_FILE_BYTES:
        raise ReleaseValidationError(
            f"{path.name} exceeds TestPyPI's 100 MB file limit"
        )
    try:
        with zipfile.ZipFile(path) as wheel:
            members = [
                member
                for member in wheel.infolist()
                if member.filename.count("/") == 1
                and member.filename.endswith(".dist-info/METADATA")
            ]
            if len(members) != 1:
                raise ReleaseValidationError(
                    f"{path.name} must contain exactly one top-level METADATA file"
                )
            if members[0].file_size > MAX_METADATA_BYTES:
                raise ReleaseValidationError(
                    f"{path.name} METADATA exceeds {MAX_METADATA_BYTES} bytes"
                )
            contents = wheel.read(members[0])
    except zipfile.BadZipFile as error:
        raise ReleaseValidationError(
            f"{path.name} is not a valid wheel archive"
        ) from error
    return BytesParser(policy=default).parsebytes(contents)


def _read_wheel(path: Path) -> WheelRecord:
    filename_name, filename_version, build, tags = parse_wheel_filename(path.name)
    if build:
        raise ReleaseValidationError(f"{path.name} must not contain a build tag")
    if len(tags) != 1:
        raise ReleaseValidationError(f"{path.name} must contain exactly one wheel tag")
    tag = next(iter(tags))
    if (
        tag.interpreter not in EXPECTED_INTERPRETERS
        or tag.abi != "none"
        or tag.platform != EXPECTED_PLATFORM
    ):
        raise ReleaseValidationError(
            f"{path.name} must use cp310-cp314-none-{EXPECTED_PLATFORM}, found {tag}"
        )

    metadata = _read_metadata(path)
    names = [str(value) for value in metadata.get_all("Name", [])]
    versions = [str(value) for value in metadata.get_all("Version", [])]
    if len(names) != 1 or len(versions) != 1:
        raise ReleaseValidationError(
            f"{path.name} must declare exactly one Name and Version"
        )
    distribution_name = names[0]
    version = _canonical_version(versions[0], f"{path.name} version")
    if (
        canonicalize_name(distribution_name) != filename_name
        or Version(version) != filename_version
    ):
        raise ReleaseValidationError(
            f"{path.name} filename and METADATA identities differ"
        )
    try:
        requirements = tuple(
            Requirement(value) for value in metadata.get_all("Requires-Dist", [])
        )
    except ValueError as error:
        raise ReleaseValidationError(
            f"{path.name} contains an invalid Requires-Dist"
        ) from error
    return WheelRecord(
        path=path,
        distribution_name=distribution_name,
        version=version,
        interpreter=tag.interpreter,
        requirements=requirements,
    )


def _exact_requirements(record: WheelRecord) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for requirement in record.requirements:
        name = canonicalize_name(requirement.name)
        specifiers = tuple(requirement.specifier)
        if (
            name in resolved
            or requirement.extras
            or requirement.url is not None
            or requirement.marker is not None
            or len(specifiers) != 1
            or specifiers[0].operator != "==="
        ):
            raise ReleaseValidationError(
                f"{record.path.name} must contain only unique exact === requirements"
            )
        resolved[name] = specifiers[0].version
    return resolved


def validate_release(directory: Path, vane_version: str) -> str:
    """Validate a complete CPython 3.10-3.14 Lance provider release."""
    canonical_vane_version = _canonical_version(vane_version, "Vane version")
    if not Version(canonical_vane_version).is_devrelease:
        raise ReleaseValidationError(
            "Vane TestPyPI version must be a development release"
        )

    paths = sorted(directory.expanduser().resolve().glob("*.whl"))
    records = tuple(_read_wheel(path) for path in paths)
    normalized_name = canonicalize_name(PROVIDER_DISTRIBUTION)
    if len(records) != 5 or {
        canonicalize_name(record.distribution_name) for record in records
    } != {normalized_name}:
        raise ReleaseValidationError(
            "release directory must contain exactly five vane-extension-lance wheels"
        )
    if {record.interpreter for record in records} != EXPECTED_INTERPRETERS:
        raise ReleaseValidationError(
            "vane-extension-lance must contain exactly one wheel for each "
            "CPython 3.10 through 3.14"
        )
    provider_versions = {record.version for record in records}
    if len(provider_versions) != 1:
        raise ReleaseValidationError(
            "vane-extension-lance wheels do not share one immutable version"
        )

    expected_requirements = {canonicalize_name("vane-ai"): canonical_vane_version}
    for record in records:
        if _exact_requirements(record) != expected_requirements:
            raise ReleaseValidationError(
                f"{record.path.name} does not exactly require "
                f"vane-ai {canonical_vane_version}"
            )
    return next(iter(provider_versions))


def _request_json(url: str) -> tuple[int, object | None]:
    request = urllib.request.Request(
        url, headers={"User-Agent": "vane-lance-provider-release-validator/1"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, None
    except (OSError, ValueError) as error:
        raise ReleaseValidationError(
            f"TestPyPI query failed for {url}: {error}"
        ) from error


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_wheel_hashes(
    directory: Path, distribution_name: str, version: str
) -> dict[str, str]:
    normalized_name = canonicalize_name(distribution_name)
    canonical_version = _canonical_version(version, "provider version")
    paths = []
    for path in sorted(directory.expanduser().resolve().glob("*.whl")):
        filename_name, filename_version, _build, _tags = parse_wheel_filename(path.name)
        if (
            filename_name == normalized_name
            and str(filename_version) == canonical_version
        ):
            paths.append(path)
    records = tuple(_read_wheel(path) for path in paths)
    if (
        len(records) != 5
        or {record.interpreter for record in records} != EXPECTED_INTERPRETERS
    ):
        raise ReleaseValidationError(
            f"expected a complete five-wheel {distribution_name}=={canonical_version} set"
        )
    return {record.path.name: _sha256(record.path) for record in records}


def _indexed_wheel_hashes(
    document: object, distribution_name: str, version: str
) -> dict[str, str]:
    if not isinstance(document, dict) or not isinstance(document.get("urls"), list):
        raise ReleaseValidationError(
            f"TestPyPI returned malformed metadata for {distribution_name}=={version}"
        )
    actual: dict[str, str] = {}
    for item in document["urls"]:
        if not isinstance(item, dict):
            raise ReleaseValidationError("TestPyPI returned a malformed file entry")
        filename = item.get("filename")
        digests = item.get("digests")
        digest = digests.get("sha256") if isinstance(digests, dict) else None
        if (
            item.get("packagetype") != "bdist_wheel"
            or not isinstance(filename, str)
            or not isinstance(digest, str)
        ):
            raise ReleaseValidationError(
                f"TestPyPI returned a non-wheel or malformed file for "
                f"{distribution_name}=={version}"
            )
        if filename in actual:
            raise ReleaseValidationError(f"TestPyPI returned duplicate {filename}")
        actual[filename] = digest
    if not actual:
        raise ReleaseValidationError(
            f"TestPyPI returned an existing version without wheels: "
            f"{distribution_name}=={version}"
        )
    return actual


def require_index_publishable(directory: Path, version: str) -> None:
    """Allow a first publish or an exact, potentially partial, immutable rerun."""
    expected = _expected_wheel_hashes(directory, PROVIDER_DISTRIBUTION, version)
    encoded_name = urllib.parse.quote(PROVIDER_DISTRIBUTION, safe="")
    encoded_version = urllib.parse.quote(version, safe="")
    status, document = _request_json(
        f"{TESTPYPI_JSON_BASE}/{encoded_name}/{encoded_version}/json"
    )
    if status == 404:
        return
    if status != 200:
        raise ReleaseValidationError(
            f"expected absent or reusable TestPyPI version, received HTTP {status}"
        )
    actual = _indexed_wheel_hashes(document, PROVIDER_DISTRIBUTION, version)
    conflicts = {
        filename: digest
        for filename, digest in actual.items()
        if expected.get(filename) != digest
    }
    if conflicts:
        raise ReleaseValidationError(
            f"indexed wheel identities conflict for "
            f"{PROVIDER_DISTRIBUTION}=={version}: {conflicts}"
        )


def require_index_match(
    directory: Path,
    version: str,
    *,
    attempts: int,
    delay_seconds: int,
) -> None:
    """Wait for TestPyPI to expose exactly the locally assembled wheels."""
    canonical_version = _canonical_version(version, "provider version")
    expected = _expected_wheel_hashes(
        directory, PROVIDER_DISTRIBUTION, canonical_version
    )
    encoded_name = urllib.parse.quote(PROVIDER_DISTRIBUTION, safe="")
    encoded_version = urllib.parse.quote(canonical_version, safe="")
    url = f"{TESTPYPI_JSON_BASE}/{encoded_name}/{encoded_version}/json"
    last_problem = "the release was not indexed"
    for attempt in range(1, attempts + 1):
        try:
            status, document = _request_json(url)
            if status == 200:
                actual = _indexed_wheel_hashes(
                    document, PROVIDER_DISTRIBUTION, canonical_version
                )
                if actual == expected:
                    return
                last_problem = (
                    f"indexed wheel identities differ: expected={expected}, "
                    f"actual={actual}"
                )
            else:
                last_problem = f"TestPyPI returned HTTP {status}"
        except ReleaseValidationError as error:
            last_problem = str(error)
        if attempt != attempts:
            time.sleep(delay_seconds)
    raise ReleaseValidationError(last_problem)


def _write_github_output(path: Path, vane_version: str, lance_version: str) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(f"vane_version={vane_version}\n")
        output.write(f"lance_version={lance_version}\n")


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--directory", required=True, type=Path)
    validate.add_argument("--vane-version", required=True)
    validate.add_argument("--github-output", type=Path)
    validate.add_argument("--require-testpypi-publishable", action="store_true")
    verify = subparsers.add_parser("verify-index")
    verify.add_argument("--directory", required=True, type=Path)
    verify.add_argument("--version", required=True)
    verify.add_argument("--attempts", default=5, type=int)
    verify.add_argument("--delay-seconds", default=15, type=int)
    return parser.parse_args()


def main() -> int:
    arguments = _parse_arguments()
    try:
        if arguments.command == "validate":
            lance_version = validate_release(
                arguments.directory, arguments.vane_version
            )
            if arguments.require_testpypi_publishable:
                require_index_publishable(arguments.directory, lance_version)
            if arguments.github_output is not None:
                _write_github_output(
                    arguments.github_output, arguments.vane_version, lance_version
                )
            print(
                json.dumps(
                    {
                        "lance_version": lance_version,
                        "vane_version": arguments.vane_version,
                    },
                    sort_keys=True,
                )
            )
        else:
            if arguments.attempts <= 0 or arguments.delay_seconds < 0:
                raise ReleaseValidationError("invalid index retry settings")
            require_index_match(
                arguments.directory,
                arguments.version,
                attempts=arguments.attempts,
                delay_seconds=arguments.delay_seconds,
            )
    except (ReleaseValidationError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
