#!/usr/bin/env python3
"""Prepare a Lance dependency update for lance-duckdb."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Sequence

try:
    from check_lance_release import parse_semver
except ModuleNotFoundError:
    # Supports importing as ci.update_lance_dependency from tests or ad hoc checks.
    from ci.check_lance_release import parse_semver  # type: ignore


LANCE_CRATES = (
    "lance",
    "lance-arrow",
    "lance-core",
    "lance-index",
    "lance-linalg",
    "lance-namespace",
    "lance-namespace-impls",
    "lance-table",
)


def normalize_version(raw: str) -> str:
    value = raw.strip()
    value = value.removeprefix("refs/tags/")
    value = value.removeprefix("v")
    try:
        parse_semver(value)
    except ValueError:
        raise ValueError(f"Unsupported Lance version or tag: {raw}") from None
    return value


def normalized_tag(version: str) -> str:
    return f"v{version}"


def branch_name(version: str) -> str:
    suffix = re.sub(r"[^a-zA-Z0-9]+", "-", version).strip("-")
    suffix = re.sub(r"-+", "-", suffix)
    return f"codex/update-lance-{suffix}"


def metadata_for(version: str) -> dict[str, str]:
    message = f"chore: update lance dependency to v{version}"
    return {
        "version": version,
        "tag": normalized_tag(version),
        "branch_name": branch_name(version),
        "commit_message": message,
        "pr_title": message,
    }


def dependency_line_pattern(crate_name: str) -> re.Pattern[str]:
    escaped = re.escape(crate_name)
    return re.compile(rf"^(\s*{escaped}\s*=\s*)(.+?)(\s*(?:#.*)?)$")


def replace_dependency_version(spec: str, version: str, crate_name: str) -> str:
    quoted_pattern = re.compile(r'^"[^"]+"$')
    if quoted_pattern.match(spec):
        return f'"{version}"'

    if spec.startswith("{") and spec.endswith("}"):
        updated, count = re.subn(
            r'(version\s*=\s*)"[^"]+"', rf'\g<1>"{version}"', spec, count=1
        )
        if count != 1:
            raise RuntimeError(
                f"Expected a version field for dependency {crate_name!r}"
            )
        return updated

    raise RuntimeError(
        f"Unsupported dependency specification for {crate_name!r}: {spec}"
    )


def update_lance_versions_in_dependencies(lines: list[str], version: str) -> set[str]:
    updated_crates: set[str] = set()
    in_dependencies = False

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_dependencies = stripped == "[dependencies]"
            continue
        if not in_dependencies or not stripped or stripped.startswith("#"):
            continue

        for crate_name in LANCE_CRATES:
            match = dependency_line_pattern(crate_name).match(line)
            if not match:
                continue
            prefix, spec, suffix = match.groups()
            lines[index] = (
                f"{prefix}{replace_dependency_version(spec.strip(), version, crate_name)}{suffix}"
            )
            updated_crates.add(crate_name)
            break

    missing = set(LANCE_CRATES) - updated_crates
    if missing:
        raise RuntimeError(
            f"Failed to locate Lance dependencies in Cargo.toml: {', '.join(sorted(missing))}"
        )
    return updated_crates


def remove_lance_patch_entries(lines: list[str]) -> None:
    in_patch_section = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_patch_section = stripped == "[patch.crates-io]"
            continue
        if not in_patch_section:
            continue
        for crate_name in LANCE_CRATES:
            if dependency_line_pattern(crate_name).match(line):
                lines[index] = ""
                break


def update_cargo_toml(repo_root: Path, version: str) -> None:
    cargo_toml = repo_root / "Cargo.toml"
    original = cargo_toml.read_text(encoding="utf-8")
    lines = original.splitlines()
    update_lance_versions_in_dependencies(lines, version)
    remove_lance_patch_entries(lines)
    updated = "\n".join(lines) + ("\n" if original.endswith("\n") else "")
    cargo_toml.write_text(updated, encoding="utf-8")


def run_command(cmd: Sequence[str], *, cwd: Path) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def locked_lance_versions(repo_root: Path) -> dict[str, str]:
    versions: dict[str, set[str]] = {crate_name: set() for crate_name in LANCE_CRATES}
    current_name: str | None = None
    for line in (repo_root / "Cargo.lock").read_text(encoding="utf-8").splitlines():
        if line == "[[package]]":
            current_name = None
            continue
        name_match = re.fullmatch(r'name = "([^"]+)"', line)
        if name_match:
            current_name = name_match.group(1)
            continue
        version_match = re.fullmatch(r'version = "([^"]+)"', line)
        if version_match and current_name in versions:
            versions[current_name].add(version_match.group(1))

    ambiguous = {
        crate_name: sorted(crate_versions)
        for crate_name, crate_versions in versions.items()
        if len(crate_versions) != 1
    }
    if ambiguous:
        raise RuntimeError(f"Could not resolve unique Lance lock versions: {ambiguous}")
    return {
        crate_name: crate_versions.pop()
        for crate_name, crate_versions in versions.items()
    }


def update_cargo_lock(repo_root: Path, version: str) -> None:
    # Lance crates use exact versions for their internal dependencies. Update the
    # directly declared crates as one transaction, then pin that resolved family
    # to the requested patch release. Including the currently locked versions in
    # the second command avoids an intermediate exact-version conflict.
    run_command(["cargo", "update", *LANCE_CRATES], cwd=repo_root)
    resolved = locked_lance_versions(repo_root)
    if any(crate_version != version for crate_version in resolved.values()):
        precise_specs = [
            f"{crate_name}@{resolved[crate_name]}" for crate_name in LANCE_CRATES
        ]
        run_command(
            ["cargo", "update", *precise_specs, "--precise", version], cwd=repo_root
        )

    final = locked_lance_versions(repo_root)
    if any(crate_version != version for crate_version in final.values()):
        raise RuntimeError(
            f"Cargo.lock did not resolve all direct Lance crates to {version}: {final}"
        )


def write_github_outputs(path: str | None, payload: dict[str, str]) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in payload.items():
            output.write(f"{key}={value}\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tag_or_version",
        help="Lance tag or version, for example refs/tags/v7.2.0-beta.1 or 7.2.0",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Path to the lance-duckdb repository root",
    )
    parser.add_argument(
        "--github-output",
        default=None,
        help="Optional GitHub Actions output file to receive metadata fields",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only print derived metadata; do not modify dependency files",
    )
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    version = normalize_version(args.tag_or_version)
    payload = metadata_for(version)

    if not args.metadata_only:
        update_cargo_toml(repo_root, version)
        update_cargo_lock(repo_root, version)

    write_github_outputs(args.github_output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
