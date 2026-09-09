#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Reject unpublished production runtimes before native builds or key access."""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path

from packaging.utils import parse_wheel_filename

PRODUCTION_KEY_REVISION = "033b549afcb498633fd6669b26c054c00363004e"


def load_release_tools(extension_root: Path):
    path = extension_root / "vane-extension-ci-tools/scripts/vane_provider_release.py"
    spec = importlib.util.spec_from_file_location("_lance_release_tools", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load pinned release tools: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def require_publish_dispatch(environment: dict[str, str]) -> None:
    if (
        environment.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
        or environment.get("GITHUB_REPOSITORY") != "AstroVela/lance-duckdb"
        or environment.get("GITHUB_REF") != "refs/heads/main_vane"
        or environment.get("GITHUB_REF_PROTECTED") != "true"
    ):
        raise ValueError(
            "provider publication requires workflow_dispatch on the protected "
            "AstroVela/lance-duckdb main_vane branch"
        )


def require_indexed_runtime(release, version: str, channel: str, config: Path) -> None:
    index = "pypi" if channel == "release" else "testpypi"
    status, document = release._request_json(
        f"{release.INDEX_JSON_BASES[index]}/vane-ai/{version}/json"
    )
    if status != 200 or not isinstance(document, dict):
        raise ValueError(f"exact Vane runtime {version} is not published on {index}")
    files = document.get("urls")
    if not isinstance(files, list):
        raise ValueError("runtime index returned an invalid file list")
    expected = release.load_config(config)
    found = set()
    for record in files:
        if not isinstance(record, dict):
            raise ValueError("runtime index returned an invalid file record")
        if record.get("packagetype") != "bdist_wheel":
            continue
        distribution, wheel_version, _, tags = parse_wheel_filename(record["filename"])
        if distribution != "vane-ai" or str(wheel_version) != version:
            raise ValueError("runtime index returned an unexpected wheel identity")
        if record.get("yanked", False) is not False:
            continue
        digest = record.get("digests", {}).get("sha256", "")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("runtime index returned an invalid wheel digest")
        found.update(
            (tag.interpreter, tag.platform)
            for tag in tags
            if tag.abi == tag.interpreter
        )
    required = {
        (interpreter, platform)
        for interpreter in expected.interpreters
        for platform in expected.platforms
    }
    if not required <= found:
        raise ValueError(
            f"exact Vane runtime is missing indexed wheels: {required - found}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", choices=("testpypi-dev", "release"), required=True)
    parser.add_argument("--extension-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--vane-source", type=Path, required=True)
    parser.add_argument("--ci-tools-version", required=True)
    parser.add_argument("--github-output", type=Path, required=True)
    arguments = parser.parse_args(argv)
    require_publish_dispatch(dict(os.environ))
    release = load_release_tools(arguments.extension_root.resolve())
    release.verify_sources(
        arguments.manifest,
        arguments.extension_root,
        arguments.vane_source,
        arguments.ci_tools_version,
    )
    version = subprocess.check_output(
        [sys.executable, "-m", "setuptools_scm"],
        cwd=arguments.vane_source,
        text=True,
        env={
            name: value
            for name, value in os.environ.items()
            if name not in {"VANE_VERSION_BRANCH", "GITHUB_REF_NAME", "GITHUB_BASE_REF"}
            and not name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION")
        },
    ).strip()
    release.validate_vane_version(version, arguments.channel)
    if arguments.channel == "release":
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", PRODUCTION_KEY_REVISION, "HEAD"],
            cwd=arguments.vane_source,
            check=True,
        )
    require_indexed_runtime(
        release,
        version,
        arguments.channel,
        arguments.extension_root / "vane-provider-release.toml",
    )
    with arguments.github_output.open("a", encoding="utf-8") as output:
        output.write(f"vane_version={version}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
