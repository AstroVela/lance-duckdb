#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Recover verified native inputs without compiling or accessing signing keys."""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import os
import re
import subprocess
from pathlib import Path

import yaml

REPOSITORY = "AstroVela/lance-duckdb"
WORKFLOW = ".github/workflows/VaneExtension.yml"
BUILDER = "scripts/build_vane_dynamic_wheel.py"
NATIVE_JOBS = (
    "vane-provider-prepare",
    "vane-provider-prepare-macos",
    "vane-provider-prepare-windows",
    "vane-provider-sign",
)
ARTIFACTS = {
    "linux_artifact_id": "vane-lance-unsigned-bundle",
    "macos_artifact_id": "vane-lance-prepare-macos",
    "windows_artifact_id": "vane-lance-prepare-windows",
    "signed_artifact_id": "vane-lance-signed-native",
}
SUCCESSFUL_JOBS = (
    "Verify the protected provider release inputs",
    "Prepare unsigned Lance native data without secrets",
    "vane-provider-prepare-macos / native",
    "vane-provider-prepare-windows / native",
    "Sign native data without build or package dependencies",
)


def _api(path: str) -> dict:
    return json.loads(
        subprocess.check_output(
            ["gh", "api", "--hostname", "github.com", f"repos/{REPOSITORY}/{path}"],
            text=True,
        )
    )


def _job_log(job_id: int) -> str:
    command = ["gh", "api", "--hostname", "github.com"]
    help_text = subprocess.check_output(["gh", "api", "--help"], text=True)
    if "--allow-escape-sequences" in help_text:
        # These raw logs are captured for parsing, never sent to a terminal.
        command.append("--allow-escape-sequences")
    command.append(f"repos/{REPOSITORY}/actions/jobs/{job_id}/logs")
    return subprocess.check_output(
        command,
        text=True,
    )


def _git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()


def require_same_native_inputs(root: Path, revision: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError("source run has an invalid commit identity")
    if _git(root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("native recovery requires a clean committed checkout")
    if subprocess.run(
        ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode:
        subprocess.run(
            [
                "git",
                "fetch",
                "--no-tags",
                "--depth=1",
                f"https://github.com/{REPOSITORY}.git",
                revision,
            ],
            cwd=root,
            check=True,
        )
    changed = _git(
        root, "diff", "--name-only", "--no-renames", revision, "HEAD"
    ).splitlines()
    packaging_only = {
        WORKFLOW,
        BUILDER,
        ".github/workflows/_vane_provider_package.yml",
        "scripts/reuse_vane_native.py",
        "docs/VANE_RELEASE.md",
    }
    unexpected = [
        name
        for name in changed
        if name not in packaging_only and not name.startswith("python/tests/")
    ]
    if unexpected:
        raise ValueError(f"native recovery inputs changed: {unexpected}")
    if BUILDER in changed:
        trees = []
        for commit in (revision, "HEAD"):
            tree = ast.parse(_git(root, "show", f"{commit}:{BUILDER}"))
            tree.body = [
                node
                for node in tree.body
                if not isinstance(node, ast.FunctionDef)
                or node.name != "_package_signed"
            ]
            trees.append(ast.dump(tree))
        if trees[0] != trees[1]:
            raise ValueError("native builder logic changed outside the package phase")
    if WORKFLOW in changed:
        old, new = (
            yaml.safe_load(_git(root, "show", f"{commit}:{WORKFLOW}"))
            for commit in (revision, "HEAD")
        )
        for field in ("env", "defaults"):
            if old.get(field) != new.get(field):
                raise ValueError(f"native workflow {field} changed")
        for name in NATIVE_JOBS:
            before, after = dict(old["jobs"][name]), dict(new["jobs"][name])
            # Recovery adds only a guard that skips these producers entirely.
            before.pop("if", None)
            after.pop("if", None)
            if before != after:
                raise ValueError(f"native workflow job changed: {name}")


def reuse_plan(root: Path, run_id: int) -> tuple[dict, dict]:
    if type(run_id) is not int or run_id <= 0:
        raise ValueError("native source run ID must be positive")
    run = _api(f"actions/runs/{run_id}")
    if (
        run.get("event") != "workflow_dispatch"
        or run.get("path") != WORKFLOW
        or run.get("head_branch") != "main_vane"
        or run.get("status") != "completed"
        or run.get("conclusion") not in {"success", "failure"}
        or run.get("repository", {}).get("full_name") != REPOSITORY
        or run.get("head_repository", {}).get("full_name") != REPOSITORY
    ):
        raise ValueError(
            "native recovery requires a completed official main_vane release run"
        )
    require_same_native_inputs(root, run["head_sha"])
    document = _api(f"actions/runs/{run_id}/jobs?per_page=100")
    jobs = document["jobs"]
    if document["total_count"] != len(jobs):
        raise ValueError("native recovery job list is incomplete")
    for name in SUCCESSFUL_JOBS:
        matches = [job for job in jobs if job["name"] == name]
        if len(matches) != 1 or matches[0].get("conclusion") != "success":
            raise ValueError(f"native source job did not succeed: {name}")
    preflight = next(job for job in jobs if job["name"] == SUCCESSFUL_JOBS[0])
    channels = re.findall(
        r"(?m)\bRELEASE_CHANNEL: ([^\r\n]+)", _job_log(preflight["id"])
    )
    if not channels or set(channels) != {"release"}:
        raise ValueError("native recovery requires the production signing profile")
    publishers = [
        job for job in jobs if job["name"] == "Publish Lance provider to TestPyPI"
    ]
    if len(publishers) != 1 or publishers[0].get("conclusion") != "skipped":
        raise ValueError("cannot repackage a source run that reached index publication")
    document = _api(f"actions/runs/{run_id}/artifacts?per_page=100")
    artifacts = document["artifacts"]
    if document["total_count"] != len(artifacts):
        raise ValueError("native recovery artifact list is incomplete")
    outputs = {"native_run_id": str(run_id), "native_source_revision": run["head_sha"]}
    retained = []
    for field, name in ARTIFACTS.items():
        matches = [artifact for artifact in artifacts if artifact["name"] == name]
        if len(matches) != 1:
            raise ValueError(f"native source artifact is missing or ambiguous: {name}")
        artifact = matches[0]
        origin = artifact.get("workflow_run", {})
        if (
            artifact.get("expired") is not False
            or type(artifact.get("id")) is not int
            or artifact["id"] <= 0
            or type(artifact.get("size_in_bytes")) is not int
            or artifact["size_in_bytes"] <= 0
            or not isinstance(artifact.get("digest"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", artifact.get("digest", "")) is None
            or origin.get("id") != run_id
            or origin.get("head_sha") != run["head_sha"]
        ):
            raise ValueError(f"native source artifact has an invalid identity: {name}")
        outputs[field] = str(artifact["id"])
        retained.append(
            {key: artifact[key] for key in ("id", "name", "digest", "size_in_bytes")}
        )
    if len({outputs[field] for field in ARTIFACTS}) != len(ARTIFACTS):
        raise ValueError("native source artifact IDs are not unique")
    return outputs, {
        "source_run_id": run_id,
        "source_commit": run["head_sha"],
        "signing_profile": "production",
        "artifacts": retained,
    }


def require_unpublished_candidate(tools, root: Path, vane_version: str) -> str:
    tools.validate_vane_version(vane_version, "release")
    config = tools.load_config(root / "vane-provider-release.toml")
    provider = config.provider("lance")
    version = tools.provider_version(vane_version, provider.release_number)
    for index, base in tools.INDEX_JSON_BASES.items():
        status, _ = tools._request_json(
            f"{base}/{provider.distribution}/{version}/json"
        )
        if status != 404:
            raise ValueError(
                f"native recovery requires an unpublished candidate on {index}: {version}"
            )
        tools._require_increasing_release(provider, version, index)
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, type=int)
    parser.add_argument("--extension-root", required=True, type=Path)
    parser.add_argument("--vane-version", required=True)
    parser.add_argument("--github-output", required=True, type=Path)
    parser.add_argument("--proof-file", required=True, type=Path)
    arguments = parser.parse_args()
    root = arguments.extension_root.resolve()
    spec = importlib.util.spec_from_file_location(
        "_reuse_preflight", root / "scripts/vane_release_preflight.py"
    )
    preflight = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(preflight)
    preflight.require_publish_dispatch(dict(os.environ))
    tools = preflight.load_release_tools(root)
    require_unpublished_candidate(tools, root, arguments.vane_version)
    outputs, proof = reuse_plan(root, arguments.run_id)
    proof["workflow_commit"] = _git(root, "rev-parse", "HEAD")
    arguments.proof_file.write_text(json.dumps(proof, indent=2) + "\n")
    with arguments.github_output.open("a", encoding="utf-8") as output:
        for key, value in outputs.items():
            output.write(f"{key}={value}\n")
    print(json.dumps(proof))


if __name__ == "__main__":
    main()
