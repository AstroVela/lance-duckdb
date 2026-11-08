# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import copy
import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "reuse_vane_native", ROOT / "scripts/reuse_vane_native.py"
)
reuse = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reuse)


def _git(root, *args):
    return subprocess.check_output(
        ["git", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
        cwd=root,
        text=True,
    ).strip()


def _write(root, name, value):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


def _commit(root):
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "fixture")
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture
def source(tmp_path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.name", "Release test")
    _git(tmp_path, "config", "user.email", "release-test@example.com")
    _write(
        tmp_path,
        reuse.BUILDER,
        "def _package_signed():\n    return 'old'\n\ndef native_build():\n    return 'fixed native inputs'\n",
    )
    workflow = {
        "jobs": {
            name: {"runs-on": "native", "steps": [{"run": "native inputs"}]}
            for name in reuse.NATIVE_JOBS
        }
    }
    _write(tmp_path, reuse.WORKFLOW, yaml.safe_dump(workflow))
    _write(tmp_path, "src/native.cpp", "native source\n")
    return tmp_path, _commit(tmp_path)


def test_recovery_allows_only_packaging_changes_and_producer_skip_guards(source):
    root, revision = source
    _write(
        root,
        reuse.BUILDER,
        (root / reuse.BUILDER)
        .read_text()
        .replace("return 'old'", "return 'absolute signed path'"),
    )
    workflow = yaml.safe_load((root / reuse.WORKFLOW).read_text())
    for job in workflow["jobs"].values():
        job["if"] = "inputs.native-run-id == ''"
    workflow["jobs"]["package"] = {"run": "package only"}
    _write(root, reuse.WORKFLOW, yaml.safe_dump(workflow))
    _write(root, "scripts/reuse_vane_native.py", "# recovery orchestration\n")
    _write(root, "python/tests/test_recovery.py", "# regression\n")
    _commit(root)
    reuse.require_same_native_inputs(root, revision)


@pytest.mark.parametrize(
    "path",
    [
        "src/native.cpp",
        "Cargo.lock",
        "vane-extension-release.toml",
        "vane-provider-release.toml",
        ".github/workflows/_vane_provider_native.yml",
    ],
)
def test_recovery_rejects_native_source_dependency_and_manifest_changes(source, path):
    root, revision = source
    _write(root, path, "changed native input\n")
    _commit(root)
    with pytest.raises(ValueError, match="native recovery inputs changed"):
        reuse.require_same_native_inputs(root, revision)


def test_recovery_rejects_builder_changes_outside_package_phase(source):
    root, revision = source
    _write(
        root,
        reuse.BUILDER,
        (root / reuse.BUILDER)
        .read_text()
        .replace("fixed native inputs", "different native build"),
    )
    _commit(root)
    with pytest.raises(ValueError, match="native builder logic changed"):
        reuse.require_same_native_inputs(root, revision)


@pytest.mark.parametrize("global_env", [False, True])
def test_recovery_rejects_native_workflow_environment_changes(source, global_env):
    root, revision = source
    workflow = yaml.safe_load((root / reuse.WORKFLOW).read_text())
    owner = workflow if global_env else workflow["jobs"][reuse.NATIVE_JOBS[0]]
    owner["env"] = {"CMAKE_ARGS": "changed flags"}
    _write(root, reuse.WORKFLOW, yaml.safe_dump(workflow))
    _commit(root)
    with pytest.raises(ValueError, match="native workflow"):
        reuse.require_same_native_inputs(root, revision)


def test_recovery_rejects_dirty_native_inputs(source):
    root, revision = source
    _write(root, "src/native.cpp", "uncommitted change\n")
    with pytest.raises(ValueError, match="clean committed checkout"):
        reuse.require_same_native_inputs(root, revision)


@pytest.fixture
def documents(monkeypatch):
    run_id = 42449970361
    revision = "a" * 40
    run = {
        "event": "workflow_dispatch",
        "path": reuse.WORKFLOW,
        "head_branch": "main_vane",
        "head_sha": revision,
        "status": "completed",
        "conclusion": "failure",
        "repository": {"full_name": reuse.REPOSITORY},
        "head_repository": {"full_name": reuse.REPOSITORY},
    }
    jobs = [
        {"id": 1000 + index, "name": name, "conclusion": "success"}
        for index, name in enumerate(reuse.SUCCESSFUL_JOBS)
    ]
    jobs.append({"name": "Publish Lance provider to TestPyPI", "conclusion": "skipped"})
    artifacts = [
        {
            "id": 100 + index,
            "name": name,
            "digest": "sha256:" + "b" * 64,
            "size_in_bytes": 4096,
            "expired": False,
            "workflow_run": {"id": run_id, "head_sha": revision},
        }
        for index, name in enumerate(reuse.ARTIFACTS.values())
    ]
    data = {
        "run": run,
        "jobs": {"total_count": len(jobs), "jobs": jobs},
        "artifacts": {"total_count": len(artifacts), "artifacts": artifacts},
    }

    def api(path):
        return copy.deepcopy(
            data[
                (
                    "jobs"
                    if "/jobs?" in path
                    else "artifacts" if "/artifacts?" in path else "run"
                )
            ]
        )

    monkeypatch.setattr(reuse, "_api", api)
    monkeypatch.setattr(
        reuse,
        "_job_log",
        lambda _id: "2026-11-08T05:14:00Z   RELEASE_CHANNEL: release\n",
    )
    monkeypatch.setattr(reuse, "require_same_native_inputs", Mock())
    return data, run_id


def test_recovery_retains_original_run_artifact_ids_and_digests(documents, tmp_path):
    data, run_id = documents
    outputs, proof = reuse.reuse_plan(tmp_path, run_id)
    assert outputs["native_run_id"] == str(run_id)
    assert [outputs[field] for field in reuse.ARTIFACTS] == [
        str(item["id"]) for item in data["artifacts"]["artifacts"]
    ]
    assert proof["source_commit"] == data["run"]["head_sha"]
    assert [item["digest"] for item in proof["artifacts"]] == [
        item["digest"] for item in data["artifacts"]["artifacts"]
    ]
    reuse.require_same_native_inputs.assert_called_once_with(
        tmp_path, data["run"]["head_sha"]
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("event", "pull_request"),
        ("head_branch", "topic"),
        ("path", "other.yml"),
        ("status", "in_progress"),
        ("conclusion", "cancelled"),
        ("head_repository", {"full_name": "untrusted/fork"}),
    ],
)
def test_recovery_rejects_ineligible_runs(documents, tmp_path, field, value):
    data, run_id = documents
    data["run"][field] = value
    with pytest.raises(ValueError, match="official main_vane release run"):
        reuse.reuse_plan(tmp_path, run_id)


@pytest.mark.parametrize("job_name", reuse.SUCCESSFUL_JOBS)
def test_recovery_requires_every_original_producer_success(
    documents, tmp_path, job_name
):
    data, run_id = documents
    next(job for job in data["jobs"]["jobs"] if job["name"] == job_name)[
        "conclusion"
    ] = "failure"
    with pytest.raises(ValueError, match="source job did not succeed"):
        reuse.reuse_plan(tmp_path, run_id)


@pytest.mark.parametrize("conclusion", ["success", "failure"])
def test_recovery_never_repackages_a_run_that_reached_publication(
    documents, tmp_path, conclusion
):
    data, run_id = documents
    data["jobs"]["jobs"][-1]["conclusion"] = conclusion
    with pytest.raises(ValueError, match="reached index publication"):
        reuse.reuse_plan(tmp_path, run_id)


@pytest.mark.parametrize(
    "field,value",
    [
        ("expired", True),
        ("digest", "sha256:wrong"),
        ("id", True),
        ("workflow_run", {"id": 1, "head_sha": "a" * 40}),
    ],
)
def test_recovery_rejects_stale_or_replaced_artifact_identities(
    documents, tmp_path, field, value
):
    data, run_id = documents
    data["artifacts"]["artifacts"][0][field] = value
    with pytest.raises(ValueError, match="invalid identity"):
        reuse.reuse_plan(tmp_path, run_id)


def test_recovery_rejects_ambiguous_artifacts(documents, tmp_path):
    data, run_id = documents
    data["artifacts"]["artifacts"].append(
        copy.deepcopy(data["artifacts"]["artifacts"][0])
    )
    data["artifacts"]["total_count"] += 1
    with pytest.raises(ValueError, match="missing or ambiguous"):
        reuse.reuse_plan(tmp_path, run_id)


@pytest.mark.parametrize("channel", ["testpypi-dev", ""])
def test_recovery_rejects_development_or_unknown_signing_profiles(
    documents, tmp_path, monkeypatch, channel
):
    _, run_id = documents
    monkeypatch.setattr(reuse, "_job_log", lambda _id: f"RELEASE_CHANNEL: {channel}\n")
    with pytest.raises(ValueError, match="production signing profile"):
        reuse.reuse_plan(tmp_path, run_id)


@pytest.mark.parametrize(
    "statuses", [(404, 404), (200, 404), (404, 200), (403, 404), (404, 500)]
)
def test_recovery_requires_candidate_absence_on_both_indexes(tmp_path, statuses):
    provider = SimpleNamespace(distribution="vane-extension-lance", release_number=1)
    tools = SimpleNamespace(
        validate_vane_version=Mock(),
        load_config=Mock(
            return_value=SimpleNamespace(provider=Mock(return_value=provider))
        ),
        provider_version=Mock(return_value="0.3.0.1"),
        INDEX_JSON_BASES={
            "testpypi": "https://test.pypi.org/pypi",
            "pypi": "https://pypi.org/pypi",
        },
        _request_json=Mock(side_effect=[(status, {}) for status in statuses]),
        _require_increasing_release=Mock(),
    )
    if statuses == (404, 404):
        assert (
            reuse.require_unpublished_candidate(tools, tmp_path, "0.3.0") == "0.3.0.1"
        )
        assert tools._request_json.call_count == 2
        assert tools._require_increasing_release.call_count == 2
    else:
        with pytest.raises(ValueError, match="unpublished candidate"):
            reuse.require_unpublished_candidate(tools, tmp_path, "0.3.0")


@pytest.mark.parametrize("collection", ["jobs", "artifacts"])
def test_recovery_rejects_incomplete_source_metadata(documents, tmp_path, collection):
    data, run_id = documents
    data[collection]["total_count"] += 1
    with pytest.raises(ValueError, match="list is incomplete"):
        reuse.reuse_plan(tmp_path, run_id)


def test_recovery_workflow_skips_all_compilation_and_signing_jobs():
    workflow = yaml.safe_load((ROOT / reuse.WORKFLOW).read_text())
    jobs = workflow["jobs"]
    for name in reuse.NATIVE_JOBS:
        assert jobs[name]["if"] == "inputs.native-run-id == ''"
    for name in (
        "vane-testpypi-wheels",
        "vane-testpypi-wheels-macos",
        "vane-testpypi-wheels-windows",
    ):
        condition = jobs[name]["if"]
        assert "!cancelled()" in condition
        assert "needs.provider-release-preflight.result == 'success'" in condition
        assert "inputs.native-run-id != ''" in condition
        assert jobs[name]["permissions"] == {"contents": "read", "actions": "read"}
    gate = jobs["provider-release-preflight"]["steps"][0]
    assert 'test "$RELEASE_CHANNEL" = release' in gate["run"]
    assert 'test "$TESTPYPI_ONLY" = true' in gate["run"]
    for platform in ("macos", "windows"):
        settings = jobs[f"vane-testpypi-wheels-{platform}"]["with"]
        assert (
            settings["artifact-run-id"]
            == "${{ needs.provider-release-preflight.outputs.native_run_id }}"
        )
        assert f"outputs.{platform}_artifact_id ||" in settings["prepared-id"]
        assert "outputs.signed_artifact_id ||" in settings["signed-id"]
    package = yaml.safe_load(
        (ROOT / ".github/workflows/_vane_provider_package.yml").read_text()
    )["jobs"]["package"]
    for step in package["steps"]:
        if step.get("uses", "").startswith("actions/download-artifact@"):
            assert (
                step["with"]["run-id"]
                == "${{ inputs.artifact-run-id || github.run_id }}"
            )
            assert step["with"]["github-token"] == "${{ github.token }}"
            assert step["with"]["digest-mismatch"] == "error"
