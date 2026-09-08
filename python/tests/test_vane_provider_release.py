# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Consumer checks for Lance's pinned shared provider release gate."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "vane-extension-ci-tools"
CONFIG = ROOT / "vane-provider-release.toml"
SCRIPT = TOOLS / "scripts/vane_provider_release.py"
SPEC = importlib.util.spec_from_file_location("vane_lance_provider_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release
SPEC.loader.exec_module(release)

VANE_VERSION = "0.2.0.dev612"
PROVIDER_VERSION = "0.2.0.0.612.1"
INTERPRETERS = ("cp310", "cp311", "cp312", "cp313", "cp314")
PLATFORM = "manylinux_2_28_x86_64"


def _tools_revision() -> str:
    # The proposed index also lets these consumer checks run before the first
    # commit adding the submodule. Production gates use the committed HEAD.
    entry = subprocess.check_output(
        [
            "git",
            "-C",
            str(ROOT),
            "ls-files",
            "--stage",
            "--",
            "vane-extension-ci-tools",
        ],
        text=True,
    )
    mode, revision, stage, path = entry.split()
    assert (mode, stage, path) == ("160000", "0", "vane-extension-ci-tools")
    return revision


def _write_wheel(
    directory: Path, interpreter: str, *, requirement: str = f"vane-ai==={VANE_VERSION}"
) -> Path:
    distribution = "vane_extension_lance"
    path = (
        directory
        / f"{distribution}-{PROVIDER_VERSION}-{interpreter}-none-{PLATFORM}.whl"
    )
    metadata = (
        "Metadata-Version: 2.4\n"
        "Name: vane-extension-lance\n"
        f"Version: {PROVIDER_VERSION}\n"
        f"Requires-Dist: {requirement}\n\n"
    )
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(
            f"{distribution}-{PROVIDER_VERSION}.dist-info/METADATA", metadata
        )
    return path


def _write_release(directory: Path) -> tuple[Path, ...]:
    return tuple(_write_wheel(directory, interpreter) for interpreter in INTERPRETERS)


def _source_arguments(directory: Path) -> list[str]:
    return [
        "--manifest",
        str(ROOT / "vane-extension.toml"),
        "--extension-root",
        str(ROOT),
        "--vane-source",
        str(directory / "vane"),
        "--ci-tools-version",
        _tools_revision(),
        "--config",
        str(CONFIG),
        "--directory",
        str(directory),
    ]


def test_config_declares_the_built_lance_matrix() -> None:
    config = release.load_config(CONFIG)
    assert config.interpreters == INTERPRETERS
    assert config.platforms == (PLATFORM,)
    assert config.max_wheel_bytes == 100_000_000
    assert config.providers == (release.Provider("lance", "vane-extension-lance", ()),)
    native = release._load_source_tools()
    manifest = native.load_manifest(ROOT / "vane-extension.toml", ROOT)
    assert manifest.name == "lance"
    assert manifest.vane_revision == "472df75ab51fd3eac2642f6646545075549e5921"


def test_shared_validate_preserves_workflow_outputs(
    tmp_path, monkeypatch, capsys
) -> None:
    _write_release(tmp_path)
    outputs = tmp_path / "github-output"
    source_gate = Mock()
    monkeypatch.setattr(release, "verify_sources", source_gate)
    monkeypatch.setattr(release, "_request_json", lambda _url: (404, None))
    command = [
        "validate",
        *_source_arguments(tmp_path),
        "--vane-version",
        VANE_VERSION,
        "--channel",
        "testpypi-dev",
        "--require-publishable-on",
        "testpypi",
        "--github-output",
        str(outputs),
    ]
    assert release.main(command) == 0
    source_gate.assert_called_once_with(
        ROOT / "vane-extension.toml", ROOT, tmp_path / "vane", _tools_revision()
    )
    expected = {"vane_version": VANE_VERSION, "lance_version": PROVIDER_VERSION}
    assert json.loads(capsys.readouterr().out) == expected
    assert (
        dict(line.split("=", 1) for line in outputs.read_text().splitlines())
        == expected
    )

    _write_wheel(tmp_path, "cp314", requirement="vane-ai>=0.2")
    assert release.main(command) == 2
    assert "exact ===" in capsys.readouterr().err


def test_shared_verify_index_uses_lance_provider(tmp_path, monkeypatch) -> None:
    paths = _write_release(tmp_path)
    document = {
        "urls": [
            {
                "filename": path.name,
                "packagetype": "bdist_wheel",
                "digests": {"sha256": release._sha256(path)},
            }
            for path in paths
        ]
    }
    source_gate = Mock()
    request = Mock(return_value=(200, document))
    monkeypatch.setattr(release, "verify_sources", source_gate)
    monkeypatch.setattr(release, "_request_json", request)
    assert (
        release.main(
            [
                "verify-index",
                *_source_arguments(tmp_path),
                "--index",
                "testpypi",
                "--provider",
                "lance",
                "--version",
                PROVIDER_VERSION,
                "--attempts",
                "1",
                "--delay-seconds",
                "0",
            ]
        )
        == 0
    )
    source_gate.assert_called_once_with(
        ROOT / "vane-extension.toml", ROOT, tmp_path / "vane", _tools_revision()
    )
    request.assert_called_once_with(
        f"https://test.pypi.org/pypi/vane-extension-lance/{PROVIDER_VERSION}/json"
    )


def test_tools_submodule_and_make_share_the_tracked_gitlink() -> None:
    actual = subprocess.check_output(
        ["git", "-C", str(TOOLS), "rev-parse", "HEAD"], text=True
    ).strip()
    assert actual == _tools_revision()
    url = subprocess.check_output(
        [
            "git",
            "-C",
            str(ROOT),
            "config",
            "--file",
            ".gitmodules",
            "submodule.vane-extension-ci-tools.url",
        ],
        text=True,
    ).strip()
    assert url == "https://github.com/AstroVela/vane-extension-ci-tools.git"
    makefile = (ROOT / "makefiles/vane_extension.Makefile").read_text()
    assert 'rev-parse "HEAD:vane-extension-ci-tools"' in makefile
    assert "override _VANE_EXPECTED_CI_TOOLS_VERSION :=" in makefile
    assert (
        "override VANE_CI_TOOLS_DIR := $(VANE_EXTENSION_ROOT)/vane-extension-ci-tools"
        in makefile
    )
    assert '--expected-sha "$(_VANE_EXPECTED_CI_TOOLS_VERSION)"' in makefile
    assert "vane_ci_tools:" not in makefile
    assert "VANE_CI_TOOLS_REPOSITORY" not in makefile


def test_workflow_uses_only_the_committed_tools_submodule() -> None:
    contents = (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    assert "repository: AstroVela/vane-extension-ci-tools" not in contents
    assert "vane-ci-tools" not in contents
    assert "scripts/validate_vane_provider_release.py" not in contents
    workflow = yaml.safe_load(contents)
    consumers = set()
    for name, job in workflow["jobs"].items():
        initialized = False
        for step in job["steps"]:
            if step.get("with", {}).get("submodules") == "recursive":
                initialized = True
            command = step.get("run", "")
            if "git submodule update --init vane-extension-ci-tools" in command:
                initialized = True
            if (
                "vane-extension-ci-tools/scripts/" not in command
                and "requirements-release.txt" not in command
            ):
                continue
            assert initialized, name
            consumers.add(name)
    assert consumers == {
        "preflight",
        "vane-provider-release-tooling",
        "vane-native-build",
        "vane-wheel-build",
        "vane-dynamic-provider-build",
        "vane-testpypi-wheels",
        "provider-release-preflight",
        "assemble-testpypi-lance",
        "verify-testpypi-lance",
        "publish-pypi-lance",
    }


def test_release_workflow_supplies_exact_sources_and_shared_config() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    commands = {}
    for name, job in workflow["jobs"].items():
        steps = job["steps"]
        for index, step in enumerate(steps):
            command = step.get("run", "")
            if (
                "vane-extension-ci-tools/scripts/vane_provider_release.py"
                not in command
            ):
                continue
            commands[name] = command
            for argument in (
                "--manifest",
                "--extension-root",
                "--vane-source",
                "--ci-tools-version",
                "HEAD:vane-extension-ci-tools",
                "--config",
                "vane-provider-release.toml",
            ):
                assert argument in command
            checkouts = [step.get("with", {}) for step in steps[:index]]
            assert any(
                checkout.get("repository")
                == "${{ steps.manifest.outputs.vane_repository }}"
                and checkout.get("ref") == "${{ steps.manifest.outputs.vane_revision }}"
                and checkout.get("path") == "vane"
                and checkout.get("persist-credentials") is False
                for checkout in checkouts
            )
    assert set(commands) == {
        "vane-testpypi-wheels",
        "assemble-testpypi-lance",
        "verify-testpypi-lance",
        "publish-pypi-lance",
    }
    assert "--provider lance" in commands["verify-testpypi-lance"]
    for job in ("vane-testpypi-wheels", "assemble-testpypi-lance"):
        assert "--channel" in commands[job]
        assert "--require-publishable-on testpypi" in commands[job]


def test_production_manifest_does_not_change_the_development_runtime() -> None:
    development = tomllib.loads((ROOT / "vane-extension.toml").read_text())
    production = tomllib.loads((ROOT / "vane-extension-release.toml").read_text())
    assert production["vane"].pop("revision") == (
        "033b549afcb498633fd6669b26c054c00363004e"
    )
    assert development["vane"].pop("revision") == (
        "472df75ab51fd3eac2642f6646545075549e5921"
    )
    assert production == development


def test_production_promotion_requires_the_exact_staged_lance_matrix(
    tmp_path, monkeypatch
) -> None:
    paths = _write_release(tmp_path)
    for path in paths:
        # Keep the existing immutable provider version shape; only the exact
        # runtime requirement distinguishes this synthetic production candidate.
        _write_wheel(tmp_path, path.name.split("-")[2], requirement="vane-ai===0.2.0")
    document = {
        "urls": [
            {
                "filename": path.name,
                "packagetype": "bdist_wheel",
                "digests": {"sha256": release._sha256(path)},
            }
            for path in paths
        ]
    }
    monkeypatch.setattr(release, "verify_sources", Mock())
    request = Mock(side_effect=[(200, document), (404, None)])
    monkeypatch.setattr(release, "_request_json", request)
    command = [
        "verify-promotion",
        *_source_arguments(tmp_path),
        "--vane-version",
        "0.2.0",
        "--attempts",
        "1",
        "--delay-seconds",
        "0",
    ]
    assert release.main(command) == 0
    assert [call.args[0] for call in request.call_args_list] == [
        f"https://test.pypi.org/pypi/vane-extension-lance/{PROVIDER_VERSION}/json",
        f"https://pypi.org/pypi/vane-extension-lance/{PROVIDER_VERSION}/json",
    ]
    document["urls"][0]["digests"]["sha256"] = "0" * 64
    request.side_effect = [(200, document)]
    assert release.main(command) == 2


def test_production_workflow_has_no_shortcut_around_qualification() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    dispatch = workflow[True]["workflow_dispatch"]["inputs"]["operation"]
    assert dispatch["default"] == "build-only"
    assert dispatch["options"] == ["build-only", "testpypi-dev", "release"]
    jobs = workflow["jobs"]
    assert "inputs.operation != 'release'" in jobs["preflight"]["if"]
    preflight = jobs["provider-release-preflight"]
    assert "environment" not in preflight
    assert "secrets." not in str(preflight)
    assert jobs["vane-testpypi-wheels"]["needs"] == "provider-release-preflight"
    assert "production-signing" in jobs["vane-testpypi-wheels"]["environment"]["name"]
    publish = jobs["publish-pypi-lance"]
    assert publish["if"] == "inputs.operation == 'release'"
    assert set(publish["needs"]) == {
        "assemble-testpypi-lance",
        "testpypi-local-lance-integration",
        "testpypi-ray-lance-integration",
    }
    assert publish["environment"]["name"] == "pypi"
    steps = publish["steps"]
    promotion = next(
        i for i, step in enumerate(steps) if "verify-promotion" in step.get("run", "")
    )
    upload = next(
        i
        for i, step in enumerate(steps)
        if "gh-action-pypi-publish" in step.get("uses", "")
    )
    indexed = next(
        i for i, step in enumerate(steps) if "--index pypi" in step.get("run", "")
    )
    assert promotion < upload < indexed
    assert "--directory dist" in steps[promotion]["run"]
    assert steps[upload]["with"]["packages-dir"] == "dist"
    assert steps[upload]["with"]["repository-url"] == "https://upload.pypi.org/legacy/"
    assert not any(
        "build_vane_dynamic_wheel.py" in step.get("run", "") for step in steps
    )


@pytest.mark.parametrize("runner", ["local", "ray"])
def test_release_smokes_use_exact_staged_bytes_and_the_correct_runtime_index(
    runner,
) -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    job = workflow["jobs"][f"testpypi-{runner}-lance-integration"]
    assert job["env"]["INDEX_URL"] == "https://test.pypi.org/simple/"
    assert "inputs.operation == 'release'" in job["env"]["VANE_RUNTIME_INDEX_URL"]
    assert "https://pypi.org/simple/" in job["env"]["VANE_RUNTIME_INDEX_URL"]
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert (
        'INDEX_URL="$VANE_RUNTIME_INDEX_URL" download_exact "vane-ai==$VANE_VERSION"'
        in commands
    )
    assert 'cmp "${expected_lance[0]}" "${lance_wheels[0]}"' in commands
    assert "unset PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_FIND_LINKS" in commands
    assert "PIP_CONFIG_FILE=/dev/null" in commands


def test_every_artifact_download_fails_on_a_digest_mismatch() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/download-artifact@"):
                assert (
                    step["uses"]
                    == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
                )
                assert step["with"]["digest-mismatch"] == "error"


def test_every_provider_builder_uses_the_selected_manifest() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    for job in workflow["jobs"].values():
        for step in job["steps"]:
            command = step.get("run", "")
            if "-I extension/scripts/build_vane_dynamic_wheel.py" in command:
                assert '--manifest "$VANE_MANIFEST"' in command
