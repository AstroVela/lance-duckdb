# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Consumer checks for Lance's pinned shared provider release gate."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import zipfile
from pathlib import Path
from unittest.mock import Mock

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
        "--require-testpypi-publishable",
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
        "assemble-testpypi-lance",
        "verify-testpypi-lance",
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
                "rev-parse HEAD:vane-extension-ci-tools",
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
    }
    assert "--provider lance" in commands["verify-testpypi-lance"]
    for job in ("vane-testpypi-wheels", "assemble-testpypi-lance"):
        assert "--require-testpypi-publishable" in commands[job]
