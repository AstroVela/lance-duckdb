# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/build_vane_dynamic_wheel.py"
SPEC = importlib.util.spec_from_file_location("build_vane_dynamic_wheel", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = builder
SPEC.loader.exec_module(builder)


def test_artifact_export_check_only_reads_external_definitions(
    tmp_path: Path, monkeypatch
) -> None:
    artifact = tmp_path / "lance.duckdb_extension"
    artifact.touch()
    commands: list[tuple[str, ...]] = []

    def capture(command, *, cwd: Path) -> str:
        assert cwd == artifact.parent
        command = tuple(command)
        commands.append(command)
        if command[0] == "file":
            return f"{artifact}: ELF 64-bit LSB shared object, stripped"
        if command[0] == "readelf":
            return "Shared library: [libc.so.6]"
        assert command[0] == "nm"
        return "lance_duckdb_cpp_init T 1000 42"

    monkeypatch.setattr(builder, "_capture", capture)

    builder._require_self_contained_artifact(artifact)

    nm_command = next(command for command in commands if command[0] == "nm")
    assert "--extern-only" in nm_command


def test_provider_builder_uses_the_shared_manifest_vcpkg_revision() -> None:
    root = SCRIPT.parents[1]
    manifest = tomllib.loads((root / "vane-extension.toml").read_text())
    assert manifest["schema_version"] == 2
    assert manifest["vcpkg"]["repository"] == "microsoft/vcpkg"
    assert builder._vcpkg_revision(root) == manifest["vcpkg"]["revision"]
    assert manifest["vcpkg"]["revision"] == "84bab45d415d22042bd0b9081aea57f362da3f35"


@pytest.mark.parametrize(
    "manifest",
    [
        'schema_version = 1\nvcpkg_commit = "' + "a" * 40 + '"\n',
        'schema_version = 2\n[vcpkg]\nrepository = "other/vcpkg"\nrevision = "'
        + "a" * 40
        + '"\n',
        'schema_version = 2\n[vcpkg]\nrepository = "microsoft/vcpkg"\nrevision = "main"\n',
    ],
)
def test_provider_builder_rejects_non_exact_vcpkg_manifests(tmp_path, manifest) -> None:
    (tmp_path / "vane-extension.toml").write_text(manifest)
    with pytest.raises(builder.QualificationError):
        builder._vcpkg_revision(tmp_path)
