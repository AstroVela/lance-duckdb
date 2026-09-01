# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


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
