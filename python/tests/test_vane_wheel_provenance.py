# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _load_module() -> ModuleType:
    module_path = (
        Path(__file__).resolve().parents[2] / "ci" / "generate_vane_wheel_provenance.py"
    )
    spec = importlib.util.spec_from_file_location(
        "generate_vane_wheel_provenance", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROVENANCE = _load_module()


@pytest.mark.parametrize(
    ("remote", "expected"),
    [
        (
            "https://github.com/AstroVela/vane.git",
            "https://github.com/AstroVela/vane.git",
        ),
        (
            "git@github.com:AstroVela/vane.git",
            "https://github.com/AstroVela/vane.git",
        ),
        (
            "ssh://git@github.com/AstroVela/vane",
            "https://github.com/AstroVela/vane.git",
        ),
    ],
)
def test_canonical_github_remote(remote: str, expected: str) -> None:
    assert PROVENANCE.canonical_github_remote(remote) == expected


def test_canonical_github_remote_rejects_non_github_source() -> None:
    with pytest.raises(PROVENANCE.ProvenanceError, match="unsupported"):
        PROVENANCE.canonical_github_remote("https://example.test/AstroVela/vane.git")


def test_require_official_repository_rejects_a_github_fork() -> None:
    with pytest.raises(PROVENANCE.ProvenanceError, match="not from the official"):
        PROVENANCE.require_official_repository(
            "https://github.com/example/vane.git",
            PROVENANCE.VANE_REPOSITORY,
            "Vane",
        )


def test_wheel_platform_tag_uses_the_exact_filename_tag(tmp_path: Path) -> None:
    wheel = tmp_path / "vane_ai-1.2.3-cp312-cp312-manylinux_2_39_x86_64.whl"
    wheel.write_bytes(b"wheel")

    assert PROVENANCE.wheel_platform_tag(wheel) == "manylinux_2_39_x86_64"
    assert (
        PROVENANCE.sha256_file(wheel)
        == "ba59926159d2aa256eb8739b8da7e2b574b960e1202c6d624cbe981cef996c91"
    )


def test_python_path_keeps_virtualenv_symlink(tmp_path: Path) -> None:
    interpreter = tmp_path / "system" / "python"
    interpreter.parent.mkdir()
    interpreter.write_bytes(b"python")
    virtualenv_python = tmp_path / "venv" / "bin" / "python"
    virtualenv_python.parent.mkdir(parents=True)
    virtualenv_python.symlink_to(interpreter)

    absolute = PROVENANCE.lexical_absolute_path(virtualenv_python)

    assert absolute == virtualenv_python
    assert absolute != virtualenv_python.resolve()


def test_load_vane_revision_requires_official_exact_pin(tmp_path: Path) -> None:
    revision = "d56c602afa3f235549d3e50a6133a680e635e7cd"
    manifest = tmp_path / "vane-extension.toml"
    manifest.write_text(
        "[vane]\n" 'repository = "AstroVela/vane"\n' f'revision = "{revision}"\n',
        encoding="utf-8",
    )

    assert PROVENANCE.load_vane_revision(manifest) == revision


def test_write_atomic_replaces_complete_contents(tmp_path: Path) -> None:
    output = tmp_path / "provenance.json"
    output.write_text("stale", encoding="utf-8")

    PROVENANCE.write_atomic(output, "fresh\n")

    assert output.read_text(encoding="utf-8") == "fresh\n"
