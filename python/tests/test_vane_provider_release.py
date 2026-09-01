# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts/validate_vane_provider_release.py"
)
SPEC = importlib.util.spec_from_file_location("validate_vane_provider_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release
SPEC.loader.exec_module(release)

VANE_VERSION = "0.2.0.dev613"
PROVIDER_VERSION = (
    "0.2.0.0.0.0.0.0.0.0.0.0.0.613."
    "1513373755.2784857594.139834240.3144919137."
    "1395718426.2485728274.815903707.3513321711"
)


def _wheel_name(interpreter: str, version: str = PROVIDER_VERSION) -> str:
    return (
        f"vane_extension_lance-{version}-{interpreter}-none-"
        "manylinux_2_28_x86_64.whl"
    )


def _write_wheel(
    directory: Path,
    interpreter: str,
    *,
    provider_version: str = PROVIDER_VERSION,
    vane_version: str = VANE_VERSION,
    requirement: str | None = None,
) -> Path:
    path = directory / _wheel_name(interpreter, provider_version)
    dist_info = f"vane_extension_lance-{provider_version}.dist-info"
    dependency = requirement or f"vane-ai === {vane_version}"
    metadata = (
        "Metadata-Version: 2.4\n"
        "Name: vane-extension-lance\n"
        f"Version: {provider_version}\n"
        f"Requires-Dist: {dependency}\n"
        "\n"
    )
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(f"{dist_info}/METADATA", metadata)
    return path


def _write_release(directory: Path) -> tuple[Path, ...]:
    return tuple(
        _write_wheel(directory, interpreter)
        for interpreter in sorted(release.EXPECTED_INTERPRETERS)
    )


def _indexed_document(paths: tuple[Path, ...]) -> dict[str, object]:
    return {
        "urls": [
            {
                "filename": path.name,
                "packagetype": "bdist_wheel",
                "digests": {"sha256": release._sha256(path)},
            }
            for path in paths
        ]
    }


def test_validate_release_accepts_exact_runtime_matrix(tmp_path: Path) -> None:
    _write_release(tmp_path)
    assert release.validate_release(tmp_path, VANE_VERSION) == PROVIDER_VERSION


def test_validate_release_rejects_missing_interpreter(tmp_path: Path) -> None:
    paths = _write_release(tmp_path)
    paths[0].unlink()
    with pytest.raises(release.ReleaseValidationError, match="exactly five"):
        release.validate_release(tmp_path, VANE_VERSION)


def test_validate_release_rejects_non_exact_runtime_dependency(
    tmp_path: Path,
) -> None:
    _write_release(tmp_path)
    target = tmp_path / _wheel_name("cp312")
    target.unlink()
    _write_wheel(tmp_path, "cp312", requirement=f"vane-ai >= {VANE_VERSION}")
    with pytest.raises(release.ReleaseValidationError, match="exact ==="):
        release.validate_release(tmp_path, VANE_VERSION)


def test_validate_release_rejects_non_development_runtime(tmp_path: Path) -> None:
    _write_release(tmp_path)
    with pytest.raises(release.ReleaseValidationError, match="development release"):
        release.validate_release(tmp_path, "0.2.0")


def test_validate_release_rejects_build_tags(tmp_path: Path) -> None:
    paths = _write_release(tmp_path)
    target = paths[0]
    parts = target.name.rsplit("-", 3)
    target.rename(target.with_name(f"{parts[0]}-1-{parts[1]}-{parts[2]}-{parts[3]}"))
    with pytest.raises(release.ReleaseValidationError, match="build tag"):
        release.validate_release(tmp_path, VANE_VERSION)


def test_validate_release_rejects_testpypi_oversize(tmp_path: Path) -> None:
    _write_release(tmp_path)
    target = tmp_path / _wheel_name("cp312")
    with target.open("r+b") as wheel:
        wheel.truncate(release.MAX_TESTPYPI_FILE_BYTES + 1)
    with pytest.raises(release.ReleaseValidationError, match="100 MB"):
        release.validate_release(tmp_path, VANE_VERSION)


def test_publishable_accepts_absent_version(tmp_path: Path, monkeypatch) -> None:
    _write_release(tmp_path)
    monkeypatch.setattr(release, "_request_json", lambda _url: (404, None))
    release.require_index_publishable(tmp_path, PROVIDER_VERSION)


def test_publishable_accepts_exact_partial_rerun(tmp_path: Path, monkeypatch) -> None:
    paths = _write_release(tmp_path)
    document = _indexed_document(paths[:2])
    monkeypatch.setattr(release, "_request_json", lambda _url: (200, document))
    release.require_index_publishable(tmp_path, PROVIDER_VERSION)


def test_publishable_rejects_conflicting_hash(tmp_path: Path, monkeypatch) -> None:
    paths = _write_release(tmp_path)
    document = _indexed_document(paths[:1])
    document["urls"][0]["digests"]["sha256"] = "0" * 64
    monkeypatch.setattr(release, "_request_json", lambda _url: (200, document))
    with pytest.raises(release.ReleaseValidationError, match="conflict"):
        release.require_index_publishable(tmp_path, PROVIDER_VERSION)


def test_index_match_requires_exact_complete_set(tmp_path: Path, monkeypatch) -> None:
    paths = _write_release(tmp_path)
    responses = iter([(404, None), (200, _indexed_document(paths))])
    monkeypatch.setattr(release, "_request_json", lambda _url: next(responses))
    release.require_index_match(tmp_path, PROVIDER_VERSION, attempts=2, delay_seconds=0)
