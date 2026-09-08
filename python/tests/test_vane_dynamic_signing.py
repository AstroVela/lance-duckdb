# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Data-only signer boundary tests; no real release keys or native loads."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import stat
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/sign_vane_dynamic_bundle.py"
SPEC = importlib.util.spec_from_file_location("sign_vane_dynamic_bundle", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
signer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = signer
SPEC.loader.exec_module(signer)

REVISION = "a" * 40
MANIFEST = (
    'schema_version = 2\nname = "lance"\n[vane]\n'
    f'repository = "AstroVela/vane"\nrevision = "{REVISION}"\n'
)


@pytest.mark.parametrize(
    "profile,filename",
    [
        ("production", "vane-extension-release.toml"),
        ("testpypi", "vane-extension.toml"),
    ],
)
def test_signer_reads_only_the_committed_selected_manifest(
    profile, filename, tmp_path, monkeypatch
) -> None:
    (tmp_path / filename).write_text("untrusted working-tree contents")
    git = Mock(return_value=MANIFEST)
    monkeypatch.setattr(signer, "_git", git)
    assert signer.committed_manifest(tmp_path, profile) == {
        "repository": "AstroVela/vane",
        "revision": REVISION,
    }
    git.assert_called_once_with(tmp_path, "show", f"HEAD:{filename}")


@pytest.mark.parametrize(
    "old,new",
    [
        ("schema_version = 2", "schema_version = 1"),
        ('name = "lance"', 'name = "other"'),
        ("AstroVela/vane", "other/vane"),
        (REVISION, "main"),
        (REVISION, "a" * 39),
    ],
)
def test_signer_manifest_rejects_nonexact_or_unofficial_inputs(
    old, new, tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(signer, "_git", Mock(return_value=MANIFEST.replace(old, new)))
    with pytest.raises(ValueError, match="committed exact official"):
        signer.committed_manifest(tmp_path, "production")


def _artifact(root: Path) -> Path:
    path = root / "artifacts/lance.duckdb_extension"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"synthetic native data" * 32 + b"\0" * 256)
    return path


def test_signer_requires_bounded_unsigned_regular_data(tmp_path) -> None:
    artifact = _artifact(tmp_path)
    signer.require_artifact(artifact)
    artifact.write_bytes(b"x" * 1024)
    with pytest.raises(ValueError, match="unsigned DuckDB"):
        signer.require_artifact(artifact)
    artifact.write_bytes(b"\0" * 512)
    with pytest.raises(ValueError, match="bounded regular"):
        signer.require_artifact(artifact)
    with artifact.open("r+b") as output:
        output.truncate(signer.MAX_ARTIFACT_BYTES + 1)
    with pytest.raises(ValueError, match="bounded regular"):
        signer.require_artifact(artifact)


def test_signer_rejects_symlink_inputs_and_parents(tmp_path) -> None:
    artifact = _artifact(tmp_path / "bundle")
    alias = tmp_path / "link.duckdb_extension"
    alias.symlink_to(artifact)
    with pytest.raises(ValueError, match="non-symlink"):
        signer.require_artifact(alias)
    linked_bundle = tmp_path / "linked-bundle"
    linked_bundle.symlink_to(artifact.parents[1], target_is_directory=True)
    with pytest.raises(ValueError, match="non-symlink"):
        signer.require_artifact(linked_bundle / "artifacts/lance.duckdb_extension")


@pytest.mark.parametrize("profile", ["production", "testpypi"])
def test_key_fingerprints_fail_closed_without_logging_private_material(
    profile, monkeypatch, capsys
) -> None:
    assert signer.KEY_FINGERPRINTS == {
        "production": "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb",
        "testpypi": "53779fb8f9c97e9dec9c66ff838839eb234d1a64d4b105671304820e627b5e32",
    }
    private = bytearray(b"synthetic private key, not a real signing key")
    public = b"synthetic public DER"
    result = subprocess.CompletedProcess([], 0, public, b"")
    run = Mock(return_value=result)
    monkeypatch.setattr(signer.subprocess, "run", run)
    with pytest.raises(ValueError, match="trust root"):
        signer.require_key_fingerprint(private, profile)
    monkeypatch.setitem(
        signer.KEY_FINGERPRINTS, profile, hashlib.sha256(public).hexdigest()
    )
    signer.require_key_fingerprint(private, profile)
    assert run.call_args.kwargs["input"] is private
    assert run.call_args.args[0][0] == "/usr/bin/openssl"
    result.returncode = 1
    result.stderr = bytes(private)
    with pytest.raises(ValueError, match="trust root"):
        signer.require_key_fingerprint(private, profile)
    captured = capsys.readouterr()
    assert "synthetic" not in captured.out + captured.err
    with pytest.raises(ValueError, match="present and bounded"):
        signer.require_key_fingerprint(bytearray(), profile)


def _git_identity(root, *arguments) -> str:
    if arguments[0] == "show":
        return MANIFEST
    if arguments == ("rev-parse", "HEAD"):
        return REVISION
    if arguments == ("remote", "get-url", "origin"):
        return "https://github.com/AstroVela/vane.git"
    assert arguments == ("status", "--porcelain", "--untracked-files=no")
    return ""


def _sign_arguments(tmp_path: Path) -> list[str]:
    return [
        "sign",
        "--profile",
        "production",
        "--extension-root",
        str(tmp_path),
        "--vane-source",
        str(tmp_path / "vane"),
        "--input-directory",
        str(tmp_path / "prepared"),
        "--output-directory",
        str(tmp_path / "signed"),
    ]


@pytest.mark.parametrize("fail", [False, True])
def test_signer_unsets_the_secret_and_destroys_the_private_temporary_file(
    tmp_path, monkeypatch, fail, capsys
) -> None:
    _artifact(tmp_path / "prepared")
    runner_temp = tmp_path / "runner-temp"
    runner_temp.mkdir()
    private = "synthetic release key"
    monkeypatch.setenv("RUNNER_TEMP", str(runner_temp))
    monkeypatch.setenv("VANE_PROVIDER_SIGNING_PRIVATE_KEY", private)

    def git(root, *arguments):
        assert "VANE_PROVIDER_SIGNING_PRIVATE_KEY" not in os.environ
        return _git_identity(root, *arguments)

    monkeypatch.setattr(signer, "_git", git)
    fingerprint = Mock()
    monkeypatch.setattr(signer, "require_key_fingerprint", fingerprint)
    private_paths = []

    def run(command, *, check, env):
        assert check
        assert command[:3] == ["/usr/bin/python3", "-I", "-S"]
        assert command[3] == str(
            tmp_path / "vane/scripts/sign_test_dynamic_extension.py"
        )
        assert "VANE_PROVIDER_SIGNING_PRIVATE_KEY" not in os.environ
        assert "VANE_PROVIDER_SIGNING_PRIVATE_KEY" not in env
        assert env["PATH"] == "/usr/bin:/bin"
        key = Path(command[5])
        assert key.is_relative_to(runner_temp)
        assert stat.S_IMODE(key.stat().st_mode) == 0o600
        assert key.read_text() == private
        private_paths.append(key)
        if fail:
            raise subprocess.CalledProcessError(1, command)
        Path(command[-1]).write_bytes(b"signed native data")

    monkeypatch.setattr(signer.subprocess, "run", run)
    if fail:
        with pytest.raises(subprocess.CalledProcessError):
            signer.main(_sign_arguments(tmp_path))
    else:
        assert signer.main(_sign_arguments(tmp_path)) == 0
    assert private_paths and all(not path.exists() for path in private_paths)
    assert not list(runner_temp.iterdir())
    assert fingerprint.call_args.args == (bytearray(), "production")
    captured = capsys.readouterr()
    assert private not in captured.out + captured.err


@pytest.mark.parametrize(
    "command,value",
    [
        ("rev-parse", "b" * 40),
        ("remote", "https://github.com/other/vane.git"),
        ("status", " M scripts/sign_test_dynamic_extension.py"),
    ],
)
def test_signer_rejects_changed_source_before_key_use(
    command, value, tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(
        signer,
        "_git",
        lambda root, *arguments: (
            value if arguments[0] == command else _git_identity(root, *arguments)
        ),
    )
    fingerprint = Mock()
    monkeypatch.setattr(signer, "require_key_fingerprint", fingerprint)
    with pytest.raises(ValueError, match="clean exact official"):
        signer.main(_sign_arguments(tmp_path))
    fingerprint.assert_not_called()


def test_signer_cli_runs_without_site_packages() -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(SCRIPT), "--help"],
        check=True,
        text=True,
        capture_output=True,
    )
    assert "manifest,sign" in result.stdout


@pytest.mark.parametrize("directory", ["prepared", "signed"])
def test_signer_never_places_private_files_in_uploaded_or_downloaded_data(
    directory, tmp_path, monkeypatch
) -> None:
    _artifact(tmp_path / "prepared")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / directory))
    monkeypatch.setattr(signer, "_git", _git_identity)
    monkeypatch.setattr(signer, "require_key_fingerprint", Mock())
    run = Mock()
    monkeypatch.setattr(signer.subprocess, "run", run)
    with pytest.raises(ValueError, match="outside downloaded and uploaded"):
        signer.main(_sign_arguments(tmp_path))
    run.assert_not_called()
