# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import importlib.util
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path
from unittest.mock import Mock

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/build_vane_dynamic_wheel.py"
SPEC = importlib.util.spec_from_file_location("build_vane_dynamic_wheel", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = builder
SPEC.loader.exec_module(builder)

PREFLIGHT_SPEC = importlib.util.spec_from_file_location(
    "vane_release_preflight", SCRIPT.with_name("vane_release_preflight.py")
)
assert PREFLIGHT_SPEC is not None and PREFLIGHT_SPEC.loader is not None
preflight = importlib.util.module_from_spec(PREFLIGHT_SPEC)
sys.modules[PREFLIGHT_SPEC.name] = preflight
PREFLIGHT_SPEC.loader.exec_module(preflight)


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
    assert (
        builder._vcpkg_revision(root / "vane-extension.toml")
        == manifest["vcpkg"]["revision"]
    )
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
        builder._vcpkg_revision(tmp_path / "vane-extension.toml")


def test_provider_builder_reads_the_selected_manifest_vcpkg_pin(tmp_path) -> None:
    development = tmp_path / "vane-extension.toml"
    production = tmp_path / "vane-extension-release.toml"
    for path, revision in ((development, "a" * 40), (production, "b" * 40)):
        path.write_text(
            'schema_version = 2\n[vcpkg]\nrepository = "microsoft/vcpkg"\n'
            f'revision = "{revision}"\n'
        )
    assert builder._vcpkg_revision(production) == "b" * 40


@pytest.mark.parametrize("version", ["0.2.0", "0.2.0rc1", "0.2.0.post1"])
def test_production_signing_requires_exact_indexed_public_runtime_wheels(
    version,
) -> None:
    builder._require_signing_policy(
        "production",
        consume=True,
        package_local_runtime=False,
        runtime_wheels=[
            Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl")
        ],
    )


@pytest.mark.parametrize(
    "version", ["0.2.0.dev612", "0.2.0+local", "1!0.2.0", "0.2", "0.2.0RC1"]
)
def test_production_signing_rejects_unreleased_or_noncanonical_runtime_versions(
    version,
) -> None:
    with pytest.raises(builder.QualificationError):
        builder._require_production_runtime_wheels(
            [Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl")]
        )


@pytest.mark.parametrize(
    "consume,package_local_runtime",
    [(False, False), (True, True)],
)
def test_production_signing_cannot_keep_keys_or_package_a_local_runtime(
    consume, package_local_runtime
) -> None:
    with pytest.raises(builder.QualificationError):
        builder._require_signing_policy(
            "production",
            consume=consume,
            package_local_runtime=package_local_runtime,
            runtime_wheels=[],
        )


def test_production_runtime_wheels_cannot_mix_versions() -> None:
    with pytest.raises(builder.QualificationError, match="one exact version"):
        builder._require_production_runtime_wheels(
            [
                Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl")
                for version in ("0.2.0", "0.2.1")
            ]
        )


def test_production_key_fingerprint_is_checked_without_logging_key_material(
    monkeypatch, capsys
) -> None:
    assert builder.PRODUCTION_PUBLIC_KEY_SHA256 == (
        "8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb"
    )
    private = bytearray(b"synthetic private key, not a real signing key")
    public = b"synthetic public DER"
    result = subprocess.CompletedProcess([], 0, public, b"")
    run = Mock(return_value=result)
    monkeypatch.setattr(builder.subprocess, "run", run)
    with pytest.raises(builder.QualificationError, match="trust root"):
        builder._require_production_signing_key(private)
    monkeypatch.setattr(
        builder, "PRODUCTION_PUBLIC_KEY_SHA256", hashlib.sha256(public).hexdigest()
    )
    builder._require_production_signing_key(private)
    assert run.call_args.kwargs["input"] is private
    assert "synthetic" not in capsys.readouterr().out
    result.returncode = 1
    result.stderr = bytes(private)
    with pytest.raises(builder.QualificationError, match="trust root"):
        builder._require_production_signing_key(private)
    assert "synthetic" not in capsys.readouterr().err


@pytest.mark.parametrize("profile", ["production", "ci-test", "testpypi"])
def test_cmake_enables_only_the_selected_testing_key(
    profile, tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(builder, "_require_file", lambda path, description: path)
    monkeypatch.setattr(
        builder,
        "_write_loadable_extension_config",
        lambda *args: tmp_path / "config.cmake",
    )
    monkeypatch.delenv("VANE_CMAKE_COMPILER_LAUNCHER", raising=False)
    identity, option = builder.SIGNING_PROFILES[profile]
    environment = builder._build_environment(
        extension_root=tmp_path,
        build_directory=tmp_path,
        vane_vcpkg_installed=tmp_path,
        vcpkg_toolchain=tmp_path / "vcpkg.cmake",
        jobs=8,
        signing_cmake_option=option,
    )
    arguments = dict(
        argument[2:].split("=", 1)
        for argument in shlex.split(environment["CMAKE_ARGS"])
        if argument.startswith("-D")
    )
    flags = {
        "VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY",
        "VANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY",
    }
    assert {flag: arguments[flag] for flag in flags} == {
        flag: "ON" if flag == option else "OFF" for flag in flags
    }
    if profile == "production":
        assert (identity, option) == ("astrovela/vane", None)


def _dispatch_environment() -> dict[str, str]:
    return {
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REPOSITORY": "AstroVela/lance-duckdb",
        "GITHUB_REF": "refs/heads/main_vane",
        "GITHUB_REF_PROTECTED": "true",
    }


@pytest.mark.parametrize(
    "name,value",
    [
        ("GITHUB_EVENT_NAME", "pull_request"),
        ("GITHUB_REPOSITORY", "example/lance-duckdb"),
        ("GITHUB_REF", "refs/heads/topic"),
        ("GITHUB_REF", "refs/tags/v0.2.0"),
        ("GITHUB_REF_PROTECTED", "false"),
    ],
)
def test_publication_rejects_unprotected_or_unreviewed_dispatches(name, value) -> None:
    environment = _dispatch_environment()
    preflight.require_publish_dispatch(environment)
    environment[name] = value
    with pytest.raises(ValueError, match="protected"):
        preflight.require_publish_dispatch(environment)


def _runtime_document(version: str) -> dict:
    return {
        "urls": [
            {
                "filename": f"vane_ai-{version}-{interpreter}-{interpreter}-manylinux_2_28_x86_64.whl",
                "packagetype": "bdist_wheel",
                "digests": {"sha256": "a" * 64},
            }
            for interpreter in ("cp310", "cp311", "cp312", "cp313", "cp314")
        ]
    }


@pytest.mark.parametrize(
    "channel,version,index",
    [
        ("release", "0.2.0", "pypi"),
        ("testpypi-dev", "0.2.0.dev612", "testpypi"),
    ],
)
def test_preflight_requires_every_exact_runtime_from_only_the_selected_index(
    channel, version, index, monkeypatch
) -> None:
    root = SCRIPT.parents[1]
    release = preflight.load_release_tools(root)
    document = _runtime_document(version)
    request = Mock(return_value=(200, document))
    monkeypatch.setattr(release, "_request_json", request)
    preflight.require_indexed_runtime(
        release, version, channel, root / "vane-provider-release.toml"
    )
    request.assert_called_once_with(
        f"{release.INDEX_JSON_BASES[index]}/vane-ai/{version}/json"
    )
    document["urls"].pop()
    with pytest.raises(ValueError, match="missing indexed wheels"):
        preflight.require_indexed_runtime(
            release, version, channel, root / "vane-provider-release.toml"
        )
    request.return_value = (404, None)
    with pytest.raises(ValueError, match=f"not published on {index}"):
        preflight.require_indexed_runtime(
            release, version, channel, root / "vane-provider-release.toml"
        )


def test_preparation_pin_cannot_bypass_release_version_preflight(
    tmp_path, monkeypatch
) -> None:
    root = SCRIPT.parents[1]
    release = preflight.load_release_tools(root)
    monkeypatch.setattr(preflight, "load_release_tools", lambda root: release)
    monkeypatch.setattr(release, "verify_sources", Mock())
    monkeypatch.setattr(
        preflight.subprocess, "check_output", Mock(return_value="0.2.0.dev641\n")
    )
    ancestry = Mock()
    monkeypatch.setattr(preflight.subprocess, "run", ancestry)
    request = Mock()
    monkeypatch.setattr(release, "_request_json", request)
    for name, value in _dispatch_environment().items():
        monkeypatch.setenv(name, value)
    output = tmp_path / "github-output"
    with pytest.raises(release.ReleaseValidationError):
        preflight.main(
            [
                "--channel",
                "release",
                "--extension-root",
                str(root),
                "--manifest",
                str(root / "vane-extension-release.toml"),
                "--vane-source",
                str(tmp_path),
                "--ci-tools-version",
                "a" * 40,
                "--github-output",
                str(output),
            ]
        )
    ancestry.assert_not_called()
    request.assert_not_called()
    assert not output.exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("yanked", True),
        ("filename", "vane_ai-0.2.0-cp314-cp314t-manylinux_2_28_x86_64.whl"),
    ],
)
def test_preflight_rejects_yanked_or_wrong_abi_runtime_files(
    field, value, monkeypatch
) -> None:
    root = SCRIPT.parents[1]
    release = preflight.load_release_tools(root)
    document = _runtime_document("0.2.0")
    document["urls"][-1][field] = value
    monkeypatch.setattr(release, "_request_json", Mock(return_value=(200, document)))
    with pytest.raises(ValueError, match="missing indexed wheels"):
        preflight.require_indexed_runtime(
            release, "0.2.0", "release", root / "vane-provider-release.toml"
        )


@pytest.mark.parametrize(
    "channel,version", [("release", "0.2.0"), ("testpypi-dev", "0.2.0.dev612")]
)
def test_preflight_preserves_exact_sources_version_and_production_ancestry(
    channel, version, tmp_path, monkeypatch
) -> None:
    root = SCRIPT.parents[1]
    release = preflight.load_release_tools(root)
    monkeypatch.setattr(preflight, "load_release_tools", lambda root: release)
    sources = Mock()
    monkeypatch.setattr(release, "verify_sources", sources)
    version_command = Mock(return_value=f"{version}\n")
    monkeypatch.setattr(preflight.subprocess, "check_output", version_command)
    ancestry = Mock()
    monkeypatch.setattr(preflight.subprocess, "run", ancestry)
    indexed = Mock()
    monkeypatch.setattr(preflight, "require_indexed_runtime", indexed)
    for name, value in _dispatch_environment().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SETUPTOOLS_SCM_PRETEND_VERSION", "0.2.0")
    monkeypatch.setenv("VANE_VERSION_BRANCH", "release/99.99")
    output = tmp_path / "github-output"
    manifest = root / (
        "vane-extension-release.toml" if channel == "release" else "vane-extension.toml"
    )
    arguments = [
        "--channel",
        channel,
        "--extension-root",
        str(root),
        "--manifest",
        str(manifest),
        "--vane-source",
        str(tmp_path),
        "--ci-tools-version",
        "a" * 40,
        "--github-output",
        str(output),
    ]
    assert preflight.main(arguments) == 0
    sources.assert_called_once_with(manifest, root, tmp_path, "a" * 40)
    assert output.read_text() == f"vane_version={version}\n"
    assert (
        "SETUPTOOLS_SCM_PRETEND_VERSION" not in version_command.call_args.kwargs["env"]
    )
    assert "VANE_VERSION_BRANCH" not in version_command.call_args.kwargs["env"]
    indexed.assert_called_once_with(
        release, version, channel, root / "vane-provider-release.toml"
    )
    if channel == "release":
        ancestry.assert_called_once_with(
            [
                "git",
                "merge-base",
                "--is-ancestor",
                preflight.PRODUCTION_KEY_REVISION,
                "HEAD",
            ],
            cwd=tmp_path,
            check=True,
        )
        indexed.reset_mock()
        ancestry.side_effect = subprocess.CalledProcessError(1, "git merge-base")
        with pytest.raises(subprocess.CalledProcessError):
            preflight.main(arguments)
        indexed.assert_not_called()
    else:
        ancestry.assert_not_called()
