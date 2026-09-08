# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace
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
    builder._require_production_runtime_wheels(
        [Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl")],
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


def _phase_arguments(phase: str, profile: str, **overrides) -> SimpleNamespace:
    arguments = {
        "phase": phase,
        "signing_profile": profile,
        "package_local_runtime": phase == "full",
        "signing_private_key": Path("ci-fixture.pem") if phase == "full" else None,
        "runtime_python": [Path(sys.executable)] if phase == "package" else [],
        "runtime_wheel": (
            [Path("vane_ai-0.2.0-cp312-cp312-manylinux_2_28_x86_64.whl")]
            if phase == "package"
            else []
        ),
        "bundle_directory": Path("prepared") if phase == "package" else None,
        "signed_artifact": (
            Path("signed/lance.duckdb_extension") if phase == "package" else None
        ),
        "vane_vcpkg_installed": None if phase == "package" else Path("vcpkg-installed"),
        "vcpkg_toolchain": None if phase == "package" else Path("vcpkg.cmake"),
        "cargo_about": None if phase == "package" else Path("cargo-about"),
    }
    arguments.update(overrides)
    return SimpleNamespace(**arguments)


@pytest.mark.parametrize(
    "phase,profile",
    [
        ("full", "ci-test"),
        ("prepare", "production"),
        ("prepare", "testpypi"),
        ("package", "production"),
        ("package", "testpypi"),
    ],
)
def test_builder_phases_are_explicit_and_valid(phase, profile) -> None:
    builder._validate_phase(_phase_arguments(phase, profile))


@pytest.mark.parametrize("phase", ["prepare", "package"])
@pytest.mark.parametrize("profile", ["production", "testpypi"])
@pytest.mark.parametrize(
    "override",
    [{"signing_private_key": Path("private.pem")}, {"package_local_runtime": True}],
)
def test_publishing_phases_cannot_take_keys_or_local_runtimes(
    phase, profile, override
) -> None:
    with pytest.raises(builder.QualificationError, match="cannot access signing keys"):
        builder._validate_phase(_phase_arguments(phase, profile, **override))


@pytest.mark.parametrize("profile", ["production", "testpypi"])
def test_full_build_cannot_sign_a_publishing_profile(profile) -> None:
    with pytest.raises(builder.QualificationError, match="only for CI"):
        builder._validate_phase(_phase_arguments("full", profile))


@pytest.mark.parametrize(
    "override",
    [
        {"runtime_python": []},
        {"bundle_directory": None},
        {"signed_artifact": None},
        {"cargo_about": Path("cargo-about")},
    ],
)
def test_package_rejects_incomplete_inputs_and_native_tools(override) -> None:
    with pytest.raises(builder.QualificationError):
        builder._validate_phase(_phase_arguments("package", "production", **override))


def test_production_runtime_wheels_cannot_mix_versions() -> None:
    with pytest.raises(builder.QualificationError, match="one exact version"):
        builder._require_production_runtime_wheels(
            [
                Path(f"vane_ai-{version}-cp312-cp312-manylinux_2_28_x86_64.whl")
                for version in ("0.2.0", "0.2.1")
            ]
        )


def test_package_main_returns_before_native_tools_or_key_access(
    tmp_path, monkeypatch
) -> None:
    arguments = _phase_arguments(
        "package",
        "production",
        jobs=1,
        extension_root=tmp_path,
        vane_source=tmp_path,
        vane_revision="a" * 40,
        build_directory=tmp_path / "build",
        output_directory=tmp_path / "dist",
    )
    monkeypatch.setattr(builder, "_parse_arguments", lambda: arguments)
    monkeypatch.setattr(builder, "_require_git_revision", Mock())
    package = Mock(return_value=0)
    monkeypatch.setattr(builder, "_package_signed", package)
    native = Mock(side_effect=AssertionError("native build must not run"))
    key = Mock(side_effect=AssertionError("private key must not be read"))
    monkeypatch.setattr(builder, "_require_vcpkg_toolchain", native)
    monkeypatch.setattr(builder, "_read_signing_private_key", key)
    assert builder.main() == 0
    package.assert_called_once_with(
        arguments, tmp_path, tmp_path / "build", tmp_path / "dist"
    )
    native.assert_not_called()
    key.assert_not_called()


def test_prepare_emits_only_unsigned_native_data_and_licenses(
    tmp_path, monkeypatch
) -> None:
    build = tmp_path / "build"
    unsigned = build / "duckdb/extension/lance/lance.duckdb_extension"
    unsigned.parent.mkdir(parents=True)
    unsigned.write_bytes(b"unsigned native data" * 32 + b"\0" * 256)
    license_file = tmp_path / "license.txt"
    license_file.write_text("data only\n")
    cargo_about = tmp_path / "cargo-about"
    cargo_about.touch(mode=0o700)
    arguments = _phase_arguments(
        "prepare",
        "production",
        jobs=1,
        extension_root=tmp_path,
        vane_source=tmp_path,
        vane_revision="a" * 40,
        manifest=tmp_path / "manifest.toml",
        vane_vcpkg_installed=tmp_path,
        cargo_about=cargo_about,
        build_directory=build,
        output_directory=tmp_path / "bundle",
    )
    monkeypatch.setattr(builder, "_parse_arguments", lambda: arguments)
    for name in (
        "_require_git_revision",
        "_require_self_contained_artifact",
        "_require_base_wheel_free_of_lance",
        "_run",
    ):
        monkeypatch.setattr(builder, name, Mock())
    monkeypatch.setattr(builder, "_vcpkg_revision", lambda path: "b" * 40)
    monkeypatch.setattr(
        builder, "_require_vcpkg_toolchain", lambda path, revision: path
    )
    monkeypatch.setattr(builder, "_platform_tag", lambda: builder.PROVIDER_PLATFORM_TAG)
    monkeypatch.setattr(builder, "_build_environment", Mock(return_value={}))
    monkeypatch.setattr(builder, "_one_wheel", Mock(return_value=tmp_path / "base.whl"))
    monkeypatch.setattr(
        builder, "_stage_license_files", Mock(return_value=(license_file,))
    )
    key = Mock(side_effect=AssertionError("prepare cannot read a signing key"))
    package = Mock(side_effect=AssertionError("prepare cannot package wheels"))
    monkeypatch.setattr(builder, "_read_signing_private_key", key)
    monkeypatch.setattr(builder, "_build_provider_matrix", package)
    assert builder.main() == 0
    assert {
        str(path.relative_to(arguments.output_directory))
        for path in arguments.output_directory.rglob("*")
        if path.is_file()
    } == {"artifacts/lance.duckdb_extension", "licenses/lance/license.txt"}
    assert (
        arguments.output_directory / "artifacts/lance.duckdb_extension"
    ).read_bytes() == unsigned.read_bytes()
    key.assert_not_called()
    package.assert_not_called()


def test_prepared_licenses_are_exact_bounded_regular_data(tmp_path) -> None:
    directory = tmp_path / "licenses/lance"
    directory.mkdir(parents=True)
    names = (
        "Lance-DuckDB-Apache-2.0.txt",
        "Vane-runtime-licenses.txt",
        "Rust-third-party-licenses.txt",
        "Rust-standard-library-licenses.html",
    )
    for name in names:
        (directory / name).write_text("license data\n")
    assert tuple(path.name for path in builder._prepared_licenses(tmp_path)) == names
    unexpected = directory / "executable.py"
    unexpected.touch()
    with pytest.raises(builder.QualificationError, match="exact expected files"):
        builder._prepared_licenses(tmp_path)
    unexpected.unlink()
    path = directory / names[0]
    path.unlink()
    path.symlink_to(directory / names[1])
    with pytest.raises(builder.QualificationError, match="bounded regular data"):
        builder._prepared_licenses(tmp_path)


def test_packaging_compares_signed_bytes_with_the_original_prepare_bundle(
    tmp_path,
) -> None:
    unsigned = tmp_path / "unsigned.duckdb_extension"
    signed = tmp_path / "signed.duckdb_extension"
    payload = b"native payload" * 100000
    unsigned.write_bytes(payload + b"\0" * 256)
    signed.write_bytes(payload + b"s" * 256)
    builder._require_original_payload(unsigned, signed)
    for replacement in (
        b"x" + payload[1:] + b"s" * 256,
        payload + b"s" * 257,
        payload + b"\0" * 256,
    ):
        signed.write_bytes(replacement)
        with pytest.raises(builder.QualificationError):
            builder._require_original_payload(unsigned, signed)
    signed.unlink()
    signed.symlink_to(unsigned)
    with pytest.raises(builder.QualificationError, match="bounded regular"):
        builder._require_original_payload(unsigned, signed)


def test_packaging_rejects_replacement_before_native_audit_or_wheel_build(
    tmp_path, monkeypatch
) -> None:
    arguments = _phase_arguments(
        "package",
        "production",
        bundle_directory=tmp_path,
        signed_artifact=tmp_path / "signed.duckdb_extension",
    )
    original = tmp_path / "artifacts/lance.duckdb_extension"
    original.parent.mkdir()
    original.write_bytes(b"original" * 100 + b"\0" * 256)
    arguments.signed_artifact.write_bytes(b"replaced" * 100 + b"s" * 256)
    audit = Mock()
    package = Mock()
    monkeypatch.setattr(builder, "_require_self_contained_artifact", audit)
    monkeypatch.setattr(builder, "_build_provider_matrix", package)
    with pytest.raises(builder.QualificationError, match="differs"):
        builder._package_signed(arguments, tmp_path, tmp_path, tmp_path)
    audit.assert_not_called()
    package.assert_not_called()


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
