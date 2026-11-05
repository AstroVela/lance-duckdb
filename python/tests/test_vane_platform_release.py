# SPDX-License-Identifier: Apache-2.0
"""Regression checks for native host policies and immutable release transfers."""

from __future__ import annotations

import importlib.util
import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from packaging.tags import Tag

ROOT = Path(__file__).resolve().parents[2]


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


builder = load_script("build_vane_dynamic_wheel")
driver = load_script("vane_platform_release")
bootstrap = load_script("bootstrap_vane_dependencies")


def test_source_cache_publishes_only_the_pinned_bytes(tmp_path, monkeypatch):
    import hashlib
    import io

    contents = b"the exact upstream archive"
    destination = tmp_path / "sources" / "archive.tar.gz"
    expected = hashlib.sha512(contents).hexdigest()
    download = Mock(return_value=io.BytesIO(contents))
    monkeypatch.setattr(bootstrap.urllib.request, "urlopen", download)
    bootstrap.cache_archive("https://example.invalid/source", destination, expected)
    assert destination.read_bytes() == contents
    bootstrap.cache_archive("https://example.invalid/source", destination, expected)
    download.assert_called_once()


@pytest.mark.parametrize("oversized", [False, True])
def test_source_cache_rejects_invalid_downloads_without_publishing(
    tmp_path, monkeypatch, oversized
):
    import hashlib
    import io

    if oversized:
        monkeypatch.setattr(bootstrap, "MAX_ARCHIVE_BYTES", 4)
    monkeypatch.setattr(
        bootstrap.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: io.BytesIO(b"invalid bytes"),
    )
    destination = tmp_path / "archive.tar.gz"
    with pytest.raises(
        ValueError, match="bounded size" if oversized else "pinned SHA-512"
    ):
        bootstrap.cache_archive(
            "https://example.invalid/source",
            destination,
            hashlib.sha512(b"valid").hexdigest(),
        )
    assert not destination.exists()
    assert not list(tmp_path.glob(".checked-source-*"))


@pytest.mark.parametrize("corrupt", [False, True])
def test_source_mirror_network_fallback_preserves_the_hash_gate(
    tmp_path, monkeypatch, corrupt
):
    import json
    import urllib.error

    source = tmp_path / "sdk"
    source.mkdir()
    (source / "vcpkg.json").write_text(
        json.dumps({"builtin-baseline": bootstrap.VANE_DEPENDENCY_BASELINE})
    )
    error = (
        ValueError("pinned SHA-512") if corrupt else urllib.error.URLError("offline")
    )
    monkeypatch.setattr(bootstrap, "cache_archive", Mock(side_effect=error))
    delegate = Mock()
    monkeypatch.setattr(bootstrap.subprocess, "run", delegate)
    arguments = ["--vane-source", str(source), "--vcpkg-root", str(tmp_path / "vcpkg")]
    if corrupt:
        with pytest.raises(ValueError, match="pinned SHA-512"):
            bootstrap.main(arguments)
        delegate.assert_not_called()
    else:
        assert bootstrap.main(arguments) == 0
        assert delegate.call_args.args[0][-len(arguments) :] == arguments


@pytest.mark.parametrize(
    "system,machine,tag,triplet",
    [
        ("linux", "x86_64", "manylinux_2_28_x86_64", "x64-linux-release"),
        ("darwin", "arm64", "macosx_15_0_arm64", "arm64-osx-release"),
        ("win32", "AMD64", "win_amd64", "x64-windows-static-release"),
    ],
)
def test_native_wheel_policy_requires_a_compatible_host(
    system, machine, tag, triplet, monkeypatch, tmp_path
):
    monkeypatch.setattr(builder.sys, "platform", system)
    monkeypatch.setattr(builder.platform, "machine", lambda: machine)
    monkeypatch.setattr(builder, "sys_tags", lambda: iter([Tag("cp312", "cp312", tag)]))
    assert builder._platform_tag() == tag
    monkeypatch.setattr(builder, "_require_file", lambda path, _description: path)
    if system == "win32":
        library = tmp_path / "dependencies" / triplet / "lib/zs.lib"
        library.parent.mkdir(parents=True)
        library.touch()
    environment = builder._build_environment(
        extension_root=ROOT,
        build_directory=tmp_path,
        vane_vcpkg_installed=tmp_path / "dependencies",
        vcpkg_toolchain=tmp_path / "vcpkg/scripts/buildsystems/vcpkg.cmake",
        jobs=2,
        signing_cmake_option=None,
    )
    assert environment["VCPKG_TARGET_TRIPLET"] == triplet
    assert environment["VCPKG_HOST_TRIPLET"] == triplet
    arguments = shlex.split(environment["CMAKE_ARGS"])
    if system == "win32":
        assert environment["CMAKE_GENERATOR"] == "Visual Studio 17 2022"
        assert "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded" in arguments
        prefix = tmp_path / "vane-dependency-prefix.cmake"
        assert "lib/zs.lib" in prefix.read_text()
        assert "set(ZLIB_USE_STATIC_LIBS ON)" in prefix.read_text()
        assert builder._venv_python(tmp_path) == tmp_path / "Scripts/python.exe"
    if system == "darwin":
        assert environment["MACOSX_DEPLOYMENT_TARGET"] == "15.0"
        assert "-DCMAKE_OSX_DEPLOYMENT_TARGET=15.0" in arguments
    monkeypatch.setattr(
        builder, "sys_tags", lambda: iter([Tag("cp312", "cp312", "any")])
    )
    with pytest.raises(builder.QualificationError, match="does not support"):
        builder._platform_tag()


@pytest.mark.parametrize(
    "system,machine", [("linux", "aarch64"), ("darwin", "x86_64"), ("win32", "ARM64")]
)
def test_provider_does_not_relabel_an_unsupported_architecture(
    system, machine, monkeypatch
):
    monkeypatch.setattr(builder.sys, "platform", system)
    monkeypatch.setattr(builder.platform, "machine", lambda: machine)
    with pytest.raises(builder.QualificationError):
        builder._platform_configuration()


@pytest.mark.parametrize("system", ["darwin", "win32"])
def test_native_audit_rejects_extra_exports_after_the_shared_binary_policy(
    system, tmp_path, monkeypatch
):
    monkeypatch.setattr(builder.sys, "platform", system)
    monkeypatch.setattr(
        builder,
        "_platform_tag",
        lambda: "win_amd64" if system == "win32" else "macosx_15_0_arm64",
    )
    monkeypatch.setenv("VANE_PROVIDER_PACKAGING_SOURCE", str(tmp_path))
    audit = Mock()
    monkeypatch.setattr(builder, "_run", audit)
    if system == "darwin":
        report = "00001 T _lance_duckdb_cpp_init\n00002 T _private_engine_symbol\n"
    else:
        report = "  1 0 00001000 lance_duckdb_cpp_init\n  2 1 00002000 private_engine_symbol\n"
    monkeypatch.setattr(builder, "_capture", lambda *_args, **_kwargs: report)
    with pytest.raises(builder.QualificationError, match="unexpected symbols"):
        builder._require_self_contained_artifact(tmp_path / "lance.duckdb_extension")
    assert "_validate_native_binary_platform" in audit.call_args.args[0][3]


def test_indexed_smoke_rejects_changed_staged_bytes_before_installing(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    directory = tmp_path / "indexed"
    monkeypatch.setenv("VANE_DYNAMIC_DIST_DIR", str(directory))
    monkeypatch.setenv("INDEX_URL", "https://test.pypi.org/simple/")
    monkeypatch.setenv("LANCE_VERSION", "0.2.0.1")
    name = "vane_extension_lance-0.2.0.1-cp312-none-win_amd64.whl"
    expected = tmp_path / "expected"
    expected.mkdir()
    (expected / name).write_bytes(b"reviewed wheel")

    def download(command, **kwargs):
        assert kwargs["env"]["PIP_CONFIG_FILE"]
        assert "PIP_EXTRA_INDEX_URL" not in kwargs["env"]
        (directory / name).write_bytes(b"replacement wheel")

    monkeypatch.setattr(driver, "run", download)
    install = Mock()
    monkeypatch.setattr(driver, "smoke", install)
    with pytest.raises(ValueError, match="differs from the immutable"):
        driver.indexed_smoke()
    install.assert_not_called()


def test_cargo_bootstrap_selects_the_full_msvc_linker_path(tmp_path, monkeypatch):
    tools = tmp_path / "MSVC tools"
    linker = tools / "bin/Hostx64/x64/link.exe"
    linker.parent.mkdir(parents=True)
    linker.touch()
    output = tmp_path / "github-env"
    monkeypatch.setenv("VCToolsInstallDir", str(tools))
    monkeypatch.setenv("GITHUB_ENV", str(output))
    driver.msvc_linker()
    assert (
        output.read_text().strip()
        == f"CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER={linker.as_posix()}"
    )


def test_windows_configuration_ignores_the_msvc_platform_variable(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(driver.sys, "platform", "win32")
    monkeypatch.setattr(driver.platform, "machine", lambda: "AMD64")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("GITHUB_ENV", str(tmp_path / "github-env"))
    monkeypatch.setenv("MANIFEST", "vane-extension.toml")
    monkeypatch.setenv("PLATFORM", "x64")  # Set by the MSVC developer environment.
    monkeypatch.setenv("VANE_PROVIDER_PLATFORM", "windows")
    driver.configure()
    assert (
        "VCPKG_TARGET_TRIPLET=x64-windows-static-release"
        in (tmp_path / "github-env").read_text()
    )
    monkeypatch.setenv("VANE_PROVIDER_PLATFORM", "macos")
    with pytest.raises(ValueError, match="does not match the native runner"):
        driver.configure()


@pytest.mark.parametrize("succeed", [True, False])
def test_manylinux_metadata_retry_is_bounded_and_preserves_package_selection(
    tmp_path, monkeypatch, succeed
):
    import os

    tools = tmp_path / "bin"
    tools.mkdir()
    state = tmp_path / "dnf-log"
    dnf = tools / "dnf"
    dnf.write_text(
        f"""#!{sys.executable}
import json, pathlib, sys
path = pathlib.Path({str(state)!r})
calls = json.loads(path.read_text()) if path.exists() else []
calls.append(sys.argv[1:])
path.write_text(json.dumps(calls))
attempt = sum('install' in call for call in calls)
sys.exit(0 if 'clean' in sys.argv or ({succeed!r} and attempt == 3) else 1)
"""
    )
    dnf.chmod(0o755)
    sleep = tools / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    sleep.chmod(0o755)
    monkeypatch.setenv("PATH", str(tools) + os.pathsep + os.environ["PATH"])
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/install_manylinux_deps.sh"), "binutils", "file"],
        capture_output=True,
        text=True,
    )
    import json

    calls = json.loads(state.read_text())
    installs = [call for call in calls if "install" in call]
    assert installs == [["--refresh", "install", "-y", "binutils", "file"]] * 3
    assert len([call for call in calls if "clean" in call]) == 2
    assert (result.returncode == 0) is succeed


def test_reusable_jobs_keep_signing_keys_out_and_preserve_artifact_ids():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    for platform in ("macos", "windows"):
        prepare = workflow["jobs"][f"vane-provider-prepare-{platform}"]
        package = workflow["jobs"][f"vane-testpypi-wheels-{platform}"]
        assert prepare["needs"] == "provider-release-preflight"
        assert "secrets" not in prepare and "secrets" not in package
        assert (
            package["with"]["prepared-id"]
            == f"${{{{ needs.vane-provider-prepare-{platform}.outputs.artifact_id }}}}"
        )
        assert (
            package["with"]["signed-id"]
            == "${{ needs.vane-provider-sign.outputs.artifact_id }}"
        )
    for filename in ("_vane_provider_native.yml", "_vane_provider_package.yml"):
        reusable = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
        assert reusable["permissions"] == {"contents": "read"}
        assert "secrets" not in reusable[True]["workflow_call"]
        for job in reusable["jobs"].values():
            assert "environment" not in job
            for step in job["steps"]:
                assert "secrets." not in str(step) and "secrets[" not in str(step)
                if step.get("uses", "").startswith("actions/download-artifact@"):
                    assert "artifact-ids" in step["with"]
                    assert step["with"]["digest-mismatch"] == "error"
