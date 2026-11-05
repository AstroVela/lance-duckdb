#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Native-host orchestration shared by the macOS and Windows provider jobs."""

from __future__ import annotations

import hashlib
import os
import platform
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHONS = ("310", "311", "312", "313", "314")


def run(command, *, env=None):
    subprocess.run([str(value) for value in command], check=True, env=env)


def output(path, values):
    with Path(path).open("a", encoding="utf-8") as destination:
        for key, value in values.items():
            if "\n" in str(value) or "\r" in str(value):
                raise ValueError("workflow values cannot contain newlines")
            destination.write(f"{key}={value}\n")


def configure():
    workspace = Path(os.environ["GITHUB_WORKSPACE"])
    if sys.platform == "darwin" and platform.machine().lower() == "arm64":
        triplet = "arm64-osx-release"
    elif sys.platform == "win32" and platform.machine().lower() in {"amd64", "x86_64"}:
        triplet = "x64-windows-static-release"
    else:
        raise ValueError("this workflow requires native macOS arm64 or Windows x64")
    expected_platform = "macos" if sys.platform == "darwin" else "windows"
    if os.environ.get("PLATFORM", expected_platform) != expected_platform:
        raise ValueError("the requested platform does not match the native runner")
    paths = {
        "VANE_EXTENSION_ROOT": workspace / "extension",
        "VANE_MANIFEST": workspace / "extension" / os.environ["MANIFEST"],
        "VANE_SOURCE_DIR": workspace / "vane",
        "VANE_PROVIDER_PACKAGING_SOURCE": workspace / "provider-packaging",
        "VANE_EXTENSION_VCPKG_ROOT": workspace / "vcpkg",
        "VANE_VCPKG_ROOT": workspace / "vane-vcpkg",
        "VANE_VCPKG_INSTALLED_DIR": workspace / "vane/vcpkg_installed",
        "VANE_DYNAMIC_BUILD_DIR": workspace / "build/vane-provider",
        "VANE_DYNAMIC_DIST_DIR": workspace / "build/vane-provider-dist",
        "VCPKG_DEFAULT_BINARY_CACHE": workspace / ".cache/vcpkg-archives",
        "VANE_CARGO_ABOUT_ROOT": workspace / ".cache/cargo-about",
        "VANE_CARGO_ABOUT": workspace
        / (
            ".cache/cargo-about/bin/cargo-about.exe"
            if sys.platform == "win32"
            else ".cache/cargo-about/bin/cargo-about"
        ),
        "VCPKG_TOOLCHAIN_PATH": workspace / "vcpkg/scripts/buildsystems/vcpkg.cmake",
    }
    values = {key: path.as_posix() for key, path in paths.items()}
    values.update(VCPKG_TARGET_TRIPLET=triplet, VCPKG_HOST_TRIPLET=triplet)
    if sys.platform == "darwin":
        values["MACOSX_DEPLOYMENT_TARGET"] = "15.0"
    output(os.environ["GITHUB_ENV"], values)


def package():
    arguments = []
    environment = os.environ.copy()
    for name in ("PIP_INDEX_URL", "PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS"):
        environment.pop(name, None)
    environment["PIP_CONFIG_FILE"] = os.devnull
    for version in PYTHONS:
        python = Path(os.environ[f"RUNTIME_PYTHON_{version}"])
        actual = subprocess.check_output(
            [
                str(python),
                "-I",
                "-c",
                "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')",
            ],
            text=True,
        ).strip()
        if actual != version:
            raise ValueError(f"runtime Python must be cp{version}, got cp{actual}")
        directory = Path(os.environ["GITHUB_WORKSPACE"]) / "runtimes" / version
        directory.mkdir(parents=True)
        run(
            (
                python,
                "-m",
                "pip",
                "download",
                "--disable-pip-version-check",
                "--no-cache-dir",
                "--no-deps",
                "--only-binary=:all:",
                "--index-url",
                os.environ["VANE_RUNTIME_INDEX_URL"],
                "--dest",
                directory,
                f"vane-ai=={os.environ['VANE_VERSION']}",
            ),
            env=environment,
        )
        wheels = list(directory.glob("vane_ai-*.whl"))
        if len(wheels) != 1:
            raise ValueError("expected one exact indexed runtime wheel")
        arguments.extend(("--runtime-python", python, "--runtime-wheel", wheels[0]))
    source = Path(os.environ["VANE_SOURCE_DIR"])
    revision = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    run(
        (
            sys.executable,
            "-I",
            ROOT / "scripts/build_vane_dynamic_wheel.py",
            "--phase",
            "package",
            "--extension-root",
            ROOT,
            "--manifest",
            os.environ["VANE_MANIFEST"],
            "--vane-source",
            source,
            "--vane-revision",
            revision,
            "--build-directory",
            os.environ["VANE_DYNAMIC_BUILD_DIR"],
            "--output-directory",
            os.environ["VANE_DYNAMIC_DIST_DIR"],
            "--signing-profile",
            os.environ["SIGNING_PROFILE"],
            "--bundle-directory",
            "prepared",
            "--signed-artifact",
            f"signed/{os.environ['PLATFORM']}/lance.duckdb_extension",
            *arguments,
        )
    )


def smoke():
    wheels = sorted(Path(os.environ["VANE_DYNAMIC_DIST_DIR"]).glob("*.whl"))
    if not wheels:
        raise ValueError("no wheels to qualify")
    with tempfile.TemporaryDirectory(prefix="lance-platform-smoke-") as value:
        root = Path(value)
        run((sys.executable, "-m", "venv", str(root / "venv")))
        python = root / (
            "venv/Scripts/python.exe" if sys.platform == "win32" else "venv/bin/python"
        )
        # Select cp312 explicitly; production directories also contain cp310/11/13/14.
        selected = [path for path in wheels if "-cp312-" in path.name]
        if not any(path.name.startswith("vane_ai-") for path in selected):
            run(
                (
                    python,
                    "-m",
                    "pip",
                    "install",
                    "--index-url",
                    os.environ["VANE_RUNTIME_INDEX_URL"],
                    "--no-deps",
                    f"vane-ai=={os.environ['VANE_VERSION']}",
                )
            )
        run(
            (
                python,
                "-m",
                "pip",
                "install",
                *selected,
                "grpcio>=1.42.0",
                "pylance==9.0.1",
            )
        )
        # Populate the indexed runtime's dependency closure from the normal index.
        if os.environ.get("VANE_VERSION"):
            run(
                (
                    python,
                    "-m",
                    "pip",
                    "install",
                    f"vane-ai=={os.environ['VANE_VERSION']}",
                )
            )
        run((python, "-m", "pip", "check"))
        for runner in ("local", "ray"):
            run(
                (
                    python,
                    "-I",
                    ROOT / "scripts/verify_vane_platform.py",
                    "--runner",
                    runner,
                    "--directory",
                    root / runner,
                )
            )


def indexed_smoke():
    directory = Path(os.environ["VANE_DYNAMIC_DIST_DIR"])
    directory.mkdir(parents=True)
    environment = os.environ.copy()
    for name in ("PIP_INDEX_URL", "PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS"):
        environment.pop(name, None)
    environment["PIP_CONFIG_FILE"] = os.devnull
    run(
        (
            sys.executable,
            "-m",
            "pip",
            "download",
            "--no-deps",
            "--only-binary=:all:",
            "--no-cache-dir",
            "--index-url",
            os.environ["INDEX_URL"],
            "--dest",
            directory,
            f"vane-extension-lance=={os.environ['LANCE_VERSION']}",
        ),
        env=environment,
    )
    wheels = list(directory.glob("vane_extension_lance-*.whl"))
    if len(wheels) != 1:
        raise ValueError("expected one indexed native provider wheel")
    expected = Path("expected") / wheels[0].name
    if (
        not expected.is_file()
        or hashlib.sha256(expected.read_bytes()).digest()
        != hashlib.sha256(wheels[0].read_bytes()).digest()
    ):
        raise ValueError("indexed provider differs from the immutable staged wheel")
    smoke()


def main():
    command = sys.argv[1]
    if command == "configure":
        configure()
    elif command == "package":
        package()
    elif command == "smoke":
        smoke()
    elif command == "indexed-smoke":
        indexed_smoke()
    elif command == "packaging-pin":
        config = tomllib.loads((ROOT / "vane-provider-release.toml").read_text())
        output(
            os.environ["GITHUB_OUTPUT"],
            {"packaging_revision": config["packaging"]["revision"]},
        )
    elif command == "toolchain":
        compiler = "cl" if sys.platform == "win32" else "clang"
        result = subprocess.run(
            [compiler] if compiler == "cl" else [compiler, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if compiler != "cl" and result.returncode:
            raise ValueError("cannot identify the compiler")
        output(
            os.environ["GITHUB_OUTPUT"],
            {
                "identity": hashlib.sha256(
                    result.stdout + os.environ.get("ImageVersion", "").encode()
                ).hexdigest()[:24]
            },
        )
    else:
        raise ValueError("unknown workflow operation")


if __name__ == "__main__":
    main()
