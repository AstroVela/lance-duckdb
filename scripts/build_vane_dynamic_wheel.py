#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Lance DuckDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Build and qualify self-contained Lance provider wheels for Vane."""

from __future__ import annotations

import argparse
import os
import platform
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Iterable, Sequence
from pathlib import Path

import tomllib
from packaging.tags import sys_tags

EXTENSION_NAME = "lance"
PROVIDER_PLATFORM_TAG = "manylinux_2_28_x86_64"
EXPECTED_RUST_RELEASE = "1.98.0"
SIGNING_PROFILES = {
    "ci-test": ("vane-ci-test-key", "VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY"),
    "testpypi": (
        "astrovela/vane-testpypi",
        "VANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY",
    ),
}
LICENSE_EXPRESSION = (
    "0BSD AND Apache-2.0 AND Apache-2.0 WITH LLVM-exception AND "
    "BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND CC0-1.0 AND "
    "CDLA-Permissive-2.0 AND ISC AND MIT AND MIT-0 AND MPL-2.0 AND "
    "NCSA AND OpenSSL AND Unicode-3.0 AND Unicode-DFS-2015 AND "
    "Unlicense AND Zlib AND bzip2-1.0.6 AND curl"
)
ALLOWED_RUNTIME_LIBRARIES = frozenset(
    {
        "ld-linux-x86-64.so.2",
        "libc.so.6",
        "libdl.so.2",
        "libgcc_s.so.1",
        "libm.so.6",
        "libpthread.so.0",
        "librt.so.1",
        "libstdc++.so.6",
    }
)
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_SIGNING_PRIVATE_KEY_BYTES = 64 * 1024
_MAX_BASE_LICENSE_BYTES = 64 * 1024 * 1024
_MAX_RUST_LICENSE_BYTES = 16 * 1024 * 1024
_MAX_RUST_STDLIB_LICENSE_BYTES = 2 * 1024 * 1024
_MAX_TESTPYPI_FILE_BYTES = 100_000_000


class QualificationError(RuntimeError):
    """Raised when an input or artifact violates the provider contract."""


def _run(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def _capture(command: Sequence[str], *, cwd: Path) -> str:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def _require_directory(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_dir():
        raise QualificationError(f"{description} is not a directory: {resolved}")
    return resolved


def _require_file(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise QualificationError(f"{description} is not a file: {resolved}")
    return resolved


def _destroy_file(path: Path) -> None:
    size = path.stat().st_size
    with path.open("r+b", buffering=0) as destination:
        destination.write(b"\0" * size)
        os.fsync(destination.fileno())
    path.unlink()


def _read_signing_private_key(path: Path, *, consume: bool) -> bytearray:
    unresolved = path.expanduser().absolute()
    if consume and unresolved.is_symlink():
        raise QualificationError(
            "consumed extension signing private key must not be a symbolic link"
        )
    resolved = _require_file(path, "extension signing private key")
    metadata = resolved.stat()
    if metadata.st_size <= 0 or metadata.st_size > _MAX_SIGNING_PRIVATE_KEY_BYTES:
        raise QualificationError("extension signing private key has an invalid size")
    if consume and (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise QualificationError(
            "consumed extension signing private key must be a private, owned regular file"
        )
    contents = bytearray(resolved.read_bytes())
    if consume:
        _destroy_file(resolved)
    return contents


def _require_git_revision(source: Path, expected: str, description: str) -> None:
    if _REVISION_RE.fullmatch(expected) is None:
        raise QualificationError(
            f"{description} expected revision is not a complete commit SHA: {expected!r}"
        )
    actual = _capture(("git", "rev-parse", "HEAD^{commit}"), cwd=source)
    if actual != expected:
        raise QualificationError(
            f"{description} checkout is {actual}, expected {expected}"
        )
    status = _capture(
        ("git", "status", "--porcelain", "--untracked-files=no"), cwd=source
    )
    if status:
        raise QualificationError(
            f"{description} checkout has tracked working-tree changes"
        )


def _one_wheel(directory: Path, pattern: str, description: str) -> Path:
    wheels = sorted(directory.glob(pattern))
    if len(wheels) != 1:
        raise QualificationError(
            f"expected exactly one {description}, found {len(wheels)} below {directory}"
        )
    return wheels[0]


def _platform_tag() -> str:
    if sys.platform != "linux" or platform.machine() != "x86_64":
        raise QualificationError("provider qualification requires Linux x86_64")
    if PROVIDER_PLATFORM_TAG not in {candidate.platform for candidate in sys_tags()}:
        raise QualificationError(
            f"build host does not support {PROVIDER_PLATFORM_TAG!r}"
        )
    return PROVIDER_PLATFORM_TAG


def _vcpkg_revision(extension_root: Path) -> str:
    manifest = tomllib.loads(
        _require_file(
            extension_root / "vane-extension.toml", "Vane extension manifest"
        ).read_text(encoding="utf-8")
    )
    revision = manifest.get("vcpkg_commit")
    if not isinstance(revision, str) or _REVISION_RE.fullmatch(revision) is None:
        raise QualificationError(
            "vane-extension.toml must contain one complete vcpkg_commit"
        )
    return revision


def _require_vcpkg_toolchain(path: Path, revision: str) -> Path:
    toolchain = _require_file(path, "vcpkg toolchain")
    try:
        vcpkg_root = toolchain.parents[2]
    except IndexError:
        raise QualificationError(
            f"vcpkg toolchain has an unexpected layout: {toolchain}"
        ) from None
    expected = (vcpkg_root / "scripts/buildsystems/vcpkg.cmake").resolve()
    if toolchain != expected:
        raise QualificationError(
            f"vcpkg toolchain has an unexpected layout: {toolchain}"
        )
    _require_git_revision(vcpkg_root, revision, "extension vcpkg")
    return toolchain


def _compiler_launcher_arguments() -> list[str]:
    launcher = os.environ.get("VANE_CMAKE_COMPILER_LAUNCHER", "")
    if not launcher:
        return []
    if launcher != "ccache":
        raise QualificationError("VANE_CMAKE_COMPILER_LAUNCHER must be ccache")
    return [
        "-DCMAKE_C_COMPILER_LAUNCHER=ccache",
        "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
    ]


def _write_loadable_extension_config(
    extension_root: Path, build_directory: Path
) -> Path:
    extension_config = _require_file(
        extension_root / "extension_config_vane.cmake",
        "Vane extension configuration",
    )
    config = build_directory / "vane-lance-loadable-config.cmake"
    config.write_text(
        "# Generated by scripts/build_vane_dynamic_wheel.py.\n"
        f'include("{extension_config.as_posix()}")\n'
        'list(FIND DUCKDB_EXTENSION_NAMES "lance" _VANE_LANCE_INDEX)\n'
        "if(_VANE_LANCE_INDEX EQUAL -1)\n"
        '  message(FATAL_ERROR "Lance extension config did not register lance")\n'
        "endif()\n"
        "set(DUCKDB_EXTENSION_LANCE_SHOULD_LINK FALSE)\n",
        encoding="utf-8",
    )
    return config


def _build_environment(
    *,
    extension_root: Path,
    build_directory: Path,
    vane_vcpkg_installed: Path,
    vcpkg_toolchain: Path,
    jobs: int,
    signing_cmake_option: str,
) -> dict[str, str]:
    target_triplet = "x64-linux"
    dependency_prefix = vane_vcpkg_installed / target_triplet
    _require_file(
        dependency_prefix / "share/protobuf/protobuf-config.cmake",
        "Vane Protobuf configuration",
    )
    prefix_config = build_directory / "vane-provider-dependency-prefix.cmake"
    prefix_config.write_text(
        "# Generated by scripts/build_vane_dynamic_wheel.py.\n"
        f'list(PREPEND CMAKE_PREFIX_PATH "{dependency_prefix}")\n',
        encoding="utf-8",
    )
    extension_config = _write_loadable_extension_config(extension_root, build_directory)
    cmake_arguments = [
        "--fresh",
        "-DBUILD_DISTRIBUTED_EXCHANGE=ON",
        "-DENABLE_EXTENSION_AUTOLOADING=OFF",
        "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
        "-DEXTENSION_STATIC_BUILD=ON",
        "-DLANCE_VANE_DISTRIBUTED=ON",
        "-DLANCE_VANE_DYNAMIC_PROVIDER=ON",
        f"-D{signing_cmake_option}=ON",
        f"-DDUCKDB_EXTENSION_CONFIGS={extension_config}",
        "-DVCPKG_BUILD=ON",
        f"-DCMAKE_TOOLCHAIN_FILE={vcpkg_toolchain}",
        f"-DVCPKG_MANIFEST_DIR={extension_root}",
        f"-DVCPKG_INSTALLED_DIR={build_directory / 'vcpkg_installed'}",
        f"-DVCPKG_TARGET_TRIPLET={target_triplet}",
        f"-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES={prefix_config}",
        *_compiler_launcher_arguments(),
    ]

    environment = os.environ.copy()
    selection_variables = {
        "VCPKG_CHAINLOAD_TOOLCHAIN_FILE",
        "VCPKG_DEFAULT_HOST_TRIPLET",
        "VCPKG_DEFAULT_TRIPLET",
        "VCPKG_OVERLAY_PORTS",
        "VCPKG_OVERLAY_TRIPLETS",
    }
    for name in tuple(environment):
        if (
            name
            in {
                "CMAKE_ARGS",
                "CMAKE_PREFIX_PATH",
                "COVERAGE",
                "DONT_LINK",
                "GITHUB_BASE_REF",
                "GITHUB_REF_NAME",
                "VANE_CMAKE_PREFIX_PATH",
                "VANE_CMAKE_COMPILER_LAUNCHER",
                "VANE_VERSION_BRANCH",
            }
            or name in selection_variables
            or name.startswith(("SETUPTOOLS_SCM_PRETEND_VERSION", "SKBUILD_"))
            or (name[:7] == "DUCKDB_" and name.endswith("_DIRECTORY"))
        ):
            environment.pop(name)
    environment.update(
        {
            "CMAKE_ARGS": shlex.join(cmake_arguments),
            "CMAKE_BUILD_PARALLEL_LEVEL": str(jobs),
            "CMAKE_GENERATOR": "Ninja",
            "SKBUILD_BUILD_DIR": str(build_directory),
            "SKBUILD_CMAKE_BUILD_TYPE": "Release",
            "VANE_VCPKG_INSTALLED_DIR": str(vane_vcpkg_installed),
            "VCPKG_MAX_CONCURRENCY": str(jobs),
            "VCPKG_TARGET_TRIPLET": target_triplet,
            "VCPKG_TOOLCHAIN_PATH": str(vcpkg_toolchain),
        }
    )
    return environment


def _require_self_contained_artifact(artifact: Path) -> None:
    file_report = _capture(("file", str(artifact)), cwd=artifact.parent)
    if (
        "ELF 64-bit" not in file_report
        or "stripped" not in file_report
        or "not stripped" in file_report
    ):
        raise QualificationError(f"{artifact.name} is not a stripped 64-bit ELF")

    dynamic = _capture(("readelf", "--dynamic", str(artifact)), cwd=artifact.parent)
    needed = frozenset(re.findall(r"Shared library: \[([^]]+)]", dynamic))
    unexpected = sorted(needed - ALLOWED_RUNTIME_LIBRARIES)
    if unexpected:
        raise QualificationError(
            f"{artifact.name} retains non-platform runtime libraries: {unexpected}"
        )
    if "(RPATH)" in dynamic or "(RUNPATH)" in dynamic:
        raise QualificationError(f"{artifact.name} retains a runtime search path")

    exported = frozenset(
        line.split()[0]
        for line in _capture(
            (
                "nm",
                "--dynamic",
                "--defined-only",
                "--extern-only",
                "--format=posix",
                str(artifact),
            ),
            cwd=artifact.parent,
        ).splitlines()
        if line.strip()
    )
    if exported != {"lance_duckdb_cpp_init"}:
        raise QualificationError(
            f"{artifact.name} exports unexpected symbols: {sorted(exported)}"
        )


def _require_base_wheel_free_of_lance(base_wheel: Path) -> None:
    with zipfile.ZipFile(base_wheel) as wheel:
        unexpected = sorted(
            name for name in wheel.namelist() if "lance" in name.lower()
        )
    if unexpected:
        raise QualificationError(
            f"base Vane wheel contains Lance provider paths: {unexpected}"
        )


def _render_base_license_bundle(base_wheel: Path) -> str:
    with zipfile.ZipFile(base_wheel) as wheel:
        members = sorted(
            (
                member
                for member in wheel.infolist()
                if ".dist-info/licenses/" in member.filename and not member.is_dir()
            ),
            key=lambda member: member.filename,
        )
        if not members:
            raise QualificationError("base Vane wheel contains no license files")
        total = sum(member.file_size for member in members)
        if total > _MAX_BASE_LICENSE_BYTES:
            raise QualificationError("base Vane wheel license bundle is too large")
        lines = [
            "Exact Vane runtime, DuckDB, and native dependency licenses",
            "===========================================================",
            "",
            f"Source wheel: {base_wheel.name}",
            "",
        ]
        for member in members:
            contents = wheel.read(member).decode("utf-8", errors="replace").strip()
            lines.extend(
                (
                    "=" * 80,
                    f"Wheel member: {member.filename}",
                    "=" * 80,
                    contents,
                    "",
                )
            )
    return "\n".join(lines).rstrip() + "\n"


def _stage_license_files(
    *,
    extension_root: Path,
    base_wheel: Path,
    cargo_about: Path,
    build_directory: Path,
) -> tuple[Path, ...]:
    license_directory = build_directory / "dynamic-extension-licenses"
    license_directory.mkdir(parents=True, exist_ok=True)

    project_license = license_directory / "Lance-DuckDB-Apache-2.0.txt"
    shutil.copyfile(
        _require_file(extension_root / "LICENSE", "Lance DuckDB license"),
        project_license,
    )
    base_bundle = license_directory / "Vane-runtime-licenses.txt"
    base_bundle.write_text(_render_base_license_bundle(base_wheel), encoding="utf-8")
    rust_bundle = license_directory / "Rust-third-party-licenses.txt"
    _run(
        (
            str(cargo_about),
            "generate",
            "--locked",
            "--all-features",
            "--fail",
            "--config",
            str(extension_root / "about.toml"),
            "--manifest-path",
            str(extension_root / "Cargo.toml"),
            "--output-file",
            str(rust_bundle),
            str(extension_root / "about.hbs"),
        ),
        cwd=extension_root,
    )
    if (
        not rust_bundle.is_file()
        or not 0 < rust_bundle.stat().st_size <= _MAX_RUST_LICENSE_BYTES
    ):
        raise QualificationError("cargo-about produced an invalid license bundle")

    rust_report = _capture(("rustc", "--version", "--verbose"), cwd=extension_root)
    release_matches = re.findall(r"^release: (.+)$", rust_report, flags=re.MULTILINE)
    if release_matches != [EXPECTED_RUST_RELEASE]:
        raise QualificationError(
            f"provider build requires rustc {EXPECTED_RUST_RELEASE}, found "
            f"{release_matches!r}"
        )
    rust_sysroot = Path(
        _capture(("rustc", "--print", "sysroot"), cwd=extension_root)
    ).resolve()
    rust_stdlib_source = _require_file(
        rust_sysroot / "share/doc/rust/COPYRIGHT-library.html",
        "Rust standard-library license bundle",
    )
    if not 0 < rust_stdlib_source.stat().st_size <= _MAX_RUST_STDLIB_LICENSE_BYTES:
        raise QualificationError("Rust standard-library license bundle is too large")
    rust_stdlib = license_directory / "Rust-standard-library-licenses.html"
    shutil.copyfile(rust_stdlib_source, rust_stdlib)
    return project_license, base_bundle, rust_bundle, rust_stdlib


def _builder_python(
    interpreter: Path, base_wheel: Path, parent: Path
) -> tuple[tempfile.TemporaryDirectory[str], Path]:
    temporary = tempfile.TemporaryDirectory(
        prefix="vane-lance-wheel-builder-", dir=parent
    )
    environment_root = Path(temporary.name)
    _run((str(interpreter), "-I", "-m", "venv", "--copies", str(environment_root)))
    python = environment_root / "bin/python"
    _run(
        (
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "packaging>=24.2",
            "tomli>=1.1; python_version < '3.11'",
            str(base_wheel),
        )
    )
    return temporary, python


def _build_provider_wheel(
    *,
    python: Path,
    vane_source: Path,
    artifact: Path,
    output_directory: Path,
    platform_tag: str,
    trust_identity: str,
    license_files: Iterable[Path],
) -> Path:
    command = [
        str(python),
        "-I",
        str(vane_source / "scripts/build_extension_wheel.py"),
        "--artifact",
        str(artifact),
        "--extension-name",
        EXTENSION_NAME,
        "--output-directory",
        str(output_directory),
        "--platform-tag",
        platform_tag,
        "--trust-identity",
        trust_identity,
        "--license-expression",
        LICENSE_EXPRESSION,
    ]
    for license_file in license_files:
        command.extend(("--license-file", str(license_file)))
    _run(command)
    wheel = _one_wheel(
        output_directory,
        "vane_extension_lance-*.whl",
        "vane-extension-lance wheel",
    )
    if wheel.stat().st_size > _MAX_TESTPYPI_FILE_BYTES:
        raise QualificationError(f"{wheel.name} exceeds TestPyPI's 100 MB file limit")
    return wheel


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", required=True, type=Path)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--vane-revision", required=True)
    parser.add_argument("--vane-vcpkg-installed", required=True, type=Path)
    parser.add_argument("--vcpkg-toolchain", required=True, type=Path)
    parser.add_argument("--cargo-about", required=True, type=Path)
    parser.add_argument("--build-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--jobs", default=8, type=int)
    parser.add_argument(
        "--signing-profile", required=True, choices=tuple(SIGNING_PROFILES)
    )
    parser.add_argument("--signing-private-key", required=True, type=Path)
    parser.add_argument("--consume-signing-private-key", action="store_true")
    runtime_group = parser.add_mutually_exclusive_group(required=True)
    runtime_group.add_argument("--package-local-runtime", action="store_true")
    runtime_group.add_argument(
        "--runtime-python", action="append", default=[], type=Path
    )
    parser.add_argument("--runtime-wheel", action="append", default=[], type=Path)
    return parser.parse_args()


def main() -> int:
    arguments = _parse_arguments()
    if arguments.jobs <= 0:
        raise QualificationError("--jobs must be a positive integer")
    if arguments.package_local_runtime and arguments.runtime_wheel:
        raise QualificationError(
            "--runtime-wheel cannot be combined with --package-local-runtime"
        )
    if len(arguments.runtime_python) != len(arguments.runtime_wheel):
        raise QualificationError(
            "--runtime-python and --runtime-wheel must be supplied equally"
        )
    if not arguments.package_local_runtime and not arguments.runtime_python:
        raise QualificationError("at least one indexed runtime pair is required")

    extension_root = _require_directory(arguments.extension_root, "extension root")
    vane_source = _require_directory(arguments.vane_source, "Vane source")
    vane_vcpkg_installed = _require_directory(
        arguments.vane_vcpkg_installed, "Vane vcpkg installation"
    )
    cargo_about = _require_file(arguments.cargo_about, "cargo-about executable")
    if not os.access(cargo_about, os.X_OK):
        raise QualificationError(f"cargo-about is not executable: {cargo_about}")
    trust_identity, signing_cmake_option = SIGNING_PROFILES[arguments.signing_profile]
    if (
        arguments.signing_profile == "testpypi"
        and not arguments.consume_signing_private_key
    ):
        raise QualificationError(
            "the TestPyPI signing profile requires --consume-signing-private-key"
        )
    indexed_runtimes = tuple(
        (
            _require_file(interpreter, "runtime Python interpreter"),
            _require_file(wheel, "indexed Vane runtime wheel"),
        )
        for interpreter, wheel in zip(
            arguments.runtime_python, arguments.runtime_wheel, strict=True
        )
    )
    if any(not os.access(interpreter, os.X_OK) for interpreter, _ in indexed_runtimes):
        raise QualificationError("one runtime Python interpreter is not executable")

    vcpkg_toolchain = _require_vcpkg_toolchain(
        arguments.vcpkg_toolchain, _vcpkg_revision(extension_root)
    )
    build_directory = arguments.build_directory.expanduser().resolve()
    output_directory = arguments.output_directory.expanduser().resolve()
    build_directory.mkdir(parents=True, exist_ok=True)
    output_directory.mkdir(parents=True, exist_ok=True)
    if list(output_directory.glob("*.whl")):
        raise QualificationError("output directory already contains a wheel")

    _require_git_revision(vane_source, arguments.vane_revision, "Vane")
    platform_tag = _platform_tag()
    environment = _build_environment(
        extension_root=extension_root,
        build_directory=build_directory,
        vane_vcpkg_installed=vane_vcpkg_installed,
        vcpkg_toolchain=vcpkg_toolchain,
        jobs=arguments.jobs,
        signing_cmake_option=signing_cmake_option,
    )
    signing_private_key_contents = _read_signing_private_key(
        arguments.signing_private_key,
        consume=arguments.consume_signing_private_key,
    )

    try:
        with tempfile.TemporaryDirectory(
            prefix="vane-base-wheel-", dir=build_directory.parent
        ) as base_output_value:
            base_output = Path(base_output_value)
            _run(
                (
                    sys.executable,
                    "-m",
                    "build",
                    "--wheel",
                    "--no-isolation",
                    "--outdir",
                    str(base_output),
                    str(vane_source),
                ),
                cwd=extension_root,
                environment=environment,
            )
            base_wheel = _one_wheel(base_output, "vane_ai-*.whl", "base Vane wheel")
            _require_base_wheel_free_of_lance(base_wheel)
            _run(
                (
                    "cmake",
                    "--build",
                    str(build_directory),
                    "--target",
                    "lance_loadable_extension",
                    "--parallel",
                    str(arguments.jobs),
                ),
                cwd=extension_root,
                environment=environment,
            )

            unsigned = _require_file(
                build_directory / "duckdb/extension/lance/lance.duckdb_extension",
                "unsigned Lance artifact",
            )
            _require_self_contained_artifact(unsigned)
            signed_directory = build_directory / "signed-vane-extensions"
            signed_directory.mkdir(parents=True, exist_ok=True)
            signed = signed_directory / unsigned.name
            key_handle, ephemeral_key_name = tempfile.mkstemp(
                prefix=".vane-extension-signing-",
                suffix=".pem",
                dir=signed_directory,
            )
            ephemeral_key = Path(ephemeral_key_name)
            try:
                with os.fdopen(key_handle, "wb") as key_output:
                    os.fchmod(key_output.fileno(), 0o600)
                    key_output.write(signing_private_key_contents)
                    key_output.flush()
                    os.fsync(key_output.fileno())
                _run(
                    (
                        sys.executable,
                        str(vane_source / "scripts/sign_test_dynamic_extension.py"),
                        "--private-key",
                        str(ephemeral_key),
                        str(unsigned),
                        str(signed),
                    )
                )
            finally:
                signing_private_key_contents[:] = b"\0" * len(
                    signing_private_key_contents
                )
                signing_private_key_contents.clear()
                if ephemeral_key.exists():
                    _destroy_file(ephemeral_key)

            licenses = _stage_license_files(
                extension_root=extension_root,
                base_wheel=base_wheel,
                cargo_about=cargo_about,
                build_directory=build_directory,
            )
            with tempfile.TemporaryDirectory(
                prefix="vane-qualified-wheels-", dir=output_directory.parent
            ) as staging_value:
                staging = Path(staging_value)
                emitted_base_wheel: Path | None = None
                if arguments.package_local_runtime:
                    repaired_base_directory = staging / "base"
                    repaired_base_directory.mkdir()
                    _run(
                        (
                            sys.executable,
                            "-m",
                            "auditwheel",
                            "repair",
                            "--plat",
                            platform_tag,
                            "--wheel-dir",
                            str(repaired_base_directory),
                            str(base_wheel),
                        )
                    )
                    emitted_base_wheel = _one_wheel(
                        repaired_base_directory,
                        "vane_ai-*.whl",
                        "repaired base Vane wheel",
                    )
                    runtimes = ((Path(sys.executable).resolve(), emitted_base_wheel),)
                else:
                    runtimes = indexed_runtimes

                provider_wheels: list[Path] = []
                for runtime_index, (runtime_python, runtime_wheel) in enumerate(
                    runtimes
                ):
                    _require_base_wheel_free_of_lance(runtime_wheel)
                    provider_directory = staging / f"extension-{runtime_index}"
                    provider_directory.mkdir()
                    builder_environment, builder_python = _builder_python(
                        runtime_python, runtime_wheel, build_directory.parent
                    )
                    try:
                        provider_wheel = _build_provider_wheel(
                            python=builder_python,
                            vane_source=vane_source,
                            artifact=signed,
                            output_directory=provider_directory,
                            platform_tag=platform_tag,
                            trust_identity=trust_identity,
                            license_files=licenses,
                        )
                        _run(
                            (
                                str(builder_python),
                                "-I",
                                str(vane_source / "scripts/verify_extension_wheel.py"),
                                "--base-wheel",
                                str(runtime_wheel),
                                "--extension-wheel",
                                str(provider_wheel),
                                "--extension-name",
                                EXTENSION_NAME,
                                "--trust-identity",
                                trust_identity,
                            )
                        )
                    finally:
                        builder_environment.cleanup()
                    provider_wheels.append(provider_wheel)

                wheels_to_emit = (
                    *((emitted_base_wheel,) if emitted_base_wheel else ()),
                    *provider_wheels,
                )
                for wheel in wheels_to_emit:
                    destination = output_directory / wheel.name
                    if destination.exists():
                        raise QualificationError(
                            f"multiple runtimes produced {destination.name}"
                        )
                    shutil.copyfile(wheel, destination)
                    print(destination)
    finally:
        signing_private_key_contents[:] = b"\0" * len(signing_private_key_contents)
        signing_private_key_contents.clear()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except QualificationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
