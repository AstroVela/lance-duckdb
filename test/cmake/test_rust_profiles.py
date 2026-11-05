# SPDX-License-Identifier: MIT

"""Exercise the real CMake build graph without compiling Lance's Rust crates."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
FAKE_CARGO = """
import json, os, sys
from pathlib import Path
args=sys.argv[1:]
profile=next(a.split('=',1)[1] for a in args if a.startswith('--profile='))
target=next(a.split('=',1)[1] for a in args if a.startswith('--target='))
directory=Path(os.environ['CARGO_TARGET_DIR'])/target/('debug' if profile=='dev' else 'release')
directory.mkdir(parents=True,exist_ok=True)
with open(os.environ['CARGO_TEST_LOG'],'a',encoding='utf-8') as output:
    output.write(json.dumps({'args':args,'profile':profile,'linker':os.environ.get('CARGO_TARGET_'+target.upper().replace('-','_')+'_LINKER')})+'\\n')
(directory/os.environ['CARGO_TEST_LIBRARY']).write_bytes(b'artifact')
"""


class RustProfileBuild(unittest.TestCase):
    def run_graph(self, generator, configurations, reproducible, msvc_linker=None):
        # MSBuild disables incremental tracking under the OS TEMP directory.
        # Use the repository's ignored build tree, as production builds do.
        test_root = REPOSITORY / "build" / "contract-tests"
        test_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="lance-cmake-", dir=test_root) as temporary:
            root = Path(temporary).resolve()
            assert root.is_relative_to(test_root.resolve())
            source, build, tools = (
                root / "source with spaces",
                root / "build with spaces",
                root / "tools",
            )
            source.mkdir()
            tools.mkdir()
            cargo = tools / "fake_cargo.py"
            cargo.write_text(FAKE_CARGO, encoding="utf-8")
            library = "lance_duckdb_ffi.lib" if os.name == "nt" else "liblance_duckdb_ffi.a"
            log = root / "calls.jsonl"
            (source / "Cargo.toml").write_text('[package]\nname="fixture"\nversion="0.0.0"\n')
            (source / "input.rs").write_text("source")
            helper = (REPOSITORY / "cmake/rust_ffi_build.cmake").as_posix()
            target = "x86_64-pc-windows-msvc" if msvc_linker else "fixture-target"
            (source / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\nproject(profiles NONE)\n"
                f"set(RUST_PLATFORM_TARGET {target})\n"
                f'set(RUST_STATICLIB_NAME "{library}")\n'
                f'set(LANCE_CARGO_COMMAND "{Path(sys.executable).as_posix()}" "{cargo.as_posix()}")\n'
                'set(RUST_TARGET_DIR "${CMAKE_BINARY_DIR}/cargo")\n'
                f'set(RUST_DEBUG_LIB "${{RUST_TARGET_DIR}}/{target}/debug/{library}")\n'
                f'set(RUST_RELEASE_LIB "${{RUST_TARGET_DIR}}/{target}/release/{library}")\n'
                'set(RUST_FFI_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/input.rs")\n'
                "set(RUST_CARGO_FEATURE_ARGS --features vane-distributed)\n"
                + ("set(RUST_CARGO_REPRODUCIBILITY_ARGS --locked)\n" if reproducible else "")
                + 'set(RUST_CARGO_ENV "CARGO_TARGET_DIR=${RUST_TARGET_DIR}")\n'
                + (f'set(WIN32 TRUE)\nset(MSVC TRUE)\nset(CMAKE_LINKER "{msvc_linker}")\n' if msvc_linker else "")
                + f'include("{helper}")\nlance_add_rust_ffi_build("${{CMAKE_CURRENT_SOURCE_DIR}}/Cargo.toml")\n',
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment.update(
                PATH=str(tools) + os.pathsep + environment.get("PATH", ""),
                CARGO_TEST_LOG=str(log),
                CARGO_TEST_LIBRARY=library,
            )
            command = ["cmake", "-S", str(source), "-B", str(build), "-G", generator]
            if generator == "Ninja":
                command += ["-DCMAKE_BUILD_TYPE=" + configurations[0]]
            self.command(command, environment)
            for index, configuration in enumerate(configurations):
                command = [
                    "cmake",
                    "--build",
                    str(build),
                    "--target",
                    "lance_duckdb_ffi_build",
                    "--config",
                    configuration,
                ]
                self.command(command, environment)
                calls = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
                expected = "dev" if configuration.lower() == "debug" else "release"
                self.assertEqual(len(calls), index + 1)
                self.assertEqual(calls[-1]["profile"], expected)
                self.assertEqual("--locked" in calls[-1]["args"], reproducible)
                self.assertIn("vane-distributed", calls[-1]["args"])
                self.assertEqual(calls[-1]["linker"], msvc_linker)
                # An up-to-date build must not invoke Cargo for either profile.
                self.command(command, environment)
                self.assertEqual(len(log.read_text(encoding="utf-8").splitlines()), index + 1)

    def command(self, command, environment):
        result = subprocess.run(
            command,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_multi_config_builds_only_the_requested_profile(self):
        for reproducible in (False, True):
            with self.subTest(reproducible=reproducible):
                self.run_graph("Ninja Multi-Config", ["Debug", "Release"], reproducible)

    def test_single_config_builds_only_the_requested_profile(self):
        for configuration in ("Debug", "Release", "RelWithDebInfo", "MinSizeRel"):
            with self.subTest(configuration=configuration):
                self.run_graph("Ninja", [configuration], True)

    def test_msvc_uses_cmake_linker_instead_of_git_bash_link(self):
        self.run_graph("Ninja", ["Release"], True, "C:/MSVC tools/bin/link.exe")

    @unittest.skipUnless(os.name == "nt", "Visual Studio generator")
    def test_visual_studio_builds_only_the_requested_profile(self):
        self.run_graph("Visual Studio 17 2022", ["Debug", "Release"], True)


if __name__ == "__main__":
    if not shutil.which("cmake") or not shutil.which("ninja"):
        raise SystemExit("cmake and ninja are required for the build contracts")
    unittest.main()
