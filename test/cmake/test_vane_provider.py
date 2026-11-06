#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compile the production provider linker rules without a Rust/engine build."""

from __future__ import annotations

import ctypes
import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("provider_builder", ROOT / "scripts/build_vane_dynamic_wheel.py")
assert SPEC and SPEC.loader
BUILDER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = BUILDER
SPEC.loader.exec_module(BUILDER)


class ProviderLinkTests(unittest.TestCase):
    def test_native_link_exports_the_entry_point_and_hides_private_symbols(self):
        with tempfile.TemporaryDirectory(prefix="vane provider link ") as value:
            root = Path(value)
            extension = root / "extension"
            extension.mkdir()
            (extension / "extension_config_vane.cmake").write_text("list(APPEND DUCKDB_EXTENSION_NAMES lance)\n")
            configuration = root / "configuration"
            configuration.mkdir()
            config = BUILDER._write_loadable_extension_config(extension, configuration)
            child = root / "duckdb/extension/lance"
            child.mkdir(parents=True)
            (root / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\n"
                "project(provider_fixture LANGUAGES CXX)\n"
                "set(CMAKE_MSVC_RUNTIME_LIBRARY MultiThreaded)\n"
                "add_subdirectory(duckdb EXCLUDE_FROM_ALL)\n"
                "add_library(_native SHARED native.cpp)\n"
            )
            (root / "duckdb/CMakeLists.txt").write_text(
                "project(engine_fixture LANGUAGES CXX)\n" "add_subdirectory(extension/lance)\n"
            )
            (child / "CMakeLists.txt").write_text(
                "project(lance LANGUAGES CXX)\n"
                f'include("{config.as_posix()}")\n'
                f'add_library(lance_loadable_extension SHARED "{(root / "provider.cpp").as_posix()}")\n'
                f'add_library(crypto_fixture STATIC "{(root / "crypto.cpp").as_posix()}")\n'
                "set_target_properties(crypto_fixture PROPERTIES POSITION_INDEPENDENT_CODE ON)\n"
                "target_link_libraries(lance_loadable_extension PRIVATE crypto_fixture)\n"
                'set_target_properties(lance_loadable_extension PROPERTIES OUTPUT_NAME lance PREFIX "" SUFFIX .duckdb_extension)\n'
                "if(MSVC)\n"
                "  set_target_properties(lance_loadable_extension PROPERTIES\n"
                '    RUNTIME_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}/extension/lance")\n'
                "  add_custom_command(TARGET crypto_fixture POST_BUILD\n"
                f'    COMMAND "{Path(sys.executable).as_posix()}" "{(ROOT / "scripts/hide_msvc_archive_exports.py").as_posix()}" "$<TARGET_FILE:crypto_fixture>"\n'
                "    VERBATIM)\n"
                "endif()\n"
                f'include("{(ROOT / "cmake/vane_provider_link.cmake").as_posix()}")\n'
                "lance_configure_vane_provider(lance_loadable_extension)\n"
            )
            export = (
                "#ifdef _WIN32\n#define ENTRY __declspec(dllexport)\n"
                '#else\n#define ENTRY __attribute__((visibility("default")))\n#endif\n'
            )
            (root / "native.cpp").write_text(export + 'extern "C" ENTRY int native_entry() { return 7; }\n')
            (root / "provider.cpp").write_text(
                export + 'extern "C" int private_crypto_symbol();\n'
                'extern "C" ENTRY int lance_duckdb_cpp_init() { return 35 + private_crypto_symbol(); }\n'
                'extern "C" int private_engine_symbol() { return 7; }\n'
            )
            (root / "crypto.cpp").write_text(export + 'extern "C" ENTRY int private_crypto_symbol() { return 7; }\n')
            generator = "Visual Studio 17 2022" if sys.platform == "win32" else "Ninja"
            subprocess.run(
                [
                    "cmake",
                    "-S",
                    str(root),
                    "-B",
                    str(root / "build"),
                    "-G",
                    generator,
                    "-DCMAKE_BUILD_TYPE=Release",
                ],
                check=True,
            )
            subprocess.run(
                [
                    "cmake",
                    "--build",
                    str(root / "build"),
                    "--target",
                    "_native",
                    "--config",
                    "Release",
                ],
                check=True,
            )
            # Vane excludes its engine subtree from the top-level build. With
            # Visual Studio, an unrelated provider is also omitted from the
            # top-level solution unless it has an explicit build dependency.
            subprocess.run(
                [
                    "cmake",
                    "--build",
                    str(root / "build"),
                    "--target",
                    "lance_loadable_extension",
                    "--config",
                    "Release",
                ],
                check=True,
            )
            suffix = ".dll" if sys.platform == "win32" else ".dylib" if sys.platform == "darwin" else ".so"
            artifact = BUILDER._provider_artifact(root / "build")
            expected = root / "build" / ("extension/lance" if sys.platform == "win32" else "duckdb/extension/lance")
            self.assertEqual(artifact.parent, expected)
            native_artifacts = list((root / "build").rglob(f"*_native{suffix}"))
            self.assertEqual(len(native_artifacts), 1)
            native = ctypes.CDLL(str(native_artifacts[0]))
            self.assertEqual(native.native_entry(), 7)
            with self.assertRaises(AttributeError):
                native.lance_duckdb_cpp_init
            library = ctypes.CDLL(str(artifact))
            self.assertEqual(library.lance_duckdb_cpp_init(), 42)
            with self.assertRaises(AttributeError):
                library.private_engine_symbol
            with self.assertRaises(AttributeError):
                library.private_crypto_symbol
            # Windows holds a loaded DLL open until process exit.
            if sys.platform == "win32":
                ctypes.windll.kernel32.FreeLibrary(ctypes.c_void_p(library._handle))
                ctypes.windll.kernel32.FreeLibrary(ctypes.c_void_p(native._handle))


if __name__ == "__main__":
    unittest.main()
