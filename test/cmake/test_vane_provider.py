#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compile the production provider linker rules without a Rust/engine build."""

from __future__ import annotations

import ctypes
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class ProviderLinkTests(unittest.TestCase):
    def test_native_link_exports_the_entry_point_and_hides_private_symbols(self):
        with tempfile.TemporaryDirectory(prefix="vane provider link ") as value:
            root = Path(value)
            (root / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\n"
                "project(provider_fixture LANGUAGES CXX)\n"
                "set(CMAKE_MSVC_RUNTIME_LIBRARY MultiThreaded)\n"
                "add_library(provider SHARED provider.cpp)\n"
                f'include("{(ROOT / "cmake/vane_provider_link.cmake").as_posix()}")\n'
                "lance_configure_vane_provider(provider)\n"
            )
            (root / "provider.cpp").write_text(
                "#ifdef _WIN32\n#define ENTRY __declspec(dllexport)\n"
                '#else\n#define ENTRY __attribute__((visibility("default")))\n#endif\n'
                'extern "C" ENTRY int lance_duckdb_cpp_init() { return 42; }\n'
                'extern "C" int private_engine_symbol() { return 7; }\n'
            )
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
                ["cmake", "--build", str(root / "build"), "--config", "Release"],
                check=True,
            )
            suffix = ".dll" if sys.platform == "win32" else ".dylib" if sys.platform == "darwin" else ".so"
            artifacts = list((root / "build").rglob(f"*provider{suffix}"))
            self.assertEqual(len(artifacts), 1)
            library = ctypes.CDLL(str(artifacts[0]))
            self.assertEqual(library.lance_duckdb_cpp_init(), 42)
            with self.assertRaises(AttributeError):
                library.private_engine_symbol
            # Windows holds a loaded DLL open until process exit.
            if sys.platform == "win32":
                ctypes.windll.kernel32.FreeLibrary(ctypes.c_void_p(library._handle))


if __name__ == "__main__":
    unittest.main()
