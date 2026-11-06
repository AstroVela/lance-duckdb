# SPDX-License-Identifier: Apache-2.0
"""Compare PE policies against an original, immutable Windows CI artifact."""

from __future__ import annotations

import ctypes
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pefile


def audit(source: Path, artifact: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "\n".join(
                (
                    "import sys, json",
                    "from pathlib import Path",
                    "sys.path.insert(0, sys.argv[1])",
                    "from vane_packaging.extension_wheel import _validate_windows_pe_platform",
                    "print(json.dumps(_validate_windows_pe_platform(Path(sys.argv[2]).read_bytes(), 'win_amd64', description='original Windows Lance', interpreter_tag='cp312')))",
                )
            ),
            str(source.resolve()),
            str(artifact.resolve()),
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def main() -> None:
    old, fixed, artifact = map(Path, sys.argv[1:])
    pe = pefile.PE(str(artifact), fast_load=True)
    pe.parse_data_directories(
        directories=[pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_IMPORT"]]
    )
    names = [entry.dll.decode("ascii") for entry in pe.DIRECTORY_ENTRY_IMPORT]
    counts = Counter(name.casefold() for name in names)
    assert counts["kernel32.dll"] == 2, names
    print("Original import descriptors:", names)

    previous = audit(old, artifact)
    assert previous.returncode != 0, previous.stdout
    assert "duplicate PE import libraries" in previous.stderr, previous.stderr
    print("Old audit reproduces the duplicate import rejection")

    corrected = audit(fixed, artifact)
    assert corrected.returncode == 0, corrected.stderr
    dependencies = json.loads(corrected.stdout)
    assert dependencies == list(dict.fromkeys(name.casefold() for name in names))
    assert "pdh.dll" in dependencies
    assert "api-ms-win-core-synch-l1-2-0.dll" in dependencies
    print("Corrected audit dependencies:", dependencies)

    library = ctypes.WinDLL(str(artifact.resolve()))
    version = library.lance_version
    version.restype = ctypes.c_char_p
    assert version().startswith(b"v1.5"), version()
    print("Windows loaded the original DLL; Lance engine:", version().decode())


if __name__ == "__main__":
    main()
