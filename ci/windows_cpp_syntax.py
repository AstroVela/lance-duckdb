"""Check real distributed-provider sources against the pinned Windows SDK."""

from pathlib import Path
import subprocess
import sys


root = Path(__file__).resolve().parents[1]
sdk = Path(sys.argv[1]).resolve() / "external/duckdb"
includes = [root / "src/include", sdk / "src/include"]
for directory in sorted((sdk / "third_party").iterdir()):
    if directory.is_dir():
        includes.append(directory)
        if (directory / "include").is_dir():
            includes.append(directory / "include")
flags = [
    "cl",
    "/nologo",
    "/Zs",
    "/EHsc",
    "/std:c++17",
    "/utf-8",
    "/DDUCKDB_STATIC_BUILD",
    "/DDUCKDB_BUILD_LOADABLE_EXTENSION",
    "/DLANCE_VANE_DISTRIBUTED",
    "/DLANCE_VANE_DYNAMIC_PROVIDER",
    *[f"/I{directory}" for directory in includes],
]
failed = []
for source in sorted((root / "src").glob("*.cpp")):
    result = subprocess.run(
        [*flags, str(source)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode:
        failed.append(source.name)
        print(
            f"FAILED {source.name}\n{(result.stdout + result.stderr)[-16000:]}",
            flush=True,
        )
    else:
        print(f"Checked {source.name}", flush=True)
if failed:
    raise SystemExit(f"Windows C++ syntax failed: {', '.join(failed)}")
