"""Generate disposable Blob v2 fixtures and exercise the native FileHandle tests.

uv run --with pylance==9.0.1 --with pyarrow==22.0.0 python \
    test/scripts/run_blob_filesystem_tests.py BUILD/extension/lance/lance_blob_filesystem_test

The subprocess protocol keeps the DuckDB connection and prepared statements
alive while PyLance appends, compacts, replaces, and removes historical metadata.
No test-only SQL functions or production mutation APIs are introduced.
"""

import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile

import lance
from lance.blob import Blob, BlobArray
import pyarrow as pa


METADATA = {
    "lance-encoding:blob-inline-size-threshold": "32",
    "lance-encoding:blob-dedicated-size-threshold": "128",
    "lance-encoding:blob-pack-file-size-threshold": "1024",
}


def fixture(directory, stable=False, external_uri=None):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    external = directory / "external space &% 中文.bin"
    external.write_bytes(b"PREFIX!external-slice-data!SUFFIX")
    target = external_uri or external.as_uri()
    unknown_target = target.replace("external-a.bin", "external-b.bin") if external_uri else target
    blobs = BlobArray.from_pylist(
        [
            b"abc",
            bytes(range(64)),
            bytes(range(64, 144)),
            bytes(i % 251 for i in range(2 * 1024 * 1024 + 257)),
            Blob.from_uri(target, 7, len(b"external-slice-data!")),
            Blob.from_uri(unknown_target),
            b"",
            None,
        ]
    )
    field = pa.field("video", blobs.type, metadata=METADATA)
    info = pa.StructArray.from_arrays([blobs], fields=[field.with_name("clip.with.dot")])
    clips = pa.ListArray.from_arrays(pa.array(range(9), type=pa.int32()), blobs)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            field,
            pa.field("info", info.type),
            pa.field("clips", clips.type),
            pa.field("plain", pa.binary()),
        ]
    )
    table = pa.Table.from_arrays(
        [pa.array(range(8), type=pa.int64()), blobs, info, clips, pa.array([b"plain"] * 8)],
        schema=schema,
    )
    path = directory / "table space &% 中文.lance"
    lance.write_dataset(
        table, path, data_storage_version="2.2", enable_stable_row_ids=stable, allow_external_blob_outside_bases=True
    )
    return path, external, table


def run(binary, stable):
    with tempfile.TemporaryDirectory(prefix="lance-blob-filesystem-") as directory:
        path, external, table = fixture(directory, stable)
        # Lance 9.0.1 cannot compact nested Blob fields. Keep snapshot/compaction
        # coverage on a separate top-level fixture; the main table tests nesting.
        snapshot_path = Path(directory) / "snapshot.lance"
        snapshot_table = table.drop(["info", "clips"])
        lance.write_dataset(
            snapshot_table,
            snapshot_path,
            data_storage_version="2.2",
            enable_stable_row_ids=stable,
            allow_external_blob_outside_bases=True,
        )
        payloads = {file: file.read_bytes() for file in path.rglob("*.blob") if file.stat().st_size > 1024 * 1024}
        assert payloads
        process = subprocess.Popen(
            [str(binary), str(path), str(external), str(snapshot_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            for line in process.stdout:
                print(line, end="", flush=True)
                if not line.startswith("MUTATE "):
                    continue
                operation = line.strip().split()[1]
                if operation == "truncate_payload":
                    for payload, payload_bytes in payloads.items():
                        payload.write_bytes(payload_bytes[:100])
                elif operation == "restore_payload":
                    for payload, payload_bytes in payloads.items():
                        payload.write_bytes(payload_bytes)
                elif operation == "append":
                    append = snapshot_table.slice(0, 1).set_column(0, "id", pa.array([8], type=pa.int64()))
                    lance.write_dataset(append, snapshot_path, mode="append", allow_external_blob_outside_bases=True)
                elif operation == "compact":
                    lance.dataset(snapshot_path).optimize.compact_files(target_rows_per_fragment=1024)
                elif operation == "replace":
                    shutil.rmtree(snapshot_path)
                    lance.write_dataset(
                        snapshot_table,
                        snapshot_path,
                        data_storage_version="2.2",
                        enable_stable_row_ids=stable,
                        allow_external_blob_outside_bases=True,
                    )
                elif operation == "remove_versions":
                    shutil.rmtree(snapshot_path / "_versions")
                else:
                    raise AssertionError(operation)
                process.stdin.write("OK\n")
                process.stdin.flush()
            if process.wait():
                raise RuntimeError(f"Blob filesystem tests failed (stable_row_ids={stable})")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    assert lance.__version__ == "9.0.1"
    for stable in (False, True):
        run(args.binary.resolve(), stable)


if __name__ == "__main__":
    main()
