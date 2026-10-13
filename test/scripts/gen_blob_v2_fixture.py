"""Generate the Blob v2 descriptor regression fixture with pylance 9.0.1.

Run: uv run --with pylance==9.0.1 --with pyarrow==22.0.0 python \
    test/scripts/gen_blob_v2_fixture.py

The external URI intentionally does not exist: descriptor reads must not fetch
payloads. Tiny encoding thresholds exercise all four descriptor kinds without
committing large binary files. Regeneration replaces only this fixture.
"""

from pathlib import Path
import random
import shutil

import lance
from lance.blob import Blob, BlobArray
import pyarrow as pa


OUT = Path(__file__).resolve().parents[2] / "test/data/blob_v2_fixture.lance"
METADATA = {
    "lance-encoding:blob-inline-size-threshold": "32",
    "lance-encoding:blob-dedicated-size-threshold": "128",
    "lance-encoding:blob-pack-file-size-threshold": "1024",
}


def main():
    assert lance.__version__ == "9.0.1", "Use the version pinned above"
    blobs = BlobArray.from_pylist(
        [
            b"abc",
            b"p" * 64,
            b"d" * 256,
            Blob.from_uri("s3://blob-pr1-does-not-exist.invalid/video.mp4", 7, 19),
            b"",
            None,
        ]
    )
    blob_field = pa.field("video", blobs.type, metadata=METADATA)
    rng = random.Random(1729)
    info = pa.StructArray.from_arrays(
        [blobs, pa.array([1, 2, 3, 4, 5, 6], type=pa.float16())],
        fields=[blob_field, pa.field("half", pa.float16())],
        mask=pa.array([False, False, False, False, True, False]),
    )
    clips_type = pa.list_(blob_field.with_name("item"))
    clips = pa.ListArray.from_arrays(pa.array(range(7), type=pa.int32()), blobs, type=clips_type)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            blob_field,
            pa.field("required_video", blobs.type, nullable=False, metadata=METADATA),
            pa.field("info", info.type),
            pa.field("clips", clips_type),
            pa.field("plain", pa.large_binary()),
            pa.field("lookalike", blobs.storage.type),
            pa.field("vec", pa.list_(pa.float32(), 2)),
            pa.field("text", pa.utf8()),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(range(6), type=pa.int64()),
            blobs,
            BlobArray.from_pylist([b"ok"] * 6),
            info,
            clips,
            pa.array([rng.randbytes(2048) for _ in range(6)], type=pa.large_binary()),
            blobs.storage,
            pa.array([[float(i), 0.0] for i in range(6)], type=schema.field("vec").type),
            pa.array(["puppy video"] * 6),
        ],
        schema=schema,
    )
    if OUT.exists():
        shutil.rmtree(OUT)
    dataset = lance.write_dataset(
        table,
        OUT,
        data_storage_version="2.2",
        allow_external_blob_outside_bases=True,
    )
    descriptors = dataset.to_table(columns=["video"]).column("video").to_pylist()
    assert [None if x is None else x["kind"] for x in descriptors] == [0, 1, 2, 3, 0, None]
    assert [None if x is None else x["size"] for x in descriptors] == [3, 64, 256, 19, 0, None]
    print(f"Wrote {OUT}: all four descriptor kinds, empty and null values")


if __name__ == "__main__":
    main()
