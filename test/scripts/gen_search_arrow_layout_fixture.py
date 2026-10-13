"""Generate nested 64-bit Arrow offset layouts for search regression tests.

Run: uv run --with pylance==9.0.1 --with pyarrow==22.0.0 python \
    test/scripts/gen_search_arrow_layout_fixture.py

The fixture combines Blob v2 fields and unmarked descriptor-shaped structs
with large lists, strings and binary values. SQL cannot preserve these Arrow
offset widths when writing a fixture through DuckDB's logical types.
"""

from pathlib import Path
import shutil

import lance
from lance.blob import BlobArray
import pyarrow as pa


OUT = Path(__file__).resolve().parents[2] / "test/data/search_arrow_layout_fixture.lance"


def main():
    assert lance.__version__ == "9.0.1", "Use the version pinned above"
    blobs = BlobArray.from_pylist([b"abc", b"defgh", None, b""])
    blob_field = pa.field("video", blobs.type)
    offsets = pa.array([0, 2, 3, 3, 3], type=pa.int64())
    mask = pa.array([False, False, False, True])
    clips = pa.LargeListArray.from_arrays(
        offsets, blobs.slice(0, 3), type=pa.large_list(blob_field.with_name("item")), mask=mask
    )

    descriptor_type = pa.struct(
        [
            ("kind", pa.uint8()),
            ("position", pa.uint64()),
            ("size", pa.uint64()),
            ("blob_id", pa.uint32()),
            ("blob_uri", pa.string()),
        ]
    )
    descriptors = pa.array(
        [
            {"kind": 7, "position": 11, "size": 17, "blob_id": 23, "blob_uri": "first"},
            {"kind": 8, "position": 13, "size": 19, "blob_id": 29, "blob_uri": "second"},
            None,
            None,
        ],
        type=descriptor_type,
    )
    lookalike = pa.LargeListArray.from_arrays(offsets, descriptors.slice(0, 3), mask=mask)
    labels = pa.array(["alpha", "duck \u4e2d", "", None], type=pa.large_string())
    payloads = pa.array([b"\x00\xffA", b"BC\x00D", b"", None], type=pa.large_binary())
    info = pa.StructArray.from_arrays(
        [blobs, labels, payloads, pa.array([1.5, -2.25, None, 0], type=pa.float16())],
        fields=[
            blob_field,
            pa.field("label", labels.type),
            pa.field("payload", payloads.type),
            pa.field("half", pa.float16()),
        ],
        mask=mask,
    )
    plain_info = pa.StructArray.from_arrays(
        [descriptors, labels, payloads], names=["descriptor", "label", "payload"], mask=mask
    )
    table = pa.table(
        {
            "id": pa.array(range(4), type=pa.int64()),
            "clips": clips,
            "info": info,
            "lookalike": lookalike,
            "plain_info": plain_info,
            "vec": pa.array([[float(i), 0.0] for i in range(4)], type=pa.list_(pa.float32(), 2)),
            "text": pa.array(["puppy video"] * 4),
        }
    )
    if OUT.exists():
        shutil.rmtree(OUT)
    dataset = lance.write_dataset(table, OUT, data_storage_version="2.2")
    read_schema = dataset.to_table().schema
    for name in ["clips", "lookalike"]:
        assert pa.types.is_large_list(read_schema.field(name).type)
    for name in ["info", "plain_info"]:
        assert pa.types.is_large_string(read_schema.field(name).type.field("label").type)
        assert pa.types.is_large_binary(read_schema.field(name).type.field("payload").type)
    print(f"Wrote {OUT}: Blob and unmarked descriptor layouts with 64-bit offsets")


if __name__ == "__main__":
    main()
