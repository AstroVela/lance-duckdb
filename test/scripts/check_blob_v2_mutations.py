"""Check DELETE/TRUNCATE preserve Blob storage schemas on disposable copies.

Requires pylance 9.0.1 and a DuckDB executable with the Lance extension linked.
Run after building: python test/scripts/check_blob_v2_mutations.py build/native/duckdb
"""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import lance


def main():
    binary = Path(sys.argv[1]).resolve()
    fixture = Path(__file__).resolve().parents[2] / "test/data/blob_v2_fixture.lance"
    with tempfile.TemporaryDirectory(prefix="lance-blob-mutations-") as directory:
        dataset_path = Path(directory) / "videos.lance"
        shutil.copytree(fixture, dataset_path)
        original = lance.dataset(dataset_path).schema

        def sql(query):
            escaped_directory = directory.replace("'", "''")
            result = subprocess.run(
                [
                    str(binary),
                    "-json",
                    "-c",
                    f"ATTACH '{escaped_directory}' AS ns (TYPE LANCE); {query}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return json.loads(result.stdout)

        deleted = sql(
            "DELETE FROM ns.main.videos WHERE id = 0; "
            "SELECT count(*) AS remaining, sum(video.size)::BIGINT AS total FROM ns.main.videos;"
        )
        assert deleted == [{"remaining": 5, "total": 339}], deleted
        after_delete = lance.dataset(dataset_path)
        assert after_delete.schema.equals(original, check_metadata=True)
        assert after_delete.count_rows() == 5

        truncated = sql("TRUNCATE ns.main.videos; SELECT count(*) AS remaining FROM ns.main.videos;")
        assert truncated == [{"remaining": 0}], truncated
        after_truncate = lance.dataset(dataset_path)
        assert after_truncate.schema.equals(original, check_metadata=True)
        assert after_truncate.count_rows() == 0
    print("Blob DELETE/TRUNCATE preserved the logical storage schema")


if __name__ == "__main__":
    main()
