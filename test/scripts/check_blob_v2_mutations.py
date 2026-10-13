"""Check DELETE predicates and Blob storage schemas on disposable copies.

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
                    "-batch",
                ],
                input=f"ATTACH '{escaped_directory}' AS ns (TYPE LANCE); {query}\n",
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

    # The optimizer moves these predicates into table_filters. A DELETE has
    # no scan-side DuckDB fallback, so incomplete translation must fail before
    # creating a version, even when another conjunct can be pushed.
    predicates = [
        ("video.kind = 3", [3]),
        ("video.size > 19", [1, 2]),
        ("id >= 2 AND video.kind = 3", [3]),
    ]
    for predicate, matches in predicates:
        with tempfile.TemporaryDirectory(prefix="lance-blob-delete-filter-") as directory:
            dataset_path = Path(directory) / "videos.lance"
            shutil.copytree(fixture, dataset_path)
            original = lance.dataset(dataset_path)
            attach = f"ATTACH '{directory}' AS ns (TYPE LANCE); "
            selected = subprocess.run(
                [str(binary), "-batch", "-json"],
                input=attach + f"SELECT list(id ORDER BY id) AS ids FROM ns.main.videos WHERE {predicate};\n",
                capture_output=True,
                text=True,
                check=True,
            )
            assert json.loads(selected.stdout) == [{"ids": matches}], selected.stdout
            rejected = subprocess.run(
                [str(binary), "-batch", "-json"],
                input=attach + f"DELETE FROM ns.main.videos WHERE {predicate};\n",
                capture_output=True,
                text=True,
            )
            assert rejected.returncode == 1, (predicate, rejected.stdout, rejected.stderr)
            assert "Lance DELETE cannot preserve all pushed table filters" in rejected.stderr, rejected.stderr
            after = lance.dataset(dataset_path)
            assert after.version == original.version
            assert after.schema.equals(original.schema, check_metadata=True)
            assert after.to_table(columns=["id"]).column("id").to_pylist() == list(range(6))

    # The complete IS NULL expression is supported with or without pushdown.
    # Also exercise an ordinary non-first column and an explicit transaction.
    for setup, predicate, remaining in [
        ("", "video IS NULL", [0, 1, 2, 3, 4]),
        ("PRAGMA disable_optimizer;", "video IS NULL", [0, 1, 2, 3, 4]),
        ("", "text = 'never'", list(range(6))),
        ("", "text = 'text'", list(range(6))),
        ("", "text = 'puppy video'", []),
        ("", "id = 1 OR id = 3", [0, 2, 4, 5]),
        ("", "id = 3", [0, 1, 2, 4, 5]),
    ]:
        with tempfile.TemporaryDirectory(prefix="lance-blob-delete-control-") as directory:
            dataset_path = Path(directory) / "videos.lance"
            shutil.copytree(fixture, dataset_path)
            original = lance.dataset(dataset_path).schema
            result = subprocess.run(
                [str(binary), "-batch", "-json"],
                input=(
                    f"ATTACH '{directory}' AS ns (TYPE LANCE); {setup} "
                    f"BEGIN; DELETE FROM ns.main.videos WHERE {predicate}; COMMIT;\n"
                ),
                capture_output=True,
                text=True,
            )
            assert result.returncode == 0, (setup, predicate, result.stdout, result.stderr)
            after = lance.dataset(dataset_path)
            assert after.schema.equals(original, check_metadata=True)
            assert after.to_table(columns=["id"]).column("id").to_pylist() == remaining, result.stdout
    print("Blob DELETE predicates and DELETE/TRUNCATE storage schemas passed")


if __name__ == "__main__":
    main()
