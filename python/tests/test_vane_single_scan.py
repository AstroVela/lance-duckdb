# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import vane

from lance_fixture import write_fixture_query
from packaged_dynamic_extension import load_packaged_dynamic_lance

pytestmark = pytest.mark.usefixtures("default_ray_runtime")


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _connect():
    connection = vane.connect(
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        }
    )
    if os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"):
        load_packaged_dynamic_lance(connection)
    else:
        connection.execute("LOAD lance")
    return connection


def _explain_json(connection, query: str) -> list[dict]:
    explain_type, payload = connection.execute(
        f"EXPLAIN (FORMAT JSON) {query}"
    ).fetchone()
    assert explain_type == "physical_plan"
    return json.loads(payload)


def _plan_nodes(plan: object):
    if isinstance(plan, dict):
        yield plan
        for value in plan.values():
            yield from _plan_nodes(value)
    elif isinstance(plan, list):
        for value in plan:
            yield from _plan_nodes(value)


def _lance_scan_info(plan: object) -> dict:
    matches = [
        node["extra_info"]
        for node in _plan_nodes(plan)
        if isinstance(node.get("extra_info"), dict)
        and "Lance Scan Backend" in node["extra_info"]
    ]
    assert len(matches) == 1
    return matches[0]


def test_wheel_owns_the_vane_lance_artifact() -> None:
    from vane import _native

    assert "site-packages" in Path(_native.__file__).resolve().parts
    connection = _connect()
    try:
        loaded, install_mode = connection.execute(
            "SELECT loaded, install_mode FROM duckdb_extensions() "
            "WHERE lower(extension_name) = 'lance'"
        ).fetchone()
        assert loaded is True
        assert str(install_mode).upper() == (
            "NOT_INSTALLED"
            if os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
            else "STATICALLY_LINKED"
        )
    finally:
        connection.close()


def test_ray_copy_rejects_an_unimplemented_receipt_contract(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "unsupported-copy.lance"
    try:
        with pytest.raises(Exception, match="copy_to_get_written_statistics"):
            connection.execute(
                f"COPY (SELECT 1::BIGINT AS id) TO {_sql_literal(path)} "
                "(FORMAT LANCE, MODE 'create')"
            )
        assert not path.exists()
    finally:
        connection.close()


def test_default_ray_lance_scan(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "default-ray.lance"
    try:
        write_fixture_query(
            connection,
            path,
            f"SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS value FROM range(12) AS source(i)",
            mode="create",
            max_rows_per_file=3,
        )
        rows = connection.execute(
            "SELECT id, value "
            f"FROM {_sql_literal(path)} "
            "WHERE id >= 3 AND id < 10 AND id % 2 = 0 "
            "ORDER BY id DESC LIMIT 3"
        ).fetchall()
        assert rows == [(8, "value-8"), (6, "value-6"), (4, "value-4")]
    finally:
        connection.close()


def test_default_ray_lance_writes_and_mutations(
    tmp_path: Path,
) -> None:
    connection = _connect()
    root = tmp_path / "default-ray-write"
    root.mkdir()
    try:
        connection.execute(f"ATTACH {_sql_literal(root)} AS lance_write (TYPE LANCE)")
        connection.execute(
            "CREATE TABLE lance_write.main.insert_target " "(id BIGINT, value VARCHAR)"
        )
        source = connection.sql(
            "SELECT i::BIGINT AS id, "
            "('value-' || i::VARCHAR)::VARCHAR AS value "
            "FROM range(12) AS source(i)"
        )

        source.insert_into("lance_write.main.insert_target")
        source.create("lance_write.main.ctas_target")
        connection.table("lance_write.main.insert_target").update(
            {"value": vane.lit("ray-update")},
            condition=vane.col("id") < 4,
        )
        connection.table("lance_write.main.insert_target").delete(
            condition=vane.col("id") >= 10
        )

        assert connection.execute(
            "SELECT id, value FROM lance_write.main.insert_target ORDER BY id"
        ).fetchall() == [
            (i, "ray-update" if i < 4 else f"value-{i}") for i in range(10)
        ]
        assert connection.execute(
            "SELECT id, value FROM lance_write.main.ctas_target ORDER BY id"
        ).fetchall() == [(i, f"value-{i}") for i in range(12)]
    finally:
        connection.close()


@pytest.mark.parametrize("numeric_type", ["FLOAT", "DOUBLE"])
def test_native_insert_preserves_numeric_lists_and_vectors(
    tmp_path: Path, monkeypatch, numeric_type: str
) -> None:
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    connection = _connect()
    root = tmp_path / "native-list-insert"
    root.mkdir()
    try:
        connection.execute(f"ATTACH {_sql_literal(root)} AS list_insert (TYPE LANCE)")
        connection.execute(
            "CREATE TABLE list_insert.main.target "
            f"(id BIGINT, metadata {numeric_type}[], embedding {numeric_type}[3])"
        )
        # Separate appends exercise all-null, empty, uniform and variable-length
        # metadata batches without changing the fixed-size embedding column.
        batches = [
            [(0, None), (1, None)],
            [(2, [])],
            [(3, [1.0]), (4, [2.0])],
            [(5, [3.0, 4.0]), (6, [5.0, 6.0, 7.0])],
        ]
        for batch in batches:
            values = ", ".join(
                f"({row_id}, {metadata if metadata is not None else 'NULL'}, [1, 2, 3])"
                for row_id, metadata in batch
            )
            connection.sql(
                f"SELECT id::BIGINT AS id, metadata::{numeric_type}[] AS metadata, "
                f"embedding::{numeric_type}[3] AS embedding "
                f"FROM (VALUES {values}) AS source(id, metadata, embedding)"
            ).insert_into("list_insert.main.target")

        assert connection.execute(
            "SELECT id, metadata, embedding FROM list_insert.main.target ORDER BY id"
        ).fetchall() == [
            (row_id, metadata, (1.0, 2.0, 3.0))
            for batch in batches
            for row_id, metadata in batch
        ]
        column_types = {
            row[0]: row[1]
            for row in connection.execute("DESCRIBE list_insert.main.target").fetchall()
        }
        assert column_types == {
            "id": "BIGINT",
            "metadata": f"{numeric_type}[]",
            "embedding": f"{numeric_type}[3]",
        }
    finally:
        connection.close()


def test_vane_rowid_in_uses_sql_membership_semantics(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "rowid-membership.lance"
    path_sql = _sql_literal(path)
    try:
        write_fixture_query(
            connection,
            path,
            f"SELECT i::BIGINT AS id FROM range(12) AS source(i)",
            mode="create",
            max_rows_per_file=3,
        )
        row_ids_by_id = dict(
            connection.execute(
                f"SELECT id, _rowid FROM {path_sql} ORDER BY id"
            ).fetchall()
        )
        selected_ids = (0, 1, 4, 4, 7, 10)
        selected_row_ids = ", ".join(
            str(row_ids_by_id[row_id]) for row_id in selected_ids
        )
        expected = [(row_id,) for row_id in (0, 1, 4, 7, 10)]

        for row_id_column in ("_rowid", "rowid"):
            predicate = f"{row_id_column} IN ({selected_row_ids}, NULL)"
            assert (
                connection.execute(
                    f"SELECT id FROM {path_sql} WHERE {predicate} ORDER BY id"
                ).fetchall()
                == expected
            )
            assert connection.execute(
                f"SELECT count(*) FROM {path_sql} WHERE {predicate}"
            ).fetchone() == (len(expected),)
            assert (
                connection.execute(
                    f"SELECT id FROM {path_sql} WHERE {row_id_column} IN (NULL)"
                ).fetchall()
                == []
            )

        assert connection.execute(
            "SELECT count(*) FROM (VALUES (0), (1), (4), (7), (10), (99)) "
            "AS ordinary(id) WHERE id IN (0, 1, 4, 4, 7, 10, NULL)"
        ).fetchone() == (len(expected),)
    finally:
        connection.close()


def test_vane_keeps_global_pushdowns_outside_lance(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "pushdown-boundaries.lance"
    path_sql = _sql_literal(path)
    try:
        write_fixture_query(
            connection,
            path,
            f"SELECT i::BIGINT AS id FROM range(20) AS source(i)",
            mode="create",
            max_rows_per_file=4,
        )

        aggregate_plan = _explain_json(
            connection, f"SELECT sum(id) FROM {path_sql} WHERE id >= 3"
        )
        aggregate_info = _lance_scan_info(aggregate_plan)
        assert "Lance Exec IR Bytes" not in aggregate_info
        assert any(
            "AGGREGATE" in str(node.get("name", "")).upper()
            for node in _plan_nodes(aggregate_plan)
        )

        limit_plan = _explain_json(
            connection, f"SELECT id FROM {path_sql} LIMIT 3 OFFSET 2"
        )
        limit_info = _lance_scan_info(limit_plan)
        assert limit_info["Lance Limit Offset Pushdown"] == "false"
        assert any(
            "LIMIT" in str(node.get("name", "")).upper()
            for node in _plan_nodes(limit_plan)
        )

        sample_plan = _explain_json(
            connection,
            f"SELECT id FROM {path_sql} TABLESAMPLE SYSTEM (50 PERCENT)",
        )
        sample_info = _lance_scan_info(sample_plan)
        assert sample_info["Lance Sampling Pushdown"] == "false"
        assert any(
            "SAMPLE" in str(node.get("name", "")).upper()
            for node in _plan_nodes(sample_plan)
        )
    finally:
        connection.close()
