# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from pathlib import Path

import vane


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _connect():
    connection = vane.connect(
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        }
    )
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


def test_static_wheel_owns_the_vane_lance_artifact() -> None:
    from vane import _native

    assert "site-packages" in Path(_native.__file__).resolve().parts
    connection = _connect()
    try:
        loaded, install_mode = connection.execute(
            "SELECT loaded, install_mode FROM duckdb_extensions() "
            "WHERE lower(extension_name) = 'lance'"
        ).fetchone()
        assert loaded is True
        assert str(install_mode).upper() == "STATICALLY_LINKED"
    finally:
        connection.close()


def test_single_node_lance_scan(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "single-node.lance"
    try:
        connection.execute(
            "COPY (SELECT i::BIGINT AS id, "
            "('value-' || i::VARCHAR)::VARCHAR AS value "
            "FROM range(12) AS source(i)) "
            f"TO {_sql_literal(path)} "
            "(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 3)"
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


def test_vane_rowid_in_uses_sql_membership_semantics(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "rowid-membership.lance"
    path_sql = _sql_literal(path)
    try:
        connection.execute(
            "COPY (SELECT i::BIGINT AS id FROM range(12) AS source(i)) "
            f"TO {path_sql} (FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 3)"
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
        connection.execute(
            "COPY (SELECT i::BIGINT AS id FROM range(20) AS source(i)) "
            f"TO {path_sql} (FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 4)"
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
