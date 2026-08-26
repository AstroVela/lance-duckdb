# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest

vane = pytest.importorskip("vane")


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
