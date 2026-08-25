# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest

import lance_duckdb as lance_module
from lance_duckdb import LANCE_EXTENSION_PATH_ENV, load_lance_extension


class RecordingConnection:
    def __init__(self) -> None:
        self.loaded: list[str] = []

    def load_extension(self, path: str) -> None:
        self.loaded.append(path)


class RecordingResult:
    def __init__(self, row: tuple[str] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[str] | None:
        return self.row


class RecordingStaticConnection(RecordingConnection):
    def __init__(self, install_mode: str | None) -> None:
        super().__init__()
        self.install_mode = install_mode
        self.executed: list[str] = []

    def execute(self, sql: str) -> RecordingResult:
        self.executed.append(sql)
        if sql == "LOAD lance":
            return RecordingResult(None)
        row = None if self.install_mode is None else (self.install_mode,)
        return RecordingResult(row)


def test_load_lance_extension_uses_explicit_preprovisioned_path(tmp_path: Path) -> None:
    artifact = tmp_path / "lance.duckdb_extension"
    artifact.write_bytes(b"artifact")
    connection = RecordingConnection()

    assert load_lance_extension(connection, artifact) is connection
    assert connection.loaded == [str(artifact.resolve())]


def test_load_lance_extension_uses_environment_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "lance.duckdb_extension"
    artifact.write_bytes(b"artifact")
    monkeypatch.setenv(LANCE_EXTENSION_PATH_ENV, str(artifact))
    connection = RecordingConnection()

    load_lance_extension(connection)

    assert connection.loaded == [str(artifact.resolve())]


def test_load_lance_extension_loads_statically_linked_lance_by_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LANCE_EXTENSION_PATH_ENV, raising=False)
    connection = RecordingStaticConnection("statically_linked")

    assert load_lance_extension(connection) is connection
    assert connection.loaded == []
    assert connection.executed[-1] == "LOAD lance"
    assert all(
        not sql.lstrip().upper().startswith("INSTALL ") for sql in connection.executed
    )


@pytest.mark.parametrize("install_mode", [None, "LOADABLE", "NOT_INSTALLED"])
def test_load_lance_extension_never_falls_back_to_install(
    monkeypatch: pytest.MonkeyPatch,
    install_mode: str | None,
) -> None:
    monkeypatch.delenv(LANCE_EXTENSION_PATH_ENV, raising=False)
    connection = RecordingStaticConnection(install_mode)

    with pytest.raises(ValueError, match=LANCE_EXTENSION_PATH_ENV):
        load_lance_extension(connection)

    assert connection.loaded == []
    assert all(
        not sql.lstrip().upper().startswith("INSTALL ") for sql in connection.executed
    )


def test_load_lance_extension_reports_connections_without_introspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LANCE_EXTENSION_PATH_ENV, raising=False)

    with pytest.raises(ValueError, match="statically linked"):
        load_lance_extension(RecordingConnection())


def test_ray_dispatch_keeps_unsupported_attached_ctas_on_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Relation:
        def create(self) -> None:
            pass

        def insert_into(self) -> None:
            pass

        def write_file(self) -> None:
            pass

    monkeypatch.setattr(lance_module, "_configured_runner_type", lambda: "ray")
    relation = Relation()

    assert not lance_module._relation_dispatch_available(relation, "create")
    assert lance_module._relation_dispatch_available(relation, "insert_into")
    assert lance_module._relation_dispatch_available(relation, "write_file")
