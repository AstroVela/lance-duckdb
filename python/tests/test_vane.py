# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import gc
import hashlib
import json
import os
import pickle
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import lance_duckdb as lance_module
from lance_duckdb import (
    LANCE_EXTENSION_PATH_ENV,
    LanceCommitOutcomeUnknownError,
    LanceDataset,
    LanceMutationOutcomeUnknownError,
    LanceNamespace,
    _option_sql,
    load_lance_extension,
    normalize_dataset_uri,
)

vane = pytest.importorskip("vane")
from vane import runners
from vane.runners.copy_outcome import CopyOutcomeUnknownError
from vane.runners.ray import set_runner_ray

STATIC_WHEEL_TEST_ENV = "LANCE_DUCKDB_STATIC_WHEEL_TEST"


def _connect():
    artifact = os.environ.get(LANCE_EXTENSION_PATH_ENV)
    static_wheel_test = os.environ.get(STATIC_WHEEL_TEST_ENV) == "1"
    if artifact and static_wheel_test:
        pytest.fail(
            f"{STATIC_WHEEL_TEST_ENV}=1 must not be combined with {LANCE_EXTENSION_PATH_ENV}"
        )
    if not artifact and not static_wheel_test:
        pytest.skip(f"set {LANCE_EXTENSION_PATH_ENV} to run Vane integration tests")
    config = {
        "autoinstall_known_extensions": "false",
        "autoload_known_extensions": "false",
    }
    if os.environ.get("LANCE_DUCKDB_TEST_ALLOW_UNSIGNED") == "1":
        config["allow_unsigned_extensions"] = True
    return load_lance_extension(vane.connect(config=config), artifact)


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_dataset(connection, path: Path, *, empty: bool = False) -> None:
    predicate = " WHERE false" if empty else ""
    connection.execute(
        "COPY (SELECT i::BIGINT AS id, ('value-' || i::VARCHAR) AS value "
        f"FROM range(12) AS source(i){predicate}) TO {_sql_literal(path)} "
        "(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 3)"
    )


def _scan(connection, path: str | Path):
    """Use the supported LanceDataset/table-function read surface."""
    return LanceDataset(path, connection).scan()


def _write_partitioned_parquet(
    connection,
    path: Path,
    *,
    start: int,
    count: int,
    file_count: int,
    value_expression: str = "('value-' || i::VARCHAR)::VARCHAR",
) -> None:
    path.mkdir()
    end = start + count
    for file_id in range(file_count):
        connection.execute(
            "COPY (SELECT i::BIGINT AS id, "
            f"{value_expression} AS value FROM range({start}, {end}) AS source(i) "
            f"WHERE i % {file_count} = {file_id}) TO "
            f"{_sql_literal(path / f'part-{file_id}.parquet')} (FORMAT PARQUET)"
        )


def _lance_transaction_count(path: Path) -> int:
    return sum(entry.is_file() for entry in (path / "_transactions").iterdir())


def _lance_data_files(path: Path) -> set[Path]:
    data = path / "data"
    if not data.exists():
        return set()
    return {entry.relative_to(path) for entry in data.rglob("*") if entry.is_file()}


def _distributed_task_prefixes(files: set[Path]) -> set[str]:
    prefixes: set[str] = set()
    for path in files:
        parts = path.name.split("_", 3)
        if len(parts) == 4 and parts[0] == "vane":
            prefixes.add("_".join(parts[:3]))
    return prefixes


def _assert_staging_empty(path: Path) -> None:
    staging = path / "_vane_staging"
    assert not staging.exists() or not any(staging.rglob("*"))


def _assert_no_coordination_artifacts(path: Path) -> None:
    assert not (path / "_vane_leases").exists()


def _run_distributed(runner, relation) -> list[dict[str, object]]:
    return [
        row for table in runner.run_iter_tables(relation) for row in table.to_pylist()
    ]


@pytest.fixture
def ray_runner(request: pytest.FixtureRequest):
    if os.environ.get("LANCE_DUCKDB_TEST_ALLOW_UNSIGNED") == "1":
        pytest.skip("Ray workers intentionally reject unsigned extension artifacts")
    request.getfixturevalue("ray_local")
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    try:
        yield runners.get_or_create_runner()
    finally:
        vane.teardown_runner()


@pytest.fixture
def ray_write_runner(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    if os.environ.get("LANCE_DUCKDB_TEST_ALLOW_UNSIGNED") == "1":
        pytest.skip("Ray workers intentionally reject unsigned extension artifacts")
    request.getfixturevalue("ray_local")
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "2")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "4")
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    try:
        yield runners.get_or_create_runner()
    finally:
        vane.teardown_runner()


def test_lance_python_api_writes_and_reads_local_dataset(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "python-api.lance"

    try:
        relation = connection.sql(
            "SELECT i::BIGINT AS id, i * 10 AS value FROM range(5) AS source(i)"
        )
        LanceDataset(path, connection).write(relation)

        assert _scan(connection, path).order("id").fetchall() == [
            (0, 0),
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
        ]
    finally:
        connection.close()


def test_vane_static_wheel_loads_lance_without_an_artifact_path() -> None:
    if os.environ.get(STATIC_WHEEL_TEST_ENV) != "1":
        pytest.skip(f"set {STATIC_WHEEL_TEST_ENV}=1 for static-wheel validation")
    assert LANCE_EXTENSION_PATH_ENV not in os.environ
    assert "LANCE_DUCKDB_TEST_ALLOW_UNSIGNED" not in os.environ
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


def test_vane_snapshot_records_exact_loadable_lance_artifact() -> None:
    if not os.environ.get(LANCE_EXTENSION_PATH_ENV):
        pytest.skip("loadable-artifact identity is separate from the static-wheel lane")
    connection = _connect()
    artifact = Path(os.environ[LANCE_EXTENSION_PATH_ENV]).resolve(strict=True)

    try:
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql("SELECT 1"),
            "lance-loadable-extension-snapshot",
        )
        snapshot = logical_plan.__getstate__()[3]
        lance_entries = [
            entry
            for entry in snapshot["extensions"]
            if entry["name"].lower() == "lance"
        ]

        assert len(lance_entries) == 1
        lance_entry = lance_entries[0]
        assert lance_entry["version"]
        assert lance_entry["mode"] == "LOADABLE"
        assert lance_entry["path"] == str(artifact)
        assert lance_entry["sha256"] == _sha256_file(artifact)
        assert any(
            contract.startswith("lance{")
            for contract in snapshot["distributed_extension_contracts"]
        )

        # Replaying against the source DatabaseInstance verifies the exact
        # loadable identity without weakening worker unsigned-code policy.
        physical_plan = logical_plan.to_physical_plan(connection)
        del physical_plan, logical_plan
        gc.collect()
    finally:
        connection.close()


def test_vane_snapshot_records_directory_lance_attachment(tmp_path: Path) -> None:
    connection = _connect()
    root = tmp_path / "snapshot-namespace"
    namespace = LanceNamespace(root, "lance_replay", connection=connection)

    try:
        connection.execute(
            "CREATE TABLE lance_replay.main.items AS SELECT 42::BIGINT AS value"
        )
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            namespace.table("items").scan(),
            "lance-attached-catalog-snapshot",
        )
        attached_databases = logical_plan.__getstate__()[3]["attached_databases"]

        assert len(attached_databases) == 1
        attach_sql = attached_databases[0]
        assert (
            f"ATTACH DATABASE {_sql_literal(namespace.namespace_id)} AS lance_replay"
            in attach_sql
        )
        assert "(type 'lance')" in attach_sql.lower()

        # This validates capture and same-DatabaseInstance planning only. It
        # does not claim detached source-connection replay.
        physical_plan = logical_plan.to_physical_plan(connection)
        assert physical_plan.idx() == "lance-attached-catalog-snapshot"
    finally:
        namespace.detach()
        connection.close()


def test_lance_dataset_write_rejects_unknown_options(tmp_path: Path) -> None:
    connection = _connect()

    try:
        dataset = LanceDataset(tmp_path / "unknown-option.lance", connection)
        with pytest.raises(ValueError, match="unsupported_option"):
            dataset.write(
                connection.sql("SELECT 1::BIGINT AS id"), unsupported_option=True
            )
        assert not Path(dataset.uri).exists()
    finally:
        connection.close()


def test_lance_dataset_write_uses_standard_copy_sql(tmp_path: Path) -> None:
    class FakeConnection:
        def execute(self, sql: str):
            assert "FORMAT lance" in sql
            assert "mode 'create'" in sql
            assert "max_rows_per_file 1" in sql
            assert "max_rows_per_file =" not in sql
            return self

    connection = FakeConnection()

    class FakeRelation:
        def sql_query(self) -> str:
            return "SELECT 1::BIGINT AS id"

    LanceDataset(tmp_path / "standard-copy.lance", connection).write(
        FakeRelation(), mode="create", max_rows_per_file=1
    )


def test_lance_dataset_write_uses_generic_vane_write_file_relation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lance_module, "_configured_runner_type", lambda: "ray")
    target = str((tmp_path / "distributed-create.lance").resolve())
    calls: list[tuple[str, str]] = []

    class FakeConnection:
        def execute(self, sql: str) -> None:
            raise AssertionError(f"unexpected driver-local SQL: {sql}")

    class FakeRelation:
        def sql_query(self) -> str:
            return "SELECT 1::BIGINT AS id"

        def write_file(self, path: str, *, format: str) -> None:
            calls.append((path, format))

    LanceDataset(target, FakeConnection()).write(FakeRelation())

    assert calls == [(target, "lance")]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_lance_runtime_fails_closed_in_a_forked_child(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "fork-runtime.lance"

    try:
        _write_dataset(connection, path)
    finally:
        connection.close()

    script = textwrap.dedent(
        r"""
        import os
        import signal
        import sys
        import time

        import vane
        from lance_duckdb import LANCE_EXTENSION_PATH_ENV, LanceDataset, load_lance_extension

        def _connect():
            config = {
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            }
            if os.environ.get("LANCE_DUCKDB_TEST_ALLOW_UNSIGNED") == "1":
                config["allow_unsigned_extensions"] = True
            return load_lance_extension(
                vane.connect(config=config),
                os.environ.get(LANCE_EXTENSION_PATH_ENV),
            )

        path = sys.argv[1]
        connection = _connect()
        assert LanceDataset(path, connection).scan().aggregate("count(*)").fetchone() == (12,)

        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                child_connection = _connect()
                LanceDataset(path, child_connection).scan().fetchall()
            except BaseException as exc:
                message = f"{type(exc).__name__}: {exc}".encode("utf-8", errors="replace")
                os.write(write_fd, message)
                os.close(write_fd)
                os._exit(0)
            os.write(write_fd, b"child unexpectedly used the inherited Lance runtime")
            os.close(write_fd)
            os._exit(1)

        os.close(write_fd)
        deadline = time.monotonic() + 5
        while True:
            waited_pid, status = os.waitpid(pid, os.WNOHANG)
            if waited_pid == pid:
                break
            if time.monotonic() >= deadline:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
                raise RuntimeError("forked child hung while entering the inherited Lance runtime")
            time.sleep(0.01)

        message = os.read(read_fd, 65_536).decode("utf-8", errors="replace")
        os.close(read_fd)
        connection.close()
        print(message)
        if os.waitstatus_to_exitcode(status) != 0:
            raise RuntimeError(message)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "initialized in process" in result.stdout
    assert "before fork" in result.stdout
    assert "spawn/exec" in result.stdout


def test_lance_writes_accept_self_contained_relations_from_another_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(lance_module, "_configured_runner_type", lambda: "local")
    target = _connect()
    source = _connect()
    namespace = LanceNamespace(
        tmp_path / "cross-connection", "target_ns", connection=target
    )

    try:
        relation = source.sql("SELECT 7::BIGINT AS id")

        dataset = LanceDataset(tmp_path / "direct.lance", target)
        dataset.write(relation)
        assert dataset.scan().fetchall() == [(7,)]

        table = namespace.create_table("items", relation)
        assert table.scan().fetchall() == [(7,)]
    finally:
        namespace.detach()
        source.close()
        target.close()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_lance_options_reject_non_finite_floats(value: float) -> None:
    with pytest.raises(ValueError, match="must be finite"):
        _option_sql({"threshold": value})


@pytest.mark.parametrize("value", [None, [1], object()])
def test_lance_options_reject_unsupported_value_types(value: object) -> None:
    with pytest.raises(
        TypeError, match="must be a string, boolean, integer, or finite float"
    ):
        _option_sql({"option": value})


def test_lance_options_reject_non_string_and_case_insensitive_duplicate_names() -> None:
    with pytest.raises(TypeError, match="option names must be strings"):
        _option_sql({1: "value"})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="duplicate case-insensitive"):
        _option_sql({"threshold": 1, "THRESHOLD": 2})


def test_lance_dataset_resolves_relative_path_once(tmp_path: Path, monkeypatch) -> None:
    original_directory = tmp_path / "original"
    other_directory = tmp_path / "other"
    original_directory.mkdir()
    other_directory.mkdir()
    seen: list[str] = []

    class FakeConnection:
        def table_function(self, name: str, parameters: list[str]) -> object:
            assert name == "__lance_scan"
            seen.append(parameters[0])
            return object()

    monkeypatch.chdir(original_directory)
    dataset = LanceDataset("relative.lance", FakeConnection())
    monkeypatch.chdir(other_directory)

    dataset.scan()

    expected = str((original_directory / "relative.lance").resolve())
    assert dataset.uri == expected
    assert seen == [expected]


def test_lance_relative_paths_ignore_duckdb_file_search_path(
    tmp_path: Path, monkeypatch
) -> None:
    working_directory = tmp_path / "working"
    search_directory = tmp_path / "search"
    working_directory.mkdir()
    search_directory.mkdir()
    monkeypatch.chdir(working_directory)
    connection = _connect()

    try:
        connection.execute(f"SET file_search_path = {_sql_literal(search_directory)}")
        dataset = LanceDataset("relative.lance", connection)
        connection.execute(
            "COPY (SELECT 7::BIGINT AS id) TO 'relative.lance' (FORMAT LANCE, MODE 'create')"
        )

        assert dataset.uri == str((working_directory / "relative.lance").resolve())
        assert dataset.scan().fetchall() == [(7,)]
        assert (working_directory / "relative.lance").is_dir()
        assert not (search_directory / "relative.lance").exists()
    finally:
        connection.close()


def test_lance_dataset_write_resolves_relative_uri(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    connection = _connect()
    try:
        dataset = LanceDataset("relative-worker.lance", connection)
        dataset.write(connection.sql("SELECT 7::BIGINT AS id"))
        assert (tmp_path / "relative-worker.lance").is_dir()
    finally:
        connection.close()


def test_lance_snapshot_context_is_side_effect_free(tmp_path: Path) -> None:
    class FakeConnection:
        def table_function(self, name: str, parameters: list[str]):
            assert name == "__lance_scan"
            assert parameters == [str(tmp_path / "snapshot.lance")]
            return object()

    dataset = LanceDataset(tmp_path / "snapshot.lance", FakeConnection())
    with dataset.snapshot() as relation:
        assert relation is not None
    _assert_no_coordination_artifacts(tmp_path / "snapshot.lance")


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="POSIX permission checks require a non-root process",
)
def test_lance_read_requires_no_dataset_write_permission(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "read-only-scan.lance"

    try:
        _write_dataset(connection, path)
        entries = [path, *path.rglob("*")]
        for entry in entries:
            entry.chmod(0o555 if entry.is_dir() else 0o444)

        assert _scan(connection, path).aggregate("count(*), sum(id)").fetchone() == (
            12,
            66,
        )
        _assert_no_coordination_artifacts(path)
    finally:
        if path.exists():
            for entry in [path, *path.rglob("*")]:
                entry.chmod(0o755 if entry.is_dir() else 0o644)
        connection.close()


def test_lance_physical_plan_capture_is_side_effect_free(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "coordinator-snapshot.lance"

    try:
        _write_dataset(connection, path)
        relation = _scan(connection, path)
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            "lance-side-effect-free-snapshot",
        )
        del relation
        gc.collect()
        _assert_no_coordination_artifacts(path)

        # Pickle round-trip models the gap between source-plan serialization
        # and Ray driver planning. It must preserve only plan data.
        logical_plan = pickle.loads(pickle.dumps(logical_plan))
        _assert_no_coordination_artifacts(path)
        physical_plan = logical_plan.to_physical_plan(connection)
        del logical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)

        # A normal MVCC commit may advance the latest manifest while the
        # coordinator still owns this older snapshot. Worker deserialization
        # must validate the checked-out fixed version, not compare the fixed
        # snapshot identity with the new latest manifest.
        connection.execute(
            "COPY (SELECT 99::BIGINT AS id, 'new'::VARCHAR AS value) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'append')"
        )

        # Vane serializes the physical root to validate submission and may
        # pickle it again while registering the query. This remains data-only.
        serialized_physical_plan = pickle.dumps(physical_plan)
        assert serialized_physical_plan
        _assert_no_coordination_artifacts(path)

        # A same-process physical-plan clone is a local serialization round
        # trip. It must reopen the serialized fixed snapshot without publishing
        # coordination state.
        cloned_physical_plan = physical_plan.clone(connection)
        _assert_no_coordination_artifacts(path)
        del cloned_physical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)

        del physical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_bound_plan_does_not_block_vacuum_or_drop(tmp_path: Path) -> None:
    connection = _connect()
    root = tmp_path / "plan-maintenance-boundary"
    path = root / "items.lance"
    attached = False

    try:
        root.mkdir()
        _write_dataset(connection, path)
        connection.execute(f"ATTACH {_sql_literal(root)} AS plan_boundary (TYPE LANCE)")
        attached = True
        relation = connection.table("plan_boundary.main.items")
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            "lance-plan-maintenance-boundary",
        )
        physical_plan = logical_plan.to_physical_plan(connection)

        vacuum_result = connection.execute(
            "VACUUM LANCE plan_boundary.main.items WITH ("
            "older_than_seconds = 0, delete_unverified = false, "
            "error_if_tagged_old_versions = true)"
        ).fetchone()
        assert vacuum_result is not None
        assert vacuum_result[0] == "cleanup"
        _assert_no_coordination_artifacts(path)

        connection.execute("DROP TABLE plan_boundary.main.items")
        assert not path.exists()

        del physical_plan, logical_plan, relation
        gc.collect()
    finally:
        if attached:
            connection.execute("DETACH plan_boundary")
        connection.close()


def test_lance_two_leaf_plan_capture_is_side_effect_free(tmp_path: Path) -> None:
    connection = _connect()
    left_path = tmp_path / "plan-transport-left.lance"
    right_path = tmp_path / "plan-transport-right.lance"

    try:
        _write_dataset(connection, left_path)
        _write_dataset(connection, right_path)
        left = _scan(connection, left_path).set_alias("capture_left")
        right = _scan(connection, right_path).set_alias("capture_right")
        relation = left.join(right, "capture_left.id = capture_right.id")
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            "lance-data-only-plan-transport",
        )
        logical_plan = pickle.loads(pickle.dumps(logical_plan))
        physical_plan = logical_plan.to_physical_plan(connection)
        physical_clone = physical_plan.clone(connection)

        for path in (left_path, right_path):
            _assert_no_coordination_artifacts(path)

        del physical_clone, physical_plan, logical_plan, relation, left, right
        gc.collect()
        for path in (left_path, right_path):
            _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_local_physical_plan_clone_can_be_reused(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "repeat-local-plan-clone.lance"

    try:
        _write_dataset(connection, path)
        relation = _scan(connection, path)
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, "lance-repeat-local-plan-clone"
        )
        physical_plan = logical_plan.to_physical_plan(connection)
        del relation, logical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)

        local_source = physical_plan.clone(connection)
        _assert_no_coordination_artifacts(path)

        first_clone = local_source.clone(connection)
        second_clone = local_source.clone(connection)
        _assert_no_coordination_artifacts(path)

        del first_clone, second_clone
        gc.collect()
        _assert_no_coordination_artifacts(path)
        del local_source
        gc.collect()
        _assert_no_coordination_artifacts(path)
        del physical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_relation_can_create_multiple_data_only_plans(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "repeat-logical-plan-transport.lance"

    try:
        _write_dataset(connection, path)
        relation = _scan(connection, path)
        first_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, "lance-first-logical-plan"
        )
        second_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, "lance-second-logical-plan"
        )
        del relation
        gc.collect()

        _assert_no_coordination_artifacts(path)

        first_physical = first_logical.to_physical_plan(connection)
        second_physical = second_logical.to_physical_plan(connection)
        del first_logical, second_logical
        gc.collect()
        _assert_no_coordination_artifacts(path)

        del first_physical
        gc.collect()
        _assert_no_coordination_artifacts(path)
        del second_physical
        gc.collect()
        _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_search_plan_capture_is_side_effect_free(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "coordinator-search-snapshot.lance"

    try:
        connection.execute(
            "COPY (SELECT * FROM (VALUES "
            "(1::BIGINT, [0.0, 0.0]::FLOAT[2]), "
            "(2::BIGINT, [1.0, 0.0]::FLOAT[2])) AS source(id, vec)) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'create')"
        )
        relation = LanceDataset(path, connection).vector_search("vec", [0.0, 0.0], k=1)
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            "lance-data-only-search-snapshot",
        )
        del relation
        gc.collect()
        _assert_no_coordination_artifacts(path)
        physical_plan = logical_plan.to_physical_plan(connection)
        del logical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)

        del physical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_worker_plan_rejects_recreated_same_version_dataset(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "recreated-generation.lance"

    try:
        _write_dataset(connection, path)
        relation = _scan(connection, path)
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            "lance-recreated-generation",
        )
        physical_plan = logical_plan.to_physical_plan(connection)
        del relation, logical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)

        # Model an external DROP/recreate. Both datasets have numeric version 1
        # and the same schema, so only the fixed generation identity
        # distinguishes the replacement.
        shutil.rmtree(path)
        connection.execute(
            "COPY (SELECT 777::BIGINT AS id, 'replacement'::VARCHAR AS value) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'create')"
        )
        with pytest.raises(Exception, match=r"(?i)(generation|serialized snapshot)"):
            physical_plan.clone(connection)

        del physical_plan
        gc.collect()
        assert _scan(connection, path).fetchall() == [(777, "replacement")]
        _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_exec_pushdown_plan_capture_is_side_effect_free(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "coordinator-exec-snapshot.lance"

    try:
        _write_dataset(connection, path)
        # AVG is covered by the Exec IR aggregate contract. The optimizer
        # replaces the original scan bind data, so the rewritten bind must retain
        # the exact version and generation as data.
        relation = _scan(connection, path).aggregate("avg(id) AS mean_id")
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            "lance-data-only-exec-snapshot",
        )
        del relation
        gc.collect()
        _assert_no_coordination_artifacts(path)
        physical_plan = logical_plan.to_physical_plan(connection)
        del logical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)

        del physical_plan
        gc.collect()
        _assert_no_coordination_artifacts(path)
    finally:
        connection.close()


def test_lance_mutation_and_maintenance_surfaces_create_no_coordination_state(
    tmp_path: Path,
) -> None:
    connection = _connect()
    root = tmp_path / "maintenance-namespace"
    path = root / "maintenance_operations.lance"

    def assert_side_effect_boundary() -> None:
        gc.collect()
        _assert_no_coordination_artifacts(path)

    try:
        root.mkdir()
        _write_dataset(connection, path)
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS maintenance_ns (TYPE LANCE)"
        )

        connection.execute(
            "ALTER TABLE maintenance_ns.main.maintenance_operations "
            "ADD COLUMN extra BIGINT DEFAULT 7"
        )
        assert_side_effect_boundary()

        connection.execute(
            "COMMENT ON TABLE maintenance_ns.main.maintenance_operations "
            "IS 'documented'"
        )
        assert_side_effect_boundary()

        connection.execute(
            "CREATE INDEX maintenance_id_idx ON "
            "maintenance_ns.main.maintenance_operations (id) USING BTREE"
        )
        assert_side_effect_boundary()

        connection.execute(
            "INSERT INTO maintenance_ns.main.maintenance_operations "
            "VALUES (20, 'inserted', 8)"
        )
        connection.execute(
            "UPDATE maintenance_ns.main.maintenance_operations "
            "SET extra = 9 WHERE id = 20"
        )
        connection.execute(
            "DELETE FROM maintenance_ns.main.maintenance_operations WHERE id = 20"
        )
        assert_side_effect_boundary()

        connection.execute(
            "SELECT * FROM lance_set_table_metadata("
            f"{_sql_literal(path)}, 'coordination-test', 'absent')"
        ).fetchall()
        assert_side_effect_boundary()

        connection.execute(
            "SELECT * FROM __lance_compact_files("
            f"{_sql_literal(path)}, "
            "'{\"target_rows_per_fragment\":1024}')"
        ).fetchall()
        assert_side_effect_boundary()

        connection.execute(
            f"SELECT * FROM lance_compact_files({_sql_literal(path)})"
        ).fetchall()
        assert_side_effect_boundary()

        connection.execute("TRUNCATE TABLE maintenance_ns.main.maintenance_operations")
        assert_side_effect_boundary()

        connection.execute("DROP TABLE maintenance_ns.main.maintenance_operations")
        assert not path.exists()
    finally:
        try:
            connection.execute("DETACH maintenance_ns")
        except Exception:
            pass
        connection.close()


def test_lance_local_query_relation_creates_no_coordination_artifacts(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(lance_module, "_configured_runner_type", lambda: "local-fast")
    connection = _connect()
    left_path = tmp_path / "left.lance"
    right_path = tmp_path / "right.lance"

    try:
        _write_dataset(connection, left_path)
        _write_dataset(connection, right_path)
        left = _scan(connection, left_path).set_alias("left_lance")
        right = _scan(connection, right_path).set_alias("right_lance")
        joined = left.join(right, "left_lance.id = right_lance.id")
        query = joined.query("joined_lance", "SELECT id FROM joined_lance ORDER BY id")
        del left, right, joined
        gc.collect()

        assert query.fetchall() == [(value,) for value in range(12)]
        for path in (left_path, right_path):
            _assert_no_coordination_artifacts(path)

        del query
        gc.collect()
        _assert_no_coordination_artifacts(left_path)
        _assert_no_coordination_artifacts(right_path)
    finally:
        connection.close()


def test_lance_directory_namespace_table_api(tmp_path: Path) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "namespace", "lance_ns", connection=connection
    )

    try:
        assert connection.execute("SHOW TABLES FROM lance_ns.main").fetchall() == []
        table = namespace.create_table(
            "items", "SELECT 1::BIGINT AS id, 'one'::VARCHAR AS label"
        )
        table.insert("SELECT 2::BIGINT AS id, 'two'::VARCHAR AS label")
        (
            table.merge(
                "SELECT * FROM (VALUES (2::BIGINT, 'TWO'::VARCHAR), (3::BIGINT, 'three'::VARCHAR)) source(id, label)",
                "target.id = source.id",
            )
            .when_matched_update({"label": "source.label"})
            .when_not_matched_insert({"id": "source.id", "label": "source.label"})
            .execute()
        )
        assert table.scan().order("id").fetchall() == [
            (1, "one"),
            (2, "TWO"),
            (3, "three"),
        ]
        assert connection.execute("SHOW TABLES FROM lance_ns.main").fetchall() == [
            ("items",)
        ]
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_rejects_empty_rest_endpoint() -> None:
    with pytest.raises(ValueError, match="endpoint cannot be empty"):
        LanceNamespace("catalog", "rest_ns", endpoint="  ")


def test_lance_namespace_rejects_non_text_namespace_and_endpoint() -> None:
    with pytest.raises(TypeError, match="namespace ID must be a string or text path"):
        LanceNamespace(b"catalog", "rest_ns")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="endpoint must be a string or None"):
        LanceNamespace("catalog", "rest_ns", endpoint=b"https://example.test")  # type: ignore[arg-type]


def test_lance_rest_table_identity_removes_endpoint_credentials_and_query() -> None:
    namespace = LanceNamespace(
        "catalog",
        "rest_ns",
        endpoint="HTTPS://user:secret@[2001:DB8::1]:8443/base/?token=hidden#fragment",
        connection=object(),
        attach=False,
    )

    table = namespace.table("ITEMS")

    assert table.uri == (
        "lance-rest://namespace/https%3A%2F%2F%5B2001%3Adb8%3A%3A1%5D%3A8443%2Fbase/catalog/main/items"
    )
    assert normalize_dataset_uri(table.uri) == table.uri

    physical_uri = normalize_dataset_uri(
        "https://[2001:db8::1]:8443/base/.vane-lance-rest/catalog/main/items"
    )
    assert table.uri != physical_uri


def test_lance_rest_table_identity_has_unambiguous_endpoint_and_namespace_boundaries() -> (
    None
):
    left = LanceNamespace(
        "catalog",
        "left",
        endpoint="https://namespace.example.test/base/tenant",
        connection=object(),
        attach=False,
    ).table("items")
    right = LanceNamespace(
        "tenant/catalog",
        "right",
        endpoint="https://namespace.example.test/base",
        connection=object(),
        attach=False,
    ).table("items")

    assert left.uri != right.uri
    assert "%2F" in right.uri


def test_lance_directory_table_uses_the_direct_dataset_identity(tmp_path: Path) -> None:
    namespace = LanceNamespace(
        tmp_path / "namespace", "lance_ns", connection=object(), attach=False
    )
    table = namespace.table("items")

    assert table.uri == normalize_dataset_uri(tmp_path / "namespace" / "items.lance")


def test_lance_existing_directory_table_identity_uses_physical_name_casing(
    tmp_path: Path,
) -> None:
    connection = _connect()
    root = tmp_path / "namespace"
    namespace = LanceNamespace(root, "lance_ns", connection=connection)

    try:
        namespace.create_table("Items", "SELECT 1::BIGINT AS id")
        alternate_case = namespace.table("items")
        assert alternate_case.uri == normalize_dataset_uri(root / "Items.lance")
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_rejects_schemas_the_extension_does_not_expose(
    tmp_path: Path,
) -> None:
    namespace = LanceNamespace(
        tmp_path / "namespace", "lance_ns", connection=object(), attach=False
    )

    with pytest.raises(ValueError, match="only the 'main' schema"):
        namespace.table("items", schema="analytics")


def test_lance_namespace_api_quotes_catalog_and_table_names(tmp_path: Path) -> None:
    # A preceding Ray fixture may have initialized the process-global native
    # runner.  This catalog/API test exercises the local coordinator directly,
    # so reset that runner state explicitly instead of mixing Ray actors with
    # the local state-machine assertions below.
    vane.teardown_runner()
    vane.set_runner_local()
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "quoted-namespace", "team.data-x", connection=connection
    )

    try:
        table = namespace.create_table(
            "items.v1-x",
            "SELECT 1::BIGINT AS id, 'old'::VARCHAR AS \"label-v1\"",
        )
        (
            table.merge(
                "SELECT * FROM (VALUES (1::BIGINT, 'merged'::VARCHAR), "
                "(2::BIGINT, 'inserted'::VARCHAR)) AS source(id, label)",
                "target.id = source.id",
            )
            .when_matched_update({"label-v1": "source.label"})
            .when_not_matched_insert({"id": "source.id", "label-v1": "source.label"})
            .execute()
        )
        table.update({"label-v1": "'updated'"}, where="id = 2")
        table.insert(
            "SELECT 3::BIGINT AS id, 'insert-helper'::VARCHAR AS label",
            columns=["id", "label-v1"],
        )
        table.add_column("extra-value", "INTEGER", default="7")
        table.drop_column("extra-value")
        table.rename_column("label-v1", "label-v2")
        assert table.scan().order("id").fetchall() == [
            (1, "merged"),
            (2, "updated"),
            (3, "insert-helper"),
        ]

        with pytest.raises(ValueError, match="UPDATE requires at least one assignment"):
            table.merge(
                "SELECT 1::BIGINT AS id", "target.id = source.id"
            ).when_matched_update({})
        with pytest.raises(ValueError, match="INSERT requires at least one value"):
            table.merge(
                "SELECT 1::BIGINT AS id", "target.id = source.id"
            ).when_not_matched_insert({})
        with pytest.raises(ValueError, match="top-level column names cannot contain"):
            table.add_column("unsupported.dot", "INTEGER")
        with pytest.raises(ValueError, match="join condition cannot be empty"):
            table.merge("SELECT 1::BIGINT AS id", "  ")

        table.create_index("items_idx", "label-v2", index_type="BTREE")
        index_relation = table.show_indexes()
        assert index_relation.filter("index_name = 'items_idx'").count(
            "*"
        ).fetchone() == (1,)

        del index_relation
        gc.collect()

        assert table.optimize()[0][0] == "compact"
        assert (
            table.vacuum(older_than_seconds=0, delete_unverified=False)[0][0]
            == "cleanup"
        )
        table.drop_index("items_idx")
    finally:
        gc.collect()
        namespace.detach()
        connection.close()


def test_lance_attached_table_overwrite_rejects_schema_change_without_catalog_damage(
    tmp_path: Path,
) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "overwrite-namespace", "overwrite_ns", connection=connection
    )

    try:
        table = namespace.create_table("items", "SELECT 1::BIGINT AS old_id")
        with pytest.raises(
            NotImplementedError, match="does not support schema changes"
        ):
            table.write(
                connection.sql("SELECT 'new'::VARCHAR AS new_label"), mode="overwrite"
            )

        relation = table.scan()
        assert relation.columns == ["old_id"]
        assert relation.fetchall() == [(1,)]
        del relation
        gc.collect()

        table.write(connection.sql("SELECT 2::BIGINT AS old_id"), mode="overwrite")
        assert table.scan().fetchall() == [(2,)]
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_detach_does_not_use_python_snapshot_timeout(
    tmp_path: Path,
) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "detach-namespace", "detach_ns", connection=connection
    )
    relation = None
    detached = False

    try:
        table = namespace.create_table("items", "SELECT 1::BIGINT AS id")
        relation = table.scan()
        # DETACH is a DuckDB catalog operation. The Python helper does not wait
        # on a process-local coordinator or raise a Python timeout here.
        namespace.detach(timeout=0.01)
        detached = True
        assert connection.execute(
            "SELECT count(*) FROM duckdb_databases() WHERE lower(database_name) = 'detach_ns'"
        ).fetchone() == (0,)
    finally:
        if relation is not None:
            del relation
            gc.collect()
        if not detached:
            namespace.detach(timeout=0)
        connection.close()


def test_lance_namespace_detach_validates_timeout_even_when_empty(
    tmp_path: Path,
) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "empty-detach-namespace", "empty_detach_ns", connection=connection
    )

    try:
        with pytest.raises(ValueError, match="finite non-negative"):
            namespace.detach(timeout=-1)
        assert connection.execute(
            "SELECT count(*) FROM duckdb_databases() WHERE lower(database_name) = 'empty_detach_ns'"
        ).fetchone() == (1,)
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_helpers_preserve_explicit_transaction_boundaries(
    tmp_path: Path,
) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "transaction-namespace", "transaction_ns", connection=connection
    )

    try:
        connection.begin()
        with pytest.raises(
            vane.NotImplementedException,
            match="does not support explicit transactions",
        ):
            namespace.create_table("items", "SELECT 1::BIGINT AS id")
        connection.rollback()
        assert (
            connection.execute("SHOW TABLES FROM transaction_ns.main").fetchall() == []
        )

        table = namespace.create_table("items", "SELECT 1::BIGINT AS id")
        connection.begin()
        with pytest.raises(
            vane.NotImplementedException,
            match="does not support explicit transactions",
        ):
            table.write(connection.sql("SELECT 2::BIGINT AS id"), mode="overwrite")
        connection.rollback()

        transactions_before_insert = _lance_transaction_count(Path(table.uri))
        connection.begin()
        table.insert("SELECT 2::BIGINT AS id")
        connection.rollback()
        assert _lance_transaction_count(Path(table.uri)) == transactions_before_insert

        connection.begin()
        with pytest.raises(
            vane.NotImplementedException,
            match="does not support explicit transactions",
        ):
            namespace.drop_table("items")
        connection.rollback()

        assert table.scan().fetchall() == [(1,)]
    finally:
        namespace.detach()
        connection.close()


def test_lance_namespace_unknown_outcome_has_no_coordination_fields(
    tmp_path: Path,
) -> None:
    class FakeConnection:
        pass

    dataset = LanceDataset(
        tmp_path / "unknown-namespace-mutation.lance", FakeConnection()
    )
    for code in (55, 56):
        native_error = RuntimeError(
            f"request timed out; outcome is unknown (code={code})"
        )

        with pytest.raises(LanceMutationOutcomeUnknownError) as mutation_result:
            with dataset._mutation("Lance table creation"):
                raise native_error
        mutation_error = mutation_result.value
        assert mutation_error.identity == normalize_dataset_uri(dataset.uri)
        assert mutation_error.operation == "Lance table creation"
        assert mutation_error.detail == str(native_error)
        assert mutation_error.safe_to_retry is False
        assert not hasattr(mutation_error, "lease_kind")
        assert not hasattr(mutation_error, "lease_token")
        restored = pickle.loads(pickle.dumps(mutation_error))
        assert restored.args == mutation_error.args
        assert restored.detail == mutation_error.detail

    native_error = RuntimeError("request timed out; outcome is unknown (code=56)")
    with pytest.raises(LanceMutationOutcomeUnknownError) as vacuum_result:
        with dataset._vacuum("Lance REST table removal"):
            raise native_error
    vacuum_error = vacuum_result.value
    assert vacuum_error.operation == "Lance REST table removal"
    assert vacuum_error.safe_to_retry is False
    assert not hasattr(vacuum_error, "lease_kind")
    assert not hasattr(vacuum_error, "lease_token")


def test_lance_namespace_unknown_outcome_uses_only_the_final_native_error_code(
    tmp_path: Path,
) -> None:
    class FakeConnection:
        pass

    dataset = LanceDataset(tmp_path / "known-namespace-failure.lance", FakeConnection())
    outer_known_error = RuntimeError(
        "untrusted service text (code=56); outer FFI failure (code=50)"
    )

    with pytest.raises(RuntimeError) as result:
        with dataset._mutation("Lance REST table creation"):
            raise outer_known_error

    assert result.value is outer_known_error


def test_lance_unknown_outcome_diagnostics_redact_uri_credentials() -> None:
    class FakeConnection:
        pass

    raw_uri = (
        "s3://access-user:access-secret@bucket/table.lance"
        "?session_token=hidden#credential-fragment"
    )
    dataset = LanceDataset(raw_uri, FakeConnection())
    native_error = RuntimeError(
        f"commit acknowledgement failed for {raw_uri} (code=55)"
    )

    with pytest.raises(LanceMutationOutcomeUnknownError) as result:
        with dataset._mutation("Lance table write"):
            raise native_error

    error = result.value
    serialized = pickle.dumps(error)
    assert error.identity == "s3://bucket/table.lance"
    assert "s3://bucket/table.lance" in str(error)
    for secret in (
        "access-user",
        "access-secret",
        "session_token",
        "hidden",
        "credential-fragment",
    ):
        assert secret not in str(error)
        assert secret.encode() not in serialized


def test_lance_native_open_error_redacts_uri_credentials() -> None:
    connection = _connect()
    raw_uri = (
        "unsupported://first-secret:second-secret@remaining-secret@host/table.lance"
        "?session_token=hidden#credential-fragment"
    )

    try:
        with pytest.raises(Exception) as result:
            _scan(connection, raw_uri).fetchall()
        message = str(result.value)
        assert "unsupported://host/table.lance" in message
        for secret in (
            "first-secret",
            "second-secret",
            "remaining-secret",
            "session_token",
            "hidden",
            "credential-fragment",
        ):
            assert secret not in message
    finally:
        connection.close()


def test_lance_copy_outcome_unknown_is_typed_and_pickle_safe(tmp_path: Path) -> None:
    class FakeConnection:
        pass

    dataset = LanceDataset(tmp_path / "unknown-copy.lance", FakeConnection())
    native_error = CopyOutcomeUnknownError("operation-1", detail="driver disconnected")

    with pytest.raises(LanceCommitOutcomeUnknownError) as result:
        with dataset._mutation("Lance table writes"):
            raise native_error

    error = result.value
    restored = pickle.loads(pickle.dumps(error))
    assert isinstance(restored, LanceCommitOutcomeUnknownError)
    assert restored.operation_id == error.operation_id
    assert restored.detail == "driver disconnected"


def test_lance_vacuum_unknown_outcome_is_typed_as_non_retryable(tmp_path: Path) -> None:
    class FakeConnection:
        pass

    dataset = LanceDataset(tmp_path / "vacuum-cleanup.lance", FakeConnection())

    with pytest.raises(LanceMutationOutcomeUnknownError) as result:
        with dataset._vacuum("Lance vacuum"):
            raise RuntimeError("native cleanup acknowledgement is unknown (code=55)")

    error = result.value
    assert error.operation == "Lance vacuum"
    assert error.safe_to_retry is False
    assert not hasattr(error, "lease_kind")
    assert not hasattr(error, "lease_token")


def test_lance_dataset_write_rejects_explicit_transactions_before_creating_dataset(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "transaction-write.lance"

    try:
        connection.begin()
        with pytest.raises(
            vane.NotImplementedException,
            match="does not support explicit transactions",
        ):
            LanceDataset(path, connection).write(
                connection.sql("SELECT 1::BIGINT AS id")
            )
        connection.rollback()

        assert not path.exists()
    finally:
        connection.close()


def test_lance_rest_scan_is_version_pinned_without_blocking_mutations(
    monkeypatch,
) -> None:
    monkeypatch.setattr(lance_module, "_configured_runner_type", lambda: "local-fast")
    executed: list[str] = []

    class FakeRelation:
        columns = ["id"]
        types = ["BIGINT"]

    class FakeResult:
        def fetchone(self) -> tuple[int]:
            return (1,)

    class FakeConnection:
        def table(self, name: str) -> FakeRelation:
            assert name == '"rest_ns"."main"."items"'
            return FakeRelation()

        def sql(self, sql: str) -> FakeRelation:
            assert sql in {"SELECT 2::BIGINT AS id", "SELECT 3::BIGINT AS id"}
            return FakeRelation()

        def execute(self, sql: str, *args: object) -> FakeConnection | FakeResult:
            if "duckdb_tables()" in sql:
                return FakeResult()
            del args
            executed.append(sql)
            return self

    connection = FakeConnection()
    namespace = LanceNamespace(
        "catalog",
        "rest_ns",
        endpoint="https://namespace.example.test",
        connection=connection,
        attach=False,
    )
    monkeypatch.setattr(namespace, "_attachment_is_read_only", lambda: False)
    table = namespace.table("items")
    relation = table.scan()

    # The fake control-plane connection only records SQL. No Python
    # coordination object is attached to this relation.
    table.insert("SELECT 2::BIGINT AS id")

    table.write("SELECT 3::BIGINT AS id", mode="overwrite")
    assert executed == [
        'INSERT INTO "rest_ns"."main"."items" SELECT 2::BIGINT AS id',
        'CREATE OR REPLACE TABLE "rest_ns"."main"."items" AS SELECT 3::BIGINT AS id',
    ]
    assert all("namespace.example.test" not in sql for sql in executed)

    del relation
    gc.collect()


def test_lance_fragment_scan_uses_distributed_scan_contract(
    tmp_path: Path, ray_runner, monkeypatch
) -> None:
    connection = _connect()
    monkeypatch.chdir(tmp_path)
    path = Path("fragmented.lance")

    try:
        _write_dataset(connection, path)
        rows = _run_distributed(
            ray_runner,
            _scan(connection, path).project("id, value"),
        )
        assert sorted(tuple(row.values()) for row in rows) == [
            (index, f"value-{index}") for index in range(12)
        ]
    finally:
        connection.close()


@pytest.mark.skipif(
    not hasattr(os, "geteuid") or os.geteuid() == 0,
    reason="POSIX permission checks require a non-root process",
)
def test_lance_distributed_read_requires_no_dataset_write_permission(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "read-only-distributed-scan.lance"

    try:
        _write_dataset(connection, path)
        entries = [path, *path.rglob("*")]
        for entry in entries:
            entry.chmod(0o555 if entry.is_dir() else 0o444)

        rows = _run_distributed(ray_runner, _scan(connection, path).project("id"))
        assert sorted(int(next(iter(row.values()))) for row in rows) == list(range(12))
        _assert_no_coordination_artifacts(path)
    finally:
        if path.exists():
            for entry in [path, *path.rglob("*")]:
                entry.chmod(0o755 if entry.is_dir() else 0o644)
        connection.close()


def test_lance_sql_write_rejects_zero_limits(tmp_path: Path) -> None:
    connection = _connect()

    try:
        with pytest.raises(Exception, match="limits must be greater than zero"):
            connection.execute(
                "COPY (SELECT 1::BIGINT AS id) TO "
                f"{_sql_literal(tmp_path / 'invalid.lance')} "
                "(FORMAT LANCE, MAX_ROWS_PER_FILE 0)"
            )
    finally:
        connection.close()


def test_lance_sql_write_rejects_empty_target() -> None:
    connection = _connect()

    try:
        with pytest.raises(Exception, match="Lance COPY target cannot be empty"):
            connection.execute("COPY (SELECT 1::BIGINT AS id) TO '' (FORMAT LANCE)")
    finally:
        connection.close()


def test_lance_empty_dataset_is_an_explicit_distributed_empty_scan(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "empty.lance"

    try:
        _write_dataset(connection, path, empty=True)
        assert _run_distributed(ray_runner, _scan(connection, path)) == []
    finally:
        connection.close()


def test_lance_search_python_api_binds_named_parameters_end_to_end(
    tmp_path: Path,
) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "search-api", "search_api", connection=connection
    )

    try:
        dataset = namespace.create_table(
            "documents",
            "SELECT * FROM (VALUES "
            "(1::BIGINT, 'a puppy follows a duck'::VARCHAR, 'animal'::VARCHAR, [0.0, 0.0]::FLOAT[2]), "
            "(2::BIGINT, 'a horse follows the puppy'::VARCHAR, 'animal'::VARCHAR, [1.0, 0.0]::FLOAT[2]), "
            "(3::BIGINT, 'a fantasy dragon'::VARCHAR, 'fiction'::VARCHAR, [5.0, 0.0]::FLOAT[2])) "
            "AS source(id, text, category, vec)",
        )
        dataset.create_index(
            "search_vec_idx",
            "vec",
            index_type="IVF_FLAT",
            num_partitions=1,
            metric_type="l2",
        )
        dataset.create_index("search_text_idx", "text", index_type="INVERTED")

        vector_ids = (
            dataset.vector_search(
                "vec",
                [0.0, 0.0],
                k=2,
                nprobes=1,
                refine_factor=2,
                prefilter=True,
                use_index=True,
                filter="category = 'animal'",
            )
            .project("id")
            .fetchall()
        )
        fts_ids = (
            dataset.fts(
                "text",
                "puppy",
                k=2,
                prefilter=True,
                filter="category = 'animal'",
            )
            .project("id")
            .order("id")
            .fetchall()
        )
        hybrid_ids = (
            dataset.hybrid_search(
                "vec",
                [0.0, 0.0],
                "text",
                "puppy",
                k=2,
                nprobes=1,
                refine_factor=2,
                prefilter=True,
                use_index=True,
                alpha=0.6,
                oversample_factor=2,
            )
            .filter("category = 'animal'")
            .project("id")
            .fetchall()
        )

        assert vector_ids == [(1,), (2,)]
        assert fts_ids == [(1,), (2,)]
        assert {row[0] for row in hybrid_ids} == {1, 2}
    finally:
        gc.collect()
        namespace.detach()
        connection.close()


def test_lance_search_local_query_is_not_distributed_credential_transport(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "local-search-credentials.lance"

    try:
        connection.execute(
            "COPY (SELECT * FROM (VALUES "
            "(1::BIGINT, 'a puppy'::VARCHAR, [0.0, 0.0]::FLOAT[2]), "
            "(2::BIGINT, 'a horse'::VARCHAR, [2.0, 0.0]::FLOAT[2])) "
            "AS source(id, text, vec)) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'create')"
        )
        dataset = LanceDataset(path, connection)
        dataset.create_index("local_text_idx", "text", index_type="INVERTED")
        connection.execute("SET s3_access_key_id='test-access-key'")
        connection.execute("SET s3_secret_access_key='test-secret-key'")

        searches = [
            (dataset.vector_search("vec", [0.0, 0.0], k=1), "local_vector"),
            (dataset.fts("text", "puppy", k=1), "local_fts"),
        ]
        for relation, alias in searches:
            local_query = relation.query(alias, f"SELECT id FROM {alias} ORDER BY id")
            assert local_query.fetchall() == [(1,)]
            del local_query
            gc.collect()

            with pytest.raises(Exception, match="serialized to workers") as error:
                vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                    relation, f"lance-{alias}-credential-transport"
                )
            assert "test-access-key" not in str(error.value)
            assert "test-secret-key" not in str(error.value)
    finally:
        connection.close()


def test_lance_vector_search_runs_as_one_global_distributed_split(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "vectors.lance"

    try:
        connection.execute(
            "COPY (SELECT * FROM (VALUES "
            "(1::BIGINT, [0.0, 0.0]::FLOAT[2]), "
            "(2::BIGINT, [1.0, 0.0]::FLOAT[2]), "
            "(3::BIGINT, [4.0, 0.0]::FLOAT[2])) AS source(id, vec)) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'create')"
        )
        relation = (
            LanceDataset(path, connection)
            .vector_search("vec", [0.0, 0.0], k=2)
            .project("id")
        )
        assert [
            next(iter(row.values())) for row in _run_distributed(ray_runner, relation)
        ] == [1, 2]
    finally:
        connection.close()


def test_lance_distributed_direct_create_then_attached_append_commit_once(
    tmp_path: Path, ray_write_runner
) -> None:
    del ray_write_runner
    connection = _connect()
    root = tmp_path / "distributed-namespace"
    path = root / "items.lance"
    create_input = tmp_path / "create-input"
    append_input = tmp_path / "append-input"
    namespace = None

    try:
        root.mkdir()
        _write_partitioned_parquet(
            connection, create_input, start=0, count=32, file_count=8
        )
        _write_partitioned_parquet(
            connection, append_input, start=32, count=16, file_count=8
        )

        LanceDataset(path, connection).write(
            connection.read_parquet(str(create_input / "*.parquet"))
        )
        create_files = _lance_data_files(path)
        assert len(_distributed_task_prefixes(create_files)) > 1
        namespace = LanceNamespace(root, "distributed_ns", connection=connection)
        table = namespace.table("items")
        assert table.scan().aggregate("count(*), min(id), max(id)").fetchone() == (
            32,
            0,
            31,
        )
        assert _lance_transaction_count(path) == 1
        _assert_staging_empty(path)

        connection.read_parquet(str(append_input / "*.parquet")).insert_into(
            table._sql_target()
        )
        append_files = _lance_data_files(path) - create_files
        assert len(_distributed_task_prefixes(append_files)) > 1
        assert table.scan().aggregate("count(*), min(id), max(id)").fetchone() == (
            48,
            0,
            47,
        )
        assert _lance_transaction_count(path) == 2
        _assert_staging_empty(path)
    finally:
        if namespace is not None:
            namespace.detach()
        connection.close()


def test_lance_distributed_direct_create_preserves_empty_schema_then_attaches(
    tmp_path: Path, ray_write_runner
) -> None:
    del ray_write_runner
    connection = _connect()
    root = tmp_path / "distributed-empty-namespace"
    path = root / "items.lance"
    source = tmp_path / "empty-input"
    namespace = None

    try:
        root.mkdir()
        _write_partitioned_parquet(connection, source, start=0, count=0, file_count=1)
        LanceDataset(path, connection).write(
            connection.read_parquet(str(source / "*.parquet"))
        )

        namespace = LanceNamespace(root, "distributed_empty_ns", connection=connection)
        table = namespace.table("items")
        relation = table.scan()
        assert relation.aggregate("count(*)").fetchone() == (0,)
        assert relation.columns == ["id", "value"]
        assert [str(logical_type) for logical_type in relation.types] == [
            "BIGINT",
            "VARCHAR",
        ]
        assert _lance_transaction_count(path) == 1
        _assert_staging_empty(path)
    finally:
        if namespace is not None:
            namespace.detach()
        connection.close()


def test_lance_distributed_direct_create_uses_generic_write_file_relation(
    tmp_path: Path, ray_write_runner
) -> None:
    del ray_write_runner
    connection = _connect()
    path = tmp_path / "distributed-direct.lance"
    source = tmp_path / "direct-input"

    try:
        _write_partitioned_parquet(connection, source, start=0, count=32, file_count=8)
        LanceDataset(path, connection).write(
            connection.read_parquet(str(source / "*.parquet"))
        )

        files = _lance_data_files(path)
        assert len(_distributed_task_prefixes(files)) > 1
        assert _scan(connection, path).aggregate(
            "count(*), min(id), max(id)"
        ).fetchone() == (32, 0, 31)
        assert _lance_transaction_count(path) == 1
        _assert_staging_empty(path)
    finally:
        connection.close()


def test_lance_distributed_write_rejects_connection_carried_credentials(
    tmp_path: Path,
    ray_write_runner,
) -> None:
    del ray_write_runner
    connection = _connect()
    path = tmp_path / "credential-snapshot-write.lance"

    try:
        connection.execute("SET s3_access_key_id='test-access-key'")
        connection.execute("SET s3_secret_access_key='test-secret-key'")
        with pytest.raises(Exception, match="worker-local credential chain"):
            LanceDataset(path, connection).write(
                connection.sql("SELECT 1::BIGINT AS id")
            )
        assert not path.exists()
    finally:
        connection.close()


def test_lance_distributed_read_rejects_unrelated_connection_credentials(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "credential-snapshot-read.lance"

    try:
        _write_dataset(connection, path)
        connection.execute("SET s3_access_key_id='test-access-key'")
        connection.execute("SET s3_secret_access_key='test-secret-key'")
        relation = _scan(connection, path)

        # DuckDB also serializes relations for process-local query/view copies.
        # Those copies reopen the dataset through this same connection and must
        # not be mistaken for Vane's cross-process plan transport.
        local_query = relation.query(
            "credential_local_scan",
            "SELECT count(*), min(id), max(id) FROM credential_local_scan",
        )
        assert local_query.fetchone() == (12, 0, 11)
        del local_query
        gc.collect()

        with pytest.raises(Exception, match="worker-local credential chain") as error:
            vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                relation, "lance-credential-snapshot-read"
            )
        assert "test-access-key" not in str(error.value)
        assert "test-secret-key" not in str(error.value)
    finally:
        connection.close()


def test_lance_distributed_attached_insert_rejects_connection_credentials(
    tmp_path: Path,
    ray_write_runner,
) -> None:
    del ray_write_runner
    connection = _connect()
    root = tmp_path / "credential-snapshot-attached"
    path = root / "items.lance"
    namespace = None

    try:
        root.mkdir()
        _write_dataset(connection, path)
        namespace = LanceNamespace(
            root, "credential_snapshot_attached", connection=connection
        )
        table = namespace.table("items")
        connection.execute("SET s3_access_key_id='test-access-key'")
        connection.execute("SET s3_secret_access_key='test-secret-key'")
        source = connection.sql("SELECT 12::BIGINT AS id, 'value-12'::VARCHAR AS value")

        with pytest.raises(
            Exception, match="does not provide a logical write target definition"
        ) as error:
            source.insert_into(table._sql_target())
        assert "test-access-key" not in str(error.value)
        assert "test-secret-key" not in str(error.value)

        # The empty distributed definition is only a serialization fence.
        # Ordinary SQL stays coordinator-local and must remain usable on the
        # same connection.
        connection.execute(
            f"INSERT INTO {table._sql_target()} VALUES "
            "(12::BIGINT, 'value-12'::VARCHAR)"
        )
        assert connection.execute(
            f"SELECT count(*), max(id) FROM {table._sql_target()}"
        ).fetchone() == (13, 12)
    finally:
        if namespace is not None:
            namespace.detach()
        connection.close()


def test_lance_rest_write_target_identity_excludes_vended_uri_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _connect()
    path = tmp_path / "rest-vended-uri-identity.lance"
    sensitive_uri = path.as_uri() + "?session_token=hidden#credential-fragment"
    arrow_schema = {
        "fields": [
            {
                "name": "id",
                "nullable": True,
                "type": {"type": "int64"},
            },
            {
                "name": "value",
                "nullable": True,
                "type": {"type": "utf8"},
            },
        ]
    }

    # The synchronous native namespace call holds the Python GIL, so the mock
    # server must run in a separate process rather than a Python thread.
    server_source = textwrap.dedent(
        r"""
        import json
        import sys
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        sensitive_uri = sys.argv[1]
        arrow_schema = json.loads(sys.argv[2])

        class NamespaceHandler(BaseHTTPRequestHandler):
            def _reply(self, payload):
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if "/table/list" not in self.path:
                    self.send_error(404)
                    return
                self._reply({"tables": ["items"]})

            def do_POST(self):
                content_length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(content_length)
                if not self.path.split("?", 1)[0].endswith("/describe"):
                    self.send_error(404)
                    return
                self._reply(
                    {
                        "location": sensitive_uri,
                        "table_uri": sensitive_uri,
                        "version": 1,
                        "schema": arrow_schema,
                    }
                )

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), NamespaceHandler)
        print(server.server_port, flush=True)
        server.serve_forever()
        """
    )
    server_process = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            server_source,
            sensitive_uri,
            json.dumps(arrow_schema),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert server_process.stdout is not None
    port_line = server_process.stdout.readline().strip()
    if not port_line:
        assert server_process.stderr is not None
        raise AssertionError(
            "REST namespace mock failed to start: " + server_process.stderr.read()
        )
    attached = False

    try:
        _write_dataset(connection, path)
        endpoint = f"http://127.0.0.1:{int(port_line)}"
        connection.execute(
            "ATTACH 'catalog' AS rest_vended_identity "
            f"(TYPE LANCE, ENDPOINT {_sql_literal(endpoint)}, READ_ONLY false)"
        )
        attached = True

        captured = []

        class CapturingRunner:
            def run_write(self, relation):
                captured.append(relation)
                return {"ok": True}

        monkeypatch.setenv("VANE_RUNNER", "ray")
        monkeypatch.setattr(
            runners,
            "set_runner_ray",
            lambda *_args, **_kwargs: CapturingRunner(),
        )
        connection.sql(
            "SELECT 2::BIGINT AS id, 'new-value'::VARCHAR AS value"
        ).insert_into("rest_vended_identity.main.items")

        assert len(captured) == 1
        logical_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_write_relation(
            captured[0], "lance-rest-vended-uri-identity"
        )
        serialized_plan = bytes(logical_plan.__getstate__()[1])

        assert path.as_uri().encode() in serialized_plan
        for secret in ("session_token", "hidden", "credential-fragment"):
            assert secret.encode() not in serialized_plan
    finally:
        try:
            if attached:
                connection.execute("DETACH rest_vended_identity")
        finally:
            connection.close()
            server_process.terminate()
            try:
                server_process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
                server_process.communicate()


def test_lance_attached_insert_uses_native_commit_conflict_and_cleans_files(
    tmp_path: Path,
) -> None:
    connection = _connect()
    root = tmp_path / "distributed-failure-namespace"
    path = root / "items.lance"
    create_input = tmp_path / "failure-create-input"
    pending_input = tmp_path / "failure-append-input"
    namespace = None
    rival_connection = None
    transaction_open = False

    try:
        root.mkdir()
        _write_partitioned_parquet(
            connection, create_input, start=0, count=16, file_count=4
        )
        _write_partitioned_parquet(
            connection,
            pending_input,
            start=16,
            count=16,
            file_count=4,
        )
        LanceDataset(path, connection).write(
            connection.read_parquet(str(create_input / "*.parquet"))
        )
        namespace = LanceNamespace(
            root, "distributed_failure_ns", connection=connection
        )
        table = namespace.table("items")
        files_before_pending_append = _lance_data_files(path)
        connection.begin()
        transaction_open = True
        connection.read_parquet(str(pending_input / "*.parquet")).insert_into(
            table._sql_target()
        )
        pending_append_files = _lance_data_files(path) - files_before_pending_append
        assert pending_append_files

        # Commit a deterministic competing MVCC transaction after the first
        # connection prepared its uncommitted append.
        rival_connection = _connect()
        LanceDataset(path, rival_connection).write(
            rival_connection.sql(
                "SELECT 100::BIGINT AS id, " "'rival'::VARCHAR AS replacement_value"
            ),
            mode="overwrite",
        )
        rival_connection.close()
        rival_connection = None

        files_after_rival_commit = _lance_data_files(path)
        try:
            with pytest.raises(Exception, match=r"(?i)(commit|conflict|version)"):
                connection.commit()
        finally:
            transaction_open = False

        namespace.detach()
        namespace = LanceNamespace(
            root, "distributed_failure_ns", connection=connection
        )
        table = namespace.table("items")
        assert table.scan().aggregate("count(*), min(id), max(id)").fetchone() == (
            1,
            100,
            100,
        )
        assert _lance_transaction_count(path) == 2
        assert _lance_data_files(path) == (
            files_after_rival_commit - pending_append_files
        )
        _assert_staging_empty(path)
        _assert_no_coordination_artifacts(path)

        # The failed coordinator commit was definitively rejected and its
        # operation-owned files were cleaned up, so a corrected append can
        # proceed.
        connection.sql(
            "SELECT 101::BIGINT AS id, 'recovered'::VARCHAR AS replacement_value"
        ).insert_into(table._sql_target())
        assert table.scan().aggregate("count(*), min(id), max(id)").fetchone() == (
            2,
            100,
            101,
        )
        assert _lance_transaction_count(path) == 3
        _assert_staging_empty(path)
        _assert_no_coordination_artifacts(path)
    finally:
        if rival_connection is not None:
            rival_connection.close()
        if transaction_open:
            connection.rollback()
        if namespace is not None:
            namespace.detach()
        connection.close()


def test_lance_attached_table_writes_use_standard_relation_api(tmp_path: Path) -> None:
    connection = _connect()
    namespace = LanceNamespace(
        tmp_path / "attached-write", "attached_write", connection=connection
    )

    try:
        table = namespace.create_table(
            "items", "SELECT 0::BIGINT AS id, 'zero'::VARCHAR AS value"
        )
        source = connection.sql(
            "SELECT * FROM (VALUES (1::BIGINT, 'one'::VARCHAR), (2::BIGINT, 'two'::VARCHAR)) AS source(id, value)"
        )
        # This is the public write boundary.  The relation method is the
        # generic Vane runner dispatch point; no Lance-named relation method
        # or Python dependency hook is involved.
        source.insert_into(table._sql_target())
        assert table.scan().order("id").fetchall() == [
            (0, "zero"),
            (1, "one"),
            (2, "two"),
        ]
        assert not hasattr(source, "write_lance")
        assert not hasattr(source, "to_lance")
        assert not hasattr(source, "execute_write_relation")
        assert not hasattr(source, "_attach_external_dependency")
    finally:
        namespace.detach()
        connection.close()


def test_lance_dataset_write_preserves_empty_schema_without_direct_relation_api(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "empty.lance"

    try:
        relation = connection.sql(
            "SELECT * FROM (VALUES (1::BIGINT, 'unused'::VARCHAR)) AS source(id, value) WHERE false"
        )
        LanceDataset(path, connection).write(relation)
        relation = _scan(connection, path)
        assert relation.aggregate("count(*)").fetchone() == (0,)
        assert relation.columns == ["id", "value"]
        assert [str(logical_type) for logical_type in relation.types] == [
            "BIGINT",
            "VARCHAR",
        ]
        assert _lance_transaction_count(path) == 1
    finally:
        connection.close()


def test_lance_dataset_write_failure_is_not_silently_retryable(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "failure.lance"

    try:
        dataset = LanceDataset(path, connection)
        dataset.write(
            connection.sql(
                "SELECT i::BIGINT AS id, i::VARCHAR AS value FROM range(16) AS t(i)"
            )
        )
        with pytest.raises(Exception):
            dataset.write(
                connection.sql(
                    "SELECT i::BIGINT AS id, i::BIGINT AS value FROM range(16) AS t(i)"
                ),
                mode="append",
            )
        assert _scan(connection, path).aggregate("count(*)").fetchone() == (16,)
    finally:
        connection.close()
