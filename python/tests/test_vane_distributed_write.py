# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import shutil
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import vane

from lance_fixture import write_fixture_query
from packaged_dynamic_extension import load_packaged_dynamic_lance

pytestmark = pytest.mark.usefixtures("default_ray_runtime")

WORKER_COUNT = 2
STABLE_ROW_IDS_DATASET = (
    Path(__file__).resolve().parents[2] / "test/data/stable_row_ids.lance"
)


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _connect():
    connection = vane.connect(
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        }
    )
    dynamic_provider = os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
    if dynamic_provider:
        load_packaged_dynamic_lance(connection)
    else:
        connection.execute("LOAD lance")
    loaded, install_mode = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() "
        "WHERE lower(extension_name) = 'lance'"
    ).fetchone()
    assert loaded is True
    if dynamic_provider:
        assert str(install_mode).upper() == "NOT_INSTALLED"
    else:
        assert str(install_mode).upper() == "STATICALLY_LINKED"
    return connection


def _write_source(connection, path: str | Path) -> None:
    write_fixture_query(
        connection,
        path,
        f"SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS value FROM range(80) AS source(i)",
        mode="create",
        max_rows_per_file=10,
    )


def _write_failure_source(connection, path: str | Path) -> None:
    # Keep the rows in one fragment that spans many DuckDB chunks so the
    # downstream writer receives data before the injected expression error.
    write_fixture_query(
        connection,
        path,
        f"SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS value FROM range(32768) AS source(i)",
        mode="create",
        max_rows_per_file=65536,
        max_rows_per_group=1024,
    )


def _write_upstream_container_target(path: Path) -> None:
    import lance
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("values", pa.list_(pa.float32())),
            pa.field("vector", pa.list_(pa.float32(), 3)),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array([-1], type=schema.field("id").type),
            pa.array([[0.5, 1.5]], type=schema.field("values").type),
            pa.array([[-1.0, 0.0, 1.0]], type=schema.field("vector").type),
        ],
        schema=schema,
    )
    lance.write_dataset(table, str(path), mode="create")

    written_schema = lance.dataset(str(path)).schema
    assert written_schema.field("values").type.value_field.name == "item"
    assert written_schema.field("vector").type.value_field.name == "item"


def _artifact_count(
    path: str | Path, directory: str, suffix: str, *, recursive: bool = False
) -> int:
    # Inspect committed file layout independently of Vane query routing.
    import pyarrow.fs as fs

    location = str(path)
    if location.startswith("s3://"):
        parsed = urlsplit(location)
        endpoint = urlsplit(os.environ["AWS_ENDPOINT_URL"])
        filesystem = fs.S3FileSystem(
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region=os.environ["AWS_REGION"],
            endpoint_override=endpoint.netloc or endpoint.path,
            scheme=endpoint.scheme or "http",
        )
        base = f"{parsed.netloc}{parsed.path}"
    else:
        filesystem = fs.LocalFileSystem()
        base = str(Path(location).resolve())
    selector = fs.FileSelector(
        f"{base.rstrip('/')}/{directory}", recursive=recursive, allow_not_found=True
    )
    return sum(
        info.type == fs.FileType.File and info.path.endswith(suffix)
        for info in filesystem.get_file_info(selector)
    )


def _manifest_count(connection, path: str | Path) -> int:
    return _artifact_count(path, "_versions", ".manifest")


def _data_file_count(connection, path: str | Path) -> int:
    return _artifact_count(path, "data", ".lance")


def _deletion_file_count(connection, path: str | Path) -> int:
    return _artifact_count(path, "_deletions", "", recursive=True)


def _attempt_manifest_count(connection, path: str | Path) -> int:
    return _artifact_count(
        path, "_vane_distributed_write_attempts", ".manifest", recursive=True
    )


def _mutation_attempt_manifest_count(connection, path: str | Path) -> int:
    return _artifact_count(
        path, "_vane_distributed_mutation_attempts", ".manifest", recursive=True
    )


def _configure_s3(connection) -> dict[str, str]:
    names = (
        "AWS_ACCESS_KEY_ID",
        "AWS_ENDPOINT_URL",
        "AWS_REGION",
        "AWS_SECRET_ACCESS_KEY",
        "LANCE_S3_BUCKET",
    )
    config = {name: os.environ[name] for name in names}
    connection.execute("LOAD httpfs")
    connection.execute(
        f"SET s3_access_key_id = {_sql_literal(config['AWS_ACCESS_KEY_ID'])}"
    )
    connection.execute(
        "SET s3_secret_access_key = " f"{_sql_literal(config['AWS_SECRET_ACCESS_KEY'])}"
    )
    connection.execute(f"SET s3_region = {_sql_literal(config['AWS_REGION'])}")
    parsed_endpoint = urlsplit(config["AWS_ENDPOINT_URL"])
    duckdb_endpoint = parsed_endpoint.netloc or parsed_endpoint.path
    connection.execute(f"SET s3_endpoint = {_sql_literal(duckdb_endpoint)}")
    connection.execute("SET s3_url_style = 'path'")
    connection.execute("SET s3_use_ssl = false")
    return config


class DistributedWriteCapture:
    def __init__(self, runner) -> None:
        self.runner = runner
        self.dispatch_count = 0
        self.last_result: dict[str, object] | None = None
        self.original_run_write = runner.run_write

        def record(logical_plan: object) -> object:
            assert isinstance(logical_plan, vane.ray_cxx.PyLogicalPlan)
            self.dispatch_count += 1
            self.last_result = None
            try:
                result = self.original_run_write(logical_plan)
            except BaseException:
                self.last_result = None
                raise
            self.last_result = result
            return result

        self.record = record
        runner.run_write = record

    def close(self) -> None:
        if self.runner.run_write is not self.record:
            raise RuntimeError("the distributed write capture lost runner ownership")
        self.runner.run_write = self.original_run_write

    def require_write(
        self,
        description: str,
        operation: Callable[[], object],
        *,
        expected_name: str,
        expected_rows: int,
        minimum_task_results: int,
        minimum_artifacts: int | None = None,
        allow_empty_tasks: bool = False,
    ) -> dict[str, object]:
        previous_count = self.dispatch_count
        self.last_result = None
        operation()
        assert (
            self.dispatch_count == previous_count + 1
        ), f"{description} did not dispatch exactly one Vane write"
        assert (
            self.last_result is not None
        ), f"{description} returned no distributed write result"
        result = self.last_result
        assert result.get("extension_write") is True
        assert result.get("extension_write_name") == expected_name
        assert result.get("extension_write_mode") == "callback"
        assert result.get("extension_catalog_committed") is True
        assert result.get("rows_copied") == expected_rows
        task_count = int(result.get("extension_task_result_count", 0))
        fragment_count = int(result.get("extension_fragment_count", 0))
        artifact_count = int(result.get("extension_artifact_count", 0))
        assert task_count >= minimum_task_results
        if expected_rows == 0:
            assert fragment_count == 0
            assert artifact_count == 0
        else:
            if allow_empty_tasks:
                assert 0 < fragment_count <= task_count
            else:
                assert fragment_count == task_count
            if minimum_artifacts is None:
                assert artifact_count >= fragment_count
            else:
                assert artifact_count >= minimum_artifacts
        return result


@pytest.fixture
def write_capture(default_ray_runtime):
    capture = DistributedWriteCapture(default_ray_runtime)
    try:
        yield capture
    finally:
        capture.close()


def _exercise_insert_and_ctas(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    source_path: str | Path,
    insert_path: str | Path,
    ctas_path: str | Path,
    empty_ctas_path: str | Path,
) -> None:
    connection.execute(
        f"CREATE TABLE {catalog}.main.insert_target " "(id BIGINT, value VARCHAR)"
    )
    source = connection.sql(f"SELECT id, value FROM {catalog}.main.source")

    capture.require_write(
        "distributed Lance INSERT",
        lambda: source.insert_into(f"{catalog}.main.insert_target"),
        expected_name="insert",
        expected_rows=80,
        minimum_task_results=2,
    )
    assert _manifest_count(connection, insert_path) == 2
    assert _attempt_manifest_count(connection, insert_path) == 0
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT "
        f"FROM {catalog}.main.insert_target"
    ).fetchone() == (80, 3160)

    capture.require_write(
        "distributed Lance CTAS",
        lambda: source.create(f"{catalog}.main.ctas_target"),
        expected_name="ctas",
        expected_rows=80,
        minimum_task_results=2,
    )
    assert _manifest_count(connection, ctas_path) == 2
    assert _attempt_manifest_count(connection, ctas_path) == 0
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT " f"FROM {catalog}.main.ctas_target"
    ).fetchone() == (80, 3160)

    empty_source = connection.sql(
        f"SELECT id, value FROM {catalog}.main.source WHERE false"
    )
    capture.require_write(
        "empty distributed Lance INSERT",
        lambda: empty_source.insert_into(f"{catalog}.main.insert_target"),
        expected_name="insert",
        expected_rows=0,
        minimum_task_results=1,
    )
    assert _manifest_count(connection, insert_path) == 2
    assert _attempt_manifest_count(connection, insert_path) == 0

    capture.require_write(
        "empty distributed Lance CTAS",
        lambda: empty_source.create(f"{catalog}.main.empty_ctas_target"),
        expected_name="ctas",
        expected_rows=0,
        minimum_task_results=1,
    )
    assert _manifest_count(connection, empty_ctas_path) == 1
    assert _attempt_manifest_count(connection, empty_ctas_path) == 0
    assert connection.execute(
        f"SELECT count(*)::BIGINT FROM {catalog}.main.empty_ctas_target"
    ).fetchone() == (0,)
    assert _manifest_count(connection, source_path) == 1


def _exercise_upstream_container_target(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    target_path: Path,
) -> None:
    source = connection.sql(
        f"SELECT id, [id::FLOAT, (id + 1)::FLOAT]::FLOAT[] AS values, "
        f"[id::FLOAT, (id + 1)::FLOAT, (id + 2)::FLOAT]::FLOAT[3] AS vector "
        f"FROM {catalog}.main.source"
    )
    capture.require_write(
        "distributed Lance INSERT into an upstream container target",
        lambda: source.insert_into(f"{catalog}.main.upstream_container_target"),
        expected_name="insert",
        expected_rows=80,
        minimum_task_results=2,
    )
    assert _manifest_count(connection, target_path) == 2
    assert _attempt_manifest_count(connection, target_path) == 0
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT, "
        f'sum(list_sum("values"))::DOUBLE, sum(vector[1])::DOUBLE '
        f"FROM {catalog}.main.upstream_container_target"
    ).fetchone() == (81, 3159, 6402.0, 3159.0)


def _exercise_failed_ctas_retention_and_explicit_retry(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    target_path: str | Path,
) -> None:
    failing_source = connection.sql(
        f"SELECT id, CASE WHEN id < 16384 THEN value "
        "ELSE error('intentional distributed Lance worker failure') END AS value "
        f"FROM {catalog}.main.failure_source"
    )
    previous_count = capture.dispatch_count
    with pytest.raises(Exception):
        failing_source.create(f"{catalog}.main.failed_ctas_target")
    assert capture.dispatch_count == previous_count + 1
    assert _manifest_count(connection, target_path) == 1
    assert connection.execute(
        f"SELECT count(*)::BIGINT FROM {catalog}.main.failed_ctas_target"
    ).fetchone() == (0,)
    assert _data_file_count(connection, target_path) == 0
    assert _attempt_manifest_count(connection, target_path) == 0

    source = connection.sql(f"SELECT id, value FROM {catalog}.main.source")
    with pytest.raises(Exception):
        source.create(f"{catalog}.main.failed_ctas_target")

    connection.execute(f"DROP TABLE {catalog}.main.failed_ctas_target")
    assert _manifest_count(connection, target_path) == 0
    source = connection.sql(f"SELECT id, value FROM {catalog}.main.source")
    capture.require_write(
        "distributed Lance CTAS retried after explicit cleanup",
        lambda: source.create(f"{catalog}.main.failed_ctas_target"),
        expected_name="ctas",
        expected_rows=80,
        minimum_task_results=2,
    )
    assert _manifest_count(connection, target_path) == 2
    assert _attempt_manifest_count(connection, target_path) == 0


def _exercise_explicit_transaction_rejection(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    insert_path: str | Path,
    ctas_path: str | Path,
) -> None:
    source = connection.sql(f"SELECT id, value FROM {catalog}.main.source")
    insert_manifest_count = _manifest_count(connection, insert_path)
    insert_row_count = connection.execute(
        f"SELECT count(*)::BIGINT FROM {catalog}.main.insert_target"
    ).fetchone()

    previous_count = capture.dispatch_count
    connection.execute("BEGIN TRANSACTION")
    try:
        with pytest.raises(Exception, match="auto-commit mode"):
            source.insert_into(f"{catalog}.main.insert_target")
    finally:
        connection.execute("ROLLBACK")
    assert capture.dispatch_count == previous_count
    assert _manifest_count(connection, insert_path) == insert_manifest_count
    assert _attempt_manifest_count(connection, insert_path) == 0
    assert (
        connection.execute(
            f"SELECT count(*)::BIGINT FROM {catalog}.main.insert_target"
        ).fetchone()
        == insert_row_count
    )

    previous_count = capture.dispatch_count
    connection.execute("BEGIN TRANSACTION")
    try:
        with pytest.raises(Exception, match="auto-commit mode"):
            source.create(f"{catalog}.main.explicit_ctas_target")
    finally:
        connection.execute("ROLLBACK")
    assert capture.dispatch_count == previous_count
    assert _manifest_count(connection, ctas_path) == 0
    assert _data_file_count(connection, ctas_path) == 0
    assert _attempt_manifest_count(connection, ctas_path) == 0


def _exercise_stale_target_type_rejection(
    connection,
    capture: DistributedWriteCapture,
    *,
    root: Path,
    target_path: Path,
) -> None:
    connection.execute("CREATE TABLE lance_write.main.stale_target (id INTEGER)")
    assert connection.execute("DESCRIBE lance_write.main.stale_target").fetchone()[
        :2
    ] == ("id", "INTEGER")

    evolution_connection = _connect()
    try:
        evolution_connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_evolve (TYPE LANCE)"
        )
        evolution_connection.execute(
            "ALTER TABLE lance_evolve.main.stale_target " "ALTER COLUMN id TYPE BIGINT"
        )
        assert evolution_connection.execute(
            "DESCRIBE lance_evolve.main.stale_target"
        ).fetchone()[:2] == ("id", "BIGINT")

        manifest_count = _manifest_count(evolution_connection, target_path)
        data_file_count = _data_file_count(evolution_connection, target_path)
        previous_count = capture.dispatch_count
        source = connection.sql("SELECT i::INTEGER AS id FROM range(3) AS source(i)")
        with pytest.raises(
            Exception,
            match="definition changed|input field 'id' has type Int32",
        ):
            source.insert_into("lance_write.main.stale_target")
        assert capture.dispatch_count == previous_count + 1
        assert _manifest_count(evolution_connection, target_path) == manifest_count
        assert _data_file_count(evolution_connection, target_path) == data_file_count
        assert _attempt_manifest_count(evolution_connection, target_path) == 0
        assert evolution_connection.execute(
            "SELECT count(*)::BIGINT FROM lance_evolve.main.stale_target"
        ).fetchone() == (0,)
    finally:
        evolution_connection.close()


def _exercise_not_null_target(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    target_path: str | Path,
) -> None:
    connection.execute(f"CREATE TABLE {catalog}.main.required_target (id INTEGER)")
    connection.execute(
        f"ALTER TABLE {catalog}.main.required_target ALTER COLUMN id SET NOT NULL"
    )
    assert connection.execute(f"DESCRIBE {catalog}.main.required_target").fetchone()[
        :3
    ] == ("id", "INTEGER", "NO")

    source = connection.sql("SELECT i::INTEGER AS id FROM range(3) AS source(i)")
    capture.require_write(
        "distributed Lance INSERT into a NOT NULL target",
        lambda: source.insert_into(f"{catalog}.main.required_target"),
        expected_name="insert",
        expected_rows=3,
        minimum_task_results=1,
    )
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT "
        f"FROM {catalog}.main.required_target"
    ).fetchone() == (3, 3)

    manifest_count = _manifest_count(connection, target_path)
    data_file_count = _data_file_count(connection, target_path)
    previous_count = capture.dispatch_count
    null_source = connection.sql("SELECT NULL::INTEGER AS id")
    with pytest.raises(Exception, match="NOT NULL constraint failed"):
        null_source.insert_into(f"{catalog}.main.required_target")
    assert capture.dispatch_count == previous_count + 1
    assert _manifest_count(connection, target_path) == manifest_count
    assert _data_file_count(connection, target_path) == data_file_count
    assert _attempt_manifest_count(connection, target_path) == 0
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT "
        f"FROM {catalog}.main.required_target"
    ).fetchone() == (3, 3)


def _exercise_nested_not_null_target(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    target_path: str | Path,
) -> None:
    connection.execute(
        f"CREATE TABLE {catalog}.main.nested_required_target "
        "(payload STRUCT(value INTEGER))"
    )
    connection.execute(
        f"ALTER TABLE {catalog}.main.nested_required_target "
        'ALTER COLUMN "payload.value" SET NOT NULL'
    )

    source = connection.sql(
        "SELECT struct_pack(value := i::INTEGER) AS payload "
        "FROM range(3) AS source(i)"
    )
    capture.require_write(
        "distributed Lance INSERT into a nested NOT NULL target",
        lambda: source.insert_into(f"{catalog}.main.nested_required_target"),
        expected_name="insert",
        expected_rows=3,
        minimum_task_results=1,
    )
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(payload.value)::BIGINT "
        f"FROM {catalog}.main.nested_required_target"
    ).fetchone() == (3, 3)

    manifest_count = _manifest_count(connection, target_path)
    data_file_count = _data_file_count(connection, target_path)
    previous_count = capture.dispatch_count
    capture.last_result = None
    child_null_source = connection.sql(
        "SELECT struct_pack(value := NULL::INTEGER) AS payload"
    )
    with pytest.raises(
        Exception,
        match="Failed to write distributed Lance worker batch",
    ):
        child_null_source.insert_into(f"{catalog}.main.nested_required_target")
    assert capture.dispatch_count == previous_count + 1
    assert capture.last_result is None
    assert _manifest_count(connection, target_path) == manifest_count
    assert _data_file_count(connection, target_path) == data_file_count
    assert _attempt_manifest_count(connection, target_path) == 0
    assert connection.execute(
        f"SELECT count(*)::BIGINT, sum(payload.value)::BIGINT "
        f"FROM {catalog}.main.nested_required_target"
    ).fetchone() == (3, 3)


def _exercise_update_and_delete(
    connection,
    capture: DistributedWriteCapture,
    *,
    catalog: str,
    target_path: str | Path,
) -> None:
    assert _manifest_count(connection, target_path) == 1

    capture.require_write(
        "distributed Lance UPDATE",
        lambda: connection.table(f"{catalog}.main.mutation_target").update(
            {"value": vane.lit("updated")},
            condition=vane.col("id") < 40,
        ),
        expected_name="update",
        expected_rows=40,
        minimum_task_results=2,
        minimum_artifacts=1,
        allow_empty_tasks=True,
    )
    assert _manifest_count(connection, target_path) == 2
    assert _mutation_attempt_manifest_count(connection, target_path) == 0
    assert connection.execute(
        f"SELECT id, value FROM {catalog}.main.mutation_target ORDER BY id"
    ).fetchall() == [(i, "updated" if i < 40 else f"value-{i}") for i in range(80)]

    capture.require_write(
        "distributed Lance DELETE",
        lambda: connection.table(f"{catalog}.main.mutation_target").delete(
            condition=vane.col("id") >= 60
        ),
        expected_name="delete",
        expected_rows=20,
        minimum_task_results=2,
        # Deleting complete fragments legitimately produces no deletion file.
        minimum_artifacts=0,
        allow_empty_tasks=True,
    )
    assert _manifest_count(connection, target_path) == 3
    assert _mutation_attempt_manifest_count(connection, target_path) == 0
    assert connection.execute(
        f"SELECT id, value FROM {catalog}.main.mutation_target ORDER BY id"
    ).fetchall() == [(i, "updated" if i < 40 else f"value-{i}") for i in range(60)]

    for name, operation in (
        (
            "zero-match distributed Lance UPDATE",
            lambda: connection.table(f"{catalog}.main.mutation_target").update(
                {"value": vane.lit("unreachable")},
                condition=vane.col("id") < 0,
            ),
        ),
        (
            "zero-match distributed Lance DELETE",
            lambda: connection.table(f"{catalog}.main.mutation_target").delete(
                condition=vane.col("id") >= 1000
            ),
        ),
    ):
        capture.require_write(
            name,
            operation,
            expected_name="update" if "UPDATE" in name else "delete",
            expected_rows=0,
            minimum_task_results=1,
        )
        assert _manifest_count(connection, target_path) == 3
        assert _mutation_attempt_manifest_count(connection, target_path) == 0


def test_two_worker_local_shared_lance_update_and_delete(
    tmp_path: Path, write_capture: DistributedWriteCapture
) -> None:
    connection = _connect()
    root = tmp_path / "distributed-mutation"
    root.mkdir()
    target_path = root / "mutation_target.lance"
    try:
        _write_source(connection, target_path)
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_mutation (TYPE LANCE)"
        )
        _exercise_update_and_delete(
            connection,
            write_capture,
            catalog="lance_mutation",
            target_path=target_path,
        )
    finally:
        connection.close()


def test_two_worker_single_fragment_lance_update_and_delete(
    tmp_path: Path, write_capture: DistributedWriteCapture
) -> None:
    connection = _connect()
    root = tmp_path / "single-fragment-distributed-mutation"
    root.mkdir()
    target_path = root / "mutation_target.lance"
    try:
        write_fixture_query(
            connection,
            target_path,
            f"SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS value FROM range(16) AS source(i)",
            mode="create",
            max_rows_per_file=64,
        )
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_single_mutation (TYPE LANCE)"
        )

        write_capture.require_write(
            "single-fragment distributed Lance UPDATE",
            lambda: connection.table(
                "lance_single_mutation.main.mutation_target"
            ).update(
                {"value": vane.lit("single-updated")},
                condition=vane.col("id") < 3,
            ),
            expected_name="update",
            expected_rows=3,
            minimum_task_results=1,
            minimum_artifacts=2,
            allow_empty_tasks=True,
        )
        write_capture.require_write(
            "single-fragment distributed Lance DELETE",
            lambda: connection.table(
                "lance_single_mutation.main.mutation_target"
            ).delete(condition=vane.col("id") >= 13),
            expected_name="delete",
            expected_rows=3,
            minimum_task_results=1,
            minimum_artifacts=1,
            allow_empty_tasks=True,
        )
        assert connection.execute(
            "SELECT id, value FROM lance_single_mutation.main.mutation_target ORDER BY id"
        ).fetchall() == [
            (i, "single-updated" if i < 3 else f"value-{i}") for i in range(13)
        ]
        assert _manifest_count(connection, target_path) == 3
        assert _mutation_attempt_manifest_count(connection, target_path) == 0
    finally:
        connection.close()


def test_distributed_lance_mutation_rejects_a_stale_bound_snapshot(
    tmp_path: Path,
    write_capture: DistributedWriteCapture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vane.runners.ray.driver import RayQueryDriverClient

    connection = _connect()
    evolution_connection = _connect()
    root = tmp_path / "stale-distributed-mutation"
    root.mkdir()
    target_path = root / "mutation_target.lance"
    try:
        _write_source(connection, target_path)
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_mutation (TYPE LANCE)"
        )
        evolution_connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_evolve (TYPE LANCE)"
        )

        original_run_copy_plan = RayQueryDriverClient.run_copy_plan
        raced = False

        def run_after_concurrent_commit(client, plan):
            nonlocal raced
            assert raced is False
            raced = True
            monkeypatch.setattr(
                RayQueryDriverClient, "run_copy_plan", original_run_copy_plan
            )
            evolution_connection.execute(
                "INSERT INTO lance_evolve.main.mutation_target "
                "VALUES (1000, 'concurrent')"
            )
            return original_run_copy_plan(client, plan)

        monkeypatch.setattr(
            RayQueryDriverClient,
            "run_copy_plan",
            run_after_concurrent_commit,
        )
        previous_dispatch_count = write_capture.dispatch_count
        with pytest.raises(
            Exception,
            match="changed after.*bound|changed after.*planned|target version changed",
        ):
            connection.table("lance_mutation.main.mutation_target").update(
                {"value": vane.lit("stale-update")},
                condition=vane.col("id") < 40,
            )
        assert raced is True
        assert write_capture.dispatch_count == previous_dispatch_count + 2
        assert write_capture.last_result is None
        assert _manifest_count(evolution_connection, target_path) == 2
        assert _mutation_attempt_manifest_count(evolution_connection, target_path) == 0
        assert evolution_connection.execute(
            "SELECT id, value FROM lance_evolve.main.mutation_target ORDER BY id"
        ).fetchall() == [(i, f"value-{i}") for i in range(80)] + [(1000, "concurrent")]
    finally:
        evolution_connection.close()
        connection.close()


def test_failed_distributed_lance_update_keeps_the_target_unchanged(
    tmp_path: Path, write_capture: DistributedWriteCapture
) -> None:
    connection = _connect()
    root = tmp_path / "failed-distributed-mutation"
    root.mkdir()
    target_path = root / "mutation_target.lance"
    try:
        _write_source(connection, target_path)
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_mutation (TYPE LANCE)"
        )
        connection.execute(
            "ALTER TABLE lance_mutation.main.mutation_target "
            "ALTER COLUMN value SET NOT NULL"
        )
        initial_manifest_count = _manifest_count(connection, target_path)
        initial_data_file_count = _data_file_count(connection, target_path)
        initial_deletion_file_count = _deletion_file_count(connection, target_path)

        previous_dispatch_count = write_capture.dispatch_count
        with pytest.raises(Exception, match="NULL|NOT NULL|worker mutation"):
            connection.table("lance_mutation.main.mutation_target").update(
                {
                    "value": vane.SQLExpression(
                        "CASE WHEN id < 40 THEN value ELSE NULL END"
                    )
                }
            )
        assert write_capture.dispatch_count == previous_dispatch_count + 1
        assert write_capture.last_result is None
        assert _manifest_count(connection, target_path) == initial_manifest_count
        assert _data_file_count(connection, target_path) == initial_data_file_count
        assert (
            _deletion_file_count(connection, target_path) == initial_deletion_file_count
        )
        assert _mutation_attempt_manifest_count(connection, target_path) == 0
        assert connection.execute(
            "SELECT id, value FROM lance_mutation.main.mutation_target ORDER BY id"
        ).fetchall() == [(i, f"value-{i}") for i in range(80)]
    finally:
        connection.close()


def test_two_worker_lance_update_preserves_stable_row_ids(
    tmp_path: Path, write_capture: DistributedWriteCapture
) -> None:
    connection = _connect()
    root = tmp_path / "distributed-stable-row-id-mutation"
    root.mkdir()
    target_path = root / "stable_target.lance"
    shutil.copytree(STABLE_ROW_IDS_DATASET, target_path)
    try:
        initial_manifest_count = _manifest_count(connection, target_path)
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_stable_mutation (TYPE LANCE)"
        )
        assert connection.execute(
            f"SELECT id, _rowid FROM {_sql_literal(target_path)} " "ORDER BY _rowid"
        ).fetchall() == [
            (0, 0),
            (2, 2),
            (3, 3),
            (5, 5),
            (6, 6),
            (8, 8),
            (9, 9),
            (11, 11),
        ]

        write_capture.require_write(
            "stable-row-id distributed Lance UPDATE",
            lambda: connection.table("lance_stable_mutation.main.stable_target").update(
                {"id": vane.col("id") + 100},
                condition=vane.col("id") < 6,
            ),
            expected_name="update",
            expected_rows=4,
            minimum_task_results=2,
            minimum_artifacts=1,
            allow_empty_tasks=True,
        )
        assert connection.execute(
            f"SELECT id, _rowid FROM {_sql_literal(target_path)} " "ORDER BY _rowid"
        ).fetchall() == [
            (100, 0),
            (102, 2),
            (103, 3),
            (105, 5),
            (6, 6),
            (8, 8),
            (9, 9),
            (11, 11),
        ]

        write_capture.require_write(
            "stable-row-id distributed Lance DELETE",
            lambda: connection.table("lance_stable_mutation.main.stable_target").delete(
                condition=(vane.col("id") >= 8) & (vane.col("id") < 100)
            ),
            expected_name="delete",
            expected_rows=3,
            minimum_task_results=2,
            minimum_artifacts=0,
            allow_empty_tasks=True,
        )
        assert connection.execute(
            f"SELECT id, _rowid FROM {_sql_literal(target_path)} " "ORDER BY _rowid"
        ).fetchall() == [
            (100, 0),
            (102, 2),
            (103, 3),
            (105, 5),
            (6, 6),
        ]
        assert _manifest_count(connection, target_path) == initial_manifest_count + 2
        assert _mutation_attempt_manifest_count(connection, target_path) == 0
    finally:
        connection.close()


def test_two_worker_local_shared_lance_insert_and_ctas(
    tmp_path: Path, write_capture: DistributedWriteCapture
) -> None:
    connection = _connect()
    root = tmp_path / "distributed-write"
    root.mkdir()
    source_path = root / "source.lance"
    failure_source_path = root / "failure_source.lance"
    insert_path = root / "insert_target.lance"
    ctas_path = root / "ctas_target.lance"
    empty_ctas_path = root / "empty_ctas_target.lance"
    failed_ctas_path = root / "failed_ctas_target.lance"
    explicit_ctas_path = root / "explicit_ctas_target.lance"
    stale_type_path = root / "stale_target.lance"
    required_target_path = root / "required_target.lance"
    nested_required_target_path = root / "nested_required_target.lance"
    upstream_container_target_path = root / "upstream_container_target.lance"
    try:
        _write_source(connection, source_path)
        _write_failure_source(connection, failure_source_path)
        _write_upstream_container_target(upstream_container_target_path)
        connection.execute(f"ATTACH {_sql_literal(root)} AS lance_write (TYPE LANCE)")
        _exercise_insert_and_ctas(
            connection,
            write_capture,
            catalog="lance_write",
            source_path=source_path,
            insert_path=insert_path,
            ctas_path=ctas_path,
            empty_ctas_path=empty_ctas_path,
        )
        _exercise_upstream_container_target(
            connection,
            write_capture,
            catalog="lance_write",
            target_path=upstream_container_target_path,
        )
        _exercise_failed_ctas_retention_and_explicit_retry(
            connection,
            write_capture,
            catalog="lance_write",
            target_path=failed_ctas_path,
        )
        _exercise_explicit_transaction_rejection(
            connection,
            write_capture,
            catalog="lance_write",
            insert_path=insert_path,
            ctas_path=explicit_ctas_path,
        )
        _exercise_stale_target_type_rejection(
            connection,
            write_capture,
            root=root,
            target_path=stale_type_path,
        )
        _exercise_not_null_target(
            connection,
            write_capture,
            catalog="lance_write",
            target_path=required_target_path,
        )
        _exercise_nested_not_null_target(
            connection,
            write_capture,
            catalog="lance_write",
            target_path=nested_required_target_path,
        )
    finally:
        connection.close()


@pytest.mark.skipif(
    os.environ.get("LANCE_TEST_S3") != "1",
    reason="requires the MinIO-backed Lance S3 test environment",
)
def test_two_worker_s3_lance_insert_and_ctas(
    write_capture: DistributedWriteCapture,
) -> None:
    connection = _connect()
    try:
        config = _configure_s3(connection)
        root = f"s3://{config['LANCE_S3_BUCKET']}/distributed-write/" f"{uuid.uuid4()}"
        source_path = f"{root}/source.lance"
        failure_source_path = f"{root}/failure_source.lance"
        insert_path = f"{root}/insert_target.lance"
        ctas_path = f"{root}/ctas_target.lance"
        empty_ctas_path = f"{root}/empty_ctas_target.lance"
        failed_ctas_path = f"{root}/failed_ctas_target.lance"
        explicit_ctas_path = f"{root}/explicit_ctas_target.lance"
        required_target_path = f"{root}/required_target.lance"
        nested_required_target_path = f"{root}/nested_required_target.lance"
        mutation_target_path = f"{root}/mutation_target.lance"
        _write_source(connection, source_path)
        _write_failure_source(connection, failure_source_path)
        _write_source(connection, mutation_target_path)
        connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_s3_write "
            "(TYPE LANCE, READ_ONLY false)"
        )
        _exercise_insert_and_ctas(
            connection,
            write_capture,
            catalog="lance_s3_write",
            source_path=source_path,
            insert_path=insert_path,
            ctas_path=ctas_path,
            empty_ctas_path=empty_ctas_path,
        )
        _exercise_failed_ctas_retention_and_explicit_retry(
            connection,
            write_capture,
            catalog="lance_s3_write",
            target_path=failed_ctas_path,
        )
        _exercise_explicit_transaction_rejection(
            connection,
            write_capture,
            catalog="lance_s3_write",
            insert_path=insert_path,
            ctas_path=explicit_ctas_path,
        )
        _exercise_not_null_target(
            connection,
            write_capture,
            catalog="lance_s3_write",
            target_path=required_target_path,
        )
        _exercise_nested_not_null_target(
            connection,
            write_capture,
            catalog="lance_s3_write",
            target_path=nested_required_target_path,
        )
        _exercise_update_and_delete(
            connection,
            write_capture,
            catalog="lance_s3_write",
            target_path=mutation_target_path,
        )
    finally:
        connection.close()
