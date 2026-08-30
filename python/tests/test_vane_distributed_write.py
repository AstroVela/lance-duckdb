# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import time
import uuid
import warnings
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import vane
from vane import runners
from vane.runners.ray import set_runner_ray

WORKER_COUNT = 2


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
    loaded, install_mode = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() "
        "WHERE lower(extension_name) = 'lance'"
    ).fetchone()
    assert loaded is True
    assert str(install_mode).upper() == "STATICALLY_LINKED"
    return connection


def _write_source(connection, path: str | Path) -> None:
    connection.execute(
        "COPY (SELECT i::BIGINT AS id, "
        "('value-' || i::VARCHAR)::VARCHAR AS value "
        "FROM range(80) AS source(i)) "
        f"TO {_sql_literal(path)} "
        "(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 10)"
    )


def _write_failure_source(connection, path: str | Path) -> None:
    # Keep the rows in one fragment that spans many DuckDB chunks so the
    # downstream writer receives data before the injected expression error.
    connection.execute(
        "COPY (SELECT i::BIGINT AS id, "
        "('value-' || i::VARCHAR)::VARCHAR AS value "
        "FROM range(32768) AS source(i)) "
        f"TO {_sql_literal(path)} "
        "(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 65536, "
        "MAX_ROWS_PER_GROUP 1024)"
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


def _manifest_count(connection, path: str | Path) -> int:
    pattern = f"{str(path).rstrip('/')}/_versions/*.manifest"
    return int(
        connection.execute(
            f"SELECT count(*)::BIGINT FROM glob({_sql_literal(pattern)})"
        ).fetchone()[0]
    )


def _data_file_count(connection, path: str | Path) -> int:
    pattern = f"{str(path).rstrip('/')}/data/*.lance"
    return int(
        connection.execute(
            f"SELECT count(*)::BIGINT FROM glob({_sql_literal(pattern)})"
        ).fetchone()[0]
    )


def _attempt_manifest_count(connection, path: str | Path) -> int:
    pattern = f"{str(path).rstrip('/')}/_vane_distributed_write_attempts/*/*.manifest"
    return int(
        connection.execute(
            f"SELECT count(*)::BIGINT FROM glob({_sql_literal(pattern)})"
        ).fetchone()[0]
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


def _execution_node_ids(ray) -> frozenset[str]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        node_ids = frozenset(
            str(node["NodeID"])
            for node in ray.nodes()
            if node.get("Alive")
            and float((node.get("Resources") or {}).get("CPU", 0)) >= 1
        )
        if len(node_ids) == WORKER_COUNT:
            return node_ids
        time.sleep(0.25)
    raise AssertionError(f"expected {WORKER_COUNT} live Ray execution nodes")


@pytest.fixture(scope="session")
def ray_cluster():
    import ray
    from ray.cluster_utils import Cluster

    if ray.is_initialized():
        ray.shutdown()
    environment = pytest.MonkeyPatch()
    cluster = None
    try:
        environment.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
        cluster = Cluster(shutdown_at_exit=False)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r"Tip: In future versions of Ray")
            cluster.add_node(
                include_dashboard=False,
                num_cpus=0,
                num_gpus=0,
                object_store_memory=128 * 1024 * 1024,
            )
            for _ in range(WORKER_COUNT):
                cluster.add_node(
                    include_dashboard=False,
                    num_cpus=1,
                    num_gpus=0,
                    object_store_memory=128 * 1024 * 1024,
                )
            ray.init(
                address=cluster.address,
                ignore_reinit_error=False,
                log_to_driver=True,
            )
        yield _execution_node_ids(ray)
    finally:
        try:
            vane.teardown_runner()
        finally:
            import ray

            ray.shutdown()
            if cluster is not None:
                cluster.shutdown()
            environment.undo()


class DistributedWriteCapture:
    def __init__(self, runner) -> None:
        self.runner = runner
        self.dispatch_count = 0
        self.last_result: dict[str, object] | None = None
        self.original_run_write = runner.run_write

        def record(*args: object, **kwargs: object) -> object:
            self.dispatch_count += 1
            result = self.original_run_write(*args, **kwargs)
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
            assert fragment_count == task_count
            assert artifact_count >= fragment_count
        return result


@pytest.fixture
def write_capture(
    ray_cluster: frozenset[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    assert len(ray_cluster) == WORKER_COUNT
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", str(WORKER_COUNT))
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", str(WORKER_COUNT))
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("VANE_SHUFFLE_LOCAL_DIRS", str(tmp_path / "shuffle"))
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    capture = DistributedWriteCapture(runners.get_or_create_runner())
    try:
        yield capture
    finally:
        capture.close()
        vane.teardown_runner()


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
    assert connection.execute(
        "SELECT data_type FROM duckdb_columns() "
        "WHERE database_name = 'lance_write' AND schema_name = 'main' "
        "AND table_name = 'stale_target' AND column_name = 'id'"
    ).fetchone() == ("INTEGER",)

    evolution_connection = _connect()
    try:
        evolution_connection.execute(
            f"ATTACH {_sql_literal(root)} AS lance_evolve (TYPE LANCE)"
        )
        evolution_connection.execute(
            "ALTER TABLE lance_evolve.main.stale_target " "ALTER COLUMN id TYPE BIGINT"
        )
        assert evolution_connection.execute(
            "SELECT data_type FROM duckdb_columns() "
            "WHERE database_name = 'lance_evolve' AND schema_name = 'main' "
            "AND table_name = 'stale_target' AND column_name = 'id'"
        ).fetchone() == ("BIGINT",)

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
    assert connection.execute(
        "SELECT is_nullable FROM duckdb_columns() "
        f"WHERE database_name = {_sql_literal(catalog)} "
        "AND schema_name = 'main' AND table_name = 'required_target' "
        "AND column_name = 'id'"
    ).fetchone() == (False,)

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
        _write_source(connection, source_path)
        _write_failure_source(connection, failure_source_path)
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
    finally:
        connection.close()
