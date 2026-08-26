# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import shutil
import time
import uuid
import warnings
from pathlib import Path

import pytest

vane = pytest.importorskip("vane")
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
    return connection


def _write_dataset(
    connection,
    path: str | Path,
    *,
    rows: int = 12,
    max_rows_per_file: int = 3,
) -> None:
    connection.execute(
        "COPY (SELECT i::BIGINT AS id, "
        "('value-' || i::VARCHAR)::VARCHAR AS value "
        f"FROM range({rows}) AS source(i)) TO {_sql_literal(path)} "
        f"(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE {max_rows_per_file})"
    )


def _physical_plan(connection, relation):
    logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        relation, f"lance-distributed-scan-{uuid.uuid4()}"
    )
    return logical.to_physical_plan(connection)


def _split_batches(connection, relation) -> dict[str, list[bytes]]:
    return {
        str(node_id): [bytes(batch) for batch in batches]
        for node_id, batches in _physical_plan(connection, relation)
        .scan_split_batch_map()
        .items()
    }


def _split_count(connection, relation) -> int:
    return sum(
        len(batches) for batches in _split_batches(connection, relation).values()
    )


def _batch_for_split(physical, split_id: str) -> bytes:
    for batches in physical.scan_split_batch_map().values():
        for batch in batches:
            for candidate_id, singleton, _ in vane.ray_cxx.split_scan_split_batch(
                batch
            ):
                if candidate_id == split_id:
                    return bytes(singleton)
    raise AssertionError(f"missing scan split {split_id}")


def _run(runner, relation) -> list[tuple[object, ...]]:
    return [
        tuple(row.values())
        for table in runner.run_iter_tables(relation)
        for row in table.to_pylist()
    ]


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


def _assert_vane_worker_topology(ray, runner) -> None:
    client = runner.query_driver_client
    if client is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    stats = ray.get(client.runner.fragment_stats.remote())
    workers = stats.get("workers") if isinstance(stats, dict) else None
    if not isinstance(workers, dict):
        raise AssertionError(
            f"Vane fragment statistics do not expose workers: {stats!r}"
        )
    assert len(workers) == WORKER_COUNT


def _ray_fte_create_task_locations() -> dict[str, str]:
    import ray
    from ray._private import ray_constants
    from ray._private.grpc_utils import init_grpc_channel
    from ray.core.generated import gcs_service_pb2_grpc
    from ray.core.generated.gcs_service_pb2 import GetTaskEventsRequest

    channel = init_grpc_channel(
        ray.get_runtime_context().gcs_address,
        ray_constants.GLOBAL_GRPC_OPTIONS,
        asynchronous=False,
    )
    try:
        reply = gcs_service_pb2_grpc.TaskInfoGcsServiceStub(channel).GetTaskEvents(
            GetTaskEventsRequest(limit=10000), timeout=10
        )
    finally:
        channel.close()
    if int(reply.status.code) != 0:
        raise RuntimeError(f"Ray GCS task event query failed: {reply.status.message}")

    locations: dict[str, str] = {}
    for event in reply.events_by_task:
        if not event.task_info.name.endswith(".fte_create_task"):
            continue
        node_id = event.state_updates.node_id or event.task_info.node_id
        if node_id:
            locations[event.task_id.hex()] = node_id.hex()
    return locations


def _settled_ray_fte_create_task_locations() -> dict[str, str]:
    deadline = time.monotonic() + 5
    previous: dict[str, str] | None = None
    stable_observations = 0
    while time.monotonic() < deadline:
        current = _ray_fte_create_task_locations()
        if current == previous:
            stable_observations += 1
            if stable_observations >= 3:
                return current
        else:
            previous = current
            stable_observations = 0
        time.sleep(0.1)
    return previous or {}


def _ray_fte_create_task_node_ids(
    baseline_task_ids: set[str], expected_count: int
) -> set[str]:
    deadline = time.monotonic() + 15
    observed_node_ids: set[str] = set()
    while time.monotonic() < deadline:
        locations = _ray_fte_create_task_locations()
        observed_node_ids = {
            node_id
            for task_id, node_id in locations.items()
            if task_id not in baseline_task_ids
        }
        if len(observed_node_ids) >= expected_count:
            return observed_node_ids
        time.sleep(0.1)
    return observed_node_ids


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
        environment.setenv("RAY_task_events_report_interval_ms", "100")
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
                    num_cpus=1,
                    num_gpus=0,
                    object_store_memory=128 * 1024 * 1024,
                )
            ray.init(
                address=cluster.address,
                ignore_reinit_error=True,
                log_to_driver=True,
            )
        yield _execution_node_ids(ray)
    finally:
        try:
            vane.teardown_runner()
        finally:
            ray.shutdown()
            if cluster is not None:
                cluster.shutdown()
            environment.undo()


@pytest.fixture
def ray_runner(ray_cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    assert len(ray_cluster) == WORKER_COUNT
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "2")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "2")
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("VANE_SHUFFLE_LOCAL_DIRS", str(tmp_path / "shuffle"))
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    try:
        yield runners.get_or_create_runner()
    finally:
        vane.teardown_runner()


def test_fragment_scan_preserves_filter_projection_aggregate_and_global_limit(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "operators.lance"
    try:
        _write_dataset(connection, path)

        aggregate = connection.sql(
            "SELECT id % 3 AS bucket, count(*) AS n, sum(id) AS total "
            f"FROM {_sql_literal(path)} WHERE id >= 2 AND id < 11 "
            "GROUP BY bucket ORDER BY bucket"
        )
        assert _split_count(connection, aggregate) == 4
        assert _run(ray_runner, aggregate) == [
            (0, 3, 18),
            (1, 3, 21),
            (2, 3, 15),
        ]

        count_only = connection.sql(f"SELECT count(*) AS n FROM {_sql_literal(path)}")
        # DuckDB can answer a bare count from the bound table cardinality, so
        # Vane correctly has no scan source to split in this plan.
        assert _split_count(connection, count_only) == 0
        assert _run(ray_runner, count_only) == [(12,)]

        constant_projection = connection.sql(
            f"SELECT 1 AS one FROM {_sql_literal(path)}"
        )
        assert _split_count(connection, constant_projection) == 4
        assert _run(ray_runner, constant_projection) == [(1,)] * 12

        limited = connection.sql(
            f"SELECT id, value FROM {_sql_literal(path)} "
            "ORDER BY id LIMIT 5 OFFSET 4"
        )
        assert _split_count(connection, limited) == 4
        assert _run(ray_runner, limited) == [
            (row_id, f"value-{row_id}") for row_id in range(4, 9)
        ]
    finally:
        connection.close()


def test_fragment_scan_uses_both_ray_execution_nodes(
    tmp_path: Path, ray_cluster: frozenset[str], ray_runner
) -> None:
    import ray

    connection = _connect()
    path = tmp_path / "worker-topology.lance"
    try:
        _write_dataset(
            connection,
            path,
            rows=65536,
            max_rows_per_file=4096,
        )
        relation = connection.sql(
            f"SELECT count(*)::BIGINT AS rows, sum(id)::HUGEINT AS total "
            f"FROM {_sql_literal(path)}"
        )
        assert _split_count(connection, relation) == 16

        baseline_task_ids = set(_settled_ray_fte_create_task_locations())
        assert _run(ray_runner, relation) == [(65536, 65536 * 65535 // 2)]
        execution_node_ids = _ray_fte_create_task_node_ids(
            baseline_task_ids, expected_count=WORKER_COUNT
        )
        assert execution_node_ids == set(ray_cluster)
        _assert_vane_worker_topology(ray, ray_runner)
    finally:
        connection.close()


def test_multiple_lance_scan_nodes_keep_independent_splits(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "multiple-scans.lance"
    dataset = _sql_literal(path)
    sql = (
        "WITH ranked AS ("
        "  SELECT id, row_number() OVER (ORDER BY id) AS row_number "
        f"  FROM {dataset} WHERE id >= 2 AND id < 11"
        ") "
        "SELECT count(*)::BIGINT, sum(left_rows.id)::BIGINT, "
        "       max(left_rows.row_number)::BIGINT "
        "FROM ranked AS left_rows "
        f"JOIN {dataset} AS right_rows ON right_rows.id = left_rows.id - 1 "
        "WHERE right_rows.value IS NOT NULL"
    )
    try:
        _write_dataset(connection, path)
        relation = connection.sql(sql)
        split_counts = {
            scan_id: len(batches)
            for scan_id, batches in _split_batches(connection, relation).items()
        }
        assert len(split_counts) == 2
        assert all(count == 4 for count in split_counts.values())
        assert _run(ray_runner, relation) == connection.execute(sql).fetchall()
    finally:
        connection.close()


def test_fragment_scan_replays_relative_paths_on_workers(
    tmp_path: Path, ray_runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _connect()
    monkeypatch.chdir(tmp_path)
    path = Path("relative.lance")
    try:
        _write_dataset(connection, path)
        relation = connection.sql(f"SELECT id FROM {_sql_literal(path)} ORDER BY id")
        assert _split_count(connection, relation) == 4
        assert _run(ray_runner, relation) == [(row_id,) for row_id in range(12)]
    finally:
        connection.close()


def test_take_scan_is_split_and_preserves_duplicates(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "take.lance"
    try:
        _write_dataset(connection, path)
        row_ids_by_id = dict(
            connection.execute(
                f"SELECT id, _rowid FROM {_sql_literal(path)} ORDER BY id"
            ).fetchall()
        )
        selected_ids = (0, 1, 4, 4, 7, 10)
        selected_row_ids = ", ".join(
            str(row_ids_by_id[row_id]) for row_id in selected_ids
        )
        for row_id_column in ("_rowid", "rowid"):
            relation = connection.sql(
                f"SELECT id FROM {_sql_literal(path)} "
                f"WHERE {row_id_column} IN ({selected_row_ids}) ORDER BY id"
            )
            assert _split_count(connection, relation) > 1
            assert _run(ray_runner, relation) == [(row_id,) for row_id in selected_ids]

        count_only = connection.sql(
            f"SELECT count(*) AS n FROM {_sql_literal(path)} "
            f"WHERE _rowid IN ({selected_row_ids})"
        )
        assert _split_count(connection, count_only) > 1
        assert _run(ray_runner, count_only) == [(6,)]
    finally:
        connection.close()


def test_sampling_is_global_and_repeatable(tmp_path: Path, ray_runner) -> None:
    connection = _connect()
    path = tmp_path / "sample.lance"
    try:
        _write_dataset(connection, path)
        sql = (
            f"SELECT id FROM {_sql_literal(path)} "
            "USING SAMPLE reservoir(5 ROWS) REPEATABLE (42)"
        )
        assert _split_count(connection, connection.sql(sql)) == 4
        first = sorted(int(row[0]) for row in _run(ray_runner, connection.sql(sql)))
        second = sorted(int(row[0]) for row in _run(ray_runner, connection.sql(sql)))
        assert len(first) == len(set(first)) == 5
        assert first == second
    finally:
        connection.close()


def test_empty_dataset_has_no_distributed_work(tmp_path: Path, ray_runner) -> None:
    connection = _connect()
    path = tmp_path / "empty.lance"
    try:
        _write_dataset(connection, path, rows=0)
        relation = connection.sql(f"SELECT * FROM {_sql_literal(path)}")
        # Vane exposes one explicit empty-source envelope, but schedules no
        # Lance fragment or take split from it.
        assert _split_count(connection, relation) == 1
        assert _run(ray_runner, relation) == []
    finally:
        connection.close()


def test_empty_assignment_clears_restricted_worker_state(tmp_path: Path) -> None:
    connection = _connect()
    nonempty_path = tmp_path / "assigned.lance"
    empty_path = tmp_path / "empty-assignment.lance"
    worker = None
    empty_worker = None
    cursor = None
    empty_cursor = None
    worker_plan = None
    empty_worker_plan = None
    result = None
    empty_result = None
    try:
        _write_dataset(connection, nonempty_path)
        _write_dataset(connection, empty_path, rows=0)
        physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(nonempty_path)}")
        )
        split_map = physical.scan_split_batch_map()
        node_id = str(next(iter(split_map)))
        split_batch = bytes(next(iter(split_map.values()))[0])

        empty_physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(empty_path)}")
        )
        empty_split_map = empty_physical.scan_split_batch_map()
        empty_batch = bytes(next(iter(empty_split_map.values()))[0])

        worker = _connect()
        cursor = worker.cursor()
        worker_plan = physical.clone(worker)
        result = vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
            cursor,
            worker_plan,
            scan_split_batch={node_id: split_batch},
        )
        assert result.completion_status == "ok"
        assert sum(table.num_rows for table in result.partition_payloads) == 3

        empty_worker = _connect()
        empty_cursor = empty_worker.cursor()
        empty_worker_plan = worker_plan.clone(empty_worker)
        empty_result = vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
            empty_cursor,
            empty_worker_plan,
            scan_split_batch={node_id: empty_batch},
        )
        assert empty_result.completion_status == "empty"
        assert sum(table.num_rows for table in empty_result.partition_payloads) == 0
    finally:
        empty_result = None
        result = None
        empty_worker_plan = None
        worker_plan = None
        if empty_cursor is not None:
            empty_cursor.close()
        if cursor is not None:
            cursor.close()
        if empty_worker is not None:
            empty_worker.close()
        if worker is not None:
            worker.close()
        connection.close()


def test_directory_namespace_scan_is_distributed(tmp_path: Path, ray_runner) -> None:
    connection = _connect()
    root = tmp_path / "namespace"
    root.mkdir()
    path = root / "items.lance"
    try:
        _write_dataset(connection, path)
        connection.execute(f"ATTACH {_sql_literal(root)} AS lance_ns (TYPE LANCE)")
        relation = connection.sql("SELECT id FROM lance_ns.main.items ORDER BY id")
        assert _split_count(connection, relation) == 4
        assert _run(ray_runner, relation) == [(row_id,) for row_id in range(12)]
    finally:
        connection.close()


def test_s3_fragment_scan_replays_vane_session_credentials(ray_runner) -> None:
    if os.environ.get("LANCE_TEST_S3") != "1":
        pytest.skip("set LANCE_TEST_S3=1 to run the distributed MinIO test")

    bucket = os.environ.get("LANCE_S3_BUCKET")
    if not bucket:
        raise AssertionError("LANCE_S3_BUCKET is required when LANCE_TEST_S3=1")
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ALLOW_HTTP",
        "AWS_ENDPOINT_URL",
        "AWS_REGION",
        "AWS_SECRET_ACCESS_KEY",
    ):
        if not os.environ.get(name):
            raise AssertionError(f"{name} is required when LANCE_TEST_S3=1")

    connection = _connect()
    namespace_root = f"s3://{bucket}/distributed-scan/{uuid.uuid4()}"
    path = f"{namespace_root}/items.lance"
    try:
        _write_dataset(connection, path)
        connection.execute(
            f"ATTACH {_sql_literal(namespace_root)} AS lance_s3_ns (TYPE LANCE)"
        )
        relation = connection.sql(f"SELECT id FROM {_sql_literal(path)} ORDER BY id")
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, f"lance-s3-session-replay-{uuid.uuid4()}"
        )
        assert not logical.has_explicit_s3_credentials()
        session_config = logical.session_config()
        assert session_config["AWS_ENDPOINT_URL"] == os.environ["AWS_ENDPOINT_URL"]
        assert session_config["AWS_ACCESS_KEY_ID"] == os.environ["AWS_ACCESS_KEY_ID"]
        assert _split_count(connection, relation) == 4
        assert _run(ray_runner, relation) == [(row_id,) for row_id in range(12)]

        namespace_relation = connection.sql(
            "SELECT id FROM lance_s3_ns.main.items ORDER BY id"
        )
        assert _split_count(connection, namespace_relation) == 4
        assert _run(ray_runner, namespace_relation) == [
            (row_id,) for row_id in range(12)
        ]
    finally:
        connection.close()


def test_worker_reopens_the_coordinator_snapshot_after_append(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "fixed-snapshot.lance"
    try:
        _write_dataset(connection, path)
        physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        )
        split_map = {
            str(node_id): [bytes(batch) for batch in batches]
            for node_id, batches in physical.scan_split_batch_map().items()
        }
        assert sum(len(batches) for batches in split_map.values()) == 4

        connection.execute(
            "COPY (SELECT 99::BIGINT AS id, 'value-99'::VARCHAR AS value) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'append')"
        )

        rows: list[int] = []
        for node_id, batches in split_map.items():
            for batch in batches:
                worker = _connect()
                cursor = worker.cursor()
                result = None
                worker_plan = None
                try:
                    worker_plan = physical.clone(worker)
                    result = (
                        vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                            cursor,
                            worker_plan,
                            scan_split_batch={node_id: batch},
                        )
                    )
                    assert result.completion_status == "ok"
                    rows.extend(
                        int(value)
                        for table in result.partition_payloads
                        for value in table.column(0).to_pylist()
                    )
                finally:
                    result = None
                    worker_plan = None
                    cursor.close()
                    worker.close()
        assert sorted(rows) == list(range(12))
    finally:
        connection.close()


def test_worker_rejects_a_same_version_dataset_replacement(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "replaced.lance"
    try:
        _write_dataset(connection, path)
        physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        )
        assert physical.scan_split_batch_map()

        shutil.rmtree(path)
        replacement = _connect()
        try:
            _write_dataset(replacement, path, rows=1)
        finally:
            replacement.close()

        worker = _connect()
        try:
            with pytest.raises(Exception, match="generation does not match"):
                physical.clone(worker)
        finally:
            worker.close()
    finally:
        connection.close()


def test_worker_rejects_foreign_split_assignments(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "split-mode.lance"
    try:
        _write_dataset(connection, path)
        row_ids = connection.execute(
            f"SELECT _rowid FROM {_sql_literal(path)} ORDER BY id LIMIT 8"
        ).fetchall()
        row_id_list = ", ".join(str(row_id[0]) for row_id in row_ids[:4])
        foreign_row_id_list = ", ".join(str(row_id[0]) for row_id in row_ids[4:])
        fragment_physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        )
        take_physical = _physical_plan(
            connection,
            connection.sql(
                f"SELECT id FROM {_sql_literal(path)} "
                f"WHERE _rowid IN ({row_id_list})"
            ),
        )
        foreign_take_physical = _physical_plan(
            connection,
            connection.sql(
                f"SELECT id FROM {_sql_literal(path)} "
                f"WHERE _rowid IN ({foreign_row_id_list})"
            ),
        )
        fragment_splits = fragment_physical.scan_split_batch_map()
        take_splits = take_physical.scan_split_batch_map()
        foreign_take_splits = foreign_take_physical.scan_split_batch_map()
        fragment_node = next(iter(fragment_splits))
        take_node = next(iter(take_splits))
        take_batch = bytes(next(iter(take_splits.values()))[0])
        foreign_take_batch = bytes(next(iter(foreign_take_splits.values()))[0])

        assignments = (
            (fragment_physical, fragment_node, take_batch),
            (take_physical, take_node, foreign_take_batch),
        )
        for physical, node_id, split_batch in assignments:
            worker = _connect()
            cursor = worker.cursor()
            worker_plan = None
            try:
                worker_plan = physical.clone(worker)
                with pytest.raises(Exception, match="Invalid Lance take scan split"):
                    vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                        cursor,
                        worker_plan,
                        scan_split_batch={str(node_id): split_batch},
                    )
            finally:
                worker_plan = None
                cursor.close()
                worker.close()
    finally:
        connection.close()


def test_worker_rejects_overlapping_take_split_ranges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _connect()
    path = tmp_path / "overlapping-take.lance"
    worker = None
    cursor = None
    worker_plan = None
    try:
        _write_dataset(connection, path)
        row_ids = [
            int(row[0])
            for row in connection.execute(
                f"SELECT _rowid FROM {_sql_literal(path)} ORDER BY id LIMIT 8"
            ).fetchall()
        ]
        full_list = ", ".join(str(row_id) for row_id in row_ids)
        prefix_list = ", ".join(str(row_id) for row_id in row_ids[:4])

        monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
        physical = _physical_plan(
            connection,
            connection.sql(
                f"SELECT id FROM {_sql_literal(path)} " f"WHERE _rowid IN ({full_list})"
            ),
        )
        node_id = str(next(iter(physical.scan_split_batch_map())))
        first_batch = _batch_for_split(physical, "take:0")

        monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "8")
        overlapping_physical = _physical_plan(
            connection,
            connection.sql(
                f"SELECT id FROM {_sql_literal(path)} "
                f"WHERE _rowid IN ({prefix_list})"
            ),
        )
        overlapping_batch = _batch_for_split(overlapping_physical, "take:1")
        merged_batch = bytes(
            vane.ray_cxx.merge_scan_split_batches([first_batch, overlapping_batch])
        )

        worker = _connect()
        cursor = worker.cursor()
        worker_plan = physical.clone(worker)
        with pytest.raises(Exception, match="Overlapping Lance take scan splits"):
            vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                cursor,
                worker_plan,
                scan_split_batch={node_id: merged_batch},
            )
    finally:
        worker_plan = None
        if cursor is not None:
            cursor.close()
        if worker is not None:
            worker.close()
        connection.close()
