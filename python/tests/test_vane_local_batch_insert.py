# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

"""Signed dynamic native writes from real local task and actor results."""

from __future__ import annotations

import json
import os

import pytest

from packaged_dynamic_extension import load_packaged_dynamic_lance


@pytest.mark.parametrize("backend", ["subprocess_task", "subprocess_actor"])
@pytest.mark.parametrize("runner", ["local", "local-fast"])
@pytest.mark.parametrize("mode", ["insert", "ctas"])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("source_kind", ["range", "lance_scan"])
def test_local_lazy_results_write_and_reopen_without_ray(
    tmp_path, monkeypatch, backend, runner, mode, empty, source_kind
):
    if not os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"):
        pytest.skip("requires the installed signed dynamic provider qualification")

    import ray
    import vane

    assert not ray.is_initialized(), "run separately from the Ray integration suite"

    def forbid_ray(*args, **kwargs):
        raise AssertionError("local Lance writes must not initialize Ray")

    monkeypatch.setattr(ray, "init", forbid_ray)
    monkeypatch.setenv("VANE_RUNNER", runner)
    assert vane.get_or_infer_runner_type() == "local"
    trace = str(tmp_path / "actor.jsonl")

    def identity(batch):
        import pyarrow as pa

        return pa.table({name: batch[name] for name in batch.schema.names})

    class IdentityActor:
        def __init__(self):
            with open(trace, "a") as log:
                log.write(json.dumps({"event": "open"}) + "\n")

        def __call__(self, batch):
            with open(trace, "a") as log:
                log.write(json.dumps({"event": "batch", "rows": batch.num_rows}) + "\n")
            return identity(batch)

        def _vane_close(self):
            with open(trace, "a") as log:
                log.write(json.dumps({"event": "close"}) + "\n")

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    query = (
        "SELECT i::BIGINT AS id, "
        "CASE WHEN i=0 THEN NULL WHEN i=1 THEN [] WHEN i%3=0 THEN [i] "
        "ELSE [i, i+1] END::DOUBLE[] AS metadata, "
        "[i, i+1, i+2]::FLOAT[3] AS embedding, "
        "{'label': 'row-' || i::VARCHAR, 'optional': CASE WHEN i%2=0 THEN NULL ELSE i END} AS detail "
        f"FROM range({0 if empty else 12}) AS input(i)"
    )

    def connect():
        connection = vane.connect(
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            }
        )
        load_packaged_dynamic_lance(connection)
        path = "'" + str(warehouse).replace("'", "''") + "'"
        connection.execute(f"ATTACH {path} AS target (TYPE lance)")
        return connection

    try:
        with connect() as connection:
            # CTAS infers floating lists as vectors. Test its supported fixed
            # vector/struct schema; INSERT additionally preserves variable lists.
            columns = (
                "id, embedding, detail"
                if mode == "ctas"
                else "id, metadata, embedding, detail"
            )
            expected = connection.sql(query).project(columns).fetchall()
            if source_kind == "lance_scan":
                connection.execute(
                    "CREATE TABLE target.main.source (id BIGINT, metadata DOUBLE[], embedding FLOAT[3], "
                    "detail STRUCT(label VARCHAR, optional BIGINT))"
                )
                connection.sql(query).insert_into("target.main.source")
                source = connection.table("target.main.source")
            else:
                source = connection.sql(query)
            source = source.project(columns)
            schema = dict(zip(source.columns, source.types))
            options = dict(
                schema=schema, batch_size=2, cpus=1, gpus=0, execution_backend=backend
            )
            if backend == "subprocess_actor":
                options["actor_number"] = 1
            relation = source.map_batches(
                IdentityActor if backend == "subprocess_actor" else identity, **options
            )
            # Column-only projection keeps the lazy references at the native boundary.
            relation = relation.project(columns)
            if mode == "insert":
                connection.execute(
                    "CREATE TABLE target.main.items (id BIGINT, metadata DOUBLE[], embedding FLOAT[3], "
                    "detail STRUCT(label VARCHAR, optional BIGINT))"
                )
                relation.insert_into("target.main.items")
            else:
                relation.create("target.main.items")
            assert (
                connection.sql("SELECT * FROM target.main.items ORDER BY id").fetchall()
                == expected
            )
        with connect() as reopened:
            assert (
                reopened.sql("SELECT * FROM target.main.items ORDER BY id").fetchall()
                == expected
            )
    finally:
        vane.teardown_runner()
    assert not ray.is_initialized()
    if backend == "subprocess_actor" and not empty:
        events = [
            json.loads(line)["event"]
            for line in (tmp_path / "actor.jsonl").read_text().splitlines()
        ]
        assert events.count("open") == events.count("close") == 1
        assert events.count("batch") >= 2
