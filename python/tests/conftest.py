# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

"""Owned Ray resources for installed-wheel integration tests."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ray_cluster():
    import ray
    import vane
    from ray.cluster_utils import Cluster

    if "VANE_RUNNER" in os.environ:
        raise RuntimeError("leave VANE_RUNNER unset to qualify the default Ray runner")
    if ray.is_initialized():
        raise RuntimeError("the integration suite must own its Ray cluster")
    environment = pytest.MonkeyPatch()
    environment.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    environment.setenv("RAY_task_events_report_interval_ms", "100")
    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=128 * 1024 * 1024,
        )
        for _ in range(2):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=128 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=False, log_to_driver=True)
        deadline = time.monotonic() + 30
        while True:
            nodes = frozenset(
                str(node["NodeID"])
                for node in ray.nodes()
                if node.get("Alive")
                and (node.get("Resources") or {}).get("CPU", 0) >= 1
            )
            if len(nodes) == 2:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("expected two Ray execution nodes")
            time.sleep(0.1)
        yield nodes
    finally:
        try:
            vane.teardown_runner()
        finally:
            try:
                ray.shutdown()
            finally:
                try:
                    cluster.shutdown()
                finally:
                    environment.undo()


@pytest.fixture
def default_ray_runtime(ray_cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import vane
    from vane import runners

    assert "VANE_RUNNER" not in os.environ
    assert len(ray_cluster) == 2
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "2")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "2")
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("VANE_SHUFFLE_LOCAL_DIRS", str(tmp_path / "shuffle"))
    vane.teardown_runner()
    runner = runners.get_or_create_runner()
    assert runner.name == "ray"
    try:
        yield runner
    finally:
        vane.teardown_runner()
