# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warnings

import pytest


@pytest.fixture(autouse=True)
def isolated_vane_runner(monkeypatch: pytest.MonkeyPatch):
    """Keep plugin tests independent from process-global Vane runner state."""
    try:
        import vane
    except ModuleNotFoundError as exc:
        if exc.name != "vane":
            raise
        yield
        return

    vane.teardown_runner()
    monkeypatch.setenv("VANE_RUNNER", "local-fast")
    try:
        yield
    finally:
        vane.teardown_runner()


@pytest.fixture(scope="session")
def ray_local():
    """Own the real two-node Ray cluster used by static-wheel tests."""
    import vane

    try:
        import ray
    except ModuleNotFoundError as exc:
        if exc.name != "ray":
            raise
        pytest.skip("ray is not installed")

    from ray.cluster_utils import Cluster

    if ray.is_initialized():
        ray.shutdown()
    cluster = Cluster(shutdown_at_exit=False)
    node_options = {
        "num_cpus": 2,
        "object_store_memory": 128 * 1024 * 1024,
    }
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=r"Tip: In future versions of Ray")
            cluster.add_node(include_dashboard=False, **node_options)
            cluster.add_node(**node_options)
            ray.init(
                address=cluster.address,
                ignore_reinit_error=True,
                logging_level="info",
                log_to_driver=True,
            )
        active_nodes = [node for node in ray.nodes() if node.get("Alive")]
        if len(active_nodes) < 2:
            raise RuntimeError(
                f"static-wheel validation requires two live Ray nodes, got {len(active_nodes)}"
            )
        yield
    finally:
        try:
            vane.teardown_runner()
        finally:
            try:
                ray.shutdown()
            finally:
                cluster.shutdown()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "real_ray: starts and uses a real Ray cluster")
    config.addinivalue_line(
        "markers", "ray_cluster_owner: owns the lifecycle of a Ray cluster"
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if {"ray_runner", "ray_write_runner"}.intersection(item.fixturenames):
            item.add_marker(pytest.mark.real_ray)
            item.add_marker(pytest.mark.ray_cluster_owner)
