#!/usr/bin/env python3
"""Benchmark Vane coordinator-frozen Lance snapshot transport."""

from __future__ import annotations

import argparse
import contextlib
import http.client
import http.server
import json
import os
import pickle
import statistics
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlsplit

import ray
import vane
from ray.cluster_utils import Cluster
from vane import runners
from vane.runners.ray import set_runner_ray


MIB = 1024 * 1024
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


@dataclass(frozen=True)
class RequestCounts:
    manifest_head: int = 0
    manifest_get: int = 0
    all_head: int = 0
    all_get: int = 0


class CountingProxy:
    """A small read-only HTTP proxy used to count S3 manifest requests."""

    def __init__(self, upstream: str) -> None:
        parsed = urlsplit(upstream)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("--upstream-endpoint must be an HTTP(S) URL")
        self._scheme = parsed.scheme
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._base_path = parsed.path.rstrip("/")
        self._lock = threading.Lock()
        self._requests: list[tuple[str, str, int]] = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802
                self._forward()

            def do_HEAD(self) -> None:  # noqa: N802
                self._forward()

            def _forward(self) -> None:
                connection_type = (
                    http.client.HTTPSConnection
                    if owner._scheme == "https"
                    else http.client.HTTPConnection
                )
                connection = connection_type(owner._host, owner._port, timeout=120)
                headers = {
                    name: value
                    for name, value in self.headers.items()
                    if name.lower() not in HOP_BY_HOP_HEADERS
                }
                target = owner._base_path + self.path
                try:
                    connection.request(self.command, target, headers=headers)
                    response = connection.getresponse()
                    with owner._lock:
                        owner._requests.append(
                            (self.command.upper(), self.path, response.status)
                        )
                    self.send_response(response.status, response.reason)
                    for name, value in response.getheaders():
                        if name.lower() not in HOP_BY_HOP_HEADERS:
                            self.send_header(name, value)
                    self.send_header("Connection", "close")
                    self.end_headers()
                    if self.command != "HEAD":
                        while chunk := response.read(MIB):
                            self.wfile.write(chunk)
                finally:
                    connection.close()
                    self.close_connection = True

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeError("counting proxy did not stop")

    def reset(self) -> None:
        with self._lock:
            self._requests.clear()

    def counts(self) -> RequestCounts:
        with self._lock:
            requests = tuple(self._requests)
        manifest = [
            (method, path, status)
            for method, path, status in requests
            if "/_versions/" in path and ".manifest" in path
        ]
        return RequestCounts(
            manifest_head=sum(method == "HEAD" for method, _, _ in manifest),
            manifest_get=sum(method == "GET" for method, _, _ in manifest),
            all_head=sum(method == "HEAD" for method, _, _ in requests),
            all_get=sum(method == "GET" for method, _, _ in requests),
        )


@contextlib.contextmanager
def optional_counting_proxy(upstream: str | None) -> Iterator[CountingProxy | None]:
    if not upstream:
        yield None
        return
    proxy = CountingProxy(upstream)
    proxy.start()
    try:
        yield proxy
    finally:
        proxy.close()


@dataclass(frozen=True)
class Sample:
    workers: int
    cache_state: str
    workload: str
    repeat: int
    planning_ms: float
    execution_ms: float
    logical_plan_bytes: int
    physical_plan_bytes: int
    split_count: int
    split_payload_bytes: int
    result_rows: int
    planning_requests: RequestCounts
    execution_requests: RequestCounts


def connect(args: argparse.Namespace, endpoint: str | None):
    connection = vane.connect(
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        }
    )
    extension_path = os.environ.get("LANCE_TEST_EXTENSION_PATH")
    connection.execute(
        f"LOAD {sql_literal(extension_path)}" if extension_path else "LOAD lance"
    )
    if args.dataset_uri.lower().startswith(("s3://", "s3a://", "s3n://")):
        if not endpoint or not args.access_key or not args.secret_key:
            raise ValueError("S3 benchmarks require endpoint and static credentials")
        connection.execute(f"SET s3_access_key_id = {sql_literal(args.access_key)}")
        connection.execute(f"SET s3_secret_access_key = {sql_literal(args.secret_key)}")
        connection.execute(f"SET s3_session_token = {sql_literal(args.session_token)}")
        connection.execute(f"SET s3_region = {sql_literal(args.region)}")
        connection.execute(f"SET s3_endpoint = {sql_literal(endpoint)}")
        connection.execute("SET s3_url_style = 'path'")
        connection.execute(
            f"SET s3_use_ssl = {'true' if endpoint.startswith('https://') else 'false'}"
        )
    return connection


def workloads(args: argparse.Namespace) -> dict[str, str]:
    dataset = sql_literal(args.dataset_uri)
    identifier = sql_identifier(args.id_column)
    vector = ", ".join(str(value) for value in args.vector)
    available = {
        # Keep a real fragment scan in the physical plan so split count and
        # split payload size remain observable. A metadata-only count plan can
        # legitimately have no scan split map.
        "scan": f"SELECT {identifier} FROM {dataset}",
        "vector": (
            f"SELECT {identifier}, _distance FROM lance_vector_search("
            f"{dataset}, {sql_literal(args.vector_column)}, "
            f"[{vector}]::FLOAT[{len(args.vector)}], k = {args.k}, use_index = true)"
        ),
        "fts": (
            f"SELECT {identifier}, _score FROM lance_fts("
            f"{dataset}, {sql_literal(args.text_column)}, "
            f"{sql_literal(args.text_query)}, k = {args.k})"
        ),
        "hybrid": (
            f"SELECT {identifier}, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{dataset}, {sql_literal(args.vector_column)}, "
            f"[{vector}]::FLOAT[{len(args.vector)}], "
            f"{sql_literal(args.text_column)}, {sql_literal(args.text_query)}, "
            f"k = {args.k}, use_index = true)"
        ),
    }
    return {name: available[name] for name in args.workloads}


def build_plan(connection, sql: str, query_id: str):
    started = time.perf_counter_ns()
    relation = connection.sql(sql)
    execution_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        relation, query_id
    )
    serialized_logical = pickle.dumps(execution_logical, protocol=5)
    metrics_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        relation, f"{query_id}-metrics"
    )
    physical = metrics_logical.to_physical_plan(connection)
    split_map = physical.scan_split_batch_map()
    split_count = sum(len(batches) for batches in split_map.values())
    split_payload_bytes = sum(
        len(bytes(batch)) for batches in split_map.values() for batch in batches
    )
    logical_plan_bytes = len(serialized_logical)
    physical_plan_bytes = len(pickle.dumps(physical, protocol=5))
    planning_ms = (time.perf_counter_ns() - started) / 1_000_000
    return (
        serialized_logical,
        planning_ms,
        logical_plan_bytes,
        physical_plan_bytes,
        split_count,
        split_payload_bytes,
    )


def execute_logical(runner, serialized_logical: bytes) -> tuple[float, int]:
    # Planning materializes the source logical object. Submit an independent
    # round-trip, matching the object Ray transports to the query driver.
    logical = pickle.loads(serialized_logical)
    client = runner._client_for_session(str(logical.session_id()))
    started = time.perf_counter_ns()
    rows = 0
    for partition in client.stream_plan(logical):
        rows += partition.partition().num_rows
    execution_ms = (time.perf_counter_ns() - started) / 1_000_000
    return execution_ms, rows


def run_sample(
    args: argparse.Namespace,
    connection,
    runner,
    proxy: CountingProxy | None,
    workers: int,
    cache_state: str,
    workload: str,
    sql: str,
    repeat: int,
) -> Sample:
    print(
        f"workers={workers} cache={cache_state} workload={workload} repeat={repeat}",
        flush=True,
    )
    if proxy:
        proxy.reset()
    plan = build_plan(
        connection,
        sql,
        f"lance-frozen-snapshot-{workers}-{cache_state}-{workload}-{uuid.uuid4()}",
    )
    if workload == "scan" and plan[4] < workers:
        raise RuntimeError(
            f"scan planned only {plan[4]} splits for {workers} workers; "
            "prepare a dataset with more fragments"
        )
    if workload != "scan" and plan[4] != 1:
        raise RuntimeError(
            f"global {workload} search planned {plan[4]} splits instead of one"
        )
    planning_requests = proxy.counts() if proxy else RequestCounts()
    if proxy:
        proxy.reset()
    execution_ms, result_rows = execute_logical(runner, plan[0])
    execution_requests = proxy.counts() if proxy else RequestCounts()
    return Sample(
        workers=workers,
        cache_state=cache_state,
        workload=workload,
        repeat=repeat,
        planning_ms=plan[1],
        execution_ms=execution_ms,
        logical_plan_bytes=plan[2],
        physical_plan_bytes=plan[3],
        split_count=plan[4],
        split_payload_bytes=plan[5],
        result_rows=result_rows,
        planning_requests=planning_requests,
        execution_requests=execution_requests,
    )


def start_cluster(workers: int, object_store_memory: int) -> Cluster:
    if ray.is_initialized():
        ray.shutdown()
    cluster = Cluster(shutdown_at_exit=False)
    cluster.add_node(
        include_dashboard=False,
        num_cpus=0,
        num_gpus=0,
        object_store_memory=object_store_memory,
    )
    for _ in range(workers):
        cluster.add_node(
            include_dashboard=False,
            num_cpus=1,
            num_gpus=0,
            object_store_memory=object_store_memory,
        )
    ray.init(address=cluster.address, ignore_reinit_error=True, log_to_driver=False)
    return cluster


def configure_runner_environment(args: argparse.Namespace, workers: int) -> None:
    os.environ["VANE_DISTRIBUTED_NODE_COUNT"] = str(workers)
    os.environ["VANE_DISTRIBUTED_WORKER_SLOTS"] = str(args.worker_slots)
    os.environ["VANE_RAY_SCAN_SPLIT_MIN_COUNT"] = str(max(workers, 1))
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"
    os.environ["VANE_SHUFFLE_LOCAL_DIRS"] = str(args.scratch_dir / f"w{workers}")


def run_worker_matrix(
    args: argparse.Namespace,
    endpoint: str | None,
    proxy: CountingProxy | None,
) -> list[Sample]:
    selected_workloads = workloads(args)
    samples: list[Sample] = []
    for workers in args.worker_counts:
        configure_runner_environment(args, workers)
        cluster = start_cluster(workers, args.object_store_memory_mib * MIB)
        try:
            for repeat in range(1, args.cold_repeats + 1):
                for workload, sql in selected_workloads.items():
                    vane.teardown_runner()
                    connection = connect(args, endpoint)
                    try:
                        set_runner_ray(noop_if_initialized=True)
                        runner = runners.get_or_create_runner()
                        samples.append(
                            run_sample(
                                args,
                                connection,
                                runner,
                                proxy,
                                workers,
                                "cold",
                                workload,
                                sql,
                                repeat,
                            )
                        )
                    finally:
                        vane.teardown_runner()
                        connection.close()

            connection = connect(args, endpoint)
            try:
                set_runner_ray(noop_if_initialized=True)
                runner = runners.get_or_create_runner()
                for workload, sql in selected_workloads.items():
                    run_sample(
                        args,
                        connection,
                        runner,
                        proxy,
                        workers,
                        "warmup",
                        workload,
                        sql,
                        0,
                    )
                for repeat in range(1, args.warm_repeats + 1):
                    for workload, sql in selected_workloads.items():
                        samples.append(
                            run_sample(
                                args,
                                connection,
                                runner,
                                proxy,
                                workers,
                                "warm",
                                workload,
                                sql,
                                repeat,
                            )
                        )
            finally:
                vane.teardown_runner()
                connection.close()
        finally:
            ray.shutdown()
            cluster.shutdown()
    return samples


def summarize(samples: list[Sample]) -> list[dict[str, object]]:
    groups: dict[tuple[int, str, str], list[Sample]] = {}
    for sample in samples:
        groups.setdefault(
            (sample.workers, sample.cache_state, sample.workload), []
        ).append(sample)
    result: list[dict[str, object]] = []
    for key, group in sorted(groups.items()):
        result.append(
            {
                "workers": key[0],
                "cache_state": key[1],
                "workload": key[2],
                "samples": len(group),
                "planning_ms_mean": statistics.fmean(s.planning_ms for s in group),
                "planning_ms_median": statistics.median(s.planning_ms for s in group),
                "execution_ms_mean": statistics.fmean(s.execution_ms for s in group),
                "execution_ms_median": statistics.median(s.execution_ms for s in group),
                "logical_plan_bytes": group[-1].logical_plan_bytes,
                "physical_plan_bytes": group[-1].physical_plan_bytes,
                "split_count": group[-1].split_count,
                "split_payload_bytes": group[-1].split_payload_bytes,
                "planning_manifest_head_mean": statistics.fmean(
                    s.planning_requests.manifest_head for s in group
                ),
                "planning_manifest_get_mean": statistics.fmean(
                    s.planning_requests.manifest_get for s in group
                ),
                "execution_manifest_head_mean": statistics.fmean(
                    s.execution_requests.manifest_head for s in group
                ),
                "execution_manifest_get_mean": statistics.fmean(
                    s.execution_requests.manifest_get for s in group
                ),
            }
        )
    return result


def parse_worker_counts(value: str) -> list[int]:
    counts = [int(part) for part in value.split(",") if part.strip()]
    if not counts or any(count <= 0 for count in counts):
        raise argparse.ArgumentTypeError("worker counts must be positive")
    return counts


def parse_vector(value: str) -> list[float]:
    vector = [float(part) for part in value.split(",") if part.strip()]
    if not vector:
        raise argparse.ArgumentTypeError("query vector must not be empty")
    return vector


def parse_workloads(value: str) -> list[str]:
    available = ("scan", "vector", "fts", "hybrid")
    selected = [part.strip() for part in value.split(",") if part.strip()]
    if not selected or len(set(selected)) != len(selected):
        raise argparse.ArgumentTypeError("workloads must be unique and non-empty")
    unknown = [name for name in selected if name not in available]
    if unknown:
        raise argparse.ArgumentTypeError("unknown workloads: " + ", ".join(unknown))
    return selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-uri", required=True)
    parser.add_argument("--worker-counts", type=parse_worker_counts, default=[1, 8, 32])
    parser.add_argument(
        "--workloads",
        type=parse_workloads,
        default=["scan", "vector", "fts", "hybrid"],
    )
    parser.add_argument("--cold-repeats", type=int, default=3)
    parser.add_argument("--warm-repeats", type=int, default=5)
    parser.add_argument("--worker-slots", type=int, default=1)
    parser.add_argument("--object-store-memory-mib", type=int, default=96)
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--vector-column", default="vec")
    parser.add_argument("--vector", type=parse_vector, default=[0.0, 0.0, 0.0, 0.0])
    parser.add_argument("--text-column", default="text")
    parser.add_argument("--text-query", default="puppy")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument(
        "--upstream-endpoint", default=os.environ.get("AWS_ENDPOINT_URL")
    )
    parser.add_argument("--access-key", default=os.environ.get("AWS_ACCESS_KEY_ID", ""))
    parser.add_argument(
        "--secret-key", default=os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    )
    parser.add_argument(
        "--session-token", default=os.environ.get("AWS_SESSION_TOKEN", "")
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument(
        "--scratch-dir",
        type=Path,
        default=Path("build/vane-frozen-snapshot-benchmark"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("build/vane-frozen-snapshot-benchmark/results.json"),
    )
    args = parser.parse_args()
    if args.cold_repeats <= 0 or args.warm_repeats <= 0:
        parser.error("repeat counts must be positive")
    if args.worker_slots <= 0 or args.object_store_memory_mib <= 0 or args.k <= 0:
        parser.error("worker, memory, and k settings must be positive")
    return args


def main() -> None:
    args = parse_args()
    args.scratch_dir.mkdir(parents=True, exist_ok=True)
    with optional_counting_proxy(args.upstream_endpoint) as proxy:
        endpoint = proxy.endpoint if proxy else args.upstream_endpoint
        samples = run_worker_matrix(args, endpoint, proxy)
    output = {
        "contract": "vane-lance-frozen-snapshot-v1",
        "dataset_uri": args.dataset_uri,
        "worker_counts": args.worker_counts,
        "request_counts_measured": proxy is not None,
        "samples": [asdict(sample) for sample in samples],
        "summary": summarize(samples),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output["summary"], indent=2))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
