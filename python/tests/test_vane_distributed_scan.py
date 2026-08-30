# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import concurrent.futures
import contextlib
import http.server
import json
import multiprocessing
import os
import pickle
import queue
import shutil
import subprocess
import sys
import threading
import time
import uuid
import warnings
from pathlib import Path

import pytest
import vane
from vane import runners
from vane.runners.ray import set_runner_ray

WORKER_COUNT = 2
STABLE_ROW_ID_FIXTURE = (
    Path(__file__).resolve().parents[2] / "test/data/stable_row_ids.lance"
)
SEARCH_FIXTURE_REST_SCHEMA = {
    "fields": [
        {"name": "id", "nullable": False, "type": {"type": "int64"}},
        {"name": "label", "nullable": False, "type": {"type": "int32"}},
        {"name": "text", "nullable": False, "type": {"type": "utf8"}},
        {"name": "keywords", "nullable": False, "type": {"type": "utf8"}},
        {
            "name": "vec",
            "nullable": False,
            "type": {
                "type": "fixed_size_list",
                "length": 4,
                "fields": [
                    {
                        "name": "item",
                        "nullable": True,
                        "type": {"type": "float32"},
                    }
                ],
            },
        },
    ]
}
S3_TEST_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_ALLOW_HTTP",
    "AWS_ENDPOINT_URL",
    "AWS_REGION",
    "AWS_SECRET_ACCESS_KEY",
    "LANCE_S3_BUCKET",
)

_FTE_RETRY_GATE_ATTR = "_lance_distributed_scan_retry_gate"
_FTE_WORKER_RETRY_GATE_ATTR = "_lance_distributed_scan_worker_retry_gate"


def _install_fte_worker_retry_gate(
    actor, target_attempt_id: str, failure_phase: str
) -> None:
    """Fail one attempt either before execution or after its native scan."""
    import asyncio
    import threading
    import types

    from vane.runners.fte import FteTaskAttemptId

    if hasattr(actor, _FTE_WORKER_RETRY_GATE_ATTR):
        raise RuntimeError("the Lance worker retry gate is already installed")
    if failure_phase not in {"before-output", "after-output"}:
        raise ValueError(f"unsupported Lance retry failure phase: {failure_phase}")
    target_attempt = FteTaskAttemptId.coerce(target_attempt_id)
    manager = actor._get_fte_task_manager()
    original_method = actor._execute_fte_request
    original_manager_execute_fn = manager.execute_fn
    had_instance_method = "_execute_fte_request" in vars(actor)
    state: dict[str, object] = {
        "lock": threading.Lock(),
        "release": threading.Event(),
        "attempt": None,
        "failure_injected": False,
        "failure_phase": failure_phase,
        "target_attempt_id": str(target_attempt),
        "had_instance_method": had_instance_method,
        "original_manager_execute_fn": original_manager_execute_fn,
        "original_method": original_method,
    }

    async def fail_first_scan_attempt(_actor, request):
        attempt = FteTaskAttemptId.coerce(request.get("task_id"))
        should_fail = False
        with state["lock"]:
            if str(attempt) == state["target_attempt_id"] and state["attempt"] is None:
                state["attempt"] = {
                    "attempt_id": str(attempt),
                    "attempt_number": int(attempt.attempt_id),
                    "task_id": str(attempt.task_id),
                    "query_id": str(attempt.query_id),
                }
                should_fail = True
        if failure_phase == "before-output" and should_fail:
            released = await asyncio.to_thread(state["release"].wait, 90)
            if not released:
                raise TimeoutError(
                    f"timed out waiting to fail Lance FTE attempt {attempt}"
                )
            with state["lock"]:
                state["failure_injected"] = True
            raise RuntimeError("injected retryable Lance scan task failure")

        result = await original_method(request)
        if not should_fail:
            return result
        released = await asyncio.to_thread(state["release"].wait, 90)
        if not released:
            raise TimeoutError(f"timed out waiting to fail Lance FTE attempt {attempt}")
        with state["lock"]:
            state["failure_injected"] = True
        del result
        raise RuntimeError("injected retryable Lance scan task failure")

    wrapper = types.MethodType(fail_first_scan_attempt, actor)
    state["wrapper"] = wrapper
    setattr(actor, _FTE_WORKER_RETRY_GATE_ATTR, state)
    actor._execute_fte_request = wrapper
    manager.execute_fn = wrapper


def _fte_worker_retry_gate_snapshot(actor) -> dict[str, object]:
    state = getattr(actor, _FTE_WORKER_RETRY_GATE_ATTR, None)
    if state is None:
        return {
            "installed": False,
            "attempt": None,
            "failure_injected": False,
            "task_status": None,
        }
    with state["lock"]:
        attempt = state["attempt"]
        snapshot = {
            "installed": True,
            "attempt": attempt,
            "failure_injected": state["failure_injected"],
        }
    task_status = None
    if attempt is not None:
        manager = actor._get_fte_task_manager()
        execution = manager.tasks.get(attempt["attempt_id"])
        if execution is not None:
            task_status = execution.status_payload()
            task_status.pop("result", None)
    snapshot["task_status"] = task_status
    return snapshot


def _release_fte_worker_retry_gate(actor) -> None:
    state = getattr(actor, _FTE_WORKER_RETRY_GATE_ATTR, None)
    if state is not None:
        state["release"].set()


def _restore_fte_worker_retry_gate(actor) -> None:
    state = getattr(actor, _FTE_WORKER_RETRY_GATE_ATTR, None)
    if state is None:
        return
    state["release"].set()
    manager = actor._get_fte_task_manager()
    if manager.execute_fn is not state["wrapper"]:
        raise RuntimeError("the Lance worker retry gate lost task-manager ownership")
    if actor._execute_fte_request is not state["wrapper"]:
        raise RuntimeError("the Lance worker retry gate lost actor-method ownership")
    manager.execute_fn = state["original_manager_execute_fn"]
    if state["had_instance_method"]:
        actor._execute_fte_request = state["original_method"]
    else:
        del actor._execute_fte_request
    delattr(actor, _FTE_WORKER_RETRY_GATE_ATTR)


def _install_fte_retry_gate(actor, failure_phase: str) -> dict[str, dict[str, object]]:
    """Gate one real scan attempt and its task retry inside the query driver."""
    import hashlib
    import pickle as actor_pickle
    import threading

    import ray
    from vane.runners.fte import FteTaskAttemptId
    from vane.runners.ray.fragment_registry import _FTE_WORKER_HANDLES
    from vane.runners.ray.fragment_worker_client import RayWorkerActorHandle

    if hasattr(actor, _FTE_RETRY_GATE_ATTR):
        raise RuntimeError("the Lance FTE retry gate is already installed")
    if failure_phase not in {"before-output", "after-output"}:
        raise ValueError(f"unsupported Lance retry failure phase: {failure_phase}")

    def digest(value: object) -> str:
        return hashlib.sha256(actor_pickle.dumps(value, protocol=5)).hexdigest()

    def canonical(value: object) -> object:
        if isinstance(value, dict):
            return tuple(
                (str(key), canonical(item))
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            )
        if isinstance(value, (list, tuple)):
            return tuple(canonical(item) for item in value)
        if isinstance(value, (set, frozenset)):
            return tuple(sorted((canonical(item) for item in value), key=repr))
        if isinstance(value, (bytes, bytearray, memoryview)):
            return ("bytes", hashlib.sha256(bytes(value)).hexdigest())
        if isinstance(value, ray.ObjectRef):
            return ("object_ref", value.hex())
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        return (type(value).__name__, digest(value))

    def plan_summary(plan_or_ref: object) -> dict[str, object] | None:
        if plan_or_ref is None:
            return None
        plan_ref = plan_or_ref if isinstance(plan_or_ref, ray.ObjectRef) else None
        plan = ray.get(plan_ref) if plan_ref is not None else plan_or_ref
        return {
            "object_ref": None if plan_ref is None else plan_ref.hex(),
            "session_id": str(plan.session_id()),
            "session_config": canonical(plan.session_config()),
            "pickle_sha256": digest(plan),
        }

    def request_summary(handle, request: dict[str, object]) -> dict[str, object]:
        attempt = FteTaskAttemptId.coerce(request.get("task_id"))
        split_summary: list[tuple[str, tuple[object, ...]]] = []
        for source_id, raw_splits in sorted(
            dict(request.get("initial_splits") or {}).items()
        ):
            for raw_split in raw_splits:
                split = dict(raw_split)
                data = split.get("data")
                data_sha256 = None if data is None else digest(bytes(data))
                split_summary.append(
                    (
                        str(source_id),
                        (
                            str(split.get("source_node_id", source_id)),
                            int(split["sequence_id"]),
                            str(split["kind"]),
                            split.get("split_id"),
                            int(split.get("source_partition_id", 0)),
                            split.get("size_bytes"),
                            data_sha256,
                        ),
                    )
                )
        immutable_fields = {
            field: canonical(request.get(field))
            for field in (
                "descriptor_version",
                "context",
                "resource_request",
                "dynamic_scan_source_node_ids",
                "source_node_ids",
            )
        }
        return {
            "attempt_number": int(attempt.attempt_id),
            "task_id": str(attempt.task_id),
            "query_id": str(attempt.query_id),
            "partition_id": int(attempt.partition_id),
            "fragment_id": str(request["fragment_id"]),
            "worker_id": str(handle.worker_id),
            "node_id": str(handle.node_id),
            "immutable_fields": immutable_fields,
            "splits": split_summary,
            "plan": plan_summary(request.get("fragment_plan")),
        }

    state: dict[str, object] = {
        "lock": threading.Lock(),
        "release1": threading.Event(),
        "attempt0": None,
        "attempt1": None,
        "attempts": {},
        "target_task_id": None,
    }
    original_create = RayWorkerActorHandle.fte_create_task

    def gated_fte_create_task(handle, request: dict[str, object]):
        attempt = FteTaskAttemptId.coerce(request.get("task_id"))
        has_scan_split = any(
            str(split.get("kind")) == "scan_split"
            for splits in dict(request.get("initial_splits") or {}).values()
            for split in splits
        )
        install_worker_gate = False
        wait_event = None
        with state["lock"]:
            target_task_id = state["target_task_id"]
            if has_scan_split and attempt.attempt_id == 0 and target_task_id is None:
                summary = request_summary(handle, request)
                state["target_task_id"] = str(attempt.task_id)
                state["attempt0"] = summary
                state["attempts"][int(attempt.attempt_id)] = summary
                install_worker_gate = True
            elif str(attempt.task_id) == target_task_id:
                summary = request_summary(handle, request)
                attempt_number = int(attempt.attempt_id)
                previous = state["attempts"].get(attempt_number)
                if previous is not None and previous != summary:
                    raise RuntimeError(
                        f"FTE retry attempt {attempt} changed across submissions"
                    )
                state["attempts"][attempt_number] = summary
                if attempt_number == 1:
                    state["attempt1"] = summary
                    wait_event = state["release1"]
        if install_worker_gate:
            ray.get(
                handle.actor_handle.__ray_call__.remote(
                    _install_fte_worker_retry_gate,
                    str(attempt),
                    failure_phase,
                )
            )
        status = original_create(handle, request)
        if wait_event is not None:
            if not wait_event.wait(90):
                raise TimeoutError(
                    f"timed out waiting to release Lance FTE attempt {attempt}"
                )
        return status

    state["original_create"] = original_create
    state["create_wrapper"] = gated_fte_create_task
    setattr(actor, _FTE_RETRY_GATE_ATTR, state)
    RayWorkerActorHandle.fte_create_task = gated_fte_create_task
    return {
        str(worker_id): {
            "actor": handle.actor_handle,
            "node_id": str(handle.node_id),
        }
        for worker_id, handle in sorted(_FTE_WORKER_HANDLES.items())
    }


def _fte_retry_gate_snapshot(
    actor, include_registry: bool = False
) -> dict[str, object]:
    state = getattr(actor, _FTE_RETRY_GATE_ATTR)
    with state["lock"]:
        snapshot = {
            "attempt0": state["attempt0"],
            "attempt1": state["attempt1"],
            "attempts": dict(state["attempts"]),
        }
    if include_registry:
        from vane.runners.ray.fte_fragment_scheduler import fte_registry_stats

        snapshot["registry"] = fte_registry_stats()
    return snapshot


def _release_fte_retry_gate(actor, attempt_number: int) -> None:
    state = getattr(actor, _FTE_RETRY_GATE_ATTR)
    if attempt_number != 1:
        raise ValueError("only retry attempt one is gated")
    state[f"release{attempt_number}"].set()


def _restore_fte_retry_gate(actor) -> None:
    from vane.runners.ray.fragment_worker_client import RayWorkerActorHandle

    state = getattr(actor, _FTE_RETRY_GATE_ATTR, None)
    if state is None:
        return
    state["release1"].set()
    if RayWorkerActorHandle.fte_create_task is not state["create_wrapper"]:
        raise RuntimeError("the Lance retry gate lost query-driver ownership")
    RayWorkerActorHandle.fte_create_task = state["original_create"]
    delattr(actor, _FTE_RETRY_GATE_ATTR)


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _connect():
    connection = vane.connect(
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        }
    )
    extension_path = os.environ.get("LANCE_TEST_EXTENSION_PATH")
    if extension_path:
        connection.execute(f"LOAD {_sql_literal(extension_path)}")
    else:
        connection.execute("LOAD lance")
    return connection


@contextlib.contextmanager
def _empty_rest_namespace_server(expected_authorization: str | None = None):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            assert self.path.startswith("/v1/namespace/")
            assert "/table/list?" in self.path
            if expected_authorization is not None:
                assert self.headers.get("Authorization") == expected_authorization
            body = b'{"tables":[]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@contextlib.contextmanager
def _authenticated_rest_namespace_server(table_uri: str):
    expected_authorization = "Bearer rest-current-token"
    backend_sentinel = "rest-backend-secret-sentinel"
    observed_authorization: list[str | None] = []
    observations_lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        def _write_json(self, status: int, payload: object) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            authorization = self.headers.get("Authorization")
            with observations_lock:
                observed_authorization.append(authorization)
            if authorization == expected_authorization:
                return True
            self._write_json(
                401,
                {
                    "error": backend_sentinel,
                    "authorization": authorization,
                },
            )
            return False

        def do_GET(self) -> None:
            assert self.path.startswith("/v1/namespace/safe-namespace-id/table/list?")
            if not self._authorized():
                return
            self._write_json(200, {"tables": ["safe-namespace-id$items"]})

        def do_POST(self) -> None:
            assert self.path.startswith("/v1/table/")
            assert "/describe?" in self.path
            if not self._authorized():
                return
            self._write_json(
                200,
                {
                    "table": "items",
                    "namespace": ["safe-namespace-id"],
                    "version": 1,
                    "location": table_uri,
                    "table_uri": table_uri,
                    "schema": {
                        "fields": [
                            {
                                "name": "id",
                                "nullable": True,
                                "type": {"type": "int64"},
                            }
                        ]
                    },
                },
            )

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield (
            f"http://{host}:{port}",
            expected_authorization,
            backend_sentinel,
            observed_authorization,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def _serve_standard_rest_physical(
    table_uri: str,
    schema: dict[str, object],
    response_version,
    bearer_token: str,
    storage_secret_marker: str,
    stop_event,
    request_queue,
    ready_pipe,
) -> None:
    class Handler(http.server.BaseHTTPRequestHandler):
        def _write_json(self, status: int, payload: object) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _record(self, body: object | None = None) -> None:
            request_queue.put(
                {
                    "method": self.command,
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "body": body,
                }
            )

        def _authorized(self) -> bool:
            return self.headers.get("Authorization") == f"Bearer {bearer_token}"

        def do_GET(self) -> None:
            self._record()
            if not self._authorized():
                self._write_json(401, {"error": "unauthorized"})
                return
            if not self.path.startswith("/v1/namespace/safe-namespace-id/table/list?"):
                self._write_json(404, {"error": "unexpected path"})
                return
            self._write_json(200, {"tables": ["safe-namespace-id$items"]})

        def do_POST(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length)
            body = json.loads(raw_body) if raw_body else {}
            self._record(body)
            if not self._authorized():
                self._write_json(401, {"error": "unauthorized"})
                return
            if not self.path.startswith("/v1/table/") or "/describe?" not in self.path:
                self._write_json(404, {"error": "unexpected path"})
                return
            response: dict[str, object] = {
                "table": "items",
                "namespace": ["safe-namespace-id"],
                "version": int(response_version.value),
                "location": table_uri,
                "table_uri": table_uri,
                "schema": schema,
            }
            if body.get("vend_credentials") is False:
                response["storage_options"] = {
                    "secret_access_key": storage_secret_marker,
                }
            self._write_json(200, response)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.timeout = 0.1
    host, port = server.server_address
    ready_pipe.send((host, port))
    ready_pipe.close()
    try:
        while not stop_event.is_set():
            server.handle_request()
    finally:
        server.server_close()


@contextlib.contextmanager
def _standard_rest_physical_server(
    table_uri: str,
    schema: dict[str, object],
    version: int,
):
    bearer_token = "standard-rest-control-token"
    storage_secret_marker = "response-storage-options-must-not-be-used"
    observed_requests: list[dict[str, object]] = []
    process_context = multiprocessing.get_context("spawn")
    response_version = process_context.Value("q", version)
    stop_event = process_context.Event()
    request_queue = process_context.Queue()
    ready_parent, ready_child = process_context.Pipe(duplex=False)
    process = process_context.Process(
        target=_serve_standard_rest_physical,
        args=(
            table_uri,
            schema,
            response_version,
            bearer_token,
            storage_secret_marker,
            stop_event,
            request_queue,
            ready_child,
        ),
    )
    process.start()
    ready_child.close()
    try:
        if not ready_parent.poll(15):
            raise RuntimeError("standard REST test server did not start")
        host, port = ready_parent.recv()
        yield (
            f"http://{host}:{port}",
            bearer_token,
            storage_secret_marker,
            observed_requests,
            response_version,
        )
    finally:
        ready_parent.close()
        stop_event.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        assert process.exitcode == 0
        while True:
            try:
                observed_requests.append(request_queue.get_nowait())
            except queue.Empty:
                break
        request_queue.close()


@contextlib.contextmanager
def _failing_object_store_server(*, allow_empty_listing: bool = False):
    backend_sentinel = "object-store-backend-secret-sentinel"
    observed_requests: list[tuple[str, str, str | None]] = []
    observations_lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        def _record_request(self) -> None:
            with observations_lock:
                observed_requests.append(
                    (self.command, self.path, self.headers.get("Authorization"))
                )

        def _empty_listing(self) -> None:
            self._record_request()
            body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                "<Name>redaction-test-bucket</Name><Prefix></Prefix>"
                "<KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys>"
                "<IsTruncated>false</IsTruncated></ListBucketResult>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _fail(self) -> None:
            self._record_request()
            body = (
                "<Error><Code>AccessDenied</Code><Message>"
                + backend_sentinel
                + "</Message></Error>"
            ).encode("utf-8")
            self.send_response(403)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self) -> None:
            if allow_empty_listing and "list-type=2" in self.path:
                self._empty_listing()
                return
            self._fail()

        do_DELETE = _fail
        do_HEAD = _fail
        do_POST = _fail
        do_PUT = _fail

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", backend_sentinel, observed_requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def _s3_test_config() -> dict[str, str]:
    if os.environ.get("LANCE_TEST_S3") != "1":
        pytest.skip("set LANCE_TEST_S3=1 to run the distributed MinIO test")
    config: dict[str, str] = {}
    for name in S3_TEST_ENV:
        value = os.environ.get(name)
        if not value:
            raise AssertionError(f"{name} is required when LANCE_TEST_S3=1")
        config[name] = value
    return config


def _run_isolated_s3_credential_check(
    tmp_path: Path,
    path: str,
    mode: str,
) -> None:
    config = _s3_test_config()
    environment = os.environ.copy()
    process_access_key = (
        config["AWS_ACCESS_KEY_ID"] if mode == "profile" else "wrong-process-access-key"
    )
    process_secret_key = (
        config["AWS_SECRET_ACCESS_KEY"]
        if mode == "profile"
        else "wrong-process-secret-key"
    )
    environment.update(
        {
            "AWS_ACCESS_KEY_ID": process_access_key,
            "AWS_SECRET_ACCESS_KEY": process_secret_key,
            "AWS_SESSION_TOKEN": "wrong-process-session-token",
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
            "LANCE_CREDENTIAL_TEST_ACCESS_KEY_ID": config["AWS_ACCESS_KEY_ID"],
            "LANCE_CREDENTIAL_TEST_MODE": mode,
            "LANCE_CREDENTIAL_TEST_PATH": path,
            "LANCE_CREDENTIAL_TEST_SECRET_ACCESS_KEY": config["AWS_SECRET_ACCESS_KEY"],
            "PYTHONSAFEPATH": "1",
            "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO": "0",
            "VANE_DISTRIBUTED_NODE_COUNT": "2",
            "VANE_DISTRIBUTED_WORKER_SLOTS": "2",
            "VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION": "1",
            "VANE_RAY_SCAN_SPLIT_MIN_COUNT": "4",
            "VANE_SHUFFLE_LOCAL_DIRS": str(tmp_path / "shuffle"),
        }
    )
    if mode != "profile":
        # Prevent an inherited profile, role, or web-identity provider from
        # accidentally making the connection/secret/missing cases succeed.
        for key in (
            "AWS_CONFIG_FILE",
            "AWS_PROFILE",
            "AWS_ROLE_ARN",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
        ):
            environment.pop(key, None)
        environment["AWS_EC2_METADATA_DISABLED"] = "true"
    if mode == "profile":
        profile_name = "lance-integration"
        credentials_file = tmp_path / "aws-credentials"
        config_file = tmp_path / "aws-config"
        credentials_file.write_text(
            f"[{profile_name}]\n"
            f"aws_access_key_id = {config['AWS_ACCESS_KEY_ID']}\n"
            f"aws_secret_access_key = {config['AWS_SECRET_ACCESS_KEY']}\n",
            encoding="utf-8",
        )
        config_file.write_text(
            f"[profile {profile_name}]\nregion = {config['AWS_REGION']}\n",
            encoding="utf-8",
        )
        for key in (
            "AWS_ACCESS_KEY_ID",
            "AWS_CONFIG_FILE",
            "AWS_PROFILE",
            "AWS_ROLE_ARN",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
        ):
            environment.pop(key, None)
        environment.update(
            {
                "LANCE_CREDENTIAL_TEST_AWS_CONFIG_FILE": str(config_file),
                "LANCE_CREDENTIAL_TEST_AWS_PROFILE": profile_name,
                "LANCE_CREDENTIAL_TEST_AWS_SHARED_CREDENTIALS_FILE": str(
                    credentials_file
                ),
            }
        )
    script = r"""
import os
import pickle
import time
import warnings

import ray
import vane
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from vane import runners
from vane.runners.ray import set_runner_ray


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def fte_create_task_locations():
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
    assert int(reply.status.code) == 0, reply.status.message
    locations = {}
    for event in reply.events_by_task:
        if not event.task_info.name.endswith(".fte_create_task"):
            continue
        node_id = event.state_updates.node_id or event.task_info.node_id
        if node_id:
            locations[event.task_id.hex()] = node_id.hex()
    return locations


def settled_fte_create_task_locations():
    deadline = time.monotonic() + 5
    previous = None
    stable_observations = 0
    while time.monotonic() < deadline:
        current = fte_create_task_locations()
        if current == previous:
            stable_observations += 1
            if stable_observations >= 3:
                return current
        else:
            previous = current
            stable_observations = 0
        time.sleep(0.1)
    return previous or {}


def new_fte_create_task_node_ids(baseline_task_ids, expected_count):
    deadline = time.monotonic() + 15
    observed = set()
    while time.monotonic() < deadline:
        observed = {
            node_id
            for task_id, node_id in fte_create_task_locations().items()
            if task_id not in baseline_task_ids
        }
        if len(observed) >= expected_count:
            return observed
        time.sleep(0.1)
    return observed


path = os.environ["LANCE_CREDENTIAL_TEST_PATH"]
access_key_id = os.environ["LANCE_CREDENTIAL_TEST_ACCESS_KEY_ID"]
secret_access_key = os.environ["LANCE_CREDENTIAL_TEST_SECRET_ACCESS_KEY"]
mode = os.environ["LANCE_CREDENTIAL_TEST_MODE"]
sensitive_values = (
    access_key_id,
    secret_access_key,
    "wrong-process-access-key",
    "wrong-process-secret-key",
    "wrong-process-session-token",
    "wrong-connection-access-key",
    "wrong-connection-secret-key",
    "changed-session-access-key",
    "changed-session-secret-key",
    "drift.invalid:1",
)
profile_environment_keys = (
    "AWS_ACCESS_KEY_ID",
    "AWS_CONFIG_FILE",
    "AWS_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
)

if ray.is_initialized():
    ray.shutdown()
cluster = Cluster(shutdown_at_exit=False)
connection = None
try:
    # Start every Ray node before installing the temporary profile in the
    # driver. Worker processes therefore cannot satisfy the scan from ambient
    # profile/static provider state.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=r"Tip: In future versions of Ray")
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=128 * 1024 * 1024,
        )
        for _ in range(2):
            cluster.add_node(
                num_cpus=1,
                num_gpus=0,
                object_store_memory=128 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=True, log_to_driver=True)

    execution_node_ids = {
        str(node["NodeID"])
        for node in ray.nodes()
        if node.get("Alive")
        and float((node.get("Resources") or {}).get("CPU", 0)) >= 1
    }
    assert len(execution_node_ids) == 2

    if mode == "profile":
        os.environ["AWS_PROFILE"] = os.environ[
            "LANCE_CREDENTIAL_TEST_AWS_PROFILE"
        ]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.environ[
            "LANCE_CREDENTIAL_TEST_AWS_SHARED_CREDENTIALS_FILE"
        ]
        os.environ["AWS_CONFIG_FILE"] = os.environ[
            "LANCE_CREDENTIAL_TEST_AWS_CONFIG_FILE"
        ]

        @ray.remote(num_cpus=0.01)
        def worker_provider_environment():
            return {key: bool(os.environ.get(key)) for key in profile_environment_keys}

        worker_environments = ray.get(
            [
                worker_provider_environment.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=node_id, soft=False
                    )
                ).remote()
                for node_id in sorted(execution_node_ids)
            ]
        )
        assert all(
            not any(environment.values()) for environment in worker_environments
        )
    elif mode not in {"connection", "secret", "missing"}:
        raise AssertionError(f"unknown credential test mode: {mode}")

    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    connection.execute("LOAD lance")
    connection.execute("LOAD httpfs")

    if mode == "connection":
        connection.execute(f"SET s3_access_key_id = {sql_literal(access_key_id)}")
        connection.execute(
            f"SET s3_secret_access_key = {sql_literal(secret_access_key)}"
        )
        # A static access-key pair has no session token. Make that part of the
        # replayable connection state so Vane cannot combine it with an
        # inherited process token.
        connection.execute("SET s3_session_token = ''")
        credential_scopes = dict(
            connection.execute(
                "SELECT name, scope FROM duckdb_settings() "
                "WHERE name IN ('s3_access_key_id', 's3_secret_access_key', "
                "'s3_session_token')"
            ).fetchall()
        )
        assert credential_scopes == {
            "s3_access_key_id": "LOCAL",
            "s3_secret_access_key": "LOCAL",
            "s3_session_token": "LOCAL",
        }

        # Exercise the exact key-pair/empty-token override before any
        # non-default DuckDB endpoint, URL-style, or TLS setting can
        # independently select the explicit Lance provider. The inherited
        # process key pair and non-empty token are intentionally invalid.
        connection.execute("RESET s3_endpoint")
        connection.execute("RESET s3_url_style")
        connection.execute("RESET s3_use_ssl")
        focused_relation = connection.sql("SELECT 1")
        focused_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            focused_relation, "lance-s3-static-pair-only-override"
        )
        assert focused_logical.has_explicit_s3_credentials()

        connection.execute(
            f"SET s3_region = {sql_literal(os.environ['AWS_REGION'])}"
        )
        connection.execute(
            f"SET s3_endpoint = {sql_literal(os.environ['AWS_ENDPOINT_URL'])}"
        )
        connection.execute("SET s3_url_style = 'path'")
        connection.execute("SET s3_use_ssl = false")
        endpoint = connection.execute(
            "SELECT value FROM duckdb_settings() WHERE name = 's3_endpoint'"
        ).fetchone()
        assert endpoint == (os.environ["AWS_ENDPOINT_URL"],)
        transport = dict(
            connection.execute(
                "SELECT name, value FROM duckdb_settings() "
                "WHERE name IN ('s3_url_style', 's3_use_ssl')"
            ).fetchall()
        )
        assert transport == {"s3_url_style": "path", "s3_use_ssl": "false"}
    elif mode == "secret":
        connection.execute("SET s3_access_key_id = 'wrong-connection-access-key'")
        connection.execute(
            "SET s3_secret_access_key = 'wrong-connection-secret-key'"
        )
        scope = f"s3://{os.environ['LANCE_S3_BUCKET']}/"
        connection.execute(
            "CREATE SECRET lance_secret_precedence ("
            "TYPE LANCE, PROVIDER config, "
            f"SCOPE {sql_literal(scope)}, "
            f"ACCESS_KEY_ID {sql_literal(access_key_id)}, "
            f"SECRET_ACCESS_KEY {sql_literal(secret_access_key)}, "
            f"REGION {sql_literal(os.environ['AWS_REGION'])}, "
            f"ENDPOINT {sql_literal(os.environ['AWS_ENDPOINT_URL'])}, "
            "VIRTUAL_HOSTED_STYLE_REQUEST false, ALLOW_HTTP true)"
        )
        namespace_root = path.rsplit("/", 1)[0]
        connection.execute(
            f"ATTACH {sql_literal(namespace_root)} AS secret_ns (TYPE LANCE)"
        )
        # Materialize the lazy table entry while the secret exists. A later
        # bind must retain coordinator-only provenance even after the secret is
        # removed from the live secret manager.
        assert connection.execute(
            "SELECT id FROM secret_ns.main.items ORDER BY id"
        ).fetchall() == [(row_id,) for row_id in range(12)]

        # Vane captures every visible ATTACH, even when the transported plan
        # does not reference that catalog. Replaying a secret-backed namespace
        # must therefore use a credential-free placeholder: the TYPE LANCE
        # secret is coordinator-only and is intentionally not serialized.
        local_path = os.path.abspath("unrelated-to-secret-namespace.lance")
        connection.execute(
            "COPY (SELECT i::BIGINT AS id FROM range(12) AS source(i)) "
            f"TO {sql_literal(local_path)} "
            "(FORMAT LANCE, MODE 'create', MAX_ROWS_PER_FILE 3)"
        )
        unrelated_relation = connection.sql(f"SELECT id FROM {sql_literal(local_path)}")
        unrelated_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            unrelated_relation, "lance-secret-namespace-unrelated-plan"
        )
        attached_databases = unrelated_logical.__getstate__()[3][
            "attached_databases"
        ]
        assert len(attached_databases) == 1
        attach_sql = attached_databases[0]
        secret_snapshot_path = (
            "vane-internal://lance/directory-planning-snapshot"
        )
        assert secret_snapshot_path in attach_sql
        assert namespace_root not in attach_sql
        serialized_unrelated = pickle.dumps(unrelated_logical)
        assert namespace_root.encode() not in serialized_unrelated
        assert access_key_id.encode() not in serialized_unrelated
        assert secret_access_key.encode() not in serialized_unrelated

        planning_connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        unrelated_physical = None
        try:
            planning_connection.execute("LOAD lance")
            restored_unrelated = pickle.loads(serialized_unrelated)
            unrelated_physical = restored_unrelated.to_physical_plan(
                planning_connection
            )
            assert (
                sum(
                    len(batches)
                    for batches in unrelated_physical.scan_split_batch_map().values()
                )
                == 4
            )
            planning_connection.execute(
                f"ATTACH {sql_literal(secret_snapshot_path)} "
                "AS secret_shadow (TYPE LANCE)"
            )
            try:
                planning_connection.execute(
                    "SELECT * FROM secret_shadow.main.any_table"
                )
            except Exception as error:
                assert "planning snapshot cannot access" in str(error)
            else:
                raise AssertionError(
                    "a directory planning snapshot exposed a table"
                )
        finally:
            unrelated_physical = None
            planning_connection.close()

    if mode == "missing":
        try:
            connection.execute(f"SELECT id FROM {sql_literal(path)} ORDER BY id")
        except Exception as error:
            message = str(error)
            assert (
                "Failed to open Lance dataset: <redacted-private-uri>" in message
            )
            assert "Lance error details redacted" in message
            assert path not in message
            for sensitive in sensitive_values:
                assert sensitive not in message
        else:
            raise AssertionError("missing S3 credentials unexpectedly opened the dataset")
    else:
        assert connection.execute(
            f"SELECT id FROM {sql_literal(path)} ORDER BY id"
        ).fetchall() == [(row_id,) for row_id in range(12)]
    if mode == "missing":
        raise SystemExit(0)
    # Keep the distributed plan to a pure Lance scan/projection. Otherwise a
    # downstream ORDER BY task could execute on the second node and make the
    # topology assertion below pass even when all scan splits ran on one node.
    relation = connection.sql(f"SELECT id FROM {sql_literal(path)}")

    if mode == "connection":
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, f"lance-s3-{mode}-precedence"
        )
        assert logical.has_explicit_s3_credentials()
        physical = logical.to_physical_plan(connection)
        split_count = sum(
            len(batches) for batches in physical.scan_split_batch_map().values()
        )
        assert split_count == 4, split_count
        sensitive_bytes = tuple(value.encode() for value in sensitive_values)
        for batches in physical.scan_split_batch_map().values():
            for batch in batches:
                payload = bytes(batch)
                assert all(value not in payload for value in sensitive_bytes)
        for rendered in (repr(logical), repr(physical)):
            assert all(value not in rendered for value in sensitive_values)
    elif mode == "secret":
        try:
            logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                relation, f"lance-s3-{mode}-precedence"
            )
            logical.to_physical_plan(connection)
        except Exception as error:
            message = str(error)
            assert "coordinator-only TYPE LANCE secret" in message
            assert all(value not in message for value in sensitive_values)
        else:
            raise AssertionError(
                "a coordinator-only TYPE LANCE secret produced a distributed plan"
            )

        connection.execute("DROP SECRET lance_secret_precedence")
        namespace_relation = connection.sql(
            "SELECT id FROM secret_ns.main.items ORDER BY id"
        )
        try:
            namespace_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                namespace_relation, "lance-s3-secret-provenance-after-drop"
            )
            namespace_logical.to_physical_plan(connection)
        except Exception as error:
            message = str(error)
            assert "coordinator-only TYPE LANCE secret" in message
            assert all(value not in message for value in sensitive_values)
        else:
            raise AssertionError(
                "a directory namespace captured from a TYPE LANCE secret "
                "became distributable after the secret was dropped"
            )
    else:
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, f"lance-s3-{mode}-replay"
        )
        assert not logical.has_explicit_s3_credentials()
        session_config = logical.session_config()
        assert session_config["AWS_PROFILE"] == "lance-integration"
        assert all(value not in repr(logical) for value in sensitive_values)

    if mode != "secret":
        set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        baseline_task_ids = set(settled_fte_create_task_locations())
        rows = sorted(
            tuple(row.values())
            for table in runner.run_iter_tables(relation)
            for row in table.to_pylist()
        )
        assert rows == [(row_id,) for row_id in range(12)]
        assert new_fte_create_task_node_ids(
            baseline_task_ids, expected_count=2
        ) == execution_node_ids
        if mode == "connection":
            namespace_root = path.rsplit("/", 1)[0]
            search_path = f"{namespace_root}/search_items.lance"
            connection.execute(
                "COPY (SELECT i::BIGINT AS id, i::INTEGER AS label, "
                "CASE WHEN i % 2 = 0 THEN 'puppy' ELSE 'kitten' END::VARCHAR "
                "AS text, [i::FLOAT, 0.0, 0.0, 0.0]::FLOAT[4] AS vec "
                "FROM range(5) AS source(i)) "
                f"TO {sql_literal(search_path)} (FORMAT LANCE, MODE 'create')"
            )
            connection.execute(
                f"ATTACH {sql_literal(namespace_root)} AS credential_ns (TYPE LANCE)"
            )
            namespace_relation = connection.sql(
                "SELECT id FROM credential_ns.main.items ORDER BY id"
            )
            namespace_rows = [
                tuple(row.values())
                for table in runner.run_iter_tables(namespace_relation)
                for row in table.to_pylist()
            ]
            assert namespace_rows == [(row_id,) for row_id in range(12)]

            search_source = sql_literal("credential_ns.main.search_items")
            namespace_searches = (
                (
                    "vector",
                    "SELECT id FROM lance_vector_search("
                    f"{search_source}, 'vec', "
                    "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                    "k = 2, use_index = false) ORDER BY _distance, id",
                ),
                (
                    "fts",
                    "SELECT id FROM lance_fts("
                    f"{search_source}, 'text', 'puppy', k = 5) "
                    "ORDER BY _score DESC, id",
                ),
                (
                    "hybrid",
                    "SELECT id FROM lance_hybrid_search("
                    f"{search_source}, 'vec', "
                    "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                    "'text', 'puppy', k = 2, use_index = false) "
                    "ORDER BY _hybrid_score DESC, id",
                ),
            )
            namespace_search_results = {
                name: connection.execute(sql).fetchall()
                for name, sql in namespace_searches
            }

            # The attached catalog entry keeps using its already-opened
            # coordinator dataset after the session changes, but replaying its
            # captured ATTACH options on workers would use the new values. The
            # planner must reject that drift before serializing either set of
            # credentials.
            connection.execute(
                "SET s3_access_key_id = 'changed-session-access-key'"
            )
            connection.execute(
                "SET s3_secret_access_key = 'changed-session-secret-key'"
            )
            connection.execute("SET s3_endpoint = 'drift.invalid:1'")
            assert connection.execute(
                "SELECT id FROM credential_ns.main.items ORDER BY id"
            ).fetchall() == [(row_id,) for row_id in range(12)]
            drifted_relation = connection.sql(
                "SELECT id FROM credential_ns.main.items ORDER BY id"
            )
            try:
                drifted_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                    drifted_relation, "lance-s3-namespace-session-drift"
                )
                drifted_logical.to_physical_plan(connection)
            except Exception as error:
                message = str(error)
                assert "query session storage settings to match" in message
                assert all(value not in message for value in sensitive_values)
            else:
                raise AssertionError(
                    "a directory namespace with changed session storage "
                    "settings produced a distributed plan"
                )
            for name, sql in namespace_searches:
                assert connection.execute(sql).fetchall() == namespace_search_results[
                    name
                ]
                search_relation = connection.sql(sql)
                try:
                    search_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                        search_relation,
                        f"lance-s3-namespace-search-session-drift-{name}",
                    )
                    search_logical.to_physical_plan(connection)
                except Exception as error:
                    message = str(error)
                    assert "query session storage settings to match" in message
                    assert all(
                        value not in message for value in sensitive_values
                    )
                else:
                    raise AssertionError(
                        f"a {name} directory namespace search with changed "
                        "session storage settings produced a distributed plan"
                    )
        client = runner.query_driver_client
        assert client is not None
        stats = ray.get(client.runner.fragment_stats.remote())
        assert len(stats["workers"]) == 2
finally:
    if connection is not None:
        connection.close()
    vane.teardown_runner()
    ray.shutdown()
    cluster.shutdown()
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    combined_output = completed.stdout + completed.stderr
    sensitive_values = (
        config["AWS_ACCESS_KEY_ID"],
        config["AWS_SECRET_ACCESS_KEY"],
        "wrong-process-access-key",
        "wrong-process-secret-key",
        "wrong-process-session-token",
        "wrong-connection-access-key",
        "wrong-connection-secret-key",
        "changed-session-access-key",
        "changed-session-secret-key",
        "drift.invalid:1",
    )
    if completed.returncode != 0:
        redacted_output = combined_output
        for value in sensitive_values:
            redacted_output = redacted_output.replace(value, "<redacted>")
        raise AssertionError(
            f"isolated credential check failed with exit code "
            f"{completed.returncode}:\n{redacted_output}"
        )
    assert all(value not in combined_output for value in sensitive_values)


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


def _rewrite_take_split(
    batch: bytes,
    *,
    old_begin: int,
    new_begin: int,
    row_ids: list[int],
) -> bytes:
    old_split_id = f"take:{old_begin}".encode()
    new_split_id = f"take:{new_begin}".encode()
    assert len(old_split_id) == len(new_split_id)

    rewritten = bytearray(batch)
    assert rewritten.count(old_split_id) == 1
    split_id_offset = rewritten.index(old_split_id)
    rewritten[split_id_offset : split_id_offset + len(old_split_id)] = new_split_id

    magic = b"LTS1"
    assert rewritten.count(magic) == 1
    begin_offset = rewritten.index(magic) + len(magic) + 36
    assert int.from_bytes(rewritten[begin_offset : begin_offset + 8], "little") == (
        old_begin
    )
    rewritten[begin_offset : begin_offset + 8] = new_begin.to_bytes(8, "little")
    row_id_offset = begin_offset + 8
    for index, row_id in enumerate(row_ids):
        offset = row_id_offset + index * 8
        rewritten[offset : offset + 8] = row_id.to_bytes(8, "little")
    return bytes(rewritten)


def _protobuf_fields(
    payload: bytearray, start: int, end: int
) -> list[tuple[int, int, int, int]]:
    fields: list[tuple[int, int, int, int]] = []

    def read_varint(offset: int) -> tuple[int, int]:
        value = 0
        shift = 0
        while offset < end and shift < 70:
            byte = payload[offset]
            offset += 1
            value |= (byte & 0x7F) << shift
            if byte < 0x80:
                return value, offset
            shift += 7
        raise AssertionError("invalid protobuf varint in Lance manifest")

    offset = start
    while offset < end:
        key, offset = read_varint(offset)
        field_number = key >> 3
        wire_type = key & 7
        if field_number == 0:
            raise AssertionError("invalid protobuf field in Lance manifest")
        if wire_type == 0:
            value_start = offset
            _, offset = read_varint(offset)
            fields.append((field_number, wire_type, value_start, offset))
        elif wire_type == 1:
            value_start = offset
            offset += 8
            fields.append((field_number, wire_type, value_start, offset))
        elif wire_type == 2:
            length, offset = read_varint(offset)
            value_start = offset
            offset += length
            fields.append((field_number, wire_type, value_start, offset))
        elif wire_type == 5:
            value_start = offset
            offset += 4
            fields.append((field_number, wire_type, value_start, offset))
        else:
            raise AssertionError(
                f"unsupported protobuf wire type {wire_type} in Lance manifest"
            )
        if offset > end:
            raise AssertionError("truncated protobuf field in Lance manifest")
    return fields


def _clear_manifest_deletion_counts(path: Path) -> None:
    versions_path = path / "_versions"
    version_hint = json.loads(
        (versions_path / "latest_version_hint.json").read_text(encoding="utf-8")
    )
    latest_version = int(version_hint["version"])
    # Lance's V2 manifest naming scheme stores version N at u64::MAX - N.
    manifest_path = versions_path / f"{(1 << 64) - 1 - latest_version}.manifest"
    assert manifest_path.is_file()
    payload = bytearray(manifest_path.read_bytes())
    assert payload[-4:] == b"LANC"
    manifest_offset = int.from_bytes(payload[-16:-8], "little", signed=True)
    manifest_length = int.from_bytes(
        payload[manifest_offset : manifest_offset + 4], "little"
    )
    manifest_start = manifest_offset + 4
    manifest_end = manifest_start + manifest_length
    assert manifest_end == len(payload) - 16

    cleared = 0
    for field_number, wire_type, fragment_start, fragment_end in _protobuf_fields(
        payload, manifest_start, manifest_end
    ):
        if field_number != 2 or wire_type != 2:
            continue
        for (
            fragment_field,
            fragment_wire,
            deletion_start,
            deletion_end,
        ) in _protobuf_fields(payload, fragment_start, fragment_end):
            if fragment_field != 3 or fragment_wire != 2:
                continue
            for (
                deletion_field,
                deletion_wire,
                value_start,
                value_end,
            ) in _protobuf_fields(payload, deletion_start, deletion_end):
                if deletion_field != 4 or deletion_wire != 0:
                    continue
                # Proto3 represents the absent legacy count as zero. Keep the
                # encoded width stable so the manifest footer stays valid.
                for offset in range(value_start, value_end - 1):
                    payload[offset] = 0x80
                payload[value_end - 1] = 0
                cleared += 1
    assert cleared > 0
    manifest_path.write_bytes(payload)


def _run(runner, relation) -> list[tuple[object, ...]]:
    return [
        tuple(row.values())
        for table in runner.run_iter_tables(relation)
        for row in table.to_pylist()
    ]


def _run_serialized_logical(runner, serialized: bytes) -> list[tuple[object, ...]]:
    logical = pickle.loads(serialized)
    client = runner._client_for_session(str(logical.session_id()))
    return [
        tuple(row.values())
        for partition in client.stream_plan(logical)
        for row in partition.partition().to_pylist()
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


@pytest.fixture
def ray_retry_runner(ray_cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    assert len(ray_cluster) == WORKER_COUNT
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "2")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "2")
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("VANE_FTE_RETRY_INITIAL_DELAY_S", "0")
    monkeypatch.setenv("VANE_FTE_STATUS_WAIT_TIMEOUT_S", "5")
    monkeypatch.setenv("VANE_FTE_CONTROL_RPC_INITIAL_BACKOFF_S", "0")
    monkeypatch.setenv("VANE_SHUFFLE_LOCAL_DIRS", str(tmp_path / "retry-shuffle"))
    vane.teardown_runner()
    set_runner_ray(noop_if_initialized=True)
    try:
        yield runners.get_or_create_runner()
    finally:
        vane.teardown_runner()


def test_global_search_overloads_match_native_and_emit_one_task(ray_runner) -> None:
    path = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path_sql = _sql_literal(path)
    searches = (
        (
            "vector-float",
            "SELECT id, _distance FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 3, use_index = false) ORDER BY _distance, id",
        ),
        (
            "vector-double",
            "SELECT id, _distance FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::DOUBLE[4], "
            "k = 3, use_index = false) ORDER BY _distance, id",
        ),
        (
            "fts",
            "SELECT id, _score FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 10) ORDER BY _score DESC, id",
        ),
        (
            "hybrid-float",
            "SELECT id, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 3, use_index = false, alpha = 0.5, "
            "oversample_factor = 4) ORDER BY _hybrid_score DESC, id",
        ),
        (
            "hybrid-double",
            "SELECT id, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::DOUBLE[4], "
            "'text', 'puppy', k = 3, use_index = false, alpha = 0.5, "
            "oversample_factor = 4) ORDER BY _hybrid_score DESC, id",
        ),
        (
            "fts-empty-query",
            "SELECT id, _score FROM lance_fts("
            f"{path_sql}, 'text', '', k = 10) ORDER BY _score DESC, id",
        ),
        (
            "hybrid-empty-query",
            "SELECT id, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', '', k = 3, use_index = false) "
            "ORDER BY _hybrid_score DESC, id",
        ),
        (
            "vector-prefilter",
            "SELECT id FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 10, prefilter = true, use_index = false) "
            "WHERE label >= 4 ORDER BY id",
        ),
        (
            "fts-prefilter",
            "SELECT id FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 10, prefilter = true) "
            "WHERE label >= 3 ORDER BY id",
        ),
        (
            "hybrid-empty-prefilter",
            "SELECT id FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 3, prefilter = true, use_index = false) "
            "WHERE label > 100 ORDER BY id",
        ),
        (
            "vector-controls-postfilter-offset",
            "SELECT id, _distance FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 5, nprobs = 2, refine_factor = 2, prefilter = false, "
            "use_index = false) WHERE label >= 2 "
            "ORDER BY _distance, id LIMIT 2 OFFSET 1",
        ),
        (
            "fts-postfilter",
            "SELECT id, _score FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 10, prefilter = false) "
            "WHERE label >= 3 ORDER BY _score DESC, id",
        ),
        (
            "hybrid-clamped-alpha-default-oversample",
            "SELECT id, _hybrid_score FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 3, use_index = false, alpha = 2.0, "
            "oversample_factor = 0) ORDER BY _hybrid_score DESC, id",
        ),
    )

    connection = _connect()
    try:
        for name, sql in searches:
            expected = connection.execute(sql).fetchall()
            relation = connection.sql(sql)
            split_batches = _split_batches(connection, relation)
            flattened = [
                batch for batches in split_batches.values() for batch in batches
            ]
            assert len(flattened) == 1, name
            assert flattened[0].count(b"LGS1") == 1, name
            assert b"global:" in flattened[0], name
            assert _run(ray_runner, relation) == expected, name
    finally:
        connection.close()


def test_global_search_computed_score_postfilters_match_native(ray_runner) -> None:
    path = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path_sql = _sql_literal(path)
    searches = (
        (
            "vector-distance",
            "SELECT id, _distance FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 3, prefilter = true, use_index = false) "
            "WHERE abs(_distance) < 1000000 ORDER BY _distance, id",
        ),
        (
            "fts-score",
            "SELECT id, _score FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 10, prefilter = true) "
            "WHERE abs(_score) < 1000000 ORDER BY _score DESC, id",
        ),
        (
            "hybrid-score",
            "SELECT id, _hybrid_score FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 3, prefilter = true, use_index = false) "
            "WHERE abs(_hybrid_score) < 1000000 "
            "ORDER BY _hybrid_score DESC, id",
        ),
    )

    connection = _connect()
    try:
        for name, sql in searches:
            expected = connection.execute(sql).fetchall()
            relation = connection.sql(sql)
            assert _split_count(connection, relation) == 1, name
            assert _run(ray_runner, relation) == expected, name
    finally:
        connection.close()


def test_direct_fts_and_hybrid_reject_complex_prefilter_rewrite() -> None:
    path = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path_sql = _sql_literal(path)
    searches = (
        (
            "fts",
            "SELECT id FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 1, prefilter = true) "
            "WHERE lower(text) = 'puppy'",
        ),
        (
            "hybrid",
            "SELECT id FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 1, prefilter = true, use_index = false) "
            "WHERE lower(text) = 'puppy'",
        ),
    )

    connection = _connect()
    try:
        for name, sql in searches:
            connection.execute(sql).fetchall()
            with pytest.raises(Exception, match="complete filter pushdown"):
                relation = connection.sql(sql)
                logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                    relation, f"lance-native-complex-prefilter-{name}"
                )
                physical = logical.to_physical_plan(connection)
                physical.scan_split_batch_map()
    finally:
        connection.close()


def test_global_search_repeated_serialization_preserves_pending_filters() -> None:
    path = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path_sql = _sql_literal(path)
    sql = (
        "SELECT id, _distance FROM lance_vector_search("
        f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
        "k = 1, prefilter = true, use_index = false) "
        "WHERE lower(text) = 'puppy eats food' ORDER BY _distance, id"
    )

    connection = _connect()
    try:
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql(sql), "lance-repeated-search-serialization"
        )
        serialized_once = pickle.dumps(logical)
        serialized_twice = pickle.dumps(pickle.loads(serialized_once))
        serialized_thrice = pickle.dumps(pickle.loads(serialized_twice))

        signatures = []
        for serialized in (serialized_once, serialized_twice, serialized_thrice):
            replay = pickle.loads(serialized)
            physical = replay.to_physical_plan(connection)
            signature = tuple(
                sorted(
                    (
                        str(node_id),
                        tuple(bytes(batch) for batch in batches),
                    )
                    for node_id, batches in physical.scan_split_batch_map().items()
                )
            )
            assert sum(len(batches) for _, batches in signature) == 1
            signatures.append(signature)
        # The split payload authenticates the finalized search state. Losing
        # pending Filter IR during the second serialization changes its digest.
        assert signatures[0] == signatures[1] == signatures[2]

        rejected_sql = (
            "SELECT id FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 3, prefilter = true, use_index = false) "
            "WHERE length(text) > 0"
        )
        rejected = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql(rejected_sql), "lance-repeated-search-rejection"
        )
        rejected_serialized = pickle.dumps(pickle.loads(pickle.dumps(rejected)))
        with pytest.raises(Exception, match="complete filter pushdown"):
            rejected_physical = pickle.loads(rejected_serialized).to_physical_plan(
                connection
            )
            rejected_physical.scan_split_batch_map()
    finally:
        connection.close()


def test_namespace_outer_filters_remain_after_top_k(ray_runner) -> None:
    root = (Path(__file__).resolve().parents[2] / "test/data").resolve()
    source = _sql_literal("postfilter_ns.main.search_test_data")
    calls = (
        (
            "vector",
            "lance_vector_search("
            f"{source}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 1, prefilter = true, use_index = false, "
            "filter = 'label >= 0')",
        ),
        (
            "fts",
            "lance_fts("
            f"{source}, 'text', 'puppy', k = 1, prefilter = true, "
            "filter = 'label >= 0')",
        ),
        (
            "hybrid",
            "lance_hybrid_search("
            f"{source}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 1, prefilter = true, use_index = false)",
        ),
    )

    connection = _connect()
    try:
        connection.execute(f"ATTACH {_sql_literal(root)} AS postfilter_ns (TYPE LANCE)")
        for name, call in calls:
            top = connection.execute(f"SELECT id FROM {call}").fetchone()
            assert top is not None, name
            sql = f"SELECT id FROM {call} WHERE id <> {int(top[0])} ORDER BY id"
            expected = connection.execute(sql).fetchall()
            if name != "hybrid":
                assert expected == [], name
            relation = connection.sql(sql)
            assert _split_count(connection, relation) == 1, name
            assert _run(ray_runner, relation) == expected, name
    finally:
        connection.close()


def test_two_global_search_nodes_keep_independent_singleton_tasks(ray_runner) -> None:
    path = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path_sql = _sql_literal(path)
    sql = (
        "SELECT source, id FROM ("
        "SELECT 'vector' AS source, id FROM lance_vector_search("
        f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
        "k = 3, use_index = false) "
        "UNION ALL "
        "SELECT 'fts' AS source, id FROM lance_fts("
        f"{path_sql}, 'text', 'puppy', k = 10)) "
        "ORDER BY source, id"
    )

    connection = _connect()
    try:
        expected = connection.execute(sql).fetchall()
        relation = connection.sql(sql)
        batches = [
            batch
            for node_batches in _split_batches(connection, relation).values()
            for batch in node_batches
        ]
        assert len(batches) == 2
        assert all(batch.count(b"LGS1") == 1 for batch in batches)
        assert len({batch[batch.index(b"global:") :][:43] for batch in batches}) == 2
        assert _run(ray_runner, relation) == expected
    finally:
        connection.close()


def test_indexed_partial_coverage_global_search_matches_native(
    tmp_path: Path, ray_runner
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path = tmp_path / "distributed-search-indexed.lance"
    path_sql = _sql_literal(path)

    connection = _connect()
    try:
        connection.execute(
            f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
            "(FORMAT LANCE, MODE 'create')"
        )
        connection.execute(
            f"CREATE INDEX vec_idx ON {path_sql} (vec) "
            "USING IVF_FLAT WITH (num_partitions=1, metric_type='l2')"
        )
        connection.execute(f"CREATE INDEX text_idx ON {path_sql} (text) USING INVERTED")
        connection.execute(
            f"CREATE TEMP TABLE lance_search_append AS "
            f"SELECT * FROM {path_sql} ORDER BY id LIMIT 2"
        )
        connection.execute(
            "UPDATE lance_search_append SET id = id + 100, " "text = text || ' puppy'"
        )
        connection.execute(
            f"COPY lance_search_append TO {path_sql} " "(FORMAT LANCE, MODE 'append')"
        )

        searches = (
            (
                "indexed-vector",
                "SELECT id, _distance FROM lance_vector_search("
                f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                "k = 5, nprobs = 1, refine_factor = 2, use_index = true) "
                "ORDER BY _distance, id",
            ),
            (
                "partial-coverage-fts",
                "SELECT id, _score FROM lance_fts("
                f"{path_sql}, 'text', 'puppy', k = 10) "
                "ORDER BY _score DESC, id",
            ),
            (
                "partial-coverage-hybrid",
                "SELECT id, _distance, _score, _hybrid_score "
                "FROM lance_hybrid_search("
                f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                "'text', 'puppy', k = 5, nprobs = 1, refine_factor = 2, "
                "use_index = true, alpha = 0.25, oversample_factor = 4) "
                "ORDER BY _hybrid_score DESC, id",
            ),
        )
        for name, sql in searches:
            expected = connection.execute(sql).fetchall()
            relation = connection.sql(sql)
            assert _split_count(connection, relation) == 1, name
            assert _run(ray_runner, relation) == expected, name
    finally:
        connection.close()


def test_global_search_keeps_snapshot_and_flat_index_plan_after_mutation(
    tmp_path: Path, ray_runner
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path = tmp_path / "distributed-search-frozen-flat.lance"
    path_sql = _sql_literal(path)
    searches = (
        (
            "vector",
            "SELECT id, _distance FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 3, use_index = true) ORDER BY _distance, id",
        ),
        (
            "fts",
            "SELECT id, _score FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 10) "
            "ORDER BY _score DESC, id",
        ),
        (
            "hybrid",
            "SELECT id, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 3, use_index = true, alpha = 0.25) "
            "ORDER BY _hybrid_score DESC, id",
        ),
    )

    connection = _connect()
    plans: list[
        tuple[
            str,
            str,
            bytes,
            tuple[tuple[str, tuple[bytes, ...]], ...],
            list[tuple[object, ...]],
        ]
    ] = []
    try:
        connection.execute(
            f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
            "(FORMAT LANCE, MODE 'create')"
        )
        for name, sql in searches:
            expected = connection.execute(sql).fetchall()
            logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                connection.sql(sql), f"lance-frozen-flat-{name}"
            )
            serialized = pickle.dumps(logical)
            physical = logical.to_physical_plan(connection)
            split_signature = tuple(
                sorted(
                    (
                        str(node_id),
                        tuple(bytes(batch) for batch in batches),
                    )
                    for node_id, batches in physical.scan_split_batch_map().items()
                )
            )
            assert sum(len(batches) for _, batches in split_signature) == 1
            plans.append((name, sql, serialized, split_signature, expected))

        mutator = _connect()
        try:
            mutator.execute(
                "COPY (SELECT -1::BIGINT AS id, -1::INTEGER AS label, "
                "'puppy puppy puppy'::VARCHAR AS text, "
                "'puppy'::VARCHAR AS keywords, "
                "[0.0, 0.0, 0.0, 0.0]::FLOAT[4] AS vec) "
                f"TO {path_sql} (FORMAT LANCE, MODE 'append')"
            )
            mutator.execute(
                f"CREATE INDEX vec_idx ON {path_sql} (vec) "
                "USING IVF_FLAT WITH (num_partitions=1, metric_type='l2')"
            )
            mutator.execute(
                f"CREATE INDEX text_idx ON {path_sql} (text) USING INVERTED"
            )
            latest_results = {
                name: mutator.execute(sql).fetchall() for name, sql in searches
            }
        finally:
            mutator.close()

        assert all(
            any(row[0] == -1 for row in latest_results[name]) for name, _ in searches
        )
        for name, _sql, serialized, split_signature, expected in plans:
            planning_connection = _connect()
            try:
                physical = pickle.loads(serialized).to_physical_plan(
                    planning_connection
                )
                replay_signature = tuple(
                    sorted(
                        (
                            str(node_id),
                            tuple(bytes(batch) for batch in batches),
                        )
                        for node_id, batches in physical.scan_split_batch_map().items()
                    )
                )
            finally:
                planning_connection.close()
            assert replay_signature == split_signature, name
            assert _run_serialized_logical(ray_runner, serialized) == expected, name
    finally:
        connection.close()


def test_global_search_keeps_selected_index_segments_after_replacement(
    tmp_path: Path, ray_runner
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    scenarios = (
        ("vector", ("vector",)),
        ("fts", ("fts",)),
        ("hybrid", ("vector", "fts")),
    )

    for kind, branches in scenarios:
        path = tmp_path / f"distributed-search-replaced-{kind}.lance"
        path_sql = _sql_literal(path)
        connection = _connect()
        try:
            connection.execute(
                f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
                "(FORMAT LANCE, MODE 'create')"
            )
            if "vector" in branches:
                connection.execute(
                    f"CREATE INDEX vec_idx ON {path_sql} (vec) "
                    "USING IVF_FLAT WITH (num_partitions=1, metric_type='l2')"
                )
            if "fts" in branches:
                connection.execute(
                    f"CREATE INDEX text_idx ON {path_sql} (text) USING INVERTED"
                )

            if kind == "vector":
                sql = (
                    "SELECT id, _distance FROM lance_vector_search("
                    f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                    "k = 3, use_index = true) ORDER BY _distance, id"
                )
            elif kind == "fts":
                sql = (
                    "SELECT id, _score FROM lance_fts("
                    f"{path_sql}, 'text', 'puppy', k = 10) "
                    "ORDER BY _score DESC, id"
                )
            else:
                sql = (
                    "SELECT id, _distance, _score, _hybrid_score "
                    "FROM lance_hybrid_search("
                    f"{path_sql}, 'vec', "
                    "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
                    "'text', 'puppy', k = 3, use_index = true, alpha = 0.25) "
                    "ORDER BY _hybrid_score DESC, id"
                )

            expected = connection.execute(sql).fetchall()
            logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                connection.sql(sql), f"lance-index-replacement-{kind}"
            )
            serialized = pickle.dumps(logical)
            physical = logical.to_physical_plan(connection)
            planned_batches = tuple(
                bytes(batch)
                for batches in physical.scan_split_batch_map().values()
                for batch in batches
            )
            assert len(planned_batches) == 1

            old_index_ids = {entry.name for entry in (path / "_indices").iterdir()}
            assert len(old_index_ids) == len(branches)
            if "vector" in branches:
                connection.execute(
                    f"CREATE INDEX vec_idx ON {path_sql} (vec) "
                    "USING IVF_FLAT WITH (num_partitions=1, metric_type='l2', "
                    "replace=true)"
                )
            if "fts" in branches:
                connection.execute(
                    f"CREATE INDEX text_idx ON {path_sql} (text) "
                    "USING INVERTED WITH (replace=true)"
                )
            current_index_ids = {entry.name for entry in (path / "_indices").iterdir()}
            assert old_index_ids < current_index_ids
            assert len(current_index_ids - old_index_ids) == len(branches)

            planning_connection = _connect()
            try:
                replay_physical = pickle.loads(serialized).to_physical_plan(
                    planning_connection
                )
                replay_batches = tuple(
                    bytes(batch)
                    for batches in replay_physical.scan_split_batch_map().values()
                    for batch in batches
                )
            finally:
                planning_connection.close()
            assert replay_batches == planned_batches, kind
            assert _run_serialized_logical(ray_runner, serialized) == expected, kind
        finally:
            connection.close()


def test_global_search_fails_when_the_frozen_snapshot_is_vacuumed(
    tmp_path: Path, ray_runner
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path = tmp_path / "distributed-search-vacuumed.lance"
    path_sql = _sql_literal(path)
    sql = (
        "SELECT id, _distance FROM lance_vector_search("
        f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
        "k = 3, use_index = false) ORDER BY _distance, id"
    )

    connection = _connect()
    try:
        connection.execute(
            f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
            "(FORMAT LANCE, MODE 'create')"
        )
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql(sql), "lance-search-vacuumed-snapshot"
        )
        serialized = pickle.dumps(logical)
        assert (
            sum(
                len(batches)
                for batches in logical.to_physical_plan(connection)
                .scan_split_batch_map()
                .values()
            )
            == 1
        )

        connection.execute(
            "COPY (SELECT -1::BIGINT AS id, -1::INTEGER AS label, "
            "'puppy'::VARCHAR AS text, 'puppy'::VARCHAR AS keywords, "
            "[0.0, 0.0, 0.0, 0.0]::FLOAT[4] AS vec) "
            f"TO {path_sql} (FORMAT LANCE, MODE 'append')"
        )
        cleanup = connection.execute(
            f"VACUUM LANCE {path_sql} WITH ("
            "older_than_seconds = 0, delete_unverified = true, "
            "error_if_tagged_old_versions = false, retain_n_versions = 1)"
        ).fetchone()
        assert cleanup[0] == "cleanup"
        assert '"old_versions":1' in cleanup[2]

        with pytest.raises(
            Exception,
            match="Failed to reopen the frozen distributed Lance search version",
        ):
            _run_serialized_logical(ray_runner, serialized)
    finally:
        connection.close()


def test_global_search_fails_when_a_selected_index_segment_is_removed(
    tmp_path: Path, ray_runner
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path = tmp_path / "distributed-search-missing-index.lance"
    path_sql = _sql_literal(path)
    sql = (
        "SELECT id, _distance FROM lance_vector_search("
        f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
        "k = 3, use_index = true) ORDER BY _distance, id"
    )

    connection = _connect()
    try:
        connection.execute(
            f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
            "(FORMAT LANCE, MODE 'create')"
        )
        connection.execute(
            f"CREATE INDEX vec_idx ON {path_sql} (vec) "
            "USING IVF_FLAT WITH (num_partitions=1, metric_type='l2')"
        )
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql(sql), "lance-search-missing-index"
        )
        serialized = pickle.dumps(logical)
        assert (
            sum(
                len(batches)
                for batches in logical.to_physical_plan(connection)
                .scan_split_batch_map()
                .values()
            )
            == 1
        )

        index_directories = list((path / "_indices").iterdir())
        assert len(index_directories) == 1
        shutil.rmtree(index_directories[0])
        with pytest.raises(Exception, match="index"):
            _run_serialized_logical(ray_runner, serialized)
    finally:
        connection.close()


def test_global_search_rejects_a_same_uri_dataset_recreation(
    tmp_path: Path, ray_runner
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path = tmp_path / "distributed-search-recreated.lance"
    path_sql = _sql_literal(path)
    sql = (
        "SELECT id, _score FROM lance_fts("
        f"{path_sql}, 'text', 'puppy', k = 10) ORDER BY _score DESC, id"
    )

    connection = _connect()
    try:
        connection.execute(
            f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
            "(FORMAT LANCE, MODE 'create')"
        )
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            connection.sql(sql), "lance-search-recreated-dataset"
        )
        serialized = pickle.dumps(logical)
        assert (
            sum(
                len(batches)
                for batches in logical.to_physical_plan(connection)
                .scan_split_batch_map()
                .values()
            )
            == 1
        )

        shutil.rmtree(path)
        replacement = _connect()
        try:
            replacement.execute(
                f"COPY (SELECT * FROM {_sql_literal(source)}) TO {path_sql} "
                "(FORMAT LANCE, MODE 'create')"
            )
        finally:
            replacement.close()

        with pytest.raises(Exception, match="snapshot generation changed"):
            _run_serialized_logical(ray_runner, serialized)
    finally:
        connection.close()


def test_standard_rest_physical_reads_survive_namespace_shutdown(ray_runner) -> None:
    path = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path_sql = _sql_literal(path)
    namespace_source = "standard_rest.main.items"
    namespace_source_sql = _sql_literal(namespace_source)
    query_pairs = (
        (
            "scan",
            f"SELECT id, label FROM {path_sql} ORDER BY id",
            f"SELECT id, label FROM {namespace_source} ORDER BY id",
        ),
        (
            "vector",
            "SELECT id, _distance FROM lance_vector_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 3, use_index = false) ORDER BY _distance, id",
            "SELECT id, _distance FROM lance_vector_search("
            f"{namespace_source_sql}, 'vec', "
            "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], k = 3, use_index = false) "
            "ORDER BY _distance, id",
        ),
        (
            "fts",
            "SELECT id, _score FROM lance_fts("
            f"{path_sql}, 'text', 'puppy', k = 10) "
            "ORDER BY _score DESC, id",
            "SELECT id, _score FROM lance_fts("
            f"{namespace_source_sql}, 'text', 'puppy', k = 10) "
            "ORDER BY _score DESC, id",
        ),
        (
            "hybrid",
            "SELECT id, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{path_sql}, 'vec', [0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "'text', 'puppy', k = 3, use_index = false, alpha = 0.25) "
            "ORDER BY _hybrid_score DESC, id",
            "SELECT id, _distance, _score, _hybrid_score "
            "FROM lance_hybrid_search("
            f"{namespace_source_sql}, 'vec', "
            "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], 'text', 'puppy', "
            "k = 3, use_index = false, alpha = 0.25) "
            "ORDER BY _hybrid_score DESC, id",
        ),
    )

    connection = _connect()
    logical_plans: list[tuple[str, bytes, list[tuple[object, ...]]]] = []
    observed_requests: list[dict[str, object]] = []
    endpoint = ""
    bearer_token = ""
    storage_secret_marker = ""
    planning_error: Exception | None = None
    try:
        direct_results = {
            name: connection.execute(direct_sql).fetchall()
            for name, direct_sql, _ in query_pairs
        }
        with _standard_rest_physical_server(
            f"file:{path}",
            SEARCH_FIXTURE_REST_SCHEMA,
            version=3,
        ) as server:
            (
                endpoint,
                bearer_token,
                storage_secret_marker,
                observed_requests,
                _response_version,
            ) = server
            connection.execute(
                "ATTACH 'safe-namespace-id' AS standard_rest (TYPE LANCE, "
                f"ENDPOINT {_sql_literal(endpoint)}, "
                f"BEARER_TOKEN {_sql_literal(bearer_token)})"
            )
            for name, _direct_sql, namespace_sql in query_pairs:
                relation = connection.sql(namespace_sql)
                try:
                    logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                        relation, f"lance-standard-rest-{name}"
                    )
                except Exception as error:
                    planning_error = error
                    break
                serialized = pickle.dumps(logical)
                for sensitive in (endpoint, bearer_token, storage_secret_marker):
                    assert sensitive.encode() not in serialized

                physical = logical.to_physical_plan(connection)
                split_count = sum(
                    len(batches) for batches in physical.scan_split_batch_map().values()
                )
                assert split_count >= 1, name
                if name != "scan":
                    assert split_count == 1, name
                logical_plans.append((name, serialized, direct_results[name]))

        if planning_error is not None:
            raise AssertionError(
                f"REST planning failed after requests {observed_requests!r}"
            ) from planning_error
        distributed_describes = [
            request
            for request in observed_requests
            if isinstance(request.get("body"), dict)
            and request["body"].get("with_table_uri") is True
            and request["body"].get("load_detailed_metadata") is True
            and request["body"].get("vend_credentials") is False
        ]
        assert len(distributed_describes) >= len(query_pairs)
        assert all(
            "context" not in request["body"] for request in distributed_describes
        )
        assert all(
            request["body"].get("version") == 3 for request in distributed_describes
        )
        request_count_after_plan = len(observed_requests)

        for name, serialized, expected in logical_plans:
            assert _run_serialized_logical(ray_runner, serialized) == expected, name
            assert len(observed_requests) == request_count_after_plan
    finally:
        connection.close()


def test_standard_rest_resolution_stays_on_the_bound_snapshot(
    tmp_path: Path,
) -> None:
    source = (
        Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
    ).resolve()
    path = tmp_path / "rest-snapshot.lance"
    shutil.copytree(source, path)
    path_sql = _sql_literal(path)
    namespace_source = _sql_literal("rest_snapshot.main.items")
    queries = (
        (
            "scan",
            "SELECT id FROM rest_snapshot.main.items ORDER BY id",
        ),
        (
            "vector",
            "SELECT id, _distance FROM lance_vector_search("
            f"{namespace_source}, 'vec', "
            "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 1, use_index = false) ORDER BY _distance, id",
        ),
    )

    connection = _connect()
    observed_requests: list[dict[str, object]] = []
    planning_errors: list[Exception] = []
    try:
        with _standard_rest_physical_server(
            f"file:{path}", SEARCH_FIXTURE_REST_SCHEMA, version=3
        ) as server:
            (
                endpoint,
                bearer_token,
                _storage_secret_marker,
                observed_requests,
                response_version,
            ) = server
            connection.execute(
                "ATTACH 'safe-namespace-id' AS rest_snapshot (TYPE LANCE, "
                f"ENDPOINT {_sql_literal(endpoint)}, "
                f"BEARER_TOKEN {_sql_literal(bearer_token)})"
            )
            # Prime the namespace dataset cache while DescribeTable reports
            # version 3. The regression requires these already-bound relations
            # to retain that snapshot while their distributed DescribeTable
            # calls run after the remote table has advanced.
            bound_relations = [(name, connection.sql(sql)) for name, sql in queries]

            mutator = _connect()
            try:
                mutator.execute(
                    "COPY (SELECT -1::BIGINT AS id, -1::INTEGER AS label, "
                    "'puppy'::VARCHAR AS text, 'puppy'::VARCHAR AS keywords, "
                    "[0.0, 0.0, 0.0, 0.0]::FLOAT[4] AS vec) "
                    f"TO {path_sql} (FORMAT LANCE, MODE 'append')"
                )
            finally:
                mutator.close()
            response_version.value = 4

            for name, relation in bound_relations:
                try:
                    logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                        relation, f"lance-rest-bound-snapshot-{name}"
                    )
                    physical = logical.to_physical_plan(connection)
                    physical.scan_split_batch_map()
                except Exception as error:
                    planning_errors.append(error)
                else:
                    raise AssertionError(
                        f"REST {name} planned from a newer DescribeTable snapshot"
                    )

        assert len(planning_errors) == len(queries)
        distributed_describes = [
            request
            for request in observed_requests
            if isinstance(request.get("body"), dict)
            and request["body"].get("with_table_uri") is True
            and request["body"].get("load_detailed_metadata") is True
            and request["body"].get("vend_credentials") is False
        ]
        assert len(distributed_describes) >= len(queries)
        assert all(
            request["body"].get("version") == 3 for request in distributed_describes
        )
    finally:
        connection.close()


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
        # Keep the plan as a pure Lance source/projection. Every new FTE task
        # observed below therefore owns scan work rather than a downstream
        # aggregate that could mask all splits running on one node.
        relation = connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        assert _split_count(connection, relation) == 16

        baseline_task_ids = set(_settled_ray_fte_create_task_locations())
        assert sorted(_run(ray_runner, relation)) == [
            (row_id,) for row_id in range(65536)
        ]
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


def test_relative_directory_namespace_replay_is_independent_of_planner_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_cwd = tmp_path / "source-cwd"
    planner_cwd = tmp_path / "planner-cwd"
    relative_root = Path("relative-namespace")
    source_path = source_cwd / relative_root / "items.lance"
    source_path.parent.mkdir(parents=True)
    planner_cwd.mkdir(parents=True)

    writer = _connect()
    try:
        _write_dataset(writer, source_path, rows=12)
    finally:
        writer.close()

    monkeypatch.chdir(source_cwd)
    source_connection = _connect()
    try:
        source_connection.execute(
            f"ATTACH {_sql_literal(relative_root)} AS relative_ns (TYPE LANCE)"
        )
        # The first lazy table bind happens only after cwd changes. ATTACH must
        # already have frozen the namespace root, rather than resolving the
        # relative path against this new directory.
        monkeypatch.chdir(planner_cwd)
        assert source_connection.execute(
            "SELECT id FROM relative_ns.main.items ORDER BY id"
        ).fetchall() == [(row_id,) for row_id in range(12)]
        relation = source_connection.sql("SELECT id FROM relative_ns.main.items")
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, "lance-relative-namespace-replay"
        )
        logical_state = logical.__getstate__()
        attached_databases = logical_state[3]["attached_databases"]
        assert len(attached_databases) == 1
        assert str((source_cwd / relative_root).resolve()) in attached_databases[0]
        serialized = pickle.dumps(logical)
    finally:
        source_connection.close()

    planning_connection = _connect()
    physical = None
    try:
        restored = pickle.loads(serialized)
        physical = restored.to_physical_plan(planning_connection)
        split_map = physical.scan_split_batch_map()
        assert sum(len(batches) for batches in split_map.values()) == 4

        rows: list[tuple[int]] = []
        for scan_id, batches in split_map.items():
            for batch in batches:
                worker = _connect()
                cursor = worker.cursor()
                worker_plan = None
                result = None
                try:
                    worker_plan = physical.clone(worker)
                    result = (
                        vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                            cursor,
                            worker_plan,
                            scan_split_batch={str(scan_id): bytes(batch)},
                        )
                    )
                    assert result.completion_status == "ok"
                    rows.extend(
                        (int(value),)
                        for table in result.partition_payloads
                        for value in table.column(0).to_pylist()
                    )
                finally:
                    result = None
                    worker_plan = None
                    cursor.close()
                    worker.close()
        assert sorted(rows) == [(row_id,) for row_id in range(12)]
    finally:
        physical = None
        planning_connection.close()


def test_rest_namespace_snapshot_is_credential_free_and_nonblocking(
    tmp_path: Path,
) -> None:
    path = tmp_path / "local-with-rest-attach.lance"
    source_connection = _connect()
    serialized = None
    endpoint = None
    bearer_token = "rest-snapshot-bearer-secret"
    token_alias = "rest-snapshot-token-alias-secret"
    api_key = "rest-snapshot-api-secret"
    header_secret = "rest-snapshot-header-secret"
    try:
        _write_dataset(source_connection, path)
        with _empty_rest_namespace_server(
            expected_authorization=f"Bearer {bearer_token}"
        ) as endpoint:
            source_connection.execute(
                "ATTACH 'safe-namespace-id' AS rest_shadow (TYPE LANCE, "
                f"ENDPOINT {_sql_literal(endpoint)}, "
                f"TOKEN {_sql_literal(token_alias)}, "
                f"BEARER_TOKEN {_sql_literal(bearer_token)}, "
                f"API_KEY {_sql_literal(api_key)}, "
                f"HEADER {_sql_literal('x-test=' + header_secret)})"
            )
            relation = source_connection.sql(
                f"SELECT id FROM {_sql_literal(path)} ORDER BY id"
            )
            logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                relation, "lance-rest-namespace-snapshot"
            )
            attached_databases = logical.__getstate__()[3]["attached_databases"]
            assert len(attached_databases) == 1
            attach_sql = attached_databases[0]
            assert "vane-internal://lance/rest-namespace-snapshot" in attach_sql
            assert "safe-namespace-id" not in attach_sql
            serialized = pickle.dumps(logical)

        # Planning below happens after the only REST server is gone. Its
        # connection snapshot must therefore replay a local placeholder rather
        # than retrying the original authenticated endpoint.
        assert endpoint is not None
        for sensitive in (
            endpoint,
            token_alias,
            bearer_token,
            api_key,
            header_secret,
        ):
            assert sensitive not in attach_sql
            assert sensitive.encode() not in serialized
    finally:
        source_connection.close()

    planning_connection = _connect()
    physical = None
    try:
        restored = pickle.loads(serialized)
        physical = restored.to_physical_plan(planning_connection)
        split_map = physical.scan_split_batch_map()
        assert sum(len(batches) for batches in split_map.values()) == 4
        planning_connection.execute(
            "ATTACH 'vane-internal://lance/rest-namespace-snapshot' "
            "AS rest_shadow (TYPE LANCE)"
        )
        with pytest.raises(Exception, match="planning snapshot cannot access"):
            planning_connection.execute("SELECT * FROM rest_shadow.main.any_table")
    finally:
        physical = None
        planning_connection.close()


def test_rest_namespace_named_secret_drop_and_recreate_is_recoverable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rest-secret-recovery.lance"
    connection = _connect()
    try:
        _write_dataset(connection, path)
        with _authenticated_rest_namespace_server(f"file:{path}") as server:
            endpoint, authorization, backend_sentinel, observed = server
            token = authorization.removeprefix("Bearer ")
            token_alias = "rest-token-alias-must-not-win"
            api_key = "rest-namespace-api-key-secret"
            create_secret = (
                "CREATE TEMPORARY SECRET rest_namespace_auth ("
                "TYPE LANCE_NAMESPACE, PROVIDER config, "
                f"SCOPE {_sql_literal(endpoint.replace('http://', 'HTTP://'))}, "
                f"TOKEN {_sql_literal(token_alias)}, "
                f"BEARER_TOKEN {_sql_literal(token)}, "
                f"API_KEY {_sql_literal(api_key)})"
            )
            connection.execute(create_secret)
            secret_string = connection.execute(
                "SELECT secret_string FROM duckdb_secrets() "
                "WHERE name = 'rest_namespace_auth'"
            ).fetchone()[0]
            assert "token=redacted" in secret_string
            assert "bearer_token=redacted" in secret_string
            assert "api_key=redacted" in secret_string
            assert token_alias not in secret_string
            assert token not in secret_string
            assert api_key not in secret_string
            connection.execute(
                "ATTACH 'safe-namespace-id' AS rest_secret_ns (TYPE LANCE, "
                f"ENDPOINT {_sql_literal(endpoint)})"
            )
            assert observed == [authorization]

            connection.execute("DROP SECRET rest_namespace_auth")
            with pytest.raises(Exception) as error:
                connection.execute("DESCRIBE rest_secret_ns.main.items")
            message = str(error.value)
            assert "details redacted" in message
            assert backend_sentinel not in message
            assert token_alias not in message
            assert token not in message
            assert api_key not in message
            assert endpoint not in message
            assert None in observed

            # The failed first materialization must not cache a zero-column
            # placeholder. Recreating the same scoped secret allows the same
            # attachment to bind the table without retaining the old value.
            connection.execute(create_secret)
            description = connection.execute(
                "DESCRIBE rest_secret_ns.main.items"
            ).fetchall()
            assert description[0][0:2] == ("id", "BIGINT")
            assert observed[-1] == authorization
    finally:
        connection.close()


def test_remote_object_store_errors_do_not_echo_resolved_secrets() -> None:
    access_key = "object-store-test-access-key"
    secret_key = "object-store-test-secret-key"
    session_token = "object-store-test-session-token"
    bucket = "redaction-test-bucket"
    connection = _connect()
    try:
        with _failing_object_store_server() as server:
            endpoint, backend_sentinel, observed_requests = server
            connection.execute(
                "CREATE TEMPORARY SECRET object_store_redaction ("
                "TYPE LANCE, PROVIDER config, "
                f"SCOPE 'S3A://{bucket}/', "
                f"ACCESS_KEY_ID {_sql_literal(access_key)}, "
                f"SECRET_ACCESS_KEY {_sql_literal(secret_key)}, "
                f"SESSION_TOKEN {_sql_literal(session_token)}, "
                "REGION 'us-east-1', "
                f"ENDPOINT {_sql_literal(endpoint)}, "
                "VIRTUAL_HOSTED_STYLE_REQUEST false, ALLOW_HTTP true, "
                "CLIENT_MAX_RETRIES '0')"
            )
            statements = (f"SELECT * FROM 's3://{bucket}/missing.lance'",)
            for statement in statements:
                with pytest.raises(Exception) as error:
                    connection.execute(statement)
                message = str(error.value)
                assert "<redacted-private-uri>" in message
                for sensitive in (
                    access_key,
                    secret_key,
                    session_token,
                    endpoint,
                    backend_sentinel,
                ):
                    assert sensitive not in message

            connection.execute("DROP SECRET object_store_redaction")
            connection.execute("LOAD httpfs")
            connection.execute(f"SET s3_access_key_id = {_sql_literal(access_key)}")
            connection.execute(f"SET s3_secret_access_key = {_sql_literal(secret_key)}")
            connection.execute(f"SET s3_session_token = {_sql_literal(session_token)}")
            connection.execute("SET s3_region = 'us-east-1'")
            connection.execute(f"SET s3_endpoint = {_sql_literal(endpoint)}")
            connection.execute("SET s3_url_style = 'path'")
            connection.execute("SET s3_use_ssl = false")
            for scheme in ("S3", "S3A", "S3N"):
                request_count = len(observed_requests)
                with pytest.raises(Exception) as error:
                    connection.execute(
                        f"SELECT * FROM '{scheme}://{bucket}/uppercase.lance'"
                    )
                assert len(observed_requests) > request_count
                message = str(error.value)
                assert "<redacted-private-uri>" in message
                for sensitive in (
                    access_key,
                    secret_key,
                    session_token,
                    endpoint,
                    backend_sentinel,
                ):
                    assert sensitive not in message
    finally:
        connection.close()


def test_unsafe_directory_namespace_uri_is_not_serialized(
    tmp_path: Path,
) -> None:
    query_secret = "directory-query-secret"
    fragment_secret = "directory-fragment-secret"
    user = "directory-user"
    password = "directory-password"
    userinfo_directory = tmp_path / "userinfo-namespace"
    query_directory = tmp_path / f"query-namespace?sig={query_secret}"
    fragment_directory = tmp_path / f"fragment-namespace#{fragment_secret}"
    namespace_directories = (
        userinfo_directory,
        query_directory,
        fragment_directory,
    )
    for directory in namespace_directories:
        directory.mkdir()
    for directory in (query_directory, fragment_directory):
        writer = _connect()
        try:
            _write_dataset(writer, directory / "items.lance")
            _write_dataset(writer, directory / "ctas_items.lance")
        finally:
            writer.close()
    unsafe_uris = {
        "userinfo_ns": (f"file://{user}:{password}@localhost{userinfo_directory}"),
        "query_ns": f"file://{query_directory}",
        "fragment_ns": f"file://{fragment_directory}",
        "single_slash_query_ns": f"file:{query_directory}",
        "single_slash_fragment_ns": f"file:{fragment_directory}",
    }
    local_path = tmp_path / "unrelated-to-unsafe-namespace.lance"

    source_connection = _connect()
    serialized = None
    attach_sql = None
    try:
        for catalog_name, unsafe_uri in unsafe_uris.items():
            source_connection.execute(
                f"ATTACH {_sql_literal(unsafe_uri)} AS {catalog_name} " "(TYPE LANCE)"
            )
        for catalog_name in (
            "query_ns",
            "fragment_ns",
            "single_slash_query_ns",
            "single_slash_fragment_ns",
        ):
            assert source_connection.execute(
                f"SELECT count(*) FROM {catalog_name}.main.items"
            ).fetchone() == (12,)
            explain_text = "\n".join(
                str(value)
                for row in source_connection.execute(
                    f"EXPLAIN SELECT id FROM {catalog_name}.main.items"
                ).fetchall()
                for value in row
            )
            for sensitive in (
                query_secret,
                fragment_secret,
                user,
                password,
            ):
                assert sensitive not in explain_text

            # EXPLAIN ANALYZE includes the user's original SQL text. Refer to
            # the table by its safe catalog name so this specifically verifies
            # that runtime operator diagnostics do not expose the backing URI.
            analyzed_text = "\n".join(
                str(value)
                for row in source_connection.execute(
                    f"EXPLAIN ANALYZE SELECT sum(id) " f"FROM {catalog_name}.main.items"
                ).fetchall()
                for value in row
            )
            for sensitive in (
                query_secret,
                fragment_secret,
                user,
                password,
            ):
                assert sensitive not in analyzed_text

            for search_sql in (
                "SELECT * FROM lance_fts("
                f"{_sql_literal(catalog_name + '.main.items')}, "
                "'value', 'value')",
                "SELECT * FROM lance_vector_search("
                f"{_sql_literal(catalog_name + '.main.items')}, "
                "'id', [1.0]::FLOAT[1])",
            ):
                search_explain = "\n".join(
                    str(value)
                    for row in source_connection.execute(
                        "EXPLAIN " + search_sql
                    ).fetchall()
                    for value in row
                )
                for sensitive in (
                    query_secret,
                    fragment_secret,
                    user,
                    password,
                ):
                    assert sensitive not in search_explain

            try:
                unsafe_relation = source_connection.sql(
                    f"SELECT id FROM {catalog_name}.main.items"
                )
                unsafe_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                    unsafe_relation,
                    f"lance-{catalog_name}-must-not-replay",
                )
                unsafe_logical.to_physical_plan(source_connection)
            except Exception as error:
                message = str(error)
                assert "replayable" in message
                for sensitive in (
                    query_secret,
                    fragment_secret,
                    user,
                    password,
                ):
                    assert sensitive not in message
            else:
                raise AssertionError(
                    f"{catalog_name} with private URI components was replayable"
                )
        _write_dataset(source_connection, local_path)
        unrelated_relation = source_connection.sql(
            f"SELECT id FROM {_sql_literal(local_path)}"
        )
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            unrelated_relation, "lance-unsafe-directory-namespace-snapshot"
        )
        attached_databases = logical.__getstate__()[3]["attached_databases"]
        assert len(attached_databases) == len(unsafe_uris)
        attach_sql = "\n".join(attached_databases)
        assert all(
            "vane-internal://lance/directory-planning-snapshot" in statement
            for statement in attached_databases
        )
        serialized = pickle.dumps(logical)
        for sensitive in (
            *unsafe_uris.values(),
            query_secret,
            fragment_secret,
            user,
            password,
        ):
            assert sensitive not in attach_sql
            assert sensitive.encode() not in serialized
    finally:
        source_connection.close()

    # Removing the source namespace proves planning does not list the original
    # URI after replay. The unrelated local scan remains physicalizable.
    for directory in namespace_directories:
        directory.rename(directory.with_name(directory.name + ".moved"))
    planning_connection = _connect()
    physical = None
    try:
        restored = pickle.loads(serialized)
        physical = restored.to_physical_plan(planning_connection)
        assert (
            sum(len(batches) for batches in physical.scan_split_batch_map().values())
            == 4
        )
        planning_connection.execute(
            "ATTACH 'vane-internal://lance/directory-planning-snapshot' "
            "AS unsafe_shadow (TYPE LANCE)"
        )
        with pytest.raises(Exception, match="planning snapshot cannot access"):
            planning_connection.execute("SELECT * FROM unsafe_shadow.main.any_table")
    finally:
        physical = None
        planning_connection.close()


def test_single_slash_uri_replay_and_diagnostics_are_private(
    tmp_path: Path,
) -> None:
    path = tmp_path / "single-slash-file-uri.lance"
    safe_uri = f"file:{path}"
    query_secret = "direct-query-secret"
    fragment_secret = "direct-fragment-secret"
    trailing_query_secret = "direct-trailing-query-secret"
    trailing_fragment_secret = "direct-trailing-fragment-secret"
    connection = _connect()
    physical = None
    try:
        _write_dataset(connection, path)
        safe_relation = connection.sql(
            f"SELECT id FROM {_sql_literal(safe_uri)} ORDER BY id"
        )
        safe_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            safe_relation, "lance-safe-single-slash-file-uri"
        )
        physical = safe_logical.to_physical_plan(connection)
        assert (
            sum(len(batches) for batches in physical.scan_split_batch_map().values())
            == 4
        )
        physical = None

        for index, (delimiter, secret) in enumerate(
            (
                ("?sig=", "literal-local-query-secret"),
                ("#", "literal-local-fragment-secret"),
            )
        ):
            literal_path = Path(
                str(tmp_path / f"literal-private-{index}.lance") + delimiter + secret
            )
            _write_dataset(connection, literal_path)
            assert connection.execute(
                f"SELECT count(*) FROM {_sql_literal(literal_path)}"
            ).fetchone() == (12,)
            explain_text = "\n".join(
                str(value)
                for row in connection.execute(
                    f"EXPLAIN SELECT id FROM {_sql_literal(literal_path)}"
                ).fetchall()
                for value in row
            )
            assert secret not in explain_text
            literal_relation = connection.sql(
                f"SELECT id FROM {_sql_literal(literal_path)} ORDER BY id"
            )
            with pytest.raises(Exception, match="replayable") as error:
                literal_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                    literal_relation, "lance-literal-local-private-path"
                )
                literal_logical.to_physical_plan(connection)
            assert secret not in str(error.value)

        for suffix, secret in (
            (f"?sig={query_secret}", query_secret),
            (f"#{fragment_secret}", fragment_secret),
            (f"/?sig={trailing_query_secret}", trailing_query_secret),
            (f"/#{trailing_fragment_secret}", trailing_fragment_secret),
        ):
            unsafe_uri = safe_uri + suffix
            assert connection.execute(
                f"SELECT count(*) FROM {_sql_literal(unsafe_uri)}"
            ).fetchone() == (12,)
            explain_text = "\n".join(
                str(value)
                for row in connection.execute(
                    f"EXPLAIN SELECT id FROM {_sql_literal(unsafe_uri)}"
                ).fetchall()
                for value in row
            )
            assert secret not in explain_text
            aggregate_explain = "\n".join(
                str(value)
                for row in connection.execute(
                    f"EXPLAIN SELECT sum(id) FROM {_sql_literal(unsafe_uri)}"
                ).fetchall()
                for value in row
            )
            assert secret not in aggregate_explain
            with pytest.raises(Exception, match="replayable") as error:
                relation = connection.sql(
                    f"SELECT id FROM {_sql_literal(unsafe_uri)} ORDER BY id"
                )
                logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                    relation, "lance-private-single-slash-file-uri"
                )
                logical.to_physical_plan(connection)
            assert secret not in str(error.value)

        for index, (delimiter, secret) in enumerate(
            (
                ("?sig=", "literal-missing-query-secret"),
                ("#", "literal-missing-fragment-secret"),
            )
        ):
            missing_path = (
                str(tmp_path / f"literal-missing-{index}.lance") + delimiter + secret
            )
            with pytest.raises(Exception) as error:
                connection.execute(f"SELECT * FROM {_sql_literal(missing_path)}")
            assert secret not in str(error.value)

        for index, (suffix, secret) in enumerate(
            (
                ("?sig=direct-missing-query-secret", "direct-missing-query-secret"),
                ("#direct-missing-fragment-secret", "direct-missing-fragment-secret"),
                (
                    "/?sig=direct-missing-trailing-query-secret",
                    "direct-missing-trailing-query-secret",
                ),
                (
                    "/#direct-missing-trailing-fragment-secret",
                    "direct-missing-trailing-fragment-secret",
                ),
            )
        ):
            missing_uri = f"file:{tmp_path / f'missing-{index}.lance'}{suffix}"
            with pytest.raises(Exception) as error:
                connection.execute(f"SELECT * FROM {_sql_literal(missing_uri)}")
            assert secret not in str(error.value)

        userinfo_user = "direct-userinfo-user"
        userinfo_password = "direct-userinfo-password"
        userinfo_uri = (
            f"file://{userinfo_user}:{userinfo_password}@localhost"
            f"{tmp_path / 'missing-userinfo.lance'}/"
        )
        with pytest.raises(Exception) as error:
            connection.execute(f"SELECT * FROM {_sql_literal(userinfo_uri)}")
        assert userinfo_user not in str(error.value)
        assert userinfo_password not in str(error.value)

        normalized_uri_cases = (
            (
                f" file:{path}?sig=leading-space-secret",
                ("leading-space-secret",),
            ),
            (
                f"file:\t//{path}?sig=embedded-tab-secret",
                ("embedded-tab-secret",),
            ),
            (
                "x:/missing.lance?sig=one-letter-query-secret",
                ("one-letter-query-secret",),
            ),
            (
                "x://one-letter-user:one-letter-password@host/missing.lance",
                ("one-letter-user", "one-letter-password"),
            ),
            (
                "s3://malformed-user:malformed-password@host:notaport/" "missing.lance",
                ("malformed-user", "malformed-password"),
            ),
            (
                " s3://leading-user:leading-password@host:notaport/" "missing.lance ",
                ("leading-user", "leading-password"),
            ),
            (
                "s3:\t//tab-user:tab-password@host:notaport/missing.lance",
                ("tab-user", "tab-password"),
            ),
        )
        for unsafe_uri, secrets in normalized_uri_cases:
            try:
                connection.execute(f"SELECT * FROM {_sql_literal(unsafe_uri)}")
            except Exception as error:
                message = str(error)
            else:
                explain_text = "\n".join(
                    str(value)
                    for row in connection.execute(
                        f"EXPLAIN SELECT id FROM {_sql_literal(unsafe_uri)}"
                    ).fetchall()
                    for value in row
                )
                for secret in secrets:
                    assert secret not in explain_text
                relation = connection.sql(
                    f"SELECT id FROM {_sql_literal(unsafe_uri)} ORDER BY id"
                )
                with pytest.raises(Exception, match="replayable") as error:
                    logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                        relation, "lance-url-normalization-private-uri"
                    )
                    logical.to_physical_plan(connection)
                message = str(error.value)
            for secret in secrets:
                assert secret not in message

        shared_memory_uri = f"shared-memory://lance-{uuid.uuid4()}/process-local.lance"
        _write_dataset(connection, shared_memory_uri)
        assert connection.execute(
            f"SELECT count(*) FROM {_sql_literal(shared_memory_uri)}"
        ).fetchone() == (12,)
        shared_memory_relation = connection.sql(
            f"SELECT id FROM {_sql_literal(shared_memory_uri)} ORDER BY id"
        )
        with pytest.raises(Exception, match="replayable"):
            shared_memory_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
                shared_memory_relation, "lance-process-local-shared-memory-uri"
            )
            shared_memory_logical.to_physical_plan(connection)

        memory_uri = f"memory:/lance-{uuid.uuid4()}"
        connection.execute(
            f"ATTACH {_sql_literal(memory_uri)} AS memory_ns (TYPE LANCE)"
        )
        memory_relation = connection.sql(
            f"SELECT id FROM {_sql_literal(safe_uri)} ORDER BY id"
        )
        memory_logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            memory_relation, "lance-process-local-memory-uri"
        )
        attach_sql = "\n".join(memory_logical.__getstate__()[3]["attached_databases"])
        assert "vane-internal://lance/directory-planning-snapshot" in attach_sql
        assert memory_uri not in attach_sql
        assert memory_uri.encode() not in pickle.dumps(memory_logical)
    finally:
        physical = None
        connection.close()


def test_take_scan_is_split_and_uses_sql_membership_semantics(
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
        expected_ids = (0, 1, 4, 7, 10)
        selected_row_ids = ", ".join(
            str(row_ids_by_id[row_id]) for row_id in selected_ids
        )
        for row_id_column in ("_rowid", "rowid"):
            relation = connection.sql(
                f"SELECT id FROM {_sql_literal(path)} "
                f"WHERE {row_id_column} IN ({selected_row_ids}, NULL) "
                "ORDER BY id"
            )
            assert _split_count(connection, relation) > 1
            assert _run(ray_runner, relation) == [(row_id,) for row_id in expected_ids]

            null_only = connection.sql(
                f"SELECT id FROM {_sql_literal(path)} "
                f"WHERE {row_id_column} IN (NULL)"
            )
            assert _run(ray_runner, null_only) == []

        count_only = connection.sql(
            f"SELECT count(*) AS n FROM {_sql_literal(path)} "
            f"WHERE _rowid IN ({selected_row_ids})"
        )
        assert _split_count(connection, count_only) > 1
        assert _run(ray_runner, count_only) == [(5,)]

        ordinary = connection.sql(
            "SELECT count(*) FROM (VALUES (0), (1), (4), (7), (10), (99)) "
            "AS ordinary(id) WHERE id IN (0, 1, 4, 4, 7, 10, NULL)"
        )
        assert _run(ray_runner, ordinary) == [(5,)]
    finally:
        connection.close()


def test_take_scan_validates_multifragment_addresses_and_deletions(
    tmp_path: Path, ray_runner
) -> None:
    connection = _connect()
    path = tmp_path / "take_deleted.lance"
    try:
        _write_dataset(connection, path)
        row_ids = [
            int(row[0])
            for row in connection.execute(
                f"SELECT _rowid FROM {_sql_literal(path)} ORDER BY id"
            ).fetchall()
        ]
        assert any(row_id >= 1 << 32 for row_id in row_ids)

        connection.execute(
            f"ATTACH {_sql_literal(tmp_path)} AS take_delete_ns "
            "(TYPE LANCE, READ_ONLY false)"
        )
        connection.execute(
            "DELETE FROM take_delete_ns.main.take_deleted " "WHERE id IN (1, 4, 7, 10)"
        )

        invalid_row_ids = (999, 99 << 32)
        row_id_list = ", ".join(str(row_id) for row_id in [*row_ids, *invalid_row_ids])
        relation = connection.sql(
            f"SELECT id FROM {_sql_literal(path)} "
            f"WHERE _rowid IN ({row_id_list}) ORDER BY id"
        )
        assert _split_count(connection, relation) > 1
        assert _run(ray_runner, relation) == [
            (row_id,) for row_id in (0, 2, 3, 5, 6, 8, 9, 11)
        ]
    finally:
        connection.close()


def test_take_scan_handles_stable_row_ids_deletions_and_bounds(ray_runner) -> None:
    # Generated with Lance 9.0.1 using enable_stable_row_ids=true, three rows
    # per file, and one deletion in every fragment. Stable row IDs equal the
    # source ids 0..11, including the deleted positions; next_row_id is 12.
    connection = _connect()
    try:
        live_ids = (0, 2, 3, 5, 6, 8, 9, 11)
        observed = connection.execute(
            f"SELECT id, _rowid FROM {_sql_literal(STABLE_ROW_ID_FIXTURE)} "
            "ORDER BY id"
        ).fetchall()
        assert observed == [(row_id, row_id) for row_id in live_ids]

        requested_row_ids = (0, 1, 5, 7, 11, 12, 1 << 63)
        row_id_list = ", ".join(f"{row_id}::UBIGINT" for row_id in requested_row_ids)
        relation = connection.sql(
            f"SELECT id FROM {_sql_literal(STABLE_ROW_ID_FIXTURE)} "
            f"WHERE _rowid IN ({row_id_list}) ORDER BY id"
        )
        assert _split_count(connection, relation) > 1
        assert _run(ray_runner, relation) == [(0,), (5,), (11,)]
    finally:
        connection.close()


def test_unknown_legacy_deletion_count_falls_back_to_fragment_scan(
    tmp_path: Path, ray_runner
) -> None:
    path = tmp_path / "legacy_deletion_count.lance"
    writer = _connect()
    try:
        _write_dataset(writer, path)
        writer.execute(
            f"ATTACH {_sql_literal(tmp_path)} AS legacy_delete_ns "
            "(TYPE LANCE, READ_ONLY false)"
        )
        writer.execute(
            "DELETE FROM legacy_delete_ns.main.legacy_deletion_count "
            "WHERE id IN (1, 4, 7, 10)"
        )
    finally:
        writer.close()

    _clear_manifest_deletion_counts(path)
    connection = _connect()
    try:
        expected_rows = [(row_id,) for row_id in (0, 2, 3, 5, 6, 8, 9, 11)]
        projected = connection.sql(f"SELECT 1::BIGINT FROM {_sql_literal(path)}")
        assert _split_count(connection, projected) == 4
        assert _run(ray_runner, projected) == [(1,) for _ in expected_rows]

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


def test_empty_dataset_uses_explicit_empty_split(tmp_path: Path, ray_runner) -> None:
    connection = _connect()
    path = tmp_path / "empty.lance"
    try:
        _write_dataset(connection, path, rows=0)
        relation = connection.sql(f"SELECT * FROM {_sql_literal(path)}")
        batches = _split_batches(connection, relation)
        assert sum(len(envelopes) for envelopes in batches.values()) == 1
        elementary_splits = [
            split_id
            for envelopes in batches.values()
            for envelope in envelopes
            for split_id, _, _ in vane.ray_cxx.split_scan_split_batch(envelope)
        ]
        # Vane requires an explicit empty split so worker plans never fall back
        # to the coordinator bind. The sentinel carries no Lance payload and
        # schedules one zero-row worker invocation.
        assert elementary_splits == ["empty"]

        baseline_task_ids = set(_settled_ray_fte_create_task_locations())
        assert _run(ray_runner, relation) == []
        new_task_ids = set(_settled_ray_fte_create_task_locations()) - baseline_task_ids
        assert len(new_task_ids) == 1
    finally:
        connection.close()


def test_empty_assignment_clears_unrestricted_take_and_can_retry(
    tmp_path: Path,
) -> None:
    connection = _connect()
    nonempty_path = tmp_path / "planned-take.lance"
    empty_path = tmp_path / "empty-assignment.lance"
    worker = None
    retry_worker = None
    cursor = None
    retry_cursor = None
    worker_plan = None
    retry_plan = None
    result = None
    retry_result = None
    try:
        _write_dataset(connection, nonempty_path)
        _write_dataset(connection, empty_path, rows=0)
        row_ids = connection.execute(
            f"SELECT _rowid FROM {_sql_literal(nonempty_path)} ORDER BY id LIMIT 4"
        ).fetchall()
        row_id_list = ", ".join(str(row_id[0]) for row_id in row_ids)
        physical = _physical_plan(
            connection,
            connection.sql(
                f"SELECT id FROM {_sql_literal(nonempty_path)} "
                f"WHERE _rowid IN ({row_id_list})"
            ),
        )
        original_split_ids = [
            split_id
            for batches in physical.scan_split_batch_map().values()
            for batch in batches
            for split_id, _, _ in vane.ray_cxx.split_scan_split_batch(batch)
        ]
        assert original_split_ids
        assert all(split_id.startswith("take:") for split_id in original_split_ids)

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
            scan_split_batch={str(next(iter(empty_split_map))): empty_batch},
        )
        assert result.completion_status == "empty"
        assert sum(table.num_rows for table in result.partition_payloads) == 0

        retry_worker = _connect()
        retry_cursor = retry_worker.cursor()
        retry_plan = worker_plan.clone(retry_worker)
        retry_result = vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
            retry_cursor,
            retry_plan,
            scan_split_batch={str(next(iter(empty_split_map))): empty_batch},
        )
        assert retry_result.completion_status == "empty"
        assert sum(table.num_rows for table in retry_result.partition_payloads) == 0
    finally:
        retry_result = None
        result = None
        retry_plan = None
        worker_plan = None
        if retry_cursor is not None:
            retry_cursor.close()
        if cursor is not None:
            cursor.close()
        if retry_worker is not None:
            retry_worker.close()
        if worker is not None:
            worker.close()
        connection.close()


def test_restricted_worker_clone_only_replays_original_assignment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = _connect()
    path = tmp_path / "restricted-retry.lance"
    worker = None
    worker_cursor = None
    worker_plan = None
    retry_worker = None
    retry_cursor = None
    retry_plan = None
    foreign_worker = None
    foreign_cursor = None
    foreign_plan = None
    empty_worker = None
    empty_cursor = None
    empty_plan = None
    result = None
    retry_result = None
    try:
        _write_dataset(connection, path)
        monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
        physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        )
        node_id = str(next(iter(physical.scan_split_batch_map())))
        original_batch = _batch_for_split(physical, "fragment:0")
        different_batch = _batch_for_split(physical, "fragment:1")
        empty_path = tmp_path / "restricted-empty.lance"
        _write_dataset(connection, empty_path, rows=0)
        empty_physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(empty_path)}")
        )
        empty_split_map = empty_physical.scan_split_batch_map()
        empty_batch = bytes(next(iter(empty_split_map.values()))[0])

        worker = _connect()
        worker_cursor = worker.cursor()
        worker_plan = physical.clone(worker)
        result = vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
            worker_cursor,
            worker_plan,
            scan_split_batch={node_id: original_batch},
        )
        assert result.completion_status == "ok"
        assert sum(table.num_rows for table in result.partition_payloads) == 3

        retry_worker = _connect()
        retry_cursor = retry_worker.cursor()
        retry_plan = worker_plan.clone(retry_worker)
        retry_result = vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
            retry_cursor,
            retry_plan,
            scan_split_batch={node_id: original_batch},
        )
        assert retry_result.completion_status == "ok"
        assert sum(table.num_rows for table in retry_result.partition_payloads) == 3

        foreign_worker = _connect()
        foreign_cursor = foreign_worker.cursor()
        foreign_plan = worker_plan.clone(foreign_worker)
        with pytest.raises(
            Exception, match="only replay its original split assignment"
        ):
            vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                foreign_cursor,
                foreign_plan,
                scan_split_batch={node_id: different_batch},
            )

        empty_worker = _connect()
        empty_cursor = empty_worker.cursor()
        empty_plan = worker_plan.clone(empty_worker)
        with pytest.raises(
            Exception, match="only replay its original split assignment"
        ):
            vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                empty_cursor,
                empty_plan,
                scan_split_batch={str(next(iter(empty_split_map))): empty_batch},
            )
    finally:
        retry_result = None
        result = None
        foreign_plan = None
        empty_plan = None
        retry_plan = None
        worker_plan = None
        if foreign_cursor is not None:
            foreign_cursor.close()
        if empty_cursor is not None:
            empty_cursor.close()
        if retry_cursor is not None:
            retry_cursor.close()
        if worker_cursor is not None:
            worker_cursor.close()
        if foreign_worker is not None:
            foreign_worker.close()
        if empty_worker is not None:
            empty_worker.close()
        if retry_worker is not None:
            retry_worker.close()
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
    config = _s3_test_config()

    connection = _connect()
    namespace_root = f"s3://{config['LANCE_S3_BUCKET']}/distributed-scan/{uuid.uuid4()}"
    path = f"{namespace_root}/items.lance"
    try:
        _write_dataset(connection, path)
        relation = connection.sql(f"SELECT id FROM {_sql_literal(path)} ORDER BY id")
        logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation, f"lance-s3-session-replay-{uuid.uuid4()}"
        )
        assert not logical.has_explicit_s3_credentials()
        session_config = logical.session_config()
        assert session_config["AWS_ENDPOINT_URL"] == config["AWS_ENDPOINT_URL"]
        assert session_config["AWS_ACCESS_KEY_ID"] == config["AWS_ACCESS_KEY_ID"]
        assert _split_count(connection, relation) == 4
        assert _run(ray_runner, relation) == [(row_id,) for row_id in range(12)]

        connection.execute("LOAD httpfs")
        connection.execute(
            f"SET s3_access_key_id = {_sql_literal(config['AWS_ACCESS_KEY_ID'])}"
        )
        connection.execute(
            "SET s3_secret_access_key = "
            f"{_sql_literal(config['AWS_SECRET_ACCESS_KEY'])}"
        )
        connection.execute(f"SET s3_region = {_sql_literal(config['AWS_REGION'])}")
        connection.execute(
            f"SET s3_endpoint = {_sql_literal(config['AWS_ENDPOINT_URL'])}"
        )
        connection.execute("SET s3_url_style = 'path'")
        connection.execute("SET s3_use_ssl = false")
        connection.execute(
            f"ATTACH {_sql_literal(namespace_root)} AS lance_s3_ns (TYPE LANCE)"
        )
        namespace_relation = connection.sql(
            "SELECT id FROM lance_s3_ns.main.items ORDER BY id"
        )
        assert _split_count(connection, namespace_relation) == 4
        assert _run(ray_runner, namespace_relation) == [
            (row_id,) for row_id in range(12)
        ]
    finally:
        connection.close()


def test_s3_connection_credentials_override_process_environment(
    tmp_path: Path,
) -> None:
    config = _s3_test_config()
    path = (
        f"s3://{config['LANCE_S3_BUCKET']}/connection-credentials/"
        f"{uuid.uuid4()}/items.lance"
    )

    seed = _connect()
    try:
        _write_dataset(seed, path)
    finally:
        seed.close()

    _run_isolated_s3_credential_check(tmp_path, path, "connection")


def test_s3_profile_credentials_are_resolved_and_replayed_to_workers(
    tmp_path: Path,
) -> None:
    config = _s3_test_config()
    path = (
        f"s3://{config['LANCE_S3_BUCKET']}/profile-credentials/"
        f"{uuid.uuid4()}/items.lance"
    )

    seed = _connect()
    try:
        _write_dataset(seed, path)
    finally:
        seed.close()

    _run_isolated_s3_credential_check(tmp_path, path, "profile")


def test_s3_coordinator_only_type_lance_secret_fails_planning_early(
    tmp_path: Path,
) -> None:
    config = _s3_test_config()
    path = (
        f"s3://{config['LANCE_S3_BUCKET']}/secret-precedence/"
        f"{uuid.uuid4()}/items.lance"
    )

    seed = _connect()
    try:
        _write_dataset(seed, path)
    finally:
        seed.close()

    _run_isolated_s3_credential_check(tmp_path, path, "secret")


def test_s3_missing_credentials_fail_safely_without_leaking_values(
    tmp_path: Path,
) -> None:
    config = _s3_test_config()
    path = (
        f"s3://{config['LANCE_S3_BUCKET']}/missing-credentials/"
        f"{uuid.uuid4()}/items.lance"
    )

    seed = _connect()
    try:
        _write_dataset(seed, path)
    finally:
        seed.close()

    _run_isolated_s3_credential_check(tmp_path, path, "missing")


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


def test_worker_fails_if_the_coordinator_snapshot_was_vacuumed(
    tmp_path: Path,
) -> None:
    connection = _connect()
    path = tmp_path / "vacuumed-snapshot.lance"
    try:
        _write_dataset(connection, path)
        physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        )
        split_map = physical.scan_split_batch_map()
        assert split_map
        node_id, batches = next(iter(split_map.items()))
        split_batch = bytes(batches[0])

        connection.execute(
            "COPY (SELECT 99::BIGINT AS id, 'value-99'::VARCHAR AS value) "
            f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'append')"
        )
        cleanup = connection.execute(
            f"VACUUM LANCE {_sql_literal(path)} WITH ("
            "older_than_seconds = 0, delete_unverified = true, "
            "error_if_tagged_old_versions = false, retain_n_versions = 1)"
        ).fetchone()
        assert cleanup[0] == "cleanup"
        assert '"old_versions":1' in cleanup[2]

        worker = _connect()
        cursor = worker.cursor()
        worker_plan = None
        try:
            worker_plan = physical.clone(worker)
            with pytest.raises(
                Exception, match="Failed to reopen fixed Lance dataset version"
            ):
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


def test_worker_rejects_a_same_version_dataset_replacement(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "replaced.lance"
    try:
        _write_dataset(connection, path)
        physical = _physical_plan(
            connection, connection.sql(f"SELECT id FROM {_sql_literal(path)}")
        )
        split_map = physical.scan_split_batch_map()
        assert split_map
        node_id, batches = next(iter(split_map.items()))
        split_batch = bytes(batches[0])

        shutil.rmtree(path)
        replacement = _connect()
        try:
            _write_dataset(replacement, path, rows=1)
        finally:
            replacement.close()

        worker = _connect()
        cursor = worker.cursor()
        worker_plan = None
        try:
            worker_plan = physical.clone(worker)
            with pytest.raises(Exception, match="generation does not match"):
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


def test_worker_rejects_foreign_split_assignments(tmp_path: Path) -> None:
    connection = _connect()
    path = tmp_path / "split-mode.lance"
    foreign_path = tmp_path / "foreign-split-mode.lance"
    try:
        _write_dataset(connection, path)
        _write_dataset(connection, foreign_path)
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
        foreign_fragment_physical = _physical_plan(
            connection,
            connection.sql(f"SELECT id FROM {_sql_literal(foreign_path)}"),
        )
        fragment_splits = fragment_physical.scan_split_batch_map()
        take_splits = take_physical.scan_split_batch_map()
        foreign_take_splits = foreign_take_physical.scan_split_batch_map()
        fragment_node = next(iter(fragment_splits))
        take_node = next(iter(take_splits))
        take_batch = bytes(next(iter(take_splits.values()))[0])
        foreign_take_batch = bytes(next(iter(foreign_take_splits.values()))[0])
        foreign_fragment_batch = _batch_for_split(
            foreign_fragment_physical, "fragment:0"
        )

        assignments = (
            (
                fragment_physical,
                fragment_node,
                take_batch,
                "Invalid Lance take scan split",
            ),
            (
                take_physical,
                take_node,
                foreign_take_batch,
                "Invalid Lance take scan split",
            ),
            (
                fragment_physical,
                fragment_node,
                foreign_fragment_batch,
                "Invalid Lance fragment scan split",
            ),
        )
        for physical, node_id, split_batch, expected_error in assignments:
            worker = _connect()
            cursor = worker.cursor()
            worker_plan = None
            try:
                worker_plan = physical.clone(worker)
                with pytest.raises(Exception, match=expected_error):
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

        monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
        physical = _physical_plan(
            connection,
            connection.sql(
                f"SELECT id FROM {_sql_literal(path)} " f"WHERE _rowid IN ({full_list})"
            ),
        )
        node_id = str(next(iter(physical.scan_split_batch_map())))
        first_batch = _batch_for_split(physical, "take:0")
        split_ids = sorted(
            int(split_id.removeprefix("take:"))
            for batches in physical.scan_split_batch_map().values()
            for batch in batches
            for split_id, _, _ in vane.ray_cxx.split_scan_split_batch(batch)
        )
        assert len(split_ids) >= 3
        second_begin = split_ids[1]
        third_begin = split_ids[2]
        second_batch = _batch_for_split(physical, f"take:{second_begin}")
        overlapping_begin = second_begin - 1
        overlapping_row_ids = row_ids[
            overlapping_begin : overlapping_begin + third_begin - second_begin
        ]
        overlapping_batch = _rewrite_take_split(
            second_batch,
            old_begin=second_begin,
            new_begin=overlapping_begin,
            row_ids=overlapping_row_ids,
        )
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


@pytest.mark.parametrize("query_kind", ("fragment-scan", "global-search"))
@pytest.mark.parametrize("failure_phase", ("before-output", "after-output"))
def test_real_ray_fte_task_retry_preserves_lance_state_exactly_once(
    tmp_path: Path, ray_retry_runner, query_kind: str, failure_phase: str
) -> None:
    import ray
    import ray.cloudpickle

    path = tmp_path / f"retry_{query_kind.replace('-', '_')}.lance"
    if query_kind == "fragment-scan":
        sql = (
            "SELECT id, "
            "CAST(to_timestamp(1704067200 + id) AS VARCHAR) AS local_time "
            f"FROM {_sql_literal(path)}"
        )
    else:
        sql = (
            "SELECT id, _distance FROM lance_vector_search("
            f"{_sql_literal(path)}, 'vec', "
            "[0.0, 0.0, 0.0, 0.0]::FLOAT[4], "
            "k = 3, use_index = false) ORDER BY _distance, id"
        )
    writer = _connect()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = None
    driver_actor = None
    worker_actors = []
    test_module = sys.modules[__name__]
    ray.cloudpickle.register_pickle_by_value(test_module)
    try:
        if query_kind == "fragment-scan":
            _write_dataset(writer, path)
            writer.execute("SET TimeZone = 'Pacific/Honolulu'")
        else:
            source = (
                Path(__file__).resolve().parents[2] / "test/data/search_test_data.lance"
            ).resolve()
            writer.execute(
                f"COPY (SELECT * FROM {_sql_literal(source)}) "
                f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'create')"
            )
        expected = sorted(writer.execute(sql).fetchall())

        # Materialize the job-scoped query driver and its two real worker actors
        # before installing the class-level gate inside that driver process.
        assert _run(ray_retry_runner, writer.sql("SELECT 1")) == [(1,)]
        _assert_vane_worker_topology(ray, ray_retry_runner)
        client = ray_retry_runner.query_driver_client
        assert client is not None
        driver_actor = client.runner
        workers = ray.get(
            driver_actor.__ray_call__.remote(
                _install_fte_retry_gate,
                failure_phase,
            )
        )
        assert len(workers) == WORKER_COUNT
        worker_actors = [worker["actor"] for worker in workers.values()]

        def run_query() -> list[tuple[object, ...]]:
            connection = _connect()
            try:
                if query_kind == "fragment-scan":
                    connection.execute("SET TimeZone = 'Pacific/Honolulu'")
                return _run(ray_retry_runner, connection.sql(sql))
            finally:
                connection.close()

        future = executor.submit(run_query)

        deadline = time.monotonic() + 45
        attempt0 = None
        while time.monotonic() < deadline:
            snapshot = ray.get(
                driver_actor.__ray_call__.remote(
                    _fte_retry_gate_snapshot,
                    False,
                )
            )
            attempt0 = snapshot["attempt0"]
            if attempt0 is not None:
                break
            if future.done():
                raise AssertionError(
                    f"query completed before a scan attempt was gated: {future.result()!r}"
                )
            time.sleep(0.05)
        assert attempt0 is not None, "timed out waiting for FTE scan attempt 0"
        assert attempt0["attempt_number"] == 0

        attempt0_worker = workers[attempt0["worker_id"]]
        assert attempt0_worker["node_id"] == attempt0["node_id"]
        attempt0_worker_actor = attempt0_worker["actor"]
        deadline = time.monotonic() + 45
        worker_attempt0 = None
        while time.monotonic() < deadline:
            worker_snapshot = ray.get(
                attempt0_worker_actor.__ray_call__.remote(
                    _fte_worker_retry_gate_snapshot
                )
            )
            worker_attempt0 = worker_snapshot["attempt"]
            if worker_attempt0 is not None:
                break
            if future.done():
                raise AssertionError(
                    "query completed before the worker reached its retry gate: "
                    f"{future.result()!r}"
                )
            time.sleep(0.05)
        assert (
            worker_attempt0 is not None
        ), "timed out waiting for the worker retry gate"
        assert worker_attempt0["attempt_number"] == 0
        assert worker_attempt0["task_id"] == attempt0["task_id"]
        assert worker_attempt0["query_id"] == attempt0["query_id"]

        # Commit after attempt 0 captured the immutable plan and before its
        # injected retryable failure is reported. Reopening latest would make
        # the retried result observably different in either query shape.
        if query_kind == "fragment-scan":
            writer.execute(
                f"ATTACH {_sql_literal(tmp_path)} AS retry_ns "
                "(TYPE LANCE, READ_ONLY false)"
            )
            writer.execute(
                "DELETE FROM retry_ns.main.retry_fragment_scan "
                "WHERE id IN (1, 4, 7, 10)"
            )
            assert writer.execute(
                f"SELECT id FROM {_sql_literal(path)} ORDER BY id"
            ).fetchall() == [(row_id,) for row_id in (0, 2, 3, 5, 6, 8, 9, 11)]
        else:
            writer.execute(
                "COPY (SELECT -1::BIGINT AS id, -1::INTEGER AS label, "
                "'puppy puppy puppy'::VARCHAR AS text, "
                "'puppy'::VARCHAR AS keywords, "
                "[0.0, 0.0, 0.0, 0.0]::FLOAT[4] AS vec) "
                f"TO {_sql_literal(path)} (FORMAT LANCE, MODE 'append')"
            )
            assert any(row[0] == -1 for row in writer.execute(sql).fetchall())

        ray.get(
            attempt0_worker_actor.__ray_call__.remote(_release_fte_worker_retry_gate)
        )

        deadline = time.monotonic() + 60
        attempt1 = None
        while time.monotonic() < deadline:
            snapshot = ray.get(
                driver_actor.__ray_call__.remote(
                    _fte_retry_gate_snapshot,
                    False,
                )
            )
            attempt1 = snapshot["attempt1"]
            if attempt1 is not None:
                break
            if future.done():
                raise AssertionError(
                    f"query terminated before FTE retry attempt 1: {future.exception()!r}"
                )
            time.sleep(0.1)
        if attempt1 is None:
            driver_snapshot = ray.get(
                driver_actor.__ray_call__.remote(
                    _fte_retry_gate_snapshot,
                    True,
                )
            )
            worker_snapshots = ray.get(
                [
                    worker_actor.__ray_call__.remote(_fte_worker_retry_gate_snapshot)
                    for worker_actor in worker_actors
                ]
            )
            registry = driver_snapshot["registry"]
            query = registry["queries"].get(attempt0["query_id"])
            retry_partition = None
            if query is not None:
                fragment = query["fragment_executions"].get(attempt0["fragment_id"])
                if fragment is not None:
                    retry_partition = fragment["partitions"].get(
                        str(attempt0["partition_id"])
                    )
            raise AssertionError(
                "timed out waiting for FTE scan attempt 1; "
                f"retry_partition={retry_partition!r}; "
                f"event_scheduler={registry['event_schedulers'].get(attempt0['query_id'])!r}; "
                f"workers={worker_snapshots!r}"
            )

        assert attempt1["attempt_number"] == 1
        assert attempt1["task_id"] == attempt0["task_id"]
        assert attempt1["worker_id"] in workers
        assert attempt1["immutable_fields"] == attempt0["immutable_fields"]
        assert attempt1["splits"] == attempt0["splits"]
        assert attempt1["plan"] == attempt0["plan"]

        snapshot = ray.get(
            driver_actor.__ray_call__.remote(
                _fte_retry_gate_snapshot,
                True,
            )
        )
        worker_snapshot = ray.get(
            attempt0_worker_actor.__ray_call__.remote(_fte_worker_retry_gate_snapshot)
        )
        assert worker_snapshot["installed"] is True
        assert worker_snapshot["failure_injected"] is True
        task_status = worker_snapshot["task_status"]
        assert task_status["state"] == "FAILED"
        assert task_status["failure"]["error_code"] == "GENERIC_INTERNAL_ERROR"
        assert task_status["failure"]["type"] == "RuntimeError"
        assert task_status["failure"]["message"] == (
            "injected retryable Lance scan task failure"
        )
        registry = snapshot["registry"]
        query = registry["queries"][attempt0["query_id"]]
        fragment = query["fragment_executions"][attempt0["fragment_id"]]
        partition = fragment["partitions"][str(attempt0["partition_id"])]
        assert partition["failure_observed"] is True
        assert partition["failure_count"] >= 1
        assert [
            attempt["attempt_number"] for attempt in partition["running_attempts"]
        ] == [1]

        ray.get(
            driver_actor.__ray_call__.remote(
                _release_fte_retry_gate,
                1,
            )
        )
        rows = sorted(future.result(timeout=60))
        ids = [int(row[0]) for row in rows]
        assert rows == expected
        assert len(ids) == len(set(ids)) == len(expected)
        if query_kind == "fragment-scan":
            assert ids == list(range(12))
            assert all(row_id in ids for row_id in (1, 4, 7, 10))
        else:
            assert -1 not in ids
        completed_snapshot = ray.get(
            driver_actor.__ray_call__.remote(
                _fte_retry_gate_snapshot,
                False,
            )
        )
        assert sorted(completed_snapshot["attempts"]) == [0, 1]
    finally:
        primary_error = sys.exception()
        cleanup_errors: list[BaseException] = []
        if worker_actors:
            try:
                ray.get(
                    [
                        worker_actor.__ray_call__.remote(_release_fte_worker_retry_gate)
                        for worker_actor in worker_actors
                    ]
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
        if driver_actor is not None:
            try:
                ray.get(
                    driver_actor.__ray_call__.remote(
                        _release_fte_retry_gate,
                        1,
                    )
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
        if future is not None and not future.done():
            try:
                future.result(timeout=30)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if worker_actors:
            try:
                ray.get(
                    [
                        worker_actor.__ray_call__.remote(_restore_fte_worker_retry_gate)
                        for worker_actor in worker_actors
                    ]
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
        if driver_actor is not None:
            try:
                ray.get(driver_actor.__ray_call__.remote(_restore_fte_retry_gate))
            except BaseException as exc:
                cleanup_errors.append(exc)
        executor.shutdown(
            wait=future is None or future.done(),
            cancel_futures=True,
        )
        try:
            writer.close()
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            ray.cloudpickle.unregister_pickle_by_value(test_module)
        except BaseException as exc:
            cleanup_errors.append(exc)
        if cleanup_errors:
            grouped_errors = [
                *([] if primary_error is None else [primary_error]),
                *cleanup_errors,
            ]
            group_type = (
                ExceptionGroup
                if all(isinstance(error, Exception) for error in grouped_errors)
                else BaseExceptionGroup
            )
            raise group_type("Lance FTE retry test cleanup failed", grouped_errors)
