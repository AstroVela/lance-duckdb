"""Run deterministic MinIO FileHandle tests and record backing HTTP ranges.

Requires pylance==9.0.1, pyarrow==22.0.0 and boto3. Start MinIO separately and
set LANCE_TEST_S3=1. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY upload fixtures;
LANCE_BLOB_S3_DATASET_KEY/SECRET and LANCE_BLOB_S3_EXTERNAL_KEY/SECRET optionally
select two restricted users for the native reader. The default endpoint is
http://127.0.0.1:9000 and the default bucket is lance-test.
"""

import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

from run_blob_filesystem_tests import fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--endpoint", default=os.getenv("LANCE_S3_ENDPOINT", "http://127.0.0.1:9000"))
    parser.add_argument("--bucket", default="lance-test")
    args = parser.parse_args()
    if os.getenv("LANCE_TEST_S3") != "1":
        raise SystemExit("Set LANCE_TEST_S3=1 to run MinIO integration tests")
    endpoint = urlsplit(args.endpoint)
    if endpoint.scheme != "http":
        raise SystemExit("The recording proxy expects a local HTTP MinIO endpoint")
    client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    buckets = {item["Name"] for item in client.list_buckets()["Buckets"]}
    if args.bucket not in buckets:
        client.create_bucket(Bucket=args.bucket)
    records = []
    record_lock = threading.Lock()

    class Proxy(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def forward(self):
            backend = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=60)
            try:
                backend.request(self.command, self.path, headers=dict(self.headers))
                response = backend.getresponse()
                body = response.read()
                with record_lock:
                    records.append(
                        {
                            "method": self.command,
                            "path": self.path.split("?")[0],
                            "range": self.headers.get("Range"),
                            "status": response.status,
                            "bytes": len(body),
                        }
                    )
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in {"transfer-encoding", "connection", "content-length"}:
                        self.send_header(name, value)
                length = response.getheader("Content-Length") if self.command == "HEAD" else str(len(body))
                if length is not None:
                    self.send_header("Content-Length", length)
                self.end_headers()
                if body:
                    self.wfile.write(body)
            finally:
                backend.close()

        do_GET = forward
        do_HEAD = forward

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    measurements = {}
    try:
        with tempfile.TemporaryDirectory(prefix="lance-s3-blob-") as directory:
            prefix = "blob-pr3"
            root = f"s3://{args.bucket}/{prefix}"
            path, external, _ = fixture(directory, external_uri=f"{root}/external-a.bin")
            for file in path.rglob("*"):
                if file.is_file():
                    client.upload_file(
                        str(file), args.bucket, f"{prefix}/dataset.lance/{file.relative_to(path).as_posix()}"
                    )
            for name in ("external-a.bin", "external-b.bin"):
                client.upload_file(str(external), args.bucket, f"{prefix}/{name}")
            process = subprocess.Popen(
                [
                    str(args.binary.resolve()),
                    "--remote",
                    f"{root}/dataset.lance",
                    f"http://127.0.0.1:{server.server_port}",
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
            )
            try:
                for line in process.stdout:
                    print(line, end="", flush=True)
                    if not line.startswith("MUTATE "):
                        continue
                    operation = line.strip().split()[1]
                    with record_lock:
                        if operation.endswith("_begin"):
                            records.clear()
                        else:
                            measurements[operation.removesuffix("_end")] = list(records)
                    process.stdin.write("OK\n")
                    process.stdin.flush()
                if process.wait():
                    print(json.dumps(records[-20:], indent=2))
                    raise RuntimeError("Remote Blob filesystem tests failed")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        assert not measurements["denied"], "denial issued backing HTTP requests"
        summary = {}
        for name in ("buffered", "direct", "concurrent", "sparse"):
            ranges = [record for record in measurements[name] if record["method"] == "GET"]
            assert ranges and all(record["range"] for record in ranges), "payload fetch without Range"
            summary[name] = {
                "requests": len(ranges),
                "bytes": sum(record["bytes"] for record in ranges),
                "largest_response": max(record["bytes"] for record in ranges),
            }
            assert summary[name]["largest_response"] <= 1024 * 1024, summary
        assert summary["buffered"]["requests"] < summary["direct"]["requests"], summary
        assert summary["buffered"]["largest_response"] < 2 * 1024 * 1024, summary
        print(json.dumps(summary, indent=2))
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
