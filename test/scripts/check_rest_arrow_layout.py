"""Run native REST ordinary-scan regressions against a local Arrow IPC mock.

Requires pylance 9.0.1 and pyarrow 22.0.0. No external service is needed.
Run: python test/scripts/check_rest_arrow_layout.py build/native/test/unittest
Vane resolves REST tables to physical snapshots and does not use this reader.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from urllib.parse import unquote, urlsplit

import lance
from lance.namespace import DirectoryNamespace
from lance_namespace import DescribeTableRequest
import pyarrow as pa


def narrow_type(data_type):
    if pa.types.is_large_list(data_type):
        return pa.list_(data_type.value_field.with_type(narrow_type(data_type.value_type)))
    if pa.types.is_struct(data_type):
        return pa.struct([field.with_type(narrow_type(field.type)) for field in data_type])
    if pa.types.is_large_string(data_type):
        return pa.string()
    if pa.types.is_large_binary(data_type):
        return pa.binary()
    return data_type


def main():
    binary = Path(sys.argv[1]).resolve()
    root = Path(__file__).resolve().parents[2]
    fixture = root / "test/data/search_arrow_layout_fixture.lance"
    dataset = lance.dataset(fixture)
    namespace = DirectoryNamespace(root=str(fixture.parent), manifest_enabled="false")
    description = namespace.describe_table(
        DescribeTableRequest(id=[fixture.stem], load_detailed_metadata=True, with_table_uri=True)
    ).to_dict()
    tables = {"large", "narrow", "reordered", "extra", "bad_descriptor", "bad_scalar", "missing", "duplicate"}
    queries = []
    errors = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def respond(self, value, status=200):
            is_ipc = isinstance(value, bytes)
            body = value if is_ipc else json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream" if is_ipc else "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            try:
                path = unquote(urlsplit(self.path).path)
                request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
                table_name = request.get("id", [""])[-1]
                if table_name not in tables:
                    self.respond({"error": "unknown table"}, 404)
                elif path.endswith("/describe"):
                    self.respond(description)
                elif path.endswith("/query"):
                    queries.append(table_name)
                    self.respond(self.query(table_name, request))
                else:
                    self.respond({"error": "unexpected operation"}, 404)
            except Exception as error:
                errors.append(str(error))
                self.respond({"error": str(error)}, 500)

        def do_GET(self):
            self.respond({"tables": sorted(tables)})

        def query(self, table_name, request):
            columns = request.get("columns", {}).get("column_names")
            table = dataset.to_table(
                columns=None if table_name == "extra" else columns,
                filter=request.get("filter"),
                limit=request.get("k") or None,
                offset=request.get("offset"),
                with_row_id=request.get("with_row_id", False),
            )
            if table_name == "narrow":
                table = table.cast(pa.schema([field.with_type(narrow_type(field.type)) for field in table.schema]))
            elif table_name in {"reordered", "extra"}:
                table = table.select(list(reversed(table.column_names)))
            elif table_name == "bad_descriptor":
                # A valid Arrow list with the wrong descriptor child type.
                clips = table.column("clips")
                fields = [
                    field.with_type(pa.uint32()) if field.name == "size" else field for field in clips.type.value_type
                ]
                table = table.set_column(
                    table.schema.get_field_index("clips"), "clips", clips.cast(pa.large_list(pa.struct(fields)))
                )
            elif table_name == "bad_scalar":
                table = table.set_column(table.schema.get_field_index("id"), "id", table.column("id").cast(pa.string()))
            elif table_name == "missing":
                table = table.drop(["clips"])
            elif table_name == "duplicate":
                table = table.append_column("id", table.column("id"))
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                # Exercise reader replacement and Arrow ownership across batches.
                writer.write_table(table, max_chunksize=2)
            return sink.getvalue().to_pybytes()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = dict(
            os.environ,
            LANCE_TEST_REST_ARROW_LAYOUT="1",
            LANCE_REST_ARROW_LAYOUT_ENDPOINT=f"http://127.0.0.1:{server.server_port}",
        )
        result = subprocess.run(
            [str(binary), "*namespace_rest_arrow_layout.test", "--reporter", "compact"],
            cwd=root,
            env=env,
            timeout=120,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert not errors, errors
    assert result.returncode == 0, result.returncode
    assert set(queries) == tables, queries
    print(f"REST Arrow layout regressions passed ({len(queries)} query_table requests)")


if __name__ == "__main__":
    main()
