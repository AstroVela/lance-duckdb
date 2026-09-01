# Vane Frozen Snapshot Benchmark

This benchmark measures the coordinator-frozen Lance snapshot contract used by
Vane distributed scans and searches. It reports the 1, 8, and 32 worker matrix
required by Issue #13.

The measured fields are:

- coordinator SQL/bind/physical planning latency;
- execution latency from submission of the already-bound logical plan through
  receipt of the final partition;
- serialized logical and physical plan sizes;
- split count and total split payload bytes; and
- manifest `HEAD` and `GET` counts during planning and execution.

Execution timing includes Vane driver-side physical materialization because
that work is part of submitting a transported logical plan. Planning is also
reported independently; the two timings are not subtracted from one another.

## Dataset

Prepare a shared S3-compatible Lance dataset with at least 32 fragments and the
following columns:

- `id`;
- `vec FLOAT[4]` with an IVF index; and
- `text VARCHAR` with an inverted index.

The repository fixture `test/data/search_test_data.lance` has the expected
schema and can be copied repeatedly into a benchmark dataset. Use small
`MAX_ROWS_PER_FILE` values so an ordinary scan can occupy every worker.

The benchmark does not create or mutate the dataset. Dataset preparation is
intentionally outside the timed process. The ordinary scan projects `id`
(configurable with `--id-column`) instead of using a metadata-only aggregate,
so fragment split count and payload size remain observable. Search workloads
project only `id` and their score columns, keeping unrelated result-column
transfer out of the snapshot acquisition measurement.

## Run

Install a Vane wheel containing this checkout's statically linked Lance
extension, Ray, and PyArrow. Then run against MinIO or another S3-compatible
service:

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://127.0.0.1:9000

python3 benches/vane_frozen_snapshot/benchmark.py \
  --dataset-uri s3://lance-vane-test/frozen-snapshot-benchmark.lance \
  --worker-counts 1,8,32 \
  --cold-repeats 3 \
  --warm-repeats 5
```

All four workloads run by default. Use, for example,
`--workloads scan,vector` for a focused diagnostic run; committed acceptance
results should retain the default `scan,vector,fts,hybrid` set.

Each cold sample creates a fresh Vane runner and DuckDB connection. Warm
samples reuse them. "Cold" therefore describes the process-local Session and
planning caches; it does not flush the operating-system page cache or any
object-store cache.

When `--upstream-endpoint` (or `AWS_ENDPOINT_URL`) is present, the runner starts
a local read-only counting proxy and sends all benchmark reads through it.
Manifest request counts are therefore observations at the object-store HTTP
boundary, not inferred values. The proxy must be reachable by every Ray worker;
the default local-cluster matrix satisfies that requirement.

The script initializes a local Ray cluster and binds its counting proxy to the
loopback interface. Measuring an externally managed remote Ray cluster requires
adapting the runner and exposing an equivalent proxy on an address reachable by
all nodes. Without the proxy, latency and size fields remain valid and
`request_counts_measured` is `false`.

Raw samples and grouped means/medians are written to
`build/vane-frozen-snapshot-benchmark/results.json` by default. Generated
results are not committed because host, object store, dataset size, and network
topology materially affect the numbers.

## Expected request contract

On a backend that supplies a reliable immutable ETag:

- workers perform zero manifest content `GET` requests after receiving a frozen
  plan;
- each worker that participates in an ordinary scan performs at most one
  manifest identity `HEAD` per query;
- a current global vector, FTS, or hybrid search uses one authenticated split,
  so only its selected worker performs that identity check; and
- warm Session cache hits must not change the frozen index segments selected by
  the coordinator.

Backends without reliable immutable object identity deliberately perform a
content validation read and should be benchmarked separately.
