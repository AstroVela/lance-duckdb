# Vane Distributed Lance Scans

`lance-duckdb` has two independent native build lanes. The default build targets
official DuckDB. The Vane lane builds the extension into a custom `vane-ai`
wheel and enables distributed scan integration with both the
`LANCE_VANE_DISTRIBUTED` C++ definition and the `vane-distributed` Cargo
feature. Artifacts from the two DuckDB C++ ABIs must not be mixed.

## Scan contract

The Vane adapter partitions an ordinary Lance scan into one elementary split
per Lance fragment. Vane remains responsible for batching those splits,
scheduling task attempts, retries, and combining downstream operators. A large
fragment is intentionally not subdivided by this contract.

Point lookups produced from `rowid` or `_rowid` predicates use take splits
instead. Each split preserves its row ID sequence and duplicates, and duplicate
results survive distributed execution. As with all SQL queries, final result
order is defined only when the query has `ORDER BY`. Vane's target split-count
hint controls how the list is divided. An empty dataset produces an explicit
empty distributed source rather than a singleton worker task.

The worker plan contains only portable scan state:

- a canonical dataset URI;
- the exact Lance version and snapshot identity selected by the coordinator;
- projection and filter state; and
- the worker's assigned opaque fragment or take splits.

Each worker opens the dataset with its local DuckDB secrets, checks out the
coordinator's exact version when necessary, and validates the snapshot identity.
There is no fallback to the latest version and no extension-specific lease or
cross-worker cache. Appends made after planning therefore do not change the
query snapshot. Worker-backed scans fail explicitly if the required snapshot is
deleted or replaced before it can be reopened.

## SQL semantics

Projection and Lance filter pushdown remain enabled inside each split. Global
operators stay owned by Vane:

- `LIMIT` and `OFFSET` are applied after all split results are combined;
- sampling is performed by Vane's distributed sampling operators; and
- aggregates use Vane's normal distributed aggregate plan.

This keeps fragment parallelism without changing the SQL result. Directory
namespace tables are supported when they resolve to a replayable physical URI.
Relative local paths are made absolute at bind time. In a multi-host cluster,
that absolute local path must be visible at the same location on every worker;
otherwise use shared object storage. Process-local `memory://` datasets and
directory namespace attachments with explicit connection options are not
distributed. Credentials are never put in worker binds or split payloads;
workers resolve them locally. For S3 scans, Vane replays its captured query
session into DuckDB's `s3_*` settings, which the worker-side adapter translates
to Lance storage options when no more-specific `TYPE LANCE` secret is present.

## Boundary

This integration covers read-only table scans, including filters, projections,
point-lookups, aggregates, sampling, global limits, empty datasets, and
directory namespace tables. It deliberately excludes vector search, full-text
search, hybrid search, and all index planning. REST namespace query scans are
also excluded until that control plane can provide a stable replayable physical
snapshot. Distributed writes are a separate feature.

The official DuckDB build keeps its existing scan optimizers and behavior when
`LANCE_VANE_DISTRIBUTED` is disabled.

## Validation

The Vane workflow builds both a native ABI compatibility harness and the custom
static wheel at the exact revision recorded in `vane-extension.toml`. The wheel
tests run from a fresh non-editable installation on a local Ray cluster with a
coordinator-only head node and two execution workers. They cover split
parallelism, point lookups, sampling, global limits, fixed snapshots,
replacement detection, empty scans, directory namespaces, and MinIO-backed S3
session replay.
