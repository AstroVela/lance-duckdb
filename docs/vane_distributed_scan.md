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
instead. SQL membership candidates are normalized before planning, so repeated
values in an `IN` predicate do not duplicate input rows. Each split preserves
the resulting row ID sequence. As with all SQL queries, final result order is
defined only when the query has `ORDER BY`. Vane's target split-count hint
controls how the list is divided. An empty dataset produces an explicit empty
distributed source rather than a singleton worker task.

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
that absolute local path must refer to the same manifest inode, modification
time, and contents on every worker; otherwise use shared object storage.
Lance `memory://` object stores do not provide a dataset that can be reopened by
an independent scan bind, so they cannot form a replayable worker snapshot.
Directory namespace scans remain replayable when their physical URI is
replayable, including when the coordinator used explicit connection options.
Credentials are never put in worker binds or split payloads. For S3 scans, Vane
replays its captured query session into DuckDB's `s3_*` settings, which the
worker-side adapter translates to Lance storage options. Credentials may also
be provisioned independently in every worker environment. Static access-key
settings must describe a complete credential tuple: explicitly set
`s3_session_token` to an empty string when the credentials do not use a session
token, so an inherited process token cannot be paired with them. A temporary
`TYPE LANCE` secret created only on the coordinator remains valid for local
queries, but Vane does not replay that secret catalog entry to workers.
Planning a distributed scan that matches such a secret therefore fails early
with an actionable capability error.

The Vane build disables Lance aggregate, limit/offset, and sampling pushdown at
registration time, including for single-node queries made with that wheel.
Vane owns those global operators so a plan cannot apply them independently in
each fragment split. The official DuckDB build retains these pushdowns.

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
