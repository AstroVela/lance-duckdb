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
sentinel. Vane schedules one no-op worker invocation for that sentinel, with no
Lance split payload, so the worker cannot fall back to the coordinator bind.

The worker plan contains only portable scan state:

- a canonical dataset URI;
- the exact Lance version and snapshot identity selected by the coordinator;
- projection and filter state; and
- the worker's assigned opaque fragment or take splits.

Each worker opens the dataset with its replayed DuckDB session credential state,
checks out the coordinator's exact version when necessary, and validates the
snapshot identity.
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
that absolute local path must refer to the same dataset snapshot on every
worker; otherwise use shared object storage. The current local-filesystem
generation identity contains the Lance manifest version and timestamp, its
transaction-file reference, and the manifest object's size and ETag when
available. It is not a content hash, so a replacement that preserves every
identity component is outside the replacement-detection guarantee.
Lance `memory:` and `shared-memory:` object stores, including both single- and
double-slash URI forms, are process-local. They do not provide a dataset that
can be reopened by an independent worker process, so they cannot form a
replayable worker snapshot.
Directory namespace scans remain replayable when their physical URI is
replayable, including when the coordinator used explicit connection options.
Credentials are never put in worker binds or split payloads. For S3 scans, Vane
replays its captured query session into DuckDB's `s3_*` settings, which the
worker-side adapter translates to Lance storage options. `s3`, `s3a`, and
`s3n` URI schemes are matched case-insensitively and normalized before this
handoff. Credentials may also be captured from a Vane session credential
provider. Static access-key settings must describe a complete credential tuple:
explicitly set
`s3_session_token` to an empty string when the credentials do not use a session
token, so an inherited process token cannot be paired with them. A `TYPE LANCE`
secret remains valid for ordinary local queries, including when the extension
is built with Vane support. Its contract is intentionally not extended to
distributed execution: Vane neither serializes nor discovers DuckDB
secret-catalog entries on workers, and independently creating an equivalent
worker secret is not a supported replay mechanism. Distributed credentials
must instead come from the replayable query-session settings or credential
provider described above. Planning a distributed scan that matches a
coordinator `TYPE LANCE` secret therefore fails early with an actionable
capability error. If the secret-backed namespace remains attached while an
unrelated plan is transported, the connection snapshot uses
an internal credential-free placeholder catalog. Directory roots containing
URI userinfo, a query string, or a fragment use the same placeholder because
those components can contain credentials and are not safe to serialize. This
applies to every recognized URI form, including `file:/path?...` as well as
`file:///path?...`. The placeholder performs no storage I/O and exposes no
namespace tables. A scan of the affected namespace also fails before plan
serialization, even when storage resolution normalized the original URI into
a local path. Errors and `EXPLAIN` output redact these private URI components.
REST namespace authentication options, including the `TOKEN` alias, are also
consumed before Vane captures attached-database SQL. A Vane REST attachment
keeps only a credential-free table-name snapshot from ATTACH and resolves the
current named secret when a listed table is first materialized. Dropping and
replacing that secret is therefore recoverable without retaining its old
value. Detach and reattach to observe tables added out of band after ATTACH.
Vane suppresses REST and storage backend error details whenever they could
repeat resolved options, vended credentials, or presigned URLs.

Create coordinator-local REST credentials with a scoped
`TYPE LANCE_NAMESPACE` secret. The `config` provider accepts `TOKEN` (or
`BEARER_TOKEN`) and `API_KEY`; an explicit `ATTACH` option overrides the
matching secret. `BEARER_TOKEN` is canonical and wins when both token spellings
are present. This secret type is registered only by the Vane build because
official DuckDB retains its existing REST attachment behavior.

The integration suite verifies a temporary shared-credentials profile through
Vane resolution, worker replay, and a real MinIO-backed Lance scan after the
profile environment is removed from shared workers. Role assumption and web
identity are covered compositionally: the pinned Vane revision owns provider
resolution, refresh, and environment scrubbing tests, while this repository
tests the same resolved static credential handoff into Lance. This repository
does not claim a real STS or web-identity service integration test.

The Vane build disables Lance aggregate, limit/offset, and sampling pushdown at
registration time, including for single-node queries made with that wheel.
Vane owns those global operators so a plan cannot apply them independently in
each fragment split. The official DuckDB build retains these pushdowns.

## Boundary

The scan integration covers read-only table scans, including filters, projections,
point-lookups, aggregates, sampling, global limits, empty datasets, and
directory namespace tables. It deliberately excludes vector search, full-text
search, hybrid search, and all index planning. REST namespace query scans are
also excluded until that control plane can provide a stable replayable physical
snapshot. Distributed writes have a separate
[write contract](./vane_distributed_write.md). Distributed replay of
`TYPE LANCE` secret catalog entries is not part of either contract.

The official DuckDB build keeps its existing scan optimizers and behavior when
`LANCE_VANE_DISTRIBUTED` is disabled.

## Validation

The Vane workflow builds both a native ABI compatibility harness and the custom
static wheel at the exact Vane and vcpkg revisions recorded in
`vane-extension.toml`. The Vane-only vcpkg pin does not alter the root manifest
used by official DuckDB builds. The wheel tests run from a fresh non-editable
installation on a local Ray cluster with a coordinator-only head node and two
execution workers. They cover split
parallelism, point lookups, sampling, global limits, fixed snapshots,
replacement detection, empty scans, directory namespaces, and MinIO-backed S3
session replay, including static and temporary-profile credentials.
