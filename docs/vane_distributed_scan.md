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

Each worker opens the coordinator's exact dataset version directly with its
replayed DuckDB session credential state and validates the snapshot identity.
There is no fallback to the latest version and no extension-specific lease or
cross-worker cache. Appends made after planning therefore do not change the
query snapshot. Worker-backed scans fail explicitly if the required snapshot is
deleted or replaced before it can be reopened.

Within one worker DuckDB database, ordinary scans and `lance_vector_search`,
`lance_fts`, and `lance_hybrid_search` share the same Lance `Session`. Lance's
native index and metadata caches are therefore reused instead of being
reimplemented by the Vane adapter. The worker also keeps one query-lifetime
fixed-snapshot handle cache keyed by resolved storage identity, version, and
generation. Both ordinary scans and all three search functions use that cache
within a query, and query completion releases every cached dataset handle. The
frozen search index plan still selects the exact index segments for each query;
a cache hit never changes index selection or snapshot validation.

The in-memory caches are process-local, so each Ray worker warms independently.
They are most effective when the runner keeps worker actors and their DuckDB
database alive across tasks. The Vane-only database-global settings
`lance_vane_index_cache_size_bytes` and
`lance_vane_metadata_cache_size_bytes` set the per-database capacity before the
first Lance access. Their defaults follow the linked Lance release. They cannot
be changed after the shared Session is created. Configure them with `SET GLOBAL`
on the source connection before its first Lance access. Vane captures those
non-default DuckDB settings with the query connection snapshot and replays them
after loading the statically linked extension on each worker database.

## Global search contract

Vector, full-text, and hybrid search are admitted as global operations. Each
search node produces exactly one authenticated global split, and Vane schedules
that split on one worker. The worker executes Lance's global top-k operation,
including hybrid reranking, against the frozen coordinator snapshot. Vane does
not currently divide one search node into fragment candidates across multiple
workers; that parallel execution model is tracked in
[Issue #9](https://github.com/AstroVela/lance-duckdb/issues/9).

Before split creation, the coordinator freezes the source class, physical URI,
dataset version and generation, schema fingerprint, filter state, search
arguments, and selected index plan. The index plan records the exact index
segments selected for the snapshot and the fragments that require flat search
when an index has partial coverage. A worker validates the complete state and
its assigned split before execution. Retries must reuse the same assignment,
and execution fails closed if the snapshot, dataset generation, schema, or
selected index segments no longer match.

For a standard REST namespace, the coordinator obtains a stable physical table
URI and detailed metadata for the already-bound version. Credentials, vended
storage options, presigned URLs, and REST endpoint details are not serialized.
Workers open that physical snapshot directly and do not contact the namespace
control plane. REST tables that cannot supply this stable, credential-free
physical identity are rejected during planning.

Search filter placement follows native Lance semantics. The search function's
named filter participates in prefiltering, while namespace outer SQL predicates
remain after top-k. A direct full-text or hybrid search with `prefilter = true`
is rejected if every predicate cannot be represented at the same stage as the
native implementation.

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

## Feature matrix

| SQL surface | Vane execution | Current constraints |
| --- | --- | --- |
| Ordinary Lance scan | One split per fragment, scheduled across workers | The physical dataset and frozen snapshot must be replayable by every worker. |
| `rowid`/`_rowid` point lookup | Ordered take splits, scheduled across workers | Duplicate `IN` candidates are normalized; final row order still requires `ORDER BY`. |
| `lance_vector_search` | One authenticated global split on one worker | Global top-k semantics are preserved; cross-worker candidate search is tracked in #9. |
| `lance_fts` | One authenticated global split on one worker | Global score ordering is preserved; cross-worker candidate search is tracked in #9. |
| `lance_hybrid_search` | One authenticated global split on one worker | Vector/text retrieval and reranking remain one global operation; cross-worker execution is tracked in #9. |
| Directory namespace reads | Resolved to a frozen replayable physical URI | Attach-time and planning-time storage state must agree. |
| Standard REST namespace reads | Coordinator resolves the bound version; workers read the physical snapshot | Requires a materialized table URI plus detailed version and schema metadata; workers do not use the REST control plane. |
| Coordinator-only `TYPE LANCE` storage secrets | Rejected for distributed execution | Use Vane's replayable query-session settings or credential provider. Native local queries retain secret support. |
| `memory:` and `shared-memory:` datasets | Rejected for distributed execution | Their process-local state cannot be reopened by an independent worker. |

## Boundary

The scan integration covers read-only table scans, including filters,
projections, point lookups, aggregates, sampling, global limits, empty datasets,
directory namespace tables, vector search, full-text search, hybrid search, and
standard REST tables that provide a stable replayable physical snapshot.
Parallelizing one global search across multiple workers remains outside this
contract and is tracked in
[Issue #9](https://github.com/AstroVela/lance-duckdb/issues/9). Additional
defensive limits for malformed internal `LSI1` collection counts are tracked in
[Issue #10](https://github.com/AstroVela/lance-duckdb/issues/10). Distributed
writes have a separate
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
execution workers. They cover split parallelism, point lookups, sampling,
global limits, fixed snapshots, replacement detection, empty scans, directory
namespaces, and MinIO-backed S3 session replay, including static and
temporary-profile credentials. Advanced-read coverage also compares vector,
full-text, and hybrid results with native execution; verifies singleton search
splits, partial index coverage, frozen selected-index segments, retry identity,
and stale-state rejection; and proves standard REST scans and searches continue
from their physical snapshot after the namespace service is unavailable.
