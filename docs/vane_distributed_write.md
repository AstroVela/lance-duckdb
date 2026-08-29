# Vane Distributed Lance Writes

The Vane build supports distributed `INSERT INTO` and `CREATE TABLE AS`
operations for replayable Lance directory-namespace tables. The integration is
compiled only when `LANCE_VANE_DISTRIBUTED` and the `vane-distributed` Cargo
feature are enabled. Official DuckDB builds retain the existing native Lance
operators and do not compile or register the Vane write adapter.

## Commit protocol

The coordinator creates one operation UUID and freezes the target before any
worker writes data. An `INSERT` freezes the current Lance version and snapshot
generation. A `CREATE TABLE AS` operation first creates an empty version-one
dataset whose Lance transaction UUID is the operation UUID, then freezes that
prepared generation.

Each selected Vane task opens that exact generation, writes data files, and
returns an uncommitted Lance append transaction through Vane's opaque extension
write result. The transaction records the operation, query, and task-attempt
identities. Its declared rows, bytes, and data-file artifacts are validated on
the coordinator. Task retries may produce new attempt results, but Vane selects
only one successful attempt for each logical task.

Before a successful worker returns its result, it atomically publishes an
operation-scoped cleanup manifest beside the dataset. This transfers ownership
of the uncommitted files out of worker memory. After Vane selects task results,
its attempt-finalization barrier has already quiesced every peer attempt. The
coordinator validates that every selected non-empty attempt has a manifest and
deletes the files and manifests of successful retry or speculation losers.
Selected manifests remain in place until the coordinator knows the commit
outcome. Manifest paths use Lance transaction UUIDs, and manifest contents are
validated against the frozen version and operation, query, and attempt
identities before any cleanup occurs.

After every selected result has been validated, the coordinator uses Lance's
batch transaction commit API to publish all append transactions as one new
dataset version. Lance commit retries are disabled at this boundary: a target
generation change fails the operation instead of silently rebasing it. Empty
writes do not create an append version. An empty `CREATE TABLE AS` therefore
keeps its prepared version-one dataset.

Before a coordinator commit starts, abort and validation failures perform
best-effort cleanup of the exact data files named by the rejected transactions
and durable attempt manifests. Cleanup never deletes a file referenced by the
current Lance manifest. After a successful commit, the same live-file check
preserves selected data files while their cleanup manifests are removed. If
commit execution has started and its outcome is unknown, selected manifests
and files are retained for conservative recovery.
A failed `CREATE TABLE AS` retains its prepared target. Lance does not expose a
generation-conditional table deletion primitive, so checking the empty
version-one generation and then recursively deleting the dataset would race
with another client committing a live version. Explicitly drop or otherwise
clean the retained target before retrying CTAS. A coordinator commit error is
reported as an unknown outcome only after commit execution starts and is never
retried automatically by the extension. A known pre-commit failure cleans all
attempt artifacts and manifests.

## Storage and credential boundary

Workers must be able to reopen the same physical dataset URI. Relative local
directory roots are canonicalized; in a multi-host cluster they must name a
filesystem shared by every worker. Shared object storage such as S3 is the
recommended production target.

Credentials are not embedded in the worker bind, transaction envelope, or
artifact metadata. S3 settings must come from Vane's replayable query-session
state or credential provider, and must match the settings captured when the
directory namespace was attached. Distributed writes reject coordinator-only
`TYPE LANCE` secrets, URIs with private userinfo/query/fragment components,
process-local memory stores, and REST namespace tables. Local/native queries in
the same Vane wheel retain the ordinary secret and REST behavior.

## Supported SQL surface

The distributed path is selected through Vane's standard relation write APIs,
which correspond to DuckDB `INSERT INTO` and directory-namespace
`CREATE TABLE AS`. Column names, logical types, nullability, storage-version
selection, target version, target generation, and task identities are checked
before commit.

`CREATE TABLE AS IF NOT EXISTS` and `CREATE OR REPLACE TABLE AS` are not
distributed. If Vane selects the extension write provider for those forms, the
adapter rejects them explicitly. Other Lance mutations, including `UPDATE`,
`DELETE`, `MERGE`, schema evolution, index changes, and REST namespace writes,
continue to use their existing native paths and are outside this distributed
write contract.

## Validation

The Vane workflow has a dedicated two-worker distributed-write job. It installs
the statically linked wheel into a fresh environment and verifies local shared
storage plus MinIO-backed S3. The contract tests cover distributed `INSERT`,
distributed `CREATE TABLE AS`, empty input, worker-result metadata, exact row
counts, single-version coordinator commits, failed-CTAS prepared-target
retention and explicit cleanup before retry, attempt-manifest removal, and
native single-node write fallback. Rust tests also exercise winner retention,
successful loser cleanup, and post-commit live-file protection. The ordinary
DuckDB build and test lane remain independent of the Vane ABI.
