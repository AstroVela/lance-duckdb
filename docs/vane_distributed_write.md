# Vane Distributed Lance Writes

The Vane build supports distributed `INSERT INTO`, `CREATE TABLE AS`, `UPDATE`,
and `DELETE` operations for replayable Lance directory-namespace tables. The
integration is compiled only when `LANCE_VANE_DISTRIBUTED` and the
`vane-distributed` Cargo feature are enabled. Official DuckDB builds retain the
existing native Lance operators and do not compile or register the Vane write
adapter.

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

## Mutation protocol

An `UPDATE` or `DELETE` freezes the scan's exact Lance version, snapshot
generation, Arrow schema (including nested nullability), source fragment set,
and row-ID mode. Workers receive only fragment-owned scan splits. Every worker
reopens the frozen generation and validates each input row ID against the
authorized source fragments and current deletion state. Stable row IDs are
resolved through Lance's row-ID index; physical row addresses are validated as
live offsets before any artifact is created.

A `DELETE` produces fragment deletion state. An `UPDATE` evaluates the bound
DuckDB `SET` expressions against rows taken from the frozen dataset, writes
replacement fragments, preserves inline stable row IDs, and produces the
matching source deletions. Each worker returns a strict uncommitted Lance
transaction carrying the operation, query, task-attempt, mutation kind, schema,
and affected-row identities. The worker object-store adapter records the exact
`data/` and `_deletions/` objects created by that attempt, so failures before a
transaction can be encoded clean only that attempt's objects without a shared
directory diff.

The coordinator rejects duplicate transactions, multiple selected attempts for
one logical task, overlapping source fragments, stale or unauthorized fragment
metadata, and any version, generation, schema, fragment, or index change. It
renumbers independently written replacement fragments, combines all selected
changes into one Lance `UPDATE` or `DELETE` transaction, and calls the commit
path exactly once with retries disabled. A zero-match mutation creates no
artifacts and no dataset version. Mutation attempt manifests use the same
winner/loser ownership lifecycle as append writes.

Compaction, optimize, merge, and other maintenance operations remain
unsupported by the distributed adapter. Vane selection fails closed instead of
falling back to coordinator-local mutation until those operations can use the
same source-and-destination fragment lifecycle.

Before a coordinator commit starts, abort and validation failures perform
best-effort cleanup of the exact data files named by the rejected transactions
and durable attempt manifests. Cleanup never deletes a file referenced by the
current Lance manifest. After a successful commit, the coordinator releases
selected cleanup manifests without attempting any artifact deletion. This
preserves files referenced by the committed version even if a concurrent
overwrite has already made that version historical. If commit execution has
started and its outcome is unknown, selected manifests and files are retained
for conservative recovery.

Append worker finalization errors rely on Lance's native uncommitted-write
failure contract. Mutation workers additionally track every attempt-owned data
or deletion object and remove it on any known pre-result failure;
`skip_auto_cleanup` applies only to post-commit version cleanup.

Distributed Lance writes require DuckDB auto-commit mode. The provider rejects
explicit transactions before freezing a worker plan, preparing a CTAS target,
or publishing an INSERT commit. Native DuckDB execution retains the existing
Lance transaction lifecycle for `BEGIN`, `COMMIT`, and `ROLLBACK`.

Every worker validates the bound Arrow field names and types against the frozen
target before accepting a batch. Arbitrary Arrow casts are not part of the
distributed append contract, so a stale catalog schema cannot silently convert
data after concurrent schema evolution. Explicit representation normalization
is retained for DuckDB's Utf8/LargeUtf8, Binary/LargeBinary, and nested list
offset variants. The existing variable-list to fixed-size-list vector
conversion is seeded from the target dimension. Float16 continues to be widened
for reads only; the SQL write guard rejects that coerced schema instead of
narrowing values implicitly.

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
which correspond to DuckDB `INSERT INTO`, directory-namespace
`CREATE TABLE AS`, `UPDATE`, and `DELETE`. Column names, logical types,
nullability, storage-version selection, target version, target generation, and
task identities are checked before commit.

`CREATE TABLE AS IF NOT EXISTS` and `CREATE OR REPLACE TABLE AS` are not
distributed. If Vane selects the extension write provider for those forms, the
adapter rejects them explicitly. `MERGE`, schema evolution, index changes,
maintenance, and REST namespace writes continue to use their existing native
paths and are outside this distributed write contract.

## Validation

The Vane workflow has a dedicated two-worker distributed-write-and-mutation
job. It installs the statically linked wheel into a fresh environment and
verifies local shared storage plus MinIO-backed S3. The contract tests cover
distributed `INSERT`, `CREATE TABLE AS`, `UPDATE`, and `DELETE`; zero-match
mutations; multiple source fragments; stable row IDs; exact readback; worker
failure cleanup; stale-plan rejection; worker-result metadata; exact row
counts; single-version coordinator commits; failed-CTAS prepared-target
retention and explicit cleanup before retry; attempt-manifest removal; explicit
transaction rejection; and native single-node fallback. Rust tests exercise
frozen target validation, winner/loser attempt cleanup, duplicate selection,
and post-commit live-file protection. The ordinary DuckDB build and test lane
remains independent of the Vane ABI.
