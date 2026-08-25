# Codex Implementation Brief: Remove Vane-Lance Custom Leases

## Status and authority

This document directs Codex to remove the repository-specific, storage-backed
lease protocol from the Lance integration while preserving the custom Vane
wheel, distributed reads and writes, and the existing Lance SQL surface.

This decision supersedes the snapshot/mutation/vacuum lease design and the
"Authorized host-lifecycle amendment" in
`docs/vane-static-wheel-implementation.md`. In particular, this task does not
authorize or require a Vane source change.

Read `AGENTS.md` and the relevant development guides before acting. Preserve
unrelated work. Do not commit, push, publish, alter a remote repository, or open
a pull request unless the user explicitly asks.

## Decision

Vane-Lance will follow the same plan-capture boundary used by Iceberg PR #2:
distributed plan capture serializes pure data and creates no persistent reader
coordination record.

Vane-Lance does not coordinate in-flight operations with external `VACUUM`,
`DROP`, version cleanup, or direct object deletion. Those operations are
permitted, and an affected query may fail if its fixed snapshot files disappear.
The caller or deployment owns concurrency policy.

The implementation must not:

- create `_vane_leases` in a Lance dataset;
- create snapshot, mutation, or vacuum lease records;
- serialize lease or handoff tokens;
- wait for active Vane operations before running `VACUUM` or `DROP`;
- require a maintenance window as a product rule;
- require Vane plan-capture commit/abort callbacks; or
- replace the removed protocol with another repository-specific lock, marker,
  heartbeat, timeout, or lease service.

This is an intentional product boundary, not a temporary omission to hide in
tests.

## What this decision does not remove

Removing custom leases does not mean removing normal correctness mechanisms.
Keep all of the following:

- an exact bind-time Lance dataset version and generation identity;
- pure-data scan/search bind state and opaque task payloads;
- strict worker validation of version, generation, table identity, and task-set
  identity;
- global top-k correctness for vector, full-text, and hybrid search;
- Vane's generic `ExtensionWriteTaskProvider` and distributed write callbacks;
- one coordinator-owned catalog/dataset commit;
- stable operation and task-attempt identities;
- retry-safe staging and cleanup of definitely uncommitted worker artifacts;
- Lance's native MVCC and optimistic commit-conflict behavior;
- fail-closed handling of an unknown commit outcome; and
- secret redaction and worker-local credential resolution.

Never recover a missing fixed version by reopening the latest dataset version.
That would silently change query results. Return a clear missing-version,
missing-file, or generation-mismatch error instead.

## Product behavior after the change

### Reads

```text
source bind selects version V and generation G
                    |
                    v
serialize V, G, schema, filters, and split identity as pure data
                    |
                    v
Ray Driver and workers reopen exactly V/G
                    |
         +----------+----------+
         |                     |
     files exist          files were removed
         |                     |
         v                     v
   execute normally       fail explicitly
```

Planning and execution must not require write permission to the dataset merely
to perform a read.

### Writes

Workers may create retry-scoped staging files only after a complete plan has
been accepted for execution. Workers return opaque artifacts or transaction
fragments, Vane selects successful attempts, and the coordinator performs one
Lance commit.

Concurrent commits use Lance's native conflict rules. Propagate a definitive
conflict; do not serialize writers with `_vane_leases` and do not blindly retry.
If the commit result is genuinely unknown, report it as unsafe to retry without
claiming that a custom lease remains held.

### `VACUUM`, cleanup, compaction, and `DROP`

Keep the existing SQL/API capabilities. This task removes only the custom
coordination wrapped around them.

- Run these operations through the existing coordinator/native Lance path.
- Do not distribute destructive maintenance across Ray workers merely to remove
  leases.
- Do not inspect Vane query state or `_vane_leases` before proceeding.
- Do not block, wait, or reject solely because a Vane relation or Ray plan may
  still exist.
- Preserve native Lance validation, transaction, and error behavior.
- Permit an external or concurrent destructive operation to invalidate an
  in-flight read. The affected read may then fail.

The documentation may recommend deployment-level coordination when users want
stronger availability, but it must not present a maintenance window as a Vane
requirement.

## Why this differs from the removed design

The removed protocol attempted to protect participating reads by writing
storage records below `_vane_leases`. It could coordinate only clients that
understood that private protocol. External Lance clients, direct object-store
deletion, and other cleanup tools could ignore it.

It also changed a read into a write operation, added object-store requests and
recovery state, and created a plan-capture failure window: Lance could publish a
`handoff=ready` record before Vane completed connection-snapshot capture. A
later Vane error then had no generic host callback with which to revoke the
record.

The new design removes the side effect instead of adding a Vane lifecycle API.
If a future deployment needs globally enforced reader retention, design that as
an authoritative catalog or service contract honored by every destructive
client, or adopt a future upstream Lance snapshot-pin mechanism. Do not restore
a hidden Vane-specific dataset directory by default.

## Iceberg PR #2 comparison boundary

Use AstroVela/duckdb-iceberg PR #2 as an architectural comparison, not as code
to copy. At its merged head
`4dd334f3449f2760e26b748b3749e9fde7cfa15a`, Iceberg distributed scan planning
serializes metadata, snapshot identity, file/split descriptions, and worker bind
state without publishing a persistent active-reader handoff. Its distributed
write side effects are owned by execution-time finalize/abort contracts.

Apply the same boundary here:

- read plan capture is side-effect free;
- execution-time write artifacts retain explicit ownership;
- external destructive maintenance is outside Vane's concurrency guarantee.

Do not add an Iceberg dependency or assume Iceberg and Lance coexist in the same
wheel as part of this task.

## Observed repository state

Revalidate this section before editing because the checkout may advance.

On 2026-08-25, the observed state was:

- repository branch: `feat/vane-main-contract-20260820`;
- repository HEAD: `6e298a86d1a52fcd7f0581ccd4740779c8cb7282`;
- `vane-extension.toml` pin:
  `230a4cea71d019a24191e541cb4a888834f84c02`;
- the above pin did not resolve in the official `AstroVela/vane` repository;
- official Vane `refs/heads/main` resolved to
  `b84f025e0aeab3e46523f48ae9d233dc28d28d62` at inspection time;
- the only working-tree modifications were
  `python/lance_duckdb/__init__.py` and `python/tests/test_vane.py`; and
- those modifications changed old lease-token reporting and therefore overlap
  this decision.

Do not reset or overwrite the two existing modified files. Inspect their exact
diff and rewrite only the obsolete lease-related hunks while preserving any
unrelated user work.

## Non-negotiable constraints

1. Do not modify, patch, or commit anything in the Vane repository.
2. Build against a clean checkout of an immutable full SHA reachable from the
   official `AstroVela/vane` `main` branch.
3. Remove any dependency on an unpublished/local Vane plan-capture lifecycle
   commit.
4. Keep the stock-DuckDB loadable Lance artifact and the custom statically linked
   Vane wheel as separate ABI-specific products.
5. Keep `VACUUM LANCE`, compaction, cleanup, and `DROP TABLE` functionality; do
   not confuse removal of custom coordination with removal of these operations.
6. Do not introduce a replacement lock or lease under a different name.
7. Do not require a reader to write to the dataset or object store.
8. Do not claim safety against external cleanup or deletion.
9. Do not silently fall back to a newer dataset version.
10. Use non-editable installs and validate the exact built wheel outside both
    source trees.

## Implementation sequence

### 1. Preflight and preserve existing work

Run and record:

```bash
git status --short
git branch --show-current
git rev-parse HEAD
git submodule status --recursive
git remote -v
```

Read at least:

- `AGENTS.md`;
- `docs/vane-static-wheel-implementation.md`;
- `docs/vane-integration.md`;
- `vane-extension.toml`;
- `extension_config_vane.cmake`;
- `src/include/lance_lease.hpp`;
- `src/lance_lease.cpp`;
- `rust/ffi/lease.rs`;
- every production reference found by searching for `LanceLease`,
  `_vane_leases`, `handoff`, `lease_token`, `lease_kind`, `mutation_lease`,
  `vacuum_lease`, and `coordinator_snapshot_lease`;
- the corresponding SQL, Python, Rust, and C++ tests; and
- the Iceberg PR #2 distributed scan serializer and write finalize/abort paths.

Do not use `git reset`, `git checkout --`, or a bulk replacement that destroys
the current Python changes. Keep an inventory of pre-existing modifications in
the final report.

### 2. Restore an official Vane baseline

Resolve official Vane `refs/heads/main` when implementation begins, verify the
selected full SHA is reachable from that branch, and record it in
`vane-extension.toml`.

The Vane checkout must have no tracked patch before or after the build. Remove
the host-lifecycle amendment and every statement that requires Vane to commit or
abort Lance handoffs. If the selected Vane main requires Lance-side API
adaptation, make that change only in this repository.

If an existing generic scan/write API is genuinely absent from official Vane
main, stop and report the exact missing contract. Do not reintroduce the lease
protocol or patch Vane to preserve it.

### 3. Remove the Rust storage-backed lease implementation

Delete `rust/ffi/lease.rs` and remove its module registration when no production
caller remains. Remove the exported lease FFI functions and constants from the
C header and Rust exports, including acquisition, preparation, adoption,
release, close, and token access.

Remove lease-only helpers, record encodings, coordination locks, recovery code,
tests, and dependencies. Regenerate dependency metadata only through the normal
Cargo workflow. Do not manually edit generated lock content.

Remove lease-specific error variants such as `LeaseRelease` if unused. Preserve
the numeric identity of unrelated FFI error codes where practical; do not create
a large unrelated renumbering diff.

After this step, production Rust code must never create, list, read, update, or
delete an `_vane_leases` path.

### 4. Remove the C++ lease layer

Delete `src/include/lance_lease.hpp` and `src/lance_lease.cpp` after removing all
callers, then remove `src/lance_lease.cpp` from `CMakeLists.txt`.

Known lease consumers to inspect include:

- `src/lance_scan.cpp`;
- `src/lance_search.cpp`;
- `src/lance_write.cpp`;
- `src/lance_insert.cpp`;
- `src/lance_update.cpp`;
- `src/lance_delete.cpp`;
- `src/lance_merge.cpp`;
- `src/lance_truncate.cpp`;
- `src/lance_index.cpp`;
- `src/lance_metadata.cpp`;
- `src/lance_maintenance.cpp`;
- `src/lance_storage.cpp`;
- `src/include/lance_scan_bind_data.hpp`;
- `src/include/lance_insert.hpp`; and
- `src/include/lance_ffi.hpp`.

Remove lease members, acquisition and release calls, destructor policy,
handoff helpers, token validation, and lease-specific error decoration. Do not
remove the surrounding operation's native Lance call, result validation,
transaction ownership, or cleanup logic.

### 5. Make read-plan serialization side-effect free

For scan, vector search, full-text search, hybrid search, and distributable
aggregate/projection execution:

- remove coordinator snapshot-lease members;
- remove handoff-token serialization and deserialization;
- remove `ready`/`adopted` transitions;
- remove worker snapshot-lease acquisition;
- preserve fixed version and generation fields;
- preserve dataset/table and split-set identities;
- preserve credential and non-replayable-option rejection;
- preserve global search semantics; and
- reopen exactly the serialized version on the Driver/worker.

Keep existing serialization property identifiers for unrelated fields where
possible to minimize churn. Compatibility with serialized bytes from another
binary is not required, but unnecessary renumbering obscures review.

A serializer may read metadata and construct in-memory payloads. It must not
write coordination state to the dataset or another external service.

### 6. Remove mutation and maintenance leases without weakening commit ownership

Remove mutation-lease wrapping from local and distributed write paths, schema
changes, metadata changes, index operations, truncate, merge, and namespace
operations.

Keep the following write behavior intact:

```text
Vane schedules retry-scoped worker attempts
  -> workers emit opaque artifacts/fragments
  -> Vane selects one successful attempt per task
  -> Lance validates the selected set
  -> the coordinator performs exactly one native Lance commit
  -> definitely uncommitted artifacts are cleaned up
```

Let Lance report optimistic conflicts. Do not add a process mutex, storage
marker, Ray actor lock, or automatic retry to reproduce mutation-lease
serialization.

For outcome-unknown errors, keep `safe_to_retry = false` or the equivalent
typed contract, but remove `lease_kind`, `lease_token`, `_vane_leases`
instructions, and claims that a custom lease was retained. Reconcile the native
commit/catalog result, not a lease record.

### 7. Preserve direct maintenance and deletion behavior

Remove vacuum-lease acquisition from cleanup, compaction, auto-cleanup, and
`DROP` paths. Preserve their SQL syntax, options, result schemas, error
propagation, and native Lance behavior.

Do not add these policies:

- “wait until all Vane jobs finish”;
- “reject because a Vane relation exists”;
- “run only in a maintenance window”;
- “delete only after a lease timeout”; or
- “ask Ray workers for permission”.

If a user directly deletes a dataset while a plan is in flight, the deletion is
allowed to proceed. A later Driver/worker open must either read the exact fixed
snapshot if it still exists or fail clearly. The race outcome is not a Vane
correctness guarantee.

### 8. Simplify the Python API

Remove Python-visible lease concepts rather than replacing a token with `None`.
In particular, inspect `LanceMutationOutcomeUnknownError` and related helpers:

- retain operation identity, redacted dataset identity, native diagnostic, and
  unsafe-to-retry semantics;
- remove `lease_kind`, `lease_token`, and storage-backed recovery instructions;
- preserve pickling only for the remaining meaningful fields; and
- keep imports optional and compatible with stock DuckDB.

Update the currently modified Python files carefully. Do not revert whole files
to HEAD because that would discard pre-existing work.

### 9. Update tests

Delete or rewrite tests whose only purpose is to validate snapshot, mutation,
vacuum, handoff, adoption, release, or recovery records. Do not delete unrelated
scan, search, write, maintenance, or error-contract coverage from the same test
files.

Add deterministic regressions for the new boundary:

1. Creating, serializing, cloning, and destroying a Lance relation/plan creates
   no `_vane_leases` directory or other persistent coordination artifact.
2. A local Lance read works when the dataset is readable but not writable, on a
   platform where permissions can be tested reliably.
3. Scan/search/exec transport retains exact version and generation identity
   after removing handoff fields.
4. Deleting and recreating a dataset at the same path after plan capture causes
   generation validation to fail; it must not read the replacement dataset.
5. A bound relation or serialized plan does not cause repository-specific
   `VACUUM` or `DROP` blocking. Use deterministic sequencing, not a timing race.
6. `VACUUM LANCE`, compaction, cleanup, and `DROP TABLE` still perform their
   existing native operations and return their existing result shape.
7. Distributed writes still select attempts, commit once on the coordinator,
   handle native conflicts, and clean definitely uncommitted staging files.
8. Outcome-unknown errors remain typed and unsafe to retry, with no lease fields
   or reconciliation instructions.
9. A real two-worker Ray scan and declared distributed write scope pass from the
   installed static wheel.
10. Neither local nor distributed tests require dataset write permission solely
    for a read.

Do not add a flaky test that asserts one exact result for a genuinely concurrent
external delete. The supported contract is that Vane does not coordinate the
race and never switches snapshots silently.

### 10. Update documentation

Update at least:

- `docs/vane-static-wheel-implementation.md`;
- `docs/vane-integration.md`;
- relevant maintenance/SQL documentation;
- examples that mention lease recovery; and
- `AGENTS.md` or `README.md` only where they contradict the new boundary.

Remove the authorized Vane host-lifecycle amendment and all claims that leases
protect reads, writes, `VACUUM`, or `DROP`. State plainly:

> Vane-Lance does not coordinate in-flight operations with external VACUUM,
> DROP, version cleanup, or direct object deletion. Concurrent destructive
> operations are permitted and may cause affected queries to fail. Concurrency
> policy is owned by the caller or deployment.

Do not call the deleted `_vane_leases` directory part of Lance format or retain
operator instructions for recovering its tokens.

## Expected production-code removals

The exact diff depends on the current checkout, but completion should normally
remove:

- `rust/ffi/lease.rs`;
- `src/include/lance_lease.hpp`;
- `src/lance_lease.cpp`;
- CMake/module/FFI registration for those files;
- lease fields in bind/global/write state;
- lease serialization properties and handoff code;
- mutation/vacuum acquisition wrappers;
- lease-specific Python error fields; and
- lease-only tests and documentation.

Use repository-wide searches after the refactor. Production code should have no
remaining `_vane_leases`, `LanceLease`, handoff-token, mutation-lease, or
vacuum-lease behavior. A negative assertion in a regression test and historical
text in this implementation brief are acceptable.

## Validation

Run focused checks first, then the repository-prescribed base suites. At minimum:

```bash
cargo fmt --all --check
cargo check --manifest-path Cargo.toml
cargo clippy --manifest-path Cargo.toml --all-targets

GEN=ninja make release -j 4
./build/release/test/unittest "test/*"

make vane_ci
make vane_wheel
```

Also run the focused Python identity/loading/provenance tests and
`python/tests/test_vane.py` through the non-editable installed-wheel workflow in
`AGENTS.md`.

For the static wheel:

- use a fresh environment outside the Lance and Vane source trees;
- install exactly the built wheel non-editably;
- clear `PYTHONPATH` and extension-path/unsigned-extension overrides;
- verify `vane._native` resolves from the installed wheel;
- verify Lance reports `STATICALLY_LINKED`;
- run real two-worker Ray coverage; and
- confirm the Vane source checkout remains clean.

If service-backed S3/REST tests are unavailable, report them as untested. Do not
infer remote-store success from local tests.

Run formatting required by the repository and finish with:

```bash
git diff --check
git status --short
```

## Acceptance criteria

The task is complete only when:

- no Vane source modification or unpublished Vane commit is required;
- `vane-extension.toml` pins a verified official Vane `main` SHA;
- the stock-DuckDB and custom Vane wheel lanes both build with the same Lance
  source but remain ABI-separated;
- production code never creates or consults `_vane_leases`;
- reads do not require dataset write permission for coordination;
- distributed plan serialization has no persistent external side effect;
- fixed version/generation validation remains intact;
- external `VACUUM`, `DROP`, cleanup, and direct deletion are not blocked by a
  Vane-specific protocol;
- existing `VACUUM LANCE`, maintenance, and `DROP TABLE` features still work;
- native write conflicts and unknown outcomes remain fail-closed and are not
  automatically retried;
- distributed writes still have one coordinator commit owner;
- local and real two-worker Ray tests pass for the declared scope;
- stock-DuckDB regressions pass; and
- documentation assigns external concurrency policy to the caller/deployment.

## Stop conditions

Stop and report rather than widening scope if:

- official Vane main lacks a generic API required by scan/write execution after
  all lease-only dependencies are removed;
- a proposed fix requires patching Vane;
- a read can work only by writing a custom coordination object;
- removing leases reveals a native Lance correctness bug that cannot be fixed
  locally without changing the requested concurrency contract;
- code attempts to replace `_vane_leases` with another private lock protocol;
- a test depends on silently reopening the latest version after the fixed
  version disappears; or
- existing user changes cannot be separated safely from the obsolete lease
  work.

Report the exact command, error, relevant SHAs, and smallest missing native or
generic contract.

## Final handoff format

When implementation is complete, report in Chinese:

1. The exact Lance and official Vane SHAs and the Vane-main ancestry check.
2. Every deleted lease component and every preserved non-lease behavior.
3. How scan/search plans retain version and generation without handoffs.
4. How writes retain one coordinator commit and unknown-outcome safety.
5. Evidence that reads and plan capture create no `_vane_leases` artifacts.
6. Evidence that `VACUUM LANCE` and `DROP TABLE` still work without Vane-specific
   blocking.
7. Installed-wheel local and real two-worker Ray results.
8. Stock-DuckDB regression results.
9. Any untested service/platform scope.
10. Confirmation that the Vane checkout remained clean.

Do not commit, push, publish, or create a pull request as part of the handoff
unless the user separately authorizes it.
