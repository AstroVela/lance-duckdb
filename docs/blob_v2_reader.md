# Internal Blob v2 reader

PR2 adds snapshot-bound Blob v2 range reads to the common Rust/FFI layer. It
registers no SQL function or filesystem. The SQL URI encoder, DuckDB FileHandle,
and production native permission/secret adapter belong to PR3.

## Read and ownership contract

`lance_blob_open_binding_dataset` opens a dataset through an authorized store
registry and returns a normal dataset handle. `lance_blob_snapshot_token`
captures the lowercase SHA-256 of the complete existing generation string:

```text
snapshot|[version,timestamp_nanos,transaction_file,manifest_size,etag]
```

The JSON tuple uses the existing serialization and empty-string defaults. The
token helper fills missing manifest metadata through the supplied access context
without replacing metadata already retained by the captured dataset. Common
version, checkout, and generation accessors preserve the Vane representation.

`lance_blob_open` creates a fresh, cache-free session, loads the exact historical
version, refreshes manifest metadata, and compares the token before resolving
the row. It never falls back to the latest version. Field paths use Lance's
schema resolver; only Blob v2 leaves reached through structs are accepted.
Legacy blobs, ordinary binary/struct columns, lists and maps are rejected.
Lance's row-ID index resolves stable IDs to physical addresses. Fragment row
counts and deletion vectors establish whether a row exists before
`take_blobs_by_addresses` supplies the inline, packed, dedicated or external
`BlobFile`. Failures to read this metadata retain their I/O or access error;
they are not treated as evidence that the row is absent.

`lance_blob_read_at` is an exact-range primitive. It validates the offset and
length before I/O, accepts empty reads through EOF, and copies no bytes on
failure. The mediated object store rejects short bodies and EOF-clipped ranges
before Lance's scheduler can slice a short response. There is no extension
payload cache or cursor in Rust. The future C++ FileHandle owns sequential
position and bounded read-ahead.

The caller owns each opaque FFI handle and frees it once. A Blob handle retains
its dataset and access lease independently of the binding dataset/access handle.
Stores and scheduled tasks also retain the lease until completion. Closing a
Blob releases the upstream file without changing the caller's last error. Close
must not race with reads of the same handle; independent handles can run
concurrently. Strings returned by the token/generation helpers use
`lance_free_string`; datasets use `lance_close_dataset`.

## Access mediation

The internal callback ABI is declared in `src/include/lance_ffi.hpp`:

- Creation copies the callback table and retains its owned context once. The
  final Rust owner releases it once, after all pending callbacks finish.
- `resolve` and `release_result` are serialized per access context. Rust copies
  result strings/options and calls `release_result` exactly once, including
  denial and invalid-result paths.
- Callbacks may run on runtime worker threads. Retain/release must be thread
  safe. The context must own its database/policy dependencies; a borrowed
  ClientContext or FileOpener is insufficient. Callbacks must catch exceptions
  and must not unwind or re-enter the same access handle.
- Authorization receives the full normalized target and operation before
  provider construction/cache lookup, listing, metadata and payload access.
  Options are selected for that target and frozen for this open. Credentials,
  endpoints and store overrides from another dataset are not inherited.

Each open uses an empty upstream `ObjectStoreRegistry` populated only with
mediated upstream providers. A target's opaque SHA-256 cache prefix separates
even two secret scopes in the same bucket, and registries are never shared
between access contexts. Provider path extraction is inverted to reconstruct
full object URIs, including HF repository prefixes and escaped path components.
Every cached store operation still performs its per-object policy check. Since
upstream registries bind clients to base URIs, the wrapper also selects an
upstream client for each actual object or listing prefix using that target's
resolved options. Clients are initialized once per target within an open; HEAD,
reads and listing cannot fall back to a base URI's credentials or endpoint.

The supported providers are `file`, `file-object-store`, `file+uring` (when
available upstream), `s3`, `gs`, `az`, `oss` and `hf`. Local providers are
constructed with Lance's explicit `file-object-store` route so optimized local
readers cannot bypass the wrapper. Unknown schemes, URI userinfo, query strings,
fragments and nonempty local URI authorities fail closed before access callbacks
or provider construction. This prevents hostnames ignored by a local provider
from creating an alternate permission target. `s3+ddb` is excluded because its
separate commit handler cannot be mediated by this store hook.

Blob diagnostics use fixed safe messages, never arbitrary callback/provider
text or storage options. Common codes 56–60 identify open, read, range, snapshot
and access failures. Code 61 identifies missing/deleted/null rows and missing
objects during Blob value retrieval. Missing row-ID, fragment or deletion
metadata remains an open failure; missing historical versions remain snapshot
failures; missing backing bytes during a read remain read failures. Typed
permission and authentication errors are recognized throughout the source chain
before provider diagnostics are discarded, including OS permission errors
wrapped by LocalFileSystem. Denial is never reported as absence. Errors return
to the caller thread before updating FFI thread-local state.

## Prototype results and limits

The recording-provider tests exercise provider-construction denial, per-object
checks on cached stores, target-scoped options in the same bucket, external
unknown-size HEAD requests, alternate bases, and all three local schemes with
128 KiB objects. Child objects and listing prefixes under one base URI use
independent scoped stores. A local HTTP S3 mock also checks that the upstream
provider signs HEAD and GET requests with the object's scoped credentials.
Denied operations leave backing-I/O counters unchanged. Row-error tests cover
deleted, null and invalid IDs in both row-ID modes, plus denied, corrupt and
missing deletion metadata. Unix permission tests use mode-000 backing files to
exercise actual OS denials during reads and unknown-size Blob opens; this check
is skipped when the process can bypass file permissions. Portable tests also
cover wrapped permission/authentication errors and diagnostic redaction.
The pinned HF provider is constructed and its repository/path mapping is
verified without requesting a public dataset. Path inversion covers encoded
separators (`%2F`), literal percent sequences (`%252F`), and empty relative paths
while retaining the original repository prefix. Local URI aliases with repeated
or encoded separators reuse the filesystem provider's normalized path. Tests
cover dataset reopening, external Blob reads, and denial at the canonical
physical object URI. Local authority aliases are rejected for all three local
schemes before provider I/O, including dataset binding, historical Blob opens
and external Blob targets.

`test/cpp/blob_access_probe.cpp` separately exercises the pinned native DuckDB
and Vane APIs. It checks native allowed-path/directory exceptions with external
access disabled, disabled backing filesystems, and native secret lookup for two
scopes in one bucket. Two workers run concurrently. An owned DatabaseInstance
remains usable after the initiating Connection and DuckDB wrapper are destroyed.
The probe also links and invokes every new common FFI entry point through its
C++ declaration. Nothing from this experiment is registered in the extension.
The S3 disable check reads native subsystem-name state; it does not load httpfs
or exercise remote filesystem dispatch.

This proves feasibility under fixed configuration. It does not establish safety
for concurrent SQL SET mutations of native allowed-path containers, implement
Vane opener overlays, or provide the production native callback bridge. PR3 must
own/copy native policy inputs under each host's synchronization rules, check the
synthetic URI as well as real targets, and preserve current-opener semantics.
Rust callback tests independently verify result allocation, worker errors and
lease teardown, including reads after binding/access owners are destroyed.

Snapshot identity relies on Lance's immutable-file contract. It is not a payload
checksum or authentication token. Normal same-URI/version recreation is tested,
including canonical identity generation without an ETag;
arbitrary out-of-band byte mutation is not guaranteed to be detected. External
objects must remain immutable: pinning the dataset does not freeze external
bytes. No live S3/MinIO or HF payload smoke test is claimed for this PR.

Lance 9.0.1's nested Blob compaction failed in the fixture experiment with
`Blob struct missing 'data' field`. Historical reopening across append and
compaction is therefore tested with top-level Blob fields; nested Blob reading
and null-parent behavior are tested separately. This reader does not implement
or change upstream compaction.

## Validation

Initial PR2 validation at base `b0d1658` (PR1), with locked Lance 9.0.1:

| Check | Native / feature OFF | Vane / feature ON |
| --- | --- | --- |
| Rust check and clippy with `-D warnings` | Passed | Passed |
| Full Rust unit suite, including 17 Blob tests | 31 passed | 84 passed |
| Native access and common-symbol probe | Passed | Passed |
| SQL descriptor, search and dual-build smoke regressions | 5 cases, 787 assertions | 4 cases, 773 assertions |

The SQL cases are `scan_blob_v2`, `search_arrow_layout`, `search_functions` and
`dual_build_smoke`, plus native `exec_pushdown_p0`. The native engine is pinned
to `3a9afebc44b060f8286f579606517077d9e882a0`; the Vane manifest pin is
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`.

Validation ran on Linux x86_64. It reused unchanged PR1 host/C++ objects and
relinked the test runners with freshly compiled Rust static libraries in each
feature mode.
Rust used the dev profile with unwind enabled, root debug information disabled,
and debug stripping at link time to fit the available disk. These results are
link/runtime validation, not a fresh release-LTO distribution build.

Follow-up Rust validation covers object-scoped clients, missing-row
classification, encoded separators, local URI normalization and authority
rejection, missing deletion metadata, and wrapped OS permission errors. All 44
native and 97 Vane Rust unit tests passed, including 30 Blob tests, along with
Clippy with `-D warnings` in both modes. The new regression tests reproduced the
reviewed failures before their corresponding fixes. The six latest regressions
all failed against `d69e757` before the fixes; the mode-000 OS permission check
ran without skipping in both modes. Rust formatting and the
repository formatter passed; the latter used the existing `.venv` because `uv`
was unavailable. C++ probes, SQL suites and live cloud integration were not
rerun for these Rust-only fixes.

Normal reproduction commands (run from the extension root) are:

```bash
uv run make format
cargo fmt --all -- --check
cargo check --locked
cargo clippy --locked --all-targets -- -D warnings
cargo check --locked --features vane-distributed
cargo clippy --locked --all-targets --features vane-distributed -- -D warnings
CARGO_PROFILE_DEV_PANIC=unwind cargo test --locked
CARGO_PROFILE_DEV_PANIC=unwind cargo test --locked --features vane-distributed
```

To build the optional native experiment with the normal build graph, add
`-DLANCE_BLOB_ACCESS_PROBE=ON` to either host's CMake configuration, then run:

```bash
cmake --build build/release --target lance_blob_access_probe
./build/release/extension/lance/lance_blob_access_probe
```

Replace `build/release` with the configured native or Vane build directory.
The target links that host's DuckDB and matching Rust library. It is off by
default. New Rust implementation and test sources are explicit CMake rebuild
dependencies in both feature modes.
