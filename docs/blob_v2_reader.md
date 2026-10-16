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
One `StoreTarget` representation owns the canonical URI, provider root and
object path. Provider construction, callbacks, credential selection, cache keys
and object operations all use this representation. Equivalent URI spellings
therefore share the same policy target and cached client. Parsing reuses Lance's
provider extraction and the upstream URL/Path types; backend adaptations are
applied only at the delegation boundary.
Every cached store operation still performs its per-object policy check. Since
upstream registries bind clients to base URIs, the wrapper also selects an
upstream client for each actual object or listing prefix using that target's
resolved options. Clients are initialized once per target within an open; HEAD,
reads and listing cannot fall back to a base URI's credentials or endpoint.

For OpenDAL-backed providers, the wrapper adapts request paths and listing
results to preserve literal percent sequences across `object_store_opendal`'s
extra decoding step. HEAD/GET metadata retains the original object path.
Before provider construction or any operation, the target is checked with
OpenDAL's own path normalizer. Nonempty paths that would change, including
leading or trailing Unicode whitespace, fail with an access error before backing
I/O. Native stores retain their original path semantics; native S3 can still
read objects with leading or trailing spaces. Interior spaces and literal
percent sequences remain distinct object-key characters.
Azure and GCS OpenDAL clients are constructed at the bucket/container origin,
while authorization and option selection still use the full object or prefix
URI. This prevents the URL path from also becoming a client root and being
applied twice.
The callback's `root` and `hf_root` options may only denote the store root:
empty strings or slash-only values such as `/` and `///`. Other values fail
with an access error before provider construction, cache lookup or object I/O,
even if the selected provider would otherwise ignore that option. The check uses
OpenDAL's root normalization and runs for every callback result. Directory
prefixes belong in the target URI, not in storage options. For example, read
`az://bucket/private/public/object` directly; do not combine
`az://bucket/public/object` with `root=private`. Both option names are checked
independently so an unused alias cannot hide a conflicting prefix.

The supported providers are `file`, `file-object-store`, `file+uring` (when
available upstream), `s3`, `gs`, `az`, `oss` and `hf`. Local providers are
constructed with Lance's explicit `file-object-store` route so optimized local
readers cannot bypass the wrapper. Unknown schemes, URI userinfo, query strings,
fragments, explicit URI ports and nonempty local URI authorities fail closed
before access callbacks or provider construction. This prevents authorities or
ports ignored by a provider from creating an alternate permission target for the
same physical object. Network endpoint ports remain valid in target-scoped
storage options. `s3+ddb` is excluded because its separate commit handler cannot
be mediated by this store hook.

All local schemes normalize to a `file:///...` policy URI through the upstream
URL/filesystem path conversion, after validating the original authority. Their
provider registrations share the same resolved credentials and raw-client cache.
The `file-object-store` scheme is used only at the backend delegation boundary.
This preserves literal percent sequences, spaces, Unicode names and platform
path semantics without filesystem I/O or symlink resolution during normalization.

S3, GCS, Azure and OSS bucket/container authorities must use literal lowercase
ASCII naming characters. Empty names, dot segments, percent escapes, encoded
delimiters and uppercase aliases fail before callbacks, cache lookup or provider
construction. For example, `s3://%2e/bucket/private/object` is rejected before an
HTTP client can remove the encoded dot segment. S3-compatible underscores and
GCS dots/underscores remain accepted, as do Azure's dollar-prefixed reserved
containers such as `$root`, `$web` and `$logs`.
Length and other service-specific naming restrictions remain upstream concerns.
These identity checks do not decode or restrict the separately parsed object key.

HF repository types use the canonical plural authorities `models`, `datasets`,
`spaces` and `buckets`. Singular and case variants are normalized before
callbacks, cache lookup and client construction, so aliases share one policy
target. Owner and repository components are decoded once to the repository-name
characters documented by
[Hugging Face](https://huggingface.co/docs/huggingface_hub/package_reference/utilities#huggingface_hub.utils.validate_repo_id).
Owner and repository names are then folded to lowercase, matching HF's
case-insensitive repository identity. For example, both `%6fwner/%72epo` and
`%4fWNER/%52EPO` become `owner/repo` for policy, scoped options, caches and HTTP
access. Object paths retain their original case: `Private/Object` and
`private/object` are distinct targets.
Empty identities, encoded delimiters, nested escapes and invalid name characters
fail closed. Repository normalization does not decode the object key a second
time. Unrecognized HF repository types also fail closed.

Blob diagnostics use fixed safe messages, never arbitrary callback/provider
text or storage options. Common codes 56–60 identify open, read, range, snapshot
and access failures. Code 61 identifies missing/deleted/null rows and missing
objects during Blob value retrieval. Missing row-ID, fragment or deletion
metadata remains an open failure; missing historical versions remain snapshot
failures; missing backing bytes during a read remain read failures. Typed
permission and authentication errors are recognized throughout the source chain
before provider diagnostics are discarded, including OS permission errors
wrapped by LocalFileSystem and typed OpenDAL permission errors wrapped by
`object_store_opendal`. Denial is never reported as absence. Errors return to the
caller thread before updating FFI thread-local state.

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
while retaining the canonical repository prefix. Local URI aliases with repeated
or encoded separators reuse the filesystem provider's normalized path. Tests
cover dataset reopening, external Blob reads, and denial at the canonical
physical object URI. Local authority aliases are rejected for all three local
schemes before provider I/O, including dataset binding, historical Blob opens
and external Blob targets.
Cloud URI port tests cover S3, GCS, Azure, OSS and HF, asserting rejection before
callbacks or provider construction. A local HTTP mock exercises OpenDAL HEAD
and GET permission failures while preserving error codes and redacting response
diagnostics; its endpoint uses an explicit port.

Additional local HTTP tests verify literal percent keys on native S3 and OpenDAL
HEAD/GET requests, plus OpenDAL listing prefixes, returned object/common-prefix
paths and subsequent reads of listed objects. Azure and GCS OpenDAL requests are
compared with upstream clients constructed at the storage origin. HF alias tests
cover singular/plural and case variants for all four repository types, plus
encoded owner/repository aliases and denial after a store has been cached.
A child process isolates HF's endpoint environment setting while its real HTTP
client reads from a local server. The test checks canonical repository routes,
literal percent object keys, shared clients and denial after cache population.
OpenDAL whitespace tests check provider creation, GET, HEAD and both listing
methods with backing-I/O counters. HTTP tests also preserve native S3 whitespace
keys and OpenDAL interior spaces. These tests require no cloud credentials or
public datasets.

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
rejection, cloud URI port rejection, missing deletion metadata, and wrapped OS
and OpenDAL permission errors. It also covers literal percent paths, HF authority
and repository aliases, Azure/GCS OpenDAL client roots, OpenDAL whitespace
rejection, and canonical targets shared by callbacks and cached clients.
All 55 native and 108 Vane Rust unit tests passed, including 41 Blob tests,
along with all-targets Clippy with
`-D warnings` in both modes. The regression tests reproduced the reviewed
failures before their corresponding fixes: the six earlier regressions failed
against `d69e757`, and
the three port/OpenDAL regression tests failed against `acbebc8`. The four new
path/alias regression tests also failed against `160e7ba` in the native build.
Five new or expanded target-identity regression checks failed against `495dc71`
in the native build, while the native S3 whitespace control still passed.
The mode-000 OS permission check ran without skipping in both modes. Rust
formatting and the repository formatter passed; the latter used the existing `.venv` because
`uv` was unavailable. C++ probes, SQL suites and live cloud integration were
not rerun for these Rust-only fixes.

The bucket-authority and local-scheme follow-up adds six tests: five regressions
fail against `5fb1f9a`, while the valid S3 HTTP control passes. Coverage includes
rejection before callbacks/provider construction and HTTP access, native/OpenDAL
HEAD and GET requests with dotted or hyphenated bucket names, canonical local
credential scopes, shared alias clients, cached-store GET/HEAD/LIST denial,
binding/snapshot aliases, and known/unknown-size external Blob reads through FFI.
Local path controls include spaces, Unicode, literal percent sequences and Unix
backslashes. All 61 native and 114 Vane Rust unit tests pass, including 47 Blob
tests, along with strict all-targets Clippy in both modes, Rust formatting and
`uv run make format`. This follow-up does not rerun C++ probes, SQL suites or
live cloud integration.

The HF repository-case and storage-root follow-up adds three tests and expands
the existing HF alias and Azure/GCS HTTP tests. All five regression checks fail
against `ce4b2c0`; the Azure/GCS HTTP control with empty and slash-only roots
passes on both revisions. Coverage includes canonical HF policy and HTTP
targets, case-sensitive object names, rejection of directory roots before
provider construction or HTTP access, and callback changes after client caching
across GET, HEAD and LIST operations. All 64 native and 117 Vane Rust unit tests
pass, including 50 Blob tests, along with strict all-targets Clippy in both modes,
Rust formatting and `uv run make format`. The Rust harness uses the dev/unwind
configuration described above. C++ probes, SQL suites and live cloud integration
were not rerun for this follow-up.

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
