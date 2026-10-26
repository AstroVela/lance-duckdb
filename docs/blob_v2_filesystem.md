# Blob v2 SQL and filesystem

PR3 exposes the common snapshot-bound [Rust reader](blob_v2_reader.md) through
`lance_blob_uri` and DuckDB's read-only `LanceBlobFileSystem`. Both are registered
in ordinary DuckDB and Vane builds. This provides file bytes and random access;
video decoding and distributed video execution are outside this change.

## SQL and snapshot lifetime

```sql
SELECT lance_blob_uri('videos.lance', 'video', _rowid)
FROM 'videos.lance';
```

The first two arguments are non-null foldable VARCHAR expressions; `_rowid` is
UBIGINT. NULL row IDs return NULL. Binding validates the original Lance schema,
accepting Blob v2 fields through structs and Lance backtick-quoted field names.
Lists/maps and ordinary binary or descriptor-like structs are rejected.
Dataset and column names may also be prepared-statement parameters. Their
constant and schema checks run after the parameter values are bound at execution.

Binding shares the connection's existing dataset cache with scans. Cache keys
include normalized paths and resolved storage options; local absolute paths,
file URIs, S3 scheme aliases and HF repository aliases share their corresponding
keys, including escaped path bytes. Cache misses
open through the authorized reader. Binding captures version and manifest token
from that same dataset. Evaluating rows only appends the unsigned row ID to an
encoded prefix, without dataset opens or payload reads per row.

Bound expressions and copied bind data retain the captured dataset until DuckDB
rebinds them. The pinned engine rebinds prepared statements containing
CONSISTENT_WITHIN_QUERY functions at execution; the scan and URI are rebound
together. Materialized URI strings retain their original version.
Existing cache invalidation rules determine when a new bind sees a later version.
Vane expires captured latest-path entries at the native QueryEnd callback, so
all bindings in one query share a snapshot while the next query can observe a
mutation committed by another coordinator.
Materialized URI strings can be passed to another connection; bound expressions
cannot be serialized/deserialized. Materialize strings locally before passing
them to a consumer that transports values rather than an executable plan.

Opening a URI checks the exact historical version and manifest identity before
resolving the row. Append/compaction do not silently move an existing URI to the
latest version. Missing history and same-path recreation fail explicitly.
These guarantees rely on Lance's immutable-file contract. The token is neither
a payload checksum nor a credential; an external object's contents must remain
immutable independently of the dataset snapshot.

## URI contract

The versioned form is:

```text
lance-blob://v1/?dataset=<encoded-uri>&version=<uint64>&snapshot=<sha256>&column=<encoded-path>&rowid=<uint64>
```

Text values use UTF-8 and percent encoding of every byte outside the RFC 3986
unreserved set. Decoding occurs once; `+` is not a space escape. The snapshot is
64 lowercase hexadecimal characters. Version is positive and row ID supports
the complete unsigned 64-bit range. Unknown, duplicate, empty or missing keys,
bad escapes, NUL, invalid UTF-8, overflow and fragments are rejected. The limits
are 64 KiB per URI, 16 KiB per decoded dataset and 4 KiB per decoded column.
Dataset targets must have a supported scheme; generated local targets are
absolute file URIs. Credentials, query strings and fragments in backing URIs
are rejected. Literal `?`/`#` in local filenames are encoded as path bytes.

## File behavior

Each handle owns its Rust reader, cursor, mutex and a window of at most 1 MiB.
Small cache misses fill a bounded window; large reads request their exact range
directly. Lance may coalesce physical requests independently. There is no global
Blob payload cache. `read_blob` itself requests the entire file, so use a
range-reading consumer when full materialization is inappropriate.

Sequential reads clip at EOF and advance only on success. Positional reads
require the entire requested range and preserve the cursor. Seek permits
positions from zero through EOF. A failed backing read invalidates the window
and preserves the cursor. Independent handles have independent cursors; close
serializes with reads and is idempotent. Operations after close fail.

Metadata reports a regular, seekable, non-disk file with the Blob's own size.
Modification time is zero (unavailable), and the version tag is empty. Files
larger than INT64_MAX cannot be opened through DuckDB's signed file interface.
Write/create/append, direct I/O and locking intentions are rejected. Glob treats
the query-containing URI literally. FileExists returns false only for structured
missing-row, null-Blob or missing-payload errors; malformed input, access denial,
snapshot errors and transport failures remain errors.

## Access and credentials

Both the synthetic URI and real metadata/payload targets obey native
`DBConfig::CanAccessFile`, allowed paths/directories and filesystem disable
checks. Local policy matching uses the decoded absolute OS path. Cloud disable
names are honored even when the corresponding DuckDB cloud extension is absent.
Registered remote handlers also retain their native dispatch priorities and
disable settings. Only the default local fallback is exempt for remote targets.
Configure policy before concurrent reads, following native DuckDB configuration
ordering; this adapter does not add synchronization to concurrent SQL SET of
native policy containers.

Initialize any native persistent secret storage before disabling its backing
filesystem, or configure the native secret storage settings accordingly. In
particular, the pinned Vane engine may need permission to inspect its secret
directory on the first lookup, including a local Blob open.

An open captures an owned database lease and, in Vane, applicable session S3
options. Native SecretManager chooses scoped TYPE LANCE secrets for each target.
It uses the initiating connection's active catalog transaction, including
uncommitted secret creation, replacement and deletion. DuckDB's transaction
callbacks retire that catalog view on commit or rollback. The backing payload
source is opened before the FileHandle is returned, so its credentials remain
owned by the handle even if the transaction ends or the connection is destroyed.
This metadata probe does not fetch payload bytes.
The first provider callback for each target returns options. Subsequent provider,
list, metadata and read callbacks authorize without repeating secret lookups.
All callbacks catch C++ exceptions and return redacted errors. No ClientContext
or FileOpener is retained
by Rust workers. Handles remain usable after the initiating connection is gone.
Reopening captures current opener state and resolves secrets again.

SQL-created secret scopes use the same canonical paths as dataset lookups.
Escaped object paths follow the provider's decoding rules: `%2F` and `/` name
the same separator, while `%252F` remains a literal percent sequence. Trailing
directory separators are preserved for scoped prefix matching.
Persistent secrets store these canonical scopes and reload through DuckDB's
native deserializer. Matching and transaction visibility are handled directly
by SecretManager, without an extension-owned scope index or cache.

## Validation and reproduction

The CMake target `lance_blob_filesystem_test` is enabled with BUILD_UNITTESTS.
It loads the matching host and extension statically and exercises real native
FileHandle calls. The Python driver generates disposable inline, packed,
dedicated, known/unknown-size external, empty, NULL and nested Blob fixtures,
using both physical and stable row IDs. A subprocess handshake tests read
failure recovery, append, compaction, replacement and historical-metadata loss
while handles and prepared statements remain alive. Lance 9.0.1 nested Blob
compaction is unsupported, so compaction uses a separate top-level fixture.

```bash
uv run make format
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features vane-distributed -- -D warnings
CARGO_PROFILE_DEV_PANIC=unwind cargo test --locked --lib
CARGO_PROFILE_DEV_PANIC=unwind cargo test --locked --lib --features vane-distributed
cmake --build build/release --target lance_blob_filesystem_test
uv run --with pylance==9.0.1 --with pyarrow==22.0.0 python \
  test/scripts/run_blob_filesystem_tests.py \
  build/release/extension/lance/lance_blob_filesystem_test
./build/release/test/unittest 'test/sql/blob_filesystem.test'
```

The MinIO driver records real HTTP request counts, bytes and ranges. It checks
target-specific credentials, transaction-visible secret changes and rollback,
retained handles after disconnect, encoded scope aliases, explicit denial
without backing requests, and a bounded-buffer versus direct-FFI comparison.
The CI job creates two users using `test/data/blob_s3_*_policy.json`; one can
read the dataset and the other can read only the external payload prefix.
Run against a disposable MinIO bucket (the driver writes `blob-pr3/`):

```bash
LANCE_TEST_S3=1 uv run --with pylance==9.0.1 --with pyarrow==22.0.0 --with boto3 \
  python test/scripts/run_s3_blob_filesystem_tests.py \
  build/release/extension/lance/lance_blob_filesystem_test
```

Set LANCE_BLOB_S3_DATASET_KEY/SECRET and LANCE_BLOB_S3_EXTERNAL_KEY/SECRET to
select restricted reader accounts. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
select the fixture uploader. See the driver's help for endpoint/bucket options.

A public HF smoke mode reads a prefix and the MP4 moov range from the fixed
[pusht-lance](https://huggingface.co/datasets/lance-format/pusht-lance) sample:

```bash
HF_ENDPOINT=https://huggingface.co \
  build/release/extension/lance/lance_blob_filesystem_test --hf \
  hf://datasets/lance-format/pusht-lance/videos.lance \
  1209f0c5e6bd8a549f22c2734cfbe33f2a38219d
```

HF access is an opt-in network smoke test; routine CI uses local/MinIO fixtures.
The Vane smoke SQL also checks common function and filesystem registration.

### Implementation validation (2026-10-16)

The implementation was checked against `main_vane` at `30a38f2`, with the pinned
native DuckDB and Vane DuckDB source revisions:

| Check | Native | Vane |
| --- | --- | --- |
| Rust unit suite | 65 passed | 118 passed |
| Strict Clippy, all targets | Passed | Passed |
| Fresh extension C++ compilation and Rust static library | Passed | Passed |
| Local FileHandle driver, physical and stable row IDs | Passed | Passed |
| Blob, schema, search, cache-scope and smoke SQL | 10 cases passed | 7 cases passed |
| Additional DML and directory-namespace SQL | 6 cases passed | Not run |
| S3 Blob SQL | Passed | Passed |
| MinIO FileHandle driver with two restricted accounts | Passed | Passed |
| Public HF prefix, MP4 metadata range and backward seek | Passed | Passed |

For a 2,097,409-byte dedicated object, sequential 64 KiB reads issued three
payload GETs with the FileHandle window, versus 33 through the direct FFI reader.
Both transferred exactly 2,097,409 bytes. The largest buffered response was
1 MiB. Two concurrent head/tail readers issued four GETs, transferring 2,097,280
bytes; five sparse seeks issued four GETs, transferring 2,621,954 bytes. Policy
denial issued zero backing requests. Metadata open was outside these measured
payload phases.

Two additional Vane SQL cases, `scan_dataset_cache` and
`dataset_cache_precise_invalidation`, expect cache hits across queries. They fail
under Vane's latest-path eviction contract. A control build restoring the base
commit's immediate scan-cache invalidation reproduced both failures. The new
QueryEnd invalidation preserves that cross-query contract while allowing the
scan and URI to share one snapshot within a query.

Runtime checks linked fresh extension objects and Rust libraries to previously
built matching host libraries. Both CMake configurations were generated, and
the native harness's generated library order was also linked and executed.
A completely clean release/LTO build, packaged provider-wheel installation,
multi-node execution, authenticated HF access and other live cloud providers
were not tested. The CI changes have not been executed by GitHub Actions here.
