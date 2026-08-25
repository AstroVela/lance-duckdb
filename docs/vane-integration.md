# Vane Integration Architecture

## Product boundary

`lance-duckdb` remains an out-of-tree DuckDB extension. This repository owns
two independent delivery lanes:

| Lane | Host | Lance form | Delivery |
| --- | --- | --- | --- |
| Stock DuckDB | Pinned official DuckDB | Loadable `.duckdb_extension` | Normal lance-duckdb build |
| Custom Vane | Exact Vane revision in `vane-extension.toml` | Statically linked into `vane._native` | Opt-in `vane-ai` wheel built here |

The custom wheel is not an official Vane release. The pinned revision is an
immutable commit reachable from official Vane main and already contains the
generic distributed scan and write contracts used by Lance. The default Vane
wheel still does not include Lance, and Vane does not fetch or pin this
repository. Lance sources, build orchestration, tests, provenance, and artifact
publication all remain here. A Vane-ABI loadable build is retained only as a
native compatibility harness; it is not the distributed product.

The host ABIs are not interchangeable. A stock-DuckDB Lance binary must never
be loaded into Vane, and a Vane object must never be loaded into stock DuckDB.
Every node in one Vane deployment must use the same custom wheel build.

## Source and build ownership

`vane-extension.toml` pins a full commit from the official
`AstroVela/vane` repository. The Vane extension CI tools clone that exact
revision and pass this checkout to Vane through DuckDB's external-extension
configuration. The build does not copy Lance sources into the Vane checkout or
patch tracked Vane files.

The two Lance CMake entry points are:

- `extension_config.cmake`, which builds the stock-DuckDB lane with
  `LANCE_VANE_DISTRIBUTED=OFF`;
- `extension_config_vane.cmake`, which builds against Vane's DuckDB fork with
  `LANCE_VANE_DISTRIBUTED=ON` and keeps static linking enabled.

The common scan, search, storage, write, and Rust FFI implementation is shared.
Only the Vane build includes Vane's generic distributed scan and write
callbacks.

Both Vane build modes require a C/C++ toolchain, Rust, `protoc`, CMake 3.29 or
newer, Ninja, and the vcpkg toolchain path. The custom wheel additionally uses
PEP 517 `build`, `pybind11`, `scikit-build-core`, `setuptools`, and
`setuptools-scm`; the pinned minimum versions and Linux system package list are
kept in `.github/workflows/VaneExtension.yml`.

### Stock DuckDB

```bash
git submodule update --init --recursive
GEN=ninja make release -j 4
./build/release/test/unittest "test/*"
```

The loadable artifact is normally written below
`build/release/extension/lance/`.

### Vane native compatibility

```bash
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make vane_ci
```

This builds against the pinned Vane ABI and runs the plugin-owned native
dual-build smoke test. The resulting Vane-ABI loadable object is test output,
not the Vane delivery artifact.

### Custom Vane wheel

```bash
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make vane_wheel
ls build/vane-wheel/dist/*.whl
```

The command uses Vane's normal PEP 517 wheel build with this repository as an
external static extension. The selected Vane checkout must be clean before and
after the build. Install the resulting wheel non-editably in a fresh virtual
environment outside both source trees.

The first milestone publishes only a CI artifact named `vane-lance-wheel` or
an equivalently explicit internal artifact. Publishing this custom build under
the same public package name and version as an official Vane wheel is outside
the supported workflow.

## Loading contract

The optional `lance-duckdb` Python package contains convenience code, not a
native binary. `load_lance_extension` has two fail-closed modes:

1. An explicit argument or `LANCE_DUCKDB_EXTENSION` identifies a local artifact
   for a compatible stock-DuckDB host. The helper canonicalizes the path,
   requires a regular file, and calls the host's `load_extension` method.
2. With no path, the helper queries `duckdb_extensions()`, requires Lance to be
   `STATICALLY_LINKED`, and executes `LOAD lance` by name.

The helper never runs `INSTALL`, searches for an artifact, downloads code, or
enables unsigned extensions.

```python
import vane
from lance_duckdb import load_lance_extension

connection = vane.connect(
    config={
        "autoinstall_known_extensions": "false",
        "autoload_known_extensions": "false",
    }
)
load_lance_extension(connection)
```

In the static lane, Vane records and replays the static extension name and
version. Workers use the Lance code already linked into their installed
`vane._native`; there is no extension path, binary transport, digest replay, or
unsigned-extension setting.

## Distributed reads

When `LANCE_VANE_DISTRIBUTED=ON`, Lance registers Vane's generic
`TableFunctionDistributedScanCallbacks` for eligible scans and searches. The
normal bind callback resolves and serializes immutable data-only state. The
coordinator returns opaque fragment splits, and workers reopen the fixed Lance
version, validate its dataset generation, and apply only their assigned splits.
Workers do not repeat catalog discovery or select a newer version.

Plan capture is side-effect free. Scan, search, and distributable
aggregate/projection payloads contain the exact bind-time version and generation
as pure data. Driver and worker initialization reopen that exact identity and
fail on a missing version or generation mismatch; they never recover by opening
the latest dataset. Creating, cloning, serializing, or destroying a plan writes
no coordination object to the dataset.

REST namespace `query_table` scans and searches are rejected by the distributed
callbacks when the namespace cannot provide a stable physical dataset
generation. They may still execute through their supported local path.

Distributed binds contain no credentials, tokens, URI user information,
queries, or fragments. Credential-bearing `TYPE LANCE` options and explicit
`s3_*` credentials make an operation local-only. A remote distributed
deployment must instead provide a worker-local credential chain independently
on every node; non-secret connection settings can still be replayed. A Lance
relation is also not serializable while its connection contains explicit S3
credentials, even if that relation uses local storage, because Vane's generic
connection snapshot would otherwise carry those unrelated values. Lance paths
shown by `EXPLAIN` redact URI user information, queries, and fragments.

Vector, full-text, and hybrid search preserve global top-k semantics. A search
that cannot be safely split uses one global distributed split rather than
concatenating worker-local top-k results.

## Distributed writes

Lance uses DuckDB's ordinary SQL and Relation entry points; Vane has no
Lance-specific executor:

```sql
COPY (SELECT ...) TO 'dataset.lance' (FORMAT lance, MODE 'create');

ATTACH '/datasets' AS lake (TYPE LANCE);
CREATE TABLE lake.main.items AS SELECT ...;
INSERT INTO lake.main.items SELECT ...;
```

For Vane distributed execution, a default direct create uses the generic
`WriteFileRelation` boundary, and attached-table INSERT uses the generic
extension write provider selected by Lance's physical operator. The current
Vane Ray translator does not expose a generic provider contract for
`BATCH_CREATE_TABLE_AS`, so attached CTAS remains coordinator-local. Direct
append, overwrite, and option-bearing writes that are not represented by the
generic relation also remain coordinator-local.

```text
ordinary DuckDB write relation
  -> Lance physical write operator
  -> ExtensionWriteTaskProvider creates one operation identity
  -> Vane schedules input partitions on Ray workers
  -> workers write retry-scoped files and transaction fragments
  -> scheduler selects one successful attempt per task
  -> coordinator validates selected fragments
  -> coordinator performs exactly one Lance commit
  -> coordinator invalidates its local dataset cache
```

Vane owns scheduling, attempt selection, transport, and the coordinator
transaction boundary. Lance owns target validation, staging format, commit,
abort, cache invalidation, orphan cleanup, and outcome-unknown behavior. Worker
state is data only; it contains no process-local C++ or Rust handle. Credentials
are resolved through the normal host/storage configuration and are not written
to plans or provenance.

## External concurrency boundary

Vane-Lance does not coordinate in-flight operations with external `VACUUM`,
`DROP`, version cleanup, or direct object deletion. Concurrent destructive
operations are permitted and may cause affected queries to fail. Concurrency
policy is owned by the caller or deployment.

Reads require only read access and retain their exact version and generation in
memory and serialized plan data. Writes rely on Lance's native MVCC and
optimistic conflict behavior. Maintenance and deletion run directly through
their existing coordinator/native Lance paths without waiting for Vane plans or
relations. Deployments that need stronger availability must coordinate these
operations outside this extension or use an authoritative mechanism honored by
every destructive client.

## Optional Python surface

`python/lance_duckdb` owns the pure-Python convenience APIs:

- `load_lance_extension`;
- `LanceDataset`;
- `LanceNamespace` and attached-table helpers;
- index, vector, full-text, and hybrid-search helpers;
- typed outcome-unknown errors and URI identity helpers.

The package works without Vane for official-DuckDB local use. Vane imports are
optional and limited to generic distributed runner dispatch and richer Vane
error types. Native Lance operators remain authoritative for transactions and
versions.

## Provenance

`ci/generate_vane_wheel_provenance.py` verifies a clean Lance checkout, a clean
official Vane checkout at the manifest pin, and the installed runtime before it
writes output. The deterministic JSON records:

- canonical Lance and Vane repository URLs and full commits;
- the Lance Git tree;
- DuckDB fork revision, fork version, and SourceID;
- the exact wheel filename, platform tag, and SHA-256;
- `lance_install_mode = STATICALLY_LINKED`.

The runtime probe runs with isolated Python path handling, confirms `vane` and
`vane._native` came from the fresh environment, disables extension autoload and
autoinstall, and verifies `LOAD lance` without an artifact path. Identity
mismatches fail the build. A conventional SHA-256 checksum file is emitted next
to the JSON manifest.

## Validation ownership

Repository CI separates four concerns:

1. Pure Python tests install `lance-duckdb` non-editably and cover identity,
   explicit/static helper loading, and provenance utilities.
2. The stock-DuckDB workflow builds its loadable extension and runs the owned
   SQLLogicTest suite.
3. The Vane native job builds against the exact pinned ABI and runs the
   dual-build smoke test.
4. The static-wheel job builds exactly one wheel, installs it in a fresh
   environment outside the source trees, verifies installed module paths and
   static identity, runs plugin-owned local and real two-node Ray tests, emits
   provenance and checksum files, and uploads `vane-lance-wheel`.

Static-wheel acceptance tests explicitly remove `LANCE_DUCKDB_EXTENSION`,
`LANCE_DUCKDB_TEST_ALLOW_UNSIGNED`, and `PYTHONPATH`, and disable host extension
autoload/autoinstall. Tests verify scan results, filter/projection behavior,
global search results, distributed direct-create and attached-INSERT outcomes,
local SQL CTAS/COPY behavior, single commits, staging cleanup, and definitive
failure handling.

## Supported and untested scope

The current custom-wheel CI target is Linux x86-64. A wheel platform tag is
recorded as evidence, not a claim of public manylinux compatibility. The custom
wheel is not published to public PyPI by this workflow.

Local directory datasets and the distributed operations in
`python/tests/test_vane.py` are the declared first-milestone scope. The
following require separate service-backed evidence and must not be inferred
from the wheel build:

- S3/MinIO or other remote object-store success;
- REST namespace distributed replay;
- detached `ATTACH` replay after the source connection is destroyed;
- Iceberg and Lance coexistence in one wheel;
- platforms not exercised by CI.

Extension identity prevents executable-code drift, while fixed Lance version and
generation state prevent silent replacement reads. They do not prevent external
cleanup from invalidating an in-flight query, and an outcome-unknown write
remains unsafe to retry automatically.
