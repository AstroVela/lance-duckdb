# Lance for DuckDB and Vane

[Lance](https://github.com/lance-format/lance/) is a modern columnar data format optimized for ML/AI workloads, with native cloud storage support. This repository contains the Lance extension source maintained for both [DuckDB](https://duckdb.org/) and [Vane](https://github.com/AstroVela/vane).

> [!IMPORTANT]
> DuckDB and Vane use different C++ ABIs. The stock-DuckDB product is a
> loadable extension. The Vane product owned by this repository is an opt-in
> custom `vane-ai` wheel with Lance statically linked. It is not an official
> Vane release, and the default upstream Vane wheel does not contain Lance.

## Build modes

The same source tree has two explicit entry points:

- `extension_config.cmake` builds the normal DuckDB extension with
  `LANCE_VANE_DISTRIBUTED=OFF` and produces the static and loadable targets.
- `extension_config_vane.cmake` enables
  `LANCE_VANE_DISTRIBUTED=ON` for Vane's fork. `make vane_ci` builds a native
  compatibility harness, while `make vane_wheel` uses the same external
  extension configuration to link Lance into a custom Vane wheel.

The distributed scan/write callbacks are selected at compile time. The common
Lance scan, search, write, and Rust FFI code remains shared, so the official
DuckDB path does not include Vane-only headers or callbacks.

## Build and test with official DuckDB

Initialize the pinned DuckDB and extension CI submodules, then use the normal
DuckDB extension targets:

```bash
git submodule update --init --recursive
GEN=ninja make release -j 4
./build/release/test/unittest "test/*"
```

## Build and test against Vane

The repository includes pinned Vane extension CI tools. They check out the
exact official Vane revision declared by `vane-extension.toml` and leave that
checkout clean. The native command verifies ABI compatibility and the
dual-build contract:

Install a C/C++ toolchain, Rust, `protoc`, and the system build tools listed in
`.github/workflows/VaneExtension.yml`. The Python build environment requires:

```bash
python -m pip install \
  build "cmake>=3.29" "ninja>=1.10" "pybind11[global]>=3.0.0" \
  "scikit-build-core>=0.11.4" "setuptools>=77" "setuptools-scm>=9.2.0"
```

```bash
export VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make vane_ci
```

Build the opt-in custom wheel through Vane's normal PEP 517 build, with this
checkout supplied as an external extension:

```bash
make vane_wheel
ls build/vane-wheel/dist/*.whl
```

The wheel must be installed non-editably in a fresh environment outside both
source trees. Its acceptance suite disables extension autoinstall/autoload and
uses neither `LANCE_DUCKDB_EXTENSION` nor
`LANCE_DUCKDB_TEST_ALLOW_UNSIGNED`. CI runs the complete plugin-owned Vane
suite against a real two-node Ray cluster and uploads the wheel, deterministic
provenance JSON, and SHA-256 checksum as `vane-lance-wheel`.

Every Ray node must use the same custom wheel build. Lance is replayed by its
static extension identity; no worker downloads or locates a separate
`.duckdb_extension` file. See
[`docs/vane-integration.md`](./docs/vane-integration.md) for the ownership,
loading, distributed execution, mutation, CI, and compatibility contracts.

## Load Lance

The optional `lance-duckdb` helper package remains pure Python and never
downloads native code. For stock DuckDB it loads only an explicit artifact path
or `LANCE_DUCKDB_EXTENSION`:

```python
import duckdb
from lance_duckdb import load_lance_extension

connection = duckdb.connect()
load_lance_extension(connection, "/opt/duckdb/extensions/lance.duckdb_extension")
```

With the custom Vane wheel, omit the path. The helper first proves Lance is
`STATICALLY_LINKED`, then executes `LOAD lance` by name:

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

## Usage

- Full SQL reference: [`docs/sql.md`](./docs/sql.md)
- Cloud storage reference: [`docs/cloud.md`](./docs/cloud.md)

### Query a Lance dataset

```sql
-- local file
SELECT *
  FROM 'path/to/dataset.lance'
  LIMIT 10;
-- s3
SELECT *
  FROM 's3://bucket/path/to/dataset.lance'
  LIMIT 10;
```

To access object store URIs (e.g. `s3://...`), configure a `TYPE LANCE` secret (see [`docs/cloud.md`](./docs/cloud.md)).

```sql
CREATE SECRET (
  TYPE LANCE,
  PROVIDER credential_chain,
  SCOPE 's3://bucket/'
);

SELECT *
  FROM 's3://bucket/path/to/dataset.lance'
  LIMIT 10;
```

### Write a Lance dataset

Use DuckDB's `COPY ... TO ...` to materialize query results as a Lance dataset.

```sql
-- Create/overwrite a Lance dataset from a query
COPY (
  SELECT 1::BIGINT AS id, 'a'::VARCHAR AS s
  UNION ALL
  SELECT 2::BIGINT AS id, 'b'::VARCHAR AS s
) TO 'path/to/out.lance' (FORMAT lance, mode 'overwrite');

-- Read it back via the replacement scan
SELECT count(*) FROM 'path/to/out.lance';

-- Append more rows to an existing dataset
COPY (
  SELECT 3::BIGINT AS id, 'c'::VARCHAR AS s
) TO 'path/to/out.lance' (FORMAT lance, mode 'append');

-- Optionally create an empty dataset (schema only)
COPY (
  SELECT 1::BIGINT AS id, 'x'::VARCHAR AS s
  LIMIT 0
) TO 'path/to/empty.lance' (FORMAT lance, mode 'overwrite');
```

To write to `s3://...` paths, configure a `TYPE LANCE` secret for that scope (see [`docs/cloud.md`](./docs/cloud.md)).

```sql
CREATE SECRET (
  TYPE LANCE,
  PROVIDER credential_chain,
  SCOPE 's3://bucket/'
);

COPY (SELECT 1 AS id) TO 's3://bucket/path/to/out.lance' (FORMAT lance, mode 'overwrite');
```

### Create a Lance dataset via `CREATE TABLE` (directory namespace)

When you `ATTACH` a directory as a Lance namespace, you can create new datasets using `CREATE TABLE` (schema-only)
or `CREATE TABLE AS SELECT` (CTAS). The dataset is written to `<namespace_root>/<table_name>.lance`.

```sql
ATTACH 'path/to/dir' AS lance_ns (TYPE LANCE);

-- Schema-only (creates an empty dataset)
CREATE TABLE lance_ns.main.my_empty (id BIGINT, s VARCHAR);

-- CTAS (writes query results)
CREATE TABLE lance_ns.main.my_dataset AS
  SELECT 1::BIGINT AS id, 'a'::VARCHAR AS s
  UNION ALL
  SELECT 2::BIGINT AS id, 'b'::VARCHAR AS s;

SELECT count(*) FROM lance_ns.main.my_dataset;
```

### Vector search

```sql
-- Search a vector column, returning distances in `_distance` (smaller is closer)
SELECT id, label, _distance
FROM lance_vector_search('path/to/dataset.lance', 'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
                         k = 5, prefilter = true)
ORDER BY _distance ASC;
```

See the SQL reference for full parameter documentation: [docs/sql.md#search](docs/sql.md#search).

### Full-text search (FTS)

```sql
-- Search a text column, returning BM25-like scores in `_score`
SELECT id, text, _score
FROM lance_fts('path/to/dataset.lance', 'text', 'puppy', k = 10, prefilter = true)
ORDER BY _score DESC;
```

See the SQL reference for full parameter documentation: [docs/sql.md#search](docs/sql.md#search).

### Hybrid search (vector + FTS)

```sql
-- Combine vector and text scores, returning `_hybrid_score` in addition to `_distance` / `_score`
SELECT id, _hybrid_score, _distance, _score
FROM lance_hybrid_search('path/to/dataset.lance',
                         'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
                         'text', 'puppy',
                         k = 10, prefilter = false,
                         alpha = 0.5, oversample_factor = 4)
ORDER BY _hybrid_score DESC;
```

See the SQL reference for full parameter documentation: [docs/sql.md#search](docs/sql.md#search).

## Contributing

Issues and PRs are welcome. High-impact areas include pushdown, parallelism/performance, type coverage, and better diagnostics.

### Manual Lance dependency bumps

This repository includes a manual GitHub Actions workflow for preparing Lance dependency bump PRs:

- `.github/workflows/codex-update-lance-dependency.yml`: manually runs Codex CLI with the repo-scoped `$lance-duckdb-update-lance-dependency` skill.
- `.agents/skills/lance-duckdb-update-lance-dependency/SKILL.md`: defines the shared workflow for latest-release resolution, duplicate PR handling, dependency updates, validation, and PR creation.
- `ci/update_lance_dependency.py`: provides the deterministic dependency update and metadata entrypoint used by the skill.

Required repository secrets:

- `LANCE_RELEASE_TOKEN`: a GitHub token that can read tags and create PRs in this repository.
- `CODEX_TOKEN`: an OpenAI API key used by Codex CLI.

## License

Apache License 2.0.
