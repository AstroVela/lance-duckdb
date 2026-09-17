# Lance Extension for Vane

[Project overview](README.md) · [DuckDB guide](DUCKDB_README.md)

[Vane](https://github.com/AstroVela/vane) is a multimodal data engine built on
DuckDB and Ray. This extension lets Vane query, write, and search
[Lance](https://github.com/lance-format/lance/) datasets through SQL and Python
relations, with local execution and distributed processing on Ray.

## Install

### Install a provider package

Development provider packages are available on
[TestPyPI](https://test.pypi.org/project/vane-extension-lance/). The published
wheels target Linux x86-64 with glibc 2.28 or newer and CPython 3.10 through
3.14. Python 3.12 is used below.

Choose a provider version and the exact `vane-ai` version required by that
provider's package metadata. Download both from TestPyPI, then install the
downloaded wheels with their remaining dependencies from PyPI:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip

LANCE_VERSION='<provider-version>'
VANE_VERSION='<matching-vane-version>'
mkdir -p wheels
python -m pip download --no-deps --only-binary=:all: \
  --index-url https://test.pypi.org/simple/ --dest wheels \
  "vane-extension-lance==$LANCE_VERSION" "vane-ai==$VANE_VERSION"
python -m pip install ./wheels/*.whl "grpcio>=1.42.0"
```

Use a fresh wheel directory for each selected package pair. Provider loading
checks compatibility with the installed Vane engine. Install the same pair on
the coordinator and every Ray worker.

Load the provider on the connection used to build your queries:

```python
import vane

connection = vane.connect()
vane.load_installed_extension("lance", connection=connection)
```

DuckDB's `INSTALL lance` installs an artifact for official DuckDB. Vane requires
the Vane provider package or the source build described below.

### Build from source (development)

The source-build target produces a custom `vane-ai` wheel with Lance statically
linked. It uses the Vane revision pinned in
[`vane-extension.toml`](vane-extension.toml) and the build helpers in
[`makefiles/vane_extension.Makefile`](makefiles/vane_extension.Makefile).

Build on Linux x86-64 with Git, a C++20 compiler, Rust/Cargo, CMake 3.29+, Ninja,
ccache, and the native dependency tools listed in
[Vane Extension CI](.github/workflows/VaneExtension.yml). From this repository:

```bash
git submodule update --init --recursive
python3.12 -m venv .venv-vane
source .venv-vane/bin/activate
python -m pip install --upgrade pip
python -m pip install build packaging "cmake>=3.29" "ninja>=1.10" \
  "pybind11[global]>=3.0.0" "scikit-build-core>=0.11.4" "setuptools-scm>=9.2.0"

make vane_wheel VANE_PYTHON="$VIRTUAL_ENV/bin/python" VANE_BUILD_JOBS=8
python -m pip install build/vane-wheel/dist/vane_ai-*.whl "grpcio>=1.42.0"
```

The target prepares the pinned Vane checkout and vcpkg dependencies under
`build/`. Reduce `VANE_BUILD_JOBS` to suit the machine. Use a non-editable
installation so Ray workers import the installed package.

With this static wheel, replace the provider-loading call above with:

```python
connection.execute("LOAD lance")
```

## Usage

Follow the examples in order: create tables, insert rows, update and delete
rows, query the results, then run searches. The examples reuse the `connection`
initialized above and leave the runner unset to use Vane's default Ray runner.
Use `.show()` to display results; use `.fetchall()` when Python code needs a
list of result rows. See the [SQL reference](docs/sql.md) for the shared SQL
interface.

### Prepare the connection

Configure the Lance directory once so the write examples have a target
namespace. This setup runs on the connection; the relation operations in the
numbered steps use Ray:

```python
connection.execute("ATTACH 'lance_demo' AS lance_ns (TYPE LANCE, READ_ONLY false)")
```

The directory is created as part of the example; no separate `mkdir` call is
needed. Local storage needs no S3 settings. On a multi-host Ray cluster, use a
filesystem mounted at the same absolute path on every worker, or shared object
storage configured as described in [Cloud storage](#cloud-storage).

To use an existing Ray cluster, configure its address before starting step 1:

```python
vane.set_runner_ray(address="auto")
```

### 1. Create tables with CTAS

Create a source table with 10,000 rows, then create a second table containing
the first 5,000 rows:

```python
connection.sql("""
    SELECT i::BIGINT AS id, ('value-' || i::VARCHAR)::VARCHAR AS value
    FROM range(10000) AS source(i)
""").create("lance_ns.main.source")

source = connection.table("lance_ns.main.source")
source.filter(vane.col("id") < 5000).create("lance_ns.main.copied")
```

CTAS means **CREATE TABLE AS SELECT**: create a new table using a query's
column names, column types, and result rows. It combines table creation and
initial data insertion. In Vane, `connection.sql("SELECT ...").create("target")`
is the relation API for this operation and uses the default Ray runner.

| Operation | Effect |
| --- | --- |
| Schema-only `CREATE TABLE` | Define a new empty table. |
| CTAS / `relation.create("target")` | Create a new table with the relation's schema and rows. |
| `relation.insert_into("target")` | Append the relation's rows to an existing table with a matching schema. |

The two tables are stored as `lance_demo/source.lance` and
`lance_demo/copied.lance`. Vane prepares each target on the coordinator, writes
data through Ray workers, and commits the selected results on the coordinator.
Use new table names when repeating the example; `.create(...)` does not
overwrite existing tables.

For Ray execution, use the relation methods shown here. Submitting a raw CTAS
statement through `connection.execute(...)` or `connection.sql(...)` uses the
native statement path in the current API.

### 2. Insert rows

Append the remaining 5,000 source rows. The `copied` table now contains all
10,000 rows:

```python
source.filter(vane.col("id") >= 5000).insert_into("lance_ns.main.copied")
```

This is the relation form of `INSERT INTO ... SELECT ...`. The target table
already exists, and the source columns match its schema.

### 3. Update and delete rows

Change the first five values, then remove rows whose IDs are 9,000 or greater:

```python
connection.table("lance_ns.main.copied").update(
    {"value": vane.lit("updated")}, condition=vane.col("id") < 5
)
connection.table("lance_ns.main.copied").delete(condition=vane.col("id") >= 9000)
```

The updated table contains 9,000 rows. The original `source` table remains at
10,000 rows. Both mutations use Ray workers, with one coordinated commit per
operation.

Distributed Lance writes support CTAS, `INSERT`, `UPDATE`, and `DELETE` for
configured directory-namespace tables in auto-commit mode. `COPY`, `MERGE`,
schema and index changes, maintenance, REST namespace writes, and CTAS
replacement/conditional forms are outside this contract. A failed distributed
CTAS can retain its prepared target. See the
[distributed write guide](docs/vane_distributed_write.md) for supported forms
and failure behavior.

### 4. Query the results

Check the remaining row count and display the five updated rows:

```python
connection.sql("SELECT count(*) AS rows FROM lance_ns.main.copied").show()
# rows = 9000

connection.sql("""
    SELECT id, value
    FROM 'lance_demo/copied.lance'
    WHERE id < 5
""").show()
# All five displayed values are 'updated'; row order is unspecified.
```

Queries can use either the registered table name or the dataset path. A
path-based read does not require a namespace. The namespace in the preparation
step selects Lance storage for writes; it does not select the runner.

Aggregate the original source data using the default Ray runner:

```python
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum
    FROM 'lance_demo/source.lance'
    WHERE id >= 100
""").show()
# rows = 9900, id_sum = 49990050
```

Vane assigns Lance fragments to workers and applies global aggregates, limits,
offsets, and sampling across the combined results. Workers read the snapshot
chosen during planning, including when the dataset is appended to later.

#### Use the Relation API

Build the same kinds of queries with Python expressions. These examples query
the original 10,000-row source table:

```python
from vane import col

source = connection.table("lance_ns.main.source")
filtered = source.filter(col("id") >= 100).select(col("id"), col("value"))

filtered.sort(col("id").asc()).limit(5).show()
```

Aggregate the filtered rows, or group the source rows by an expression:

```python
filtered.aggregate("count(*) AS rows, sum(id)::BIGINT AS id_sum").show()
# rows = 9900, id_sum = 49990050

(
    source.select((col("id") % 2).alias("bucket"), col("id"))
    .aggregate("bucket, count(*) AS rows, sum(id)::BIGINT AS id_sum", group_expr="bucket")
    .sort(col("bucket").asc())
    .show()
)
```

`filter`, `select`, `sort`, `limit`, and `aggregate` build lazy relations.
`show()` executes a relation to display a preview. The five-row preview is a
separate relation, so `filtered` still represents all 9,900 matching rows.
The `.create(...)` and `.insert_into(...)` methods from steps 1 and 2 also
accept filtered relations when you want to write their results.

### 5. Run vector, full-text, and hybrid searches

Run search SQL through `connection.sql(...).show()` using the default Ray
runner. Search needs text and vector columns, so the examples below use a
separate, existing dataset at `path/to/dataset.lance` with `id`, `text`, and a
four-dimensional `vec` column of type `FLOAT[4]`. Replace that path with your
search dataset; the two-column tables above demonstrate table writes and
queries:

```python
connection.sql("""
    SELECT id, _distance
    FROM lance_vector_search(
        'path/to/dataset.lance', 'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
        k = 5, use_index = false, prefilter = true
    )
    ORDER BY _distance ASC, id ASC
""").show()

connection.sql("""
    SELECT id, text, _score
    FROM lance_fts('path/to/dataset.lance', 'text', 'puppy', k = 10)
    ORDER BY _score DESC, id ASC
""").show()

connection.sql("""
    SELECT id, _hybrid_score, _distance, _score
    FROM lance_hybrid_search(
        'path/to/dataset.lance',
        'vec', [0.1, 0.2, 0.3, 0.4]::FLOAT[4], 'text', 'puppy',
        k = 10, prefilter = false, alpha = 0.5, oversample_factor = 4
    )
    ORDER BY _hybrid_score DESC, id ASC
""").show()
```

| Search | Distributed execution |
| --- | --- |
| Exact vector search | Eligible `use_index = false` queries distribute fragment candidates and merge a global top-k. |
| Indexed vector search | Eligible queries with explicit positive `nprobs`, no refinement or post-filter, and enough disjoint work distribute index and uncovered-fragment candidates. |
| Full-text search | Eligible unfiltered direct-dataset queries with at least two disjoint inverted-index segments covering the complete snapshot and at least 4096 rows distribute candidates using shared global BM25 statistics. |
| Hybrid search | Runs as one global Lance search on one worker. |

Searches outside the candidate-distribution conditions execute as one global
search task. The optimizer decides whether there is enough work to distribute;
selecting Ray does not imply every search uses multiple workers. See the
[search SQL reference](docs/sql.md#search),
[scan and search contract](docs/vane_distributed_scan.md#global-search-contract),
[indexed vector conditions](docs/vane_indexed_vector_candidates.md), and
[FTS conditions](docs/vane_fts_candidates.md) for the complete behavior.

### Cloud storage

The local examples above do not need cloud credentials or S3 connection
settings. For a multi-host Ray cluster, use shared object storage or a filesystem
mounted at the same path on each worker.

Before querying or writing S3 datasets, configure replayable Vane session
credentials and a writable target namespace as described in the
[distributed scan guide](docs/vane_distributed_scan.md#sql-semantics) and
[distributed write guide](docs/vane_distributed_write.md#storage-and-credential-boundary).
The [cloud reference](docs/cloud.md) covers object-store configuration.

### Tested development package

Validation on 2026-09-07 with `vane-ai==0.2.0.dev612` and its matching Lance
provider covered Ray table creation, insertion, mutation, aggregation, and
Relation API queries. The search examples were tested with a separate fixture:
vector and hybrid search passed, while full-text search returning `text` failed
with a string-conversion error. Additional tests with a multi-fragment dataset
reproduced a batch-index error in ordered row previews. These runtime issues
have not been fixed by the documentation changes.

## Contributing

See [Contributing](README.md#contributing) for shared development resources.
`make vane_native` builds the native compatibility harness and runs its smoke
test. The [Vane workflow](.github/workflows/VaneExtension.yml) also covers static
and provider wheels, local queries, Ray scans, distributed writes, and
MinIO-backed storage. The relevant integration tests are
[`test_vane_single_scan.py`](python/tests/test_vane_single_scan.py),
[`test_vane_distributed_scan.py`](python/tests/test_vane_distributed_scan.py), and
[`test_vane_distributed_write.py`](python/tests/test_vane_distributed_write.py).

## License

[Apache License 2.0](LICENSE).
