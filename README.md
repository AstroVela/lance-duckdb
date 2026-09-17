# Lance Extension for Vane and DuckDB

[Lance](https://github.com/lance-format/lance/) is a columnar data format for
ML and AI workloads, with support for vector indexes and cloud storage. This
repository brings Lance datasets to [Vane](https://github.com/AstroVela/vane)
and [DuckDB](https://duckdb.org/) through SQL scans, namespace tables, writes,
and vector, full-text, and hybrid search.

## Choose your runtime

| Runtime | Use it for | Installation and examples |
| --- | --- | --- |
| **Vane** | Python and SQL pipelines with local execution or distributed scans, supported writes, and eligible searches on Ray | [VANE_README.md](VANE_README.md) |
| **DuckDB** | Querying, updating, and searching Lance datasets in official DuckDB | [DUCKDB_README.md](DUCKDB_README.md) |

The two runtimes share the Lance SQL interface and build against different
DuckDB engines. Use the extension package or build intended for your runtime;
their native extension binaries are not interchangeable.

## Capabilities

- **Read Lance datasets** from local paths and object-store URIs, with projection
  and filter pushdown.
- **Work with tables** through `ATTACH ... (TYPE LANCE)` directory and REST
  namespaces.
- **Write and manage data** with `COPY`, table DDL/DML, indexes, and maintenance
  statements, as described in the [SQL reference](docs/sql.md).
- **Search data** with `lance_vector_search`, `lance_fts`, and
  `lance_hybrid_search`.
- **Scale with Vane** using Ray workers for fragment scans, supported
  directory-namespace writes, and eligible vector and full-text searches.
  Global SQL operators and search ranking remain coordinated by Vane.

Vane's distributed execution has specific storage, credential, and operation
requirements. The [Vane guide](VANE_README.md) explains these alongside runnable
examples; the [DuckDB guide](DUCKDB_README.md) covers native DuckDB usage.

## Documentation

- [Vane installation and walkthrough](VANE_README.md): create tables, insert
  rows, update and delete, query, then search
- [Vane Relation API examples](VANE_README.md#use-the-relation-api)
- [DuckDB installation and usage](DUCKDB_README.md)
- [SQL reference](docs/sql.md): scans, namespaces, writes, search, indexes, and
  maintenance
- [Cloud storage](docs/cloud.md): object-store options and native local secrets
- [Vane distributed scans](docs/vane_distributed_scan.md): snapshots, shared
  storage, credentials, and search support
- [Vane distributed writes](docs/vane_distributed_write.md): supported mutations
  and commit behavior
- [Indexed vector search](docs/vane_indexed_vector_candidates.md) and
  [indexed full-text search](docs/vane_fts_candidates.md): conditions for
  distributing search work

## Contributing

Issues and pull requests are welcome. Areas for contribution include pushdown,
parallelism, performance, type coverage, and diagnostics. Each runtime guide
includes its source-build instructions. See the
[SQL test guide](test/sql/README.md), [C++ guidelines](docs/cpp_guidelines.md),
and [Rust guidelines](docs/rust_guidelines.md) for development details.

The [DuckDB guide](DUCKDB_README.md#manual-lance-dependency-bumps) also documents
the shared workflow for preparing Lance dependency updates.

## License

[Apache License 2.0](LICENSE).
