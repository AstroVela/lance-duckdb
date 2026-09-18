# Lance Extension for Vane and DuckDB

[Lance](https://github.com/lance-format/lance/) is a columnar data format for
ML and AI workloads, with support for vector indexes and cloud storage. This
repository brings Lance datasets to [Vane](https://github.com/AstroVela/vane)
and [DuckDB](https://duckdb.org/) through SQL scans, namespace tables, writes,
and vector, full-text, and hybrid search.

## Choose your runtime

| Runtime | Use it for | Installation and examples |
| --- | --- | --- |
| **Vane** | Python and SQL pipelines using the default Ray runner for scans, supported writes, and searches | [VANE_README.md](VANE_README.md) |
| **DuckDB** | Querying, updating, and searching Lance datasets in official DuckDB | [DUCKDB_README.md](DUCKDB_README.md) |

## License

[Apache License 2.0](LICENSE).
