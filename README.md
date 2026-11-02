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

## Rust build configuration contracts

The Rust FFI build requires CMake 3.20 or later and uses the selected configuration
on all generators. Configuration-dependent outputs prevent inactive Visual Studio
build rules from running; the minimum version follows that CMake feature.
`Debug` builds Cargo's `dev` profile; other configurations use `release`.
Single-config generators default to Release when no build type is set.
Visual Studio, Xcode and Ninja Multi-Config select one profile per build without
changing feature flags or the existing conditional lock-file policy.

Run `python test/cmake/test_rust_profiles.py` with CMake and Ninja on PATH.
The tests exercise the actual custom-command dependency graph with a recording
Cargo fixture, including profile switching, up-to-date builds, paths with spaces
and lock/feature arguments. They do not compile the Rust crates or qualify the
resulting extension ABI. Existing extension build tests retain that role.
