# This file is included by DuckDB's build system. It specifies which extension
# to load. Keep official DuckDB builds isolated from a stale Vane CMake cache.
if(NOT DEFINED LANCE_VANE_EXTENSION_CONFIG_VANE)
  set(LANCE_VANE_DISTRIBUTED
      OFF
      CACHE BOOL "Build the Vane distributed Lance scan adapter" FORCE)
endif()

# Extension from this repo
duckdb_extension_load(lance
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
