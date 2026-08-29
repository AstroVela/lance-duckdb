PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=lance
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

CORE_EXTENSIONS=''

# Default build: skip DuckDB's parquet extension.
# Override by providing EXT_FLAGS that already contains -DSKIP_EXTENSIONS=...
ifeq (,$(findstring -DSKIP_EXTENSIONS=,$(EXT_FLAGS)))
	EXT_FLAGS += -DSKIP_EXTENSIONS=parquet
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Keep Vane-only variables and implementation out of ordinary DuckDB builds.
# These public entry points delegate to a standalone makefile only when used.
.PHONY: vane_verify_ci_tools vane_validate vane_prepare vane_identity \
	vane_native vane_ci vane_wheel_dependencies vane_wheel
vane_verify_ci_tools vane_validate vane_prepare vane_identity vane_native vane_ci \
	vane_wheel_dependencies vane_wheel:
	+@$(MAKE) --no-print-directory \
		-f "$(PROJ_DIR)makefiles/vane_extension.Makefile" \
		VANE_EXTENSION_ROOT="$(PROJ_DIR)" "$@"

.PHONY: configure_ci
configure_ci:
	@bash scripts/configure_ci.sh
