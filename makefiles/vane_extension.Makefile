# Standalone Vane build entry points for this extension. The root Makefile
# invokes this file recursively so none of these defaults affect official
# DuckDB builds.

VANE_EXTENSION_ROOT ?= $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
VANE_CI_TOOLS_VERSION := 28c253d71645b627e90307a8c0f42bf74bda0046
VANE_CI_TOOLS_REPOSITORY := https://github.com/AstroVela/vane-extension-ci-tools.git
VANE_CI_TOOLS_DIR ?= $(VANE_EXTENSION_ROOT)/build/vane-extension-ci-tools
VANE_MANIFEST ?= $(VANE_EXTENSION_ROOT)/vane-extension.toml
VANE_SOURCE_DIR ?= $(VANE_EXTENSION_ROOT)/build/vane-source
VANE_NATIVE_BUILD_DIR ?= $(VANE_EXTENSION_ROOT)/build/vane-native
VANE_WHEEL_BUILD_DIR ?= $(VANE_EXTENSION_ROOT)/build/vane-wheel
VANE_WHEEL_DIST_DIR ?= $(VANE_WHEEL_BUILD_DIR)/dist
VANE_EXTENSION_VCPKG_ROOT ?= $(VANE_SOURCE_DIR)/.cache/extension-vcpkg
VANE_VCPKG_ROOT ?= $(VANE_SOURCE_DIR)/.cache/vcpkg
VANE_VCPKG_INSTALLED_DIR ?= $(VANE_SOURCE_DIR)/vcpkg_installed
VANE_BUILD_JOBS ?= 8
VANE_PYTHON ?= python3
VANE_SKIP_NATIVE_TESTS ?= 0
VCPKG_TARGET_TRIPLET ?= x64-linux

VANE_EXTENSION_COMMAND = $(VANE_PYTHON) \
	"$(VANE_CI_TOOLS_DIR)/scripts/vane_extension.py" \
	--manifest "$(VANE_MANIFEST)" \
	--extension-root "$(VANE_EXTENSION_ROOT)"

.PHONY: vane_ci_tools vane_verify_ci_tools vane_validate vane_prepare vane_identity \
	vane_native vane_ci vane_wheel_dependencies vane_wheel

vane_ci_tools:
	@set -eu; \
	if test ! -d "$(VANE_CI_TOOLS_DIR)/.git"; then \
		test ! -e "$(VANE_CI_TOOLS_DIR)" || { \
			printf '%s exists but is not a Git checkout\n' "$(VANE_CI_TOOLS_DIR)" >&2; \
			exit 2; \
		}; \
		mkdir -p "$(dir $(VANE_CI_TOOLS_DIR))"; \
		tmp_dir=$$(mktemp -d "$(dir $(VANE_CI_TOOLS_DIR)).vane-ci-tools.XXXXXX"); \
		trap 'rm -rf "$$tmp_dir"' EXIT; \
		git init --quiet "$$tmp_dir"; \
		git -C "$$tmp_dir" remote add origin "$(VANE_CI_TOOLS_REPOSITORY)"; \
		git -C "$$tmp_dir" fetch --quiet --depth=1 origin "$(VANE_CI_TOOLS_VERSION)"; \
		git -C "$$tmp_dir" -c advice.detachedHead=false checkout --quiet --detach FETCH_HEAD; \
		mv "$$tmp_dir" "$(VANE_CI_TOOLS_DIR)"; \
		trap - EXIT; \
	fi; \
	actual=$$(git -C "$(VANE_CI_TOOLS_DIR)" rev-parse HEAD); \
	test "$$actual" = "$(VANE_CI_TOOLS_VERSION)" || { \
		printf 'Vane CI tools revision mismatch: expected %s, got %s\n' \
			"$(VANE_CI_TOOLS_VERSION)" "$$actual" >&2; \
		exit 2; \
	}; \
	ci_tools_status=$$(git -C "$(VANE_CI_TOOLS_DIR)" status --porcelain); \
	test -z "$$ci_tools_status" || { \
		echo "VANE_CI_TOOLS_DIR must be a clean checkout" >&2; \
		exit 2; \
	}

vane_verify_ci_tools: vane_ci_tools
	$(VANE_EXTENSION_COMMAND) verify-ci-tools \
		--ci-tools-source "$(VANE_CI_TOOLS_DIR)" \
		--expected-sha "$(VANE_CI_TOOLS_VERSION)"

vane_validate: vane_verify_ci_tools
	$(VANE_EXTENSION_COMMAND) manifest

vane_prepare: vane_validate
	$(VANE_EXTENSION_COMMAND) prepare --vane-source "$(VANE_SOURCE_DIR)"

vane_identity: vane_prepare
	$(VANE_EXTENSION_COMMAND) identity --vane-source "$(VANE_SOURCE_DIR)"

vane_native: vane_prepare
	$(VANE_EXTENSION_COMMAND) native \
		--vane-source "$(VANE_SOURCE_DIR)" \
		--build-dir "$(VANE_NATIVE_BUILD_DIR)" \
		--jobs "$(VANE_BUILD_JOBS)" \
		$(if $(filter 1,$(VANE_SKIP_NATIVE_TESTS)),--skip-tests,)

vane_ci: vane_native

vane_wheel_dependencies: vane_prepare
	@case "$(VANE_BUILD_JOBS)" in \
		''|*[!0-9]*|0) echo "VANE_BUILD_JOBS must be a positive integer" >&2; exit 2 ;; \
	esac
	@test "$(VCPKG_TARGET_TRIPLET)" = "x64-linux" || \
		{ echo "VCPKG_TARGET_TRIPLET must be x64-linux" >&2; exit 2; }
	@test "$$(uname -s)" = "Linux" && test "$$(uname -m)" = "x86_64" && \
		test "$$(getconf LONG_BIT)" = "64" || \
		{ echo "Vane wheel builds require 64-bit x86 Linux" >&2; exit 2; }
	@set -eu; \
	extension_vcpkg_commit=$$("$(VANE_PYTHON)" -c \
		'import sys, tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["vcpkg_commit"])' \
		"$(VANE_MANIFEST)"); \
	case "$$extension_vcpkg_commit" in \
		''|*[!0-9a-f]*) \
			echo "vcpkg_commit must be a full lowercase commit SHA" >&2; \
			exit 2 ;; \
	esac; \
	test "$${#extension_vcpkg_commit}" -eq 40 || { \
		echo "vcpkg_commit must be a full lowercase commit SHA" >&2; \
		exit 2; \
	}; \
	if test -e "$(VANE_EXTENSION_VCPKG_ROOT)" && \
		! test -d "$(VANE_EXTENSION_VCPKG_ROOT)/.git"; then \
		echo "VANE_EXTENSION_VCPKG_ROOT exists but is not a Git checkout" >&2; \
		exit 2; \
	fi; \
	if ! test -d "$(VANE_EXTENSION_VCPKG_ROOT)/.git"; then \
		mkdir -p "$(VANE_EXTENSION_VCPKG_ROOT)"; \
		git init --quiet "$(VANE_EXTENSION_VCPKG_ROOT)"; \
		git -C "$(VANE_EXTENSION_VCPKG_ROOT)" remote add origin \
			https://github.com/microsoft/vcpkg.git; \
	fi; \
	test "$$(git -C "$(VANE_EXTENSION_VCPKG_ROOT)" remote get-url origin)" = \
		"https://github.com/microsoft/vcpkg.git" || { \
		echo "VANE_EXTENSION_VCPKG_ROOT has an unexpected origin" >&2; \
		exit 2; \
	}; \
	if ! git -C "$(VANE_EXTENSION_VCPKG_ROOT)" cat-file -e \
		"$$extension_vcpkg_commit^{commit}" 2>/dev/null; then \
		git -C "$(VANE_EXTENSION_VCPKG_ROOT)" fetch --depth=1 origin \
			"$$extension_vcpkg_commit"; \
	fi; \
	git -C "$(VANE_EXTENSION_VCPKG_ROOT)" \
		-c advice.detachedHead=false checkout --quiet --detach "$$extension_vcpkg_commit"; \
	extension_vcpkg_status=$$(git -C "$(VANE_EXTENSION_VCPKG_ROOT)" status --porcelain); \
	test -z "$$extension_vcpkg_status" || { \
		echo "VANE_EXTENSION_VCPKG_ROOT must be a clean checkout" >&2; \
		exit 2; \
	}; \
	"$(VANE_EXTENSION_VCPKG_ROOT)/bootstrap-vcpkg.sh" -disableMetrics
	@set -eu; \
	if test -e "$(VANE_VCPKG_ROOT)" && \
		! test -d "$(VANE_VCPKG_ROOT)/.git"; then \
		echo "VANE_VCPKG_ROOT exists but is not a Git checkout" >&2; \
		exit 2; \
	fi; \
	if test -d "$(VANE_VCPKG_ROOT)/.git"; then \
		test "$$(git -C "$(VANE_VCPKG_ROOT)" remote get-url origin)" = \
			"https://github.com/microsoft/vcpkg.git" || { \
			echo "VANE_VCPKG_ROOT has an unexpected origin" >&2; \
			exit 2; \
		}; \
		vane_vcpkg_status=$$(git -C "$(VANE_VCPKG_ROOT)" status --porcelain); \
		test -z "$$vane_vcpkg_status" || { \
			echo "VANE_VCPKG_ROOT must be a clean checkout" >&2; \
			exit 2; \
		}; \
	fi
	VCPKG_ROOT="$(VANE_VCPKG_ROOT)" \
	VCPKG_INSTALLED_DIR="$(VANE_VCPKG_INSTALLED_DIR)" \
	VCPKG_TARGET_TRIPLET="$(VCPKG_TARGET_TRIPLET)" \
	VCPKG_MAX_CONCURRENCY="$(VANE_BUILD_JOBS)" \
	bash "$(VANE_SOURCE_DIR)/scripts/bootstrap_vcpkg.sh" "$(VANE_SOURCE_DIR)"
	@set -eu; \
	vane_baseline=$$("$(VANE_PYTHON)" -c \
		'import json, sys; print(json.load(open(sys.argv[1]))["builtin-baseline"])' \
		"$(VANE_SOURCE_DIR)/vcpkg.json"); \
	test "$$(git -C "$(VANE_VCPKG_ROOT)" rev-parse HEAD)" = "$$vane_baseline" || { \
		echo "VANE_VCPKG_ROOT revision does not match the exact Vane manifest" >&2; \
		exit 2; \
	}; \
	vane_vcpkg_status=$$(git -C "$(VANE_VCPKG_ROOT)" status --porcelain); \
	test -z "$$vane_vcpkg_status" || { \
		echo "VANE_VCPKG_ROOT must remain a clean checkout" >&2; \
		exit 2; \
	}

vane_wheel: vane_wheel_dependencies
	VANE_VCPKG_INSTALLED_DIR="$(VANE_VCPKG_INSTALLED_DIR)" \
	VCPKG_TARGET_TRIPLET="$(VCPKG_TARGET_TRIPLET)" \
	VCPKG_MAX_CONCURRENCY="$(VANE_BUILD_JOBS)" \
	VCPKG_TOOLCHAIN_PATH="$(VANE_EXTENSION_VCPKG_ROOT)/scripts/buildsystems/vcpkg.cmake" \
	$(VANE_EXTENSION_COMMAND) wheel \
		--vane-source "$(VANE_SOURCE_DIR)" \
		--build-dir "$(VANE_WHEEL_BUILD_DIR)" \
		--dist-dir "$(VANE_WHEEL_DIST_DIR)" \
		--jobs "$(VANE_BUILD_JOBS)"
