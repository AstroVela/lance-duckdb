# SPDX-License-Identifier: Apache-2.0

function(lance_configure_vane_provider LOADABLE_EXTENSION_NAME)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME}
                             PRIVATE LANCE_VANE_DYNAMIC_PROVIDER)
  # The provider carries the Rust closure and its matching DuckDB engine in one
  # artifact. Hide private definitions and strip at link time, before DuckDB's
  # signature footer is replaced by the release signer.
  set_target_properties(
    ${LOADABLE_EXTENSION_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden
                                          VISIBILITY_INLINES_HIDDEN ON)
  if(APPLE)
    set(LANCE_VANE_EXPORTS "${CMAKE_CURRENT_BINARY_DIR}/vane-lance-exports.txt")
    file(WRITE "${LANCE_VANE_EXPORTS}" "_lance_duckdb_cpp_init\n")
    set_property(
      TARGET ${LOADABLE_EXTENSION_NAME}
      APPEND
      PROPERTY LINK_DEPENDS "${LANCE_VANE_EXPORTS}")
    target_link_options(
      ${LOADABLE_EXTENSION_NAME} PRIVATE
      "-Wl,-exported_symbols_list,${LANCE_VANE_EXPORTS}" -Wl,-dead_strip -Wl,-S
      -Wl,-x)
  elseif(MSVC)
    set(LANCE_VANE_EXPORTS "${CMAKE_CURRENT_BINARY_DIR}/vane-lance-exports.def")
    file(WRITE "${LANCE_VANE_EXPORTS}" "EXPORTS\n  lance_duckdb_cpp_init\n")
    set_property(
      TARGET ${LOADABLE_EXTENSION_NAME}
      APPEND
      PROPERTY LINK_DEPENDS "${LANCE_VANE_EXPORTS}")
    target_link_options(
      ${LOADABLE_EXTENSION_NAME}
      PRIVATE
      "/DEF:${LANCE_VANE_EXPORTS}"
      /INCREMENTAL:NO
      /OPT:REF
      /OPT:ICF
      /DEBUG:NONE)
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(LANCE_VANE_VERSION_SCRIPT
        "${CMAKE_CURRENT_BINARY_DIR}/vane-lance-exports.map")
    file(WRITE "${LANCE_VANE_VERSION_SCRIPT}"
         "{\n  global: lance_duckdb_cpp_init;\n  local: *;\n};\n")
    set_property(
      TARGET ${LOADABLE_EXTENSION_NAME}
      APPEND
      PROPERTY LINK_DEPENDS "${LANCE_VANE_VERSION_SCRIPT}")
    target_link_options(
      ${LOADABLE_EXTENSION_NAME}
      PRIVATE
      "-Wl,--version-script=${LANCE_VANE_VERSION_SCRIPT}"
      -Wl,--exclude-libs,ALL
      -Wl,-Bsymbolic
      -Wl,--gc-sections
      -Wl,--strip-all
      -Wl,-z,defs)
  else()
    message(FATAL_ERROR "Unsupported Vane provider toolchain")
  endif()
endfunction()
