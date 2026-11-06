# SPDX-License-Identifier: MIT

# Configuration-dependent OUTPUT requires CMake 3.20. A single custom command
# prevents Visual Studio/Xcode from executing an inactive profile's build rule.
function(lance_add_rust_ffi_build manifest)
  get_filename_component(rust_working_directory "${manifest}" DIRECTORY)
  if(NOT LANCE_CARGO_COMMAND)
    set(LANCE_CARGO_COMMAND cargo)
  endif()
  if(WIN32
     AND MSVC
     AND RUST_PLATFORM_TARGET MATCHES "windows-msvc$")
    # Git Bash puts its unrelated link.exe before MSVC on PATH. Use the linker
    # selected by CMake, including when Cargo links build scripts.
    string(TOUPPER "${RUST_PLATFORM_TARGET}" rust_linker_target)
    string(REPLACE "-" "_" rust_linker_target "${rust_linker_target}")
    list(APPEND RUST_CARGO_ENV
         "CARGO_TARGET_${rust_linker_target}_LINKER=${CMAKE_LINKER}")
  endif()
  set(cargo_profile "$<IF:$<CONFIG:Debug>,dev,release>")
  set(rust_profile "$<IF:$<CONFIG:Debug>,debug,release>")
  set(rust_output
      "${RUST_TARGET_DIR}/${RUST_PLATFORM_TARGET}/${rust_profile}/${RUST_STATICLIB_NAME}"
  )
  set(rust_finalize_command)
  if(MSVC AND LANCE_VANE_DYNAMIC_PROVIDER)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    set(rust_export_filter
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../scripts/hide_msvc_archive_exports.py"
    )
    list(APPEND RUST_FFI_DEPENDS "${rust_export_filter}")
    # Static dependencies can carry explicit dllexport linker directives that
    # override the provider's .def export list. Keep the whole Rust closure
    # private without changing dependency code or cryptographic configuration.
    set(rust_finalize_command COMMAND "${Python3_EXECUTABLE}"
                              "${rust_export_filter}" "${rust_output}")
  endif()
  add_custom_command(
    OUTPUT "${rust_output}"
    COMMAND
      ${CMAKE_COMMAND} -E env ${RUST_CARGO_ENV} ${LANCE_CARGO_COMMAND} build
      ${RUST_CARGO_FEATURE_ARGS} ${RUST_CARGO_REPRODUCIBILITY_ARGS}
      "--manifest-path=${manifest}" "--profile=${cargo_profile}"
      "--target=${RUST_PLATFORM_TARGET}" ${rust_finalize_command}
    WORKING_DIRECTORY "${rust_working_directory}"
    DEPENDS ${RUST_FFI_DEPENDS}
    VERBATIM)
  add_custom_target(lance_duckdb_ffi_build DEPENDS "${rust_output}")
endfunction()
