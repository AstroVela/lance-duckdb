# SPDX-License-Identifier: MIT

# Configuration-dependent OUTPUT requires CMake 3.20. A single custom command
# prevents Visual Studio/Xcode from executing an inactive profile's build rule.
function(lance_add_rust_ffi_build manifest)
  get_filename_component(rust_working_directory "${manifest}" DIRECTORY)
  if(NOT LANCE_CARGO_COMMAND)
    set(LANCE_CARGO_COMMAND cargo)
  endif()
  set(cargo_profile "$<IF:$<CONFIG:Debug>,dev,release>")
  set(rust_profile "$<IF:$<CONFIG:Debug>,debug,release>")
  set(rust_output
      "${RUST_TARGET_DIR}/${RUST_PLATFORM_TARGET}/${rust_profile}/${RUST_STATICLIB_NAME}"
  )
  add_custom_command(
    OUTPUT "${rust_output}"
    COMMAND
      ${CMAKE_COMMAND} -E env ${RUST_CARGO_ENV} ${LANCE_CARGO_COMMAND} build
      ${RUST_CARGO_FEATURE_ARGS} ${RUST_CARGO_REPRODUCIBILITY_ARGS}
      "--manifest-path=${manifest}" "--profile=${cargo_profile}"
      "--target=${RUST_PLATFORM_TARGET}"
    WORKING_DIRECTORY "${rust_working_directory}"
    DEPENDS ${RUST_FFI_DEPENDS}
    VERBATIM)
  add_custom_target(lance_duckdb_ffi_build DEPENDS "${rust_output}")
endfunction()
