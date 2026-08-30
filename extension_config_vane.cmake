set(LANCE_VANE_EXTENSION_CONFIG_VANE TRUE)
set(LANCE_VANE_DISTRIBUTED
    ON
    CACHE BOOL "Build the Vane distributed Lance scan and write adapters" FORCE)

# The wheel build uses the extension manifest for host tools such as protoc,
# while Vane owns the C++ libraries linked into vane._native. Keep Protobuf and
# its Abseil dependencies on Vane's single vcpkg prefix so two incompatible
# inline namespaces cannot be linked into the same wheel.
if(DEFINED ENV{VANE_VCPKG_INSTALLED_DIR}
   AND NOT "$ENV{VANE_VCPKG_INSTALLED_DIR}" STREQUAL "")
  if(NOT DEFINED ENV{VCPKG_TARGET_TRIPLET} OR "$ENV{VCPKG_TARGET_TRIPLET}"
                                              STREQUAL "")
    message(
      FATAL_ERROR
        "VCPKG_TARGET_TRIPLET is required with VANE_VCPKG_INSTALLED_DIR")
  endif()

  set(_LANCE_VANE_DEPENDENCY_PREFIX
      "$ENV{VANE_VCPKG_INSTALLED_DIR}/$ENV{VCPKG_TARGET_TRIPLET}")
  # Arrow asks for the lowercase protobuf package before gRPC asks for the
  # uppercase Protobuf package. Pin both CMake package-name variants.
  foreach(_LANCE_PACKAGE_DIR IN
          ITEMS "protobuf_DIR;protobuf" "Protobuf_DIR;protobuf" "absl_DIR;absl"
                "utf8_range_DIR;utf8_range")
    list(GET _LANCE_PACKAGE_DIR 0 _LANCE_PACKAGE_VARIABLE)
    list(GET _LANCE_PACKAGE_DIR 1 _LANCE_PACKAGE_NAME)
    set(_LANCE_PACKAGE_PATH
        "${_LANCE_VANE_DEPENDENCY_PREFIX}/share/${_LANCE_PACKAGE_NAME}")
    if(NOT IS_DIRECTORY "${_LANCE_PACKAGE_PATH}")
      message(
        FATAL_ERROR "Missing Vane dependency package: ${_LANCE_PACKAGE_PATH}")
    endif()
    set(${_LANCE_PACKAGE_VARIABLE}
        "${_LANCE_PACKAGE_PATH}"
        CACHE PATH "Vane-owned package directory" FORCE)
  endforeach()
  unset(_LANCE_PACKAGE_DIR)
  unset(_LANCE_PACKAGE_NAME)
  unset(_LANCE_PACKAGE_PATH)
  unset(_LANCE_PACKAGE_VARIABLE)
  unset(_LANCE_VANE_DEPENDENCY_PREFIX)
endif()

include(${CMAKE_CURRENT_LIST_DIR}/extension_config.cmake)
