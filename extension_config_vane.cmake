set(LANCE_VANE_EXTENSION_CONFIG_VANE TRUE)
set(LANCE_VANE_DISTRIBUTED
    ON
    CACHE BOOL "Build the Vane distributed Lance scan adapter" FORCE)
include(${CMAKE_CURRENT_LIST_DIR}/extension_config.cmake)
unset(LANCE_VANE_EXTENSION_CONFIG_VANE)
