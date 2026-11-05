include("$ENV{VCPKG_ROOT}/triplets/community/arm64-osx-release.cmake")
set(VCPKG_OSX_DEPLOYMENT_TARGET 15.0)
# c-ares' link-only probe sees the SDK 27 symbol despite the older deployment
# target. Keep its portable pipe/fcntl implementation for macOS 15.
list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DHAVE_PIPE2=OFF)
