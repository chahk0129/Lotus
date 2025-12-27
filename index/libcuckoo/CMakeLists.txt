cmake_minimum_required(VERSION 3.1.0)
project(libcuckoo LANGUAGES C CXX)

set(libcuckoo_VERSION_MAJOR 0)
set(libcuckoo_VERSION_MINOR 2)
set(libcuckoo_VERSION_PATCH 0)

# put these in the cache so they show up in ccmake
option (BUILD_EXAMPLES "build example libcuckoo programs")

# Add the libcuckoo interface target
add_subdirectory(libcuckoo)

# Add C interface target
add_subdirectory(libcuckoo-c)
