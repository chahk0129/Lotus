project(rdma_db)
cmake_minimum_required(VERSION 3.16.0 FATAL_ERROR)

# -----------------------------------------------
# Environment
# -----------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wno-deprecated-declarations")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -O0" CACHE INTERNAL "compiler options" FORCE)
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -g -fsanitize=address -fno-omit-frame-pointer" CACHE INTERNAL "compiler options" FORCE)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS} -O3" CACHE INTERNAL "compiler options" FORCE)

set(LINK_FLAGS "-lpthread -libverbs -ljemalloc -lcityhash -lboost_system -lboost_coroutine")

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE)
    message("${BoldGreen}Release mode: ${ColourReset} " ${CMAKE_BUILD_TYPE})
endif()

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

# -----------------------------------------------
# Includes
# -----------------------------------------------
include_directories(${PROJECT_SOURCE_DIR} index/libcuckoo) 
#find_library(JEMALLOC_LIB jemalloc)

# -----------------------------------------------
# Files
# -----------------------------------------------
file(GLOB_RECURSE GLOBAL_SRC "*.cc" "*.cpp" "*.c") 

# Exclude unwanted files and directories
list(FILTER GLOBAL_SRC EXCLUDE REGEX ".*template\\.cc$")
list(FILTER GLOBAL_SRC EXCLUDE REGEX ".*template\\.cpp$")
list(FILTER GLOBAL_SRC EXCLUDE REGEX ".*/test/.*")
list(FILTER GLOBAL_SRC EXCLUDE REGEX ".*/CMakeFiles/.*")
list(FILTER GLOBAL_SRC EXCLUDE REGEX ".*/build/.*")
list(FILTER GLOBAL_SRC EXCLUDE REGEX ".*/cmake-build-.*")

# Create core library
add_library(db_core ${GLOBAL_SRC})

# -----------------------------------------------
# Benchmark Tests --- Local Index
# -----------------------------------------------
add_executable(idx_test test/local_tree.cpp) 
target_link_libraries(idx_test db_core ${LINK_FLAGS})
#target_link_libraries(idx_test db_core pthread ibverbs ${JEMALLOC_LIB})

add_executable(skiplist_test test/skiplist.cpp) 
target_link_libraries(skiplist_test db_core ${LINK_FLAGS})
#target_link_libraries(skiplist_test db_core pthread ibverbs ${JEMALLOC_LIB})

# -----------------------------------------------
# Benchmark Tests --- YCSB
# -----------------------------------------------
add_executable(run_compute test/main.cpp)
add_executable(run_memory test/main.cpp)

target_compile_definitions(run_compute PUBLIC -DCLIENT)
target_compile_definitions(run_memory PUBLIC -DSERVER)

target_link_libraries(run_compute db_core pthread ibverbs ${LINK_FLAGS})
target_link_libraries(run_memory db_core pthread ibverbs ${LINK_FLAGS})
#target_link_libraries(run_compute db_core pthread ibverbs ${JEMALLOC_LIB} ${TBB_LIBRARIES} ${CITYHASH_LIBRARIES})
#target_link_libraries(run_memory db_core pthread ibverbs ${JEMALLOC_LIB} ${TBB_LIBRARIES} ${CITYHASH_LIBRARIES})
