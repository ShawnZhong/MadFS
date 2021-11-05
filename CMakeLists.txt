cmake_minimum_required(VERSION 3.16)
project(uLayFS)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "\
    ${CMAKE_CXX_FLAGS} \
    -march=native \
    -Werror -Wall -Wextra \
    -Wno-unused-parameter \
    -Wno-unused-function \
    -Wno-unused-variable \
")

set(CMAKE_CXX_FLAGS_DEBUG "\
    ${CMAKE_CXX_FLAGS_DEBUG} \
    -fsanitize=undefined \
")

add_library(ulayfs SHARED
    src/alloc.h src/alloc.cpp
    src/bitmap.h
    src/block.h
    src/btable.h
    src/entry.h
    src/file.h src/file.cpp
    src/futex.h
    src/layout.h
    src/lib.h src/lib.cpp
    src/log.h
    src/mtable.h
    src/params.h
    src/posix.h
    src/tx.h src/tx.cpp
    src/utils.h
    )
target_link_libraries(ulayfs dl)

# enable link time optimization
set_property(TARGET ulayfs PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)

# build options
option(MEM_PROTECT "Enable memory protection to avoid stray writes" OFF)
option(NO_SHARING "Assume that no two processes concurrently operate on the same file" OFF)
option(RELAXED "Relax visibility and serialization guarantee on concurrent access" OFF)
option(USE_HUGE_PAGE "Use huge page for mmap" ON)
option(USE_MAP_SYNC "Use MAP_SYNC for mmap" ON)
option(FORCE_MAP_POPULATE "Always use MAP_POPULATE for mmap" OFF)

# Get the current working branch
execute_process(
    COMMAND git rev-parse --abbrev-ref HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_BRANCH
    OUTPUT_STRIP_TRAILING_WHITESPACE)

# Get the latest commit hash
execute_process(
    COMMAND git rev-parse HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE)

configure_file("src/config.h.in" "config.h")

include_directories(${CMAKE_CURRENT_BINARY_DIR})

# tests
include_directories(src)

add_executable(test_basic test/test_basic.cpp)
target_link_libraries(test_basic ulayfs)

add_executable(test_futex test/test_futex.cpp)

add_executable(test_rw test/test_rw.cpp)
target_link_libraries(test_rw ulayfs)

add_executable(test_sync test/test_sync.cpp)
target_link_libraries(test_sync ulayfs pthread)
