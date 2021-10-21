cmake_minimum_required(VERSION 3.16)
project(uLayFS)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

add_library(ulayfs SHARED
    src/alloc.h src/alloc.cpp
    src/btable.h
    src/file.h src/file.cpp
    src/futex.h
    src/layout.h src/layout.cpp
    src/lib.h src/lib.cpp
    src/mtable.h
    src/params.h
    src/posix.h
    src/utils.h
    )
target_link_libraries(ulayfs dl)

add_executable(test_basic test/basic.c)
target_link_libraries(test_basic ulayfs)

add_executable(test_futex test/futex.cpp)

add_executable(test_alloc test/alloc.cpp)
target_link_libraries(test_alloc ulayfs)

add_executable(test_utils test/utils.cpp)
target_link_libraries(test_utils ulayfs)

# build options
option(MEM_PROTECT "Avoid stray writes" OFF)
option(RELAXED "Relaxed" OFF)
option(USE_HUGE_PAGE "Use huge page for mmap" OFF)
option(USE_MAP_SYNC "Use MAP_SYNC for mmap" OFF)
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
