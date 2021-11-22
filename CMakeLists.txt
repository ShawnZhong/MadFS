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
    -Wno-unused-private-field \
")

add_library(ulayfs SHARED
    src/alloc.h src/alloc.cpp
    src/bitmap.h
    src/block.h
    src/btable.h src/btable.cpp
    src/entry.h
    src/file.h src/file.cpp
    src/idx.h
    src/const.h
    src/lib.h src/lib.cpp
    src/log.h src/log.cpp
    src/mtable.h
    src/params.h
    src/posix.h src/posix.cpp
    src/tx.h src/tx.cpp
    src/utils.h
    )

# external dependencies
include(FetchContent)
FetchContent_Declare(tbb URL https://github.com/oneapi-src/oneTBB/releases/download/v2021.4.0/oneapi-tbb-2021.4.0-lin.tgz)
FetchContent_MakeAvailable(tbb)
set(TBB_DIR ${tbb_SOURCE_DIR}/lib/cmake/tbb)
find_package(TBB)

target_link_libraries(ulayfs dl pthread TBB::tbb)

# enable link time optimization
set_property(TARGET ulayfs PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)

# build options
option(MEM_PROTECT "Enable memory protection to avoid stray writes" OFF)
option(NO_SHARING "Assume that no two processes concurrently operate on the same file" OFF)
option(RELAXED "Relax visibility and serialization guarantee on concurrent access" OFF)
option(USE_HUGE_PAGE "Use huge page for mmap" ON)
option(USE_MAP_SYNC "Use MAP_SYNC for mmap" ON)
option(FORCE_MAP_POPULATE "Always use MAP_POPULATE for mmap" OFF)
option(TX_FLUSH_ONLY_FSYNC "Only flush transaction entries in fsync" OFF)

# check huge page support
execute_process(
    COMMAND sysctl -n vm.nr_hugepages
    OUTPUT_VARIABLE NR_HUGEPAGES
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if (${NR_HUGEPAGES} EQUAL "0")
    message(WARNING
        "Huge page not available. Disable huge page support for mmap. \
        Please use `sudo sysctl -w vm.nr_hugepages=512` to configure huge page.")
    set(USE_HUGE_PAGE OFF)
endif ()

# get the current working branch
execute_process(
    COMMAND git rev-parse --abbrev-ref HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_BRANCH
    OUTPUT_STRIP_TRAILING_WHITESPACE)

if (NOT GIT_BRANCH)
    set(GIT_BRANCH Unknown)
endif ()

# get the latest commit hash
execute_process(
    COMMAND git rev-parse HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE)

if (NOT GIT_COMMIT_HASH)
    set(GIT_COMMIT_HASH Unknown)
endif ()

configure_file("src/config.h.in" "config.h")

include_directories(${CMAKE_CURRENT_BINARY_DIR})

# tests
include_directories(src)

add_executable(test_basic test/test_basic.cpp test/common.h)
target_link_libraries(test_basic ulayfs)

add_executable(test_rw test/test_rw.cpp test/common.h)
target_link_libraries(test_rw ulayfs)

add_executable(test_sync test/test_sync.cpp test/common.h)
target_link_libraries(test_sync ulayfs)

# sanitizers
set(CMAKE_CXX_FLAGS_ASAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler for Asan build type or configuration." FORCE)

set(CMAKE_SHARED_LINKER_FLAGS_ASAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address" CACHE STRING
    "Linker lags to be used to create shared libraries for Asan build type." FORCE)

set(CMAKE_CXX_FLAGS_UBSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined" CACHE STRING
    "Flags used by the C++ compiler during UndefinedBehaviourSanitizer builds." FORCE)

set(CMAKE_CXX_FLAGS_MSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=memory -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler during MemorySanitizer builds." FORCE)

set(CMAKE_CXX_FLAGS_TSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread" CACHE STRING
    "Flags used by the C++ compiler during ThreadSanitizer builds." FORCE)

set(CMAKE_CXX_FLAGS_PMEMCHECK
    "${CMAKE_CXX_FLAGS_DEBUG} -mno-avx512f -DUSE_PMEMCHECK" CACHE STRING
    "Flags used by the C++ compiler during pmemcheck builds." FORCE)
