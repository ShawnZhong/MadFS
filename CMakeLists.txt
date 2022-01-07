cmake_minimum_required(VERSION 3.16)
project(uLayFS)
set(CMAKE_CXX_STANDARD 20)

add_library(ulayfs SHARED
    src/alloc.h src/alloc.cpp
    src/bitmap.h
    src/block.h
    src/btable.h src/btable.cpp
    src/const.h
    src/debug.h src/debug.cpp
    src/entry.h
    src/file.h src/file.cpp
    src/flock.h
    src/gc.h
    src/idx.h
    src/lib.h src/lib.cpp
    src/log.h src/log.cpp
    src/mtable.h src/mtable.cpp
    src/offset.h src/offset.cpp
    src/posix.h src/posix.cpp
    src/convert.h
    src/tx.h src/tx.cpp
    src/utils.h src/utils.cpp
    )

#[[
build flags
]]

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-function -Wno-unused-private-field")
target_compile_options(ulayfs PRIVATE -march=native -Werror)
set_property(TARGET ulayfs PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE) # LTO

#[[
dependencies
]]

include(FetchContent)
FetchContent_Declare(tbb URL https://github.com/oneapi-src/oneTBB/releases/download/v2021.5.0/oneapi-tbb-2021.5.0-lin.tgz)
FetchContent_MakeAvailable(tbb)

find_package(TBB REQUIRED HINTS ${tbb_SOURCE_DIR}/lib/cmake/tbb)
target_link_libraries(ulayfs dl pthread TBB::tbb)


#[[
build options
]]

option(ULAYFS_MEM_PROTECT "Enable memory protection to avoid stray writes" OFF)
option(ULAYFS_NO_SHARING "Assume that no two processes concurrently operate on the same file" OFF)
option(ULAYFS_RELAXED "Relax visibility and serialization guarantee on concurrent access" OFF)
option(ULAYFS_USE_MAP_SYNC "Use MAP_SYNC for mmap" ON)
option(ULAYFS_FORCE_MAP_POPULATE "Always use MAP_POPULATE for mmap" OFF)
option(ULAYFS_TX_FLUSH_ONLY_FSYNC "Only flush transaction entries in fsync" OFF)
option(ULAYFS_USE_PMEMCHECK "Enable pmemcheck macros" OFF)

execute_process(COMMAND git rev-parse --abbrev-ref HEAD
    OUTPUT_VARIABLE GIT_BRANCH
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if (NOT GIT_BRANCH)
    set(GIT_BRANCH Unknown)
endif ()

execute_process(COMMAND git rev-parse HEAD
    OUTPUT_VARIABLE GIT_COMMIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if (NOT GIT_COMMIT_HASH)
    set(GIT_COMMIT_HASH Unknown)
endif ()

#[[
tests, benchmarks, and tools
]]

option(ULAYFS_BUILD_TESTS "Build tests" ON)
option(ULAYFS_BUILD_TOOLS "Build tools" ON)
option(ULAYFS_BUILD_BENCH "Build benchmarks" OFF)

if (ULAYFS_BUILD_TESTS)
    include_directories(src)

    add_executable(test_basic test/test_basic.cpp test/common.h)
    add_executable(test_rw test/test_rw.cpp test/common.h)
    add_executable(test_sync test/test_sync.cpp test/common.h)

    target_link_libraries(test_basic ulayfs)
    target_link_libraries(test_rw ulayfs)
    target_link_libraries(test_sync ulayfs pthread)
endif ()

if (ULAYFS_BUILD_TOOLS)
    add_executable(info tools/info.cpp)
    add_executable(to_ulayfs tools/to_ulayfs.cpp)
    add_executable(from_ulayfs tools/from_ulayfs.cpp)

    target_link_libraries(info ulayfs)
    target_link_libraries(to_ulayfs ulayfs)
    target_link_libraries(from_ulayfs ulayfs)
endif ()

if (ULAYFS_BUILD_BENCH)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

    unset(CMAKE_CXX_STANDARD)
    FetchContent_Declare(
        leveldb
        GIT_REPOSITORY https://github.com/google/leveldb.git
        GIT_TAG 8f464e7f68fd9d50ed39b2866ef8dac9c837439d
    )
    FetchContent_MakeAvailable(leveldb)
    set(CMAKE_CXX_STANDARD 20)

    FetchContent_Declare(cxxopts URL https://github.com/jarro2783/cxxopts/archive/refs/tags/v2.2.0.zip)
    FetchContent_MakeAvailable(cxxopts)

    add_executable(leveldb_ycsb bench/leveldb_ycsb.cpp)
    add_executable(micro_st bench/micro_st.cpp bench/common.h)
    add_executable(micro_mt bench/micro_mt.cpp bench/common.h)
    add_executable(micro_meta bench/micro_meta.cpp bench/common.h)

    target_link_libraries(leveldb_ycsb cxxopts leveldb)
    target_link_libraries(micro_st benchmark::benchmark)
    target_link_libraries(micro_mt benchmark::benchmark)
    target_link_libraries(micro_meta benchmark::benchmark)
endif ()

#[[
sanitizers and tooling
]]

set(CMAKE_CXX_FLAGS_ASAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler during AddressSanitizer build." FORCE)

set(CMAKE_SHARED_LINKER_FLAGS_ASAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address" CACHE STRING
    "Flags used by the C++ linker during AddressSanitizer build." FORCE)

set(CMAKE_CXX_FLAGS_UBSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined" CACHE STRING
    "Flags used by the C++ compiler during UndefinedBehaviourSanitizer build." FORCE)

set(CMAKE_CXX_FLAGS_MSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=memory -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler during MemorySanitizer build." FORCE)

set(CMAKE_CXX_FLAGS_TSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread" CACHE STRING
    "Flags used by the C++ compiler during ThreadSanitizer build." FORCE)

set(CMAKE_CXX_FLAGS_PMEMCHECK
    "${CMAKE_CXX_FLAGS_DEBUG} -mno-avx512f" CACHE STRING
    "Flags used by the C++ compiler during Pmemcheck build." FORCE)

if (${CMAKE_BUILD_TYPE} STREQUAL "pmemcheck")
    set(ULAYFS_USE_PMEMCHECK ON)
    FetchContent_Declare(pmemcheck URL https://github.com/ShawnZhong/pmemcheck/releases/download/v3.17/pmemcheck.tgz)
    FetchContent_MakeAvailable(pmemcheck)
    include_directories(${pmemcheck_SOURCE_DIR}/include)
endif ()

set(CMAKE_CXX_FLAGS_PROFILE
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}" CACHE STRING
    "Flags used by the C++ compiler during Profile build." FORCE)

if (${CMAKE_BUILD_TYPE} STREQUAL "profile")
    FetchContent_Declare(flamegraph GIT_REPOSITORY https://github.com/brendangregg/FlameGraph)
    FetchContent_MakeAvailable(flamegraph)
endif ()

configure_file("src/config.h.in" "config.h")
include_directories(${CMAKE_CURRENT_BINARY_DIR})
