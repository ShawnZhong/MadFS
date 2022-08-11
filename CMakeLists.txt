cmake_minimum_required(VERSION 3.16)
project(uLayFS)
set(CMAKE_CXX_STANDARD 20)
include(FetchContent)

add_library(ulayfs SHARED
    src/alloc.h src/alloc.cpp
    src/bitmap.h
    src/block/block.h
    src/block/log.h
    src/block/meta.h
    src/block/tx.h
    src/blk_table.h src/blk_table.cpp
    src/const.h
    src/counter.h
    src/debug.h src/debug.cpp
    src/entry.h
    src/file.h src/file.cpp
    src/gc.h
    src/idx.h
    src/lib.h src/lib.cpp
    src/logging.h src/logging.cpp
    src/mem_table.h
    src/tx/offset.h
    src/persist.h
    src/posix.h src/posix.cpp
    src/convert.h
    src/tx/cursor.h
    src/tx/lock.h
    src/tx/mgr.h src/tx/mgr.cpp
    src/tx/tx.h
    src/tx/read.h
    src/tx/write.h
    src/tx/write_aligned.h
    src/tx/write_unaligned.h
    src/utils.h
    )

#[[
build flags
]]
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Wno-unused")
target_compile_options(ulayfs PRIVATE -Werror)
set_property(TARGET ulayfs PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE) # LTO

#[[
build options
]]

option(ULAYFS_USE_MAP_SYNC "Use MAP_SYNC for mmap" ON)
option(ULAYFS_FORCE_MAP_POPULATE "Always use MAP_POPULATE for mmap" ON)
option(ULAYFS_TX_FLUSH_ONLY_FSYNC "Only flush transaction entries in fsync" OFF)
option(ULAYFS_USE_PMEMCHECK "Enable pmemcheck macros" OFF)
option(ULAYFS_COUNTER "Enable performance counters" OFF)
option(ULAYFS_CC_OCC "Use OCC for CC" ON)
option(ULAYFS_CC_MUTEX "Use robust mutex for CC" OFF)
option(ULAYFS_CC_SPINLOCK "Use spinlock for CC" OFF)
option(ULAYFS_CC_RWLOCK "Use rwlock for CC" OFF)

include(cmake/tooling.cmake)
include(cmake/libpmem2.cmake)

configure_file("src/config.h.in" "config.h" @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

#[[
dependencies
]]

FetchContent_Declare(
    magic-enum
    GIT_REPOSITORY https://github.com/Neargye/magic_enum.git
    GIT_TAG v0.8.0
)
FetchContent_Declare(tbb URL https://github.com/oneapi-src/oneTBB/releases/download/v2021.5.0/oneapi-tbb-2021.5.0-lin.tgz)
FetchContent_MakeAvailable(magic-enum tbb)

find_package(TBB REQUIRED HINTS ${tbb_SOURCE_DIR}/lib/cmake/tbb)
target_link_libraries(ulayfs pmem2 dl pthread atomic TBB::tbb magic_enum::magic_enum)

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

    FetchContent_Declare(
        leveldb
        GIT_REPOSITORY https://github.com/google/leveldb.git
        GIT_TAG e4ccaa0c9cb1a019df3df74eb5db4ec55e9e02ed
        PATCH_COMMAND git apply --reject ${CMAKE_SOURCE_DIR}/bench/leveldb.patch || true
    )
    FetchContent_Declare(cxxopts URL https://github.com/jarro2783/cxxopts/archive/refs/tags/v2.2.0.zip)
    FetchContent_Declare(
        tpcc_sqlite
        GIT_REPOSITORY https://github.com/ShawnZhong/tpcc_sqlite.git
    )
    FetchContent_MakeAvailable(leveldb cxxopts tpcc_sqlite)

    add_executable(leveldb_ycsb bench/leveldb_ycsb.cpp)
    add_executable(micro_st bench/micro_st.cpp bench/common.h)
    add_executable(micro_mt bench/micro_mt.cpp bench/common.h)
    add_executable(micro_meta bench/micro_meta.cpp bench/common.h)

    target_link_libraries(leveldb_ycsb cxxopts leveldb)
    target_link_libraries(micro_st benchmark::benchmark)
    target_link_libraries(micro_mt benchmark::benchmark)
    target_link_libraries(micro_meta benchmark::benchmark)
endif ()
