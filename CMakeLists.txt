cmake_minimum_required(VERSION 3.16)
project(MadFS)
set(CMAKE_CXX_STANDARD 20)
include(FetchContent)

file(GLOB_RECURSE madfs_SRC CONFIGURE_DEPENDS "src/*.h" "src/*.cpp")
add_library(madfs SHARED ${madfs_SRC})

#[[
build flags
]]
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Wno-unused")
target_compile_options(madfs PRIVATE -Werror -Wsign-conversion)
set_property(TARGET madfs PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE) # LTO

#[[
build options
]]

option(MADFS_MAP_SYNC "Use MAP_SYNC for mmap" ON)
option(MADFS_MAP_POPULATE "Use MAP_POPULATE for mmap" ON)
option(MADFS_TX_FLUSH_ONLY_FSYNC "Only flush transaction entries in fsync" ON)
option(MADFS_USE_PMEMCHECK "Enable pmemcheck macros" OFF)
option(MADFS_TIMER "Enable timer" OFF)
option(MADFS_CC_OCC "Use OCC for CC" ON)
option(MADFS_CC_MUTEX "Use robust mutex for CC" OFF)
option(MADFS_CC_SPINLOCK "Use spinlock for CC" OFF)
option(MADFS_CC_RWLOCK "Use rwlock for CC" OFF)

include(cmake/tooling.cmake)
include(cmake/libpmem2.cmake)

configure_file("src/config.h.in" "config.h" @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(src)

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
target_link_libraries(madfs pmem2 dl pthread atomic TBB::tbb magic_enum::magic_enum)

#[[
tests, benchmarks, and tools
]]

option(MADFS_BUILD_TESTS "Build tests" ON)
option(MADFS_BUILD_TOOLS "Build tools" ON)
option(MADFS_BUILD_BENCH "Build benchmarks" OFF)

if (MADFS_BUILD_TESTS)
    include_directories(src)

    add_executable(test_basic test/test_basic.cpp test/common.h)
    add_executable(test_rw test/test_rw.cpp test/common.h)
    add_executable(test_sync test/test_sync.cpp test/common.h)
    add_executable(test_gc test/test_gc.cpp test/common.h)

    target_link_libraries(test_basic madfs)
    target_link_libraries(test_rw madfs)
    target_link_libraries(test_sync madfs pthread)
    target_link_libraries(test_gc madfs)
endif ()

if (MADFS_BUILD_TOOLS)
    add_executable(info tools/info.cpp)
    add_executable(to_madfs tools/to_madfs.cpp)
    add_executable(from_madfs tools/from_madfs.cpp)
    add_executable(gc tools/gc.cpp)

    target_link_libraries(info madfs)
    target_link_libraries(to_madfs madfs)
    target_link_libraries(from_madfs madfs)
    target_link_libraries(gc madfs)
endif ()

if (MADFS_BUILD_BENCH)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

    FetchContent_Declare(
        leveldb
        GIT_REPOSITORY https://github.com/google/leveldb.git
        GIT_TAG e4ccaa0c9cb1a019df3df74eb5db4ec55e9e02ed
        PATCH_COMMAND git apply --reject ${CMAKE_SOURCE_DIR}/bench/leveldb.patch || true
    )
    FetchContent_Declare(cxxopts URL https://github.com/jarro2783/cxxopts/archive/refs/tags/v3.0.0.zip)
    FetchContent_Declare(
        tpcc_sqlite
        GIT_REPOSITORY https://github.com/ShawnZhong/tpcc_sqlite.git
    )
    FetchContent_MakeAvailable(leveldb cxxopts tpcc_sqlite)

    add_executable(leveldb_ycsb bench/leveldb_ycsb.cpp)
    add_executable(micro_st bench/micro_st.cpp bench/common.h)
    add_executable(micro_mt bench/micro_mt.cpp bench/common.h)
    add_executable(micro_gc bench/micro_gc.cpp)
    add_executable(bench_open bench/bench_open.cpp)

    target_link_libraries(leveldb_ycsb cxxopts leveldb)
    target_link_libraries(micro_st benchmark::benchmark)
    target_link_libraries(micro_mt benchmark::benchmark)
    target_link_libraries(micro_gc cxxopts madfs) # We always want to link madfs for gc
    target_link_libraries(bench_open cxxopts madfs)
endif ()
