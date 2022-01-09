set(ULAYFS_USE_LIBPMEM2 ON)
FetchContent_Declare(pmdk URL https://github.com/pmem/pmdk/releases/download/1.11.1/pmdk-1.11.1.tar.gz)
FetchContent_MakeAvailable(pmdk)
add_library(
    pmem2 STATIC
    ${pmdk_SOURCE_DIR}/src/libpmem2/libpmem2.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/badblocks.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/badblocks_ndctl.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/config.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/deep_flush.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/errormsg.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/memops_generic.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/map.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/map_posix.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/persist.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/persist_posix.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/pmem2_utils.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/usc_ndctl.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/source.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/source_posix.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/vm_reservation.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/vm_reservation_posix.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/auto_flush_linux.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/deep_flush_linux.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/extent_linux.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/pmem2_utils_linux.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/pmem2_utils_ndctl.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/region_namespace_ndctl.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/numa_ndctl.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/init.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/cpu.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memcpy/memcpy_nt_avx.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memcpy/memcpy_nt_sse2.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memcpy/memcpy_nt_avx512f.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memset/memset_nt_avx.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memset/memset_nt_sse2.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memset/memset_nt_avx512f.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memcpy/memcpy_t_avx.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memcpy/memcpy_t_sse2.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memcpy/memcpy_t_avx512f.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memset/memset_t_avx.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memset/memset_t_sse2.c
    ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64/memset/memset_t_avx512f.c
    ${pmdk_SOURCE_DIR}/src/core/alloc.c
    ${pmdk_SOURCE_DIR}/src/core/fs_posix.c
    ${pmdk_SOURCE_DIR}/src/core/os_posix.c
    ${pmdk_SOURCE_DIR}/src/core/os_thread_posix.c
    ${pmdk_SOURCE_DIR}/src/core/out.c
    ${pmdk_SOURCE_DIR}/src/core/ravl.c
    ${pmdk_SOURCE_DIR}/src/core/ravl_interval.c
    ${pmdk_SOURCE_DIR}/src/core/util.c
    ${pmdk_SOURCE_DIR}/src/core/util_posix.c
)
set_property(TARGET pmem2 PROPERTY POSITION_INDEPENDENT_CODE ON)
target_compile_options(pmem2 PRIVATE -march=native)
target_compile_definitions(pmem2 PRIVATE -DSRCVERSION="0.2")
target_link_libraries(pmem2 ndctl)
target_include_directories(
    pmem2
    PUBLIC ${pmdk_SOURCE_DIR}/src
    PUBLIC ${pmdk_SOURCE_DIR}/src/core
    PUBLIC ${pmdk_SOURCE_DIR}/src/common
    PUBLIC ${pmdk_SOURCE_DIR}/src/libpmem2
    PUBLIC ${pmdk_SOURCE_DIR}/src/libpmem2/x86_64
    PUBLIC ${pmdk_SOURCE_DIR}/src/include
)
