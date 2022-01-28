FetchContent_Declare(
    pmdk
    URL https://github.com/pmem/pmdk/releases/download/1.11.1/pmdk-1.11.1.tar.gz
)
FetchContent_MakeAvailable(pmdk)
set(pmdk_SOURCE_DIR ${pmdk_SOURCE_DIR}/src)
file(
    GLOB libpmem2_SRC
    ${pmdk_SOURCE_DIR}/libpmem2/libpmem2.c
    ${pmdk_SOURCE_DIR}/libpmem2/badblocks.c
    ${pmdk_SOURCE_DIR}/libpmem2/badblocks_none.c
    ${pmdk_SOURCE_DIR}/libpmem2/config.c
    ${pmdk_SOURCE_DIR}/libpmem2/deep_flush.c
    ${pmdk_SOURCE_DIR}/libpmem2/errormsg.c
    ${pmdk_SOURCE_DIR}/libpmem2/memops_generic.c
    ${pmdk_SOURCE_DIR}/libpmem2/map.c
    ${pmdk_SOURCE_DIR}/libpmem2/map_posix.c
    ${pmdk_SOURCE_DIR}/libpmem2/persist.c
    ${pmdk_SOURCE_DIR}/libpmem2/persist_posix.c
    ${pmdk_SOURCE_DIR}/libpmem2/pmem2_utils.c
    ${pmdk_SOURCE_DIR}/libpmem2/usc_none.c
    ${pmdk_SOURCE_DIR}/libpmem2/source.c
    ${pmdk_SOURCE_DIR}/libpmem2/source_posix.c
    ${pmdk_SOURCE_DIR}/libpmem2/vm_reservation.c
    ${pmdk_SOURCE_DIR}/libpmem2/vm_reservation_posix.c
    ${pmdk_SOURCE_DIR}/libpmem2/auto_flush_linux.c
    ${pmdk_SOURCE_DIR}/libpmem2/deep_flush_linux.c
    ${pmdk_SOURCE_DIR}/libpmem2/extent_linux.c
    ${pmdk_SOURCE_DIR}/libpmem2/pmem2_utils_linux.c
    ${pmdk_SOURCE_DIR}/libpmem2/pmem2_utils_none.c
    ${pmdk_SOURCE_DIR}/libpmem2/region_namespace_none.c
    ${pmdk_SOURCE_DIR}/libpmem2/numa_none.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/init.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/cpu.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memcpy/memcpy_nt_avx.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memcpy/memcpy_nt_avx512f.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memcpy/memcpy_nt_sse2.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memset/memset_nt_avx.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memset/memset_nt_avx512f.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memset/memset_nt_sse2.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memcpy/memcpy_t_avx.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memcpy/memcpy_t_avx512f.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memcpy/memcpy_t_sse2.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memset/memset_t_avx.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memset/memset_t_avx512f.c
    ${pmdk_SOURCE_DIR}/libpmem2/x86_64/memset/memset_t_sse2.c
    ${pmdk_SOURCE_DIR}/core/alloc.c
    ${pmdk_SOURCE_DIR}/core/fs_posix.c
    ${pmdk_SOURCE_DIR}/core/os_posix.c
    ${pmdk_SOURCE_DIR}/core/os_thread_posix.c
    ${pmdk_SOURCE_DIR}/core/out.c
    ${pmdk_SOURCE_DIR}/core/ravl.c
    ${pmdk_SOURCE_DIR}/core/ravl_interval.c
    ${pmdk_SOURCE_DIR}/core/util.c
    ${pmdk_SOURCE_DIR}/core/util_posix.c
)

add_library(pmem2 STATIC ${libpmem2_SRC})
set_property(TARGET pmem2 PROPERTY POSITION_INDEPENDENT_CODE ON)
target_compile_options(pmem2 PRIVATE -mavx512f)
target_compile_definitions(pmem2 PRIVATE -DSRCVERSION="0.2" -DAVX512F_AVAILABLE=1)
target_include_directories(
    pmem2
    PUBLIC ${pmdk_SOURCE_DIR}
    PUBLIC ${pmdk_SOURCE_DIR}/core
    PUBLIC ${pmdk_SOURCE_DIR}/common
    PUBLIC ${pmdk_SOURCE_DIR}/libpmem2
    PUBLIC ${pmdk_SOURCE_DIR}/libpmem2/x86_64
    PUBLIC ${pmdk_SOURCE_DIR}/include
)
