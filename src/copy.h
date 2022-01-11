#pragma once

#include "utils.h"

#if ULAYFS_USE_LIBPMEM2
#include <libpmem2.h>
#include <libpmem2/map.h>
#include <libpmem2/persist.h>
void pmem2_set_mem_fns(struct pmem2_map *map);
static pmem2_memcpy_fn pmem2_memcpy = []() {
  struct pmem2_map map;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  map.effective_granularity = PMEM2_GRANULARITY_CACHE_LINE;
  pmem2_set_mem_fns(&map);
  return map.memcpy_fn;
}();
#endif

namespace ulayfs {
namespace pmem {
namespace internal {
/**
 * Different implementation of memcpy:
 * - native: just memcpy then flush
 * - kernel: linux kernel implementation, use movnti
 */

/**
 * Naive implementation: use memcpy then flush
 */
static inline void memcpy_native(void *dst, const void *src, size_t size) {
  memcpy(dst, src, size);
  persist_unfenced(dst, size);
}

/**
 * Linux kernel's implementation of memcpy_flushcache
 * from: /arch/x86/lib/usercopy_64.c
 * use as many movnti as possible
 */
static inline void memcpy_kernel(void *dst, const void *src, size_t size) {
  unsigned long dest = (unsigned long)dst;
  unsigned long source = (unsigned long)src;

  /* cache copy and flush to align dest */
  if (!IS_ALIGNED(dest, 8)) {
    size_t len = std::min(size, ALIGN_UP(dest, 8) - dest);
    memcpy_native((void *)dest, (void *)source, len);
    dest += len;
    source += len;
    size -= len;
    if (!size) return;
  }

  /* 4x8 movnti loop */
  while (size >= 32) {
    asm("movq    (%0), %%r8\n"
        "movq   8(%0), %%r9\n"
        "movq  16(%0), %%r10\n"
        "movq  24(%0), %%r11\n"
        "movnti  %%r8,   (%1)\n"
        "movnti  %%r9,  8(%1)\n"
        "movnti %%r10, 16(%1)\n"
        "movnti %%r11, 24(%1)\n" ::"r"(source),
        "r"(dest)
        : "memory", "r8", "r9", "r10", "r11");
    dest += 32;
    source += 32;
    size -= 32;
  }

  /* 1x8 movnti loop */
  while (size >= 8) {
    asm("movq    (%0), %%r8\n"
        "movnti  %%r8,   (%1)\n" ::"r"(source),
        "r"(dest)
        : "memory", "r8");
    dest += 8;
    source += 8;
    size -= 8;
  }

  /* 1x4 movnti loop */
  while (size >= 4) {
    asm("movl    (%0), %%r8d\n"
        "movnti  %%r8d,   (%1)\n" ::"r"(source),
        "r"(dest)
        : "memory", "r8");
    dest += 4;
    source += 4;
    size -= 4;
  }

  /* cache copy for remaining bytes */
  if (size) memcpy_native((void *)dest, (void *)source, size);
}

}  // namespace internal

static inline void memcpy_persist(void *dst, const void *src, size_t size,
                                  bool fenced = false) {
  if constexpr (BuildOptions::persist == BuildOptions::Persist::KERNEL) {
    internal::memcpy_kernel(dst, src, size);
  } else if constexpr (BuildOptions::persist == BuildOptions::Persist::PMDK) {
#if ULAYFS_USE_LIBPMEM2
    pmem2_memcpy(dst, src, size, PMEM2_F_MEM_NONTEMPORAL);
#else
    assert(false);
#endif
  } else if constexpr (BuildOptions::persist == BuildOptions::Persist::NATIVE) {
    internal::memcpy_native(dst, src, size);
  }
  if (fenced) _mm_sfence();
}
}  // namespace pmem

namespace dram {
static inline void memcpy(void *dst, const void *src, size_t size) {
  if constexpr (BuildOptions::persist == BuildOptions::Persist::PMDK) {
#if ULAYFS_USE_LIBPMEM2
    pmem2_memcpy(dst, src, size, PMEM2_F_MEM_TEMPORAL | PMEM2_F_MEM_NOFLUSH);
#else
    assert(false);
#endif
  } else {
    std::memcpy(dst, src, size);
  }
}
}  // namespace dram
}  // namespace ulayfs
