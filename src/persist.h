#pragma once

#include <immintrin.h>

#include <cstring>

#include "config.h"
#include "const.h"
#include "utils.h"

#if ULAYFS_USE_PMEMCHECK == 1
#include <valgrind/pmemcheck.h>
// ref: https://pmem.io/valgrind/generated/pmc-manual.html
#else
#define VALGRIND_PMC_REMOVE_PMEM_MAPPING(...) ({})
#define VALGRIND_PMC_REGISTER_PMEM_MAPPING(...) ({})
#endif

#if ULAYFS_USE_LIBPMEM2
#include <libpmem2.h>
extern pmem2_memcpy_fn pmem2_memcpy;
#else
#define pmem2_memcpy(...) ({})
#endif

namespace ulayfs::pmem {
/**
 * persist the cache line that contains p from any level of the cache
 * hierarchy using the appropriate instruction
 *
 * Note that the this instruction might be reordered
 */
static inline void persist_cl_unfenced(void *p) {
  if constexpr (BuildOptions::support_clwb)
    return _mm_clwb(p);
  else if constexpr (BuildOptions::support_clflushopt)
    return _mm_clflushopt(p);
  else
    return _mm_clflush(p);
}

/**
 * persist the cache line that contains p without reordering
 */
static inline void persist_cl_fenced(void *p) {
  persist_cl_unfenced(p);
  _mm_sfence();
}

/**
 * same as persist_cl above but take `fenced` argument
 */
static inline void persist_cl(void *p, bool fenced) {
  persist_cl_unfenced(p);
  if (fenced) _mm_sfence();
}

/**
 * persist the range [buf, buf + len) with possibly reordering
 */
static inline void persist_unfenced(void *buf, uint64_t len) {
  // adjust for cacheline alignment
  len += (uint64_t)buf & (CACHELINE_SIZE - 1);
  for (uint64_t i = 0; i < len; i += CACHELINE_SIZE)
    persist_cl_unfenced((char *)buf + i);
}

/**
 * persist the range [buf, buf + len) without reordering
 */
static inline void persist_fenced(void *buf, uint64_t len) {
  persist_unfenced(buf, len);
  _mm_sfence();
}

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
    pmem2_memcpy(dst, src, size, PMEM2_F_MEM_NONTEMPORAL);
  } else if constexpr (BuildOptions::persist == BuildOptions::Persist::NATIVE) {
    internal::memcpy_native(dst, src, size);
  }
  if (fenced) _mm_sfence();
}
}  // namespace ulayfs::pmem
