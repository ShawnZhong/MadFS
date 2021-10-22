#pragma once

#include <immintrin.h>

#include "config.h"
#include "params.h"

#define panic_if(expr, msg)                              \
  do {                                                   \
    if (expr) {                                          \
      printf("[%s:%d] %s: %m", __FILE__, __LINE__, msg); \
      exit(1);                                           \
    }                                                    \
  } while (0)

// adopted from `include/linux/align.h`
#define ALIGN_MASK(x, mask) (((x) + (mask)) & ~(mask))
#define ALIGN_UP(x, a) ALIGN_MASK((x), (a - 1))
#define IS_ALIGNED(x, a) (((uint64_t)x & (a - 1)) == 0)

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
 * persist the range [buf, buf + len) with possibly reordering
 */
static inline void persist_unfenced(void *buf, uint32_t len) {
  // adjust for cacheline alignment
  len += (uint64_t)buf & (CACHELINE_SIZE - 1);
  for (uint32_t i = 0; i < len; i += CACHELINE_SIZE)
    persist_cl_unfenced((char *)buf + i);
}

/**
 * persist the range [buf, buf + len) without reordering
 */
static inline void persist_fenced(void *buf, uint32_t len) {
  persist_unfenced(buf, len);
  _mm_sfence();
}
}  // namespace ulayfs::pmem
