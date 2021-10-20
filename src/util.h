#include <cpuid.h>
#include <immintrin.h>

#include <cstdint>

#include "config.h"
#include "layout.h"

#define IS_ALIGNED(x, a) (((uint64_t)x & (a - 1)) == 0)

namespace ulayfs::pmem {
static inline void persist_cl_unordered(void *p) {
  if constexpr (BuildOptions::support_clwb)
    return _mm_clwb(p);
  else if constexpr (BuildOptions::support_clflushopt)
    return _mm_clflushopt(p);
  else
    return _mm_clflush(p);
}

static inline void persist_cl_ordered(void *p) {
  persist_cl_unordered(p);
  _mm_sfence();
}

static inline void persist_unordered(void *buf, uint32_t len) {
  if (!IS_ALIGNED(buf, pmem::CACHELINE_SIZE)) len++;
  for (uint32_t i = 0; i < len; i += pmem::CACHELINE_SIZE)
    persist_cl_unordered((char *)buf + i);
}

static inline void persist_ordered(void *buf, uint32_t len) {
  persist_unordered(buf, len);
  _mm_sfence();
}
}  // namespace ulayfs::pmem