#include <cpuid.h>
#include <immintrin.h>

#include <cstdint>

#include "config.h"
#include "layout.h"

namespace ulayfs::pmem {

static inline void persist_cl(void *p) {
  if constexpr (BuildOptions::support_clwb)
    return _mm_clwb(p);
  else if constexpr (BuildOptions::support_clflushopt)
    return _mm_clflushopt(p);
  else
    return _mm_clflush(p);
}

static inline void persist(char *buf, uint32_t len, bool fence) {
  len = len + ((unsigned long)(buf) & (pmem::CACHELINE_SIZE - 1));
  for (uint32_t i = 0; i < len; i += pmem::CACHELINE_SIZE) persist_cl(buf + i);
  if (fence) _mm_sfence();
}

}  // namespace ulayfs::pmem