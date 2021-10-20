#include <cpuid.h>
#include <immintrin.h>

#include <cstdint>

namespace ulayfs::pmem {

#define CACHELINE_SIZE (64)

int support_clflushopt = 0;
int support_clwb = 0;

static inline void cpuid(unsigned func, unsigned subfunc, unsigned cpuinfo[4]) {
  __cpuid_count(func, subfunc, cpuinfo[0], cpuinfo[1], cpuinfo[2], cpuinfo[3]);
}

static inline int is_cpu_feature_present(unsigned func, unsigned reg,
                                         unsigned bit) {
  unsigned cpuinfo[4] = {0};

  /* check CPUID level first */
  cpuid(0x0, 0x0, cpuinfo);
  if (cpuinfo[0] < func) return 0;

  cpuid(func, 0x0, cpuinfo);
  return (cpuinfo[reg] & bit) != 0;
}

static inline int is_cpu_clflushopt_present(void) {
  return is_cpu_feature_present(0x7, 1, bit_CLFLUSHOPT);
}

static inline int is_cpu_clwb_present(void) {
  return is_cpu_feature_present(0x7, 1, bit_CLWB);
}

static inline void check_arch_support(void) {
  if (is_cpu_clflushopt_present()) support_clflushopt = 1;
  if (is_cpu_clwb_present()) support_clwb = 1;
}

static inline void ulayfs_flush_buffer(char *buf, uint32_t len, bool fence) {
  uint32_t i;
  len = len + ((unsigned long)(buf) & (CACHELINE_SIZE - 1));
  if (support_clwb) {
    for (i = 0; i < len; i += CACHELINE_SIZE) _mm_clwb(buf + i);
  } else if (support_clflushopt) {
    for (i = 0; i < len; i += CACHELINE_SIZE) _mm_clflushopt(buf + i);
  } else {
    for (i = 0; i < len; i += CACHELINE_SIZE) _mm_clflush(buf + i);
  }
  // Do a fence only if asked
  if (fence) _mm_sfence();
}

}  // namespace ulayfs::pmem