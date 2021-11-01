#pragma once

#include <immintrin.h>

#include <ctime>
#include <iomanip>
#include <iostream>

#include "config.h"
#include "params.h"

#define __FILENAME__ \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define log(msg, ...)                                                       \
  do {                                                                      \
    std::time_t t = std::time(nullptr);                                     \
    std::tm *tm = std::localtime(&t);                                       \
    fprintf(stderr, "%02d:%02d:%02d [%8s:%d] " msg "\n", tm->tm_hour,       \
            tm->tm_min, tm->tm_sec, __FILENAME__, __LINE__, ##__VA_ARGS__); \
  } while (0)

#define panic_if(expr, msg, ...)                 \
  do {                                           \
    if (expr) {                                  \
      log("[PANIC] " msg ": %m", ##__VA_ARGS__); \
      exit(1);                                   \
    }                                            \
  } while (0)

#define debug(msg, ...)                   \
  do {                                    \
    if constexpr (BuildOptions::debug) {  \
      log("[DEBUG] " msg, ##__VA_ARGS__); \
    }                                     \
  } while (0)

// adopted from `include/linux/align.h`
#define ALIGN_MASK(x, mask) (((x) + (mask)) & ~(mask))
#define ALIGN_UP(x, a) ALIGN_MASK((x), ((a)-1))
#define ALIGN_DOWN(x, a) ((x) & ~((a)-1))
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
}  // namespace ulayfs::pmem
