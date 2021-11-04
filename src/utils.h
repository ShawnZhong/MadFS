#pragma once

#include <immintrin.h>

#include <ctime>
#include <iomanip>
#include <iostream>

#include "config.h"
#include "params.h"

/*
 * The following macros used for assertion and logging
 * Defined as macros since we want to have access to __FILE__ and __LINE__
 */

#define FPRINTF(file, fmt, ...)                                     \
  std::time_t t = std::time(nullptr);                               \
  std::tm *tm = std::localtime(&t);                                 \
  const char *s = strrchr(__FILE__, '/');                           \
  const char *filename = s ? s + 1 : __FILE__;                      \
  fprintf(file, "%02d:%02d:%02d [%8s:%-3d] " fmt "\n", tm->tm_hour, \
          tm->tm_min, tm->tm_sec, filename, __LINE__, ##__VA_ARGS__);

// PANIC_IF is active for both debug and release modes
#define PANIC_IF(expr, msg, ...)                           \
  do {                                                     \
    if (!(expr)) break;                                    \
    FPRINTF(stderr, "[PANIC] " msg ": %m", ##__VA_ARGS__); \
    exit(EXIT_FAILURE);                                    \
  } while (0)

// DEBUG, INFO, and WARN are not active in release mode
static FILE *log_file = stderr;
#define LOG(level, msg, ...)                                     \
  do {                                                           \
    if constexpr (BuildOptions::debug) {                         \
      if (level < runtime_options.log_level) break;              \
      const char *level_str = level == 1   ? "DEBUG"             \
                              : level == 2 ? "INFO"              \
                              : level == 3 ? "WARN"              \
                                           : "UNK";              \
      FPRINTF(log_file, "[%5s] " msg, level_str, ##__VA_ARGS__); \
    }                                                            \
  } while (0)

#define DEBUG(msg, ...) LOG(1, msg, ##__VA_ARGS__)
#define INFO(msg, ...) LOG(2, msg, ##__VA_ARGS__)
#define WARN(msg, ...) LOG(3, msg, ##__VA_ARGS__)

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
