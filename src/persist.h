#pragma once

#include <immintrin.h>

#include <cstring>

#include "config.h"
#include "const.h"
#include "utils.h"

extern "C" {
// https://github.com/pmem/pmdk/blob/master/src/libpmem2/x86_64/memcpy_memset.h
void memmove_movnt_avx512f_clflush(char *, const char *, size_t);
void memmove_movnt_avx512f_clflushopt(char *, const char *, size_t);
void memmove_movnt_avx512f_clwb(char *, const char *, size_t);
void memmove_movnt_avx_clflush_wcbarrier(char *, const char *, size_t);
void memmove_movnt_avx_clflushopt_wcbarrier(char *, const char *, size_t);
void memmove_movnt_avx_clwb_wcbarrier(char *, const char *, size_t);

void memmove_mov_avx512f_noflush(char *, const char *, size_t);
void memmove_mov_avx_noflush(char *, const char *, size_t);
}

namespace ulayfs {

static inline void fence() {
  if constexpr (BuildOptions::enable_timer) {
    _mm_mfence();
  } else {
    _mm_sfence();
  }
}

namespace pmem {
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
  fence();
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
  fence();
}

static inline void memcpy_persist(char *dst, const char *src, size_t size) {
  if constexpr (BuildOptions::support_avx512f) {
    if constexpr (BuildOptions::support_clwb)
      memmove_movnt_avx512f_clwb(dst, src, size);
    else if constexpr (BuildOptions::support_clflushopt)
      memmove_movnt_avx512f_clflushopt(dst, src, size);
    else
      memmove_movnt_avx512f_clflush(dst, src, size);
  } else {
    if constexpr (BuildOptions::support_clwb)
      memmove_movnt_avx_clwb_wcbarrier(dst, src, size);
    else if constexpr (BuildOptions::support_clflushopt)
      memmove_movnt_avx_clflushopt_wcbarrier(dst, src, size);
    else
      memmove_movnt_avx_clflush_wcbarrier(dst, src, size);
    VALGRIND_PMC_DO_FLUSH(dst, size);
  }
}
}  // namespace pmem

namespace dram {
static inline void memcpy(char *dst, const char *src, size_t size) {
  if constexpr (BuildOptions::support_avx512f) {
    memmove_mov_avx512f_noflush(dst, src, size);
  } else {
    memmove_mov_avx_noflush(dst, src, size);
  }
}
}  // namespace dram

}  // namespace ulayfs