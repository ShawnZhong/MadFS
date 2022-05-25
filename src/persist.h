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
#define VALGRIND_PMC_DO_FLUSH(...) ({})
#endif

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

static inline void memcpy_persist(char *dst, const char *src, size_t size,
                                  bool fenced = false) {
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
  if (fenced) _mm_sfence();
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