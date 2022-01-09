#pragma once

#include <immintrin.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <exception>

#include "config.h"
#include "const.h"

#ifdef ULAYFS_USE_LIBPMEM
#include <libpmem.h>
#endif

#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
// ref:
// https://github.com/llvm/llvm-project/blob/main/compiler-rt/include/sanitizer/msan_interface.h
#else
#define __msan_unpoison(...) ({})
#define __msan_scoped_disable_interceptor_checks(...) ({})
#define __msan_scoped_enable_interceptor_checks(...) ({})
#endif

#if ULAYFS_USE_PMEMCHECK == 1
#include <valgrind/pmemcheck.h>
// ref: https://pmem.io/valgrind/generated/pmc-manual.html
#else
#define VALGRIND_PMC_REMOVE_PMEM_MAPPING(...) ({})
#define VALGRIND_PMC_REGISTER_PMEM_MAPPING(...) ({})
#endif

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

/*
 * The following macros used for assertion and logging
 * Defined as macros since we want to have access to __FILE__ and __LINE__
 */

__attribute__((tls_model("initial-exec"))) extern thread_local const pid_t tid;
extern FILE *log_file;

#define FPRINTF(file, fmt, ...)                                             \
  do {                                                                      \
    auto now = std::chrono::high_resolution_clock::now();                   \
    std::chrono::duration<double> sec = now.time_since_epoch();             \
    const char *s = strrchr(__FILE__, '/');                                 \
    const char *filename = s ? s + 1 : __FILE__;                            \
    fprintf(file, "[Thread %d] %f [%14s:%-3d] " fmt "\n", tid, sec.count(), \
            filename, __LINE__, ##__VA_ARGS__);                             \
  } while (0)

// PANIC_IF is active for both debug and release modes
#define PANIC_IF(expr, msg, ...)                           \
  do {                                                     \
    if (likely(!(expr))) break;                            \
    FPRINTF(stderr, "[PANIC] " msg ": %m", ##__VA_ARGS__); \
    assert(false);                                         \
    throw FatalException();                                \
  } while (0)
#define PANIC(msg, ...) PANIC_IF(true, msg, ##__VA_ARGS__)

// TRACE, DEBUG, INFO, and WARN are not active in release mode
#define LOG(level, msg, ...)                                \
  do {                                                      \
    if constexpr (!BuildOptions::debug) break;              \
    if (level < runtime_options.log_level) break;           \
    constexpr const char *level_str_arr[] = {               \
        "[\u001b[37mTRACE\u001b[0m]",                       \
        "[\u001b[32mDEBUG\u001b[0m]",                       \
        "[\u001b[34mINFO\u001b[0m] ",                       \
        "[\u001b[31mWARN\u001b[0m] ",                       \
    };                                                      \
    constexpr const char *level_str = level_str_arr[level]; \
    FPRINTF(log_file, "%s " msg, level_str, ##__VA_ARGS__); \
  } while (0)

#define TRACE(msg, ...) LOG(0, msg, ##__VA_ARGS__)
#define DEBUG(msg, ...) LOG(1, msg, ##__VA_ARGS__)
#define INFO(msg, ...) LOG(2, msg, ##__VA_ARGS__)
#define WARN(msg, ...) LOG(3, msg, ##__VA_ARGS__)

// adopted from `include/linux/align.h`
#define ALIGN_MASK(x, mask) (((x) + (mask)) & ~(mask))
#define ALIGN_UP(x, a) ALIGN_MASK((x), ((typeof(x))(a)-1))
#define ALIGN_DOWN(x, a) ((x) & ~((typeof(x))(a)-1))
#define IS_ALIGNED(x, a) (((x) & ((typeof(x))(a)-1)) == 0)

namespace ulayfs {

struct FileInitException : public std::exception {
  explicit FileInitException(const char *msg) : msg(msg) {}
  [[nodiscard]] const char *what() const noexcept override { return msg; }
  const char *msg;
};

struct FatalException : public std::exception {};

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

/**
 * Different implementation of memcpy:
 * - flush: just memcpy then flush
 * - kernel: linux kernel implementation, use movnti
 *
 * These functions should not be called directly but through memcpy_persist
 */
static inline void memcpy_persist_kernel(void *dst, const void *src,
                                         size_t size);
static inline void memcpy_persist_pmdk(void *dst, const void *src, size_t size);
static inline void memcpy_persist_flush(void *dst, const void *src,
                                        size_t size);

static inline void memcpy_persist(void *dst, const void *src, size_t size,
                                  bool fenced = false) {
  switch (BuildOptions::persist_impl) {
    case BuildOptions::PersistImpl::PMDK:
      memcpy_persist_pmdk(dst, src, size);
      break;
    case BuildOptions::PersistImpl::KERNEL:
      memcpy_persist_kernel(dst, src, size);
      break;
    case BuildOptions::PersistImpl::FLUSH:
      memcpy_persist_flush(dst, src, size);
      break;
    default:
      assert(false);
  }
  if (fenced) _mm_sfence();
}

/**
 * Linux kernel's implementation of memcpy_flushcache
 * from: /arch/x86/lib/usercopy_64.c
 * use as many movnti as possible
 */
static inline void memcpy_persist_kernel(void *dst, const void *src,
                                         size_t size) {
  unsigned long dest = (unsigned long)dst;
  unsigned long source = (unsigned long)src;

  /* cache copy and flush to align dest */
  if (!IS_ALIGNED(dest, 8)) {
    size_t len = std::min(size, ALIGN_UP(dest, 8) - dest);
    memcpy_persist_flush((void *)dest, (void *)source, len);
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
  if (size) memcpy_persist_flush((void *)dest, (void *)source, size);
}

static inline void memcpy_persist_pmdk(void *dst, const void *src,
                                       size_t size) {
#ifdef ULAYFS_USE_LIBPMEM
  pmem_memcpy_nodrain(dst, src, size);
#else
  // fall back to kernel implementation
  memcpy_persist_kernel(dst, src, size);
#endif
}

/**
 * Naive implementation: use memcpy than flush
 */
static inline void memcpy_persist_flush(void *dst, const void *src,
                                        size_t size) {
  memcpy(dst, src, size);
  persist_unfenced(dst, size);
}

}  // namespace pmem
}  // namespace ulayfs
