#pragma once

#include <tbb/cache_aligned_allocator.h>

#include <bit>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <exception>

#include "config.h"
#include "posix.h"

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

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

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

/**
 * @return the next power of 2 greater than x. If x is already a power of 2,
 * the next power of 2 is returned.
 */
template <typename T>
static inline T next_pow2(T x) {
  // countl_zero counts the number of leading 0-bits in x
  return T(1) << (sizeof(T) * 8 - std::countl_zero(x));
}

template <typename T>
class zero_allocator : public tbb::cache_aligned_allocator<T> {
 public:
  using value_type = T;
  using propagate_on_container_move_assignment = std::true_type;
  using is_always_equal = std::true_type;

  zero_allocator() = default;
  template <typename U>
  explicit zero_allocator(const zero_allocator<U> &) noexcept {};

  T *allocate(std::size_t n) {
    T *ptr = tbb::cache_aligned_allocator<T>::allocate(n);
    std::memset(static_cast<void *>(ptr), 0, n * sizeof(value_type));
    return ptr;
  }
};

// to indicate still actively use the file, one must acquire this file to
// prevent gc or other utilities; may block
// no explict release; lock will be released during close
static inline void flock_guard(int fd) {
  int ret = posix::flock(fd, LOCK_SH);
  PANIC_IF(ret != 0, "flock acquisition with LOCK_SH fails");
}

static inline bool try_acquire_flock(int fd) {
  int ret = posix::flock(fd, LOCK_EX | LOCK_NB);
  if (ret == 0) return true;
  PANIC_IF(errno != EWOULDBLOCK,
           "flock acquisition with LOCK_EX | LOCK_NB fails");
  return false;
}

static inline void release_flock(int fd) {
  int ret = posix::flock(fd, LOCK_UN);
  PANIC_IF(ret != 0, "flock release fails");
}

}  // namespace ulayfs
