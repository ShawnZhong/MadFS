#pragma once

#include <pthread.h>
#include <tbb/cache_aligned_allocator.h>

#include <bit>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <exception>

#include "config.h"
#include "posix.h"
#include "utils/logging.h"

// adopted from `include/linux/align.h`
#define ALIGN_MASK(x, mask) (((x) + (mask)) & ~(mask))
#define ALIGN_UP(x, a) ALIGN_MASK((x), (static_cast<decltype(x)>(a) - 1))
#define ALIGN_DOWN(x, a) ((x) & ~(static_cast<decltype(x)>(a) - 1))
#define IS_ALIGNED(x, a) (((x) & (static_cast<decltype(x)>(a) - 1)) == 0)

namespace madfs {

struct FileInitException : public std::exception {
  explicit FileInitException(const char *msg) : msg(msg) {}
  [[nodiscard]] const char *what() const noexcept override { return msg; }
  const char *msg;
};

/**
 * @return the next power of 2 greater than x. If x is already a power of 2,
 * the next power of 2 is returned.
 */
template <typename T>
static inline T next_pow2(T x) {
  // countl_zero counts the number of leading 0-bits in x
  return T(1) << ((int)sizeof(T) * 8 - std::countl_zero(x));
}

/**
 * Disable copy constructor and copy assignment to avoid accidental copy
 */
class noncopyable {
 public:
  noncopyable() = default;
  noncopyable(const noncopyable &) = delete;
  noncopyable &operator=(const noncopyable &) = delete;
};

static void init_robust_mutex(pthread_mutex_t *mutex) {
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
  pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
  pthread_mutex_init(mutex, &attr);
}

}  // namespace madfs
