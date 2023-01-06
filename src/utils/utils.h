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
inline constexpr auto align_up(auto x, auto a) {
  return (x + (a - 1)) & ~(a - 1);
}
inline constexpr auto align_down(auto x, auto a) { return x & ~(a - 1); }
inline constexpr auto is_aligned(auto x, auto a) {
  return !(x & (static_cast<decltype(x)>(a) - 1));
}

namespace ulayfs {

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

static void init_robust_mutex(pthread_mutex_t *mutex) {
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
  pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
  pthread_mutex_init(mutex, &attr);
}

}  // namespace ulayfs
