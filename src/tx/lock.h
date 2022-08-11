#pragma once

#include <pthread.h>

namespace ulayfs::dram {

namespace detail {

/**
 * @brief A nop lock struct.
 *
 * By default, the concurrency control is non-blocking, and this struct is used
 * as a dummy lock.
 *
 * This struct does not occupy any space if embedded in another class or struct.
 */
struct NopLock {
  void rdlock(){};
  void wrlock(){};
  void unlock(){};
};

/**
 * @brief A lock struct that uses pthread_mutex_t.
 *
 * Used when ULAYFS_CC_MUTEX is set.
 */
struct MutexLock {
  pthread_mutex_t mutex;
  MutexLock() {  // NOLINT(cppcoreguidelines-pro-type-member-init)
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&mutex, &attr);
  }
  ~MutexLock() { pthread_mutex_destroy(&mutex); }
  void rdlock() { pthread_mutex_lock(&mutex); };
  void wrlock() { pthread_mutex_lock(&mutex); };
  void unlock() { pthread_mutex_unlock(&mutex); };
};

/**
 * @brief A lock struct that uses pthread_spinlock_t.
 *
 * Used when ULAYFS_CC_SPINLOCK is set.
 */
struct Spinlock {
  pthread_spinlock_t spinlock;
  Spinlock() {  // NOLINT(cppcoreguidelines-pro-type-member-init)
    pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
  }
  ~Spinlock() { pthread_spin_destroy(&spinlock); }
  void rdlock() { pthread_spin_lock(&spinlock); };
  void wrlock() { pthread_spin_lock(&spinlock); };
  void unlock() { pthread_spin_unlock(&spinlock); };
};

/**
 * @brief A lock struct that uses pthread_rwlock_t.
 *
 * Used when ULAYFS_CC_RWLOCK is set.
 */
struct RwLock {
  pthread_rwlock_t rwlock;
  RwLock() {  // NOLINT(cppcoreguidelines-pro-type-member-init)
    pthread_rwlock_init(&rwlock, nullptr);
  }
  ~RwLock() { pthread_rwlock_destroy(&rwlock); }
  void rdlock() { pthread_rwlock_rdlock(&rwlock); };
  void wrlock() { pthread_rwlock_wrlock(&rwlock); };
  void unlock() { pthread_rwlock_unlock(&rwlock); };
};

static auto make_cc_lock() {
  if constexpr (BuildOptions::cc_occ) return NopLock{};
  if constexpr (BuildOptions::cc_mutex) return MutexLock{};
  if constexpr (BuildOptions::cc_spinlock) return Spinlock{};
  if constexpr (BuildOptions::cc_rwlock) return RwLock{};
}

}  // namespace detail

using Lock = decltype(detail::make_cc_lock());
}  // namespace ulayfs::dram
