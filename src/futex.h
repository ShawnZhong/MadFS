#pragma once

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>

#include "config.h"

namespace ulayfs {

class Futex {
  std::atomic_uint32_t val = 1;

  inline static long futex(std::atomic_uint32_t *uaddr, int futex_op,
                           uint32_t val, const struct timespec *timeout,
                           uint32_t *uaddr2, uint32_t val3) {
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
  }

 public:
  void init() { val = 1; }

  void acquire() {
    while (true) {
      uint32_t one = 1;
      val.compare_exchange_strong(one, 0, std::memory_order_acq_rel,
                                  std::memory_order_acquire);

      long rc = futex(&val, FUTEX_TRYLOCK_PI, 0, nullptr, nullptr, 0);
      if (errno == EAGAIN) continue;
      if (rc == -1) perror("futex-acquire");
      return;
    }
  }

  void release() {
    val = 0;
    long rc = futex(&val, FUTEX_WAKE, 1, nullptr, nullptr, 0);
    if (rc == -1) perror("futex-release");
  }
};

static_assert(sizeof(Futex) == 4, "Futex must of 4 bytes");
}  // namespace ulayfs
