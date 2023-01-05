#pragma once

#include <dlfcn.h>
#include <fcntl.h>
#include <gnu/lib-names.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>

#include "config.h"

namespace ulayfs::posix {

/*
 * To use a posix function `fn`, you need to add a line DEFINE_FN(fn); It
 * declares the function pointer `posix::fn` of type `&::fn`, which is declared
 * in the system header. The value of is initialized to the value returned by
 * `dlsym` during runtime. The function pointer is declared as `inline` to avoid
 * multiple definitions in different translation units.
 *
 * The functions declared in the system headers are defined in `ops`, which
 * contains our implementation of the posix functions.
 *
 * For example, `::open` in the global namespace is declared in <fcntl.h> and
 * defined in our `ops/open.cpp`. The use of `extern "C"` makes sure that the
 * symbol for `ulayfs::open` is not mangled by C++, and thus is the same as
 * `open`. The posix version `posix::open` is declared and defined below.
 */

inline void* glibc_handle = dlopen(LIBC_SO, RTLD_LAZY);

#define DEFINE_FN(fn)                                                       \
  inline const decltype(&::fn) fn = []() noexcept {                         \
    auto res = reinterpret_cast<decltype(&::fn)>(dlsym(glibc_handle, #fn)); \
    assert(res != nullptr);                                                 \
    return res;                                                             \
  }()

DEFINE_FN(lseek);
DEFINE_FN(write);
DEFINE_FN(pwrite);
DEFINE_FN(read);
DEFINE_FN(pread);
DEFINE_FN(open);
DEFINE_FN(fopen);
DEFINE_FN(close);
DEFINE_FN(fclose);
DEFINE_FN(mmap);
DEFINE_FN(mremap);
DEFINE_FN(munmap);
DEFINE_FN(fallocate);
DEFINE_FN(ftruncate);
DEFINE_FN(fsync);
DEFINE_FN(fdatasync);
DEFINE_FN(flock);
DEFINE_FN(fcntl);
DEFINE_FN(unlink);
DEFINE_FN(rename);

#ifndef _STAT_VER
DEFINE_FN(fstat);
DEFINE_FN(stat);
#else
// See stat.cpp
DEFINE_FN(__xstat);
DEFINE_FN(__fxstat);

static int fstat(int fd, struct stat* buf) {
  __msan_unpoison(buf, sizeof(struct stat));
  return posix::__fxstat(_STAT_VER, fd, buf);
}

static int stat(const char* pathname, struct stat* buf) {
  __msan_unpoison(buf, sizeof(struct stat));
  return posix::__xstat(_STAT_VER, pathname, buf);
}
#endif

}  // namespace ulayfs::posix
