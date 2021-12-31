#pragma once

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>

namespace ulayfs::posix {

/*
 * To use a posix function `fn`, you need to:
 *
 * (1) declare the function `posix::fn` by adding `DECL_FN(fn)` below.
 *     The type of `posix::fn` follows from the type of `&::fn`, which is
 *     declared in the system header.
 *
 * (2) define and initialize the function `posix::fn` in `posix.cpp` by adding a
 *     `INIT_FN(fn)` statement. This initializes `posix::fn` to the function
 *     pointer returned by `dlsym` during runtime.
 *
 * The functions declared in the system headers are defined in `lib.cpp`, which
 * contains our implementation of the posix functions.
 *
 * For example, `open` in the global namespace is declared in <fcntl.h> and
 * defined in our `lib.cpp`. The use of `extern "C"` makes sure that the symbol
 * for `ulayfs::open` is not mangled by C++, and thus is the same as `open`. The
 * posix version `posix::open` is declared below as an extern variable and
 * initialized via `dlsym` during global variable initialization.
 */

#define DECL_FN(fn) extern const decltype(&::fn) fn

DECL_FN(lseek);
DECL_FN(write);
DECL_FN(pwrite);
DECL_FN(read);
DECL_FN(pread);
DECL_FN(open);
DECL_FN(close);
DECL_FN(fclose);
DECL_FN(mmap);
DECL_FN(munmap);
DECL_FN(remap_file_pages);
DECL_FN(fallocate);
DECL_FN(fsync);

#undef DECL_FN

// [lf]stat are wrappers to internal functions in glibc, so we need to hook the
// actual functions instead
static int stat(const char *filename, struct stat *buf) {
  return __xstat(_STAT_VER, filename, buf);
}
static int lstat(const char *filename, struct stat *buf) {
  return __lxstat(_STAT_VER, filename, buf);
}
static int fstat(int fd, struct stat *buf) {
  return __fxstat(_STAT_VER, fd, buf);
}

}  // namespace ulayfs::posix
