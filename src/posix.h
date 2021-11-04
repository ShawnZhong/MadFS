#pragma once

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ulayfs::posix {
#define REGISTER_FN(fn)                                                  \
  static auto fn = []() noexcept {                                       \
    auto res = reinterpret_cast<decltype(&::fn)>(dlsym(RTLD_NEXT, #fn)); \
    assert(res != nullptr);                                              \
    return res;                                                          \
  }()

REGISTER_FN(lseek);
REGISTER_FN(write);
REGISTER_FN(pwrite);
REGISTER_FN(read);
REGISTER_FN(pread);
REGISTER_FN(open);
REGISTER_FN(close);
REGISTER_FN(mmap);
REGISTER_FN(munmap);
REGISTER_FN(ftruncate);

#undef REGISTER_FN

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
