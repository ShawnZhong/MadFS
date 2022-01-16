#include "posix.h"

#include <dlfcn.h>

#include <cassert>

namespace ulayfs::posix {
#define INIT_FN(fn)                                                      \
  const decltype(&::fn) fn = []() noexcept {                             \
    auto res = reinterpret_cast<decltype(&::fn)>(dlsym(RTLD_NEXT, #fn)); \
    assert(res != nullptr);                                              \
    return res;                                                          \
  }()

INIT_FN(lseek);
INIT_FN(write);
INIT_FN(pwrite);
INIT_FN(read);
INIT_FN(pread);
INIT_FN(open);
INIT_FN(close);
INIT_FN(fclose);
INIT_FN(mmap);
INIT_FN(mremap);
INIT_FN(munmap);
INIT_FN(fallocate);
INIT_FN(ftruncate);
INIT_FN(fsync);
INIT_FN(fdatasync);
INIT_FN(flock);
INIT_FN(fcntl);
INIT_FN(unlink);
INIT_FN(rename);
INIT_FN(__xstat);
INIT_FN(__lxstat);
INIT_FN(__fxstat);

#undef INIT_FN

}  // namespace ulayfs::posix
