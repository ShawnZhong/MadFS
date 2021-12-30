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
INIT_FN(munmap);
INIT_FN(remap_file_pages);
INIT_FN(fallocate);
INIT_FN(fsync);

#undef INIT_FN

}  // namespace ulayfs::posix
