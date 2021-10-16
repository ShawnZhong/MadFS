#pragma once

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ulayfs::posix {
#define REGISTER_FN(fn) \
  static auto fn = reinterpret_cast<decltype(&::fn)>(dlsym(RTLD_NEXT, #fn))

REGISTER_FN(stat);
REGISTER_FN(fstat);
REGISTER_FN(write);
REGISTER_FN(read);
REGISTER_FN(open);
REGISTER_FN(close);
REGISTER_FN(mmap);
REGISTER_FN(munmap);
REGISTER_FN(ftruncate);

#undef REGISTER_FN
}  // namespace ulayfs::posix
