#pragma once

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ulayfs::posix {
#define REGISTER_FN(name) \
  auto name = reinterpret_cast<decltype(&::name)>(dlsym(RTLD_NEXT, #name))

REGISTER_FN(stat);
REGISTER_FN(write);
REGISTER_FN(read);
REGISTER_FN(open);
REGISTER_FN(close);
REGISTER_FN(mmap);
REGISTER_FN(munmap);

#undef REGISTER_FN
}  // namespace ulayfs::posix
