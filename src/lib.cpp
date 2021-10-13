#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>

namespace ulayfs {
namespace posix {
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
}  // namespace posix

extern "C" {
ssize_t write(int fd, const void *buf, size_t count) {
  printf("write:count:%lu\n", count);
  return posix::write(fd, buf, count);
}

ssize_t read(int fd, void *buf, size_t count) {
  printf("read:count:%lu\n", count);
  return posix::write(fd, buf, count);
}
}
}  // namespace ulayfs