#include <cstdio>
#include <dlfcn.h>
#include <unistd.h>

namespace unistd {
#define REGISTER_FN(name)                                                      \
  auto name = reinterpret_cast<decltype(&::name)>(dlsym(RTLD_NEXT, #name))

REGISTER_FN(write);
REGISTER_FN(read);
} // namespace unistd

extern "C" {
ssize_t write(int fd, const void *buf, size_t count) {
  printf("write:count:%lu\n", count);
  return unistd::write(fd, buf, count);
}

ssize_t read(int fd, void *buf, size_t count) {
  printf("read:count:%lu\n", count);
  return unistd::write(fd, buf, count);
}
}