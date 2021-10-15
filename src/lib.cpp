#include <cstdio>

#include "config.h"
#include "layout.h"
#include "posix.h"

namespace ulayfs {
extern "C" {
ssize_t write(int fd, const void *buf, size_t count) {
  if constexpr (build_options.debug) {
    printf("write:count:%lu\n", count);
  }
  return posix::write(fd, buf, count);
}

ssize_t read(int fd, void *buf, size_t count) {
  if constexpr (build_options.debug) {
    printf("read:count:%lu\n", count);
  }
  return posix::read(fd, buf, count);
}
}  // extern "C"
}  // namespace ulayfs
