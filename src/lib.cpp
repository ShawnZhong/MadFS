#include <cstdarg>
#include <cstdio>
#include <unordered_map>

#include "config.h"
#include "file.h"
#include "layout.h"
#include "posix.h"

namespace ulayfs {
std::unordered_map<int, dram::File *> files;

extern "C" {
int open(const char *pathname, int flags, ...) {
  va_list valist;
  va_start(valist, flags);
  mode_t mode = va_arg(valist, mode_t);
  va_end(valist);

  auto file = new dram::File();
  int fd = file->open(pathname, flags, mode);
  files[fd] = file;
  return fd;
}

ssize_t write(int fd, const void *buf, size_t count) {
  if constexpr (BuildOptions::debug) {
    printf("write:count:%lu\n", count);
  }
  return posix::write(fd, buf, count);
}

ssize_t read(int fd, void *buf, size_t count) {
  if constexpr (BuildOptions::debug) {
    printf("read:count:%lu\n", count);
  }
  return posix::read(fd, buf, count);
}
}  // extern "C"
}  // namespace ulayfs
