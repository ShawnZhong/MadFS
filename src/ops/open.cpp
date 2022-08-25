#include <cstdarg>

#include "lib.h"
#include "timer.h"

namespace ulayfs {
extern "C" {
int open(const char* pathname, int flags, ...) {
  mode_t mode = 0;
  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  int fd;
  struct stat stat_buf;
  bool is_valid = dram::File::try_open(fd, stat_buf, pathname, flags, mode);
  if (!is_valid) {
    LOG_DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    return fd;
  }

  try {
    add_file(fd, stat_buf, flags, pathname);
    LOG_INFO("ulayfs::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
    timer.count<Event::OPEN>();
  } catch (const FileInitException& e) {
    LOG_WARN("File \"%s\": ulayfs::open failed: %s. Fallback to syscall",
             pathname, e.what());
    LOG_DEBUG("posix::open(%s, %x, %x) = %d", pathname, flags, mode, fd);
  } catch (const FatalException& e) {
    LOG_WARN("File \"%s\": ulayfs::open failed with fatal error.", pathname);
    return -1;
  }
  return fd;
}

int open64(const char* pathname, int flags, ...) {
  mode_t mode = 0;
  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  return open(pathname, flags, mode);
}

int openat64([[maybe_unused]] int dirfd, const char* pathname, int flags, ...) {
  mode_t mode = 0;
  if (__OPEN_NEEDS_MODE(flags)) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, mode_t);
    va_end(arg);
  }

  // TODO: implement the case where pathname is relative to dirfd
  return open(pathname, flags, mode);
}

FILE* fopen(const char* filename, const char* mode) {
  FILE* file = posix::fopen(filename, mode);
  LOG_DEBUG("posix::fopen(%s, %s) = %p", filename, mode, file);
  return file;
}
}
}  // namespace ulayfs
