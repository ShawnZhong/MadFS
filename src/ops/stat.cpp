#include "lib.h"
#include "utils/timer.h"

namespace ulayfs {
extern "C" {
int __fxstat([[maybe_unused]] int ver, int fd, struct stat* buf) {
  int rc = posix::fstat(fd, buf);
  if (unlikely(rc < 0)) {
    LOG_WARN("fstat failed for fd = %d: %m", fd);
    return rc;
  }

  if (auto file = get_file(fd)) {
    file->stat(buf);
    LOG_DEBUG("ulayfs::fstat(%d, {.st_size = %ld})", fd, buf->st_size);
  } else {
    LOG_DEBUG("posix::fstat(%d)", fd);
  }

  return 0;
}

int __fxstat64([[maybe_unused]] int ver, int fd, struct stat64* buf) {
  return __fxstat(ver, fd, reinterpret_cast<struct stat*>(buf));
}

int __xstat([[maybe_unused]] int ver, const char* pathname, struct stat* buf) {
  if (int rc = posix::__xstat(ver, pathname, buf); unlikely(rc < 0)) {
    LOG_WARN("posix::stat(%s) = %d: %m", pathname, rc);
    return rc;
  }

  if (ssize_t rc = getxattr(pathname, SHM_XATTR_NAME, nullptr, 0); rc > 0) {
    int fd = open(pathname, O_RDONLY);
    if (auto file = get_file(fd)) {
      file->stat(buf);
      LOG_DEBUG("ulayfs::stat(%s, {.st_size = %ld})", pathname, buf->st_size);
      close(fd);
      return 0;
    }
  }

  LOG_DEBUG("posix::stat(%s, {.st_size = %ld})", pathname, buf->st_size);
  return 0;
}

int __xstat64([[maybe_unused]] int ver, const char* pathname,
              struct stat64* buf) {
  return __xstat(ver, pathname, reinterpret_cast<struct stat*>(buf));
}
}
}  // namespace ulayfs
