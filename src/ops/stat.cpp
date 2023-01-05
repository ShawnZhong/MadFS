#include "lib.h"
#include "utils/timer.h"

namespace ulayfs {
extern "C" {
int fstat(int fd, struct stat* buf) {
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

int fstat64(int fd, struct stat64* buf) {
  return fstat(fd, reinterpret_cast<struct stat*>(buf));
}

int stat(const char* pathname, struct stat* buf) {
  if (int rc = posix::stat(pathname, buf); unlikely(rc < 0)) {
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

int stat64(const char* pathname, struct stat64* buf) {
  return stat(pathname, reinterpret_cast<struct stat*>(buf));
}

#ifdef _STAT_VER
// For glibc 2.31 (used by Ubuntu 20.04), `{f}stat` are wrappers around
// `__{f}xstat`, which take an additional version argument `_STAT_VER`.
//
// Since glibc 2.33, the wrappers are removed and becomes actual symbols
// exported by the library. See:
// https://github.com/bminor/glibc/commit/8ed005daf0ab03e142500324a34087ce179ae78e
// https://github.com/bminor/glibc/commit/30f1c7439489bf756a45e349d69be1826e0c9bd8

int __fxstat([[maybe_unused]] int ver, int fd, struct stat* buf) {
  return fstat(fd, buf);
}

int __fxstat64([[maybe_unused]] int ver, int fd, struct stat64* buf) {
  return fstat64(fd, buf);
}

int __xstat([[maybe_unused]] int ver, const char* pathname, struct stat* buf) {
  return stat(pathname, buf);
}

int __xstat64([[maybe_unused]] int ver, const char* pathname,
              struct stat64* buf) {
  return stat64(pathname, buf);
}
#endif
}
}  // namespace ulayfs
