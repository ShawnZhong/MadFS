#include "lib.h"
#include "utils/timer.h"

namespace ulayfs {
extern "C" {
ssize_t read(int fd, void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    timer.start<Event::READ>(count);
    auto res = file->read(buf, count);
    LOG_DEBUG("ulayfs::read(%s, buf, %zu) = %zu", file->path, count, res);
    timer.stop<Event::READ>();
    return res;
  } else {
    auto res = posix::read(fd, buf, count);
    LOG_DEBUG("posix::read(%d, buf, %zu) = %zu", fd, count, res);
    return res;
  }
}

ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    timer.start<Event::PREAD>(count);
    auto res = file->pread(buf, count, offset);
    timer.stop<Event::PREAD>();
    LOG_DEBUG("ulayfs::pread(%s, buf, %zu, %ld) = %zu", file->path, count,
              offset, res);
    return res;
  } else {
    auto res = posix::pread(fd, buf, count, offset);
    LOG_DEBUG("posix::pread(%d, buf, %zu, %ld) = %zu", fd, count, offset, res);
    return res;
  }
}

ssize_t pread64(int fd, void* buf, size_t count, off64_t offset) {
  return pread(fd, buf, count, offset);
}

ssize_t __read_chk(int fd, void* buf, size_t count,
                   [[maybe_unused]] size_t buflen) {
  if (buflen >= count) {
    LOG_WARN("__read_chk(%d, buf, %zu, %zu)", fd, count, buflen);
  }
  return read(fd, buf, count);
}

ssize_t __pread_chk(int fd, void* buf, size_t count, off_t offset,
                    [[maybe_unused]] size_t buflen) {
  if (buflen >= count) {
    LOG_WARN("__pread_chk(%d, buf, %zu, %ld, %zu)", fd, count, offset, buflen);
  }
  return pread(fd, buf, count, offset);
}
}
}  // namespace ulayfs
