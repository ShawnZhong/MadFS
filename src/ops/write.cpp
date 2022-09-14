#include "lib.h"
#include "utils/timer.h"

namespace ulayfs {
extern "C" {
ssize_t write(int fd, const void* buf, size_t count) {
  if (auto file = get_file(fd)) {
    timer.start<Event::WRITE>(count);
    ssize_t res = file->write(buf, count);
    timer.stop<Event::WRITE>();
    LOG_DEBUG("ulayfs::write(%s, buf, %zu) = %zu", file->path, count, res);
    return res;
  } else {
    ssize_t res = posix::write(fd, buf, count);
    LOG_DEBUG("posix::write(%d, buf, %zu) = %zu", fd, count, res);
    return res;
  }
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
  if (auto file = get_file(fd)) {
    TimerGuard<Event::PWRITE> timer_guard(count);
    ssize_t res = file->pwrite(buf, count, static_cast<size_t>(offset));
    LOG_DEBUG("ulayfs::pwrite(%s, buf, %zu, %zu) = %zu", file->path, count,
              offset, res);
    return res;
  } else {
    LOG_DEBUG("posix::pwrite(%d, buf, %zu, %ld)", fd, count, offset);
    return posix::pwrite(fd, buf, count, offset);
  }
}

ssize_t pwrite64(int fd, const void* buf, size_t count, off64_t offset) {
  return pwrite(fd, buf, count, offset);
}
}
}  // namespace ulayfs
