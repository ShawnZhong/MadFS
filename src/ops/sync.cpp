#include "lib.h"
#include "timer.h"

namespace ulayfs {
extern "C" {
int fsync(int fd) {
  if (auto file = get_file(fd)) {
    TimerGuard<Event::FSYNC> timer_guard;
    LOG_DEBUG("ulayfs::fsync(%d)", fd);
    return file->fsync();
  } else {
    LOG_DEBUG("posix::fsync(%d)", fd);
    return posix::fsync(fd);
  }
}

int fdatasync(int fd) {
  if (auto file = get_file(fd)) {
    TimerGuard<Event::FSYNC> timer_guard;
    LOG_DEBUG("ulayfs::fdatasync(%s)", file->path);
    return file->fsync();
  } else {
    LOG_DEBUG("posix::fdatasync(%d)", fd);
    return posix::fdatasync(fd);
  }
}
}
}  // namespace ulayfs
