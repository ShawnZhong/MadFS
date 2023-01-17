#include "lib.h"
#include "utils/timer.h"

namespace madfs {
extern "C" {
off_t lseek(int fd, off_t offset, int whence) {
  if (auto file = get_file(fd)) {
    LOG_DEBUG("madfs::lseek(%d, %ld, %d)", fd, offset, whence);
    return file->lseek(offset, whence);
  } else {
    LOG_DEBUG("posix::lseek(%d, %ld, %d)", fd, offset, whence);
    return posix::lseek(fd, offset, whence);
  }
}

off64_t lseek64(int fd, off64_t offset, int whence) {
  return lseek(fd, offset, whence);
}
}
}  // namespace madfs
