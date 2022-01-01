#include "file.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs {

// to indicate still actively use the file, one must acquire this file to
// prevent gc or other utilities; may block
// no explict release; lock will be released during close
static void flock_guard(int fd) {
  int ret = posix::flock(fd, LOCK_SH);
  PANIC_IF(ret != 0, "flock acquisition with LOCK_SH fails");
}

static bool try_acquired(int fd) {
  int ret = posix::flock(fd, LOCK_EX | LOCK_NB);
  if (ret == 0) return true;
  PANIC_IF(errno != EWOULDBLOCK,
           "flock acquisition with LOCK_EX | LOCK_NB fails");
  return false;
}

static void release(int fd) {
  int ret = posix::flock(fd, LOCK_UN);
  PANIC_IF(ret != 0, "flock release fails");
}

}  // namespace ulayfs
