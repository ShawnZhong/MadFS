#include "file.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs {

namespace dram {

class GarbageCollector {
 public:
  // to indicate still actively use the file, one must acquire this file to
  // prevent gc; may block
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

  static File* open_file(const char* pathname, bool& is_exclusive) {
    int fd;
    struct stat stat_buf;
    if (File::try_open(fd, stat_buf, pathname, O_RDWR, 0)) return nullptr;

    is_exclusive = try_acquired(fd);
    return new File(fd, stat_buf, O_RDWR, /*guard*/ false);
  }

  static int do_gc(const char* pathname) {
    bool is_exclusive;
    File* file = open_file(pathname, is_exclusive);
    if (!file) return -1;
    INFO("GarbageCollector: open file %s in %s mode", pathname,
         is_exclusive ? "EX" : "SH");
    INFO("GarbageCollector: start transaction & log gc");
    file->tx_gc();
    if (is_exclusive) {
      INFO("GarbageCollector: try remove bitmaps on the shared memory");
      file->unlink_shm();
    }
    return 0;
  }
};

}  // namespace dram
}  // namespace ulayfs
