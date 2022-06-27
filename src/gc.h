#include "file.h"
#include "flock.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::utility {

class GarbageCollector {
 public:
  static dram::File* open_file(const char* pathname, bool& is_exclusive) {
    int fd;
    struct stat stat_buf;
    if (dram::File::try_open(fd, stat_buf, pathname, O_RDWR, 0)) return nullptr;

    is_exclusive = flock::try_acquire(fd);
    return new dram::File(fd, stat_buf, O_RDWR, /*guard*/ false);
  }

  static int do_gc(const char* pathname) {
    bool is_exclusive;
    dram::File* file = open_file(pathname, is_exclusive);
    if (!file) return -1;
    LOG_INFO("GarbageCollector: open file %s in %s mode", pathname,
             is_exclusive ? "EX" : "SH");
    LOG_INFO("GarbageCollector: start transaction & log gc");
    file->tx_gc();
    if (is_exclusive) {
      LOG_INFO("GarbageCollector: try remove bitmaps on the shared memory");
      file->unlink_shm();
    }
    return 0;
  }
};

}  // namespace ulayfs::utility
