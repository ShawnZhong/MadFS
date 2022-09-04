#include "file.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::utility {

class GarbageCollector {
 public:
  static dram::File* open_file(const char* pathname) {
    int fd;
    struct stat stat_buf;
    if (dram::File::try_open(fd, stat_buf, pathname, O_RDWR, 0)) return nullptr;

    return new dram::File(fd, stat_buf, O_RDWR, nullptr, /*guard*/ false);
  }

  static int do_gc(const char* pathname) {
    dram::File* file = open_file(pathname);
    if (file == nullptr) return -1;
    LOG_INFO("GarbageCollector: start transaction & log gc");
    uint64_t file_size = file->blk_table.update(/*do_alloc*/ false);
    TxEntryIdx tail_tx_idx = file->blk_table.get_tx_idx();
    file->tx_mgr.gc(tail_tx_idx.block_idx, file_size);
    return 0;
  }
};

}  // namespace ulayfs::utility
