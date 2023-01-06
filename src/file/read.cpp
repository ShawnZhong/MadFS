#include "tx/read.h"

#include "file/file.h"

namespace ulayfs::dram {
ssize_t File::pread(char* buf, size_t count, size_t offset) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  TimerGuard<Event::READ_TX> timer_guard;
  timer.start<Event::READ_TX_CTOR>();
  return ReadTx(
             TxArgs{
                 .lock = &lock,
                 .offset_mgr = &offset_mgr,
                 .mem_table = &mem_table,
                 .blk_table = &blk_table,
                 .allocator = get_local_allocator(),
                 .offset = offset,
                 .count = count,
             },
             buf)
      .exec();
}

ssize_t File::read(char* buf, size_t count) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;

  return ReadTx(
             TxArgs{
                 .lock = &lock,
                 .offset_mgr = &offset_mgr,
                 .mem_table = &mem_table,
                 .blk_table = &blk_table,
                 .allocator = get_local_allocator(),
                 .offset = std::nullopt,
                 .count = count,
             },
             buf)
      .exec();
}
}  // namespace ulayfs::dram
