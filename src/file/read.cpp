#include "tx/read.h"

#include "file/file.h"

namespace ulayfs::dram {

ssize_t File::read(char* buf, size_t count, std::optional<size_t> offset) {
  if (unlikely(!can_read)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  TimerGuard<Event::READ_TX> timer_guard;
  timer.start<Event::READ_TX_CTOR>();
  auto arg = TxArg(TxType::READ, &lock, &offset_mgr, &mem_table, &blk_table,
                   get_local_allocator(), buf, count, offset);
  return ReadTx(arg).exec();
}
}  // namespace ulayfs::dram
