#include "file/file.h"
#include "tx/write_aligned.h"
#include "tx/write_unaligned.h"

namespace ulayfs::dram {
ssize_t File::write(const char* buf, size_t count,
                    std::optional<size_t> offset) {
  if (unlikely(!can_write)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;

  auto arg =
      TxArg(TxType::WRITE, &lock, &offset_mgr, &mem_table, &blk_table,
            get_local_allocator(), const_cast<char*>(buf), count, offset);

  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && arg.offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    timer.start<Event::ALIGNED_TX_CTOR>();
    return AlignedTx(arg).exec();
  }

  // another special case where range is within a single block
  if ((BLOCK_SIZE_TO_IDX(arg.offset)) ==
      BLOCK_SIZE_TO_IDX(arg.offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return SingleBlockTx(arg).exec();
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return MultiBlockTx(arg).exec();
  }
}
}  // namespace ulayfs::dram
