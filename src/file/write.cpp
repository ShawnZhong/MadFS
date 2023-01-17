#include "file/file.h"
#include "tx/write_aligned.h"
#include "tx/write_unaligned.h"

namespace madfs::dram {
ssize_t File::pwrite(const char* buf, size_t count, size_t offset) {
  if (unlikely(!can_write)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    timer.start<Event::ALIGNED_TX_CTOR>();
    return AlignedTx(this, buf, count, offset).exec();
  }

  // another special case where range is within a single block
  if ((BLOCK_SIZE_TO_IDX(offset)) == BLOCK_SIZE_TO_IDX(offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return SingleBlockTx(this, buf, count, offset).exec();
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return MultiBlockTx(this, buf, count, offset).exec();
  }
}

ssize_t File::write(const char* buf, size_t count) {
  if (unlikely(!can_write)) {
    errno = EBADF;
    return -1;
  }
  if (unlikely(count == 0)) return 0;

  FileState state;
  uint64_t ticket;
  uint64_t offset;
  blk_table.update([&](const FileState& file_state) {
    offset = offset_mgr.acquire(count, file_state.file_size,
                                /*stop_at_boundary*/ false, ticket);
    state = file_state;
  });

  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    return Tx::exec_and_release_offset<AlignedTx>(this, buf, count, offset,
                                                  state, ticket);
  }

  // another special case where range is within a single block
  if (BLOCK_SIZE_TO_IDX(offset) == BLOCK_SIZE_TO_IDX(offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return Tx::exec_and_release_offset<SingleBlockTx>(this, buf, count, offset,
                                                      state, ticket);
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return Tx::exec_and_release_offset<MultiBlockTx>(this, buf, count, offset,
                                                     state, ticket);
  }
}
}  // namespace madfs::dram
