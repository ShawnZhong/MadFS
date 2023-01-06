#include "file/file.h"

namespace ulayfs::dram {
int File::fsync() {
  FileState state;
  blk_table.update(&state);
  TxCursor::flush_up_to(&mem_table, meta, state.cursor);
  // we keep an invariant that tx_tail must be a valid (non-overflow) idx
  // an overflow index implies that the `next` pointer of the block is not set
  // (and thus not flushed) yet, so we cannot assume it is equivalent to the
  // first index of the next block
  // here we use the last index of the block to enforce reflush later
  uint16_t capacity = state.cursor.idx.get_capacity();
  if (unlikely(state.cursor.idx.local_idx >= capacity))
    state.cursor.idx.local_idx = static_cast<uint16_t>(capacity - 1);
  meta->set_flushed_tx_tail(state.cursor.idx);
  return 0;
}
}  // namespace ulayfs::dram
