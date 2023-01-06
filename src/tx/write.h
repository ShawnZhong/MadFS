#pragma once

#include "tx.h"

namespace ulayfs::dram {

class WriteTx : public Tx {
 protected:
  std::vector<LogicalBlockIdx>& recycle_image;

  // the logical index of the destination data block
  std::vector<LogicalBlockIdx>& dst_lidxs;
  // the pointer to the destination data block
  std::vector<pmem::Block*>& dst_blocks;

  // the tx entry to be committed (may or may not inline)
  pmem::TxEntry commit_entry;
  LogCursor log_cursor;
  uint16_t leftover_bytes;

  WriteTx(TxArg& arg)
      : Tx(arg),
        recycle_image(local_buf_image_lidxs),
        dst_lidxs(local_buf_dst_lidxs),
        dst_blocks(local_buf_dst_blocks) {
    // reset recycle_image
    recycle_image.clear();
    recycle_image.resize(num_blocks);
    dst_lidxs.clear();
    dst_blocks.clear();

    // for overwrite, "leftover_bytes" is zero; only in append we care
    // append log without fence because we only care flush completion
    // before try_commit
    uint32_t rest_num_blocks = num_blocks;
    while (rest_num_blocks > 0) {
      uint32_t chunk_num_blocks =
          std::min(rest_num_blocks, BITMAP_ENTRY_BLOCKS_CAPACITY);
      auto lidx = arg.allocator->block.alloc(chunk_num_blocks);
      dst_lidxs.push_back(lidx);
      rest_num_blocks -= chunk_num_blocks;
    }
    assert(!dst_lidxs.empty());

    for (auto lidx : dst_lidxs)
      dst_blocks.push_back(arg.mem_table->lidx_to_addr_rw(lidx));
    assert(!dst_blocks.empty());
  }

  // NOTE: this function can only be called after file_size is known
  void update_leftover_bytes() {
    // this is how many bytes left at last block that is not written by us
    leftover_bytes = align_up(end_offset, BLOCK_SIZE) - end_offset;
    // then verify if this is the end of file; if not, leftover must be zero
    if (leftover_bytes > 0 &&
        end_offset <= align_down(arg.state.file_size, BLOCK_SIZE))
      leftover_bytes = 0;
  }

  void prepare_commit_entry(bool skip_update_leftover_bytes = false) {
    // skip if file_size is unknown but leftover_bytes is known
    if (!skip_update_leftover_bytes) update_leftover_bytes();
    if (pmem::TxEntryInline::can_inline(num_blocks, begin_vidx, dst_lidxs[0]) &&
        leftover_bytes == 0) {
      commit_entry = pmem::TxEntryInline(num_blocks, begin_vidx, dst_lidxs[0]);
    } else {
      // it's fine that we append log first as long we don't publish it by tx
      log_cursor = arg.allocator->log_entry.append(
          pmem::LogEntry::Op::LOG_OVERWRITE,  // op
          leftover_bytes,                     // leftover_bytes
          num_blocks,                         // total_blocks
          begin_vidx,                         // begin_virtual_idx
          dst_lidxs                           // begin_logical_idxs
      );
      commit_entry = pmem::TxEntryIndirect(log_cursor.idx);
    }
  }

  void recheck_commit_entry() {
    // because we haven't implemented truncate, the file size can only grow up.
    // it's possible that a transaction was fewer leftover bytes but not more.
    if (commit_entry.is_inline()) return;
    uint16_t old_leftover_bytes = leftover_bytes;
    update_leftover_bytes();
    if (old_leftover_bytes == leftover_bytes) return;
    if (pmem::TxEntryInline::can_inline(num_blocks, begin_vidx, dst_lidxs[0]) &&
        leftover_bytes == 0) {
      commit_entry = pmem::TxEntryInline(num_blocks, begin_vidx, dst_lidxs[0]);
      return;
      // the previously allocated log entries should be recycled, but for now,
      // we just leave them there waiting for gc.
    }
    log_cursor.update_leftover_bytes(arg.mem_table, leftover_bytes);
  }
};
}  // namespace ulayfs::dram
