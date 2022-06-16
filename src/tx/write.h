#pragma once

#include "tx.h"

namespace ulayfs::dram {

class WriteTx : public Tx {
 protected:
  /*
   * write-specific arguments
   */
  const char* const buf;

  Allocator* allocator;

  std::vector<LogicalBlockIdx>& recycle_image;

  // the logical index of the destination data block
  std::vector<LogicalBlockIdx>& dst_lidxs;
  // the pointer to the destination data block
  std::vector<pmem::Block*>& dst_blocks;

  // the tx entry to be committed (may or may not inline)
  pmem::TxEntry commit_entry;

  WriteTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
          size_t offset)
      : Tx(file, tx_mgr, count, offset),
        buf(buf),
        allocator(file->get_local_allocator()),
        recycle_image(local_buf_image_lidxs),
        dst_lidxs(local_buf_dst_lidxs),
        dst_blocks(local_buf_dst_blocks) {
    // reset recycle_image
    recycle_image.clear();
    recycle_image.resize(num_blocks);
    dst_lidxs.clear();
    dst_blocks.clear();

    // TODO: handle writev requests
    // for overwrite, "leftover_bytes" is zero; only in append we care
    // append log without fence because we only care flush completion
    // before try_commit
    uint32_t rest_num_blocks = num_blocks;
    while (rest_num_blocks > 0) {
      uint32_t chunk_num_blocks =
          std::min(rest_num_blocks, BITMAP_BLOCK_CAPACITY);
      auto lidx = allocator->alloc(chunk_num_blocks);
      dst_lidxs.push_back(lidx);
      rest_num_blocks -= chunk_num_blocks;
    }
    assert(!dst_lidxs.empty());

    for (auto lidx : dst_lidxs)
      dst_blocks.push_back(file->lidx_to_addr_rw(lidx));
    assert(!dst_blocks.empty());

    uint16_t leftover_bytes = ALIGN_UP(end_offset, BLOCK_SIZE) - end_offset;
    if (leftover_bytes == 0 &&
        pmem::TxEntryInline::can_inline(num_blocks, begin_vidx, dst_lidxs[0])) {
      commit_entry = pmem::TxEntryInline(num_blocks, begin_vidx, dst_lidxs[0]);
    } else {
      // it's fine that we append log first as long we don't publish it by tx
      auto log_entry_idx =
          log_mgr->append(allocator, pmem::LogEntry::Op::LOG_OVERWRITE,  // op
                          leftover_bytes,  // leftover_bytes
                          num_blocks,      // total_blocks
                          begin_vidx,      // begin_virtual_idx
                          dst_lidxs        // begin_logical_idxs
          );
      commit_entry = pmem::TxEntryIndirect(log_entry_idx);
    }
  }
  WriteTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
          size_t offset, TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block,
          uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset) {
    is_offset_depend = true;
    this->tail_tx_idx = tail_tx_idx;
    this->tail_tx_block = tail_tx_block;
    this->ticket = ticket;
  }
};
}  // namespace ulayfs::dram
