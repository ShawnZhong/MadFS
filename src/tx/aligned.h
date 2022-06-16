#include "write.h"

namespace ulayfs::dram {
class AlignedTx : public WriteTx {
 public:
  AlignedTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
            size_t offset)
      : WriteTx(file, tx_mgr, buf, count, offset) {}

  AlignedTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
            size_t offset, TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block,
            uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset, tail_tx_idx, tail_tx_block,
                ticket) {}

  ssize_t exec() override {
    pmem::TxEntry conflict_entry;

    // since everything is block-aligned, we can copy data directly
    const char* rest_buf = buf;
    size_t rest_count = count;
    for (auto block : dst_blocks) {
      size_t num_bytes = std::min(rest_count, BITMAP_CAPACITY_IN_BYTES);
      pmem::memcpy_persist(block->data_rw(), rest_buf, num_bytes);
      rest_buf += num_bytes;
      rest_count -= num_bytes;
    }
    _mm_sfence();

    // make a local copy of the tx tail
    if (!is_offset_depend)
      file->update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
    for (uint32_t i = 0; i < num_blocks; ++i)
      recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);

    if (is_offset_depend) file->wait_offset(ticket);

  retry:
    conflict_entry =
        tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
    if (!conflict_entry.is_valid()) goto done;
    // we don't check the return value of handle_conflict here because we don't
    // care whether there is a conflict, as long as recycle_image gets updated
    handle_conflict(conflict_entry, begin_vidx, end_vidx - 1, recycle_image);
    goto retry;

  done:
    // recycle the data blocks being overwritten
    allocator->free(recycle_image);
    return static_cast<ssize_t>(count);
  }
};
}  // namespace ulayfs::dram