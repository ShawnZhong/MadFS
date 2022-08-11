#include "write.h"

namespace ulayfs::dram {
class AlignedTx : public WriteTx {
 public:
  AlignedTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
            size_t offset)
      : WriteTx(file, tx_mgr, buf, count, offset) {}

  AlignedTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
            size_t offset, FileState state, uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset, state, ticket) {}

  ssize_t exec() {
    // since everything is block-aligned, we can copy data directly
    const char* rest_buf = buf;
    size_t rest_count = count;

    counter.start_timer<Event::ALIGNED_TX_COPY>();
    for (auto block : dst_blocks) {
      size_t num_bytes = std::min(rest_count, BITMAP_BYTES_CAPACITY);
      pmem::memcpy_persist(block->data_rw(), rest_buf, num_bytes);
      rest_buf += num_bytes;
      rest_count -= num_bytes;
    }
    _mm_sfence();
    counter.stop_timer<Event::ALIGNED_TX_COPY>(/*fence=*/true);

    // for aligned tx, `leftover_bytes` is always zero, so we don't need to know
    // file size before prepare commit entry.
    // thus, we move it before `file->update` to shrink the critical section
    leftover_bytes = 0;
    counter.start_timer<Event::ALIGNED_TX_PREPARE>();
    prepare_commit_entry(/*skip_update_leftover_bytes*/ true);
    counter.stop_timer<Event::ALIGNED_TX_PREPARE>();

    // make a local copy of the tx tail
    counter.start_timer<Event::ALIGNED_TX_UPDATE>();
    if (!is_offset_depend) file->update(&state, /*do_alloc*/ true);
    counter.stop_timer<Event::ALIGNED_TX_UPDATE>();

    // for an aligned tx, leftover_bytes must be zero, so there is no need to
    // validate whether we falsely assume this tx can be inline
    for (uint32_t i = 0; i < num_blocks; ++i)
      recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);

    counter.start_timer<Event::ALIGNED_TX_WAIT_OFFSET>();
    if (is_offset_depend) tx_mgr->offset_mgr.wait_offset(ticket);
    counter.stop_timer<Event::ALIGNED_TX_WAIT_OFFSET>();

  retry:
    if constexpr (BuildOptions::cc_occ) {
      counter.start_timer<Event::ALIGNED_TX_COMMIT>();
      pmem::TxEntry conflict_entry =
          tx_mgr->try_commit(commit_entry, &state.cursor);
      counter.stop_timer<Event::ALIGNED_TX_COMMIT>();
      if (!conflict_entry.is_valid()) goto done;
      // we don't check the return value of handle_conflict here because we
      // don't care whether there is a conflict, as long as recycle_image gets
      // updated
      handle_conflict(conflict_entry, begin_vidx, end_vidx - 1, recycle_image);
      // aligned transaction will never have leftover bytes, so no need to
      // recheck commit_entry
      goto retry;
    } else {
      tx_mgr->try_commit(commit_entry, &state.cursor);
    }

  done:
    // recycle the data blocks being overwritten
    allocator->free(recycle_image);
    return static_cast<ssize_t>(count);
  }
};
}  // namespace ulayfs::dram
