#include "write.h"

namespace ulayfs::dram {
class AlignedTx : public WriteTx {
 public:
  AlignedTx(File* file, const char* buf, size_t count, size_t offset)
      : WriteTx(file, buf, count, offset) {}

  AlignedTx(File* file, const char* buf, size_t count, size_t offset,
            FileState state, uint64_t ticket)
      : WriteTx(file, buf, count, offset, state, ticket) {}

  ssize_t exec() {
    timer.stop<Event::ALIGNED_TX_CTOR>();
    timer.start<Event::ALIGNED_TX_EXEC>();

    {
      TimerGuard<Event::ALIGNED_TX_COPY> timer_guard;

      // since everything is block-aligned, we can copy data directly
      const char* rest_buf = buf;
      size_t rest_count = count;

      for (auto block : dst_blocks) {
        size_t num_bytes = std::min(rest_count, BITMAP_ENTRY_BYTES_CAPACITY);
        pmem::memcpy_persist(block->data_rw(), rest_buf, num_bytes);
        rest_buf += num_bytes;
        rest_count -= num_bytes;
      }
      fence();
    }

    {
      TimerGuard<Event::ALIGNED_TX_UPDATE> timer_guard;
      if (!is_offset_depend) file->update(&state, allocator);
    }

    if (allocator->tx_block.get_pinned_idx() != state.get_tx_block_idx())
      allocator->log_entry.reset();

    {
      TimerGuard<Event::ALIGNED_TX_PREPARE> timer_guard;
      // for aligned tx, `leftover_bytes` is always zero, so we don't need to
      // know file size before prepare commit entry. thus, we move it before
      // `file->update` to shrink the critical section
      leftover_bytes = 0;
      prepare_commit_entry(/*skip_update_leftover_bytes*/ true);
    }

    {
      TimerGuard<Event::ALIGNED_TX_RECYCLE> timer_guard;
      for (uint32_t i = 0; i < num_blocks; ++i)
        recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);
    }

    {
      TimerGuard<Event::ALIGNED_TX_WAIT_OFFSET> timer_guard;
      if (is_offset_depend) offset_mgr->wait(ticket);
    }

    if constexpr (BuildOptions::cc_occ) {
      TimerGuard<Event::ALIGNED_TX_COMMIT> timer_guard;
      while (true) {
        pmem::TxEntry conflict_entry =
            state.cursor.try_commit(commit_entry, mem_table, allocator);
        if (!conflict_entry.is_valid()) break;

        bool into_new_block = false;
        // we don't check the return value of handle_conflict here because we
        // don't care whether there is a conflict, as long as recycle_image gets
        // updated
        handle_conflict(conflict_entry, begin_vidx, end_vidx - 1, recycle_image,
                        commit_entry.is_inline() ? nullptr : &into_new_block);
        if (into_new_block) {
          assert(!commit_entry.is_inline());
          allocator->log_entry.free(log_cursor);
          allocator->log_entry.reset();
          // re-prepare (incl. append new log entries)
          prepare_commit_entry(/*skip_update_leftover_bytes*/ true);
        }
        // aligned transaction will never have leftover bytes, so no need to
        // recheck commit_entry
      }
    } else {
      TimerGuard<Event::ALIGNED_TX_COMMIT> timer_guard;
      state.cursor.try_commit(commit_entry, mem_table, allocator);
    }

    {
      TimerGuard<Event::ALIGNED_TX_FREE> timer_guard;
      // recycle the data blocks being overwritten
      allocator->block.free(recycle_image);
    }

    // update the pinned tx block
    allocator->tx_block.pin(state.get_tx_block_idx());

    timer.stop<Event::ALIGNED_TX_EXEC>();

    return static_cast<ssize_t>(count);
  }
};
}  // namespace ulayfs::dram
