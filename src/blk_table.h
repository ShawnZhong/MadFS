#pragma once

#include <tbb/concurrent_vector.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <type_traits>

#include "bitmap.h"
#include "block/block.h"
#include "const.h"
#include "cursor/log.h"
#include "cursor/tx_entry.h"
#include "entry.h"
#include "idx.h"
#include "utils/tbb.h"
#include "utils/utils.h"

namespace madfs::dram {
struct FileState {
  TxCursor cursor;
  uint64_t file_size;

  // each thread will `pin` the tx block that it is currently reading so gc
  // won't reclaim this tx block and associated log entry block
  [[nodiscard]] LogicalBlockIdx get_tx_block_idx() const {
    return cursor.idx.block_idx;
  }
};
static_assert(sizeof(FileState) == 24);

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  MemTable* mem_table;

  tbb::concurrent_vector<std::atomic<LogicalBlockIdx>,
                         zero_allocator<std::atomic<LogicalBlockIdx>>>
      table;
  static_assert(std::atomic<LogicalBlockIdx>::is_always_lock_free);

  FileState state;
  /**
   * Version of the file state above.
   * It can only be updated with file->spinlock held. Before a writer (of
   * BlkTable) tries to update the three fields above, it must increment the
   * version; after it's done, it must increment it again.
   * Thus, when this version is odd, it means someone is updating three fields
   * above and they are in a temporary inconsistent state.
   */
  std::atomic<uint64_t> version;

  // move spinlock into a separated cacheline
  union {
    pthread_spinlock_t spinlock;
    char cl[CACHELINE_SIZE];
  };

 public:
  explicit BlkTable(MemTable* mem_table)
      : mem_table(mem_table),
        state{TxCursor::from_meta(mem_table->get_meta()), 0},
        version(0) {
    table.grow_to_at_least(NUM_BLOCKS_PER_GROW);
    pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
  }

  ~BlkTable() { pthread_spin_destroy(&spinlock); }

  /**
   * @return the logical block index corresponding the the virtual block index
   *  0 is returned if the virtual block index is not allocated yet
   */
  [[nodiscard]] LogicalBlockIdx vidx_to_lidx(
      VirtualBlockIdx virtual_block_idx) const {
    if (virtual_block_idx >= table.size()) return 0;
    return table[virtual_block_idx.get()];
  }

  void update(FileState* result_state, Allocator* allocator = nullptr) {
    if (!need_update(result_state, allocator)) return;
    pthread_spin_lock(&spinlock);
    update_unsafe(allocator);
    *result_state = state;
    pthread_spin_unlock(&spinlock);
  }

  template <typename Fn>
  void update(Fn&& fn, Allocator* allocator = nullptr) {
    pthread_spin_lock(&spinlock);
    update_unsafe(allocator);
    fn(const_cast<const FileState&>(state));
    pthread_spin_unlock(&spinlock);
  }

  /**
   * Update the block table by applying the transactions; not thread-safe
   *
   * @param allocator if given, allow allocation when iterating the tx_idx
   * @param bitmap_mgr if given, initialized the bitmap
   */
  uint64_t update_unsafe(Allocator* allocator = nullptr,
                         BitmapMgr* bitmap_mgr = nullptr) {
    TimerGuard<Event::UPDATE> timer_guard;
    TxCursor cursor = state.cursor;

    // it's possible that the previous update move idx to overflow state
    if (bool success = cursor.handle_overflow(mem_table, allocator); !success) {
      // if still overflow, allocator must be not available
      assert(!allocator);
      // if still overflow, we must have reached the tail already
      return state.file_size;
    }

    // inc the version into an odd number to indicate temporarily inconsistency
    uint64_t old_ver = version.load(std::memory_order_relaxed);
    version.store(old_ver + 1, std::memory_order_release);

    LogicalBlockIdx prev_tx_block_idx = 0;
    while (true) {
      auto tx_entry = cursor.get_entry();
      if (!tx_entry.is_valid()) break;
      if (bitmap_mgr && cursor.idx.block_idx != prev_tx_block_idx)
        bitmap_mgr->set_allocated(cursor.idx.block_idx);
      if (tx_entry.is_inline())
        apply_inline_tx(tx_entry.inline_entry);
      else
        apply_indirect_tx(tx_entry.indirect_entry, bitmap_mgr);
      prev_tx_block_idx = cursor.idx.block_idx;
      if (bool success = cursor.advance(mem_table, allocator); !success) break;
    }

    // mark all live data blocks in bitmap
    if (bitmap_mgr)
      for (const auto& logical_idx : table)
        bitmap_mgr->set_allocated(logical_idx);

    state.cursor = cursor;

    // inc the version into an even number to indicate they are consistent now
    version.store(old_ver + 2, std::memory_order_release);

    return state.file_size;
  }

  [[nodiscard]] FileState get_state_unsafe() const { return state; }

 private:
  /**
   * Quick check if update is necessary; thread safe
   * This check is guarantee to not write any shared data structure so avoid
   * cache coherence traffic. If this function return true, do not acquire
   * spinlock in file.
   *
   * @param result_state if no need to update, file state is stored here
   * @param allocator if given, allow allocation
   * @return whether update is necessary
   */
  [[nodiscard]] bool need_update(FileState* result_state,
                                 Allocator* allocator) const {
    uint64_t curr_ver = version.load(std::memory_order_acquire);
    if (curr_ver & 1) return true;  // old version means inconsistency
    *result_state = state;
    if (curr_ver != version.load(std::memory_order_acquire)) return true;
    bool success = result_state->cursor.handle_overflow(mem_table, allocator);
    if (!success) {
      return false;
    }
    // if it's not valid, there is no new tx to the tx history, thus no need to
    // acquire spinlock to update
    return result_state->cursor.get_entry().is_valid();
  }

  void grow_to_fit(VirtualBlockIdx idx) {
    if (table.size() > idx.get()) return;
    table.grow_to_at_least(next_pow2(idx.get()));
  }

  /**
   * Apply an indirect transaction to the block table
   *
   * @param tx_entry the entry to be applied
   * @param bitmap_mgr if passed, initialized the bitmap
   */
  void apply_indirect_tx(pmem::TxEntryIndirect tx_entry,
                         BitmapMgr* bitmap_mgr) {
    LogCursor log_cursor(tx_entry, mem_table, bitmap_mgr);

    uint32_t num_blocks;
    VirtualBlockIdx begin_vidx, end_vidx;
    uint16_t leftover_bytes;

    do {
      begin_vidx = log_cursor->begin_vidx;
      num_blocks = log_cursor->num_blocks;
      end_vidx = begin_vidx + num_blocks;
      grow_to_fit(end_vidx);

      for (uint32_t offset = 0; offset < log_cursor->num_blocks; ++offset)
        table[begin_vidx.get() + offset] =
            log_cursor->begin_lidxs[offset / BITMAP_ENTRY_BLOCKS_CAPACITY] +
            offset % BITMAP_ENTRY_BLOCKS_CAPACITY;
      // only the last one matters, so this variable will keep being overwritten
      leftover_bytes = log_cursor->leftover_bytes;
    } while (log_cursor.advance(mem_table, bitmap_mgr));

    if (uint64_t new_file_size = BLOCK_IDX_TO_SIZE(end_vidx) - leftover_bytes;
        new_file_size > state.file_size) {
      state.file_size = new_file_size;
    }
  }

  /**
   * Apply an inline transaction to the block table
   * @param tx_entry the entry to be applied
   */
  void apply_inline_tx(pmem::TxEntryInline tx_entry) {
    uint32_t num_blocks = tx_entry.num_blocks;
    // for dummy entry, do nothing
    if (num_blocks == 0) return;
    VirtualBlockIdx begin_vidx = tx_entry.begin_virtual_idx;
    LogicalBlockIdx begin_lidx = tx_entry.begin_logical_idx;
    VirtualBlockIdx end_vidx = begin_vidx + num_blocks;
    grow_to_fit(end_vidx);

    // update block table mapping
    for (uint32_t i = 0; i < num_blocks; ++i)
      table[begin_vidx.get() + i] = begin_lidx + i;

    // update file size if this write exceeds current file size
    // inline tx must be aligned to BLOCK_SIZE boundary
    uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx);
    if (now_file_size > state.file_size) state.file_size = now_file_size;
  }

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
    out << "BlkTable:\n";
    out << "\tfile_size: " << b.state.file_size << "\n";
    out << "\ttail_tx_idx: " << b.state.cursor.idx << "\n";
    for (size_t i = 0; i < b.table.size(); ++i) {
      if (b.table[i].load() == 0) continue;
      if (i >= 100) {
        out << "\t...\n";
        break;
      }
      out << "\t" << i << " -> " << b.table[i] << "\n";
    }
    return out;
  }
};

}  // namespace madfs::dram
