#include "blk_table.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

#include "const.h"
#include "idx.h"
#include "tx/cursor.h"
#include "tx/mgr.h"

namespace ulayfs::dram {

bool BlkTable::need_update(FileState* result_state,
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

uint64_t BlkTable::update(Allocator* allocator, BitmapMgr* bitmap_mgr) {
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

void BlkTable::grow_to_fit(VirtualBlockIdx idx) {
  if (table.size() > idx.get()) return;
  table.grow_to_at_least(next_pow2(idx.get()));
}

void BlkTable::apply_indirect_tx(pmem::TxEntryIndirect tx_entry,
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

void BlkTable::apply_inline_tx(pmem::TxEntryInline tx_entry) {
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

std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
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

}  // namespace ulayfs::dram
