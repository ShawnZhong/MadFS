#include "blk_table.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

#include "const.h"
#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

bool BlkTable::need_update(FileState* result_state, bool do_alloc) const {
  uint64_t curr_ver = version.load(std::memory_order_acquire);
  if (curr_ver & 1) return true;  // old version means inconsistency
  *result_state = state;
  if (curr_ver != version.load(std::memory_order_release)) return true;
  if (!tx_mgr->handle_cursor_overflow(&result_state->cursor, do_alloc))
    return false;
  // if it's not valid, there is no new tx to the tx history, thus no need to
  // acquire spinlock to update
  return result_state->cursor.get_entry().is_valid();
}

uint64_t BlkTable::update(bool do_alloc, bool init_bitmap) {
  TxCursor cursor = state.cursor;
  LogicalBlockIdx prev_tx_block_idx;

  // it's possible that the previous update move idx to overflow state
  if (!tx_mgr->handle_cursor_overflow(&cursor, do_alloc)) {
    // if still overflow, do_alloc must be unset
    assert(!do_alloc);
    // if still overflow, we must have reached the tail already
    return state.file_size;
  }

  // inc the version into an odd number to indicate temporarily inconsistency
  uint64_t old_ver = version.load(std::memory_order_relaxed);
  version.store(old_ver + 1, std::memory_order_acquire);

  prev_tx_block_idx = 0;
  while (true) {
    auto tx_entry = cursor.get_entry();
    if (!tx_entry.is_valid()) break;
    if (init_bitmap && cursor.idx.block_idx != prev_tx_block_idx)
      file->set_allocated(cursor.idx.block_idx);
    if (tx_entry.is_inline())
      apply_inline_tx(tx_entry.inline_entry);
    else
      apply_indirect_tx(tx_entry.indirect_entry, init_bitmap);
    prev_tx_block_idx = cursor.idx.block_idx;
    if (!tx_mgr->advance_cursor(&cursor, do_alloc)) break;
  }

  // mark all live data blocks in bitmap
  if (init_bitmap)
    for (const auto& logical_idx : table) file->set_allocated(logical_idx);

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
                                 bool init_bitmap) {
  auto [curr_entry, curr_block] =
      tx_mgr->get_log_entry(tx_entry.get_log_entry_idx(), init_bitmap);

  uint32_t num_blocks;
  VirtualBlockIdx begin_vidx, end_vidx;
  uint16_t leftover_bytes;

  while (true) {
    assert(curr_entry && curr_block);
    begin_vidx = curr_entry->begin_vidx;
    num_blocks = curr_entry->num_blocks;
    end_vidx = begin_vidx + num_blocks;
    grow_to_fit(end_vidx);

    for (uint32_t offset = 0; offset < curr_entry->num_blocks; ++offset)
      table[begin_vidx.get() + offset] =
          curr_entry->begin_lidxs[offset / BITMAP_BLOCK_CAPACITY] +
          offset % BITMAP_BLOCK_CAPACITY;
    // only the last one matters, so this variable will keep being overwritten
    leftover_bytes = curr_entry->leftover_bytes;
    const auto& [next_entry, next_block] =
        tx_mgr->get_next_log_entry(curr_entry, curr_block, init_bitmap);
    if (next_entry == nullptr) break;
    curr_entry = next_entry;
    curr_block = next_block;
  }

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
    if (b.table[i].load() != 0) {
      out << "\t" << i << " -> " << b.table[i] << "\n";
    }
  }
  return out;
}

}  // namespace ulayfs::dram
