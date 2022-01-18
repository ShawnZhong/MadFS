#include "btable.h"

#include <cstdint>

#include "block.h"
#include "const.h"
#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

uint64_t BlkTable::update(bool do_alloc, bool init_bitmap) {
  // it's possible that the previous update move idx to overflow state
  if (!tx_mgr->handle_idx_overflow(tail_tx_idx, tail_tx_block, do_alloc)) {
    // if still overflow, do_alloc must be unset
    assert(!do_alloc);
    // if still overflow, we must have reached the tail already
    return file_size;
  }

  LogicalBlockIdx prev_tx_block_idx = 0;
  while (true) {
    auto tx_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
    if (!tx_entry.is_valid()) break;
    if (init_bitmap && tail_tx_idx.block_idx != prev_tx_block_idx)
      file->set_allocated(tail_tx_idx.block_idx);
    if (tx_entry.is_inline())
      apply_tx(tx_entry.inline_entry);
    else
      apply_tx(tx_entry.indirect_entry, file->get_log_mgr(), init_bitmap);
    prev_tx_block_idx = tail_tx_idx.block_idx;
    if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc)) break;
  }

  // mark all live data blocks in bitmap
  if (init_bitmap)
    for (const auto logical_idx : table) file->set_allocated(logical_idx);

  return file_size;
}

void BlkTable::resize_to_fit(VirtualBlockIdx idx) {
  if (table.size() > idx) return;
  // countl_zero counts the number of leading 0-bits
  // if idx is already a pow of 2, it will be rounded to the next pow of 2
  // so that the table has enough space to hold this index
  int next_pow2 = 1 << (sizeof(idx) * 8 - std::countl_zero(idx));
  table.resize(next_pow2);
}

void BlkTable::apply_tx(pmem::TxEntryIndirect tx_entry, LogMgr* log_mgr,
                        bool init_bitmap) {
  pmem::LogEntryBlock* curr_block;
  pmem::LogEntry* curr_entry =
      log_mgr->get_entry(tx_entry.get_log_entry_idx(), curr_block, init_bitmap);
  assert(curr_entry && curr_block);

  uint32_t num_blocks;
  LogicalBlockIdx begin_vidx, end_vidx;
  uint16_t leftover_bytes;

  while (true) {
    begin_vidx = curr_entry->begin_vidx;
    num_blocks = curr_entry->header.num_blocks;
    end_vidx = begin_vidx + num_blocks;
    resize_to_fit(end_vidx);

    for (uint32_t offset = 0; offset < curr_entry->header.num_blocks; ++offset)
      table[begin_vidx + offset] = curr_entry->lidxs[offset / BITMAP_CAPACITY] +
                                   offset % BITMAP_CAPACITY;

    curr_entry = log_mgr->get_next_entry(curr_entry, curr_block, init_bitmap);
    if (!curr_entry) {
      leftover_bytes = curr_entry->header.leftover_bytes;
      break;
    }
  }

  uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx) - leftover_bytes;
  file_size = std::max(file_size, now_file_size);
}

void BlkTable::apply_tx(pmem::TxEntryInline tx_commit_inline_entry) {
  uint32_t num_blocks = tx_commit_inline_entry.num_blocks;
  // for dummy entry, do nothing
  if (num_blocks == 0) return;
  VirtualBlockIdx begin_vidx = tx_commit_inline_entry.begin_virtual_idx;
  LogicalBlockIdx begin_lidx = tx_commit_inline_entry.begin_logical_idx;
  VirtualBlockIdx end_vidx = begin_vidx + num_blocks;
  resize_to_fit(end_vidx);

  // update block table mapping
  for (uint32_t i = 0; i < num_blocks; ++i)
    table[begin_vidx + i] = begin_lidx + i;

  // update file size if this write exceeds current file size
  // inline tx must be aligned to BLOCK_SIZE boundary
  uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx);
  if (now_file_size > file_size) file_size = now_file_size;
}

std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
  out << "BlkTable:\n";
  out << "\tfile_size: " << b.file_size << "\n";
  out << "\ttail_tx_idx: " << b.tail_tx_idx << "\n";
  for (size_t i = 0; i < b.table.size(); ++i) {
    if (b.table[i] != 0) {
      out << "\t" << i << " -> " << b.table[i] << "\n";
    }
  }
  return out;
}

}  // namespace ulayfs::dram
