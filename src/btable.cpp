#include "btable.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>

#include "const.h"
#include "file.h"
#include "idx.h"
#include "log.h"

namespace ulayfs::dram {

uint64_t BlkTable::update(bool do_alloc, bool init_bitmap) {
  TxEntryIdx tx_idx_local =
      tail_tx_idx.load(std::memory_order_relaxed).tx_entry_idx;
  pmem::TxBlock* tx_block_local = tail_tx_block.load(std::memory_order_relaxed);
  LogicalBlockIdx prev_tx_block_idx;

  // it's possible that the previous update move idx to overflow state
  if (!tx_mgr->handle_idx_overflow(tx_idx_local, tx_block_local, do_alloc)) {
    // if still overflow, do_alloc must be unset
    assert(!do_alloc);
    // if still overflow, we must have reached the tail already
    return file_size.load(std::memory_order_relaxed);
  }

  // inc the version into an odd number to indicate temporarily inconsistency
  uint64_t old_ver = version.load(std::memory_order_relaxed);
  version.store(old_ver + 1, std::memory_order_acquire);

  prev_tx_block_idx = 0;
  while (true) {
    auto tx_entry = tx_mgr->get_entry_from_block(tx_idx_local, tx_block_local);
    if (!tx_entry.is_valid()) break;
    if (init_bitmap && tx_idx_local.block_idx != prev_tx_block_idx)
      file->set_allocated(tx_idx_local.block_idx);
    if (tx_entry.is_inline())
      apply_tx(tx_entry.inline_entry);
    else
      apply_tx(tx_entry.indirect_entry, file->get_log_mgr(), init_bitmap);
    prev_tx_block_idx = tx_idx_local.block_idx;
    if (!tx_mgr->advance_tx_idx(tx_idx_local, tx_block_local, do_alloc)) break;
  }

  // mark all live data blocks in bitmap
  if (init_bitmap)
    for (const auto logical_idx : table) file->set_allocated(logical_idx);

  tail_tx_idx.store(tx_idx_local, std::memory_order_relaxed);
  tail_tx_block.store(tx_block_local, std::memory_order_relaxed);

  // inc the version into an even number to indicate they are consistent now
  version.store(old_ver + 2, std::memory_order_release);

  return file_size.load(std::memory_order_relaxed);
}

void BlkTable::grow_to_fit(VirtualBlockIdx idx) {
  if (table.size() > idx.get()) return;
  table.grow_to_at_least(next_pow2(idx.get()));
}

void BlkTable::apply_tx(pmem::TxEntryIndirect tx_entry, LogMgr* log_mgr,
                        bool init_bitmap) {
  pmem::LogEntryBlock* curr_block;
  pmem::LogEntry* curr_entry =
      log_mgr->get_entry(tx_entry.get_log_entry_idx(), curr_block, init_bitmap);

  uint32_t num_blocks;
  VirtualBlockIdx begin_vidx, end_vidx;
  uint16_t leftover_bytes;

  do {
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
    curr_entry = log_mgr->get_next_entry(curr_entry, curr_block, init_bitmap);
  } while (curr_entry);

  uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx) - leftover_bytes;
  if (now_file_size > file_size.load(std::memory_order_relaxed))
    file_size.store(now_file_size, std::memory_order_relaxed);
}

void BlkTable::apply_tx(pmem::TxEntryInline tx_commit_inline_entry) {
  uint32_t num_blocks = tx_commit_inline_entry.num_blocks;
  // for dummy entry, do nothing
  if (num_blocks == 0) return;
  VirtualBlockIdx begin_vidx = tx_commit_inline_entry.begin_virtual_idx;
  LogicalBlockIdx begin_lidx = tx_commit_inline_entry.begin_logical_idx;
  VirtualBlockIdx end_vidx = begin_vidx + num_blocks;
  grow_to_fit(end_vidx);

  // update block table mapping
  for (uint32_t i = 0; i < num_blocks; ++i)
    table[begin_vidx.get() + i] = begin_lidx + i;

  // update file size if this write exceeds current file size
  // inline tx must be aligned to BLOCK_SIZE boundary
  uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx);
  if (now_file_size > file_size.load(std::memory_order_relaxed))
    file_size.store(now_file_size, std::memory_order_relaxed);
}

std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
  out << "BlkTable:\n";
  out << "\tfile_size: " << b.file_size.load(std::memory_order_relaxed) << "\n";
  out << "\ttail_tx_idx: "
      << b.tail_tx_idx.load(std::memory_order_relaxed).tx_entry_idx << "\n";
  for (size_t i = 0; i < b.table.size(); ++i) {
    if (b.table[i] != 0) {
      out << "\t" << i << " -> " << b.table[i] << "\n";
    }
  }
  return out;
}

}  // namespace ulayfs::dram
