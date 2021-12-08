#include "btable.h"

#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

void BlkTable::update(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                      uint64_t* new_file_size, bool do_alloc) {
  pthread_spin_lock(&spinlock);

  // it's possible that the previous update move idx to overflow state
  if (!tx_mgr->handle_idx_overflow(tail_tx_idx, tail_tx_block, do_alloc)) {
    // if still overflow, do_alloc must be unset
    assert(!do_alloc);
    // if still overflow, we must have reached the tail already
    return;
  }

  auto log_mgr = file->get_local_log_mgr();

  while (true) {
    auto tx_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
    if (!tx_entry.is_valid()) break;
    if (tx_entry.is_inline())
      apply_tx(tx_entry.commit_inline_entry);
    else
      apply_tx(tx_entry.commit_entry, log_mgr);
    if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc)) break;
  }

  // return it out
  tx_idx = tail_tx_idx;
  tx_block = tail_tx_block;
  if (new_file_size) *new_file_size = file_size;

  pthread_spin_unlock(&spinlock);
}

void BlkTable::resize_to_fit(VirtualBlockIdx idx) {
  if (table.size() > idx) return;
  // countl_zero counts the number of leading 0-bits
  // if idx is already a pow of 2, it will be rounded to the next pow of 2
  // so that the table has enough space to hold this index
  int next_pow2 = 1 << (sizeof(idx) * 8 - std::countl_zero(idx));
  table.resize(next_pow2);
}

void BlkTable::apply_tx(pmem::TxCommitEntry tx_commit_entry, LogMgr* log_mgr) {
  auto log_entry_idx = tx_commit_entry.log_entry_idx;

  uint32_t num_blocks;
  uint16_t leftover_bytes;
  VirtualBlockIdx begin_virtual_idx;
  std::vector<LogicalBlockIdx> begin_logical_idxs;
  log_mgr->get_coverage(log_entry_idx, begin_virtual_idx, num_blocks,
                        &begin_logical_idxs, &leftover_bytes);

  size_t now_logical_idx_off = 0;
  VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
  VirtualBlockIdx end_virtual_idx = begin_virtual_idx + num_blocks;
  resize_to_fit(end_virtual_idx);

  // update block table mapping
  while (now_virtual_idx < end_virtual_idx) {
    uint16_t chunk_blocks =
        end_virtual_idx > now_virtual_idx + MAX_BLOCKS_PER_BODY
            ? MAX_BLOCKS_PER_BODY
            : end_virtual_idx - now_virtual_idx;
    for (uint32_t i = 0; i < chunk_blocks; ++i) {
      table[now_virtual_idx + i] = begin_logical_idxs[now_logical_idx_off] + i;
    }
    now_virtual_idx += chunk_blocks;
    now_logical_idx_off++;
  }

  // update file size if this write exceeds current file size
  // currently we don't support ftruncate, so the file size is always growing
  uint64_t now_file_size =
      (static_cast<uint64_t>(end_virtual_idx) << BLOCK_SHIFT) - leftover_bytes;
  if (now_file_size > file_size) file_size = now_file_size;
}

void BlkTable::apply_tx(pmem::TxCommitInlineEntry tx_commit_inline_entry) {
  uint32_t num_blocks = tx_commit_inline_entry.num_blocks;
  VirtualBlockIdx begin_vidx = tx_commit_inline_entry.begin_virtual_idx;
  LogicalBlockIdx begin_lidx = tx_commit_inline_entry.begin_logical_idx;
  VirtualBlockIdx end_vidx = begin_vidx + num_blocks;
  resize_to_fit(end_vidx);

  // update block table mapping
  for (uint32_t i = 0; i < num_blocks; ++i)
    table[begin_vidx + i] = begin_lidx + i;

  // update file size if this write exceeds current file size
  // inline tx must be aligned to BLOCK_SIZE boundary
  uint64_t now_file_size = static_cast<uint64_t>(end_vidx) << BLOCK_SHIFT;
  if (now_file_size > file_size) file_size = now_file_size;
}

std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
  out << "BlkTable:\n";
  for (size_t i = 0; i < b.table.size(); ++i) {
    if (b.table[i] != 0) {
      out << "\t" << i << " -> " << b.table[i] << "\n";
    }
  }
  return out;
}

}  // namespace ulayfs::dram
