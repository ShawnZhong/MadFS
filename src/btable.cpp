#include "btable.h"

#include "file.h"

namespace ulayfs::dram {

void BlkTable::get_tail_tx(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block) {
  tx_idx = tail_tx_idx;
  tx_block = &file->mem_table.get(tx_idx.block_idx)->tx_block;
}

void BlkTable::update(bool do_alloc) {
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
    if (tx_entry.is_commit()) apply_tx(tx_entry.commit_entry, log_mgr);
    if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc)) break;
  }

  pthread_spin_unlock(&spinlock);
}

void BlkTable::resize_to_fit(VirtualBlockIdx idx) {
  if (table.size() > idx) return;
  // __builtin_clz counts the number of leading 0-bits for an unsigned int
  // if idx is already a pow of 2, it will be rounded to the next pow of 2
  // so that the table has enough space to hold this index
  int next_pow2 = 1 << (32 - __builtin_clz(idx));
  table.resize(next_pow2);
}

void BlkTable::apply_tx(pmem::TxCommitEntry tx_commit_entry, LogMgr* log_mgr) {
  auto log_entry_idx = tx_commit_entry.log_entry_idx;

  uint32_t num_blocks;
  VirtualBlockIdx begin_virtual_idx;
  std::vector<LogicalBlockIdx> begin_logical_idxs;
  log_mgr->get_coverage(log_entry_idx, begin_virtual_idx, num_blocks,
                        &begin_logical_idxs);

  size_t now_logical_idx_off = 0;
  VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
  VirtualBlockIdx end_virtual_idx = begin_virtual_idx + num_blocks;
  resize_to_fit(end_virtual_idx);

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
