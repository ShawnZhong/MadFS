#include "tx.h"

#include "entry.h"

namespace ulayfs::dram {
void TxMgr::advance_tx_idx(pmem::TxEntryIdx& idx,
                           pmem::TxLogBlock*& tx_log_block) const {
  assert(idx.local_idx >= 0);

  // next index is within the same block, just increment local index
  uint16_t capacity = idx.block_idx == 0 ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
  if (idx.local_idx < capacity - 1) {
    idx.local_idx++;
    return;
  }

  // move to the next block
  idx.block_idx = idx.block_idx == 0 ? meta->get_next_tx_block()
                                     : tx_log_block->get_next_tx_block();
  assert(idx.block_idx != 0);
  idx.local_idx = 0;
  tx_log_block = &mem_table->get_addr(idx.block_idx)->tx_log_block;
}

pmem::TxEntryIdx TxMgr::begin_tx(VirtualBlockIdx begin_virtual_idx,
                                 uint32_t num_blocks) {
  pmem::TxBeginEntry tx_begin_entry{begin_virtual_idx, num_blocks};
  return try_append(tx_begin_entry);
}

pmem::TxEntryIdx TxMgr::commit_tx(pmem::TxEntryIdx tx_begin_idx,
                                  pmem::LogEntryIdx log_entry_idx) {
  // TODO: compute begin_offset from tx_begin_idx
  pmem::TxCommitEntry tx_commit_entry{0, log_entry_idx};
  return try_append(tx_commit_entry);
}

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << "Transaction Log: \n";

  pmem::TxEntryIdx idx{};
  pmem::TxLogBlock* tx_log_block{nullptr};

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(idx, tx_log_block);
    if (!tx_entry.is_valid()) break;
    out << "\t" << idx << ": " << tx_entry << "\n";
    tx_mgr.advance_tx_idx(idx, tx_log_block);
  }

  return out;
}
}  // namespace ulayfs::dram
