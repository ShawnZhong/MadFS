#pragma once

#include <unordered_map>

#include "layout.h"
#include "tx.h"

namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  pmem::MetaBlock* meta;
  pmem::TxEntryIdx recent_tx_log_tail;

  MemTable* mem_table;

  //  std::unordered_map<pmem::VirtualBlockIdx, pmem::LogicalBlockIdx> table;
  std::vector<pmem::LogicalBlockIdx> table;

  inline pmem::TxEntry get_tx_entry(pmem::TxEntryIdx idx) {
    const auto [block_idx, local_idx] = idx;
    auto block = mem_table->get_addr(block_idx);
    auto tx_log_block = &block->tx_log_block;
    return tx_log_block->get_entry(local_idx);
  }

  inline void apply_tx(pmem::TxCommitEntry tx_commit_entry) {
    auto log_entry_idx = tx_commit_entry.log_entry_idx;
  }

 public:
  BlkTable() = default;
  explicit BlkTable(pmem::MetaBlock* meta, MemTable* mem_table)
      : meta(meta), mem_table(mem_table), recent_tx_log_tail() {}

  void put(pmem::VirtualBlockIdx virtual_block_idx,
           pmem::LogicalBlockIdx logical_block_idx) {
    table[virtual_block_idx] = logical_block_idx;
  }

  void range_put(pmem::VirtualBlockIdx virtual_block_idx,
                 pmem::LogicalBlockIdx logical_block_idx, uint32_t num_blocks) {
    for (uint32_t i = 0; i < num_blocks; ++i) {
      table[virtual_block_idx + i] = logical_block_idx + i;
    }
  }

  pmem::LogicalBlockIdx get(pmem::VirtualBlockIdx virtual_block_idx) {
    return table[virtual_block_idx];
  }

  void update() {
    auto tx_log_tail = meta->get_tx_log_tail();
    if (recent_tx_log_tail == tx_log_tail) return;
    auto [recent_block_idx, recent_local_idx] = recent_tx_log_tail;

    if (recent_block_idx == 0) {
      for (; recent_local_idx < pmem::NUM_INLINE_TX_ENTRY; ++recent_local_idx) {
        auto tx_entry = meta->get_inline_tx_entry(recent_local_idx);
        if (tx_entry.data == 0) break;
        if (!tx_entry.is_commit()) continue;
        auto tx_commit_entry = tx_entry.commit_entry;
        apply_tx(tx_commit_entry);
      }

      recent_tx_log_tail.local_idx = recent_local_idx;
      if (recent_tx_log_tail == tx_log_tail) return;

      recent_tx_log_tail = meta->get_tx_log_head();
    }

    // TODO: apply tx in blocks

    recent_tx_log_tail = tx_log_tail;
  }
};
}  // namespace ulayfs::dram
