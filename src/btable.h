#pragma once

#include <ostream>
#include <unordered_map>

#include "block.h"
#include "layout.h"
#include "tx.h"

namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  pmem::MetaBlock* meta;

  MemTable* mem_table;
  TxMgr* tx_mgr;

  std::vector<LogicalBlockIdx> table;

  /**
   * Get log entry based on the log entry index
   */
  pmem::LogEntry get_log_entry(pmem::LogEntryIdx idx) {
    auto block = mem_table->get_addr(idx.block_idx);
    auto log_entry_block = &block->log_entry_block;
    return log_entry_block->get_entry(idx.local_idx);
  }

  /**
   * Apply a transaction to the block table
   */
  void apply_tx(pmem::TxCommitEntry tx_commit_entry) {
    auto log_entry_idx = tx_commit_entry.log_entry_idx;
    auto log_entry = get_log_entry(log_entry_idx);
    // TODO: linked list
    range_put(log_entry.start_virtual_idx, log_entry.start_logical_idx,
              log_entry.num_blocks);
  }

 public:
  BlkTable() = default;
  explicit BlkTable(pmem::MetaBlock* meta, MemTable* mem_table, TxMgr* tx_mgr)
      : meta(meta), mem_table(mem_table), tx_mgr(tx_mgr) {
    table.resize(16);
  }

  void put(VirtualBlockIdx virtual_block_idx,
           LogicalBlockIdx logical_block_idx) {
    table[virtual_block_idx] = logical_block_idx;
  }

  void range_put(VirtualBlockIdx virtual_block_idx,
                 LogicalBlockIdx logical_block_idx, uint32_t num_blocks) {
    if (table.size() < virtual_block_idx + num_blocks) {
      table.resize(table.size() * 2);
    }
    for (uint32_t i = 0; i < num_blocks; ++i) {
      put(virtual_block_idx + i, logical_block_idx + i);
    }
  }

  LogicalBlockIdx get(VirtualBlockIdx virtual_block_idx) {
    return table[virtual_block_idx];
  }

  /**
   * Update the block table by applying the transactions
   */
  void update() {
    while (true) {
      auto tx_entry = tx_mgr->get_curr_tx_entry();
      if (!tx_entry.is_valid()) break;
      if (tx_entry.is_commit()) {
        apply_tx(tx_entry.commit_entry);
      }
      tx_mgr->forward_tx_idx();
    }
  }

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
    out << "BlkTable:\n";
    for (int i = 0; i < b.table.size(); ++i) {
      if (b.table[i] != 0) {
        out << "\t" << i << " -> " << b.table[i] << "\n";
      }
    }
    return out;
  }
};
}  // namespace ulayfs::dram
