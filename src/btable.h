#pragma once

#include <ostream>
#include <unordered_map>

#include "block.h"
#include "layout.h"
#include "log.h"
#include "tx.h"

namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  pmem::MetaBlock* meta;

  MemTable* mem_table;
  LogMgr* log_mgr;
  TxMgr* tx_mgr;

  std::vector<LogicalBlockIdx> table;

  // keep track of the next TxEntry to apply
  pmem::TxEntryIdx tail_tx_idx;
  pmem::TxLogBlock* tail_tx_block;
  bool need_alloc;

 public:
  BlkTable() = default;
  explicit BlkTable(pmem::MetaBlock* meta, MemTable* mem_table, LogMgr* log_mgr,
                    TxMgr* tx_mgr)
      : meta(meta),
        mem_table(mem_table),
        log_mgr(log_mgr),
        tx_mgr(tx_mgr),
        tail_tx_idx(),
        tail_tx_block(nullptr),
        need_alloc(false) {
    table.resize(16);
  }

  void put(VirtualBlockIdx virtual_block_idx,
           LogicalBlockIdx logical_block_idx) {
    table[virtual_block_idx] = logical_block_idx;
  }

  LogicalBlockIdx get(VirtualBlockIdx virtual_block_idx) {
    return table[virtual_block_idx];
  }

  /**
   * Get the next tx to apply
   *
   * @param[out] tx_idx the index of the current transaction tail
   * @param[out] tx_block the log block corresponding to the transaction
   */
  void get_tail_tx(pmem::TxEntryIdx& tx_idx,
                   pmem::TxLogBlock*& tx_block) const {
    tx_idx = tail_tx_idx;
    tx_block = tail_tx_block;
  }

  /**
   * Apply a transaction to the block table
   */
  void apply_tx(pmem::TxCommitEntry tx_commit_entry) {
    auto log_entry_idx = tx_commit_entry.log_entry_idx;
    auto log_entry = log_mgr->get_entry(log_entry_idx);
    // TODO: linked list
    if (table.size() < log_entry->begin_virtual_idx + log_entry->num_blocks)
      table.resize(table.size() * 2);
    for (uint32_t i = 0; i < log_entry->num_blocks; ++i)
      put(log_entry->begin_virtual_idx + i, log_entry->begin_logical_idx + i);
  }

  /**
   * Update the block table by applying the transactions
   *
   * @param do_alloc whether we allow allocation when iterating the tx_idx.
   * default is false, and only set to true when write permission is granted
   */
  void update(bool do_alloc = false) {
    // we have reached the last entry and the next block is not allocated yet
    if (need_alloc && tail_tx_block->get_next_tx_block() == 0) return;

    while (true) {
      auto tx_entry = tx_mgr->get_entry(tail_tx_idx, tail_tx_block);
      if (!tx_entry.is_valid()) break;
      if (tx_entry.is_commit()) apply_tx(tx_entry.commit_entry);
      // FIXME: handle race condition??
      need_alloc =
          !(tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc));
      if (need_alloc) break;
    }
  }

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
    out << "BlkTable:\n";
    for (size_t i = 0; i < b.table.size(); ++i) {
      if (b.table[i] != 0) {
        out << "\t" << i << " -> " << b.table[i] << "\n";
      }
    }
    return out;
  }
};
}  // namespace ulayfs::dram
