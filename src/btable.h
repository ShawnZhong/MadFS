#pragma once

#include <cstdint>
#include <ostream>
#include <unordered_map>

#include "block.h"
#include "idx.h"
#include "layout.h"
#include "log.h"
#include "tx.h"
#include "utils.h"

namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  pmem::MetaBlock* meta;

  MemTable* mem_table;
  LogMgr* log_mgr;
  TxMgr* tx_mgr;

  std::vector<LogicalBlockIdx> table;

  // keep track of the next TxEntry to apply
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;

 public:
  BlkTable() = default;
  explicit BlkTable(pmem::MetaBlock* meta, MemTable* mem_table, LogMgr* log_mgr,
                    TxMgr* tx_mgr)
      : meta(meta),
        mem_table(mem_table),
        log_mgr(log_mgr),
        tx_mgr(tx_mgr),
        tail_tx_idx(),
        tail_tx_block(nullptr) {
    table.resize(16);
  }

  /**
   * @return the logical block index corresponding the the virtual block index
   *  0 is returned if the virtual block index is not allocated yet
   */
  [[nodiscard]] LogicalBlockIdx get(VirtualBlockIdx virtual_block_idx) const {
    if (virtual_block_idx >= table.size()) return 0;
    return table[virtual_block_idx];
  }

  /**
   * Update the block table by applying the transactions
   *
   * @param do_alloc whether we allow allocation when iterating the tx_idx.
   * default is false, and only set to true when write permission is granted
   */
  void update(bool do_alloc = false) {
    // it's possible that the previous update move idx to overflow state
    if (!tx_mgr->handle_idx_overflow(tail_tx_idx, tail_tx_block, do_alloc)) {
      // if still overflow, do_alloc must be unset
      assert(!do_alloc);
      // if still overflow, we must have reached the tail already
      return;
    }

    while (true) {
      auto tx_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
      if (!tx_entry.is_valid()) break;
      if (tx_entry.is_commit()) apply_tx(tx_entry.commit_entry);
      // FIXME: handle race condition??
      if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc)) break;
    }
  }

  /**
   * Get the next tx to apply
   *
   * @param[out] tx_idx the index of the current transaction tail
   * @param[out] tx_block the log block corresponding to the transaction
   */
  void get_tail_tx(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block) const {
    tx_idx = tail_tx_idx;
    tx_block = tail_tx_block;
  }

 private:
  /**
   * Apply a transaction to the block table
   */
  void apply_tx(pmem::TxCommitEntry tx_commit_entry) {
    auto log_entry_idx = tx_commit_entry.log_entry_idx;
    auto log_entry = log_mgr->get_entry(log_entry_idx);
    // TODO: linked list
    if (table.size() <= log_entry->begin_virtual_idx + log_entry->num_blocks)
      table.resize(table.size() * 2);
    for (uint32_t i = 0; i < log_entry->num_blocks; ++i)
      table[log_entry->begin_virtual_idx + i] =
          log_entry->begin_logical_idx + i;
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

/**
 * A sugar function to map vidx to addr
 */
static inline pmem::Block* tables_vidx_to_addr(MemTable* mem_table,
                                               const BlkTable* blk_table,
                                               VirtualBlockIdx idx) {
  return mem_table->get(blk_table->get(idx));
}

}  // namespace ulayfs::dram
