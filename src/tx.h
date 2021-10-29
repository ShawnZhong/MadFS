#pragma once

#include <ostream>

#include "alloc.h"
#include "block.h"
#include "mtable.h"

namespace ulayfs::dram {

class TxMgr {
 private:
  pmem::MetaBlock* meta;

  Allocator* allocator;
  MemTable* mem_table;

  // the tail of the local tx entry
  pmem::TxEntryIdx local_tx_tail;

  // the block for the local_tx_tail
  pmem::TxLogBlock* local_tx_log_block;

  // the tail of the local log entry
  pmem::LogEntryIdx local_log_tail;

 public:
  TxMgr() = default;
  TxMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table)
      : meta(meta),
        allocator(allocator),
        mem_table(mem_table),
        local_tx_tail(),
        local_tx_log_block(nullptr),
        local_log_tail() {}

  /**
   * Move to the next tx index
   */
  void forward_tx_idx() {
    local_tx_tail = get_next_tx_idx(local_tx_tail, &local_tx_log_block);
  }

  /**
   * @return the current transaction entry
   */
  [[nodiscard]] pmem::TxEntry get_curr_tx_entry() const {
    return get_tx_entry(local_tx_tail, local_tx_log_block);
  }

  /**
   * Begin a transaction that affects the range of blocks
   * [start_virtual_idx, start_virtual_idx + num_blocks)
   * @param start_virtual_idx
   * @param num_blocks
   */
  pmem::TxEntryIdx begin_tx(VirtualBlockIdx start_virtual_idx,
                            uint32_t num_blocks) {
    pmem::TxBeginEntry tx_begin_entry{start_virtual_idx, num_blocks};
    auto tx_log_tail = append_tx_begin_entry(tx_begin_entry);
    meta->set_tx_log_tail(tx_log_tail);
    return tx_log_tail;
  }

  pmem::TxEntryIdx commit_tx(pmem::TxEntryIdx tx_begin_idx,
                             pmem::LogEntryIdx log_entry_idx) {
    // TODO: compute begin_offset from tx_begin_idx
    pmem::TxCommitEntry tx_commit_entry{0, log_entry_idx};
    auto tx_log_tail = append_tx_commit_entry(tx_commit_entry);
    meta->set_tx_log_tail(tx_log_tail);
    return tx_log_tail;
  }

  pmem::LogEntryIdx write_log_entry(VirtualBlockIdx start_virtual_idx,
                                    LogicalBlockIdx start_logical_idx,
                                    uint8_t num_blocks,
                                    uint16_t last_remaining);

 private:
  /**
   * Read the entry from the MetaBlock or TxLogBlock
   */
  pmem::TxEntry get_tx_entry(pmem::TxEntryIdx idx,
                             pmem::TxLogBlock* tx_log_block) const {
    const auto [block_idx, local_idx] = idx;
    if (block_idx == 0) return meta->get_inline_tx_entry(local_idx);
    return tx_log_block->get_entry(local_idx);
  }

  /**
   * Move to the next transaction entry
   *
   * @param idx the current index
   * @param tx_log_block output parameter, change to the TxLogBlock
   * corresponding to the next idx
   *
   * @return the next tx entry
   */
  pmem::TxEntryIdx get_next_tx_idx(pmem::TxEntryIdx idx,
                                   pmem::TxLogBlock** tx_log_block) const;

  /**
   * given a current tx_log_block, return the next block id
   * allocate one if the next one doesn't exist
   */
  LogicalBlockIdx get_next_tx_log_block_idx(pmem::TxLogBlock* tx_log_block);

  /**
   * append a transaction begin_tx entry to the tx_log
   */
  pmem::TxEntryIdx append_tx_begin_entry(pmem::TxBeginEntry tx_begin_entry);

  /**
   * append a transaction commit_tx entry to the tx_log
   */
  pmem::TxEntryIdx append_tx_commit_entry(pmem::TxCommitEntry tx_commit_entry);

  // debug
 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};
}  // namespace ulayfs::dram
