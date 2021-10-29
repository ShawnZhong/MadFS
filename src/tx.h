#pragma once

#include <ostream>

#include "alloc.h"
#include "block.h"
#include "entry.h"
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

 public:
  TxMgr() = default;
  TxMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table)
      : meta(meta),
        allocator(allocator),
        mem_table(mem_table),
        local_tx_tail(),
        local_tx_log_block(nullptr) {}

  /**
   * Move to the next tx index
   */
  void advance() { advance_tx_idx(local_tx_tail, local_tx_log_block); }

  /**
   * @return the current transaction entry
   */
  [[nodiscard]] pmem::TxEntry get() const {
    return get_entry_from_block(local_tx_tail, local_tx_log_block);
  }

  /**
   * Begin a transaction that affects the range of blocks
   * [begin_virtual_idx, begin_virtual_idx + num_blocks)
   */
  pmem::TxEntryIdx begin_tx(VirtualBlockIdx begin_virtual_idx,
                            uint32_t num_blocks);

  pmem::TxEntryIdx commit_tx(pmem::TxEntryIdx tx_begin_idx,
                             pmem::LogEntryIdx log_entry_idx);

 private:
  /**
   * Read the entry from the MetaBlock or TxLogBlock
   */
  pmem::TxEntry get_entry_from_block(pmem::TxEntryIdx idx,
                                     pmem::TxLogBlock* tx_log_block) const {
    const auto [block_idx, local_idx] = idx;
    if (block_idx == 0) return meta->get_inline_tx_entry(local_idx);
    return tx_log_block->get(local_idx);
  }

  /**
   * Move to the next transaction entry
   *
   * @param idx the current index, will be changed to the next index
   * @param tx_log_block output parameter, change to the TxLogBlock
   * corresponding to the next idx
   */
  void advance_tx_idx(pmem::TxEntryIdx& idx,
                      pmem::TxLogBlock*& tx_log_block) const;

  /**
   * given a current tx_log_block, return the next block id
   * allocate one if the next one doesn't exist
   *
   * @return the index of the next TxLogBlock
   */
  LogicalBlockIdx get_next_tx_block(pmem::TxLogBlock* tx_log_block) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};
}  // namespace ulayfs::dram
