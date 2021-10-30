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
  [[nodiscard]] pmem::TxEntry get_entry() const {
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
    if (block_idx == 0) return meta->get_tx_entry(local_idx);
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
   * @tparam Block MetaBlock or TxLogBlock
   * @param block the block that needs a next block to be allocated
   * @return the block id of the allocated block
   */
  template <class Block>
  LogicalBlockIdx alloc_next_block(Block block) const {
    // allocate the next block
    auto new_block_id = allocator->alloc(1);
    bool success = block->set_next_tx_block(new_block_id);
    if (success) {
      return new_block_id;
    } else {
      // there is a race condition for adding the new blocks
      allocator->free(new_block_id, 1);
      return block->get_next_tx_block();
    }
  }

  /**
   * @tparam Entry TxBeginEntry or TxCommitEntry
   * @param entry the tx entry to be appended
   * @return the index of the inserted entry
   */
  template <class Entry>
  pmem::TxEntryIdx try_append(Entry entry) {
    pmem::TxEntryIdx global_tx_tail = meta->get_tx_log_tail();
    auto [block_idx_hint, local_idx_hint] =
        global_tx_tail > local_tx_tail ? global_tx_tail : local_tx_tail;

    // append to the inline tx_entries
    if (block_idx_hint == 0) {
      auto local_idx = meta->try_append_tx(entry, local_idx_hint);
      if (local_idx == NUM_INLINE_TX_ENTRY - 1) alloc_next_block(meta);
      if (local_idx >= 0) return {0, local_idx};
    }

    // inline tx_entries are full, append to the tx log blocks
    while (true) {
      auto block = mem_table->get_addr(block_idx_hint);
      auto tx_log_block = &block->tx_log_block;

      // try to append an entry to the current block
      auto local_idx = tx_log_block->try_append(entry, local_idx_hint);
      if (local_idx == NUM_TX_ENTRY - 1) alloc_next_block(tx_log_block);
      if (local_idx >= 0) return {block_idx_hint, local_idx};

      // current block if full, try next one
      block_idx_hint = tx_log_block->get_next_tx_block();
      local_idx_hint = 0;
    }
  }

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};
}  // namespace ulayfs::dram
