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
                                    uint16_t last_remaining) {
    // prepare the log_entry
    pmem::LogEntry log_entry;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    log_entry.op = pmem::LOG_OVERWRITE;
    log_entry.last_remaining = last_remaining;
    log_entry.num_blocks = num_blocks;
    log_entry.next.block_idx = 0;
    log_entry.next.local_idx = 0;
    log_entry.start_virtual_idx = start_virtual_idx;
    log_entry.start_logical_idx = start_logical_idx;

    // check we need to allocate a new log entry block
    if (local_log_tail.block_idx == 0 ||
        local_log_tail.local_idx == NUM_LOG_ENTRY - 1) {
      local_log_tail.block_idx = allocator->alloc(1);
      local_log_tail.local_idx = 0;
    }

    // append the log entry
    auto block = mem_table->get_addr(local_log_tail.block_idx);
    auto log_entry_block = &block->log_entry_block;
    log_entry_block->append(log_entry, local_log_tail.local_idx);

    return local_log_tail;
  }

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
                                   pmem::TxLogBlock** tx_log_block) const {
    // the current one is an inline tx entry
    if (idx.block_idx == 0) {
      // the next entry is still an inline tx entry
      if (idx.local_idx + 1 < NUM_INLINE_TX_ENTRY) {
        idx.local_idx++;
        return idx;
      }

      // move to the tx block
      idx = meta->get_tx_log_head();
      return idx;
    }

    // the current on is in tx_log_block, and the next one is in the same block
    if (idx.local_idx + 1 < NUM_TX_ENTRY) {
      idx.local_idx++;
      return idx;
    }

    // move to the next block
    *tx_log_block = &mem_table->get_addr(idx.block_idx)->tx_log_block;
    idx.block_idx = (*tx_log_block)->get_next_block_idx();
    idx.local_idx = 0;
    assert(idx.block_idx != 0);
    return idx;
  }

  /**
   * given a current tx_log_block, return the next block id
   * allocate one if the next one doesn't exist
   */
  LogicalBlockIdx get_next_tx_log_block_idx(pmem::TxLogBlock* tx_log_block) {
    auto block_idx = tx_log_block->get_next_block_idx();
    if (block_idx != 0) return block_idx;

    // allocate the next block
    auto new_block_id = allocator->alloc(1);
    bool success = tx_log_block->set_next_block_idx(new_block_id);
    if (success) {
      return new_block_id;
    } else {
      // there is a race condition for adding the new blocks
      allocator->free(new_block_id, 1);
      return tx_log_block->get_next_block_idx();
    }
  }

  /**
   * append a transaction begin_tx entry to the tx_log
   */
  pmem::TxEntryIdx append_tx_begin_entry(pmem::TxBeginEntry tx_begin_entry) {
    auto [block_idx_hint, local_idx_hint] = meta->get_tx_log_tail();

    // append to the inline tx_entries
    if (block_idx_hint == 0) {
      auto local_idx = meta->inline_try_begin(tx_begin_entry, local_idx_hint);
      if (local_idx >= 0) return {0, local_idx};
    }

    // inline tx_entries are full, append to the tx log blocks
    while (true) {
      auto block = mem_table->get_addr(block_idx_hint);
      auto tx_log_block = &block->tx_log_block;

      // try to append a begin entry to the current block
      auto local_idx = tx_log_block->try_begin(tx_begin_entry, local_idx_hint);
      if (local_idx >= 0) return {block_idx_hint, local_idx};

      // current block if full, try next one
      block_idx_hint = get_next_tx_log_block_idx(tx_log_block);
      local_idx_hint = 0;
    }
  };

  /**
   * append a transaction commit_tx entry to the tx_log
   */
  pmem::TxEntryIdx append_tx_commit_entry(pmem::TxCommitEntry tx_commit_entry) {
    // TODO: OCC
    auto [block_idx_hint, local_idx_hint] = meta->get_tx_log_tail();

    // append to the inline tx_entries
    if (block_idx_hint == 0) {
      auto local_idx = meta->inline_try_commit(tx_commit_entry, local_idx_hint);
      if (local_idx >= 0) return {0, local_idx};
    }

    // inline tx_entries are full, append to the tx log blocks
    while (true) {
      auto block = mem_table->get_addr(block_idx_hint);
      auto tx_log_block = &block->tx_log_block;

      // try to append a begin entry to the current block
      auto local_idx =
          tx_log_block->try_commit(tx_commit_entry, local_idx_hint);
      if (local_idx >= 0) return {block_idx_hint, local_idx};

      // current block if full, try next one
      block_idx_hint = get_next_tx_log_block_idx(tx_log_block);
      local_idx_hint = 0;
    }
  };

  // debug
 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
    out << "Transaction Log: \n";

    pmem::TxEntryIdx idx{};
    pmem::TxLogBlock* tx_log_block{nullptr};

    while (true) {
      auto tx_entry = tx_mgr.get_tx_entry(idx, tx_log_block);
      if (!tx_entry.is_valid()) break;
      out << "\t" << idx << ": " << tx_entry << "\n";
      idx = tx_mgr.get_next_tx_idx(idx, &tx_log_block);
    }

    return out;
  }
};
}  // namespace ulayfs::dram
