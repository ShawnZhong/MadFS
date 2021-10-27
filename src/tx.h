#pragma once

#include <ostream>

#include "file.h"
#include "tx_iter.h"

namespace ulayfs::dram {

class TxMgr {
 private:
  pmem::MetaBlock* meta;

  Allocator* allocator;
  MemTable* mem_table;

  // the tail of the local log entry
  pmem::LogEntryIdx local_tail;

  /**
   * given a current tx_log_block, return the next block id
   * allocate one if the next one doesn't exist
   */
  inline pmem::LogicalBlockIdx get_next_tx_log_block_idx(
      pmem::TxLogBlock* tx_log_block) {
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
  inline pmem::TxEntryIdx append_tx_begin_entry(
      pmem::TxBeginEntry tx_begin_entry) {
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
  inline pmem::TxEntryIdx append_tx_commit_entry(
      pmem::TxCommitEntry tx_commit_entry) {
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

 public:
  TxMgr() = default;
  TxMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table)
      : meta(meta), allocator(allocator), mem_table(mem_table), local_tail() {}

  [[nodiscard]] TxIter begin() const { return TxIter(meta, mem_table, {0, 0}); }
  [[nodiscard]] TxIter end() const { return TxIter(meta, mem_table, {0, -1}); }
  [[nodiscard]] TxIter iter(pmem::TxEntryIdx idx) const {
    return {meta, mem_table, idx};
  }

  /**
   * Begin a transaction that affects the range of blocks
   * [start_virtual_idx, start_virtual_idx + num_blocks)
   * @param start_virtual_idx
   * @param num_blocks
   */
  pmem::TxEntryIdx begin_tx(pmem::VirtualBlockIdx start_virtual_idx,
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

  pmem::LogEntryIdx write_log_entry(pmem::VirtualBlockIdx start_virtual_idx,
                                    pmem::LogicalBlockIdx start_logical_idx,
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
    if (local_tail.block_idx == 0 ||
        local_tail.local_idx == pmem::NUM_LOG_ENTRY - 1) {
      local_tail.block_idx = allocator->alloc(1);
      local_tail.local_idx = 0;
    }

    // append the log entry
    auto block = mem_table->get_addr(local_tail.block_idx);
    auto log_entry_block = &block->log_entry_block;
    log_entry_block->append(log_entry, local_tail.local_idx);

    return local_tail;
  }

  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
    out << "Transaction Log: \n";
    for (auto it = tx_mgr.begin(); it != tx_mgr.end(); ++it) {
      out << "\t" << it.get_idx() << ": " << *it << "\n";
    }
    return out;
  }
};
}  // namespace ulayfs::dram
