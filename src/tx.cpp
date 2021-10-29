#include "tx.h"

namespace ulayfs::dram {
void TxMgr::next_tx_idx(pmem::TxEntryIdx& idx,
                        pmem::TxLogBlock*& tx_log_block) const {
  // the current one is an inline tx entry
  if (idx.block_idx == 0) {
    // the next entry is still an inline tx entry
    if (idx.local_idx + 1 < NUM_INLINE_TX_ENTRY) {
      idx.local_idx++;
      return;
    }

    // move to the tx block
    idx = meta->get_tx_log_head();
    return;
  }

  // the current one is in tx_log_block, and the next one is in the same block
  if (idx.local_idx + 1 < NUM_TX_ENTRY) {
    idx.local_idx++;
    return;
  }

  // move to the next block
  tx_log_block = &mem_table->get_addr(idx.block_idx)->tx_log_block;
  idx.block_idx = tx_log_block->get_next_block_idx();
  idx.local_idx = 0;
  assert(idx.block_idx != 0);
  return;
}

LogicalBlockIdx TxMgr::get_next_tx_block(pmem::TxLogBlock* tx_log_block) const {
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

pmem::TxEntryIdx TxMgr::begin_tx(VirtualBlockIdx begin_virtual_idx,
                                 uint32_t num_blocks) {
  pmem::TxBeginEntry tx_begin_entry{begin_virtual_idx, num_blocks};
  pmem::TxEntryIdx global_tx_tail = meta->get_tx_log_tail();
  auto [block_idx_hint, local_idx_hint] =
      global_tx_tail > local_tx_tail ? global_tx_tail : local_tx_tail;

  // append to the inline tx_entries
  if (block_idx_hint == 0) {
    auto local_idx = meta->inline_try_begin(tx_begin_entry, local_idx_hint);
    if (local_idx >= 0) {
      local_tx_tail = {0, local_idx};
      goto done;
    }
  }

  // inline tx_entries are full, append to the tx log blocks
  while (true) {
    auto block = mem_table->get_addr(block_idx_hint);
    auto tx_log_block = &block->tx_log_block;

    // try to append a begin entry to the current block
    auto local_idx = tx_log_block->try_begin(tx_begin_entry, local_idx_hint);
    if (local_idx >= 0) {
      local_tx_tail = {block_idx_hint, local_idx};
      goto done;
    }

    // current block if full, try next one
    block_idx_hint = get_next_tx_block(tx_log_block);
    local_idx_hint = 0;
  }

done:
  // FIXME: in the latest design, only update tail if flush
  meta->set_tx_log_tail(local_tx_tail);
  return local_tx_tail;
}

pmem::TxEntryIdx TxMgr::commit_tx(pmem::TxEntryIdx tx_begin_idx,
                                  pmem::LogEntryIdx log_entry_idx) {
  // TODO: compute begin_offset from tx_begin_idx
  pmem::TxCommitEntry tx_commit_entry{0, log_entry_idx};
  pmem::TxEntryIdx global_tx_tail = meta->get_tx_log_tail();
  auto [block_idx_hint, local_idx_hint] =
      global_tx_tail > local_tx_tail ? global_tx_tail : local_tx_tail;

  // append to the inline tx_entries
  if (block_idx_hint == 0) {
    auto local_idx = meta->inline_try_commit(tx_commit_entry, local_idx_hint);
    if (local_idx >= 0) {
      local_tx_tail = {0, local_idx};
      goto done;
    }
  }

  // inline tx_entries are full, append to the tx log blocks
  while (true) {
    auto block = mem_table->get_addr(block_idx_hint);
    auto tx_log_block = &block->tx_log_block;

    // try to append a begin entry to the current block
    auto local_idx = tx_log_block->try_commit(tx_commit_entry, local_idx_hint);
    if (local_idx >= 0) {
      local_tx_tail = {block_idx_hint, local_idx};
      goto done;
    }

    // current block if full, try next one
    block_idx_hint = get_next_tx_block(tx_log_block);
    local_idx_hint = 0;
  }

done:
  meta->set_tx_log_tail(local_tx_tail);
  return local_tx_tail;
}

pmem::LogEntryIdx TxMgr::write_log_entry(VirtualBlockIdx begin_virtual_idx,
                                         LogicalBlockIdx begin_logical_idx,
                                         uint8_t num_blocks,
                                         uint16_t last_remaining) {
  // prepare the log_entry
  pmem::LogEntry log_entry;  // NOLINT(cppcoreguidelines-pro-type-member-init)
  log_entry.op = pmem::LOG_OVERWRITE;
  log_entry.last_remaining = last_remaining;
  log_entry.num_blocks = num_blocks;
  log_entry.next.block_idx = 0;
  log_entry.next.local_idx = 0;
  log_entry.begin_virtual_idx = begin_virtual_idx;
  log_entry.begin_logical_idx = begin_logical_idx;

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

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << "Transaction Log: \n";

  pmem::TxEntryIdx idx{};
  pmem::TxLogBlock* tx_log_block{nullptr};

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(idx, tx_log_block);
    if (!tx_entry.is_valid()) break;
    out << "\t" << idx << ": " << tx_entry << "\n";
    tx_mgr.next_tx_idx(idx, tx_log_block);
  }

  return out;
}
}  // namespace ulayfs::dram
