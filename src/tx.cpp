#include "tx.h"

#include "entry.h"

namespace ulayfs::dram {
void TxMgr::advance_tx_idx(pmem::TxEntryIdx& idx,
                           pmem::TxLogBlock*& tx_log_block) const {
  assert(idx.local_idx >= 0);

  // next index is within the same block, just increment local index
  uint16_t capacity = idx.block_idx == 0 ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
  if (idx.local_idx < capacity - 1) {
    idx.local_idx++;
    return;
  }

  // move to the next block
  idx.block_idx = idx.block_idx == 0 ? meta->get_next_tx_block()
                                     : tx_log_block->get_next_tx_block();
  assert(idx.block_idx != 0);
  idx.local_idx = 0;
  tx_log_block = &mem_table->get_addr(idx.block_idx)->tx_log_block;
}

pmem::TxEntryIdx TxMgr::begin_tx(VirtualBlockIdx begin_virtual_idx,
                                 uint32_t num_blocks) {
  pmem::TxBeginEntry tx_begin_entry{begin_virtual_idx, num_blocks};
  return try_append(tx_begin_entry);
}

pmem::TxEntryIdx TxMgr::commit_tx(pmem::TxEntryIdx tx_begin_idx,
                                  pmem::LogEntryIdx log_entry_idx) {
  // TODO: compute begin_offset from tx_begin_idx
  pmem::TxCommitEntry tx_commit_entry{0, log_entry_idx};
  return try_append(tx_commit_entry);
}

template <class B>
LogicalBlockIdx TxMgr::alloc_next_block(B* block) const {
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

// explicit template instantiations
template LogicalBlockIdx TxMgr::alloc_next_block(pmem::MetaBlock* block) const;
template LogicalBlockIdx TxMgr::alloc_next_block(pmem::TxLogBlock* block) const;

template <class Entry>
pmem::TxEntryIdx TxMgr::try_append(Entry entry) {
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

// explicit template instantiations
template pmem::TxEntryIdx TxMgr::try_append(pmem::TxCommitEntry entry);
template pmem::TxEntryIdx TxMgr::try_append(pmem::TxBeginEntry entry);

void TxMgr::copy_data(const void* buf, size_t count, uint64_t local_offset,
                      LogicalBlockIdx& begin_dst_idx,
                      LogicalBlockIdx& begin_src_idx) {
  // the address of the start of the new blocks
  char* dst = mem_table->get_addr(begin_dst_idx)->data;

  // if the offset is not block-aligned, copy the remaining bytes at the
  // beginning to the shadow page
  if (local_offset) {
    char* src = mem_table->get_addr(begin_src_idx)->data;
    memcpy(dst, src, local_offset);
  }

  // write the actual buffer
  memcpy(dst + local_offset, buf, count);

  // persist the changes
  pmem::persist_fenced(dst, count + local_offset);
}

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << "Transaction Log: \n";

  pmem::TxEntryIdx idx{};
  pmem::TxLogBlock* tx_log_block{nullptr};

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(idx, tx_log_block);
    if (!tx_entry.is_valid()) break;
    out << "\t" << idx << ": " << tx_entry << "\n";
    tx_mgr.advance_tx_idx(idx, tx_log_block);
  }

  return out;
}
}  // namespace ulayfs::dram
