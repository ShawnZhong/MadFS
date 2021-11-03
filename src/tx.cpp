#include "tx.h"

#include "block.h"
#include "entry.h"
#include "idx.h"
#include "layout.h"

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
  pmem::TxEntryIdx curr_idx = local_tx_tail;
  pmem::TxLogBlock* curr_block = local_tx_tail_block;
  LogicalBlockIdx next_block_idx;
  LogicalBlockIdx tmp;

  if (!curr_block) {  // handle meta
    if (!(next_block_idx = meta->get_next_tx_block())) {
      curr_idx.local_idx = meta->find_tail(curr_idx.local_idx);
      for (; curr_idx.local_idx < NUM_INLINE_TX_ENTRY; ++curr_idx.local_idx)
        if (meta->try_append(tx_begin_entry, curr_idx.local_idx) == 0)
          return curr_idx;
      next_block_idx = alloc_next_block(meta);
    }
    curr_idx.block_idx = next_block_idx;
    curr_idx.local_idx = 0;
    curr_block = &mem_table->get_addr(curr_idx.block_idx)->tx_log_block;
  }

  tmp = curr_idx.block_idx;
  curr_idx = find_tail(curr_idx);
  // if not in the same block, update curr_block
  if (curr_idx.block_idx != tmp)
    curr_block = &mem_table->get_addr(curr_idx.block_idx)->tx_log_block;

retry:
  for (; curr_idx.local_idx < NUM_TX_ENTRY; ++curr_idx.local_idx) {
    auto res = curr_block->try_append(tx_begin_entry, curr_idx.local_idx);
    if (!res) return curr_idx;
  }

  // if fail to append to the current block; allocate a new one and retry
  curr_idx.block_idx = alloc_next_block(curr_block);
  curr_idx.local_idx = 0;
  curr_block = &mem_table->get_addr(curr_idx.block_idx)->tx_log_block;
  goto retry;
}

pmem::TxEntryIdx TxMgr::commit_tx(pmem::LogEntryIdx log_entry_idx) {
  // TODO: compute begin_offset from tx_begin_idx
  pmem::TxCommitEntry tx_commit_entry{0, log_entry_idx};
  // TODO: impl...
  return {0, 0};
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
