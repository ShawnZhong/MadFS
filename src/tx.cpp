#include "tx.h"

#include <cstddef>

#include "block.h"
#include "entry.h"
#include "idx.h"
#include "layout.h"
#include "params.h"
#include "utils.h"

namespace ulayfs::dram {

uint64_t TxMgr::try_commit(pmem::TxEntry entry, pmem::TxEntryIdx& tx_idx,
                           pmem::TxLogBlock*& tx_block, bool cont_if_fail) {
  pmem::TxEntryIdx curr_idx = tx_idx;
  pmem::TxLogBlock* curr_block = tx_block;
  LogicalBlockIdx next_block_idx;
  uint64_t ret;

  assert((curr_idx.block_idx == 0) == (curr_block == nullptr));

  for (;; advance_tx_idx(curr_idx, curr_block, true)) {
    ret = curr_idx.block_idx == 0
              ? meta->try_append(entry, curr_idx.local_idx)
              : curr_block->try_append(entry, curr_idx.local_idx);
    if (ret == 0) goto done;
    if (!cont_if_fail) goto done_no_update;
  }

done:
  tx_idx = curr_idx;
  tx_block = curr_block;
  // fall through
done_no_update:
  return ret;
}

// TODO: maybe reclaim the old blocks right after commit?
void TxMgr::do_cow(const void* buf, size_t count, size_t offset) {
  // caller should handle count == 0 so that we keep the invariant that once
  // do_cow is called, exactly one tx must happen
  assert(count != 0);
  size_t end_offset = offset + count;
  VirtualBlockIdx begin_vidx = offset >> BLOCK_SHIFT;
  VirtualBlockIdx begin_full_vidx = ALIGN_UP(offset, BLOCK_SIZE) >> BLOCK_SHIFT;
  VirtualBlockIdx end_vidx = ALIGN_UP(end_offset, BLOCK_SIZE) >> BLOCK_SHIFT;
  VirtualBlockIdx end_full_vidx = end_offset >> BLOCK_SHIFT;
  size_t num_blocks = end_vidx - begin_vidx;

  LogicalBlockIdx dst_idx = allocator->alloc(num_blocks);
  pmem::Block* dst_blocks = mem_table->get_addr(dst_idx);

  // special case that we have everything aligned, no OCC
  if (begin_vidx == begin_full_vidx && end_vidx == end_full_vidx) {
    assert(count % BLOCK_SIZE == 0);
    memcpy(reinterpret_cast<char*>(dst_blocks), buf, count);
    // we do unfence here because log_mgr's append will do fence
    persist_unfenced(dst_blocks, count);
    // then prep for commit
    pmem::LogEntry entry = {
        pmem::LogOp::LOG_OVERWRITE,        // op
        0,                                 // last_remaining
        static_cast<uint8_t>(num_blocks),  // num_blocks
        {},                                // next
        begin_vidx,                        // begin_virtual_idx
        dst_idx,                           // begin_logical_idx
    };
    pmem::LogEntryIdx log_idx = log_mgr->append(entry);
    try_commit(pmem::TxCommitEntry(num_blocks, begin_vidx, log_idx),
               tail_tx_idx, tail_tx_block, true);
    // TODO: update tail_tx_idx and tail_tx_block
    return;
  }

  // first copy middle blocks
  // NOTE: dst_blocks is pointer of Block*, so just add index is enough, no need
  // to do shift
  pmem::Block* mid_blocks = dst_blocks + (begin_full_vidx - begin_vidx);
  size_t num_mid_blocks = (end_full_vidx - begin_full_vidx);
  size_t bytes_first_block = (begin_full_vidx << BLOCK_SHIFT) - offset;
  size_t bytes_last_block = end_offset - (end_full_vidx << BLOCK_SHIFT);
  memcpy(reinterpret_cast<char*>(mid_blocks),
         static_cast<const char*>(buf) + bytes_first_block,
         num_mid_blocks << BLOCK_SHIFT);
  persist_unfenced(mid_blocks, num_mid_blocks << BLOCK_SHIFT);

  // TODO: then prepare the first and last block
}

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

void TxMgr::find_tail(pmem::TxEntryIdx& tx_idx,
                      pmem::TxLogBlock*& tx_block) const {
  pmem::TxEntryIdx& curr_idx = tx_idx;
  pmem::TxLogBlock*& curr_block = tx_block;
  LogicalBlockIdx next_block_idx;
  assert((curr_idx.block_idx == 0) == (curr_block == nullptr));

  if (!curr_idx.block_idx) {  // search from meta
    if (!(next_block_idx = meta->get_next_tx_block())) {
      curr_idx.local_idx = meta->find_tail(curr_idx.local_idx);
      if (curr_idx.local_idx < NUM_INLINE_TX_ENTRY) goto done;
      next_block_idx = alloc_next_block(meta);
    }
    curr_idx.block_idx = next_block_idx;
    curr_idx.local_idx = 0;
    curr_block = &mem_table->get_addr(curr_idx.block_idx)->tx_log_block;
  }

  if (!(next_block_idx = curr_block->get_next())) {
    curr_idx.local_idx = curr_block->find_tail(curr_idx.local_idx);
    if (curr_idx.local_idx < NUM_TX_ENTRY) goto done;
  }

retry:
  do {
    curr_idx.block_idx = next_block_idx;
    curr_block = &(mem_table->get_addr(next_block_idx)->tx_log_block);
  } while ((next_block_idx = curr_block->get_next()));

  curr_idx.local_idx = curr_block->find_tail();
  if (curr_idx.local_idx < NUM_TX_ENTRY) goto done;

  next_block_idx = alloc_next_block(meta);
  goto retry;

done:
  tx_idx = curr_idx;
  tx_block = curr_block;
};

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
