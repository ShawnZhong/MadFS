#include "tx.h"

#include <cstddef>

#include "block.h"
#include "btable.h"
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
  // whether copy the first/last block (if both not, no COW is necessary)
  bool copy_first = begin_full_vidx != begin_vidx;
  bool copy_last = end_full_vidx != end_vidx;

  LogicalBlockIdx dst_idx = allocator->alloc(num_blocks);
  pmem::Block* dst_blocks = mem_table->get_addr(dst_idx);

  // for overwrite, "last_remaining" is zero; only in append we will care so
  // TODO: handle linked list
  pmem::LogEntry log_entry = {
      pmem::LogOp::LOG_OVERWRITE,        // op
      0,                                 // last_remaining
      static_cast<uint8_t>(num_blocks),  // num_blocks
      {},                                // next
      begin_vidx,                        // begin_virtual_idx
      dst_idx,                           // begin_logical_idx
  };
  pmem::LogEntryIdx log_idx = log_mgr->append(log_entry, /* fenced */ false);
  // It's fine that we append log first as long we don't publish it by tx

  // special case that we have everything aligned, no OCC
  if (!copy_first && !copy_last) {
    assert(count % BLOCK_SIZE == 0);
    memcpy(reinterpret_cast<char*>(dst_blocks), buf, count);
    // we do unfence here because log_mgr's append will do fence
    persist_unfenced(dst_blocks, count);
    pmem::TxEntryIdx tail_tx_idx;
    pmem::TxLogBlock* tail_tx_block;
    // make a local copy
    blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);
    _mm_sfence();  // make sure flush of block and log entry is done
    try_commit(pmem::TxCommitEntry(num_blocks, begin_vidx, log_idx),
               tail_tx_idx, tail_tx_block, true);
    return;
  }

  // TODO: handle the case the first block is the last block...
  if (begin_full_vidx > end_full_vidx) {
    assert(begin_full_vidx - end_full_vidx == 1);
    // ???
  }

  // first copy middle blocks
  pmem::Block* full_blocks = dst_blocks + (begin_full_vidx - begin_vidx);
  size_t num_full_blocks = (end_full_vidx - begin_full_vidx);
  size_t bytes_first_block = (begin_full_vidx << BLOCK_SHIFT) - offset;
  size_t bytes_last_block = end_offset - (end_full_vidx << BLOCK_SHIFT);
  memcpy(reinterpret_cast<char*>(full_blocks),
         static_cast<const char*>(buf) + bytes_first_block,
         num_full_blocks << BLOCK_SHIFT);
  persist_unfenced(full_blocks, num_full_blocks << BLOCK_SHIFT);

  pmem::TxEntryIdx tail_tx_idx;
  pmem::TxLogBlock* tail_tx_block;
  blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);

  LogicalBlockIdx begin_lidx, end_lidx;
  pmem::Block* first_src_block;
  pmem::Block* last_src_block;
  uint64_t ret;

  if (copy_first) {
    assert(begin_full_vidx - begin_vidx == 1);
    begin_lidx = blk_table->get(begin_vidx);
    first_src_block = mem_table->get_addr(begin_lidx);
  }
  if (copy_last) {
    assert(end_vidx - end_full_vidx == 1);
    end_lidx = blk_table->get(end_full_vidx);
    last_src_block = mem_table->get_addr(end_lidx);
  }

redo:  // redo copy
  if (copy_first) {
    // copy first block
    memcpy(dst_blocks->data, first_src_block, BLOCK_SIZE);
    memcpy(dst_blocks->data + BLOCK_SIZE - bytes_first_block, buf,
           bytes_first_block);
    persist_unfenced(dst_blocks, BLOCK_SIZE);
  }
  if (copy_last) {
    // copy last block
    memcpy((dst_blocks + (end_full_vidx - begin_vidx))->data, last_src_block,
           BLOCK_SIZE);
    memcpy((dst_blocks + (end_full_vidx - begin_vidx))->data,
           reinterpret_cast<const char*>(buf) + (count - bytes_last_block),
           bytes_last_block);
    persist_unfenced((dst_blocks + (end_full_vidx - begin_vidx)), BLOCK_SIZE);
  }
  _mm_sfence();

retry:  // retry commit
  ret = try_commit(pmem::TxCommitEntry(num_blocks, begin_vidx, log_idx),
                   tail_tx_idx, tail_tx_block, false);
  if (ret == 0) return;
  // recalculate copy_first/last to indicate what we care about
  copy_first = begin_full_vidx != begin_vidx;
  copy_last = end_full_vidx != end_vidx;
  // handle_conflict will update copy_first/last according to indicate whether
  // redo is needed
  if (handle_conflict(ret, tail_tx_idx, tail_tx_block, copy_first, copy_last,
                      begin_vidx, end_full_vidx, first_src_block,
                      last_src_block))
    goto redo;  // conflict is detected, redo copy
  else
    goto retry;  // we have moved to the new tail, retry commit
}

bool TxMgr::handle_conflict(pmem::TxEntry curr_entry,
                            pmem::TxEntryIdx& tail_tx_idx,
                            pmem::TxLogBlock*& tail_tx_block, bool& copy_first,
                            bool& copy_last, VirtualBlockIdx first_vidx,
                            VirtualBlockIdx last_vidx,
                            pmem::Block*& first_src_block,
                            pmem::Block*& last_src_block) {
  bool redo_first = false;
  bool redo_last = false;
  VirtualBlockIdx begin_vidx;
  uint32_t num_blocks;

  do {
    // TODO: handle linked list
    if (curr_entry.is_commit()) {
      LogicalBlockIdx begin_lidx = 0;
      num_blocks = curr_entry.commit_entry.num_blocks;
      if (num_blocks)
        begin_vidx = curr_entry.commit_entry.begin_virtual_idx;
      else {  // dereference log_entry_idx
        pmem::LogEntry log_entry =
            get_log_entry_from_commit(curr_entry.commit_entry);
        num_blocks = log_entry.num_blocks;
        begin_vidx = log_entry.begin_virtual_idx;
        begin_lidx = log_entry.begin_logical_idx;
      }
      if (copy_first && begin_vidx <= first_vidx &&
          first_vidx < begin_vidx + num_blocks) {
        if (begin_lidx == 0) {  // lazy dereference log idx
          pmem::LogEntry log_entry =
              get_log_entry_from_commit(curr_entry.commit_entry);
          begin_lidx = log_entry.begin_logical_idx;
        }
        redo_first = true;
        LogicalBlockIdx lidx = begin_lidx + (first_vidx - begin_vidx);
        first_src_block = mem_table->get_addr(lidx);
      }
      if (copy_last && begin_vidx <= last_vidx &&
          last_vidx < begin_vidx + num_blocks) {
        if (begin_lidx == 0) {  // lazy dereference log idx
          pmem::LogEntry log_entry =
              get_log_entry_from_commit(curr_entry.commit_entry);
          begin_lidx = log_entry.begin_logical_idx;
        }
        redo_last = true;
        LogicalBlockIdx lidx = begin_lidx + (last_vidx - begin_vidx);
        last_src_block = mem_table->get_addr(lidx);
      }
    } else {
      // FIXME: there should not be any other one
      assert(0);
    }
    advance_tx_idx(tail_tx_idx, tail_tx_block);
    curr_entry = get_entry(tail_tx_idx, tail_tx_block);
  } while (curr_entry.is_valid());

  copy_first = redo_first;
  copy_last = redo_last;
  return redo_first || redo_last;
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
