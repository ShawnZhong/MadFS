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

/*
 * TxMgr
 */

pmem::TxEntry TxMgr::try_commit(pmem::TxEntry entry, pmem::TxEntryIdx& tx_idx,
                                pmem::TxLogBlock*& tx_block,
                                bool cont_if_fail = false) {
  pmem::TxEntryIdx curr_idx = tx_idx;
  pmem::TxLogBlock* curr_block = tx_block;

  bool is_inline = curr_idx.block_idx == 0;

  assert(is_inline == (curr_block == nullptr));

  while (true) {
    pmem::TxEntry conflict_entry =
        is_inline ? meta->try_append(entry, curr_idx.local_idx)
                  : curr_block->try_append(entry, curr_idx.local_idx);
    if (conflict_entry.is_empty()) {  // success
      tx_idx = curr_idx;
      tx_block = curr_block;
      return conflict_entry;
    }
    if (!cont_if_fail) return conflict_entry;
    advance_tx_idx(curr_idx, curr_block, /*do_alloc*/ true);
  }
}

// TODO: maybe reclaim the old blocks right after commit?
void TxMgr::do_cow(const void* buf, size_t count, size_t offset) {
  Tx tx(this, buf, count, offset);
  tx.do_cow();
}

pmem::Block* TxMgr::get_data_block_from_vidx(VirtualBlockIdx idx) const {
  LogicalBlockIdx logical_block_idx = blk_table->get(idx);
  return mem_table->get_addr(logical_block_idx);
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
  out << "Transactions: \n";

  pmem::TxEntryIdx idx{};
  pmem::TxLogBlock* tx_log_block{nullptr};

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(idx, tx_log_block);
    if (!tx_entry.is_valid()) break;
    auto commit_entry = tx_entry.commit_entry;
    out << "\t" << idx << " -> " << commit_entry << "\n";
    out << "\t\t" << commit_entry.log_entry_idx << " -> "
        << tx_mgr.get_log_entry_from_commit(commit_entry) << "\n";
    tx_mgr.advance_tx_idx(idx, tx_log_block);
  }

  return out;
}

/*
 * Tx
 */

TxMgr::Tx::Tx(TxMgr* tx_mgr, const void* buf, size_t count, size_t offset)
    : tx_mgr(tx_mgr),

      // input properties
      buf(static_cast<const char*>(buf)),
      count(count),
      offset(offset),

      // derived properties
      end_offset(offset + count),
      begin_vidx(offset >> BLOCK_SHIFT),
      end_vidx(ALIGN_UP(end_offset, BLOCK_SIZE) >> BLOCK_SHIFT),
      num_blocks(end_vidx - begin_vidx),

      dst_idx(tx_mgr->allocator->alloc(num_blocks)),
      dst_blocks(tx_mgr->mem_table->get_addr(dst_idx)),

      log_idx([&] {  // IIFE for complex initialization
        // for overwrite, "last_remaining" is zero;
        // only in append we will care so
        // TODO: handle linked list
        pmem::LogEntry log_entry = {
            pmem::LogOp::LOG_OVERWRITE,        // op
            0,                                 // last_remaining
            static_cast<uint8_t>(num_blocks),  // num_blocks
            {},                                // next
            begin_vidx,                        // begin_virtual_idx
            dst_idx,                           // begin_logical_idx
        };
        // append log without fence because we only care flush completion before
        // try_commit
        return tx_mgr->log_mgr->append(log_entry, /* fenced */ false);
        // it's fine that we append log first as long we don't publish it by tx
      }()) {}

void TxMgr::Tx::do_cow() {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    do_cow_aligned();
    return;
  }

  // another special case where range is within a single block
  if ((offset >> BLOCK_SHIFT) == ((offset + count - 1) >> BLOCK_SHIFT)) {
    do_cow_single_block();
    return;
  }

  // unaligned multi-block write
  do_cow_multiple_blocks();
}

void TxMgr::Tx::do_cow_aligned() const {
  // since everything is block-aligned, we can copy data directly
  memcpy(dst_blocks->data, buf, count);

  // we have unfenced here because log_mgr's append will do fence
  persist_unfenced(dst_blocks, count);

  // make a local copy of the tx tail
  auto [tail_tx_idx, tail_tx_block] = tx_mgr->blk_table->get_tail_tx();

  // make sure flush of block and log entry is done
  _mm_sfence();

  // commit the transaction, it's fine if the tx fails due to race condition
  pmem::TxCommitEntry entry(num_blocks, begin_vidx, log_idx);
  tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block, /*cont_if_fail*/ true);
}

void TxMgr::Tx::do_cow_single_block() {
  assert(num_blocks == 1);

  // local_offset is the starting offset within the block
  const size_t local_offset = offset - begin_vidx * BLOCK_SIZE;

  // the tx entry to be committed
  const pmem::TxCommitEntry entry(num_blocks, begin_vidx, log_idx);

  // must acquire the tx tail before any get
  auto [tail_tx_idx, tail_tx_block] = tx_mgr->blk_table->get_tail_tx();

  // src block is the block to be copied over
  pmem::Block* src_block = tx_mgr->get_data_block_from_vidx(begin_vidx);

  bool copy = true;

redo:
  // copy data from the source block if src_block exists
  if (src_block) memcpy(dst_blocks->data, src_block->data, BLOCK_SIZE);

  // copy data from buf
  memcpy(dst_blocks->data + local_offset, buf, count);

  // persist the data
  persist_fenced(dst_blocks, BLOCK_SIZE);

retry:
  // try to commit the tx entry
  auto conflict_entry = tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block);
  if (conflict_entry.is_empty()) return;  // success, no conflict

  assert(copy);
  if (handle_conflict(conflict_entry, tail_tx_idx, tail_tx_block, copy,
                      begin_vidx, src_block))
    goto redo;
  else
    goto retry;
}

void TxMgr::Tx::do_cow_multiple_blocks() {
  // the index of the first virtual block that needs to be copied entirely
  const VirtualBlockIdx begin_full_vidx =
      ALIGN_UP(offset, BLOCK_SIZE) >> BLOCK_SHIFT;
  // the index of the last virtual block that needs to be copied entirely
  const VirtualBlockIdx end_full_vidx = end_offset >> BLOCK_SHIFT;

  // number of bytes to be written in the beginning.
  // If the offset is 4097, then this var should be 4095.
  const size_t first_block_local_offset = ALIGN_UP(offset, BLOCK_SIZE) - offset;

  // number of bytes to be written for the last block
  // If the end_offset is 4097, then this var should be 1.
  const size_t last_block_local_offset =
      end_offset - ALIGN_DOWN(end_offset, BLOCK_SIZE);

  // full blocks are blocks that can be written from buf directly without
  // copying the src data
  if (size_t num_full_blocks = end_full_vidx - begin_full_vidx;
      num_full_blocks > 0) {
    pmem::Block* full_blocks = dst_blocks + (begin_full_vidx - begin_vidx);
    const size_t num_bytes = num_full_blocks << BLOCK_SHIFT;
    memcpy(full_blocks->data, buf + first_block_local_offset, num_bytes);
    persist_unfenced(full_blocks, num_bytes);
  }

  // whether copy the first/last block
  bool copy_first = begin_full_vidx != begin_vidx;
  bool copy_last = end_full_vidx != end_vidx;

  // address of fisrt and last block (only set if copy_first/last is true)
  pmem::Block* first_src_block;
  pmem::Block* last_src_block;

  // only get a snapshot of the tail when starting critical piece
  auto [tail_tx_idx, tail_tx_block] = tx_mgr->blk_table->get_tail_tx();

  if (copy_first) {
    assert(begin_full_vidx - begin_vidx == 1);
    first_src_block = tx_mgr->get_data_block_from_vidx(begin_vidx);
  }
  if (copy_last) {
    assert(end_vidx - end_full_vidx == 1);
    last_src_block = tx_mgr->get_data_block_from_vidx(end_full_vidx);
  }

  // the entry to be committed
  const pmem::TxCommitEntry entry(num_blocks, begin_vidx, log_idx);

redo:
  // copy first block
  if (copy_first) {
    // copy the data from the source block if exits
    if (first_src_block) memcpy(dst_blocks->data, first_src_block, BLOCK_SIZE);

    // write data from the buf to the first block
    char* dst = dst_blocks->data + BLOCK_SIZE - first_block_local_offset;
    memcpy(dst, buf, first_block_local_offset);

    persist_unfenced(dst_blocks, BLOCK_SIZE);
  }

  // copy last block
  if (copy_last) {
    pmem::Block* last_dst_block = dst_blocks + (end_full_vidx - begin_vidx);

    // copy the data from the source block if exits
    if (last_src_block)
      memcpy(last_dst_block->data, last_src_block, BLOCK_SIZE);

    // write data from the buf to the last block
    const char* src = buf + (count - last_block_local_offset);
    memcpy(last_dst_block->data, src, last_block_local_offset);

    persist_unfenced(last_dst_block, BLOCK_SIZE);
  }
  _mm_sfence();

retry:
  // try to commit the transaction
  auto conflict_entry = tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block);
  if (conflict_entry.is_empty()) return;  // success

  // recalculate copy_first/last to indicate what we care about
  copy_first = begin_full_vidx != begin_vidx;
  copy_last = end_full_vidx != end_vidx;

  // handle_conflict will update copy_first/last according to indicate whether
  // redo is needed
  if (handle_conflict(conflict_entry, tail_tx_idx, tail_tx_block, copy_first,
                      copy_last, begin_vidx, end_full_vidx, first_src_block,
                      last_src_block))
    goto redo;  // conflict is detected, redo copy
  else
    goto retry;  // we have moved to the new tail, retry commit
}

bool TxMgr::Tx::handle_conflict(
    pmem::TxEntry curr_entry, pmem::TxEntryIdx& tail_tx_idx,
    pmem::TxLogBlock*& tail_tx_block, bool& copy_first, bool& copy_last,
    VirtualBlockIdx first_vidx, VirtualBlockIdx last_vidx,
    pmem::Block*& first_src_block, pmem::Block*& last_src_block) {
  bool redo_first = false;
  bool redo_last = false;
  VirtualBlockIdx begin_vidx;
  uint32_t num_blocks;

  do {
    // TODO: handle linked list
    if (curr_entry.is_commit()) {
      LogicalBlockIdx begin_lidx = 0;
      num_blocks = curr_entry.commit_entry.num_blocks;
      if (num_blocks) {  // inline tx
        begin_vidx = curr_entry.commit_entry.begin_virtual_idx;
      } else {  // dereference log_entry_idx
        pmem::LogEntry log_entry =
            tx_mgr->get_log_entry_from_commit(curr_entry.commit_entry);
        num_blocks = log_entry.num_blocks;
        begin_vidx = log_entry.begin_virtual_idx;
        begin_lidx = log_entry.begin_logical_idx;
      }
      if (copy_first && begin_vidx <= first_vidx &&
          first_vidx < begin_vidx + num_blocks) {
        if (begin_lidx == 0) {  // lazy dereference log idx
          pmem::LogEntry log_entry =
              tx_mgr->get_log_entry_from_commit(curr_entry.commit_entry);
          begin_lidx = log_entry.begin_logical_idx;
        }
        redo_first = true;
        LogicalBlockIdx lidx = begin_lidx + (first_vidx - begin_vidx);
        first_src_block = tx_mgr->mem_table->get_addr(lidx);
      }
      if (copy_last && begin_vidx <= last_vidx &&
          last_vidx < begin_vidx + num_blocks) {
        if (begin_lidx == 0) {  // lazy dereference log idx
          pmem::LogEntry log_entry =
              tx_mgr->get_log_entry_from_commit(curr_entry.commit_entry);
          begin_lidx = log_entry.begin_logical_idx;
        }
        redo_last = true;
        LogicalBlockIdx lidx = begin_lidx + (last_vidx - begin_vidx);
        last_src_block = tx_mgr->mem_table->get_addr(lidx);
      }
    } else {
      // FIXME: there should not be any other one
      assert(0);
    }
    tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block);
    curr_entry = tx_mgr->get_entry(tail_tx_idx, tail_tx_block);
  } while (curr_entry.is_valid());

  copy_first = redo_first;
  copy_last = redo_last;
  return redo_first || redo_last;
}

bool TxMgr::Tx::handle_conflict(pmem::TxEntry curr_entry,
                                pmem::TxEntryIdx& tail_tx_idx,
                                pmem::TxLogBlock*& tail_tx_block, bool& copy,
                                VirtualBlockIdx vidx, pmem::Block*& src_block) {
  bool dummy_copy = false;
  VirtualBlockIdx dummy_vidx = 0;
  pmem::Block* dummy_src_block = nullptr;

  return handle_conflict(curr_entry, tail_tx_idx, tail_tx_block, copy,
                         dummy_copy, vidx, dummy_vidx, src_block,
                         dummy_src_block);
}

}  // namespace ulayfs::dram
