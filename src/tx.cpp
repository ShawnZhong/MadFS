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

pmem::TxEntry TxMgr::try_commit(pmem::TxEntry entry, TxEntryIdx& tx_idx,
                                pmem::TxBlock*& tx_block,
                                bool cont_if_fail = false) {
  TxEntryIdx curr_idx = tx_idx;
  pmem::TxBlock* curr_block = tx_block;

  handle_idx_overflow(tx_idx, tx_block, true);

  bool is_inline = curr_idx.block_idx == 0;
  assert(is_inline == (curr_block == nullptr));

  while (true) {
    pmem::TxEntry conflict_entry =
        is_inline ? meta->try_append(entry, curr_idx.local_idx)
                  : curr_block->try_append(entry, curr_idx.local_idx);
    if (!conflict_entry.is_valid()) {  // success
      tx_idx = curr_idx;
      tx_block = curr_block;
      return conflict_entry;
    }
    if (!cont_if_fail) return conflict_entry;
    bool success = advance_tx_idx(curr_idx, curr_block, /*do_alloc*/ true);
    assert(success);
  }
}

// TODO: maybe reclaim the old blocks right after commit?
void TxMgr::do_write(const void* buf, size_t count, size_t offset) {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    AlignedTx tx(this, buf, count, offset);
    tx.do_write();
    return;
  }

  // another special case where range is within a single block
  if ((offset >> BLOCK_SHIFT) == ((offset + count - 1) >> BLOCK_SHIFT)) {
    SingleBlockTx tx(this, buf, count, offset);
    tx.do_write();
    return;
  }

  // unaligned multi-block write
  MultiBlockTx tx(this, buf, count, offset);
  tx.do_write();
}

pmem::Block* TxMgr::vidx_to_addr(VirtualBlockIdx vidx) const {
  return tables_vidx_to_addr(this->mem_table, this->blk_table, vidx);
}

void TxMgr::find_tail(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block) const {
  TxEntryIdx& curr_idx = tx_idx;
  pmem::TxBlock*& curr_block = tx_block;
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
    curr_block = &mem_table->get(curr_idx.block_idx)->tx_block;
  }

  if (!(next_block_idx = curr_block->get_next())) {
    curr_idx.local_idx = curr_block->find_tail(curr_idx.local_idx);
    if (curr_idx.local_idx < NUM_TX_ENTRY) goto done;
  }

retry:
  do {
    curr_idx.block_idx = next_block_idx;
    curr_block = &(mem_table->get(next_block_idx)->tx_block);
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
template LogicalBlockIdx TxMgr::alloc_next_block(pmem::TxBlock* block) const;

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << "Transactions: \n";

  TxEntryIdx tx_idx = {0, 0};
  pmem::TxBlock* tx_block = nullptr;

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(tx_idx, tx_block);
    if (!tx_entry.is_valid()) break;
    auto commit_entry = tx_entry.commit_entry;
    out << "\t" << tx_idx << " -> " << commit_entry << "\n";
    out << "\t\t" << commit_entry.log_entry_idx << " -> "
        << tx_mgr.get_log_entry_from_commit(commit_entry) << "\n";
    if (!tx_mgr.advance_tx_idx(tx_idx, tx_block, /*do_alloc*/ false)) break;
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
      dst_blocks(tx_mgr->mem_table->get(dst_idx)),

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

/*
 * AlignedTx
 */

TxMgr::AlignedTx::AlignedTx(TxMgr* tx_mgr, const void* buf, size_t count,
                            size_t offset)
    : Tx(tx_mgr, buf, count, offset) {}

void TxMgr::AlignedTx::do_write() {
  // since everything is block-aligned, we can copy data directly
  memcpy(dst_blocks->data(), buf, count);

  // we have unfenced here because log_mgr's append will do fence
  persist_unfenced(dst_blocks, count);

  // make a local copy of the tx tail
  tx_mgr->blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);

  // make sure flush of block and log entry is done
  _mm_sfence();

  // commit the transaction, it's fine if the tx fails due to race condition
  pmem::TxCommitEntry entry(num_blocks, begin_vidx, log_idx);
  tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block, /*cont_if_fail*/ true);
}

/*
 * CoWTx
 */

TxMgr::CoWTx::CoWTx(TxMgr* tx_mgr, const void* buf, size_t count, size_t offset)
    : Tx(tx_mgr, buf, count, offset),
      entry(pmem::TxCommitEntry(num_blocks, begin_vidx, log_idx)),
      begin_full_vidx(ALIGN_UP(offset, BLOCK_SIZE) >> BLOCK_SHIFT),
      end_full_vidx(end_offset >> BLOCK_SHIFT),
      num_full_blocks(end_full_vidx - begin_full_vidx),
      copy_first(begin_full_vidx != begin_vidx),
      copy_last(end_full_vidx != end_vidx) {}

bool TxMgr::CoWTx::handle_conflict(pmem::TxEntry curr_entry,
                                   VirtualBlockIdx first_vidx,
                                   VirtualBlockIdx last_vidx = 0) {
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
        first_src_block = tx_mgr->mem_table->get(lidx);
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
        last_src_block = tx_mgr->mem_table->get(lidx);
      }
    } else {
      // FIXME: there should not be any other one
      assert(0);
    }
    bool success =
        tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, /*do_alloc*/ false);
    if (!success) break;
    curr_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
  } while (curr_entry.is_valid());

  copy_first = redo_first;
  copy_last = redo_last;
  return redo_first || redo_last;
}

/*
 * SingleBlockTx
 */

TxMgr::SingleBlockTx::SingleBlockTx(TxMgr* tx_mgr, const void* buf,
                                    size_t count, size_t offset)
    : CoWTx(tx_mgr, buf, count, offset),
      local_offset(offset - begin_vidx * BLOCK_SIZE) {
  assert(num_blocks == 1);
  copy_last = false;
}

void TxMgr::SingleBlockTx::do_write() {
  // must acquire the tx tail before any get
  tx_mgr->blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);

  // src block is the block to be copied over
  // we only use first_* but not last_* because they are the same one
  first_src_block = tx_mgr->vidx_to_addr(begin_vidx);

redo:
  // copy data from the source block if src_block exists
  if (first_src_block)
    memcpy(dst_blocks->data(), first_src_block->data(), BLOCK_SIZE);

  // copy data from buf
  memcpy(dst_blocks->data() + local_offset, buf, count);

  // persist the data
  persist_fenced(dst_blocks, BLOCK_SIZE);

retry:
  // try to commit the tx entry
  auto conflict_entry = tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) return;  // success, no conflict

  assert(copy_first);
  if (handle_conflict(conflict_entry, begin_vidx))
    goto redo;
  else
    goto retry;
}

/*
 * MultiBlockTx
 */

TxMgr::MultiBlockTx::MultiBlockTx(TxMgr* tx_mgr, const void* buf, size_t count,
                                  size_t offset)
    : CoWTx(tx_mgr, buf, count, offset),
      first_block_local_offset(ALIGN_UP(offset, BLOCK_SIZE) - offset),
      last_block_local_offset(end_offset - ALIGN_DOWN(end_offset, BLOCK_SIZE)) {
}

void TxMgr::MultiBlockTx::do_write() {
  // copy full blocks first
  if (num_full_blocks > 0) {
    pmem::Block* full_blocks = dst_blocks + (begin_full_vidx - begin_vidx);
    const size_t num_bytes = num_full_blocks << BLOCK_SHIFT;
    memcpy(full_blocks->data(), buf + first_block_local_offset, num_bytes);
    persist_unfenced(full_blocks, num_bytes);
  }

  // only get a snapshot of the tail when starting critical piece
  tx_mgr->blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);

  if (copy_first) {
    assert(begin_full_vidx - begin_vidx == 1);
    first_src_block = tx_mgr->vidx_to_addr(begin_vidx);
  }
  if (copy_last) {
    assert(end_vidx - end_full_vidx == 1);
    last_src_block = tx_mgr->vidx_to_addr(end_full_vidx);
  }

redo:
  // copy first block
  if (copy_first) {
    // copy the data from the source block if exits
    if (first_src_block)
      memcpy(dst_blocks->data(), first_src_block, BLOCK_SIZE);

    // write data from the buf to the first block
    char* dst = dst_blocks->data() + BLOCK_SIZE - first_block_local_offset;
    memcpy(dst, buf, first_block_local_offset);

    persist_unfenced(dst_blocks, BLOCK_SIZE);
  }

  // copy last block
  if (copy_last) {
    pmem::Block* last_dst_block = dst_blocks + (end_full_vidx - begin_vidx);

    // copy the data from the source block if exits
    if (last_src_block)
      memcpy(last_dst_block->data(), last_src_block, BLOCK_SIZE);

    // write data from the buf to the last block
    const char* src = buf + (count - last_block_local_offset);
    memcpy(last_dst_block->data(), src, last_block_local_offset);

    persist_unfenced(last_dst_block, BLOCK_SIZE);
  }
  _mm_sfence();

retry:
  // try to commit the transaction
  auto conflict_entry = tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) return;  // success

  // recalculate copy_first/last to indicate what we care about
  copy_first = begin_full_vidx != begin_vidx;
  copy_last = end_full_vidx != end_vidx;

  // handle_conflict will update copy_first/last according to indicate whether
  // redo is needed
  if (handle_conflict(conflict_entry, begin_vidx, end_full_vidx))
    goto redo;  // conflict is detected, redo copy
  else
    goto retry;  // we have moved to the new tail, retry commit
}

}  // namespace ulayfs::dram
