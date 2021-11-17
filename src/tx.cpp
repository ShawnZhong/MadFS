#include "tx.h"

#include <cstddef>
#include <cstring>
#include <vector>

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

// TODO: maybe reclaim the old blocks right after commit?
void TxMgr::do_write(const char* buf, size_t count, size_t offset) {
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

// TODO: handle read reaching EOF
// TODO: more fancy handle_conflict strategy
ssize_t TxMgr::do_read(char* buf, size_t count, size_t offset) {
  size_t end_offset = offset + count;
  VirtualBlockIdx begin_vidx = offset >> BLOCK_SHIFT;
  VirtualBlockIdx end_vidx = ALIGN_UP(end_offset, BLOCK_SIZE) >> BLOCK_SHIFT;
  size_t first_block_local_offset = offset & (BLOCK_SIZE - 1);
  size_t first_block_size = BLOCK_SIZE - first_block_local_offset;
  if (first_block_size > count) first_block_size = count;

  VirtualBlockIdx curr_vidx;
  const pmem::Block* curr_block;
  pmem::TxEntry curr_entry;
  size_t buf_offset;

  std::vector<LogicalBlockIdx> redo_image(end_vidx - begin_vidx, 0);

  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);

  // first handle the first block (which might not be full block)
  curr_block = vidx_to_addr_ro(begin_vidx);
  memcpy(buf, curr_block->data_ro() + first_block_local_offset,
         first_block_size);
  buf_offset = first_block_size;

  // then handle middle full blocks (which might not exist)
  for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
    curr_block = vidx_to_addr_ro(curr_vidx);
    memcpy(buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
    buf_offset += BLOCK_SIZE;
  }

  // if we have multiple blocks to read
  if (begin_vidx != end_vidx - 1) {
    assert(curr_vidx == end_vidx - 1);
    curr_block = vidx_to_addr_ro(curr_vidx);
    memcpy(buf + buf_offset, curr_block->data_ro(), count - buf_offset);
  }

  do {
    // check the tail is still tail
    if (!handle_idx_overflow(tail_tx_idx, tail_tx_block, false)) goto done;
    curr_entry = get_entry_from_block(tail_tx_idx, tail_tx_block);
    if (!curr_entry.is_valid()) goto done;

    // then scan the log and build redo_image; if no redo needed, we are done
    if (!handle_conflict(curr_entry, begin_vidx, end_vidx - 1, tail_tx_idx,
                         tail_tx_block, /*is_range*/ true, nullptr, nullptr,
                         nullptr, nullptr, &redo_image))
      goto done;

    // redo:
    LogicalBlockIdx redo_lidx;

    // first handle the first block (which might not be full block)
    redo_lidx = redo_image[0];
    if (redo_lidx) {
      curr_block = mem_table->get(redo_lidx);
      memcpy(buf, curr_block->data_ro() + first_block_local_offset,
             first_block_size);
    }
    buf_offset = first_block_size;

    // then handle middle full blocks (which might not exist)
    for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
      redo_lidx = redo_image[curr_vidx - begin_vidx];
      if (redo_lidx) {
        curr_block = mem_table->get(redo_lidx);
        memcpy(buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
      }
      buf_offset += BLOCK_SIZE;
    }

    // if we have multiple blocks to read
    if (begin_vidx != end_vidx - 1) {
      redo_lidx = redo_image[curr_vidx - begin_vidx];
      if (redo_lidx) {
        curr_block = mem_table->get(redo_lidx);
        memcpy(buf + buf_offset, curr_block->data_ro(), count - buf_offset);
      }
    }

    // reset redo_image
    std::fill(redo_image.begin(), redo_image.end(), 0);
  } while (true);

done:
  return count;
}

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

pmem::Block* TxMgr::vidx_to_addr_rw(VirtualBlockIdx vidx) const {
  return tables_vidx_to_addr(this->mem_table, this->blk_table, vidx);
}

const pmem::Block* TxMgr::vidx_to_addr_ro(VirtualBlockIdx vidx) const {
  static const char empty_block[BLOCK_SIZE]{};

  LogicalBlockIdx lidx = this->blk_table->get(vidx);
  if (lidx == 0) return reinterpret_cast<const pmem::Block*>(&empty_block);
  return this->mem_table->get(lidx);
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

bool TxMgr::handle_conflict(pmem::TxEntry curr_entry,
                            VirtualBlockIdx first_vidx,
                            VirtualBlockIdx last_vidx, TxEntryIdx& tail_tx_idx,
                            pmem::TxBlock*& tail_tx_block, bool is_range,
                            bool* redo_first, bool* redo_last,
                            LogicalBlockIdx* first_lidx,
                            LogicalBlockIdx* last_lidx,
                            std::vector<LogicalBlockIdx>* redo_image) {
  // `le` prefix stands for "log entry", meaning read from log entry
  VirtualBlockIdx le_first_vidx, le_last_vidx;
  LogicalBlockIdx le_begin_lidx;
  VirtualBlockIdx overlap_first_vidx, overlap_last_vidx;
  uint32_t num_blocks;
  bool need_redo = false;

  do {
    // TODO: implement the case where num_blocks is over 64 and there
    //       are multiple begin_logical_idxs
    // TODO: handle writev requests
    if (curr_entry.is_commit()) {
      le_begin_lidx = 0;
      num_blocks = curr_entry.commit_entry.num_blocks;
      if (num_blocks) {  // inline tx
        le_first_vidx = curr_entry.commit_entry.begin_virtual_idx;
      } else {  // dereference log_entry_idx
        log_mgr->get_coverage(curr_entry.commit_entry.log_entry_idx,
                              le_first_vidx, num_blocks);
      }
      le_last_vidx = le_first_vidx + num_blocks - 1;
      if (last_vidx < le_first_vidx || first_vidx > le_last_vidx) goto next;

      overlap_first_vidx =
          le_first_vidx > first_vidx ? le_first_vidx : first_vidx;
      overlap_last_vidx = le_last_vidx < last_vidx ? le_last_vidx : last_vidx;

      need_redo = true;
      if (le_begin_lidx == 0) {  // lazy dereference log idx
        std::vector<LogicalBlockIdx> le_begin_lidxs;
        log_mgr->get_coverage(curr_entry.commit_entry.log_entry_idx,
                              le_first_vidx, num_blocks, &le_begin_lidxs);
        le_begin_lidx = le_begin_lidxs.front();
      }

      if (is_range) {
        for (VirtualBlockIdx vidx = overlap_first_vidx;
             vidx <= overlap_last_vidx; ++vidx) {
          auto offset = vidx - first_vidx;
          (*redo_image)[offset] = le_begin_lidx + offset;
        }
      } else {
        if (overlap_first_vidx == first_vidx) {
          *redo_first = true;
          *first_lidx = first_vidx + le_begin_lidx - le_first_vidx;
        }
        if (overlap_last_vidx == last_vidx) {
          *redo_last = true;
          *last_lidx = last_vidx + le_begin_lidx - le_first_vidx;
        }
      }
    } else {
      // FIXME: there should not be any other one
      assert(0);
    }
  next:
    if (!advance_tx_idx(tail_tx_idx, tail_tx_block, /*do_alloc*/ false)) break;
    curr_entry = get_entry_from_block(tail_tx_idx, tail_tx_block);
  } while (curr_entry.is_valid());

  return need_redo;
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
        << *(tx_mgr.log_mgr->get_head_entry(commit_entry.log_entry_idx))
        << "\n";
    if (!tx_mgr.advance_tx_idx(tx_idx, tx_block, /*do_alloc*/ false)) break;
  }

  return out;
}

/*
 * Tx
 */

TxMgr::Tx::Tx(TxMgr* tx_mgr, const char* buf, size_t count, size_t offset)
    : tx_mgr(tx_mgr),

      // input properties
      buf(buf),
      count(count),
      offset(offset),

      // derived properties
      end_offset(offset + count),
      begin_vidx(offset >> BLOCK_SHIFT),
      end_vidx(ALIGN_UP(end_offset, BLOCK_SIZE) >> BLOCK_SHIFT),
      num_blocks(end_vidx - begin_vidx),

      dst_idx(tx_mgr->allocator->alloc(num_blocks)),
      dst_blocks(tx_mgr->mem_table->get(dst_idx)) {
  // TODO: implement the case where num_blocks is over 64 and there
  //       are multiple begin_logical_idxs
  // TODO: handle writev requests
  // for overwrite, "leftover_bytes" is zero; only in append we care
  // append log without fence because we only care flush completion
  // before try_commit
  this->log_idx = tx_mgr->log_mgr->append(pmem::LogOp::LOG_OVERWRITE,  // op
                                         0,           // leftover_bytes
                                         num_blocks,  // total_blocks
                                         begin_vidx,  // begin_virtual_idx
                                         {dst_idx},   // begin_logical_idxs
                                         false        // fenced
  );
  // it's fine that we append log first as long we don't publish it by tx
}

/*
 * AlignedTx
 */

TxMgr::AlignedTx::AlignedTx(TxMgr* tx_mgr, const char* buf, size_t count,
                            size_t offset)
    : Tx(tx_mgr, buf, count, offset) {}

void TxMgr::AlignedTx::do_write() {
  // since everything is block-aligned, we can copy data directly
  memcpy(dst_blocks->data(), buf, count);
  persist_fenced(dst_blocks, count);

  // make a local copy of the tx tail
  tx_mgr->blk_table->get_tail_tx(tail_tx_idx, tail_tx_block);
  // commit the transaction, it's fine if the tx fails due to race condition
  pmem::TxCommitEntry entry(num_blocks, begin_vidx, log_idx);
  tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block, /*cont_if_fail*/ true);
}

/*
 * CoWTx
 */

TxMgr::CoWTx::CoWTx(TxMgr* tx_mgr, const char* buf, size_t count, size_t offset)
    : Tx(tx_mgr, buf, count, offset),
      entry(pmem::TxCommitEntry(num_blocks, begin_vidx, log_idx)),
      begin_full_vidx(ALIGN_UP(offset, BLOCK_SIZE) >> BLOCK_SHIFT),
      end_full_vidx(end_offset >> BLOCK_SHIFT),
      num_full_blocks(end_full_vidx - begin_full_vidx),
      copy_first(begin_full_vidx != begin_vidx),
      copy_last(end_full_vidx != end_vidx),
      src_first_lidx(0),
      src_last_lidx(0) {}

/*
 * SingleBlockTx
 */

TxMgr::SingleBlockTx::SingleBlockTx(TxMgr* tx_mgr, const char* buf,
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
  src_first_lidx = tx_mgr->blk_table->get(begin_vidx);

redo:
  // copy original data
  if (src_first_lidx)  // TODO: handle src_first_lidx == 0
    memcpy(dst_blocks->data(),
           tx_mgr->mem_table->get(src_first_lidx)->data_ro(), BLOCK_SIZE);
  // copy data from buf
  memcpy(dst_blocks->data() + local_offset, buf, count);

  // persist the data
  persist_fenced(dst_blocks, BLOCK_SIZE);

retry:
  // try to commit the tx entry
  auto conflict_entry = tx_mgr->try_commit(entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) return;  // success, no conflict

  // we just treat begin_vidx as both first and last vidx
  if (tx_mgr->handle_conflict(conflict_entry, begin_vidx, begin_vidx,
                              tail_tx_idx, tail_tx_block, /*is_range*/ false,
                              &copy_first, &copy_last, &src_first_lidx,
                              &src_last_lidx, nullptr))
    goto redo;
  else
    goto retry;
}

/*
 * MultiBlockTx
 */

TxMgr::MultiBlockTx::MultiBlockTx(TxMgr* tx_mgr, const char* buf, size_t count,
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
    src_first_lidx = tx_mgr->blk_table->get(begin_vidx);
  }
  if (copy_last) {
    assert(end_vidx - end_full_vidx == 1);
    src_last_lidx = tx_mgr->blk_table->get(end_full_vidx);
  }

redo:
  // copy first block
  if (copy_first) {
    // copy the data from the source block if exits
    if (src_first_lidx)  // TODO: handle src_first_lidx == 0
      memcpy(dst_blocks->data(),
             tx_mgr->mem_table->get(src_first_lidx)->data_ro(), BLOCK_SIZE);

    // write data from the buf to the first block
    char* dst = dst_blocks->data() + BLOCK_SIZE - first_block_local_offset;
    memcpy(dst, buf, first_block_local_offset);

    persist_unfenced(dst_blocks, BLOCK_SIZE);
  }

  // copy last block
  if (copy_last) {
    pmem::Block* last_dst_block = dst_blocks + (end_full_vidx - begin_vidx);

    // copy the data from the source block if exits
    if (src_last_lidx)  // TODO: handle src_last_lidx == 0
      memcpy(last_dst_block->data(),
             tx_mgr->mem_table->get(src_last_lidx)->data_ro(), BLOCK_SIZE);

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
  if (tx_mgr->handle_conflict(conflict_entry, begin_vidx, end_full_vidx,
                              tail_tx_idx, tail_tx_block, /*is_range*/ false,
                              &copy_first, &copy_last, &src_first_lidx,
                              &src_last_lidx, nullptr))
    goto redo;  // conflict is detected, redo copy
  else
    goto retry;  // we have moved to the new tail, retry commit
}

}  // namespace ulayfs::dram
