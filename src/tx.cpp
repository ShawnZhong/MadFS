#include "tx.h"

#include <cstddef>
#include <cstring>
#include <vector>

#include "block.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "params.h"
#include "utils.h"

namespace ulayfs::dram {

/*
 * TxMgr
 */

ssize_t TxMgr::do_read(char* buf, size_t count, size_t offset) {
  return ReadTx(file, buf, count, offset).do_read();
}

// TODO: maybe reclaim the old blocks right after commit?
void TxMgr::do_write(const char* buf, size_t count, size_t offset) {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    AlignedTx(file, buf, count, offset).do_write();
    return;
  }

  // another special case where range is within a single block
  if ((offset >> BLOCK_SHIFT) == ((offset + count - 1) >> BLOCK_SHIFT)) {
    SingleBlockTx(file, buf, count, offset).do_write();
    return;
  }

  // unaligned multi-block write
  MultiBlockTx(file, buf, count, offset).do_write();
}

bool TxMgr::tx_idx_greater(TxEntryIdx lhs, TxEntryIdx rhs) {
  if (lhs.block_idx == rhs.block_idx) return lhs.local_idx > rhs.local_idx;
  if (lhs.block_idx == 0) return false;
  if (rhs.block_idx == 0) return true;
  return file->lidx_to_addr_ro(lhs.block_idx)->tx_block.get_tx_seq() >
         file->lidx_to_addr_ro(rhs.block_idx)->tx_block.get_tx_seq();
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
    if (pmem::TxEntry::need_flush(curr_idx.local_idx)) {
      flush_tx_entries(meta->get_tx_tail(), curr_idx, curr_block);
      meta->set_tx_tail(curr_idx);
    }
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

bool TxMgr::handle_idx_overflow(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                                bool do_alloc) const {
  const bool is_inline = tx_idx.is_inline();
  uint16_t capacity = is_inline ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
  if (unlikely(tx_idx.local_idx >= capacity)) {
    LogicalBlockIdx block_idx =
        is_inline ? meta->get_next_tx_block() : tx_block->get_next_tx_block();
    if (block_idx == 0) {
      if (!do_alloc) return false;
      block_idx =
          is_inline ? alloc_next_block(meta) : alloc_next_block(tx_block);
    }
    tx_idx.block_idx = block_idx;
    tx_idx.local_idx -= capacity;
    tx_block = &file->lidx_to_addr_rw(tx_idx.block_idx)->tx_block;
  }
  return true;
}

void TxMgr::flush_tx_entries(TxEntryIdx tx_idx_begin, TxEntryIdx tx_idx_end,
                             pmem::TxBlock* tx_block_end) {
  if (!tx_idx_greater(tx_idx_end, tx_idx_begin)) return;
  pmem::TxBlock* tx_block_begin;
  // handle special case of inline tx
  if (tx_idx_begin.block_idx == 0) {
    if (tx_idx_end.block_idx == 0) {
      meta->flush_tx_entries(tx_idx_begin.local_idx, tx_idx_end.local_idx);
      goto done;
    }
    meta->flush_tx_block(tx_idx_begin.local_idx);
    // now the next block is the "new begin"
    tx_idx_begin = {meta->get_next_tx_block(), 0};
  }
  while (tx_idx_begin.block_idx != tx_idx_end.block_idx) {
    tx_block_begin = &file->lidx_to_addr_rw(tx_idx_begin.block_idx)->tx_block;
    tx_block_begin->flush_tx_block(tx_idx_begin.local_idx);
    tx_idx_begin = {tx_block_begin->get_next_tx_block(), 0};
    // special case: tx_idx_end is the first entry of the next block, which
    // means we only need to flush the current block and no need to
    // dereference to get the last block
  }
  if (tx_idx_begin.local_idx == tx_idx_end.local_idx) goto done;
  if (!tx_block_end)
    tx_block_end = &file->lidx_to_addr_rw(tx_idx_end.block_idx)->tx_block;
  tx_block_end->flush_tx_entries(tx_idx_begin.local_idx, tx_idx_end.local_idx);

done:
  _mm_sfence();
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
    curr_block = &file->lidx_to_addr_rw(curr_idx.block_idx)->tx_block;
  }

  if (!(next_block_idx = curr_block->get_next_tx_block())) {
    curr_idx.local_idx = curr_block->find_tail(curr_idx.local_idx);
    if (curr_idx.local_idx < NUM_TX_ENTRY) goto done;
  }

retry:
  do {
    curr_idx.block_idx = next_block_idx;
    curr_block = &(file->lidx_to_addr_rw(next_block_idx)->tx_block);
  } while ((next_block_idx = curr_block->get_next_tx_block()));

  curr_idx.local_idx = curr_block->find_tail();
  if (curr_idx.local_idx < NUM_TX_ENTRY) goto done;

  next_block_idx = alloc_next_block(curr_block);
  goto retry;

done:
  tx_idx = curr_idx;
  tx_block = curr_block;
};

template <class B>
LogicalBlockIdx TxMgr::alloc_next_block(B* block) const {
  // allocate the next block
  LogicalBlockIdx new_block_idx = file->get_local_allocator()->alloc(1);
  pmem::Block* new_block = file->lidx_to_addr_rw(new_block_idx);
  memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
  new_block->tx_block.set_tx_seq(block->get_tx_seq() + 1);
  pmem::persist_cl_unfenced(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1]);
  new_block->zero_init(0, NUM_CL_PER_BLOCK - 1);
  bool success = block->set_next_tx_block(new_block_idx);
  if (success) {
    return new_block_idx;
  } else {
    // there is a race condition for adding the new blocks
    file->get_local_allocator()->free(new_block_idx, 1);
    return block->get_next_tx_block();
  }
}

// explicit template instantiations
template LogicalBlockIdx TxMgr::alloc_next_block(pmem::MetaBlock* block) const;
template LogicalBlockIdx TxMgr::alloc_next_block(pmem::TxBlock* block) const;

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << "Transactions: \n";

  auto log_mgr = tx_mgr.file->get_local_log_mgr();

  TxEntryIdx tx_idx = {0, 0};
  pmem::TxBlock* tx_block = nullptr;

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(tx_idx, tx_block);
    if (!tx_entry.is_valid()) break;

    // print tx entry
    out << "\t" << tx_idx << " -> " << tx_entry << "\n";

    // print log entries if the tx is not inlined
    if (!tx_entry.is_inline()) {
      auto head_entry_idx = tx_entry.commit_entry.log_entry_idx;
      auto head_entry = log_mgr->get_head_entry(head_entry_idx);
      out << "\t\t" << *head_entry << "\n";

      // print log coverage
      uint32_t num_blocks;
      VirtualBlockIdx begin_virtual_idx;
      std::vector<LogicalBlockIdx> begin_logical_idxs;
      log_mgr->get_coverage(head_entry_idx, begin_virtual_idx, num_blocks,
                            &begin_logical_idxs);
      out << "\t\tLogCoverage{";
      out << "n_blk=" << num_blocks << ", ";
      out << "vidx=" << begin_virtual_idx << ", ";
      out << "begin_lidxs=[";
      for (const auto& idx : begin_logical_idxs) out << idx << ", ";
      out << "]}\n";
    }

    if (!tx_mgr.advance_tx_idx(tx_idx, tx_block, /*do_alloc*/ false)) break;
  }

  return out;
}

/*
 * Tx
 */

TxMgr::Tx::Tx(File* file, size_t count, size_t offset)
    : file(file),
      tx_mgr(&file->tx_mgr),

      // input properties
      count(count),
      offset(offset),

      // derived properties
      end_offset(offset + count),
      begin_vidx(offset >> BLOCK_SHIFT),
      end_vidx(ALIGN_UP(end_offset, BLOCK_SIZE) >> BLOCK_SHIFT),
      num_blocks(end_vidx - begin_vidx) {}

bool TxMgr::Tx::handle_conflict(pmem::TxEntry curr_entry,
                                VirtualBlockIdx first_vidx,
                                VirtualBlockIdx last_vidx,
                                LogicalBlockIdx conflict_image[]) {
  // `le` prefix stands for "log entry", meaning read from log entry
  VirtualBlockIdx le_first_vidx, le_last_vidx;
  LogicalBlockIdx le_begin_lidx;
  VirtualBlockIdx overlap_first_vidx, overlap_last_vidx;
  uint32_t num_blocks;
  bool has_conflict = false;
  auto log_mgr = file->get_local_log_mgr();

  do {
    // TODO: implement the case where num_blocks is over 64 and there
    //       are multiple begin_logical_idxs
    // TODO: handle writev requests
    if (curr_entry.is_inline()) {  // inline tx entry
      num_blocks = curr_entry.commit_inline_entry.num_blocks;
      le_first_vidx = curr_entry.commit_inline_entry.begin_virtual_idx;
      le_begin_lidx = curr_entry.commit_inline_entry.begin_logical_idx;
      le_last_vidx = le_first_vidx + num_blocks - 1;

      if (last_vidx < le_first_vidx || first_vidx > le_last_vidx) goto next;
      has_conflict = true;
      overlap_first_vidx =
          le_first_vidx > first_vidx ? le_first_vidx : first_vidx;
      overlap_last_vidx = le_last_vidx < last_vidx ? le_last_vidx : last_vidx;

      for (VirtualBlockIdx vidx = overlap_first_vidx; vidx <= overlap_last_vidx;
           ++vidx) {
        auto offset = vidx - first_vidx;
        conflict_image[offset] = le_begin_lidx + offset;
      }
    } else {  // non-inline tx entry
      le_begin_lidx = 0;
      num_blocks = curr_entry.commit_entry.num_blocks;
      if (num_blocks) {  // some info in log entries is partially inline
        le_first_vidx = curr_entry.commit_entry.begin_virtual_idx;
      } else {  // dereference log_entry_idx
        log_mgr->get_coverage(curr_entry.commit_entry.log_entry_idx,
                              le_first_vidx, num_blocks);
      }
      le_last_vidx = le_first_vidx + num_blocks - 1;
      if (last_vidx < le_first_vidx || first_vidx > le_last_vidx) goto next;

      has_conflict = true;
      overlap_first_vidx =
          le_first_vidx > first_vidx ? le_first_vidx : first_vidx;
      overlap_last_vidx = le_last_vidx < last_vidx ? le_last_vidx : last_vidx;

      if (le_begin_lidx == 0) {  // lazy dereference log idx
        std::vector<LogicalBlockIdx> le_begin_lidxs;
        log_mgr->get_coverage(curr_entry.commit_entry.log_entry_idx,
                              le_first_vidx, num_blocks, &le_begin_lidxs);
        le_begin_lidx = le_begin_lidxs.front();
      }

      for (VirtualBlockIdx vidx = overlap_first_vidx; vidx <= overlap_last_vidx;
           ++vidx) {
        auto offset = vidx - first_vidx;
        conflict_image[offset] = le_begin_lidx + offset;
      }
    }
  next:
    if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, /*do_alloc*/ false))
      break;
    curr_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
  } while (curr_entry.is_valid());

  return has_conflict;
}

// TODO: handle read reaching EOF
// TODO: more fancy handle_conflict strategy
ssize_t TxMgr::ReadTx::do_read() {
  size_t first_block_local_offset = offset & (BLOCK_SIZE - 1);
  size_t first_block_size = BLOCK_SIZE - first_block_local_offset;
  if (first_block_size > count) first_block_size = count;

  VirtualBlockIdx curr_vidx;
  const pmem::Block* curr_block;
  pmem::TxEntry curr_entry;
  size_t buf_offset;

  LogicalBlockIdx redo_image[num_blocks];
  memset(redo_image, 0, sizeof(LogicalBlockIdx) * num_blocks);

  file->blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ false);

  // first handle the first block (which might not be full block)
  curr_block = file->vidx_to_addr_ro(begin_vidx);
  memcpy(buf, curr_block->data_ro() + first_block_local_offset,
         first_block_size);
  buf_offset = first_block_size;

  // then handle middle full blocks (which might not exist)
  for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
    curr_block = file->vidx_to_addr_ro(curr_vidx);
    memcpy(buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
    buf_offset += BLOCK_SIZE;
  }

  // if we have multiple blocks to read
  if (begin_vidx != end_vidx - 1) {
    assert(curr_vidx == end_vidx - 1);
    curr_block = file->vidx_to_addr_ro(curr_vidx);
    memcpy(buf + buf_offset, curr_block->data_ro(), count - buf_offset);
  }

  while (true) {
    // check the tail is still tail
    if (!tx_mgr->handle_idx_overflow(tail_tx_idx, tail_tx_block, false)) break;
    curr_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
    if (!curr_entry.is_valid()) break;

    // then scan the log and build redo_image; if no redo needed, we are done
    if (!handle_conflict(curr_entry, begin_vidx, end_vidx - 1, redo_image))
      break;

    // redo:
    LogicalBlockIdx redo_lidx;

    // first handle the first block (which might not be full block)
    redo_lidx = redo_image[0];
    if (redo_lidx) {
      curr_block = file->lidx_to_addr_ro(redo_lidx);
      memcpy(buf, curr_block->data_ro() + first_block_local_offset,
             first_block_size);
      redo_image[0] = 0;
    }
    buf_offset = first_block_size;

    // then handle middle full blocks (which might not exist)
    for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
      redo_lidx = redo_image[curr_vidx - begin_vidx];
      if (redo_lidx) {
        curr_block = file->lidx_to_addr_ro(redo_lidx);
        memcpy(buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
        redo_image[curr_vidx - begin_vidx] = 0;
      }
      buf_offset += BLOCK_SIZE;
    }

    // if we have multiple blocks to read
    if (begin_vidx != end_vidx - 1) {
      redo_lidx = redo_image[curr_vidx - begin_vidx];
      if (redo_lidx) {
        curr_block = file->lidx_to_addr_ro(redo_lidx);
        memcpy(buf + buf_offset, curr_block->data_ro(), count - buf_offset);
        redo_image[curr_vidx - begin_vidx] = 0;
      }
    }
  }

  return static_cast<ssize_t>(count);
}

TxMgr::WriteTx::WriteTx(File* file, const char* buf, size_t count,
                        size_t offset)
    : Tx(file, count, offset),
      buf(buf),
      log_mgr(file->get_local_log_mgr()),
      allocator(file->get_local_allocator()),
      dst_lidx(allocator->alloc(num_blocks)),
      dst_blocks(file->lidx_to_addr_rw(dst_lidx)) {
  // TODO: implement the case where num_blocks is over 64 and there
  //       are multiple begin_logical_idxs
  // TODO: handle writev requests
  // for overwrite, "leftover_bytes" is zero; only in append we care
  // append log without fence because we only care flush completion
  // before try_commit
  if (pmem::TxCommitInlineEntry::can_inline(num_blocks, begin_vidx, dst_lidx)) {
    commit_entry = pmem::TxCommitInlineEntry(num_blocks, begin_vidx, dst_lidx);
  } else {
    // it's fine that we append log first as long we don't publish it by tx
    auto log_entry_idx = log_mgr->append(pmem::LogOp::LOG_OVERWRITE,  // op
                                         0,           // leftover_bytes
                                         num_blocks,  // total_blocks
                                         begin_vidx,  // begin_virtual_idx
                                         {dst_lidx},  // begin_logical_idxs
                                         false        // fenced
    );

    commit_entry = pmem::TxCommitEntry(num_blocks, begin_vidx, log_entry_idx);
  }
}

/*
 * AlignedTx
 */
void TxMgr::AlignedTx::do_write() {
  pmem::TxEntry conflict_entry;
  LogicalBlockIdx recycle_image[num_blocks];

  // since everything is block-aligned, we can copy data directly
  memcpy(dst_blocks->data_rw(), buf, count);
  persist_fenced(dst_blocks, count);

  // make a local copy of the tx tail
  file->blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  for (uint32_t i = 0; i < num_blocks; ++i)
    recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);

retry:
  conflict_entry = tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) goto done;
  // we don't check the return value of handle_conflict here because we don't
  // care whether there is a conflict, as long as recycle_image gets updated
  handle_conflict(conflict_entry, begin_vidx, end_vidx - 1, recycle_image);
  goto retry;

done:
  // recycle the data blocks being overwritten
  allocator->free(recycle_image, num_blocks);
}

void TxMgr::SingleBlockTx::do_write() {
  pmem::TxEntry conflict_entry;
  LogicalBlockIdx recycle_image[1];

  // must acquire the tx tail before any get
  file->blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  recycle_image[0] = file->vidx_to_lidx(begin_vidx);
  assert(recycle_image[0] != dst_lidx);

redo:
  // copy original data
  const pmem::Block* src_block = file->lidx_to_addr_ro(recycle_image[0]);
  memcpy(dst_blocks->data_rw(), src_block->data_ro(), BLOCK_SIZE);
  // copy data from buf
  memcpy(dst_blocks->data_rw() + local_offset, buf, count);

  // persist the data
  persist_fenced(dst_blocks, BLOCK_SIZE);

retry:
  // try to commit the tx entry
  conflict_entry = tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) goto done;  // success, no conflict

  // we just treat begin_vidx as both first and last vidx
  if (!handle_conflict(conflict_entry, begin_vidx, begin_vidx, recycle_image))
    goto retry;
  else
    goto redo;

done:
  allocator->free(recycle_image[0]);
}

/*
 * MultiBlockTx
 */

void TxMgr::MultiBlockTx::do_write() {
  // if need_copy_first/last is false, this means it is handled by the full
  // block copy and never need redo
  const bool need_copy_first = begin_full_vidx != begin_vidx;
  const bool need_copy_last = end_full_vidx != end_vidx;
  // do_copy_first/last indicates do we actually need to do copy; in the case of
  // redo, we may skip if no change is maded
  bool do_copy_first = true;
  bool do_copy_last = true;
  pmem::TxEntry conflict_entry;
  LogicalBlockIdx src_first_lidx, src_last_lidx;
  LogicalBlockIdx recycle_image[num_blocks];

  // copy full blocks first
  if (num_full_blocks > 0) {
    pmem::Block* full_blocks = dst_blocks + (begin_full_vidx - begin_vidx);
    const size_t num_bytes = num_full_blocks << BLOCK_SHIFT;
    memcpy(full_blocks->data_rw(), buf + first_block_local_offset, num_bytes);
    persist_unfenced(full_blocks, num_bytes);
  }

  // only get a snapshot of the tail when starting critical piece
  file->blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  for (uint32_t i = 0; i < num_blocks; ++i)
    recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);
  src_first_lidx = recycle_image[0];
  src_last_lidx = recycle_image[num_blocks - 1];

redo:
  // copy first block
  if (need_copy_first && do_copy_first) {
    // copy the data from the first source block if exists
    const char* src = file->lidx_to_addr_ro(src_first_lidx)->data_ro();
    memcpy(dst_blocks->data_rw(), src, BLOCK_SIZE);

    // write data from the buf to the first block
    char* dst = dst_blocks->data_rw() + BLOCK_SIZE - first_block_local_offset;
    memcpy(dst, buf, first_block_local_offset);

    persist_unfenced(dst_blocks, BLOCK_SIZE);
  }

  // copy last block
  if (need_copy_last && do_copy_last) {
    pmem::Block* last_dst_block = dst_blocks + (end_full_vidx - begin_vidx);

    // copy the data from the last source block if exits
    const char* block_src = file->lidx_to_addr_ro(src_last_lidx)->data_ro();
    memcpy(last_dst_block->data_rw(), block_src, BLOCK_SIZE);

    // write data from the buf to the last block
    const char* buf_src = buf + (count - last_block_local_offset);
    memcpy(last_dst_block->data_rw(), buf_src, last_block_local_offset);

    persist_unfenced(last_dst_block, BLOCK_SIZE);
  }
  _mm_sfence();

retry:
  // try to commit the transaction
  conflict_entry = tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) goto done;  // success
  // make a copy of the first and last again
  src_first_lidx = recycle_image[0];
  src_last_lidx = recycle_image[num_blocks - 1];
  if (!handle_conflict(conflict_entry, begin_vidx, end_full_vidx,
                       recycle_image))
    goto retry;  // we have moved to the new tail, retry commit
  else {
    do_copy_first = src_first_lidx != recycle_image[0];
    do_copy_last = src_last_lidx != recycle_image[num_blocks - 1];
    if (do_copy_first || do_copy_last)
      goto redo;
    else
      goto retry;
  }

done:
  // recycle the data blocks being overwritten
  allocator->free(recycle_image, num_blocks);
}

}  // namespace ulayfs::dram
