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

  if (!(next_block_idx = curr_block->get_next_tx_block())) {
    curr_idx.local_idx = curr_block->find_tail(curr_idx.local_idx);
    if (curr_idx.local_idx < NUM_TX_ENTRY) goto done;
  }

retry:
  do {
    curr_idx.block_idx = next_block_idx;
    curr_block = &(mem_table->get(next_block_idx)->tx_block);
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
  pmem::Block* new_block = mem_table->get(new_block_idx);
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

    // print log head
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

      has_conflict = true;
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
    } else {
      // FIXME: there should not be any other one
      assert(0);
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
      dst_idx(allocator->alloc(num_blocks)),
      dst_blocks(file->lidx_to_addr_rw(dst_idx)) {
  // TODO: implement the case where num_blocks is over 64 and there
  //       are multiple begin_logical_idxs
  // TODO: handle writev requests
  // for overwrite, "leftover_bytes" is zero; only in append we care
  // append log without fence because we only care flush completion
  // before try_commit
  this->log_idx = log_mgr->append(pmem::LogOp::LOG_OVERWRITE,  // op
                                  0,                           // leftover_bytes
                                  num_blocks,                  // total_blocks
                                  begin_vidx,  // begin_virtual_idx
                                  {dst_idx},   // begin_logical_idxs
                                  false        // fenced
  );
  // it's fine that we append log first as long we don't publish it by tx
  this->commit_entry = pmem::TxCommitEntry(num_blocks, begin_vidx, log_idx);
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

  while (true) {
    conflict_entry =
        tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
    if (!conflict_entry.is_valid()) break;
    // we don't check the return value of handle_conflict here because we don't
    // care whether there is a conflict, as long as recycle_image gets updated
    handle_conflict(conflict_entry, begin_vidx, end_vidx - 1, recycle_image);
  }

  // recycle the data blocks being overwritten
  allocator->free(recycle_image, num_blocks);
}

void TxMgr::SingleBlockTx::do_write() {
  pmem::TxEntry conflict_entry;
  LogicalBlockIdx recycle_image[1];

  // must acquire the tx tail before any get
  file->blk_table.update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  recycle_image[0] = file->vidx_to_lidx(begin_vidx);

redo:
  // copy original data
  memcpy(dst_blocks->data_rw(),
         file->lidx_to_addr_ro(recycle_image[0])->data_ro(), BLOCK_SIZE);
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
  allocator->free(recycle_image[0], 1);
}

/*
 * MultiBlockTx
 */

void TxMgr::MultiBlockTx::do_write() {
  pmem::TxEntry conflict_entry;
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
  LogicalBlockIdx src_first_lidx = recycle_image[0];
  LogicalBlockIdx src_last_lidx = recycle_image[num_blocks - 1];

redo:
  // copy first block
  if (src_first_lidx != recycle_image[0]) {
    src_first_lidx = recycle_image[0];
    memcpy(dst_blocks->data_rw(),
           file->lidx_to_addr_ro(src_first_lidx)->data_ro(), BLOCK_SIZE);

    // write data from the buf to the first block
    char* dst = dst_blocks->data_rw() + BLOCK_SIZE - first_block_local_offset;
    memcpy(dst, buf, first_block_local_offset);

    persist_unfenced(dst_blocks, BLOCK_SIZE);
  }

  // copy last block
  if (src_last_lidx != recycle_image[num_blocks - 1]) {
    src_last_lidx = recycle_image[num_blocks - 1];
    pmem::Block* last_dst_block = dst_blocks + (end_full_vidx - begin_vidx);

    // copy the data from the source block if exits
    memcpy(last_dst_block->data_rw(),
           file->lidx_to_addr_ro(src_last_lidx)->data_ro(), BLOCK_SIZE);

    // write data from the buf to the last block
    const char* src = buf + (count - last_block_local_offset);
    memcpy(last_dst_block->data_rw(), src, last_block_local_offset);

    persist_unfenced(last_dst_block, BLOCK_SIZE);
  }
  _mm_sfence();

retry:
  // try to commit the transaction
  conflict_entry = tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) goto done;  // success

  if (!handle_conflict(conflict_entry, begin_vidx, end_full_vidx,
                       recycle_image))
    goto retry;  // we have moved to the new tail, retry commit
  else {
    // we don't redo if the conflict happens in the middle
    if (src_first_lidx == recycle_image[0] &&
        src_last_lidx == recycle_image[num_blocks - 1])
      goto retry;
    goto redo;  // conflict is detected, redo copy
  }

done:
  // recycle the data blocks being overwritten
  allocator->free(recycle_image, num_blocks);
}

}  // namespace ulayfs::dram
