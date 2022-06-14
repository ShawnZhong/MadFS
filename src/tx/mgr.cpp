#include "mgr.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <vector>

#include "alloc.h"
#include "block.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "log.h"
#include "persist.h"
#include "tx/aligned.h"
#include "tx/cow.h"
#include "tx/read.h"
#include "utils.h"

namespace ulayfs::dram {

ssize_t TxMgr::do_pread(char* buf, size_t count, size_t offset) {
  return ReadTx(file, this, buf, count, offset).do_read();
}

ssize_t TxMgr::do_read(char* buf, size_t count) {
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  uint64_t file_size;
  uint64_t ticket;
  uint64_t offset = file->update_with_offset(tail_tx_idx, tail_tx_block, count,
                                             true, ticket, &file_size,
                                             /*do_alloc*/ false);
  ReadTx tx(file, this, buf, count, offset, tail_tx_idx, tail_tx_block,
            file_size, ticket);
  ssize_t ret = tx.do_read();

  file->release_offset(ticket, tx.tail_tx_idx, tx.tail_tx_block);
  return ret;
}

ssize_t TxMgr::do_pwrite(const char* buf, size_t count, size_t offset) {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0)
    return AlignedTx(file, this, buf, count, offset).do_write();

  // another special case where range is within a single block
  if ((BLOCK_SIZE_TO_IDX(offset)) == BLOCK_SIZE_TO_IDX(offset + count - 1))
    return SingleBlockTx(file, this, buf, count, offset).do_write();

  // unaligned multi-block write
  return MultiBlockTx(file, this, buf, count, offset).do_write();
}

ssize_t TxMgr::do_write(const char* buf, size_t count) {
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  uint64_t ticket;
  uint64_t offset = file->update_with_offset(tail_tx_idx, tail_tx_block, count,
                                             false, ticket, nullptr,
                                             /*do_alloc*/ false);

  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0)
    return WriteTx::do_write_and_validate_offset<AlignedTx>(
        file, this, buf, count, offset, tail_tx_idx, tail_tx_block, ticket);

  // another special case where range is within a single block
  if (BLOCK_SIZE_TO_IDX(offset) == BLOCK_SIZE_TO_IDX(offset + count - 1))
    return WriteTx::do_write_and_validate_offset<SingleBlockTx>(
        file, this, buf, count, offset, tail_tx_idx, tail_tx_block, ticket);

  // unaligned multi-block write
  return WriteTx::do_write_and_validate_offset<MultiBlockTx>(
      file, this, buf, count, offset, tail_tx_idx, tail_tx_block, ticket);
}

pmem::TxEntry TxMgr::get_entry_from_block(TxEntryIdx idx,
                                          pmem::TxBlock* tx_block) const {
  assert(idx.local_idx <
         (idx.block_idx == 0 ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY_PER_BLOCK));
  if (idx.block_idx == 0) return meta->get_tx_entry(idx.local_idx);
  return tx_block->get(idx.local_idx);
}

bool TxMgr::advance_tx_idx(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                           bool do_alloc) const {
  assert(tx_idx.local_idx >= 0);
  tx_idx.local_idx++;
  return handle_idx_overflow(tx_idx, tx_block, do_alloc);
}

bool TxMgr::tx_idx_greater(const TxEntryIdx lhs_idx, const TxEntryIdx rhs_idx,
                           const pmem::TxBlock* lhs_block,
                           const pmem::TxBlock* rhs_block) {
  if (lhs_idx.block_idx == rhs_idx.block_idx)
    return lhs_idx.local_idx > rhs_idx.local_idx;
  if (lhs_idx.block_idx == 0) return false;
  if (rhs_idx.block_idx == 0) return true;
  if (!lhs_block)
    lhs_block = &file->lidx_to_addr_ro(lhs_idx.block_idx)->tx_block;
  if (!rhs_block)
    rhs_block = &file->lidx_to_addr_ro(rhs_idx.block_idx)->tx_block;
  return lhs_block->get_tx_seq() > rhs_block->get_tx_seq();
}

pmem::TxEntry TxMgr::try_commit(pmem::TxEntry entry, TxEntryIdx& tx_idx,
                                pmem::TxBlock*& tx_block) {
  handle_idx_overflow(tx_idx, tx_block, true);

  bool is_inline = tx_idx.block_idx == 0;
  assert(is_inline == (tx_block == nullptr));

  if (pmem::TxEntry::need_flush(tx_idx.local_idx)) {
    flush_tx_entries(meta->get_tx_tail(), tx_idx, tx_block);
    meta->set_tx_tail(tx_idx);
  }
  return is_inline ? meta->try_append(entry, tx_idx.local_idx)
                   : tx_block->try_append(entry, tx_idx.local_idx);
}

bool TxMgr::handle_idx_overflow(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                                bool do_alloc) const {
  const bool is_inline = tx_idx.is_inline();
  uint16_t capacity = tx_idx.get_capacity();
  if (unlikely(tx_idx.local_idx >= capacity)) {
    LogicalBlockIdx block_idx =
        is_inline ? meta->get_next_tx_block() : tx_block->get_next_tx_block();
    if (block_idx == 0) {
      if (!do_alloc) return false;
      tx_idx.block_idx = is_inline ? alloc_next_block(meta, tx_block)
                                   : alloc_next_block(tx_block, tx_block);
      tx_idx.local_idx -= capacity;
    } else {
      tx_idx.block_idx = block_idx;
      tx_idx.local_idx -= capacity;
      tx_block = &file->lidx_to_addr_rw(tx_idx.block_idx)->tx_block;
    }
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
  LogicalBlockIdx next_block_idx;
  assert((tx_idx.block_idx == 0) == (tx_block == nullptr));

  if (tx_idx.block_idx == 0) {  // search from meta
    if ((next_block_idx = meta->get_next_tx_block()) != 0) {
      tx_idx.local_idx = meta->find_tail(tx_idx.local_idx);
      return;
    } else {
      tx_idx.block_idx = next_block_idx;
      tx_idx.local_idx = 0;
      tx_block = &file->lidx_to_addr_rw(tx_idx.block_idx)->tx_block;
    }
  }

  while ((next_block_idx = tx_block->get_next_tx_block()) != 0) {
    tx_idx.block_idx = next_block_idx;
    tx_block = &(file->lidx_to_addr_rw(next_block_idx)->tx_block);
  }
  tx_idx.local_idx = tx_block->find_tail(tx_idx.local_idx);
};

template <class B>
LogicalBlockIdx TxMgr::alloc_next_block(B* block,
                                        pmem::TxBlock*& new_tx_block) const {
  // allocate the next block
  auto allocator = file->get_local_allocator();
  pmem::Block* new_block;
  LogicalBlockIdx new_block_idx =
      allocator->alloc_tx_block(block->get_tx_seq() + 1, new_block);

  bool success = block->set_next_tx_block(new_block_idx);
  if (success) {
    new_tx_block = &new_block->tx_block;
    return new_block_idx;
  } else {
    // there is a race condition for adding the new blocks
    allocator->free_tx_block(new_block_idx, new_block);
    new_block_idx = block->get_next_tx_block();
    new_tx_block = &file->lidx_to_addr_rw(new_block_idx)->tx_block;
    return new_block_idx;
  }
}

void TxMgr::gc(const LogicalBlockIdx tail_tx_block_idx, uint64_t file_size) {
  // skip if tail_tx_block is meta block, it directly follows meta or there is
  // only one tx block between meta and tail_tx_block
  LogicalBlockIdx orig_tx_block_idx = meta->get_next_tx_block();
  if (tail_tx_block_idx == 0 || orig_tx_block_idx == tail_tx_block_idx ||
      file->lidx_to_addr_ro(orig_tx_block_idx)->tx_block.get_next_tx_block() ==
          tail_tx_block_idx)
    return;

  auto allocator = file->get_local_allocator();

  auto tail_block = file->lidx_to_addr_rw(tail_tx_block_idx);
  auto num_blocks = BLOCK_SIZE_TO_IDX(ALIGN_UP(file_size, BLOCK_SIZE));
  auto leftover_bytes = ALIGN_UP(file_size, BLOCK_SIZE) - file_size;

  uint32_t tx_seq = 1;
  auto first_tx_block_idx = allocator->alloc(1);
  auto new_block = file->lidx_to_addr_rw(first_tx_block_idx);
  memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
  new_block->tx_block.set_tx_seq(tx_seq++);
  auto new_tx_block = &new_block->tx_block;
  TxEntryIdx tx_idx = {first_tx_block_idx, 0};

  VirtualBlockIdx begin = 0;
  VirtualBlockIdx i = 1;
  for (; i < num_blocks; i++) {
    auto curr_blk_idx = file->vidx_to_lidx(i);
    auto prev_blk_idx = file->vidx_to_lidx(i - 1);
    if (curr_blk_idx == 0) break;
    // continuous blocks can be placed in 1 tx
    if (curr_blk_idx - prev_blk_idx == 1 &&
        i - begin <= /*TODO: pmem::TxEntryIndirect::NUM_BLOCKS_MAX*/ 63)
      continue;

    auto commit_entry =
        pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
    new_tx_block->store(commit_entry, tx_idx.local_idx);
    if (!advance_tx_idx(tx_idx, new_tx_block, false)) {
      // current block is full, flush it and allocate a new block
      auto new_tx_block_idx = allocator->alloc(1);
      new_tx_block->set_next_tx_block(new_tx_block_idx);
      pmem::persist_unfenced(new_tx_block, BLOCK_SIZE);
      new_block = file->lidx_to_addr_rw(new_tx_block_idx);
      memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
      new_block->tx_block.set_tx_seq(tx_seq++);
      new_tx_block = &new_block->tx_block;
      tx_idx = {new_tx_block_idx, 0};
    }
    begin = i;
  }

  // add the last commit entry
  {
    if (leftover_bytes == 0) {
      auto commit_entry =
          pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
      new_tx_block->store(commit_entry, tx_idx.local_idx);
    } else {
      // since i - begin <= 63, this can fit into one log entry
      auto begin_lidx = std::vector{file->vidx_to_lidx(begin)};
      auto log_head_idx = file->get_log_mgr()->append(
          allocator, pmem::LogEntry::Op::LOG_OVERWRITE, leftover_bytes,
          i - begin, begin, begin_lidx);
      auto commit_entry = pmem::TxEntryIndirect(log_head_idx);
      new_tx_block->store(commit_entry, tx_idx.local_idx);
    }
  }
  // pad the last block with dummy tx entries
  while (advance_tx_idx(tx_idx, new_tx_block, false))
    new_tx_block->store(pmem::TxEntry::TxEntryDummy, tx_idx.local_idx);
  // last block points to the tail, meta points to the first block
  new_tx_block->set_next_tx_block(tail_tx_block_idx);
  // abort if new transaction history is longer than the old one
  if (tail_block->tx_block.get_tx_seq() <= new_tx_block->get_tx_seq())
    goto abort;
  pmem::persist_fenced(new_tx_block, BLOCK_SIZE);
  while (!meta->set_next_tx_block(first_tx_block_idx, orig_tx_block_idx))
    ;
  // FIXME: use better API to flush the first cache line
  pmem::persist_cl_fenced(&meta);

  // invalidate tx in meta block so we can free the log blocks they point to
  meta->invalidate_tx_entries();

  return;

abort:
  // free the new tx blocks
  auto new_tx_blk_idx = first_tx_block_idx;
  do {
    auto next_tx_blk_idx = new_tx_block->get_next_tx_block();
    allocator->free(new_tx_blk_idx, 1);
    new_tx_blk_idx = next_tx_blk_idx;
  } while (new_tx_blk_idx != tail_tx_block_idx && new_tx_blk_idx != 0);
  allocator->return_free_list();
}

// explicit template instantiations
template LogicalBlockIdx TxMgr::alloc_next_block(
    pmem::MetaBlock* block, pmem::TxBlock*& new_block) const;
template LogicalBlockIdx TxMgr::alloc_next_block(
    pmem::TxBlock* block, pmem::TxBlock*& new_block) const;

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << "Transactions: \n";

  auto log_mgr = tx_mgr.file->get_log_mgr();

  TxEntryIdx tx_idx = {0, 0};
  pmem::TxBlock* tx_block = nullptr;

  while (true) {
    auto tx_entry = tx_mgr.get_entry_from_block(tx_idx, tx_block);
    if (!tx_entry.is_valid()) break;
    if (tx_entry.is_dummy()) goto next;

    // print tx entry
    out << "\t" << tx_idx << " -> " << tx_entry << "\n";

    // print log entries if the tx is not inlined
    if (!tx_entry.is_inline()) {
      pmem::LogEntryBlock* curr_block;
      pmem::LogEntry* curr_entry = log_mgr->get_entry(
          tx_entry.indirect_entry.get_log_entry_idx(), curr_block);
      assert(curr_entry && curr_block);

      do {
        out << "\t\t" << *curr_entry << "\n";
      } while ((curr_entry = log_mgr->get_next_entry(curr_entry, curr_block)));
    }
  next:
    if (!tx_mgr.advance_tx_idx(tx_idx, tx_block, /*do_alloc*/ false)) break;
  }

  return out;
}

}  // namespace ulayfs::dram
