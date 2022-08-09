#include "mgr.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <vector>

#include "alloc.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "persist.h"
#include "tx/read.h"
#include "tx/write_aligned.h"
#include "tx/write_unaligned.h"
#include "utils.h"

namespace ulayfs::dram {

ssize_t TxMgr::do_pread(char* buf, size_t count, size_t offset) {
  return ReadTx(file, this, buf, count, offset).exec();
}

ssize_t TxMgr::do_read(char* buf, size_t count) {
  FileState state;
  uint64_t ticket;
  uint64_t offset;
  file->update_with_offset(&state, count,
                           /*stop_at_boundary*/ true, ticket, offset,
                           /*do_alloc*/ false);

  return Tx::exec_and_release_offset<ReadTx>(file, this, buf, count, offset,
                                             state, ticket);
}

ssize_t TxMgr::do_pwrite(const char* buf, size_t count, size_t offset) {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0)
    return AlignedTx(file, this, buf, count, offset).exec();

  // another special case where range is within a single block
  if ((BLOCK_SIZE_TO_IDX(offset)) == BLOCK_SIZE_TO_IDX(offset + count - 1))
    return SingleBlockTx(file, this, buf, count, offset).exec();

  // unaligned multi-block write
  return MultiBlockTx(file, this, buf, count, offset).exec();
}

ssize_t TxMgr::do_write(const char* buf, size_t count) {
  FileState state;
  uint64_t ticket;
  uint64_t offset;
  file->update_with_offset(&state, count,
                           /*stop_at_boundary*/ false, ticket, offset,
                           /*do_alloc*/ false);

  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0)
    return Tx::exec_and_release_offset<AlignedTx>(file, this, buf, count,
                                                  offset, state, ticket);

  // another special case where range is within a single block
  if (BLOCK_SIZE_TO_IDX(offset) == BLOCK_SIZE_TO_IDX(offset + count - 1))
    return Tx::exec_and_release_offset<SingleBlockTx>(file, this, buf, count,
                                                      offset, state, ticket);

  // unaligned multi-block write
  return Tx::exec_and_release_offset<MultiBlockTx>(file, this, buf, count,
                                                   offset, state, ticket);
}

bool TxMgr::advance_cursor(TxCursor* cursor, bool do_alloc) const {
  assert(cursor->idx.local_idx >= 0);
  cursor->idx.local_idx++;
  return handle_cursor_overflow(cursor, do_alloc);
}

pmem::TxEntry TxMgr::get_tx_entry(const TxCursor cursor) const {
  if (cursor.idx.block_idx == 0)
    return meta->get_tx_entry(cursor.idx.local_idx);
  return cursor.block->get(cursor.idx.local_idx);
}

std::tuple<pmem::LogEntry*, pmem::LogEntryBlock*> TxMgr::get_log_entry(
    LogEntryIdx idx, bool init_bitmap) const {
  if (init_bitmap) file->set_allocated(idx.block_idx);
  pmem::LogEntryBlock* curr_block =
      &file->lidx_to_addr_rw(idx.block_idx)->log_entry_block;
  return {curr_block->get(idx.local_offset), curr_block};
}

[[nodiscard]] std::tuple<pmem::LogEntry*, pmem::LogEntryBlock*>
TxMgr::get_next_log_entry(const pmem::LogEntry* curr_entry,
                          pmem::LogEntryBlock* curr_block,
                          bool init_bitmap) const {
  // check if we are at the end of the linked list
  if (!curr_entry->has_next) return {nullptr, nullptr};

  // next entry is in the same block
  if (curr_entry->is_next_same_block)
    return {curr_block->get(curr_entry->next.local_offset), curr_block};

  // move to the next block
  LogicalBlockIdx next_block_idx = curr_entry->next.block_idx;
  if (init_bitmap) file->set_allocated(next_block_idx);
  const auto next_block =
      &file->lidx_to_addr_rw(next_block_idx)->log_entry_block;
  // if the next entry is on another block, it must be from the first byte
  return {next_block->get(0), next_block};
}

LogEntryIdx TxMgr::append_log_entry(
    Allocator* allocator, pmem::LogEntry::Op op, uint16_t leftover_bytes,
    uint32_t num_blocks, VirtualBlockIdx begin_vidx,
    const std::vector<LogicalBlockIdx>& begin_lidxs) const {
  const auto& [first_entry, first_idx, first_block] =
      allocator->alloc_log_entry(num_blocks);

  pmem::LogEntry* curr_entry = first_entry;
  pmem::LogEntryBlock* curr_block = first_block;

  // i to iterate through begin_lidxs across entries
  // j to iterate within each entry
  uint32_t i, j;
  i = 0;
  while (true) {
    curr_entry->op = op;
    curr_entry->begin_vidx = begin_vidx;
    for (j = 0; j < curr_entry->get_lidxs_len(); ++j)
      curr_entry->begin_lidxs[j] = begin_lidxs[i + j];
    const auto& [next_entry, next_block] =
        get_next_log_entry(curr_entry, curr_block);
    if (next_entry != nullptr) {
      curr_entry->leftover_bytes = 0;
      curr_entry->persist();
      curr_entry = next_entry;
      curr_block = next_block;
      i += j;
      begin_vidx += (j << BITMAP_BLOCK_CAPACITY_SHIFT);
    } else {  // last entry
      curr_entry->leftover_bytes = leftover_bytes;
      curr_entry->persist();
      break;
    }
  }
  return first_idx;
}

void TxMgr::update_log_entry_leftover_bytes(LogEntryIdx first_idx,
                                            uint16_t leftover_bytes) {
  const auto& [first_entry, first_block] = get_log_entry(first_idx);
  pmem::LogEntry* curr_entry = first_entry;
  pmem::LogEntryBlock* curr_block = first_block;
  while (true) {
    const auto& [next_entry, next_block] =
        get_next_log_entry(curr_entry, curr_block);
    if (next_entry != nullptr) {
      curr_entry = next_entry;
      curr_block = next_block;
    } else {
      curr_entry->leftover_bytes = leftover_bytes;
      curr_entry->persist();
      break;
    }
  }
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

pmem::TxEntry TxMgr::try_commit(pmem::TxEntry entry, TxCursor* cursor) {
  handle_cursor_overflow(cursor, true);

  bool is_inline = cursor->idx.block_idx == 0;
  assert(is_inline == (cursor->block == nullptr));

  if (pmem::TxEntry::need_flush(cursor->idx.local_idx)) {
    flush_tx_entries(meta->get_tx_tail(), *cursor);
    meta->set_tx_tail(cursor->idx);
  }
  return is_inline ? meta->try_append(entry, cursor->idx.local_idx)
                   : cursor->block->try_append(entry, cursor->idx.local_idx);
}

bool TxMgr::handle_cursor_overflow(TxCursor* cursor, bool do_alloc) const {
  const bool is_inline = cursor->idx.is_inline();
  uint16_t capacity = cursor->idx.get_capacity();
  if (unlikely(cursor->idx.local_idx >= capacity)) {
    LogicalBlockIdx block_idx = is_inline ? meta->get_next_tx_block()
                                          : cursor->block->get_next_tx_block();
    if (block_idx == 0) {
      if (!do_alloc) return false;
      cursor->idx.block_idx =
          is_inline ? alloc_next_block(meta, cursor->block)
                    : alloc_next_block(cursor->block, cursor->block);
      cursor->idx.local_idx -= capacity;
    } else {
      cursor->idx.block_idx = block_idx;
      cursor->idx.local_idx -= capacity;
      cursor->block = &file->lidx_to_addr_rw(cursor->idx.block_idx)->tx_block;
    }
  }
  return true;
}

void TxMgr::flush_tx_entries(TxEntryIdx begin_idx, TxCursor cursor) {
  if (!tx_idx_greater(cursor.idx, begin_idx)) return;
  pmem::TxBlock* tx_block_begin;
  // handle special case of inline tx
  if (begin_idx.block_idx == 0) {
    if (cursor.idx.block_idx == 0) {
      meta->flush_tx_entries(begin_idx.local_idx, cursor.idx.local_idx);
      goto done;
    }
    meta->flush_tx_block(begin_idx.local_idx);
    // now the next block is the "new begin"
    begin_idx = {meta->get_next_tx_block(), 0};
  }
  while (begin_idx.block_idx != cursor.idx.block_idx) {
    tx_block_begin = &file->lidx_to_addr_rw(begin_idx.block_idx)->tx_block;
    tx_block_begin->flush_tx_block(begin_idx.local_idx);
    begin_idx = {tx_block_begin->get_next_tx_block(), 0};
    // special case: tx_idx_end is the first entry of the next block, which
    // means we only need to flush the current block and no need to
    // dereference to get the last block
  }
  if (begin_idx.local_idx == cursor.idx.local_idx) goto done;
  cursor.block->flush_tx_entries(begin_idx.local_idx, cursor.idx.local_idx);

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
  TxCursor cursor = {.idx = {first_tx_block_idx, 0},
                     .block = &new_block->tx_block};

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
    cursor.block->store(commit_entry, cursor.idx.local_idx);
    if (!advance_cursor(&cursor, false)) {
      // current block is full, flush it and allocate a new block
      auto new_tx_block_idx = allocator->alloc(1);
      cursor.block->set_next_tx_block(new_tx_block_idx);
      pmem::persist_unfenced(cursor.block, BLOCK_SIZE);
      new_block = file->lidx_to_addr_rw(new_tx_block_idx);
      memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
      new_block->tx_block.set_tx_seq(tx_seq++);
      cursor.block = &new_block->tx_block;
      cursor.idx = {new_tx_block_idx, 0};
    }
    begin = i;
  }

  // add the last commit entry
  {
    if (leftover_bytes == 0) {
      auto commit_entry =
          pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
      cursor.block->store(commit_entry, cursor.idx.local_idx);
    } else {
      // since i - begin <= 63, this can fit into one log entry
      auto begin_lidx = std::vector{file->vidx_to_lidx(begin)};
      auto log_head_idx =
          append_log_entry(allocator, pmem::LogEntry::Op::LOG_OVERWRITE,
                           leftover_bytes, i - begin, begin, begin_lidx);
      auto commit_entry = pmem::TxEntryIndirect(log_head_idx);
      cursor.block->store(commit_entry, cursor.idx.local_idx);
    }
  }
  // pad the last block with dummy tx entries
  while (advance_cursor(&cursor, false))
    cursor.block->store(pmem::TxEntry::TxEntryDummy, cursor.idx.local_idx);
  // last block points to the tail, meta points to the first block
  cursor.block->set_next_tx_block(tail_tx_block_idx);
  // abort if new transaction history is longer than the old one
  if (tail_block->tx_block.get_tx_seq() <= cursor.block->get_tx_seq())
    goto abort;
  pmem::persist_fenced(cursor.block, BLOCK_SIZE);
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
    auto next_tx_blk_idx = cursor.block->get_next_tx_block();
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
  TxCursor cursor{};

  while (true) {
    auto tx_entry = tx_mgr.get_tx_entry(cursor);
    if (!tx_entry.is_valid()) break;
    if (tx_entry.is_dummy()) goto next;

    // print tx entry
    out << "\t" << cursor.idx << " -> " << tx_entry << "\n";

    // print log entries if the tx is not inlined
    if (!tx_entry.is_inline()) {
      auto [curr_entry, curr_block] =
          tx_mgr.get_log_entry(tx_entry.indirect_entry.get_log_entry_idx());
      assert(curr_entry && curr_block);

      while (true) {
        out << "\t\t" << *curr_entry << "\n";
        const auto& [next_entry, next_block] =
            tx_mgr.get_next_log_entry(curr_entry, curr_block);
        if (!next_entry) break;
        curr_entry = next_entry;
        curr_block = next_block;
      }
    }
  next:
    if (!tx_mgr.advance_cursor(&cursor, /*do_alloc*/ false)) break;
  }

  return out;
}

}  // namespace ulayfs::dram
