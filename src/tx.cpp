#include "tx.h"

#include <cstddef>
#include <cstring>
#include <vector>

#include "block.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "persist.h"
#include "utils.h"

namespace ulayfs::dram {

/**
 * Temporary, thread-local store for a sequence of objects.
 * Compared to variable-length array on the stack, it's less likely to overflow.
 * By reusing the same vector, it avoids the overhead of memory allocation from
 * the globally shared heap.
 */

// This one is used for redo_image in ReadTx and recycle_image in WriteTx
thread_local std::vector<LogicalBlockIdx> local_buf_image_lidxs;

// These are are used in WriteTx for dst blocks
thread_local std::vector<LogicalBlockIdx> local_buf_dst_lidxs;
thread_local std::vector<pmem::Block*> local_buf_dst_blocks;

/*
 * TxMgr
 */

ssize_t TxMgr::do_pread(char* buf, size_t count, size_t offset) {
  return ReadTx(file, this, buf, count, offset).do_read();
}

ssize_t TxMgr::do_read(char* buf, size_t count) {
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  uint64_t file_size;
  uint64_t ticket;
  int64_t offset = file->update_with_offset(tail_tx_idx, tail_tx_block, count,
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

template <typename TX>
ssize_t TxMgr::WriteTx::do_write_and_validate_offset(
    File* file, TxMgr* tx_mgr, const char* buf, size_t count, size_t offset,
    TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block, uint64_t ticket) {
  TX tx(file, tx_mgr, buf, count, offset, tail_tx_idx, tail_tx_block, ticket);
  ssize_t ret = tx.do_write();
  tx.file->release_offset(ticket, tx.tail_tx_idx, tx.tail_tx_block);
  return ret;
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
        out << "\t\t" << *curr_entry;
      } while ((curr_entry = log_mgr->get_next_entry(curr_entry, curr_block)));
    }
  next:
    if (!tx_mgr.advance_tx_idx(tx_idx, tx_block, /*do_alloc*/ false)) break;
  }

  return out;
}

/*
 * Tx
 */

TxMgr::Tx::Tx(File* file, TxMgr* tx_mgr, size_t count, size_t offset)
    : file(file),
      tx_mgr(tx_mgr),
      log_mgr(file->get_log_mgr()),

      // input properties
      count(count),
      offset(offset),

      // derived properties
      end_offset(offset + count),
      begin_vidx(BLOCK_SIZE_TO_IDX(offset)),
      end_vidx(BLOCK_SIZE_TO_IDX(ALIGN_UP(end_offset, BLOCK_SIZE))),
      num_blocks(end_vidx - begin_vidx),
      is_offset_depend(false) {}

bool TxMgr::Tx::handle_conflict(pmem::TxEntry curr_entry,
                                VirtualBlockIdx first_vidx,
                                VirtualBlockIdx last_vidx,
                                std::vector<LogicalBlockIdx>& conflict_image) {
  bool has_conflict = false;

  // TODO: handle writev requests
  do {
    if (curr_entry.is_inline()) {  // inline tx entry
      has_conflict |= get_conflict_image(
          first_vidx, last_vidx, curr_entry.inline_entry.begin_virtual_idx,
          curr_entry.inline_entry.begin_logical_idx,
          curr_entry.inline_entry.num_blocks, conflict_image);
    } else {  // non-inline tx entry
      pmem::LogEntryBlock* curr_le_block;
      pmem::LogEntry* curr_le_entry = log_mgr->get_entry(
          curr_entry.indirect_entry.get_log_entry_idx(), curr_le_block);

      do {
        uint32_t i;
        for (i = 0; i < curr_le_entry->get_lidxs_len() - 1; ++i) {
          has_conflict |= get_conflict_image(
              first_vidx, last_vidx,
              curr_le_entry->begin_vidx + (i << BITMAP_CAPACITY_SHIFT),
              curr_le_entry->begin_lidxs[i], BITMAP_CAPACITY, conflict_image);
        }
        has_conflict |= get_conflict_image(
            first_vidx, last_vidx,
            curr_le_entry->begin_vidx + (i << BITMAP_CAPACITY_SHIFT),
            curr_le_entry->begin_lidxs[i],
            curr_le_entry->get_last_lidx_num_blocks(), conflict_image);
      } while ((curr_le_entry =
                    log_mgr->get_next_entry(curr_le_entry, curr_le_block)));
    }
    if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, /*do_alloc*/ false))
      break;
    curr_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
  } while (curr_entry.is_valid());

  return has_conflict;
}

// TODO: more fancy handle_conflict strategy
ssize_t TxMgr::ReadTx::do_read() {
  size_t first_block_overlap_size = offset & (BLOCK_SIZE - 1);
  size_t first_block_size = BLOCK_SIZE - first_block_overlap_size;
  if (first_block_size > count) first_block_size = count;

  VirtualBlockIdx curr_vidx;
  const pmem::Block* curr_block;
  pmem::TxEntry curr_entry;
  size_t buf_offset;

  std::vector<LogicalBlockIdx>& redo_image = local_buf_image_lidxs;
  redo_image.clear();
  redo_image.resize(num_blocks, 0);

  if (!is_offset_depend)
    file_size = file->update(tail_tx_idx, tail_tx_block, /*do_alloc*/ false);
  // reach EOF
  if (offset >= file_size) return 0;
  if (offset + count > file_size) {  // partial read; recalculate end_*
    count = file_size - offset;
    end_offset = offset + count;
    end_vidx = BLOCK_SIZE_TO_IDX(ALIGN_UP(end_offset, BLOCK_SIZE));
  }

  // first handle the first block (which might not be full block)
  curr_block = file->vidx_to_addr_ro(begin_vidx);
  memcpy(buf, curr_block->data_ro() + first_block_overlap_size,
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

redo:
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
    if (redo_lidx != 0) {
      curr_block = file->lidx_to_addr_ro(redo_lidx);
      memcpy(buf, curr_block->data_ro() + first_block_overlap_size,
             first_block_size);
      redo_image[0] = 0;
    }
    buf_offset = first_block_size;

    // then handle middle full blocks (which might not exist)
    for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
      redo_lidx = redo_image[curr_vidx - begin_vidx];
      if (redo_lidx != 0) {
        curr_block = file->lidx_to_addr_ro(redo_lidx);
        memcpy(buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
        redo_image[curr_vidx - begin_vidx] = 0;
      }
      buf_offset += BLOCK_SIZE;
    }

    // if we have multiple blocks to read
    if (begin_vidx != end_vidx - 1) {
      redo_lidx = redo_image[curr_vidx - begin_vidx];
      if (redo_lidx != 0) {
        curr_block = file->lidx_to_addr_ro(redo_lidx);
        memcpy(buf + buf_offset, curr_block->data_ro(), count - buf_offset);
        redo_image[curr_vidx - begin_vidx] = 0;
      }
    }
  }

  // we actually don't care what's the previous tx's tail, because we will need
  // to validate against the latest tail anyway
  if (is_offset_depend)
    if (!file->validate_offset(ticket, tail_tx_idx, tail_tx_block)) {
      // we don't need to revalidate after redo
      is_offset_depend = false;
      goto redo;
    }

  return static_cast<ssize_t>(count);
}

TxMgr::WriteTx::WriteTx(File* file, TxMgr* tx_mgr, const char* buf,
                        size_t count, size_t offset)
    : Tx(file, tx_mgr, count, offset),
      buf(buf),
      allocator(file->get_local_allocator()),
      recycle_image(local_buf_image_lidxs),
      dst_lidxs(local_buf_dst_lidxs),
      dst_blocks(local_buf_dst_blocks) {
  // reset recycle_image
  recycle_image.clear();
  recycle_image.resize(num_blocks);
  local_buf_dst_lidxs.clear();
  local_buf_dst_blocks.clear();

  // TODO: handle writev requests
  // for overwrite, "leftover_bytes" is zero; only in append we care
  // append log without fence because we only care flush completion
  // before try_commit
  uint32_t rest_num_blocks = num_blocks;
  while (rest_num_blocks > 0) {
    uint32_t chunk_num_blocks = std::min(rest_num_blocks, BITMAP_CAPACITY);
    auto lidx = allocator->alloc(chunk_num_blocks);
    dst_lidxs.push_back(lidx);
    rest_num_blocks -= chunk_num_blocks;
  }
  assert(!dst_lidxs.empty());

  for (auto lidx : dst_lidxs) dst_blocks.push_back(file->lidx_to_addr_rw(lidx));
  assert(!dst_blocks.empty());

  uint16_t leftover_bytes = ALIGN_UP(end_offset, BLOCK_SIZE) - end_offset;
  if (pmem::TxEntryInline::can_inline(num_blocks, begin_vidx, dst_lidxs[0],
                                      leftover_bytes)) {
    commit_entry = pmem::TxEntryInline(num_blocks, begin_vidx, dst_lidxs[0]);
  } else {
    // it's fine that we append log first as long we don't publish it by tx
    auto log_entry_idx =
        log_mgr->append(allocator, pmem::LogEntry::Op::LOG_OVERWRITE,  // op
                        leftover_bytes,  // leftover_bytes
                        num_blocks,      // total_blocks
                        begin_vidx,      // begin_virtual_idx
                        dst_lidxs        // begin_logical_idxs
        );
    commit_entry = pmem::TxEntryIndirect(log_entry_idx);
  }
}

/*
 * AlignedTx
 */
ssize_t TxMgr::AlignedTx::do_write() {
  pmem::TxEntry conflict_entry;

  // since everything is block-aligned, we can copy data directly
  const char* rest_buf = buf;
  size_t rest_count = count;
  for (auto block : dst_blocks) {
    size_t num_bytes = std::min(rest_count, BITMAP_CAPACITY_IN_BYTES);
    pmem::memcpy_persist(block->data_rw(), rest_buf, num_bytes);
    rest_buf += num_bytes;
    rest_count -= num_bytes;
  }
  _mm_sfence();

  // make a local copy of the tx tail
  if (!is_offset_depend)
    file->update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  for (uint32_t i = 0; i < num_blocks; ++i)
    recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);

  if (is_offset_depend) file->wait_offset(ticket);

retry:
  conflict_entry = tx_mgr->try_commit(commit_entry, tail_tx_idx, tail_tx_block);
  if (!conflict_entry.is_valid()) goto done;
  // we don't check the return value of handle_conflict here because we don't
  // care whether there is a conflict, as long as recycle_image gets updated
  handle_conflict(conflict_entry, begin_vidx, end_vidx - 1, recycle_image);
  goto retry;

done:
  // recycle the data blocks being overwritten
  allocator->free(recycle_image);
  return static_cast<ssize_t>(count);
}

ssize_t TxMgr::SingleBlockTx::do_write() {
  pmem::TxEntry conflict_entry;

  // must acquire the tx tail before any get
  if (!is_offset_depend)
    file->update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  recycle_image[0] = file->vidx_to_lidx(begin_vidx);
  assert(recycle_image[0] != dst_lidxs[0]);

  // copy data from buf
  pmem::memcpy_persist(dst_blocks[0]->data_rw() + local_offset, buf, count);

redo:
  // copy original data
  const pmem::Block* src_block = file->lidx_to_addr_ro(recycle_image[0]);
  assert(dst_blocks.size() == 1);
  pmem::memcpy_persist(dst_blocks[0]->data_rw(), src_block->data_ro(),
                       local_offset);
  pmem::memcpy_persist(dst_blocks[0]->data_rw() + local_offset + count,
                       src_block->data_ro() + local_offset + count,
                       BLOCK_SIZE - (local_offset + count));

  if (is_offset_depend) file->wait_offset(ticket);

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
  allocator->free(recycle_image[0]);  // it has only single block
  return static_cast<ssize_t>(count);
}

/*
 * MultiBlockTx
 */

ssize_t TxMgr::MultiBlockTx::do_write() {
  // if need_copy_first/last is false, this means it is handled by the full
  // block copy and never need redo
  const bool need_copy_first = begin_full_vidx != begin_vidx;
  const bool need_copy_last = end_full_vidx != end_vidx;
  // do_copy_first/last indicates do we actually need to do copy; in the case of
  // redo, we may skip if no change is made
  bool do_copy_first = true;
  bool do_copy_last = true;
  pmem::TxEntry conflict_entry;
  LogicalBlockIdx src_first_lidx, src_last_lidx;

  // copy full blocks first
  if (num_full_blocks > 0) {
    const char* rest_buf = buf;
    size_t rest_full_count = BLOCK_NUM_TO_SIZE(num_full_blocks);
    for (size_t i = 0; i < dst_blocks.size(); ++i) {
      // get logical block pointer for this iter
      // first block in first chunk could start from partial
      pmem::Block* full_blocks = dst_blocks[i];
      if (i == 0) {
        full_blocks += (begin_full_vidx - begin_vidx);
        rest_buf += first_block_overlap_size;
      }
      // calculate num of full block bytes to be copied in this iter
      // takes care of last block in last chunk which might be partial
      size_t num_bytes = rest_full_count;
      if (dst_blocks.size() > 1) {
        if (i == 0 && need_copy_first)
          num_bytes = BITMAP_CAPACITY_IN_BYTES - BLOCK_SIZE;
        else if (i < dst_blocks.size() - 1)
          num_bytes = BITMAP_CAPACITY_IN_BYTES;
      }
      // actual memcpy
      pmem::memcpy_persist(full_blocks->data_rw(), rest_buf, num_bytes);
      rest_buf += num_bytes;
      rest_full_count -= num_bytes;
    }
  }

  // only get a snapshot of the tail when starting critical piece
  if (!is_offset_depend)
    file->update(tail_tx_idx, tail_tx_block, /*do_alloc*/ true);
  for (uint32_t i = 0; i < num_blocks; ++i)
    recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);
  src_first_lidx = recycle_image[0];
  src_last_lidx = recycle_image[num_blocks - 1];

  // write data from the buf to the first block
  char* dst = dst_blocks[0]->data_rw() + BLOCK_SIZE - first_block_overlap_size;
  pmem::memcpy_persist(dst, buf, first_block_overlap_size);

  // write data from the buf to the last block
  pmem::Block* last_dst_block = dst_blocks.back() +
                                (end_full_vidx - begin_vidx) -
                                BITMAP_CAPACITY * (dst_blocks.size() - 1);
  const char* buf_src = buf + (count - last_block_overlap_size);
  pmem::memcpy_persist(last_dst_block->data_rw(), buf_src,
                       last_block_overlap_size);

redo:
  // copy first block
  if (need_copy_first && do_copy_first) {
    // copy the data from the first source block if exists
    pmem::memcpy_persist(dst_blocks[0]->data_rw(),
                         file->lidx_to_addr_ro(src_first_lidx)->data_ro(),
                         BLOCK_SIZE - first_block_overlap_size);
  }

  // copy last block
  if (need_copy_last && do_copy_last) {
    // copy the data from the last source block if exits
    pmem::memcpy_persist(last_dst_block->data_rw() + last_block_overlap_size,
                         file->lidx_to_addr_ro(src_last_lidx)->data_ro() +
                             last_block_overlap_size,
                         BLOCK_SIZE - last_block_overlap_size);
  }
  _mm_sfence();

  if (is_offset_depend) file->wait_offset(ticket);

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
  allocator->free(recycle_image);
  return static_cast<ssize_t>(count);
}

}  // namespace ulayfs::dram
