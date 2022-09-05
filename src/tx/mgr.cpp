#include "mgr.h"

#include <cassert>
#include <cstddef>
#include <vector>

#include "alloc.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "tx/cursor.h"
#include "tx/read.h"
#include "tx/write_aligned.h"
#include "tx/write_unaligned.h"
#include "utils.h"

namespace ulayfs::dram {

ssize_t TxMgr::do_pread(char* buf, size_t count, size_t offset) {
  TimerGuard<Event::READ_TX> timer_guard;
  timer.start<Event::READ_TX_CTOR>();
  return ReadTx(file, this, buf, count, offset).exec();
}

ssize_t TxMgr::do_read(char* buf, size_t count) {
  FileState state;
  uint64_t ticket;
  uint64_t offset;
  file->update_with_offset(&state, count,
                           /*stop_at_boundary*/ true, ticket, offset);

  return Tx::exec_and_release_offset<ReadTx>(file, this, buf, count, offset,
                                             state, ticket);
}

ssize_t TxMgr::do_pwrite(const char* buf, size_t count, size_t offset) {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    timer.start<Event::ALIGNED_TX_CTOR>();
    return AlignedTx(file, this, buf, count, offset).exec();
  }

  // another special case where range is within a single block
  if ((BLOCK_SIZE_TO_IDX(offset)) == BLOCK_SIZE_TO_IDX(offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return SingleBlockTx(file, this, buf, count, offset).exec();
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return MultiBlockTx(file, this, buf, count, offset).exec();
  }
}

ssize_t TxMgr::do_write(const char* buf, size_t count) {
  FileState state;
  uint64_t ticket;
  uint64_t offset;
  file->update_with_offset(&state, count,
                           /*stop_at_boundary*/ false, ticket, offset);

  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    return Tx::exec_and_release_offset<AlignedTx>(file, this, buf, count,
                                                  offset, state, ticket);
  }

  // another special case where range is within a single block
  if (BLOCK_SIZE_TO_IDX(offset) == BLOCK_SIZE_TO_IDX(offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return Tx::exec_and_release_offset<SingleBlockTx>(file, this, buf, count,
                                                      offset, state, ticket);
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return Tx::exec_and_release_offset<MultiBlockTx>(file, this, buf, count,
                                                     offset, state, ticket);
  }
}

std::tuple<pmem::LogEntry*, pmem::LogEntryBlock*> TxMgr::get_log_entry(
    LogEntryIdx idx, BitmapMgr* bitmap_mgr) const {
  if (bitmap_mgr) bitmap_mgr->set_allocated(idx.block_idx);
  pmem::LogEntryBlock* curr_block =
      &mem_table->lidx_to_addr_rw(idx.block_idx)->log_entry_block;
  return {curr_block->get(idx.local_offset), curr_block};
}

[[nodiscard]] std::tuple<pmem::LogEntry*, pmem::LogEntryBlock*>
TxMgr::get_next_log_entry(const pmem::LogEntry* curr_entry,
                          pmem::LogEntryBlock* curr_block,
                          BitmapMgr* bitmap_mgr) const {
  // check if we are at the end of the linked list
  if (!curr_entry->has_next) return {nullptr, nullptr};

  // next entry is in the same block
  if (curr_entry->is_next_same_block)
    return {curr_block->get(curr_entry->next.local_offset), curr_block};

  // move to the next block
  LogicalBlockIdx next_block_idx = curr_entry->next.block_idx;
  if (bitmap_mgr) bitmap_mgr->set_allocated(next_block_idx);
  const auto next_block =
      &mem_table->lidx_to_addr_rw(next_block_idx)->log_entry_block;
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
      begin_vidx += (j << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT);
    } else {  // last entry
      curr_entry->leftover_bytes = leftover_bytes;
      curr_entry->persist();
      break;
    }
  }
  return first_idx;
}

void TxMgr::update_log_entry_leftover_bytes(LogEntryIdx first_idx,
                                            uint16_t leftover_bytes) const {
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

pmem::TxEntry TxMgr::try_commit(pmem::TxEntry entry, TxCursor* cursor) const {
  cursor->handle_overflow(mem_table, file->get_local_allocator());

  if (pmem::TxEntry::need_flush(cursor->idx.local_idx)) {
    TxCursor::flush_up_to(mem_table, *cursor);
    meta->set_tx_tail(cursor->idx);
  }

  return cursor->try_append(entry);
}

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << tx_mgr.offset_mgr;
  out << "Transactions: \n";
  TxCursor cursor({}, tx_mgr.meta);
  int count = 1;

  while (true) {
    auto tx_entry = cursor.get_entry();
    if (!tx_entry.is_valid()) break;
    if (tx_entry.is_dummy()) goto next;

    // print tx entry
    out << "\t" << count++ << ": " << cursor.idx << " -> " << tx_entry << "\n";

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
    if (bool success = cursor.advance(tx_mgr.mem_table); !success) break;
  }

  return out;
}

}  // namespace ulayfs::dram
