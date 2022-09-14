#include "mgr.h"

#include <cmath>
#include <cstddef>
#include <vector>

#include "alloc/alloc.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "tx/read.h"
#include "tx/tx_cursor.h"
#include "tx/write_aligned.h"
#include "tx/write_unaligned.h"

namespace ulayfs::dram {

ssize_t TxMgr::do_pread(char* buf, size_t count, size_t offset,
                        Allocator* allocator) {
  TimerGuard<Event::READ_TX> timer_guard;
  timer.start<Event::READ_TX_CTOR>();
  return ReadTx(allocator, this, buf, count, offset).exec();
}

ssize_t TxMgr::do_read(char* buf, size_t count, Allocator* allocator) {
  uint64_t ticket;
  uint64_t offset;
  FileState state = blk_table->update([&](FileState* state) {
    offset = offset_mgr->acquire(count, state->file_size,
                                 /*stop_at_boundary*/ true, ticket);
  });

  return Tx::exec_and_release_offset<ReadTx>(allocator, this, buf, count,
                                             offset, state, ticket);
}

ssize_t TxMgr::do_pwrite(const char* buf, size_t count, size_t offset,
                         Allocator* allocator) {
  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    timer.start<Event::ALIGNED_TX_CTOR>();
    return AlignedTx(allocator, this, buf, count, offset).exec();
  }

  // another special case where range is within a single block
  if ((BLOCK_SIZE_TO_IDX(offset)) == BLOCK_SIZE_TO_IDX(offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return SingleBlockTx(allocator, this, buf, count, offset).exec();
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return MultiBlockTx(allocator, this, buf, count, offset).exec();
  }
}

ssize_t TxMgr::do_write(const char* buf, size_t count, Allocator* allocator) {
  uint64_t ticket;
  uint64_t offset;
  FileState state = blk_table->update(
      [&](FileState* state) {
        offset = offset_mgr->acquire(count, state->file_size,
                                     /*stop_at_boundary*/ false, ticket);
      },
      allocator);

  // special case that we have everything aligned, no OCC
  if (count % BLOCK_SIZE == 0 && offset % BLOCK_SIZE == 0) {
    TimerGuard<Event::ALIGNED_TX> timer_guard;
    return Tx::exec_and_release_offset<AlignedTx>(allocator, this, buf, count,
                                                  offset, state, ticket);
  }

  // another special case where range is within a single block
  if (BLOCK_SIZE_TO_IDX(offset) == BLOCK_SIZE_TO_IDX(offset + count - 1)) {
    TimerGuard<Event::SINGLE_BLOCK_TX> timer_guard;
    return Tx::exec_and_release_offset<SingleBlockTx>(
        allocator, this, buf, count, offset, state, ticket);
  }

  // unaligned multi-block write
  {
    TimerGuard<Event::MULTI_BLOCK_TX> timer_guard;
    return Tx::exec_and_release_offset<MultiBlockTx>(
        allocator, this, buf, count, offset, state, ticket);
  }
}

std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr) {
  out << tx_mgr.offset_mgr;
  out << "Transactions: \n";

  TxCursor cursor(tx_mgr.mem_table->get_meta());
  int count = 0;

  while (true) {
    auto tx_entry = cursor.get_entry();
    if (!tx_entry.is_valid()) break;
    if (tx_entry.is_dummy()) goto next;

    count++;
    if (count > 100) {
      if (count % static_cast<int>(exp10(floor(log10(count)) - 1)) != 0)
        goto next;
    }

    out << "\t" << count << ": " << cursor.idx << " -> " << tx_entry << "\n";

    // print log entries if the tx is not inlined
    if (!tx_entry.is_inline()) {
      LogCursor log_cursor(tx_entry.indirect_entry, tx_mgr.mem_table);
      do {
        out << "\t\t" << *log_cursor << "\n";
      } while (log_cursor.advance(tx_mgr.mem_table));
    }

  next:
    if (bool success = cursor.advance(tx_mgr.mem_table); !success) break;
  }

  out << "\ttotal = " << count++ << "\n";

  return out;
}

}  // namespace ulayfs::dram
