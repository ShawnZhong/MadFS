#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <vector>

#include "alloc.h"
#include "block/block.h"
#include "entry.h"
#include "idx.h"
#include "lock.h"
#include "offset.h"

namespace ulayfs::dram {

class File;

class TxMgr {
 private:
  File* file;
  pmem::MetaBlock* meta;

 public:
  Lock lock;  // nop lock is used by default
  OffsetMgr offset_mgr;

 public:
  TxMgr(File* file, pmem::MetaBlock* meta)
      : file(file), meta(meta), offset_mgr(this) {}

  ssize_t do_pread(char* buf, size_t count, size_t offset);
  ssize_t do_read(char* buf, size_t count);
  ssize_t do_pwrite(const char* buf, size_t count, size_t offset);
  ssize_t do_write(const char* buf, size_t count);

  /**
   * Advance cursor to the next transaction entry
   *
   * @param cursor the cursor to advance
   * @param do_alloc whether allocation is allowed when reaching the end of
   * a block
   *
   * @return true on success; false when reaches the end of a block and do_alloc
   * is false. The advance would happen anyway but in the case of false, it is
   * in a overflow state
   */
  bool advance_cursor(TxCursor* cursor, bool do_alloc) const;

  /**
   * Get log entry given the index
   *
   * @param idx the log entry index
   * @param bitmap_mgr if set, initialize the bitmap
   * @return a tuple of (log entry, the block containing the entry)
   */
  [[nodiscard]] std::tuple<pmem::LogEntry*, pmem::LogEntryBlock*> get_log_entry(
      LogEntryIdx idx, BitmapMgr* bitmap_mgr = nullptr) const;

  /**
   * get the next log entry
   *
   * @param curr_entry the current log entry
   * @param curr_block the current log entry block; will be updated if
   * move on to the next block
   * @param bitmap_mgr if passed, initialized the bitmap
   * @return a tuple of (next_log_entry, next_log_entry_block)
   */
  [[nodiscard]] std::tuple<pmem::LogEntry*, pmem::LogEntryBlock*>
  get_next_log_entry(const pmem::LogEntry* curr_entry,
                     pmem::LogEntryBlock* curr_block,
                     BitmapMgr* bitmap_mgr = nullptr) const;

  /**
   * populate log entries required by a single transaction; do persist but not
   * fenced
   *
   * @param allocator allocator to use for allocating log entries
   * @param op operation code, e.g., LOG_OVERWRITE
   * @param leftover_bytes remaining empty bytes in the last block
   * @param num_blocks total number blocks touched
   * @param begin_vidx start of virtual index
   * @param begin_lidxs ordered list of logical indices for each chunk of
   * virtual index
   * @return index of the first LogHeadEntry for later retrival of the whole
   *         group of entries
   */
  LogEntryIdx append_log_entry(
      Allocator* allocator, pmem::LogEntry::Op op, uint16_t leftover_bytes,
      uint32_t num_blocks, VirtualBlockIdx begin_vidx,
      const std::vector<LogicalBlockIdx>& begin_lidxs) const;

  /**
   * reset leftover_bytes that was previously passed into append_log_entry
   *
   * @param first_idx return value of append_log_entry
   * @param leftover_bytes new value of leftover_bytes
   */
  void update_log_entry_leftover_bytes(LogEntryIdx first_idx,
                                       uint16_t leftover_bytes) const;

  /**
   * Try to commit an entry
   *
   * @param entry entry to commit
   * @param cursor the cursor to commit to (might be updated under overflow)
   * @return empty entry on success; conflict entry otherwise
   */
  pmem::TxEntry try_commit(pmem::TxEntry entry, TxCursor* cursor);

  /**
   * If the given cursor is in an overflow state, update it if allowed.
   *
   * @param cursor the cursor to update
   * @param do_alloc whether allocation is allowed
   * @return true if the idx is not in overflow state; false otherwise
   */
  bool handle_cursor_overflow(TxCursor* cursor, bool do_alloc) const;

  /**
   * Flush tx entries
   *
   * @param begin the start cursor of the entries to flush, exclusive
   * @param end the end cursor of the entries to flush, inclusive
   */
  void flush_tx_entries(TxCursor begin, TxCursor end);
  void flush_tx_entries(TxEntryIdx begin, TxCursor end);

 private:
  /**
   * Move along the linked list of TxBlock and find the tail. The returned
   * tail may not be up-to-date due to race condition. No new blocks will be
   * allocated. If the end of TxBlock is reached, just return NUM_TX_ENTRY as
   * the TxLocalIdx.
   */
  void find_tail(TxEntryIdx& curr_idx, pmem::TxBlock*& curr_block) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

}  // namespace ulayfs::dram
