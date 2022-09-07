#pragma once

#include "bitmap.h"
#include "mem_table.h"

namespace ulayfs::dram {

/**
 * A LogCursor is a pointer to a log entry. It does not store the mem_table and
 * needs the caller to pass it in to advance it.
 */
struct LogCursor {
  LogEntryIdx idx;
  pmem::LogEntryBlock* block;
  pmem::LogEntry* entry;

  LogCursor() = default;

  /**
   * @param idx the index of a log entry
   * @param block the block of a log entry
   * @param entry the pointer to a log entry
   * @param bitmap_mgr if not null, the bitmap manager will be used to mark the
   * bitmap entries as used
   */
  LogCursor(LogEntryIdx idx, pmem::LogEntryBlock* block, pmem::LogEntry* entry,
            BitmapMgr* bitmap_mgr = nullptr)
      : idx(idx), block(block), entry(entry) {
    if (bitmap_mgr) bitmap_mgr->set_allocated(idx.block_idx);
  }

  /**
   * @param idx the index of a log entry
   * @param mem_table the mem_table used to get the block address
   * @param bitmap_mgr if not null, the bitmap manager will be used to mark the
   * bitmap entries as used
   */
  LogCursor(LogEntryIdx idx, MemTable* mem_table,
            BitmapMgr* bitmap_mgr = nullptr)
      : idx(idx),
        block(&mem_table->lidx_to_addr_rw(idx.block_idx)->log_entry_block),
        entry(block->get(idx.local_offset)) {
    if (bitmap_mgr) bitmap_mgr->set_allocated(idx.block_idx);
  }

  /**
   * @param tx_entry an indirect transaction entry
   * @param mem_table the mem_table used to get the block address
   * @param bitmap_mgr if not null, the bitmap manager will be used to mark the
   * bitmap entries as used
   */
  LogCursor(pmem::TxEntryIndirect tx_entry, MemTable* mem_table,
            BitmapMgr* bitmap_mgr = nullptr)
      : idx(tx_entry.get_log_entry_idx()),
        block(&mem_table->lidx_to_addr_rw(idx.block_idx)->log_entry_block),
        entry(block->get(idx.local_offset)) {
    if (bitmap_mgr) bitmap_mgr->set_allocated(idx.block_idx);
  }

  pmem::LogEntry* operator->() const { return entry; }
  pmem::LogEntry& operator*() const { return *entry; }

  /**
   * @brief Advance the cursor to the next log entry.
   *
   * @param mem_table the mem_table used to convert logical index to address
   * @param bitmap_mgr if not nullptr, the bitmap_mgr will be used to set the
   * bitmap of the block
   * @return true if the cursor is advanced to the next log entry
   */
  bool advance(MemTable* mem_table, BitmapMgr* bitmap_mgr = nullptr) {
    // check if we are at the end of the linked list
    if (!entry->has_next) return false;

    // next entry is in the same block
    if (entry->is_next_same_block) {
      entry = block->get(entry->next.local_offset);
      return true;
    }

    // move to the next block
    LogicalBlockIdx next_block_idx = entry->next.block_idx;
    if (bitmap_mgr) bitmap_mgr->set_allocated(next_block_idx);
    block = &mem_table->lidx_to_addr_rw(next_block_idx)->log_entry_block;
    // if the next entry is on another block, it must be from the first byte
    entry = block->get(0);
    return true;
  }

  /**
   * Update the left over bytes in the last log entry of the linked list.
   * @param mem_table the mem_table used to convert logical index to address
   * @param leftover_bytes the new leftover bytes to be updated
   */
  void update_leftover_bytes(MemTable* mem_table, uint16_t leftover_bytes) {
    LogCursor log_cursor = *this;
    while (log_cursor.advance(mem_table))
      ;
    log_cursor->leftover_bytes = leftover_bytes;
    log_cursor->persist();
  }
};

}  // namespace ulayfs::dram
