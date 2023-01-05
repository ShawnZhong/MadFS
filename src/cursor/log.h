#pragma once

#include "bitmap.h"
#include "mem_table.h"

namespace ulayfs::dram {

/**
 * A LogCursor is a pointer to a log entry. It does not store the mem_table and
 * needs the caller to pass it in to advance it.
 *
 * It is 16 bytes in size and can be passed around by value in the registers.
 */
struct LogCursor {
  LogEntryIdx idx;
  pmem::LogEntryBlock* block;

  LogCursor() = default;

  /**
   * @param idx the index of a log entry
   * @param block the block of the log entry
   * @param bitmap_mgr if not null, the bitmap manager will be used to mark the
   * bitmap entries as used
   */
  LogCursor(LogEntryIdx idx, pmem::LogEntryBlock* block,
            BitmapMgr* bitmap_mgr = nullptr)
      : idx(idx), block(block) {
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
      : LogCursor(idx,
                  &mem_table->lidx_to_addr_rw(idx.block_idx)->log_entry_block,
                  bitmap_mgr) {}

  /**
   * @param tx_entry an indirect transaction entry
   * @param mem_table the mem_table used to get the block address
   * @param bitmap_mgr if not null, the bitmap manager will be used to mark the
   * bitmap entries as used
   */
  LogCursor(pmem::TxEntryIndirect tx_entry, MemTable* mem_table,
            BitmapMgr* bitmap_mgr = nullptr)
      : LogCursor(tx_entry.get_log_entry_idx(), mem_table, bitmap_mgr) {}

  [[nodiscard]] pmem::LogEntry* get_entry() const {
    return block->get(idx.local_offset);
  }
  pmem::LogEntry* operator->() const { return get_entry(); }
  pmem::LogEntry& operator*() const { return *get_entry(); }

  /**
   * @brief Advance the cursor to the next log entry.
   *
   * @param mem_table the mem_table used to convert logical index to address
   * @param bitmap_mgr if not nullptr, the bitmap_mgr will be used to set the
   * bitmap of the block
   * @return true if the cursor is advanced to the next log entry
   */
  bool advance(MemTable* mem_table, BitmapMgr* bitmap_mgr = nullptr) {
    pmem::LogEntry* entry = get_entry();

    // check if we are at the end of the linked list
    if (!entry->has_next) return false;

    // next entry is in the same block
    if (entry->is_next_same_block) {
      idx.local_offset = entry->next.local_offset;
      return true;
    }

    // move to the next block
    idx.block_idx = entry->next.block_idx;
    idx.local_offset = 0;  // must be the first on in the next block
    block = &mem_table->lidx_to_addr_rw(idx.block_idx)->log_entry_block;
    if (bitmap_mgr) bitmap_mgr->set_allocated(idx.block_idx);
    return true;
  }

  /**
   * Update the left over bytes in the last log entry of the linked list.
   * @param mem_table the mem_table used to convert logical index to address
   * @param leftover_bytes the new leftover bytes to be updated
   */
  void update_leftover_bytes(MemTable* mem_table,
                             uint16_t leftover_bytes) const {
    LogCursor log_cursor = *this;  // make a copy
    while (log_cursor.advance(mem_table))
      ;
    log_cursor->leftover_bytes = leftover_bytes;
    log_cursor->persist_header();
  }

  /**
   * @brief get all log entry blocks linked with the given head; only called by
   * GarbageCollector
   *
   * @param log_cursor[in] the head of log entry linked list
   * @param le_blocks[out] the set to put results; can be non-empty, and
   * pre-existing elements will be untouched
   */
  void get_all_blocks(MemTable* mem_table,
                      std::unordered_set<uint32_t>& le_blocks) const {
    LogCursor log_cursor = *this;  // make a copy
    LogicalBlockIdx prev_block_idx = log_cursor.idx.block_idx;
    le_blocks.emplace(prev_block_idx.get());
    while (log_cursor.advance(mem_table)) {
      if (prev_block_idx != log_cursor.idx.block_idx) {
        prev_block_idx = log_cursor.idx.block_idx;
        le_blocks.emplace(prev_block_idx.get());
      }
    }
  }
};

static_assert(sizeof(LogCursor) == 16);

}  // namespace ulayfs::dram
