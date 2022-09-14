#pragma once

#include "alloc/block.h"
#include "tx/log_cursor.h"

namespace ulayfs::dram {

class LogEntryAllocator {
  BlockAllocator* block_allocator;
  MemTable* mem_table;

  // the current in-use log entry block
  pmem::LogEntryBlock* curr_log_block{nullptr};
  LogicalBlockIdx curr_log_block_idx{0};
  LogLocalOffset curr_log_offset{0};  // offset of the next available byte

 public:
  LogEntryAllocator(BlockAllocator* block_allocator, MemTable* mem_table)
      : block_allocator(block_allocator), mem_table(mem_table) {}

  /**
   * populate log entries required by a single transaction; do persist but not
   * fenced
   *
   * @param op operation code, e.g., LOG_OVERWRITE
   * @param leftover_bytes remaining empty bytes in the last block
   * @param num_blocks total number blocks touched
   * @param begin_vidx start of virtual index
   * @param begin_lidxs ordered list of logical indices for each chunk of
   * virtual index
   * @return a cursor pointing to the first log entry
   */
  LogCursor append(pmem::LogEntry::Op op, uint16_t leftover_bytes,
                   uint32_t num_blocks, VirtualBlockIdx begin_vidx,
                   const std::vector<LogicalBlockIdx>& begin_lidxs) {
    const LogCursor head = this->alloc(num_blocks);
    LogCursor log_cursor = head;

    // i to iterate through begin_lidxs across entries
    // j to iterate within each entry
    uint32_t i, j;
    i = 0;
    while (true) {
      log_cursor->op = op;
      log_cursor->begin_vidx = begin_vidx;
      for (j = 0; j < log_cursor->get_lidxs_len(); ++j)
        log_cursor->begin_lidxs[j] = begin_lidxs[i + j];
      if (log_cursor->has_next) {
        log_cursor->leftover_bytes = 0;
        log_cursor->persist();
        i += j;
        begin_vidx += (j << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT);
        log_cursor.advance(mem_table);
      } else {  // last entry
        log_cursor->leftover_bytes = leftover_bytes;
        log_cursor->persist();
        break;
      }
    }
    return head;
  }

 private:
  /**
   * Allocate a linked list of log entry that could fit a mapping of the given
   * length
   *
   * @param num_blocks how long this mapping should be
   * @return a log cursor pointing to the first log entry
   */
  LogCursor alloc(uint32_t num_blocks) {
    // for a log entry with only one logical block index, it takes 16 bytes
    // if smaller than that, do not try to allocate log entry there
    constexpr uint32_t min_required_size =
        pmem::LogEntry::FIXED_SIZE + sizeof(LogicalBlockIdx);
    if (curr_log_block_idx == 0 ||
        BLOCK_SIZE - curr_log_offset < min_required_size) {
      // no enough space left, do block allocation
      curr_log_block_idx = block_allocator->alloc(1);
      curr_log_block =
          &mem_table->lidx_to_addr_rw(curr_log_block_idx)->log_entry_block;
      curr_log_offset = 0;
    }

    LogEntryIdx first_idx = {curr_log_block_idx, curr_log_offset};
    pmem::LogEntryBlock* first_block = curr_log_block;
    pmem::LogEntry* first_entry = curr_log_block->get(curr_log_offset);
    pmem::LogEntry* curr_entry = first_entry;
    uint32_t needed_lidxs_cnt =
        ALIGN_UP(num_blocks, BITMAP_ENTRY_BLOCKS_CAPACITY) >>
        BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
    while (true) {
      assert(curr_entry);
      curr_log_offset += pmem::LogEntry::FIXED_SIZE;
      uint32_t avail_lidxs_cnt =
          (BLOCK_SIZE - curr_log_offset) / sizeof(LogicalBlockIdx);
      assert(avail_lidxs_cnt > 0);
      if (needed_lidxs_cnt <= avail_lidxs_cnt) {
        curr_entry->has_next = false;
        curr_entry->num_blocks = num_blocks;
        curr_log_offset += needed_lidxs_cnt * sizeof(LogicalBlockIdx);
        return {first_idx, first_block};
      }

      curr_entry->has_next = true;
      curr_entry->num_blocks = avail_lidxs_cnt
                               << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
      curr_log_offset += avail_lidxs_cnt * sizeof(LogicalBlockIdx);
      needed_lidxs_cnt -= avail_lidxs_cnt;
      num_blocks -= curr_entry->num_blocks;

      assert(curr_log_offset <= BLOCK_SIZE);
      if (BLOCK_SIZE - curr_log_offset < min_required_size) {
        curr_log_block_idx = block_allocator->alloc(1);
        curr_log_block =
            &mem_table->lidx_to_addr_rw(curr_log_block_idx)->log_entry_block;
        curr_log_offset = 0;
        curr_entry->is_next_same_block = false;
        curr_entry->next.block_idx = curr_log_block_idx;
      } else {
        curr_entry->is_next_same_block = true;
        curr_entry->next.local_offset = curr_log_offset;
      }
      curr_entry = curr_log_block->get(curr_log_offset);
    }
  }

 public:
  /**
   * a log entry is discarded because reset_log_entry() is called before commit;
   * the uncommitted entry must be (semi-)freed to prevent memory leak
   *
   * return log entry blocks that are exclusively taken by this log entry; if
   * there is other log entries on this block, leave this block alone
   */
  void free(const LogCursor& log_cursor) {
    pmem::LogEntry* curr_entry = log_cursor.get_entry();
    pmem::LogEntryBlock* curr_block = log_cursor.block;
    LogicalBlockIdx curr_block_idx = log_cursor.idx.block_idx;

    // NOTE: we assume free() will always keep the block in the local free list
    //       instead of publishing it immediately. this makes it safe to read
    //       the log entry block even after calling free()

    // do we need to free the first le block? no if there is other le ahead
    if (log_cursor.idx.local_offset == 0) block_allocator->free(curr_block_idx);

    while (curr_entry->has_next) {
      if (curr_entry->is_next_same_block) {
        curr_entry = curr_block->get(curr_entry->next.local_offset);
      } else {
        curr_block_idx = curr_entry->next.block_idx;
        curr_block =
            &mem_table->lidx_to_addr_rw(curr_block_idx)->log_entry_block;
        curr_entry = curr_block->get(0);
        block_allocator->free(curr_block_idx);
      }
    }
  }

  /**
   * when moving into a new tx block, reset states associated with log entry
   * allocation so that the next time calling alloc_log_entry will allocate from
   * a new log entry block
   */
  void reset() {
    // this is to trigger new log entry block allocation
    curr_log_block_idx = 0;
    // technically, setting curr_log_block and curr_log_offset are unnecessary
    // because they are guarded by setting curr_log_block_idx zero
  }
};
}  // namespace ulayfs::dram
