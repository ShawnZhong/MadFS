#pragma once

#include <ostream>
#include <vector>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "idx.h"
#include "layout.h"
#include "mtable.h"

namespace ulayfs::dram {

class LogMgr {
 private:
  pmem::MetaBlock* meta;

  Allocator* allocator;
  MemTable* mem_table;

  // blocks for storing log entries, max 512 entries per block
  std::vector<LogicalBlockIdx> log_blocks;
  // pointer to current LogBlock == the one identified by log_blocks.back()
  pmem::LogEntryBlock* curr_block;
  // local index of the first free entry slot in the last block
  // might equal NUM_LOCAL_ENTREIS when a new log block is not allocated yet
  LogLocalIdx free_local_idx;

 public:
  LogMgr() = default;
  LogMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table)
      : meta(meta),
        allocator(allocator),
        mem_table(mem_table),
        log_blocks(),
        curr_block(nullptr),
        free_local_idx(NUM_LOG_ENTRY) {}

  const pmem::LogEntry* get_entry(LogEntryIdx idx) {
    return mem_table->get(idx.block_idx)->log_entry_block.get(idx.local_idx);
  }

  // syntax sugar for union dispatching
  const pmem::LogBodyEntry* get_body_entry(LogEntryIdx idx) {
    return &get_entry(idx)->body_entry;
  }

  const pmem::LogHeadEntry* get_head_entry(LogEntryIdx idx) {
    return &get_entry(idx)->head_entry;
  }

  // TODO: return op and leftover_bytes
  // TODO: handle writev requests
  /**
   * get total coverage of the group of log entries starting at the head at idx
   *
   * @param first_head_idx LogEntryIdx of the first head entry
   * @param need_logical_idxs if true, returns a list of corresponding
   *                          logical indices; pass true when applying the
   *                          transaction, and pass false when checking OCC
   * @param[out] begin_virtual_idx begin_virtual_idx of the coverage
   * @param[out] num_blocks number of blocks in the coverage
   * @param[out] begin_logical_idxs pointer to vector of logical indices
   */
  void get_coverage(LogEntryIdx first_head_idx, bool need_logical_idxs,
                    VirtualBlockIdx& begin_virtual_idx, uint32_t& num_blocks,
                    std::vector<LogicalBlockIdx>* begin_logical_idxs = nullptr);

  // TODO: handle writev requests
  /**
   * populate log entries required by a single transaction
   *
   * @param op operation code, e.g., LOG_OVERWRITE
   * @param leftover_bytes remaining empty bytes in the last block
   * @param total_blocks total number blocks touched
   * @param begin_virtual_idx start of virtual index
   * @param begin_logical_idxs ordered list of logical indices for each chunk
   *                           of virtual index
   * @param fenced whether to force memory fencing of log block
   * @return index of the first LogHeadEntry for later retrival of the whole
   *         group of entries
   */
  LogEntryIdx append(pmem::LogOp op, uint16_t leftover_bytes,
                     uint32_t total_blocks, VirtualBlockIdx begin_virtual_idx,
                     const std::vector<LogicalBlockIdx>& begin_logical_idxs,
                     bool fenced = true);

 private:
  /**
   * allocate a log entry, possibly triggering allocating a new LogBlock
   */
  pmem::LogEntry* alloc_entry(pmem::LogHeadEntry* prev_head_entry = nullptr) {
    if (free_local_idx == NUM_LOG_ENTRY) {
      LogicalBlockIdx idx = allocator->alloc(1);
      log_blocks.push_back(idx);
      curr_block = &mem_table->get(idx)->log_entry_block;
      free_local_idx = 0;
      if (prev_head_entry) prev_head_entry->next.next_block_idx = idx;
    } else {
      if (prev_head_entry)
        prev_head_entry->next.next_local_idx = free_local_idx;
    }

    PANIC_IF(curr_block == nullptr, "curr_block is null");
    pmem::LogEntry* entry = curr_block->get(free_local_idx);
    free_local_idx++;
    return entry;
  }

  // syntax sugar for union dispatching
  pmem::LogHeadEntry* alloc_head_entry(
      pmem::LogHeadEntry* prev_head_entry = nullptr) {
    return &alloc_entry(prev_head_entry)->head_entry;
  }

  pmem::LogBodyEntry* alloc_body_entry() { return &alloc_entry()->body_entry; }

  /**
   * get the number of free entries in the current LogBlock
   */
  [[nodiscard]] uint16_t num_free_entries() {
    return NUM_LOG_ENTRY - free_local_idx;
  }
};
}  // namespace ulayfs::dram
