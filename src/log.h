#pragma once

#include <ostream>

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
      : meta(meta), allocator(allocator), mem_table(mem_table),
        log_blocks(), curr_block(nullptr), free_local_idx(NUM_LOG_ENTRY) {}

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
                    std::vector<
                        LogicalBlockIdx>* begin_logical_idxs = nullptr) {
    LogEntryIdx idx = first_head_idx;
    const pmem::LogHeadEntry* head_entry = get_head_entry(first_head_idx);

    // a head entry at the last slot of a LogBlock could have 0 body entries
    if (head_entry->num_blocks == 0) {
      PANIC_IF(!head_entry->overflow, "should have overflow segment");
      idx = LogEntryIdx{head_entry->next.next_block_idx, 0};
      head_entry = get_head_entry(idx);
    }

    num_blocks = 0;
    while (head_entry != nullptr) {

      if (num_blocks == 0) {
        idx.local_idx++;
        const pmem::LogBodyEntry* body_entry = get_body_entry(idx);
        begin_virtual_idx = body_entry->begin_virtual_idx;
      }

      // now idx points to a body entry
      if (need_logical_idxs) {
        PANIC_IF(begin_logical_idxs == nullptr, "begin_logical_idxs is null");
        uint32_t segment_blocks = 0;
        while (segment_blocks < head_entry->num_blocks) {
          const pmem::LogBodyEntry* body_entry = get_body_entry(idx);
          begin_logical_idxs->push_back(body_entry->begin_logical_idx);
          segment_blocks += MAX_BLOCKS_PER_BODY;
          idx.local_idx++;
        }
      }
      num_blocks += head_entry->num_blocks;

      if (head_entry->overflow) {
        idx = LogEntryIdx{head_entry->next.next_block_idx, 0};
        head_entry = get_head_entry(idx);
      } else
        head_entry = nullptr;
    }
  }

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
                     bool fenced = true) {
    pmem::LogHeadEntry* head_entry = alloc_head_entry();
    LogEntryIdx first_head_idx =
        LogEntryIdx{log_blocks.back(), LogLocalIdx(free_local_idx - 1)};
    VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
    size_t now_logical_idx_off = 0;

    while (head_entry != nullptr) {
      LogLocalIdx persist_start_idx = LogLocalIdx(free_local_idx - 1);
      head_entry->op = op;

      uint32_t num_blocks = total_blocks;
      uint32_t max_blocks = num_free_entries() * MAX_BLOCKS_PER_BODY;
      if (num_blocks > max_blocks) {
        num_blocks = max_blocks;
        head_entry->overflow = true;
        head_entry->saturate = true;
      } else if (num_blocks > max_blocks - MAX_BLOCKS_PER_BODY) {
        head_entry->saturate = true;
        head_entry->leftover_bytes = leftover_bytes;
      }

      head_entry->num_blocks = num_blocks;
      total_blocks -= num_blocks;

      // populate body entries until done or until current LogBlock filled up
      while (num_blocks > 0) {
        pmem::LogBodyEntry* body_entry = alloc_body_entry();
        PANIC_IF(now_logical_idx_off >= begin_logical_idxs.size(),
                 "begin_logical_idxs vector not long enough");
        body_entry->begin_virtual_idx = now_virtual_idx;
        body_entry->begin_logical_idx =
            begin_logical_idxs[now_logical_idx_off++];
        now_virtual_idx += MAX_BLOCKS_PER_BODY;
        num_blocks = num_blocks <= MAX_BLOCKS_PER_BODY ? 0
                     : num_blocks - MAX_BLOCKS_PER_BODY;
      }

      curr_block->persist(persist_start_idx, free_local_idx, fenced);
      if (head_entry->overflow)
        head_entry = alloc_head_entry(head_entry);
      else
        head_entry = nullptr;
    }

    return first_head_idx;
  }

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
      if (prev_head_entry)
        prev_head_entry->next.next_block_idx = idx;
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

  pmem::LogBodyEntry* alloc_body_entry() {
    return &alloc_entry()->body_entry;
  }

  /**
   * get the number of free entries in the current LogBlock
   */
  [[nodiscard]] uint16_t num_free_entries() {
    return NUM_LOG_ENTRY - free_local_idx;
  }
};
}  // namespace ulayfs::dram
