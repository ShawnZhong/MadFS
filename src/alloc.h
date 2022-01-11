#pragma once

#include <array>
#include <stdexcept>
#include <vector>

#include "block.h"
#include "config.h"
#include "const.h"
#include "idx.h"
#include "mtable.h"
#include "posix.h"

namespace ulayfs::dram {
class File;

// per-thread data structure
class Allocator {
  File* file;
  Bitmap* bitmap;

  // free_lists[n-1] means a free list of size n beginning from LogicalBlockIdx
  std::array<std::vector<LogicalBlockIdx>, BITMAP_CAPACITY> free_lists;

  // used as a hint for search; recent is defined to be "the next one to search"
  // keep id for idx translation
  // TODO: this may be useful for dynamically growing bitmap
  // BitmapBlockId recent_bitmap_block_id;
  // NOTE: this is the index within recent_bitmap_block
  BitmapIdx recent_bitmap_idx;

  // blocks for storing log entries, max 512 entries per block
  std::vector<LogicalBlockIdx> log_blocks;
  // pointer to current LogBlock == the one identified by log_blocks.back()
  pmem::LogEntryBlock* curr_log_block;
  // local index of the first free entry slot in the last block
  // might equal NUM_LOCAL_ENTREIS when a new log block is not allocated yet
  LogLocalUnpackIdx free_log_local_idx;

 public:
  Allocator(File* file, Bitmap* bitmap)
      : file(file),
        bitmap(bitmap),
        recent_bitmap_idx(),
        log_blocks(),
        curr_log_block(nullptr),
        free_log_local_idx(NUM_LOG_ENTRY) {}

  ~Allocator() { return_free_list(); }

  /**
   * allocate contiguous blocks (num_blocks must <= 64)
   * if large number of blocks required, please break it into multiple alloc and
   * use log entries to chain them together
   *
   * @param num_blocks number of blocks to allocate
   * @return the logical block id of the first block
   */
  [[nodiscard]] LogicalBlockIdx alloc(uint32_t num_blocks);

  /**
   * Free the blocks in the range [block_idx, block_idx + num_blocks)
   */
  void free(LogicalBlockIdx block_idx, uint32_t num_blocks = 1);

  /**
   * Free an array of blocks, but the logical block indexes are not necessary
   * continuous
   */
  void free(const LogicalBlockIdx recycle_image[], uint32_t image_size);

  void return_free_list() {
    for (uint32_t n = 1; n < BITMAP_CAPACITY; ++n)
      for (LogicalBlockIdx lidx : free_lists[n]) Bitmap::free(bitmap, lidx, n);
  }
  /*
   * LogEntry allocations
   */

  /**
   * allocate a log entry, possibly triggering allocating a new LogBlock
   */
  pmem::LogEntry* alloc_log_entry(
      bool pack_align = false, pmem::LogHeadEntry* prev_head_entry = nullptr);

  // syntax sugar for union dispatching
  pmem::LogHeadEntry* alloc_head_entry(
      pmem::LogHeadEntry* prev_head_entry = nullptr) {
    return &alloc_log_entry(/*pack_align*/ true, prev_head_entry)->head_entry;
  }

  pmem::LogBodyEntry* alloc_body_entry() {
    return &alloc_log_entry()->body_entry;
  }

  /**
   * get the number of free entries in the current LogBlock
   */
  [[nodiscard]] uint16_t num_free_log_entries() const {
    return NUM_LOG_ENTRY - free_log_local_idx;
  }

  /**
   * get the last allocated entry's local index
   */
  [[nodiscard]] LogLocalUnpackIdx last_log_local_idx() const {
    return free_log_local_idx - 1;
  }

  [[nodiscard]] pmem::LogEntryBlock* get_curr_log_block() const {
    return curr_log_block;
  }

  [[nodiscard]] LogEntryIdx get_first_head_idx() {
    return {log_blocks.back(),
            static_cast<LogLocalIdx>(last_log_local_idx() >> 1)};
  }
};

}  // namespace ulayfs::dram
