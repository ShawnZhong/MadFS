#pragma once

#include <array>
#include <stdexcept>
#include <vector>

#include "block.h"
#include "config.h"
#include "const.h"
#include "entry.h"
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

  // the current in-use log entry block
  pmem::LogEntryBlock* curr_log_block;
  LogicalBlockIdx curr_log_block_idx;
  LogLocalOffset curr_log_offset;  // offset of the next available byte

  // a tx block may be allocated but unused when another thread does that first
  // this tx block will then be saved here for future use
  // a tx block candidate must have all bytes zero except the sequence number
  pmem::Block* avail_tx_block;
  LogicalBlockIdx avail_tx_block_idx;

 public:
  Allocator(File* file, Bitmap* bitmap)
      : file(file),
        bitmap(bitmap),
        recent_bitmap_idx(),
        curr_log_block(nullptr),
        curr_log_block_idx(0),
        curr_log_offset(0),
        avail_tx_block(nullptr),
        avail_tx_block_idx(0) {}

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
  void free(const std::vector<LogicalBlockIdx>& recycle_image);

  void return_free_list() {
    for (uint32_t n = 1; n < BITMAP_CAPACITY; ++n)
      for (LogicalBlockIdx lidx : free_lists[n])
        Bitmap::free(bitmap, static_cast<BitmapIdx>(lidx.get()), n);
  }

  /**
   * Allocate a linked list of log entry that could fit a mapping of the given
   * length
   *
   * @param[in] num_blocks how long this mapping should be
   * @param[out] first_idx the log entry index of the head of the linked list
   * @param[out] first_block the log entry block where the head locates
   * @return the head of the linked list
   */
  pmem::LogEntry* alloc_log_entry(uint32_t num_blocks, LogEntryIdx& first_idx,
                                  pmem::LogEntryBlock*& first_block);

  LogicalBlockIdx alloc_tx_block(uint32_t seq, pmem::Block*& tx_block);

  void free_tx_block(LogicalBlockIdx tx_block_idx, pmem::Block* tx_block);
};

}  // namespace ulayfs::dram
