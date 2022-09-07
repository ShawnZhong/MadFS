#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "bitmap.h"
#include "block/block.h"
#include "config.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "mem_table.h"
#include "posix.h"
#include "shm.h"

namespace ulayfs::dram {
// per-thread data structure
class Allocator {
  MemTable* mem_table;
  BitmapMgr* bitmap_mgr;
  ShmMgr* shm_mgr;

  // free_lists[n-1] means a free list of size n beginning from LogicalBlockIdx
  std::array<std::vector<LogicalBlockIdx>, BITMAP_ENTRY_BLOCKS_CAPACITY>
      free_lists;

  // used as a hint for search; recent is defined to be "the next one to search"
  // keep id for idx translation
  // TODO: this may be useful for dynamically growing bitmap
  // BitmapBlockId recent_bitmap_block_id;
  // NOTE: this is the index within recent_bitmap_block
  BitmapIdx recent_bitmap_idx{};

  // the current in-use log entry block
  pmem::LogEntryBlock* curr_log_block{nullptr};
  LogicalBlockIdx curr_log_block_idx{0};
  LogLocalOffset curr_log_offset{0};  // offset of the next available byte

  // a tx block may be allocated but unused when another thread does that first
  // this tx block will then be saved here for future use
  // a tx block candidate must have all bytes zero except the sequence number
  pmem::Block* avail_tx_block{nullptr};
  LogicalBlockIdx avail_tx_block_idx{0};
  size_t shm_thread_idx;

  // each thread will pin a tx block so that the garbage collector will not
  // reclaim this block and blocks after it
  LogicalBlockIdx pinned_tx_block_idx{0};
  LogicalBlockIdx* notify_addr{nullptr};
  // TODO: a pointer to a shared memory location to publish this value

 public:
  Allocator(MemTable* mem_table, BitmapMgr* bitmap_mgr, ShmMgr* shm_mgr,
            size_t shm_thread_idx)
      : mem_table(mem_table),
        bitmap_mgr(bitmap_mgr),
        shm_mgr(shm_mgr),
        shm_thread_idx(shm_thread_idx) {}

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

  /**
   * Return all the blocks in the free list to the bitmap
   */
  void return_free_list() {
    for (uint32_t n = 0; n < BITMAP_ENTRY_BLOCKS_CAPACITY; ++n)
      for (LogicalBlockIdx lidx : free_lists[n])
        bitmap_mgr->free(static_cast<BitmapIdx>(lidx.get()), n + 1);
  }

  /**
   * Allocate a linked list of log entry that could fit a mapping of the given
   * length
   *
   * @param num_blocks how long this mapping should be
   * @return a tuple containing
   *    1. the head of the linked list,
   *    2. the log entry index of the head of the linked list, and
   *    3. the log entry block where the head locates
   */
  std::tuple<pmem::LogEntry*, LogEntryIdx, pmem::LogEntryBlock*>
  alloc_log_entry(uint32_t num_blocks);

  /**
   * a log entry is discarded because reset_log_entry() is called before commit;
   * the uncommitted entry must be (semi-)freed to prevent memory leak
   */
  void free_log_entry(pmem::LogEntry* first_entry, LogEntryIdx first_idx,
                      pmem::LogEntryBlock* first_block);

  /**
   * when moving into a new tx block, reset states associated with log entry
   * allocation so that the next time calling alloc_log_entry will allocate from
   * a new log entry block
   */
  void reset_log_entry();

  [[nodiscard]] LogicalBlockIdx get_pinned_tx_block_idx() const {
    return pinned_tx_block_idx;
  }

  /**
   * Update the shared memory to notify this thread has moved to a new tx block;
   * do nothing if there is no moving
   * @param tx_block_idx the index of the currently referenced tx block; zero if
   * this is the first time this thread try to pin a tx block. if this is the
   * first time this thread tries to pin, it must acquire a slot on the shared
   * memory and then publish a zero. this notify gc threads that there is a
   * thread performing the initial log replaying and do not reclaim any blocks.
   */
  void pin_tx_block(LogicalBlockIdx tx_block_idx) {
    if (!notify_addr) {
      assert(tx_block_idx == 0);
      // TODO: allocate from shared memory!
      return;
    }
    if (pinned_tx_block_idx == tx_block_idx) return;
    pinned_tx_block_idx = tx_block_idx;
    // TODO: uncomment this line after setting shared memory
    // *notify_addr = tx_block_idx;
  }

  /**
   * @tparam B MetaBlock or TxBlock
   * @param block the block that needs a next block to be allocated
   * @return the block id of the allocated block and the new tx block allocated
   */
  template <class B>
  std::tuple<LogicalBlockIdx, pmem::TxBlock*> alloc_next_tx_block(B* block) {
    auto [new_block_idx, new_block] = alloc_tx_block(block->get_tx_seq() + 1);

    bool success = block->try_set_next_tx_block(new_block_idx);
    if (!success) {  // race condition for adding the new blocks
      free_tx_block(new_block_idx, new_block);
      new_block_idx = block->get_next_tx_block();
      new_block = &mem_table->lidx_to_addr_rw(new_block_idx)->tx_block;
    }

    shm_mgr->get_per_thread_data(shm_thread_idx)->tx_block_idx =
        new_block_idx.get();

    return {new_block_idx, new_block};
  }

 private:
  /**
   * Allocate a tx block
   * @param tx_seq the tx sequence number of the tx block (gc_seq = 0)
   * @return a tuple of the block index and the block address
   */
  std::tuple<LogicalBlockIdx, pmem::TxBlock*> alloc_tx_block(uint32_t tx_seq);

  /**
   * Free a tx block
   * @param tx_block_idx the index of the tx block
   * @param tx_block the address of the tx block to free
   */
  void free_tx_block(LogicalBlockIdx tx_block_idx, pmem::TxBlock* tx_block);
};

}  // namespace ulayfs::dram
