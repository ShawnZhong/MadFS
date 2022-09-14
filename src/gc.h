#pragma once

#include "file.h"
#include "idx.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::utility {

/**
 * Garbage collecting transaction blocks and log blocks. This function builds
 * a new transaction history from block table and uses it to replace the old
 * transaction history. We assume that a dedicated single-threaded process
 * will run this function so it is safe to directly access blk_table.
 **/
class GarbageCollector {
 public:
  const std::unique_ptr<dram::File> file;
  const uint64_t file_size;
  const dram::TxCursor old_tail;
  const dram::TxCursor old_head;
  dram::Allocator* allocator;

  explicit GarbageCollector(const char* pathname)
      : file([&]() {
          int fd;
          struct stat stat_buf;
          bool success =
              dram::File::try_open(fd, stat_buf, pathname, O_RDWR, 0);
          if (!success) {
            PANIC("Fail to open file \"%s\"", pathname);
          }

          return std::make_unique<dram::File>(fd, stat_buf, O_RDWR, pathname);
        }()),
        file_size(file->blk_table.get_file_state().file_size),
        old_tail(file->blk_table.get_file_state().cursor),
        old_head(dram::TxCursor::from_idx(file->meta->get_next_tx_block(),
                                          &file->mem_table)),
        allocator(file->get_local_allocator()) {
    // we don't care the per-thread data for the gc thread
    allocator->tx_block.reset_per_thread_data();
  }

  [[nodiscard]] dram::File* get_file() const { return file.get(); }

  void do_gc() const {
    LOG_INFO("GarbageCollector: start transaction & log gc");

    if (!need_gc()) {
      LOG_INFO("GarbageCollector: no need to gc");
      return;
    }

    bool success = create_new_linked_list();
    if (!success) {
      return;
    }

    recycle();

    LOG_INFO("GarbageCollector: done");
  }

  [[nodiscard]] bool need_gc() const {
    LOG_DEBUG("GarbageCollector: old_tail=%d", old_tail.idx.block_idx.get());

    // skip if tail_tx_block is meta block
    if (old_tail.idx.block_idx == 0) return false;

    LogicalBlockIdx first_tx_idx = file->meta->get_next_tx_block();

    // skip if the tail directly follows meta
    if (first_tx_idx == old_tail.idx.block_idx) return false;

    LogicalBlockIdx tx_block_idx = first_tx_idx;
    const pmem::TxBlock* tx_block =
        &file->mem_table.lidx_to_addr_ro(tx_block_idx)->tx_block;

    // skip if there is only one tx block between meta and tail, because at best
    // we can only make this single block into one block
    if (tx_block->get_next_tx_block() == old_tail.idx.block_idx) return false;

    // skip if there is no tx block with gc_seq == 0 before the tail, which
    // means there is no work can be done since last gc
    while ((tx_block_idx = tx_block->get_next_tx_block()) !=
           old_tail.idx.block_idx)
      tx_block = &file->mem_table.lidx_to_addr_ro(tx_block_idx)->tx_block;
    // if gc_seq is zero, this means there is at least one new full tx block
    // since last gc
    return tx_block->get_gc_seq() == 0;
  }

  [[nodiscard]] bool create_new_linked_list() const {
    LogicalBlockIdx old_first_tx_block_idx = file->meta->get_next_tx_block();
    assert(old_first_tx_block_idx != 0);
    uint32_t gc_seq = file->mem_table.lidx_to_addr_ro(old_first_tx_block_idx)
                          ->tx_block.get_gc_seq() +
                      1;
    uint32_t tx_seq = 1;
    auto first_tx_block_idx = allocator->block.alloc(1);
    auto new_block = file->mem_table.lidx_to_addr_rw(first_tx_block_idx);
    memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
    new_block->tx_block.set_tx_seq(tx_seq++, gc_seq);
    dram::TxCursor new_cursor({first_tx_block_idx, 0}, &new_block->tx_block);

    VirtualBlockIdx begin = 0;
    VirtualBlockIdx i = 1;

    // create new linked list from the block table
    {
      TimerGuard<Event::GC_CREATE> timer_guard;

      auto num_blocks = BLOCK_SIZE_TO_IDX(ALIGN_UP(file_size, BLOCK_SIZE));
      for (; i < num_blocks; i++) {
        auto curr_blk_idx = file->vidx_to_lidx(i);
        auto prev_blk_idx = file->vidx_to_lidx(i - 1);
        if (curr_blk_idx == 0) continue;
        // continuous blocks can be placed in 1 tx
        if (curr_blk_idx - prev_blk_idx == 1 &&
            i - begin < pmem::TxEntryInline::NUM_BLOCKS_MAX)
          continue;
        auto entry =
            pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
        new_cursor.block->store(entry, new_cursor.idx.local_idx);
        if (bool success = new_cursor.advance(&file->mem_table); !success) {
          // current block is full, flush it and allocate a new block
          auto new_tx_block_idx = allocator->block.alloc(1);
          new_cursor.block->try_set_next_tx_block(new_tx_block_idx);
          pmem::persist_unfenced(new_cursor.block, BLOCK_SIZE);
          new_block = file->mem_table.lidx_to_addr_rw(new_tx_block_idx);
          memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0,
                 CACHELINE_SIZE);
          new_block->tx_block.set_tx_seq(tx_seq++, gc_seq);
          new_cursor.block = &new_block->tx_block;
          new_cursor.idx = {new_tx_block_idx, 0};
        }
        begin = i;
      }
    }

    // add the last commit entry
    {
      auto leftover_bytes = ALIGN_UP(file_size, BLOCK_SIZE) - file_size;
      if (leftover_bytes == 0) {
        auto commit_entry =
            pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
        new_cursor.block->store(commit_entry, new_cursor.idx.local_idx);
      } else {
        // since i - begin <= 63, this can fit into one log entry
        auto begin_lidx = std::vector{file->vidx_to_lidx(begin)};
        dram::LogCursor log_cursor = file->tx_mgr.append_log_entry(
            allocator, pmem::LogEntry::Op::LOG_OVERWRITE, leftover_bytes,
            i - begin, begin, begin_lidx);
        auto commit_entry = pmem::TxEntryIndirect(log_cursor.idx);
        new_cursor.block->store(commit_entry, new_cursor.idx.local_idx);
      }
      pmem::persist_unfenced(new_cursor.block, BLOCK_SIZE);
    }

    // pad the last block with dummy tx entries
    while (!new_cursor.advance(&file->mem_table))
      new_cursor.block->store(pmem::TxEntry::TxEntryDummy,
                              new_cursor.idx.local_idx);
    // last block points to the tail, meta points to the first block
    new_cursor.block->try_set_next_tx_block(old_tail.idx.block_idx);
    pmem::persist_unfenced(new_cursor.block, BLOCK_SIZE);
    // abort if new transaction history is longer than the old one
    auto tail_block = file->mem_table.lidx_to_addr_rw(old_tail.idx.block_idx);
    if (tail_block->tx_block.get_tx_seq() <= new_cursor.block->get_tx_seq()) {
      // abort, free the new tx blocks
      auto new_tx_blk_idx = first_tx_block_idx;
      do {
        auto next_tx_blk_idx = new_cursor.block->get_next_tx_block();
        allocator->block.free(new_tx_blk_idx, 1);
        new_tx_blk_idx = next_tx_blk_idx;
      } while (new_tx_blk_idx != old_tail.idx.block_idx && new_tx_blk_idx != 0);
      allocator->block.return_free_list();
      LOG_WARN("GarbageCollector: new tx history is longer than the old one");
      return false;
    }
    pmem::persist_fenced(new_cursor.block, BLOCK_SIZE);
    file->meta->set_next_tx_block(first_tx_block_idx);

    // invalidate tx in meta block, so we can free the log blocks they point to
    file->meta->invalidate_tx_entries();
    return true;
  }

  [[nodiscard]] LogicalBlockIdx get_smallest_tx_idx() const {
    LogicalBlockIdx smallest_tx_idx =
        std::numeric_limits<LogicalBlockIdx::numeric_type>::max();
    for (size_t i = 0; i < MAX_NUM_THREADS; ++i) {
      auto per_thread_data = file->shm_mgr.get_per_thread_data(i);
      if (!per_thread_data->is_data_valid()) continue;
      LogicalBlockIdx curr_tx_idx = per_thread_data->get_tx_block_idx();
      if (curr_tx_idx < smallest_tx_idx) smallest_tx_idx = curr_tx_idx;
    }
    return smallest_tx_idx;
  }

  void recycle() const {
    LogicalBlockIdx idx = get_smallest_tx_idx();
    LOG_DEBUG("GarbageCollector: smallest tx idx: %d", idx.get());
    // blocks before the pivot can be immediately recycled, blocks after the
    // pivot needs to be recycled later.
    const dram::TxCursor pivot =
        dram::TxCursor::from_idx(idx, &file->mem_table);

    dram::TxCursor curr = old_head;
    while (curr < pivot) {  // free up to the pivot
      dram::TxCursor prev = curr;
      // we first advance and then free the previous block
      bool success = curr.advance_to_next_block(&file->mem_table);
      assert(success);
      allocator->block.free(prev.idx.block_idx);
      LOG_DEBUG("GarbageCollector: freed block %d", prev.idx.block_idx.get());
    }

    while (curr.idx.block_idx < old_tail.idx.block_idx) {
      LOG_DEBUG("GarbageCollector: block %d cannot be recycled now",
                curr.idx.block_idx.get());
      if (!curr.advance_to_next_block(&file->mem_table)) break;
    }

    // TODO: scan shared memory to get a snapshot of which tx block every thread
    // has pinned and
    // 1) if a tx block is neither pinned nor linked after a pinned tx block,
    //    release this block immediately by marking the bitmap
    // 2) otherwise, link this block into an outdated block linked list
    //
    // Also scan the outdated tx block linked list to free blocks with the above
    // rules
  }
};

}  // namespace ulayfs::utility
