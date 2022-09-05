#include "file.h"
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
  std::unique_ptr<dram::File> file;

  explicit GarbageCollector(dram::File* file) : file(file) {}
  explicit GarbageCollector(const char* pathname) {
    int fd;
    struct stat stat_buf;
    bool success = dram::File::try_open(fd, stat_buf, pathname, O_RDWR, 0);
    if (!success) {
      PANIC("Fail to open file \"%s\"", pathname);
    }

    file = std::make_unique<dram::File>(fd, stat_buf, O_RDWR, pathname,
                                        /*guard*/ false);
  }

  [[nodiscard]] dram::File* get_file() const { return file.get(); }

  [[nodiscard]] size_t get_smallest_tx_idx() const {
    size_t smallest_tx_idx = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < MAX_NUM_THREADS; ++i) {
      auto per_thread_data = file->shm_mgr.get_per_thread_data(i);
      if (per_thread_data->is_empty()) continue;
      size_t curr_tx_idx = per_thread_data->tx_block_idx;
      if (curr_tx_idx < smallest_tx_idx) smallest_tx_idx = curr_tx_idx;
    }
    return smallest_tx_idx;
  }

  void gc() const {
    LOG_INFO("GarbageCollector: start transaction & log gc");
    uint64_t file_size = file->blk_table.update();
    TxEntryIdx tail_tx_idx = file->blk_table.get_tx_idx();

    // skip if tail_tx_block is meta block
    if (tail_tx_idx.block_idx == 0) return;

    LogicalBlockIdx orig_first_tx_block_idx = file->meta->get_next_tx_block();

    // skip if the tail directly follows meta
    if (orig_first_tx_block_idx == tail_tx_idx.block_idx) return;

    // skip if there is only one  tx block between meta and tail
    if (file->mem_table.lidx_to_addr_ro(orig_first_tx_block_idx)
            ->tx_block.get_next_tx_block() == orig_first_tx_block_idx)
      return;

    LogicalBlockIdx tail_tx_block_idx = tail_tx_idx.block_idx;
    auto allocator = file->get_local_allocator();
    auto tail_block = file->mem_table.lidx_to_addr_rw(tail_tx_block_idx);
    auto num_blocks = BLOCK_SIZE_TO_IDX(ALIGN_UP(file_size, BLOCK_SIZE));
    auto leftover_bytes = ALIGN_UP(file_size, BLOCK_SIZE) - file_size;

    uint32_t tx_seq = 1;
    auto first_tx_block_idx = allocator->alloc(1);
    auto new_block = file->mem_table.lidx_to_addr_rw(first_tx_block_idx);
    memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
    new_block->tx_block.set_tx_seq(tx_seq++);
    dram::TxCursor cursor({first_tx_block_idx, 0}, &new_block->tx_block);

    VirtualBlockIdx begin = 0;
    VirtualBlockIdx i = 1;
    for (; i < num_blocks; i++) {
      auto curr_blk_idx = file->vidx_to_lidx(i);
      auto prev_blk_idx = file->vidx_to_lidx(i - 1);
      if (curr_blk_idx == 0) break;
      // continuous blocks can be placed in 1 tx
      if (curr_blk_idx - prev_blk_idx == 1 &&
          i - begin <= pmem::TxEntryInline::NUM_BLOCKS_MAX)
        continue;

      auto commit_entry =
          pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
      cursor.block->store(commit_entry, cursor.idx.local_idx);
      if (bool success = cursor.advance(&file->mem_table); !success) {
        // current block is full, flush it and allocate a new block
        auto new_tx_block_idx = allocator->alloc(1);
        cursor.block->try_set_next_tx_block(new_tx_block_idx);
        pmem::persist_unfenced(cursor.block, BLOCK_SIZE);
        new_block = file->mem_table.lidx_to_addr_rw(new_tx_block_idx);
        memset(&new_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0,
               CACHELINE_SIZE);
        new_block->tx_block.set_tx_seq(tx_seq++);
        cursor.block = &new_block->tx_block;
        cursor.idx = {new_tx_block_idx, 0};
      }
      begin = i;
    }

    // add the last commit entry
    {
      if (leftover_bytes == 0) {
        auto commit_entry =
            pmem::TxEntryInline(i - begin, begin, file->vidx_to_lidx(begin));
        cursor.block->store(commit_entry, cursor.idx.local_idx);
      } else {
        // since i - begin <= 63, this can fit into one log entry
        auto begin_lidx = std::vector{file->vidx_to_lidx(begin)};
        auto log_head_idx = file->tx_mgr.append_log_entry(
            allocator, pmem::LogEntry::Op::LOG_OVERWRITE, leftover_bytes,
            i - begin, begin, begin_lidx);
        auto commit_entry = pmem::TxEntryIndirect(log_head_idx);
        cursor.block->store(commit_entry, cursor.idx.local_idx);
      }
    }
    // pad the last block with dummy tx entries
    while (!cursor.advance(&file->mem_table))
      cursor.block->store(pmem::TxEntry::TxEntryDummy, cursor.idx.local_idx);
    // last block points to the tail, meta points to the first block
    cursor.block->try_set_next_tx_block(tail_tx_block_idx);
    // abort if new transaction history is longer than the old one
    if (tail_block->tx_block.get_tx_seq() <= cursor.block->get_tx_seq())
      goto abort;
    pmem::persist_fenced(cursor.block, BLOCK_SIZE);
    file->meta->set_next_tx_block(first_tx_block_idx);

    // invalidate tx in meta block so we can free the log blocks they point to
    file->meta->invalidate_tx_entries();

    LOG_INFO("GarbageCollector: done");
    return;

  abort:
    // free the new tx blocks
    auto new_tx_blk_idx = first_tx_block_idx;
    do {
      auto next_tx_blk_idx = cursor.block->get_next_tx_block();
      allocator->free(new_tx_blk_idx, 1);
      new_tx_blk_idx = next_tx_blk_idx;
    } while (new_tx_blk_idx != tail_tx_block_idx && new_tx_blk_idx != 0);
    allocator->return_free_list();
    LOG_INFO("GarbageCollector: aborted");
  }
};

}  // namespace ulayfs::utility
