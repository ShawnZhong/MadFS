#include "alloc.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <cstring>

#include "bitmap.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"
#include "utils.h"

namespace ulayfs::dram {

LogicalBlockIdx Allocator::alloc(uint32_t num_blocks) {
  assert(num_blocks <= BITMAP_BLOCK_CAPACITY);

  if (!free_lists[num_blocks - 1].empty()) {
    LogicalBlockIdx lidx = free_lists[num_blocks - 1].back();
    free_lists[num_blocks - 1].pop_back();
    LOG_TRACE(
        "Allocator::alloc: allocating from free list (fully consumed): "
        "[n_blk: %d, lidx: %u]",
        num_blocks, lidx.get());
    return lidx;
  }

  for (uint32_t n = num_blocks + 1; n <= BITMAP_BLOCK_CAPACITY; ++n) {
    if (!free_lists[n - 1].empty()) {
      LogicalBlockIdx lidx = free_lists[n - 1].back();

      free_lists[n - 1].pop_back();
      free_lists[n - num_blocks - 1].push_back(lidx + num_blocks);
      LOG_TRACE(
          "Allocator::alloc: allocating from free list (partially consumed): "
          "[n_blk: %d, lidx: %u] -> [n_blk: %d, lidx: %u]",
          n, lidx.get(), n - num_blocks, lidx.get() + num_blocks);
      return lidx;
    }
  }

  bool is_found = false;

retry:
  // then we have to allocate from global bitmaps
  // but try_alloc doesn't necessarily return the number of blocks we want
  uint64_t allocated_bits;
  BitmapIdx allocated_idx =
      Bitmap::try_alloc(bitmap, NUM_BITMAP, recent_bitmap_idx, allocated_bits);
  LOG_TRACE("Allocator::alloc: allocating from bitmap %d: 0x%lx", allocated_idx,
            allocated_bits);

  // add available bits to the local free list
  uint32_t num_bits_left = BITMAP_BLOCK_CAPACITY;
  LogicalBlockIdx allocated_block_idx;
  while (num_bits_left > 0) {
    // first remove all trailing ones
    auto num_right_ones =
        static_cast<uint32_t>(std::countr_one(allocated_bits));
    allocated_bits >>= num_right_ones;
    num_bits_left -= num_right_ones;

    // allocated_bits should have many trailing zeros
    uint32_t num_right_zeros = std::min(
        static_cast<uint32_t>(std::countr_zero(allocated_bits)), num_bits_left);
    // if not, it means no bits left
    if (num_right_zeros == 0) break;

    if (!is_found && num_right_zeros >= num_blocks) {
      is_found = true;
      allocated_block_idx =
          allocated_idx + BITMAP_BLOCK_CAPACITY - num_bits_left;
      LOG_TRACE("Allocator::alloc: allocated blocks: [n_blk: %d, lidx: %u]",
                num_right_zeros, allocated_block_idx.get());
      if (num_right_zeros > num_blocks) {
        free_lists[num_right_zeros - num_blocks - 1].emplace_back(
            allocated_idx + BITMAP_BLOCK_CAPACITY - num_bits_left + num_blocks);
        LOG_TRACE(
            "Allocator::alloc: unused blocks saved: [n_blk: %d, lidx: %u]",
            num_right_zeros - num_blocks,
            allocated_idx + BITMAP_BLOCK_CAPACITY - num_bits_left + num_blocks);
      }
    } else {
      free_lists[num_right_zeros - 1].emplace_back(
          allocated_idx + BITMAP_BLOCK_CAPACITY - num_bits_left);
      LOG_TRACE("Allocator::alloc: unused blocks saved: [n_blk: %d, lidx: %u]",
                num_right_zeros,
                allocated_idx + BITMAP_BLOCK_CAPACITY - num_bits_left);
    }
    allocated_bits >>= num_right_zeros;
    num_bits_left -= num_right_zeros;
  }
  // this recent is not useful because we have taken all bits; move on
  recent_bitmap_idx = allocated_idx + BITMAP_BLOCK_CAPACITY;

  // don't have the right size, retry
  if (!is_found) goto retry;
  return allocated_block_idx;
}

void Allocator::free(LogicalBlockIdx block_idx, uint32_t num_blocks) {
  if (block_idx == 0) return;
  LOG_TRACE("Allocator::alloc: adding to free list: [%u, %u)", block_idx.get(),
            num_blocks + block_idx.get());
  free_lists[num_blocks - 1].emplace_back(block_idx);
}

void Allocator::free(const std::vector<LogicalBlockIdx>& recycle_image) {
  // try to group blocks
  // we don't try to merge the blocks with existing free list since the
  // searching is too expensive
  uint32_t group_begin = 0;
  LogicalBlockIdx group_begin_lidx = 0;
  uint32_t image_size = recycle_image.size();

  for (uint32_t curr = group_begin; curr < image_size; ++curr) {
    if (group_begin_lidx == 0) {  // new group not started yet
      if (recycle_image[curr] == 0) continue;
      // start a new group
      group_begin = curr;
      group_begin_lidx = recycle_image[curr];
    } else {
      // continue the group if it matches the expectation
      if (recycle_image[curr] == group_begin_lidx + (curr - group_begin))
        continue;
      LOG_TRACE("Allocator::free: adding to free list: [%u, %u)",
                group_begin_lidx.get(),
                curr - group_begin + group_begin_lidx.get());
      free_lists[curr - group_begin - 1].emplace_back(group_begin_lidx);
      group_begin_lidx = recycle_image[curr];
      if (group_begin_lidx != 0) group_begin = curr;
    }
  }
  if (group_begin_lidx != 0) {
    LOG_TRACE("Allocator::free: adding to free list: [%u, %u)",
              group_begin_lidx.get(),
              group_begin_lidx.get() + image_size - group_begin);
    free_lists[image_size - group_begin - 1].emplace_back(group_begin_lidx);
  }
}

std::tuple<pmem::LogEntry*, LogEntryIdx, pmem::LogEntryBlock*>
Allocator::alloc_log_entry(uint32_t num_blocks) {
  // for a log entry with only one logical block index, it takes 16 bytes
  // if smaller than that, do not try to allocate log entry there
  constexpr uint32_t min_required_size =
      pmem::LogEntry::FIXED_SIZE + sizeof(LogicalBlockIdx);
  if (curr_log_block_idx == 0 ||
      BLOCK_SIZE - curr_log_offset < min_required_size) {
    // no enough space left, do block allocation
    curr_log_block_idx = alloc(1);
    curr_log_block =
        &file->lidx_to_addr_rw(curr_log_block_idx)->log_entry_block;
    curr_log_offset = 0;
  }

  LogEntryIdx first_idx = {curr_log_block_idx, curr_log_offset};
  pmem::LogEntryBlock* first_block = curr_log_block;
  pmem::LogEntry* first_entry = curr_log_block->get(curr_log_offset);
  pmem::LogEntry* curr_entry = first_entry;
  uint32_t needed_lidxs_cnt = ALIGN_UP(num_blocks, BITMAP_BLOCK_CAPACITY) >>
                              BITMAP_BLOCK_CAPACITY_SHIFT;
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
      return {first_entry, first_idx, first_block};
    }

    curr_entry->has_next = true;
    curr_entry->num_blocks = avail_lidxs_cnt << BITMAP_BLOCK_CAPACITY_SHIFT;
    curr_log_offset += avail_lidxs_cnt * sizeof(LogicalBlockIdx);
    needed_lidxs_cnt -= avail_lidxs_cnt;
    num_blocks -= curr_entry->num_blocks;

    assert(curr_log_offset <= BLOCK_SIZE);
    if (BLOCK_SIZE - curr_log_offset < min_required_size) {
      curr_log_block_idx = alloc(1);
      curr_log_block =
          &file->lidx_to_addr_rw(curr_log_block_idx)->log_entry_block;
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

// return log entry blocks that are exclusively taken by this log entry; if
// there is other log entries on this block, leave this block alone
void Allocator::free_log_entry(pmem::LogEntry* first_entry,
                               LogEntryIdx first_idx,
                               pmem::LogEntryBlock* first_block) {
  pmem::LogEntry* curr_entry = first_entry;
  pmem::LogEntryBlock* curr_block = first_block;
  LogicalBlockIdx curr_block_idx = first_idx.block_idx;

  // NOTE: we assume free() will always keep the block in the local free list
  //       instead of publishing it immediately. this makes it safe to read the
  //       log entry block even after calling free()

  // do we need to free the first le block? no if there is other le ahead
  if (first_idx.local_offset == 0) free(curr_block_idx);

  while (curr_entry->has_next) {
    if (curr_entry->is_next_same_block) {
      curr_entry = curr_block->get(curr_entry->next.local_offset);
    } else {
      curr_block_idx = curr_entry->next.block_idx;
      curr_block = &file->lidx_to_addr_rw(curr_block_idx)->log_entry_block;
      curr_entry = curr_block->get(0);
      free(curr_block_idx);
    }
  }
}

void Allocator::reset_log_entry() {
  // this is to trigger new log entry block allocation
  curr_log_block_idx = 0;
  // technically, setting curr_log_block and curr_log_offset are unnecessary
  // because they are guarded by setting curr_log_block_idx zero
}

std::tuple<LogicalBlockIdx, pmem::TxBlock*> Allocator::alloc_tx_block(
    uint32_t tx_seq) {
  if (avail_tx_block) {
    pmem::Block* tx_block = avail_tx_block;
    avail_tx_block = nullptr;
    tx_block->tx_block.set_tx_seq(tx_seq);
    pmem::persist_cl_fenced(&tx_block->cache_lines[NUM_CL_PER_BLOCK - 1]);
    return {avail_tx_block_idx, &tx_block->tx_block};
  }

  LogicalBlockIdx new_block_idx = alloc(1);
  pmem::Block* tx_block = file->lidx_to_addr_rw(new_block_idx);
  memset(&tx_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
  tx_block->tx_block.set_tx_seq(tx_seq);
  pmem::persist_cl_unfenced(&tx_block->cache_lines[NUM_CL_PER_BLOCK - 1]);
  tx_block->zero_init(0, NUM_CL_PER_BLOCK - 1);
  return {new_block_idx, &tx_block->tx_block};
}

void Allocator::free_tx_block(LogicalBlockIdx tx_block_idx,
                              pmem::TxBlock* tx_block) {
  assert(!avail_tx_block);
  avail_tx_block_idx = tx_block_idx;
  avail_tx_block = reinterpret_cast<pmem::Block*>(tx_block);
}

}  // namespace ulayfs::dram
