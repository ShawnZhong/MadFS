#pragma once

#include <array>
#include <bit>
#include <vector>

#include "bitmap.h"
#include "idx.h"
#include "mem_table.h"

namespace ulayfs::dram {
class BlockAllocator {
  MemTable* mem_table;
  BitmapMgr* bitmap_mgr;

  // free_lists[n-1] means a free list of size n beginning from LogicalBlockIdx
  std::array<std::vector<LogicalBlockIdx>, BITMAP_ENTRY_BLOCKS_CAPACITY>
      free_lists{};

  BitmapIdx recent_bitmap_idx{};

 public:
  BlockAllocator(MemTable* mem_table, BitmapMgr* bitmap_mgr)
      : mem_table(mem_table), bitmap_mgr(bitmap_mgr) {}
  ~BlockAllocator() { return_free_list(); }

  /**
   * allocate contiguous blocks (num_blocks must <= 64)
   * if large number of blocks required, please break it into multiple alloc and
   * use log entries to chain them together
   *
   * @param num_blocks number of blocks to allocate
   * @return the logical block id of the first block
   */
  [[nodiscard]] LogicalBlockIdx alloc(uint32_t num_blocks) {
    assert(num_blocks <= BITMAP_ENTRY_BLOCKS_CAPACITY);

    if (!free_lists[num_blocks - 1].empty()) {
      LogicalBlockIdx lidx = free_lists[num_blocks - 1].back();
      free_lists[num_blocks - 1].pop_back();
      LOG_TRACE(
          "Allocator::alloc: allocating from free list (fully consumed): "
          "[n_blk: %d, lidx: %u]",
          num_blocks, lidx.get());
      return lidx;
    }

    for (uint32_t n = num_blocks + 1; n <= BITMAP_ENTRY_BLOCKS_CAPACITY; ++n) {
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
    auto [allocated_idx, allocated_bits] =
        bitmap_mgr->try_alloc(recent_bitmap_idx);
    LOG_TRACE("Allocator::alloc: allocating from bitmap %d: 0x%lx",
              allocated_idx, allocated_bits);

    // add available bits to the local free list
    uint32_t num_bits_left = BITMAP_ENTRY_BLOCKS_CAPACITY;
    LogicalBlockIdx allocated_block_idx;
    while (num_bits_left > 0) {
      // first remove all trailing ones
      auto num_right_ones =
          static_cast<uint32_t>(std::countr_one(allocated_bits));
      allocated_bits >>= num_right_ones;
      num_bits_left -= num_right_ones;

      // allocated_bits should have many trailing zeros
      uint32_t num_right_zeros =
          std::min(static_cast<uint32_t>(std::countr_zero(allocated_bits)),
                   num_bits_left);
      // if not, it means no bits left
      if (num_right_zeros == 0) break;

      if (!is_found && num_right_zeros >= num_blocks) {
        is_found = true;
        allocated_block_idx =
            allocated_idx + BITMAP_ENTRY_BLOCKS_CAPACITY - num_bits_left;
        LOG_TRACE("Allocator::alloc: allocated blocks: [n_blk: %d, lidx: %u]",
                  num_right_zeros, allocated_block_idx.get());
        if (num_right_zeros > num_blocks) {
          free_lists[num_right_zeros - num_blocks - 1].emplace_back(
              allocated_idx + BITMAP_ENTRY_BLOCKS_CAPACITY - num_bits_left +
              num_blocks);
          LOG_TRACE(
              "Allocator::alloc: unused blocks saved: [n_blk: %d, lidx: %u]",
              num_right_zeros - num_blocks,
              allocated_idx + BITMAP_ENTRY_BLOCKS_CAPACITY - num_bits_left +
                  num_blocks);
        }
      } else {
        free_lists[num_right_zeros - 1].emplace_back(
            allocated_idx + BITMAP_ENTRY_BLOCKS_CAPACITY - num_bits_left);
        LOG_TRACE(
            "Allocator::alloc: unused blocks saved: [n_blk: %d, lidx: %u]",
            num_right_zeros,
            allocated_idx + BITMAP_ENTRY_BLOCKS_CAPACITY - num_bits_left);
      }
      allocated_bits >>= num_right_zeros;
      num_bits_left -= num_right_zeros;
    }
    // this recent is not useful because we have taken all bits; move on
    recent_bitmap_idx = allocated_idx + BITMAP_ENTRY_BLOCKS_CAPACITY;

    // don't have the right size, retry
    if (!is_found) goto retry;
    return allocated_block_idx;
  }

  /**
   * Free the blocks in the range [block_idx, block_idx + num_blocks)
   */
  void free(LogicalBlockIdx block_idx, uint32_t num_blocks = 1) {
    if (block_idx == 0) return;
    LOG_TRACE("Allocator::alloc: adding to free list: [%u, %u)",
              block_idx.get(), num_blocks + block_idx.get());
    free_lists[num_blocks - 1].emplace_back(block_idx);
  }

  /**
   * Free an array of blocks, but the logical block indexes are not necessary
   * continuous
   */
  void free(const std::vector<LogicalBlockIdx>& recycle_image) {
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

  /**
   * Return all the blocks in the free list to the bitmap
   */
  void return_free_list() {
    for (uint32_t n = 0; n < BITMAP_ENTRY_BLOCKS_CAPACITY; ++n)
      for (LogicalBlockIdx lidx : free_lists[n])
        bitmap_mgr->free(static_cast<BitmapIdx>(lidx.get()), n + 1);

    free_lists = {};
  }
};
}  // namespace ulayfs::dram
