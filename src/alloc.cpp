#include "alloc.h"

#include <cassert>

#include "layout.h"

namespace ulayfs::dram {

LogicalBlockIdx Allocator::alloc(uint32_t num_blocks) {
  LogicalBlockIdx bitmap_block_idx;
  pmem::BitmapBlock* bitmap_block;
  assert(num_blocks <= BITMAP_CAPACITY);

  // we first try to allocate from the free list
  for (auto it = free_list.begin(); it != free_list.end(); ++it) {
    // exact match
    if (it->first == num_blocks) {
      auto idx = it->second;
      free_list.erase(it);
      return idx;
    }

    // split a free list element
    if (it->first > num_blocks) {
      auto idx = it->second;
      it->first -= num_blocks;
      it->second += num_blocks;
      // re-sort these elements
      std::sort(free_list.begin(), it + 1);
      return idx;
    }
  }

  // then we have to allocate from global bitmaps
  if (recent_bitmap_block_id == 0) {
    recent_bitmap_local_idx = meta->inline_alloc_batch(recent_bitmap_local_idx);
    if (recent_bitmap_local_idx >= 0) goto add_to_free_list;
    recent_bitmap_block_id++;
    recent_bitmap_local_idx = 0;
  }

  while (true) {
    bitmap_block_idx =
        pmem::BitmapBlock::get_bitmap_block_idx(recent_bitmap_block_id);
    bitmap_block = &(mtable->get_addr(bitmap_block_idx)->bitmap_block);
    recent_bitmap_local_idx = bitmap_block->alloc_batch();
    if (recent_bitmap_local_idx >= 0) goto add_to_free_list;
    recent_bitmap_block_id++;
    recent_bitmap_local_idx = 0;
  }

add_to_free_list:
  assert(recent_bitmap_local_idx >= 0);
  // push in decreasing order so pop will in increasing order
  LogicalBlockIdx allocated = pmem::BitmapBlock::get_block_idx(
      recent_bitmap_block_id, recent_bitmap_local_idx);
  if (num_blocks < BITMAP_CAPACITY) {
    free_list.emplace_back(BITMAP_CAPACITY - num_blocks,
                           allocated + num_blocks);
    std::sort(free_list.begin(), free_list.end());
  }
  // this recent is not useful because we have taken all bits; move on
  recent_bitmap_local_idx++;
  return allocated;
}

void Allocator::free(LogicalBlockIdx block_idx, uint32_t num_blocks) {
  // TODO: implement this
}

}  // namespace ulayfs::dram
