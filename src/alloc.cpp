#include "alloc.h"

#include <cstdlib>

#include "file.h"
#include "layout.h"

namespace ulayfs::dram {

pmem::BlockIdx Allocator::alloc_one() {
  if (free_list.empty()) {
    uint32_t bitmap_block_id = recent_bitmap_block_id;
    pmem::BlockIdx bitmap_block_idx = recent_bitmap_block;
    pmem::BlockLocalIdx batch_idx = 0;

    if (!bitmap_block_id) {  // allocate from meta
      batch_idx = meta->inline_alloc_batch();
      if (batch_idx >= 0) goto add_to_free_list;
      // fail to do inline_alloc; must allocate from other bitmap
      if (!meta->bitmap_head) {
        // TODO: allocate a bitmap block and CAS into meta->bitmap_head
      }
      bitmap_block_idx = meta->bitmap_head;
      ++bitmap_block_id;
    }

    // now try to allocate from a normal BitmapBlock
    while (true) {
      pmem::BitmapBlock* bitmap_block =
          &(idx_map->get_addr(bitmap_block_idx)->bitmap_block);
      batch_idx = bitmap_block->alloc_batch();
      if (batch_idx >= 0) goto add_to_free_list;
      // we have to move to the next bitmap block to search
      bitmap_block_idx = bitmap_block->next;
      if (bitmap_block_idx) {
        // TODO: allocate a bitmap block and update bitmap_block_idx
      }
      ++bitmap_block_id;
    }

  add_to_free_list:
    // push in decreasing order so pop will in increaseing order
    pmem::BlockIdx batch_allocated =
        pmem::BitmapBlock::get_block_idx(bitmap_block_id, batch_idx);
    for (auto offset = 63; offset >= 0; offset--)
      free_list.push_back(batch_allocated + offset);
    recent_bitmap_block_id = bitmap_block_id;
    recent_bitmap_block = bitmap_block_idx;
  }

  pmem::BlockIdx block_idx = free_list.back();
  grow(block_idx);  // ensure this block is valid (backed by a kernel fs block)
  free_list.pop_back();
  return block_idx;
}

};  // namespace ulayfs::dram
