#include "alloc.h"

#include <cstdlib>

#include "layout.h"

namespace ulayfs::dram {

pmem::BlockIdx Allocator::alloc_one() {
  if (free_list.empty()) {
    pmem::BlockIdx bitmap_block_idx = 0;
    pmem::BlockLocalIdx batch_idx = 0;
    if (recent_bitmap_block) {
      // TODO: try to allocate from the recent bitmap block
    } else {
      batch_idx = meta->inline_alloc_batch();
      if (batch_idx >= 0) goto add_to_free_list;
      // otherwise, must allocate from other bitmap blocks
      // TODO: allocate from other bitmap blocks
    }  // TODO: may need to grow bitmap block
       // TODO: update revent bitmap block...

  add_to_free_list:
    // push in decreasing order so pop will in increaseing order
    for (auto offset = 63; offset >= 0; offset--)
      free_list.push_back(batch_idx + offset);
  }

  pmem::BlockIdx block_idx = free_list.back();
  grow(block_idx);  // ensure this block is valid (backed by a kernel fs block)
  free_list.pop_back();
  return block_idx;
}

};  // namespace ulayfs::dram
