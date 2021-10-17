#pragma once

#include <stdexcept>
#include <vector>

#include "config.h"
#include "layout.h"
#include "map.h"
#include "posix.h"

namespace ulayfs::dram {

class File;

// per-thread data structure
class Allocator {
  pmem::MetaBlock* meta;
  IdxMap* idx_map;
  int fd;

  // a copy of global num_blocks in MetaBlock to avoid shared memory access
  // may be out-of-date; must re-read global one if necessary
  uint32_t num_blocks_local_copy;

  // this local free_list maintains blocks allocated from the global free_list
  // and not used yet
  std::vector<pmem::BlockIdx> free_list;

  // used as a hint for search
  pmem::BlockIdx recent_bitmap_block;
  // the logical bitmap index (used for get_block_idx)
  uint32_t recent_bitmap_block_id;
  // NOTE: this is the index within recent_bitmap_block
  pmem::BlockLocalIdx recent_bitmap_local_idx;

  // called by other public functions with lock held
  void grow_no_lock(pmem::BlockIdx idx) {
    // we need to revalidate under after acquiring lock
    if (idx < meta->num_blocks) return;
    uint32_t new_num_blocks = ((idx >> LayoutOptions::grow_unit_shift) + 1)
                              << LayoutOptions::grow_unit_shift;
    int ret = posix::ftruncate(fd, static_cast<long>(new_num_blocks)
                                       << pmem::BLOCK_SHIFT);
    if (ret) throw std::runtime_error("Fail to ftruncate!");
    meta->num_blocks = new_num_blocks;
  }

 public:
  Allocator()
      : fd(-1),
        meta(nullptr),
        num_blocks_local_copy(0),
        recent_bitmap_block(0),
        recent_bitmap_block_id(0),
        recent_bitmap_local_idx(0) {}

  void init(int fd, pmem::MetaBlock* meta, IdxMap* idx_map) {
    this->fd = fd;
    this->meta = meta;
    this->free_list.reserve(64);
    this->num_blocks_local_copy = meta->num_blocks;
    this->idx_map = idx_map;
  }

  // ask more blocks for the kernel filesystem, such that idx is valid
  void grow(pmem::BlockIdx idx) {
    // fast path: if smaller than local copy; return
    if (idx < num_blocks_local_copy) return;

    // medium path: update local copy and retry
    num_blocks_local_copy = meta->num_blocks;
    if (idx < num_blocks_local_copy) return;

    // slow path: acquire lock to verify and grow if necessary
    meta->lock();
    grow_no_lock(idx);
    meta->unlock();
  }

  pmem::BlockIdx grow_bitmap_block();

  pmem::BlockIdx alloc_one();
};

};  // namespace ulayfs::dram
