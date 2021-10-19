#pragma once

#include <stdexcept>
#include <vector>

#include "config.h"
#include "layout.h"
#include "mtable.h"
#include "posix.h"

namespace ulayfs::dram {

// per-thread data structure
class Allocator {
  pmem::MetaBlock* meta;
  MemTable* mtable;
  int fd;

  // this local free_list maintains blocks allocated from the global free_list
  // and not used yet; pair: <size, idx>
  // sorted in the increasing order (least size first)
  std::vector<std::pair<uint32_t, pmem::BlockIdx>> free_list;

  // used as a hint for search; recent is defined to be "the next one to search"
  // keep id for idx translation
  pmem::BitmapBlockId recent_bitmap_block_id;
  // NOTE: this is the index within recent_bitmap_block
  pmem::BlockLocalIdx recent_bitmap_local_idx;

 public:
  Allocator()
      : fd(-1),
        meta(nullptr),
        recent_bitmap_block_id(0),
        recent_bitmap_local_idx(0) {}

  void init(int fd, pmem::MetaBlock* meta, MemTable* mtable) {
    this->fd = fd;
    this->meta = meta;
    this->free_list.reserve(64);
    this->mtable = mtable;
  }

  // allocate contiguous blocks (num_blocks must <= 64)
  // if large number of blocks required, please break it into multiple alloc
  // and use log entries to chain them together
  pmem::BlockIdx alloc(uint32_t num_blocks);
};

};  // namespace ulayfs::dram
