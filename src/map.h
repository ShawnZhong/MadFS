#pragma once

#include <linux/mman.h>

#include <cstddef>
#include <stdexcept>
#include <unordered_map>

#include "config.h"
#include "layout.h"
#include "posix.h"

namespace ulayfs::dram {

constexpr static uint32_t GROW_UNIT_IN_BLOCK_SHIFT =
    LayoutOptions::grow_unit_shift - pmem::BLOCK_SHIFT;
constexpr static uint32_t GROW_UNIT_IN_BLOCK_MASK =
    (1 << GROW_UNIT_IN_BLOCK_SHIFT) - 1;

// map index into address
// this is a more low-level data structure than Allocator
// it should maintain the virtualization of infinite large of file
// everytime it gets a BlockIdx:
// - if this block is already mapped; return addr
// - if this block is allocated from kernel filesystem, mmap and return
//   the addr
// - if this block is not even allocated from kernel filesystem, grow
//   it, map it, and return the address
class IdxMap {
  pmem::MetaBlock* meta;
  int fd;

  // a copy of global num_blocks in MetaBlock to avoid shared memory access
  // may be out-of-date; must re-read global one if necessary
  uint32_t num_blocks_local_copy;

  std::unordered_map<pmem::BlockIdx, pmem::Block*> map;

 private:
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
  IdxMap() : fd(-1), num_blocks_local_copy(0), map(){};

  pmem::MetaBlock* init(int fd, size_t file_size) {
    this->fd = fd;
    if ((file_size & (pmem::BLOCK_SIZE - 1)) != 0)
      throw std::runtime_error("Invalid layout: non-block-aligned file size!");
    if ((file_size & (LayoutOptions::grow_unit_size - 1)) != 0) {
      file_size = ((file_size >> LayoutOptions::grow_unit_shift) + 1)
                  << LayoutOptions::grow_unit_shift;
      int ret = posix::ftruncate(fd, file_size);
    }
    pmem::Block* blocks = static_cast<pmem::Block*>(
        posix::mmap(nullptr, file_size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_HUGETLB | MAP_HUGE_2MB, fd, 0));
    if (!blocks) throw std::runtime_error("Fail to mmap!");
    this->meta = &(blocks->meta_block);
    uint32_t num_blocks = file_size >> pmem::BLOCK_SHIFT;
    constexpr uint32_t num_blocks_per_grow =
        LayoutOptions::grow_unit_size / pmem::BLOCK_SIZE;
    for (pmem::BlockIdx idx = 0; idx < num_blocks; idx += num_blocks_per_grow)
      map.emplace(idx, blocks + idx);
    this->num_blocks_local_copy = meta->num_blocks;
    return this->meta;
  }

  // ask more blocks for the kernel filesystem, so that idx is valid
  void validate(pmem::BlockIdx idx) {
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

  // the idx might pass Allocator's grow() to ensure there is a backing kernel
  // filesystem block
  // get_addr will then check if it has been mapped into the address space; if
  // not, it does mapping first
  pmem::Block* get_addr(pmem::BlockIdx idx) {
    pmem::BlockIdx hugepage_idx = idx & ~GROW_UNIT_IN_BLOCK_MASK;
    auto offset = ((idx & GROW_UNIT_IN_BLOCK_MASK) << pmem::BLOCK_SHIFT);
    auto it = map.find(hugepage_idx);
    if (it != map.end()) return it->second + offset;

    // validate if this idx has real blocks allocated; do allocation if not
    validate(idx);

    pmem::Block* hugepage_blocks = static_cast<pmem::Block*>(posix::mmap(
        nullptr, LayoutOptions::prealloc_size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HUGETLB | MAP_HUGE_2MB, fd,
        hugepage_idx << pmem::BLOCK_SHIFT));
    if (!hugepage_blocks) throw std::runtime_error("Fail to mmap!");
    map.emplace(hugepage_idx, hugepage_blocks);
    return hugepage_blocks + offset;
  }
};

};  // namespace ulayfs::dram
