#pragma once

#include <linux/mman.h>

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
class IdxMap {
  int fd;
  std::unordered_map<pmem::BlockIdx, pmem::Block*> map;

 public:
  IdxMap() = default;

  pmem::MetaBlock* init(int fd) {
    this->fd = fd;
    pmem::Block* blocks = static_cast<pmem::Block*>(posix::mmap(
        nullptr, LayoutOptions::prealloc_size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HUGETLB | MAP_HUGE_2MB, fd, 0));
    if (!blocks) throw std::runtime_error("Fail to mmap!");
    map.emplace(0, blocks);
    return &blocks->meta_block;
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
