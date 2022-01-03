#pragma once

#include <linux/mman.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include <cstddef>
#include <stdexcept>

#include "block.h"
#include "config.h"
#include "params.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::dram {

constexpr static uint32_t GROW_UNIT_IN_BLOCK_SHIFT =
    GROW_UNIT_SHIFT - BLOCK_SHIFT;
constexpr static uint32_t GROW_UNIT_IN_BLOCK_MASK =
    (1 << GROW_UNIT_IN_BLOCK_SHIFT) - 1;
constexpr static uint32_t NUM_BLOCKS_PER_GROW = GROW_UNIT_SIZE >> BLOCK_SHIFT;

// map LogicalBlockIdx into memory address
// this is a more low-level data structure than Allocator
// it should maintain the virtualization of infinite large of file
// everytime it gets a LogicalBlockIdx:
// - if this block is already mapped; return addr
// - if this block is allocated from kernel filesystem, mmap and return
//   the addr
// - if this block is not even allocated from kernel filesystem, grow
//   it, map it, and return the address
class MemTable {
  pmem::MetaBlock* meta;
  int fd;
  int prot;

  tbb::concurrent_unordered_map<LogicalBlockIdx, pmem::Block*> table;

  // a vector of <addr, length> pairs
  tbb::concurrent_vector<std::tuple<void*, size_t>> mmap_regions;

 private:
  // called by other public functions with lock held
  void grow(LogicalBlockIdx idx) {
    // the new file size should be a multiple of grow unit
    // we have `idx + 1` since we want to grow the file when idx is a multiple
    // of the number of blocks in a grow unit (e.g., 512 for 2 MB grow)
    uint64_t file_size = ALIGN_UP(BLOCK_IDX_TO_SIZE(idx + 1), GROW_UNIT_SIZE);

    int ret = posix::fallocate(fd, 0, 0, static_cast<off_t>(file_size));
    PANIC_IF(ret, "fd %d: fallocate failed", fd);
    meta->set_num_blocks_if_larger(BLOCK_SIZE_TO_IDX(file_size));
  }

  /**
   * a private helper function that calls mmap internally
   * @return the pointer to the first block on the persistent memory
   */
  pmem::Block* mmap_file(size_t length, off_t offset, int flags = 0) {
    if constexpr (BuildOptions::use_map_sync)
      flags |= MAP_SHARED_VALIDATE | MAP_SYNC;
    else
      flags |= MAP_SHARED;
    if constexpr (BuildOptions::force_map_populate) flags |= MAP_POPULATE;

    void* addr = posix::mmap(nullptr, length, prot, flags, fd, offset);

    if (unlikely(addr == MAP_FAILED)) {
      if constexpr (BuildOptions::use_map_sync) {
        if (errno == EOPNOTSUPP) {
          WARN("MAP_SYNC not supported for fd = %d. Retry w/o MAP_SYNC", fd);
          flags &= ~(MAP_SHARED_VALIDATE | MAP_SYNC);
          flags |= MAP_SHARED;
          addr = posix::mmap(nullptr, length, prot, flags, fd, offset);
        }
      }

      PANIC_IF(addr == MAP_FAILED, "mmap fd = %d failed", fd);
    }
    VALGRIND_PMC_REGISTER_PMEM_MAPPING(addr, length);
    mmap_regions.emplace_back(addr, length);
    return static_cast<pmem::Block*>(addr);
  }

 public:
  MemTable(int fd, off_t file_size, bool read_only);

  ~MemTable() {
    for (const auto& [addr, length] : mmap_regions) {
      munmap(addr, length);
      VALGRIND_PMC_REMOVE_PMEM_MAPPING(addr, length);
    }
  }

  [[nodiscard]] pmem::MetaBlock* get_meta() const { return meta; }

  // ask more blocks for the kernel filesystem, so that idx is valid
  void validate(LogicalBlockIdx idx) {
    // fast path: if smaller than the number of block; return
    if (idx < meta->get_num_blocks()) return;

    // slow path: acquire lock to verify and grow if necessary
    grow(idx);
  }

  /**
   * the idx might pass Allocator's grow() to ensure there is a backing kernel
   * filesystem block
   *
   * it will then check if it has been mapped into the address space; if not,
   * it does mapping first
   *
   * @param idx the logical block index
   * @return the Block pointer if idx is not 0; nullptr for idx == 0, and the
   * caller should handle this case
   */
  pmem::Block* get(LogicalBlockIdx idx) {
    if (idx == 0) return nullptr;

    LogicalBlockIdx hugepage_idx = idx & ~GROW_UNIT_IN_BLOCK_MASK;
    LogicalBlockIdx hugepage_local_idx = idx & GROW_UNIT_IN_BLOCK_MASK;
    if (auto it = table.find(hugepage_idx); it != table.end())
      return it->second + hugepage_local_idx;

    // validate if this idx has real blocks allocated; do allocation if not
    validate(idx);

    uint64_t hugepage_size = BLOCK_IDX_TO_SIZE(hugepage_idx);
    pmem::Block* hugepage_blocks = mmap_file(
        GROW_UNIT_SIZE, static_cast<off_t>(hugepage_size), MAP_POPULATE);
    table.emplace(hugepage_idx, hugepage_blocks);
    return hugepage_blocks + hugepage_local_idx;
  }

  friend std::ostream& operator<<(std::ostream& out, const MemTable& m);
};

}  // namespace ulayfs::dram
