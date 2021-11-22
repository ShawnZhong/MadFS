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
constexpr static uint32_t NUM_BLOCKS_PER_GROW = GROW_UNIT_SIZE / BLOCK_SIZE;

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

  tbb::concurrent_unordered_map<LogicalBlockIdx, pmem::Block*> table;

  // a vector of <addr, length> pairs
  tbb::concurrent_vector<std::tuple<void*, size_t>> mmap_regions;

 private:
  // called by other public functions with lock held
  void grow_no_lock(LogicalBlockIdx idx) {
    // we need to revalidate under after acquiring lock
    if (idx < meta->get_num_blocks()) return;

    // the new file size should be a multiple of grow unit
    // we have `idx + 1` since we want to grow the file when idx is a multiple
    // of the number of blocks in a grow unit (e.g., 512 for 2 MB grow)
    size_t file_size =
        ALIGN_UP(static_cast<size_t>(idx + 1) << BLOCK_SHIFT, GROW_UNIT_SIZE);

    int ret = posix::ftruncate(fd, static_cast<off_t>(file_size));
    PANIC_IF(ret, "ftruncate failed");
    meta->set_num_blocks_no_lock(file_size >> BLOCK_SHIFT);
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
    if constexpr (BuildOptions::use_huge_page)
      flags |= MAP_HUGETLB | MAP_HUGE_2MB;

    int prot = PROT_READ | PROT_WRITE;

    void* addr = posix::mmap(nullptr, length, prot, flags, fd, offset);

    if (unlikely(addr == MAP_FAILED)) {
      // Note that the order of the following two fallback plans matters
      if constexpr (BuildOptions::use_huge_page) {
        if (errno == EINVAL) {
          WARN(
              "Huge page not supported for fd = %d. "
              "Please check `cat /proc/meminfo | grep Huge`. "
              "Retry w/o huge page",
              fd);
          flags &= ~(MAP_HUGETLB | MAP_HUGE_2MB);
          addr = posix::mmap(nullptr, length, prot, flags, fd, offset);
        }
      }

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
  MemTable(int fd, off_t file_size) {
    this->fd = fd;

    // grow to multiple of grow_unit_size if the file is empty or the file
    // size is not grow_unit aligned
    bool should_grow = file_size == 0 || !IS_ALIGNED(file_size, GROW_UNIT_SIZE);
    if (should_grow) {
      file_size =
          file_size == 0 ? PREALLOC_SIZE : ALIGN_UP(file_size, GROW_UNIT_SIZE);
      int ret = posix::ftruncate(fd, file_size);
      PANIC_IF(ret, "ftruncate failed");
    }

    pmem::Block* blocks = mmap_file(file_size, 0);
    meta = &blocks->meta_block;

    // compute number of blocks and update the mata block if necessary
    auto num_blocks = file_size >> BLOCK_SHIFT;
    if (should_grow) meta->set_num_blocks_no_lock(num_blocks);

    // initialize the mapping
    for (LogicalBlockIdx idx = 0; idx < num_blocks; idx += NUM_BLOCKS_PER_GROW)
      table.emplace(idx, blocks + idx);
  }

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
    meta->lock();
    grow_no_lock(idx);
    meta->unlock();
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

    size_t hugepage_size = static_cast<size_t>(hugepage_idx) << BLOCK_SHIFT;
    pmem::Block* hugepage_blocks = mmap_file(
        GROW_UNIT_SIZE, static_cast<off_t>(hugepage_size), MAP_POPULATE);
    table.emplace(hugepage_idx, hugepage_blocks);
    return hugepage_blocks + hugepage_local_idx;
  }

  friend std::ostream& operator<<(std::ostream& out, const MemTable& m) {
    out << "MemTable:\n";
    for (const auto& [blk_idx, mem_addr] : m.table) {
      out << "\t" << blk_idx << " - " << blk_idx + NUM_BLOCKS_PER_GROW << ": ";
      out << mem_addr << " - " << mem_addr + NUM_BLOCKS_PER_GROW << "\n";
    }
    return out;
  }
};

}  // namespace ulayfs::dram
