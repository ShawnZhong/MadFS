#pragma once

#include <linux/mman.h>

#include <cstddef>
#include <stdexcept>
#include <unordered_map>

#include "block.h"
#include "config.h"
#include "layout.h"
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

  // a copy of global num_blocks in MetaBlock to avoid shared memory access
  // may be out-of-date; must re-read global one if necessary
  uint32_t num_blocks_local_copy;

  std::unordered_map<LogicalBlockIdx, pmem::Block*> table;

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
  pmem::Block* mmap_file(size_t length, off_t offset, int flags = 0) const {
    if constexpr (BuildOptions::use_map_sync)
      flags |= MAP_SHARED_VALIDATE | MAP_SYNC;
    else
      flags |= MAP_SHARED;
    if constexpr (BuildOptions::force_map_populate) flags |= MAP_POPULATE;
    if constexpr (BuildOptions::use_huge_page)
      flags |= MAP_HUGETLB | MAP_HUGE_2MB;

    void* addr =
        posix::mmap(nullptr, length, PROT_READ | PROT_WRITE, flags, fd, offset);
    PANIC_IF(addr == (void*)-1, "mmap fd = %d failed", fd);
    return static_cast<pmem::Block*>(addr);
  }

 public:
  MemTable() = default;

  MemTable(int fd, off_t file_size) {
    this->fd = fd;

    // grow to multiple of grow_unit_size if the file is empty or the file size
    // is not grow_unit aligned
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
    num_blocks_local_copy = file_size >> BLOCK_SHIFT;
    if (should_grow) meta->set_num_blocks_no_lock(num_blocks_local_copy);

    // initialize the mapping
    for (LogicalBlockIdx idx = 0; idx < num_blocks_local_copy;
         idx += NUM_BLOCKS_PER_GROW)
      table.emplace(idx, blocks + idx);
  }

  [[nodiscard]] pmem::MetaBlock* get_meta() const { return meta; }

  // ask more blocks for the kernel filesystem, so that idx is valid
  void validate(LogicalBlockIdx idx) {
    // fast path: if smaller than local copy; return
    if (idx < num_blocks_local_copy) return;

    // medium path: update local copy and retry
    num_blocks_local_copy = meta->get_num_blocks();
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
  pmem::Block* get_addr(LogicalBlockIdx idx) {
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
