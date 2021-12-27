#pragma once

#include <linux/mman.h>
#include <tbb/concurrent_map.h>

#include <atomic>
#include <cstddef>
#include <functional>
#include <mutex>
#include <stdexcept>

#include "block.h"
#include "config.h"
#include "idx.h"
#include "params.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::dram {

constexpr static uint32_t GROW_UNIT_IN_BLOCK_SHIFT =
    GROW_UNIT_SHIFT - BLOCK_SHIFT;
constexpr static uint32_t GROW_UNIT_IN_BLOCK_MASK =
    (1 << GROW_UNIT_IN_BLOCK_SHIFT) - 1;
constexpr static uint32_t NUM_BLOCKS_PER_GROW = GROW_UNIT_SIZE / BLOCK_SIZE;

constexpr static uint64_t MMAP_REGION_GAP = (1UL << 30);  // 1G

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
  // coordinate across multiple files to map to different regions
  static std::atomic<char*> next_hint;

  pmem::MetaBlock* meta;
  int fd;
  int prot;

  using MapType =
      tbb::concurrent_map<LogicalBlockIdx,
                          std::pair<pmem::Block*, std::atomic_uint32_t>,
                          std::greater<LogicalBlockIdx>>;
  MapType mmap_regions;

  // only for mmap
  std::mutex mmap_lock;

 private:
  // called by other public functions with lock held
  void grow(LogicalBlockIdx idx) {
    // the new file size should be a multiple of grow unit
    // we have `idx + 1` since we want to grow the file when idx is a multiple
    // of the number of blocks in a grow unit (e.g., 512 for 2 MB grow)
    size_t file_size =
        ALIGN_UP(static_cast<size_t>(idx + 1) << BLOCK_SHIFT, GROW_UNIT_SIZE);

    int ret = posix::fallocate(fd, 0, 0, static_cast<off_t>(file_size));
    PANIC_IF(ret, "fallocate failed");
    meta->set_num_blocks_if_larger(file_size >> BLOCK_SHIFT);
  }

  /**
   * a private helper function that calls mmap internally
   * @return the pointer to the newly mapped region
   */
  void* mmap_file(void* hint_addr, size_t length, off_t offset, int flags = 0) {
    if constexpr (BuildOptions::use_map_sync)
      flags |= MAP_SHARED_VALIDATE | MAP_SYNC;
    else
      flags |= MAP_SHARED;
    if constexpr (BuildOptions::force_map_populate) flags |= MAP_POPULATE;

    void* addr = posix::mmap(hint_addr, length, prot, flags, fd, offset);

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
    return addr;
  }

 public:
  MemTable(int fd, uint64_t file_size, bool read_only)
      : fd(fd), prot(read_only ? PROT_READ : PROT_READ | PROT_WRITE) {
    // grow to multiple of grow_unit_size if the file is empty or the file
    // size is not grow_unit aligned
    bool should_grow = file_size == 0 || !IS_ALIGNED(file_size, GROW_UNIT_SIZE);
    if (should_grow) {
      file_size =
          file_size == 0 ? PREALLOC_SIZE : ALIGN_UP(file_size, GROW_UNIT_SIZE);
      int ret = posix::fallocate(fd, 0, 0, file_size);
      PANIC_IF(ret, "fallocate failed");
    }

    char* hint = nullptr;
    if (next_hint.load(std::memory_order_acquire))
      hint = next_hint.fetch_add(MMAP_REGION_GAP, std::memory_order_acq_rel);
    void* addr = mmap_file(hint, file_size, 0, 0);
    if (!hint)
      next_hint.compare_exchange_strong(hint, static_cast<char*>(addr),
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
    meta = &static_cast<pmem::Block*>(addr)->meta_block;

    // compute number of blocks and update the meta block if necessary
    auto num_blocks = file_size >> BLOCK_SHIFT;
    if (should_grow) meta->set_num_blocks_if_larger(num_blocks);

    mmap_regions.emplace(
        std::piecewise_construct, std::forward_as_tuple(/*lidx*/ 0),
        std::forward_as_tuple(/*addr*/ static_cast<pmem::Block*>(addr),
                              /*num_block*/ file_size >> BLOCK_SHIFT));
  }

  ~MemTable() {
    for (const auto& [lix, addr_size] : mmap_regions) {
      const auto& [addr, num_blocks] = addr_size;
      munmap(addr, num_blocks << BLOCK_SHIFT);
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
   * @param lidx the logical block index
   * @return the Block pointer if idx is not 0; nullptr for idx == 0, and the
   * caller should handle this case
   */
  pmem::Block* get(LogicalBlockIdx lidx) {
    if (lidx == 0) return nullptr;

    auto it = mmap_regions.lower_bound(lidx);
    assert(it != mmap_regions.end());

    const auto& [begin_lidx, addr_size] = *it;
    const auto& [addr, size] = addr_size;
    pmem::Block* ideal_addr = addr + (lidx - begin_lidx);
    auto size_local = size.load(std::memory_order_relaxed);
    if (lidx < begin_lidx + size_local) return ideal_addr;

    // validate if this idx has real blocks allocated; do allocation if not
    validate(lidx);

    {
      LogicalBlockIdx hugepage_idx = lidx & ~GROW_UNIT_IN_BLOCK_MASK;
      void* hint = addr + (hugepage_idx - begin_lidx);
      std::lock_guard<std::mutex> lock(mmap_lock);
      // must reload, since another thread may modify it while we wait for lock
      size_local = size.load(std::memory_order_relaxed);
      if (lidx < begin_lidx + size_local) return ideal_addr;

      void* mapped_addr =
          mmap_file(hint, GROW_UNIT_SIZE, hugepage_idx << BLOCK_SHIFT);
      if (mapped_addr == hint) return ideal_addr;
      mmap_regions.emplace(
          std::piecewise_construct, std::forward_as_tuple(hugepage_idx),
          std::forward_as_tuple(static_cast<pmem::Block*>(mapped_addr),
                                NUM_BLOCKS_PER_GROW));
      return static_cast<pmem::Block*>(mapped_addr) + (lidx - hugepage_idx);
    }
  }

  friend std::ostream& operator<<(std::ostream& out, const MemTable& m) {
    out << "MemTable:\n";
    for (const auto& [blk_idx, addr_size] : m.mmap_regions) {
      const auto& [addr, num_blocks] = addr_size;
      out << "\t" << blk_idx << " - " << blk_idx + num_blocks << ": ";
      out << addr << " - " << addr + num_blocks << BLOCK_SIZE << "\n";
    }
    return out;
  }
};

}  // namespace ulayfs::dram
