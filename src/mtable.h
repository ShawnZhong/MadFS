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
constexpr static uint32_t GROW_UNIT_IN_BLOCK = GROW_UNIT_SIZE / BLOCK_SIZE;

constexpr static uint64_t MMAP_REGION_GAP_IN_BLOCK = (1UL << 18);  // 1G

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
  static std::atomic<pmem::Block*> next_hint;

  pmem::MetaBlock* meta;
  int fd;
  int prot;

  tbb::concurrent_map<LogicalBlockIdx,
                      std::pair<pmem::Block*, std::atomic_uint32_t>,
                      std::greater<LogicalBlockIdx>>
      mmap_regions;

  // only for mmap
  std::mutex mmap_lock;

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

    uint32_t num_blocks = file_size >> BLOCK_SHIFT;

    pmem::Block* hint = nullptr;
    if (next_hint.load(std::memory_order_acquire))
      hint = next_hint.fetch_sub(ALIGN_UP(num_blocks, MMAP_REGION_GAP_IN_BLOCK),
                                 std::memory_order_acq_rel);
    pmem::Block* mapped_addr = mmap_file(hint, file_size, 0, 0);
    if (!hint)
      next_hint.compare_exchange_strong(
          hint, mapped_addr - MMAP_REGION_GAP_IN_BLOCK,
          std::memory_order_acq_rel, std::memory_order_acquire);
    meta = &mapped_addr[0].meta_block;

    // compute number of blocks and update the meta block if necessary
    if (should_grow) meta->set_num_blocks_if_larger(num_blocks);

    mmap_regions.emplace(std::piecewise_construct,
                         std::forward_as_tuple(/*lidx*/ 0),
                         std::forward_as_tuple(mapped_addr, num_blocks));
  }

  ~MemTable() {
    for (const auto& [lix, addr_num_blocks] : mmap_regions) {
      const auto& [addr, num_blocks] = addr_num_blocks;
      munmap(addr, num_blocks << BLOCK_SHIFT);
      VALGRIND_PMC_REMOVE_PMEM_MAPPING(addr, num_blocks << BLOCK_SHIFT);
    }
  }

  [[nodiscard]] pmem::MetaBlock* get_meta() const { return meta; }

  /**
   * the idx might pass Allocator's grow_size() to ensure there is a backing
   * kernel filesystem block
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

    auto& [begin_lidx, addr_num_blocks] = *it;
    auto& [addr, num_blocks] = addr_num_blocks;
    pmem::Block* ideal_addr = addr + (lidx - begin_lidx);
    uint32_t num_blocks_old = num_blocks.load(std::memory_order_relaxed);
    if (lidx < begin_lidx + num_blocks_old) return ideal_addr;

    // validate if this idx has real blocks allocated; do allocation if not
    validate_size(lidx);

    return mmap_region(lidx);
  }

 private:
  // ask more blocks for the kernel filesystem, so that idx is valid
  void validate_size(LogicalBlockIdx idx) {
    // fast path: if smaller than the number of block; return
    if (idx < meta->get_num_blocks()) return;

    // slow path: acquire lock to verify and grow if necessary
    grow_size(idx);
  }

  // called by other public functions with lock held
  void grow_size(LogicalBlockIdx idx) {
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
  pmem::Block* mmap_file(void* hint_addr, size_t length, off_t offset,
                         int flags = 0, bool allow_fail = false) {
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

      PANIC_IF(!allow_fail && addr == MAP_FAILED, "mmap fd = %d failed", fd);
    }
    if (addr != MAP_FAILED) VALGRIND_PMC_REGISTER_PMEM_MAPPING(addr, length);
    return static_cast<pmem::Block*>(addr);
  }

  pmem::Block* mmap_region(LogicalBlockIdx lidx) {
    // acquire mutex here!
    std::lock_guard<std::mutex> lock(mmap_lock);

    // must reload, since another thread may modify it while we wait for lock
    auto it = mmap_regions.lower_bound(lidx);
    auto& [begin_lidx, addr_num_blocks] = *it;
    auto& [addr, num_blocks] = addr_num_blocks;
    uint32_t num_blocks_old = num_blocks.load(std::memory_order_relaxed);
    pmem::Block* ideal_addr = addr + (lidx - begin_lidx);
    if (lidx < begin_lidx + num_blocks_old) return ideal_addr;

    uint32_t num_blocks_new =
        ALIGN_UP(lidx - begin_lidx + 1, GROW_UNIT_IN_BLOCK);
    // std::cout << "lidx: " << lidx << ", begin_lidx: " << begin_lidx
    //           << ", num_blocks_old: " << num_blocks_old
    //           << ", num_blocks_new: " << num_blocks_new << std::endl;
    uint32_t num_blocks_extended = num_blocks_new - num_blocks_old;
    assert(num_blocks_extended != 0);

    uint32_t new_region_begin_lidx = begin_lidx + num_blocks_old;
    pmem::Block* hint = addr + num_blocks_old;
    pmem::Block* mapped_addr =
        mmap_file(hint, num_blocks_extended << BLOCK_SHIFT,
                  new_region_begin_lidx << BLOCK_SHIFT, MAP_FIXED_NOREPLACE,
                  /*allow_fail*/ true);
    if (mapped_addr == hint) {
      num_blocks.store(num_blocks_new, std::memory_order_relaxed);
      return ideal_addr;
    }
    if (mapped_addr == MAP_FAILED) {
      // make three attempts
      for (uint i = 0; i < 3; ++i) {
        WARN("mmap with MAP_FIXED_NOREPLACE fails; will retry...");
        hint = next_hint.fetch_sub(
            (i + 1) * ALIGN_UP(num_blocks_extended, MMAP_REGION_GAP_IN_BLOCK),
            std::memory_order_acq_rel);
        mapped_addr =
            mmap_file(hint, num_blocks_extended << BLOCK_SHIFT,
                      new_region_begin_lidx << BLOCK_SHIFT, MAP_FIXED_NOREPLACE,
                      /*allow_fail*/ true);
        std::cout << "hint: " << hint << "\n";
        if (mapped_addr == hint) break;
      }
      WARN(
          "mmap with MAP_FIXED_NOREPLACE continuously fails after three "
          "attempts; will try without MAP_FIXED_NOREPLACE...");
      mapped_addr = mmap_file(hint, num_blocks_extended << BLOCK_SHIFT,
                              new_region_begin_lidx << BLOCK_SHIFT);
    }

    auto [new_iter, is_emplaced] = mmap_regions.emplace(
        std::piecewise_construct, std::forward_as_tuple(new_region_begin_lidx),
        std::forward_as_tuple(mapped_addr, num_blocks_extended));
    assert(is_emplaced);
    return mapped_addr + (lidx - new_region_begin_lidx);
  }

 public:
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
