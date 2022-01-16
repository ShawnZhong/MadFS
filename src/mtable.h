#pragma once

#include <linux/mman.h>
#include <tbb/concurrent_vector.h>

#include <bit>
#include <cstddef>
#include <stdexcept>

#include "block.h"
#include "config.h"
#include "const.h"
#include "idx.h"
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
// - if this block is not even allocated from kernel filesystem, grow_to_fit and
//   map it, and return the address
class MemTable {
  pmem::MetaBlock* meta;
  int fd;
  int prot;

  // immutable after ctor
  pmem::Block* first_region;
  uint32_t first_region_num_blocks;

  // map a chunk_idx to addr, where chunk_idx =
  // (lidx - first_region_num_blocks) >> GROW_UNIT_IN_BLOCK_SHIFT
  tbb::concurrent_vector<pmem::Block*> table;

  // a vector of <addr, length> pairs
  tbb::concurrent_vector<std::tuple<void*, size_t>> mmap_regions;

 public:
  MemTable(int fd, off_t file_size, bool read_only);

  ~MemTable() {
    for (const auto& [addr, length] : mmap_regions) {
      munmap(addr, length);
      VALGRIND_PMC_REMOVE_PMEM_MAPPING(addr, length);
    }
  }

  [[nodiscard]] pmem::MetaBlock* get_meta() const { return meta; }

  /**
   * it will then check if it has been mapped into the address space; if not,
   * it does mapping first; if the file does not even have the corresponding
   * data block, it allocates from the kernel.
   *
   * @param idx the logical block index
   * @return the Block pointer if idx is not 0; nullptr for idx == 0, and the
   * caller should handle this case
   */
  pmem::Block* get(LogicalBlockIdx idx) {
    if (unlikely(idx == 0)) return nullptr;
    // super fast path: within first_region, no need touch concurrent vector
    if (idx < first_region_num_blocks) return &first_region[idx];

    // fast path: just look up
    uint32_t chunk_idx =
        (idx - first_region_num_blocks) >> GROW_UNIT_IN_BLOCK_SHIFT;
    uint32_t chunk_local_idx = idx & GROW_UNIT_IN_BLOCK_MASK;
    bool need_resize = true;
    if (chunk_idx < table.size()) {
      need_resize = false;
      pmem::Block* chunk_addr = table[chunk_idx];
      if (chunk_addr) return chunk_addr + chunk_local_idx;
    }

    // ensure this idx has real blocks allocated; do allocation if not
    uint32_t old_num_blocks = meta->get_num_blocks();
    assert(IS_ALIGNED(old_num_blocks, NUM_BLOCKS_PER_GROW));
    if (idx < old_num_blocks) {
      // only mmap the wanted chunk; not need to allocate
      LogicalBlockIdx chunk_begin_lidx = idx & ~GROW_UNIT_IN_BLOCK_MASK;
      pmem::Block* chunk_addr =
          mmap_file(GROW_UNIT_SIZE,
                    static_cast<off_t>(BLOCK_IDX_TO_SIZE(chunk_begin_lidx)),
                    MAP_POPULATE);
      if (need_resize) {
        uint32_t next_pow2 =
            1 << (sizeof(chunk_idx) * 8 - std::countl_zero(chunk_idx));
        table.resize(next_pow2);
      }
      assert(!table[chunk_idx]);
      table[chunk_idx] = chunk_addr;
      return chunk_addr + chunk_local_idx;
    }
    uint32_t alloc_num_blocks =
        std::max(old_num_blocks / GROW_UNIT_FACTOR, idx + 1 - old_num_blocks);
    alloc_num_blocks = ALIGN_UP(alloc_num_blocks, NUM_BLOCKS_PER_GROW);
    uint32_t new_num_blocks = old_num_blocks + alloc_num_blocks;
    int ret = posix::fallocate(
        fd, 0, 0, static_cast<off_t>(BLOCK_IDX_TO_SIZE(new_num_blocks)));
    PANIC_IF(ret, "fd %d: fallocate failed", fd);
    meta->set_num_blocks_if_larger(new_num_blocks);

    pmem::Block* chunks_addr = mmap_file(
        BLOCK_SIZE_TO_IDX(alloc_num_blocks),
        static_cast<off_t>(BLOCK_IDX_TO_SIZE(old_num_blocks)), MAP_POPULATE);
    uint32_t chunk_begin_idx =
        (old_num_blocks - first_region_num_blocks) >> GROW_UNIT_IN_BLOCK_SHIFT;
    uint32_t chunk_end_idx =
        (new_num_blocks - first_region_num_blocks) >> GROW_UNIT_IN_BLOCK_SHIFT;

    // FIXME: can there be a race?
    if (chunk_end_idx > table.size()) {
      uint32_t next_pow2 =
          1 << (sizeof(chunk_end_idx) * 8 - std::countl_zero(chunk_end_idx));
      table.resize(next_pow2);
    }

    for (uint32_t chunk_curr_idx = chunk_begin_idx;
         chunk_curr_idx < chunk_end_idx; ++chunk_curr_idx) {
      assert(!table[chunk_curr_idx]);
      table[chunk_curr_idx] =
          chunks_addr +
          ((chunk_curr_idx - chunk_begin_idx) >> GROW_UNIT_IN_BLOCK_SHIFT);
    }
    return chunks_addr +
           ((chunk_idx - chunk_begin_idx) >> GROW_UNIT_IN_BLOCK_SHIFT) +
           chunk_local_idx;
  }

 private:
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
  friend std::ostream& operator<<(std::ostream& out, const MemTable& m);
};

}  // namespace ulayfs::dram
