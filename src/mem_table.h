#pragma once

#include <linux/mman.h>
#include <tbb/concurrent_vector.h>

#include <cerrno>
#include <cstdint>
#include <iosfwd>
#include <tuple>

#include "block/block.h"
#include "config.h"
#include "const.h"
#include "idx.h"
#include "posix.h"
#include "utils/logging.h"
#include "utils/tbb.h"
#include "utils/utils.h"

namespace ulayfs::dram {

constexpr static uint32_t GROW_UNIT_IN_BLOCK_SHIFT =
    GROW_UNIT_SHIFT - BLOCK_SHIFT;
constexpr static uint32_t GROW_UNIT_IN_BLOCK_MASK =
    (1 << GROW_UNIT_IN_BLOCK_SHIFT) - 1;

// map LogicalBlockIdx into memory address
// this is a more low-level data structure than Allocator
// it should maintain the virtualization of infinite large of file
// everytime it gets a LogicalBlockIdx:
// - if this block is already mapped; return addr
// - if this block is allocated from kernel filesystem, mmap and return
//   the addr
// - if this block is not even allocated from kernel filesystem, grow_to_fit and
//   map it, and return the address
class MemTable : noncopyable {
  pmem::MetaBlock* meta;
  int fd;
  int prot;

  // immutable after ctor
  pmem::Block* first_region;
  uint32_t first_region_num_blocks;

  // map a chunk_idx to addr, where chunk_idx =
  // (lidx - first_region_num_blocks) >> GROW_UNIT_IN_BLOCK_SHIFT
  tbb::concurrent_vector<std::atomic<pmem::Block*>,
                         zero_allocator<std::atomic<pmem::Block*>>>
      table;
  static_assert(std::atomic<pmem::Block*>::is_always_lock_free);

  // a vector of <addr, length> pairs
  tbb::concurrent_vector<std::tuple<void*, size_t>> mmap_regions;

 public:
  MemTable(int fd, off_t init_file_size, bool read_only)
      : fd(fd), prot(read_only ? PROT_READ : PROT_READ | PROT_WRITE) {
    bool is_empty = init_file_size == 0;
    // grow to multiple of grow_unit_size if the file is empty or the file
    // size is not grow_unit aligned
    bool should_grow = is_empty || !is_aligned(init_file_size, GROW_UNIT_SIZE);
    off_t file_size = init_file_size;
    if (should_grow) {
      file_size =
          is_empty ? PREALLOC_SIZE : align_up(init_file_size, GROW_UNIT_SIZE);
      int ret = posix::fallocate(fd, 0, 0, file_size);
      PANIC_IF(ret < 0, "fallocate failed");
    }

    first_region = mmap_file(static_cast<size_t>(file_size), 0, 0);
    first_region_num_blocks = BLOCK_SIZE_TO_IDX(file_size);
    meta = &first_region[0].meta_block;
    if (!is_empty && !meta->is_valid())
      throw FileInitException("invalid meta block");

    // update the mata block if necessary
    if (should_grow)
      meta->set_num_logical_blocks_if_larger(first_region_num_blocks);
  }

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
  pmem::Block* lidx_to_addr_rw(LogicalBlockIdx idx) {
    if (unlikely(idx == 0)) return nullptr;
    // super fast path: within first_region, no need touch concurrent vector
    if (idx < first_region_num_blocks) return &first_region[idx.get()];

    // fast path: just look up
    uint32_t chunk_idx =
        (idx - first_region_num_blocks) >> GROW_UNIT_IN_BLOCK_SHIFT;
    uint32_t chunk_local_idx = idx & GROW_UNIT_IN_BLOCK_MASK;
    if (chunk_idx < table.size()) {
      pmem::Block* chunk_addr = table[chunk_idx];
      if (chunk_addr) return chunk_addr + chunk_local_idx;
    } else {
      table.grow_to_at_least(next_pow2(chunk_idx));
    }

    // ensure this idx has real blocks allocated; do allocation if not
    grow_to_fit(idx);

    LogicalBlockIdx chunk_begin_lidx = idx & ~GROW_UNIT_IN_BLOCK_MASK;
    pmem::Block* chunk_addr = mmap_file(
        GROW_UNIT_SIZE, static_cast<off_t>(BLOCK_IDX_TO_SIZE(chunk_begin_lidx)),
        MAP_POPULATE);
    table[chunk_idx] = chunk_addr;
    return chunk_addr + chunk_local_idx;
  }

  [[nodiscard]] const pmem::Block* lidx_to_addr_ro(LogicalBlockIdx lidx) {
    constexpr static const char __attribute__((aligned(BLOCK_SIZE)))
    empty_block[BLOCK_SIZE]{};
    if (lidx == 0) return reinterpret_cast<const pmem::Block*>(&empty_block);
    return lidx_to_addr_rw(lidx);
  }

 private:
  // ask more blocks for the kernel filesystem, so that idx is valid
  void grow_to_fit(LogicalBlockIdx idx) {
    // fast path: if smaller than the number of block; return
    if (idx < meta->get_num_logical_blocks()) return;

    // slow path: acquire lock to verify and grow_to_fit if necessary
    // the new file size should be a multiple of grow_to_fit unit
    // we have `idx + 1` since we want to grow_to_fit the file when idx is a
    // multiple of the number of blocks in a grow_to_fit unit (e.g., 512 for 2
    // MB grow_to_fit)
    uint64_t file_size = align_up(BLOCK_IDX_TO_SIZE(idx + 1), GROW_UNIT_SIZE);

    int ret = posix::fallocate(fd, 0, 0, static_cast<off_t>(file_size));
    PANIC_IF(ret, "fd %d: fallocate failed", fd);
    meta->set_num_logical_blocks_if_larger(BLOCK_SIZE_TO_IDX(file_size));
  }

  /**
   * a private helper function that calls mmap internally
   * @return the pointer to the first block on the persistent memory
   */
  pmem::Block* mmap_file(size_t length, off_t offset, int flags = 0) {
    if constexpr (BuildOptions::map_sync)
      flags |= MAP_SHARED_VALIDATE | MAP_SYNC;
    else
      flags |= MAP_SHARED;
    if constexpr (BuildOptions::map_populate) flags |= MAP_POPULATE;

    void* addr = posix::mmap(nullptr, length, prot, flags, fd, offset);

    if (unlikely(addr == MAP_FAILED)) {
      if constexpr (BuildOptions::map_sync) {
        if (errno == EOPNOTSUPP) {
          LOG_WARN("MAP_SYNC not supported for fd = %d. Retry w/o MAP_SYNC",
                   fd);
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
  friend std::ostream& operator<<(std::ostream& out, const MemTable& m) {
    out << "MemTable:\n";
    out << "\t" << 0 << " - " << m.first_region_num_blocks << ": "
        << m.first_region << "\n";

    uint32_t chunk_idx = m.first_region_num_blocks >> GROW_UNIT_IN_BLOCK_SHIFT;
    for (const auto& mem_addr : m.table) {
      LogicalBlockIdx chunk_begin_lidx = chunk_idx << GROW_UNIT_IN_BLOCK_SHIFT;
      out << "\t" << chunk_begin_lidx << " - "
          << chunk_begin_lidx + NUM_BLOCKS_PER_GROW << ": " << mem_addr << "\n";
      ++chunk_idx;
    }
    return out;
  }
};

}  // namespace ulayfs::dram
