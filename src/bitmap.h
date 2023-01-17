#pragma once

#include <atomic>
#include <bit>
#include <bitset>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <optional>
#include <ostream>
#include <sstream>

#include "const.h"
#include "idx.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace madfs::utility {
class Converter;
}

namespace madfs::dram {
class File;

// All member functions are thread-safe and require no locks
class BitmapEntry : noncopyable {
 private:
  std::atomic<uint64_t> entry;

 public:
  constexpr static uint64_t BITMAP_ALL_USED =
      std::numeric_limits<uint64_t>::max();

  // return the index of the bit (0-63)
  std::optional<BitmapIdx> alloc_one() {
  retry:
    uint64_t b = entry.load(std::memory_order_acquire);
    if (b == BITMAP_ALL_USED) return -1;
    uint64_t allocated = (~b) & (b + 1);  // which bit is allocated
    // if bitmap is exactly the same as we saw previously, set it allocated
    if (!entry.compare_exchange_strong(b, b & allocated,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire))
      goto retry;
    return static_cast<BitmapIdx>(std::countr_zero(b));
  }

  // allocate all blocks in this bit; return true on success
  bool alloc_all() {
    uint64_t expected = 0;
    if (!entry.compare_exchange_strong(expected, BITMAP_ALL_USED,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire))
      return false;
    return true;
  }

  // allocate all blocks in this bitmap that's unused
  // need to manually check contiguous bits
  uint64_t alloc_rest() {
    return entry.exchange(BITMAP_ALL_USED, std::memory_order_acq_rel);
  }

  // free blocks in [begin_idx, begin_idx + len)
  void free(BitmapIdx begin_idx, uint32_t len) {
  retry:
    uint64_t b = entry.load(std::memory_order_acquire);
    uint64_t freed = b & ~((((uint64_t)1 << len) - 1) << begin_idx);
    if (!entry.compare_exchange_strong(b, freed, std::memory_order_acq_rel,
                                       std::memory_order_acquire))
      goto retry;
  }

  // WARN: not thread-safe
  void set_allocated(uint32_t idx) {
    entry.store(entry.load(std::memory_order_relaxed) | (1UL << idx),
                std::memory_order_relaxed);
  }

  // WARN: not thread-safe
  void set_unallocated(uint32_t idx) {
    entry.store(entry.load(std::memory_order_relaxed) & ~(1UL << idx),
                std::memory_order_relaxed);
  }

  void set_allocated_all() {
    entry.store(BITMAP_ALL_USED, std::memory_order_relaxed);
  }

  void set_unallocated_all() { entry.store(0, std::memory_order_relaxed); }

  // WARN: not thread-safe
  // get a read-only snapshot of bitmap
  [[nodiscard]] bool is_allocated(uint32_t idx) const {
    return entry.load(std::memory_order_relaxed) & (1UL << idx);
  }

  [[nodiscard]] uint64_t is_empty() const {
    return entry.load(std::memory_order_relaxed) == 0;
  }

  friend std::ostream& operator<<(std::ostream& out, const BitmapEntry& b) {
    for (size_t i = 0; i < BITMAP_ENTRY_BLOCKS_CAPACITY; ++i) {
      out << (b.entry.load(std::memory_order_relaxed) & (1ul << i) ? "1" : "0");
    }
    return out;
  }
};

static_assert(sizeof(BitmapEntry) == BITMAP_ENTRY_SIZE,
              "BitmapEntry must of 64 bits");

class BitmapMgr : noncopyable {
  BitmapEntry* entries{nullptr};

  friend ::madfs::dram::File;
  friend ::madfs::utility::Converter;

 public:
  BitmapMgr() = default;
  void set_allocated(LogicalBlockIdx block_idx) const {
    entries[block_idx >> BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT].set_allocated(
        block_idx & (BITMAP_ENTRY_BLOCKS_CAPACITY - 1));
  }

  /**
   * allocate a single block in the bitmap
   * used for managing MetaBlock::inline_bitmap
   *
   * @param hint hint to the empty bit
   */
  [[nodiscard]] std::optional<BitmapIdx> alloc_one(BitmapIdx hint) const {
    uint32_t idx =
        static_cast<uint32_t>(hint) >> BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
    for (; idx < NUM_BITMAP_ENTRIES; ++idx) {
      if (auto ret = entries[idx].alloc_one(); ret.has_value())
        return (idx << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT) + ret.value();
    }
    return {};
  }

  /**
   * allocating 64 blocks in the bitmap
   * used for managing MetaBlock::inline_bitmap
   *
   * @param hint hint to the empty bit
   * @return the BitmapIdx
   */
  [[nodiscard]] std::optional<BitmapIdx> alloc_batch(BitmapIdx hint) const {
    uint32_t idx =
        static_cast<uint32_t>(hint) >> BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
    for (; idx < NUM_BITMAP_ENTRIES; ++idx) {
      if (bool success = entries[idx].alloc_all(); success) {
        return idx << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
      }
    }
    return {};
  }

  /**
   * try to allocate from hint until one bitmap contains at least one available
   * block
   *
   * @param hint hint to search
   * @return the index of current bitmap entry and the entry itself
   */
  [[nodiscard]] std::tuple<BitmapIdx, uint64_t> try_alloc(
      BitmapIdx hint) const {
    uint32_t idx =
        static_cast<uint32_t>(hint) >> BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
    for (; idx < NUM_BITMAP_ENTRIES; ++idx) {
      uint64_t allocated_bits = entries[idx].alloc_rest();
      if (allocated_bits != BitmapEntry::BITMAP_ALL_USED)
        return {idx << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT, allocated_bits};
    }
    PANIC("Failed to allocate from bitmap");
  }

  /**
   * free the blocks within index range [begin, begin + len)
   * here we assume that [begin, begin + len) is within the same bitmap
   *
   * @param begin the BitmapLocalIndex starting from which it will be freed
   * @param len the number of bits to be freed
   */
  void free(BitmapIdx begin, uint8_t len) const {
    LOG_TRACE("Freeing [%d, %d)", begin, begin + len);

    entries[static_cast<uint32_t>(begin) >> BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT]
        .free(static_cast<uint32_t>(begin) & (BITMAP_ENTRY_BLOCKS_CAPACITY - 1),
              len);
  }

  friend std::ostream& operator<<(std::ostream& out, const BitmapMgr& b) {
    out << "BitmapMgr: \n";
    for (size_t i = 0; i < NUM_BITMAP_ENTRIES; ++i) {
      if (b.entries[i].is_empty()) continue;
      out << "\t" << std::setw(6) << std::right
          << i * BITMAP_ENTRY_BLOCKS_CAPACITY << " - " << std::setw(6)
          << std::left << (i + 1) * BITMAP_ENTRY_BLOCKS_CAPACITY - 1 << ": "
          << b.entries[i] << "\n";
    }
    return out;
  }
};

}  // namespace madfs::dram
