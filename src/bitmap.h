#pragma once

#include <atomic>
#include <bit>
#include <bitset>
#include <cstdint>
#include <ostream>

#include "const.h"
#include "idx.h"
#include "logging.h"
#include "utils.h"

namespace ulayfs::dram {
// All member functions are thread-safe and require no locks
class Bitmap {
 private:
  std::atomic_uint64_t bitmap;

 public:
  constexpr static uint64_t BITMAP_ALL_USED = 0xffffffffffffffff;

  // return the index of the bit (0-63); -1 if fail
  BitmapIdx alloc_one() {
  retry:
    uint64_t b = bitmap.load(std::memory_order_acquire);
    if (b == BITMAP_ALL_USED) return -1;
    uint64_t allocated = (~b) & (b + 1);  // which bit is allocated
    // if bitmap is exactly the same as we saw previously, set it allocated
    if (!bitmap.compare_exchange_strong(b, b & allocated,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire))
      goto retry;
    return static_cast<BitmapIdx>(std::countr_zero(b));
  }

  // allocate all blocks in this bit; return 0 if succeeds, -1 otherwise
  BitmapIdx alloc_all() {
    uint64_t expected = 0;
    if (!bitmap.compare_exchange_strong(expected, BITMAP_ALL_USED,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire))
      return -1;
    return 0;
  }

  // allocate all blocks in this bitmap that's unused
  // need to manually check contiguous bits
  uint64_t alloc_rest() {
    return bitmap.exchange(BITMAP_ALL_USED, std::memory_order_acq_rel);
  }

  // free blocks in [begin_idx, begin_idx + len)
  void free(BitmapIdx begin_idx, uint32_t len) {
  retry:
    uint64_t b = bitmap.load(std::memory_order_acquire);
    uint64_t freed = b & ~((((uint64_t)1 << len) - 1) << begin_idx);
    if (!bitmap.compare_exchange_strong(b, freed, std::memory_order_acq_rel,
                                        std::memory_order_acquire))
      goto retry;
  }

  // WARN: not thread-safe
  void set_allocated(uint32_t idx) {
    bitmap.store(bitmap.load(std::memory_order_relaxed) | (1UL << idx),
                 std::memory_order_relaxed);
  }

  // WARN: not thread-safe
  void set_unallocated(uint32_t idx) {
    bitmap.store(bitmap.load(std::memory_order_relaxed) & ~(1UL << idx),
                 std::memory_order_relaxed);
  }

  void set_allocated_all() {
    bitmap.store(BITMAP_ALL_USED, std::memory_order_relaxed);
  }

  void set_unallocated_all() { bitmap.store(0, std::memory_order_relaxed); }

  // WARN: not thread-safe
  // get a read-only snapshot of bitmap
  [[nodiscard]] bool is_allocated(uint32_t idx) const {
    return bitmap.load(std::memory_order_relaxed) & (1UL << idx);
  }

  /*** Below are static functions for allocation from a bitmap array ***/

  /**
   * a static helper function for allocating a single block in the bitmap
   * also used for managing MetaBlock::inline_bitmap
   *
   * @param bitmaps a pointer to an array of bitmaps
   * @param num_bitmaps the total number of bitmaps in the array
   * @param hint hint to the empty bit
   */
  static BitmapIdx alloc_one(Bitmap bitmaps[], uint32_t num_bitmaps,
                             BitmapIdx hint) {
    BitmapIdx ret;
    uint32_t idx = static_cast<uint32_t>(hint) >> BITMAP_BLOCK_CAPACITY_SHIFT;
    for (; idx < num_bitmaps; ++idx) {
      ret = bitmaps[idx].alloc_one();
      if (ret >= 0) return (idx << BITMAP_BLOCK_CAPACITY_SHIFT) + ret;
    }
    return -1;
  }

  /**
   * a static helper function for allocating 64 blocks in the bitmap
   * also used for managing MetaBlock::inline_bitmap
   *
   * @param bitmaps a pointer to an array of bitmaps
   * @param num_bitmaps the total number of bitmaps in the array
   * @param hint hint to the empty bit
   * @return the BitmapIdx
   */
  static BitmapIdx alloc_batch(Bitmap bitmaps[], uint32_t num_bitmaps,
                               BitmapIdx hint) {
    BitmapIdx ret;
    uint32_t idx = static_cast<uint32_t>(hint) >> BITMAP_BLOCK_CAPACITY_SHIFT;
    for (; idx < num_bitmaps; ++idx) {
      ret = bitmaps[idx].alloc_all();
      if (ret >= 0) return idx << BITMAP_BLOCK_CAPACITY_SHIFT;
    }
    return -1;
  }

  /**
   * try to allocate from hint until one bitmap contains at least one available
   * block
   *
   * @param[in] bitmaps a pointer to an array of bitmaps
   * @param[in] num_bitmaps the total number of bitmaps in the arrary
   * @param[in] hint hint to search
   * @param[out] allocated_bits the bitmap with blocks available (0 stands for
   * not previous occupied and thus useable)
   * @return the index of current bitmap
   */
  static BitmapIdx try_alloc(Bitmap bitmaps[], uint32_t num_bitmaps,
                             BitmapIdx hint, uint64_t& allocated_bits) {
    uint32_t idx = static_cast<uint32_t>(hint) >> BITMAP_BLOCK_CAPACITY_SHIFT;
    for (; idx < num_bitmaps; ++idx) {
      allocated_bits = bitmaps[idx].alloc_rest();
      if (allocated_bits != BITMAP_ALL_USED)
        return idx << BITMAP_BLOCK_CAPACITY_SHIFT;
    }
    return -1;
  }

  /**
   * free the blocks within index range [begin, begin + len)
   * here we assume that [begin, begin + len) is within the same bitmap
   *
   * @param bitmaps a pointer to an array of bitmaps
   * @param begin the BitmapLocalIndex starting from which it will be freed
   * @param len the number of bits to be freed
   */
  static void free(Bitmap bitmaps[], BitmapIdx begin, uint8_t len) {
    LOG_TRACE("Freeing [%d, %d)", begin, begin + len);

    bitmaps[static_cast<uint32_t>(begin) >> BITMAP_BLOCK_CAPACITY_SHIFT].free(
        static_cast<uint32_t>(begin) & (BITMAP_BLOCK_CAPACITY - 1), len);
  }

  friend std::ostream& operator<<(std::ostream& out, const Bitmap& b) {
    for (size_t i = 0; i < BITMAP_BLOCK_CAPACITY; ++i) {
      out << (b.bitmap.load(std::memory_order_relaxed) & (1l << i) ? "1" : "0");
    }
    return out;
  }
};

static_assert(sizeof(Bitmap) == BITMAP_SIZE, "Bitmap must of 64 bits");

}  // namespace ulayfs::dram
