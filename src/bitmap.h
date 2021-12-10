#pragma once

#include <atomic>
#include <bit>
#include <cstdint>

#include "idx.h"
#include "utils.h"

namespace ulayfs {

// how many blocks a bitmap can manage
// (that's why call it "capacity" instead of "size")
constexpr static uint32_t BITMAP_CAPACITY_SHIFT = 6;
constexpr static uint32_t BITMAP_CAPACITY = 1 << BITMAP_CAPACITY_SHIFT;

namespace pmem {
// All member functions are thread-safe and require no locks
// TODO: move to namespace dram
class Bitmap {
 private:
  std::atomic_uint64_t bitmap;

 public:
  constexpr static uint64_t BITMAP_ALL_USED = 0xffffffffffffffff;

  // return the index of the bit (0-63); -1 if fail
  BitmapLocalIdx alloc_one() {
  retry:
    uint64_t b = bitmap.load(std::memory_order_acquire);
    if (b == BITMAP_ALL_USED) return -1;
    uint64_t allocated = (~b) & (b + 1);  // which bit is allocated
    // if bitmap is exactly the same as we saw previously, set it allocated
    if (!bitmap.compare_exchange_strong(b, b & allocated,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire))
      goto retry;
    persist_cl_fenced(&bitmap);
    return static_cast<BitmapLocalIdx>(std::countr_zero(b));
  }

  // allocate all blocks in this bit; return 0 if succeeds, -1 otherwise
  BitmapLocalIdx alloc_all() {
    uint64_t expected = 0;
    if (!bitmap.compare_exchange_strong(expected, BITMAP_ALL_USED,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire))
      return -1;
    persist_cl_fenced(&bitmap);
    return 0;
  }

  // WARN: not thread-safe
  void set_allocated(uint32_t idx) {
    bitmap.store(bitmap.load(std::memory_order_relaxed) | (1 << idx),
                 std::memory_order_relaxed);
    persist_cl_fenced(&bitmap);
  }

  // WARN: not thread-safe
  void set_unallocated(uint32_t idx) {
    bitmap.store(bitmap.load(std::memory_order_relaxed) & ~(1 << idx),
                 std::memory_order_relaxed);
    persist_cl_fenced(&bitmap);
  }

  // WARN: not thread-safe
  // get a read-only snapshot of bitmap
  [[nodiscard]] uint64_t get() const {
    return bitmap.load(std::memory_order_relaxed);
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
  static BitmapLocalIdx alloc_one(Bitmap bitmaps[], uint16_t num_bitmaps,
                                  BitmapLocalIdx hint) {
    BitmapLocalIdx ret;
    BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
    // the first block's first bit is reserved (used by bitmap block itself)
    // if anyone allocates it, it must retry
    if (idx == 0) {
      ret = bitmaps[0].alloc_one();
      if (ret == 0) ret = bitmaps[0].alloc_one();
      if (ret > 0) return ret;
      ++idx;
    }
    for (; idx < num_bitmaps; ++idx) {
      ret = bitmaps[idx].alloc_one();
      if (ret < 0) continue;
      return (idx << BITMAP_CAPACITY_SHIFT) + ret;
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
   * @return the BitmapLocalIdx
   */
  static BitmapLocalIdx alloc_batch(Bitmap bitmaps[], uint16_t num_bitmaps,
                                    BitmapLocalIdx hint) {
    BitmapLocalIdx ret;
    BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
    // we cannot allocate a whole batch from the first bitmap
    if (idx == 0) ++idx;
    for (; idx < num_bitmaps; ++idx) {
      ret = bitmaps[idx].alloc_all();
      if (ret < 0) continue;
      return idx << BITMAP_CAPACITY_SHIFT;
    }
    return -1;
  }
};

static_assert(sizeof(Bitmap) == 8, "Bitmap must of 64 bits");

}  // namespace pmem
}  // namespace ulayfs
