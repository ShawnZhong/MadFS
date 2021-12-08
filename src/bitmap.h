#pragma once

#include <bit>
#include <bitset>
#include <cstdint>
#include <ostream>

#include "idx.h"
#include "utils.h"

namespace ulayfs {

// how many blocks a bitmap can manage
// (that's why call it "capacity" instead of "size")
constexpr static uint32_t BITMAP_CAPACITY_SHIFT = 6;
constexpr static uint32_t BITMAP_CAPACITY = 1 << BITMAP_CAPACITY_SHIFT;

namespace dram {
// All member functions are thread-safe and require no locks
// TODO: move to namespace dram
class Bitmap {
 private:
  uint64_t bitmap;

 public:
  constexpr static uint64_t BITMAP_ALL_USED = 0xffffffffffffffff;

  // return the index of the bit (0-63); -1 if fail
  BitmapLocalIdx alloc_one() {
  retry:
    uint64_t b = __atomic_load_n(&bitmap, __ATOMIC_ACQUIRE);
    if (b == BITMAP_ALL_USED) return -1;
    uint64_t allocated = (~b) & (b + 1);  // which bit is allocated
    // if bitmap is exactly the same as we saw previously, set it allocated
    if (!__atomic_compare_exchange_n(&bitmap, &b, b & allocated, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
      goto retry;
    return static_cast<BitmapLocalIdx>(std::countr_zero(b));
  }

  // allocate all blocks in this bit; return 0 if succeeds, -1 otherwise
  BitmapLocalIdx alloc_all() {
    uint64_t expected = 0;
    if (__atomic_load_n(&bitmap, __ATOMIC_ACQUIRE) != 0) return -1;
    if (!__atomic_compare_exchange_n(&bitmap, &expected, BITMAP_ALL_USED, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
      return -1;
    return 0;
  }

  // free blocks in [begin_idx, begin_idx + len)
  void free(BitmapLocalIdx begin_idx, uint32_t len) {
  retry:
    uint64_t b = __atomic_load_n(&bitmap, __ATOMIC_ACQUIRE);
    uint64_t freed = b & ~(((1 << len) - 1) << begin_idx);
    if (!__atomic_compare_exchange_n(&bitmap, &b, freed, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
      goto retry;
  }

  // WARN: not thread-safe
  void set_allocated(uint32_t idx) { bitmap |= (1 << idx); }

  // WARN: not thread-safe
  void set_unallocated(uint32_t idx) { bitmap &= ~(1 << idx); }

  // get a read-only snapshot of bitmap
  [[nodiscard]] uint64_t get() const { return bitmap; }

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

  /**
   * free the blocks within index range [begin, begin + len)
   * here we assume that [begin, begin + len) is within the same bitmap
   *
   * @param bitmaps a pointer to an array of bitmaps
   * @param begin the BitmapLocalIndex starting from which it will be freed
   * @param len the number of bits to be freed
   */
  static void free(Bitmap bitmaps[], BitmapLocalIdx begin, size_t len) {
    bitmaps[begin >> BITMAP_CAPACITY_SHIFT].free(
        begin & (BITMAP_CAPACITY_SHIFT - 1), len);
  }

  friend std::ostream& operator<<(std::ostream& out, const Bitmap& b) {
    out << std::bitset<64>(b.bitmap);
    return out;
  }
};

static_assert(sizeof(Bitmap) == 8, "Bitmap must of 64 bits");

}  // namespace dram
}  // namespace ulayfs
