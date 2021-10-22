#include "layout.h"

namespace ulayfs::pmem {

/*
 *  Bitmap
 */

BlockLocalIdx Bitmap::alloc_one() {
retry:
  uint64_t b = __atomic_load_n(&bitmap, __ATOMIC_ACQUIRE);
  if (b == BITMAP_ALL_USED) return -1;
  uint64_t allocated = (~b) & (b + 1);  // which bit is allocated
  // if bitmap is exactly the same as we saw previously, set it allocated
  if (!__atomic_compare_exchange_n(&bitmap, &b, b & allocated, true,
                                   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    goto retry;
  persist_cl_fenced(&bitmap);
  return std::countr_zero(b);
}

BlockLocalIdx Bitmap::alloc_all() {
  uint64_t expected = 0;
  if (__atomic_load_n(&bitmap, __ATOMIC_ACQUIRE) != 0) return -1;
  if (!__atomic_compare_exchange_n(&bitmap, &expected, BITMAP_ALL_USED, false,
                                   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
    return -1;
  persist_cl_fenced(&bitmap);
  return 0;
}

void Bitmap::set_allocated(uint32_t idx) {
  bitmap |= (1 << idx);
  persist_cl_fenced(&bitmap);
}

/*
 *  MetaBlock
 */

BlockLocalIdx MetaBlock::inline_alloc_one(BlockLocalIdx hint) {
  int ret;
  BlockLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
  // the first block's first bit is reserved (used by bitmap block itself)
  // if anyone allocates it, it must retry
  if (idx == 0) {
    ret = inline_bitmaps[0].alloc_one();
    if (ret == 0) ret = inline_bitmaps[0].alloc_one();
    if (ret > 0) return ret;
    ++idx;
  }
  for (; idx < NUM_INLINE_BITMAP; ++idx) {
    ret = inline_bitmaps[idx].alloc_one();
    if (ret < 0) continue;
    return (idx << BITMAP_CAPACITY_SHIFT) + ret;
  }
  return -1;
}

BlockLocalIdx MetaBlock::inline_alloc_batch(BlockLocalIdx hint) {
  int ret = 0;
  BlockLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
  // we cannot allocate a whole batch from the first bitmap
  if (idx == 0) ++idx;
  for (; idx < NUM_INLINE_TX_ENTRY; ++idx) {
    ret = inline_bitmaps[idx].alloc_all();
    if (ret < 0) continue;
    return (idx << BITMAP_CAPACITY_SHIFT);
  }
  return -1;
}

std::ostream& operator<<(std::ostream& out, const MetaBlock& b) {
  out << "MetaBlock: \n";
  out << "\tsignature: \"" << b.signature << "\"\n";
  out << "\tfilesize: " << b.file_size << "\n";
  out << "\tnum_blocks: " << b.num_blocks << "\n";
  return out;
}

/*
 *  BitmapBlock
 */

BlockLocalIdx BitmapBlock::alloc_one(BlockLocalIdx hint) {
  int ret;
  BlockLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
  if (idx == 0) {
    ret = bitmaps[0].alloc_one();
    if (ret == 0) ret = bitmaps[0].alloc_one();
    if (ret > 0) return ret;
    ++idx;
  }
  for (; idx < NUM_BITMAP; ++idx) {
    ret = bitmaps[idx].alloc_one();
    if (ret < 0) continue;
    return (idx << BITMAP_CAPACITY_SHIFT) + ret;
  }
  return -1;
}

BlockLocalIdx BitmapBlock::alloc_batch(BlockLocalIdx hint) {
  int ret = 0;
  BlockLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
  if (idx == 0) ++idx;
  for (; idx < NUM_BITMAP; ++idx) {
    ret = bitmaps[idx].alloc_all();
    if (ret < 0) continue;
    return (idx << BITMAP_CAPACITY_SHIFT);
  }
  return -1;
}
}  // namespace ulayfs::pmem
