#include "layout.h"

namespace ulayfs::pmem {

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