#include "layout.h"

namespace ulayfs::pmem {

/*
 *  MetaBlock
 */

BitmapLocalIdx MetaBlock::inline_alloc_one(BitmapLocalIdx hint) {
  int ret;
  BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
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

BitmapLocalIdx MetaBlock::inline_alloc_batch(BitmapLocalIdx hint) {
  int ret = 0;
  BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
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
 *  TxLogBlock
 */

template <uint32_t NUM_ENTRY, typename Entry, typename LocalIdx>
LocalIdx try_append(Entry entries[], Entry entry, LocalIdx hint_tail = 0) {
  for (LocalIdx idx = hint_tail; idx < NUM_ENTRY; ++idx) {
    uint64_t expected = 0;
    if (__atomic_load_n(&entry, __ATOMIC_ACQUIRE)) continue;
    if (__atomic_compare_exchange_n(&entries[idx], &expected, entry, false,
                                    __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
      persist_cl_fenced(&entries[idx]);
      return idx;
    }
  }
  return -1;
}

TxLocalIdx TxLogBlock::try_begin(TxBeginEntry begin_entry,
                                 TxLocalIdx hint_tail) {
  return try_append<NUM_TX_ENTRY>(tx_entries, begin_entry.entry, hint_tail);
}

TxLocalIdx TxLogBlock::try_commit(TxCommitEntry commit_entry,
                                  TxLocalIdx hint_tail) {
  // FIXME: this one is actually wrong. In OCC, we have to verify there is no
  // new transcation overlap with our range
  return try_append<NUM_TX_ENTRY>(tx_entries, commit_entry.entry, hint_tail);
}

/*
 *  BitmapBlock
 */

BitmapLocalIdx BitmapBlock::alloc_one(BitmapLocalIdx hint) {
  int ret;
  BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
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

BitmapLocalIdx BitmapBlock::alloc_batch(BitmapLocalIdx hint) {
  int ret = 0;
  BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
  if (idx == 0) ++idx;
  for (; idx < NUM_BITMAP; ++idx) {
    ret = bitmaps[idx].alloc_all();
    if (ret < 0) continue;
    return (idx << BITMAP_CAPACITY_SHIFT);
  }
  return -1;
}

BitmapLocalIdx LogEntryBlock::try_append(LogEntry log_entry,
                                         LogLocalIdx hint_tail) {
  for (LogLocalIdx idx = hint_tail; idx < NUM_LOG_ENTRY; ++idx) {
    // we use the first word if a log entry is valid or not
    if (__atomic_load_n(&log_entries[idx].word1, __ATOMIC_ACQUIRE)) continue;

    // try to write the 1st word using CAS and write the 2nd word normally
    uint64_t expected = 0;
    if (__atomic_compare_exchange_n(&log_entries[idx].word1, &expected,
                                    log_entry.word1, false, __ATOMIC_RELEASE,
                                    __ATOMIC_ACQUIRE)) {
      log_entries[idx].word2 = log_entry.word2;
      persist_cl_fenced(&log_entries[idx]);
      return idx;
    }
  }
  return -1;
}

}  // namespace ulayfs::pmem
