#include "layout.h"

namespace ulayfs::pmem {

/**
 * @param entries a pointer to an array of tx entries
 * @param num_entries the total number of entries in the array
 * @param entry the target entry to be appended
 * @param hint hint to the tail of the log
 * @return
 */
inline TxLocalIdx try_append(TxEntry entries[], uint32_t num_entries,
                             TxEntry entry, TxLocalIdx hint) {
  for (TxLocalIdx idx = hint; idx < num_entries; ++idx) {
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

/*
 *  MetaBlock
 */

TxLocalIdx MetaBlock::inline_try_begin(TxBeginEntry entry, TxLocalIdx hint) {
  return try_append(inline_tx_entries, NUM_INLINE_TX_ENTRY, entry.data, hint);
}

TxLocalIdx MetaBlock::inline_try_commit(TxCommitEntry entry, TxLocalIdx hint) {
  // TODO: OCC
  return try_append(inline_tx_entries, NUM_INLINE_TX_ENTRY, entry.data, hint);
}

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

TxLocalIdx TxLogBlock::try_begin(TxBeginEntry begin_entry, TxLocalIdx hint) {
  return try_append(tx_entries, NUM_TX_ENTRY, begin_entry.data, hint);
}

TxLocalIdx TxLogBlock::try_commit(TxCommitEntry commit_entry, TxLocalIdx hint) {
  // FIXME: this one is actually wrong. In OCC, we have to verify there is no
  // new transcation overlap with our range
  return try_append(tx_entries, NUM_TX_ENTRY, commit_entry.data, hint);
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
