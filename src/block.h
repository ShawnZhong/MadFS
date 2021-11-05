#pragma once

#include "bitmap.h"
#include "entry.h"
#include "layout.h"

namespace ulayfs::pmem {
/**
 * The base class for all the blocks
 *
 * Remove copy/move constructor and assignment operator to avoid accidental copy
 */
class BaseBlock {
 public:
  BaseBlock(BaseBlock const&) = delete;
  BaseBlock(BaseBlock&&) = delete;
  BaseBlock& operator=(BaseBlock const&) = delete;
  BaseBlock& operator=(BaseBlock&&) = delete;
};

/**
 * In the current design, the inline bitmap in the meta block can manage 16k
 * blocks (64 MB in total); after that, every 32k blocks (128 MB) will have its
 * first block as the bitmap block that manages its allocation.
 * We assign "bitmap_block_id" to these bitmap blocks, where id=0 is the inline
 * one in the meta block (LogicalBlockIdx=0); bitmap block id=1 is the block
 * with LogicalBlockIdx 16384; id=2 is the one with LogicalBlockIdx 32768, etc.
 */
class BitmapBlock : public BaseBlock {
 private:
  Bitmap bitmaps[NUM_BITMAP];

 public:
  // first bit of is the bitmap block itself
  void init() { bitmaps[0].set_allocated(0); }

  // allocate one block; return the index of allocated block
  // accept a hint for which bit to start searching
  // usually hint can just be the last idx return by this function
  BitmapLocalIdx alloc_one(BitmapLocalIdx hint = 0) {
    return Bitmap::alloc_one(bitmaps, NUM_BITMAP, hint);
  }

  // 64 blocks are considered as one batch; return the index of the first block
  BitmapLocalIdx alloc_batch(BitmapLocalIdx hint = 0) {
    return Bitmap::alloc_batch(bitmaps, NUM_BITMAP, hint);
  }

  // map `bitmap_local_idx` from alloc_one/all to the LogicalBlockIdx
  static LogicalBlockIdx get_block_idx(BitmapBlockId bitmap_block_id,
                                       BitmapLocalIdx bitmap_local_idx) {
    if (bitmap_block_id == 0) return bitmap_local_idx;
    return (bitmap_block_id << BITMAP_BLOCK_CAPACITY_SHIFT) +
           INLINE_BITMAP_CAPACITY + bitmap_local_idx;
  }

  // make bitmap id to its block idx
  static LogicalBlockIdx get_bitmap_block_idx(BitmapBlockId bitmap_block_id) {
    return get_block_idx(bitmap_block_id, 0);
  }

  // reverse mapping of get_bitmap_block_idx
  static BitmapBlockId get_bitmap_block_id(LogicalBlockIdx idx) {
    return (idx - INLINE_BITMAP_CAPACITY) >> BITMAP_BLOCK_CAPACITY_SHIFT;
  }
};

class TxLogBlock : public BaseBlock {
  LogicalBlockIdx prev;
  LogicalBlockIdx next;
  TxEntry tx_entries[NUM_TX_ENTRY];

 public:
  TxLocalIdx find_tail(TxLocalIdx hint = 0) {
    return TxEntry::find_tail<NUM_TX_ENTRY>(tx_entries, hint);
  }

  uint64_t try_append(TxEntry entry, TxLocalIdx idx) {
    return TxEntry::try_append<NUM_TX_ENTRY>(tx_entries, entry, idx);
  }

  [[nodiscard]] TxEntry get(TxLocalIdx idx) {
    assert(idx >= 0 && idx < NUM_TX_ENTRY);
    return tx_entries[idx];
  }

  [[nodiscard]] LogicalBlockIdx get_next() { return next; }
  [[nodiscard]] LogicalBlockIdx get_prev() { return prev; }

  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const { return next; }

  /**
   * Set the next block index
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    bool success = __atomic_compare_exchange_n(
        &next, &expected, block_idx, true, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    if (success) persist_cl_fenced(&next);
    return success;
  }
};

// LogEntryBlock is per-thread to avoid contention
class LogEntryBlock : public BaseBlock {
  LogEntry log_entries[NUM_LOG_ENTRY];

 public:
  [[nodiscard]] const LogEntry& get(LogLocalIdx idx) {
    assert(idx >= 0 && idx < NUM_LOG_ENTRY);
    return log_entries[idx];
  }

  // TODO: linked list
  void set(LogLocalIdx idx, pmem::LogEntry entry, bool fenced = true,
           bool flushed = true) {
    log_entries[idx] = entry;
    if (!flushed) return;
    if (fenced)
      persist_cl_fenced(&log_entries[idx]);
    else
      persist_cl_unfenced(&log_entries[idx]);
  }
};

class DataBlock : public BaseBlock {
 public:
  char data[BLOCK_SIZE];
};

/*
 * LogicalBlockIdx 0 -> MetaBlock; other blocks can be any type of blocks
 */
class MetaBlock : public BaseBlock {
 private:
  // contents in the first cache line
  union {
    struct {
      // file signature
      char signature[SIGNATURE_SIZE];

      // if inline_tx_entries is used up, this points to the next log block
      LogicalBlockIdx next_tx_block;

      // hint to find tx log tail; not necessarily up-to-date
      TxEntryIdx tx_log_tail;
    };

    // padding avoid cache line contention
    char cl1[CACHELINE_SIZE];
  };

  // set futex to another cacheline to avoid futex's contention affect
  // reading the metadata above
  union {
    struct {
      // address for futex to lock, 4 bytes in size
      // this lock is ONLY used for ftruncate
      Futex meta_lock;

      // file size in bytes (logical size to users)
      // modifications to this usually requires meta_lock being held
      uint64_t file_size;

      // total number of blocks actually in this file (including unused ones)
      // modifications to this usually requires meta_lock being held
      uint32_t num_blocks;
    };

    // padding
    char cl2[CACHELINE_SIZE];
  };

  // for the rest of 62 cache lines:
  // 32 cache lines for bitmaps (~16k blocks = 64M)
  Bitmap inline_bitmaps[NUM_INLINE_BITMAP];

  // 30 cache lines for tx log (~120 txs)
  TxEntry inline_tx_entries[NUM_INLINE_TX_ENTRY];

  static_assert(sizeof(inline_bitmaps) == 32 * CACHELINE_SIZE,
                "inline_bitmaps must be 32 cache lines");

  static_assert(sizeof(inline_tx_entries) == 30 * CACHELINE_SIZE,
                "inline_tx_entries must be 30 cache lines");

 public:
  /**
   * only called if a new file is created
   * We can assume that all other fields are zero-initialized upon ftruncate
   */
  void init() {
    // the first block is always used (by MetaBlock itself)
    meta_lock.init();
    memcpy(signature, FILE_SIGNATURE, SIGNATURE_SIZE);

    persist_cl_fenced(&cl1);
  }

  // check whether the meta block is valid
  bool is_valid() {
    return std::memcmp(signature, FILE_SIGNATURE, SIGNATURE_SIZE) == 0;
  }

  // acquire/release meta lock (usually only during allocation)
  // we don't need to call persistence since futex is robust to crash
  void lock() { meta_lock.acquire(); }
  void unlock() { meta_lock.release(); }

  /*
   * Getters and setters
   */

  // called by other public functions with lock held
  void set_num_blocks_no_lock(uint32_t num_blocks) {
    this->num_blocks = num_blocks;
    persist_cl_fenced(&cl1);
  }

  /**
   * Set the next tx block index
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    bool success =
        __atomic_compare_exchange_n(&next_tx_block, &expected, block_idx, true,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    if (success) persist_cl_fenced(&next_tx_block);
    return success;
  }

  void set_tx_log_tail(TxEntryIdx tx_log_tail) {
    if (tx_log_tail <= this->tx_log_tail) return;
    this->tx_log_tail = tx_log_tail;
    persist_cl_fenced(&cl1);
  }

  [[nodiscard]] uint32_t get_num_blocks() const { return num_blocks; }
  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const {
    return next_tx_block;
  }
  [[nodiscard]] TxEntryIdx get_tx_log_tail() const { return tx_log_tail; }

  [[nodiscard]] TxEntry get_tx_entry(TxLocalIdx idx) const {
    assert(idx >= 0 && idx < NUM_INLINE_TX_ENTRY);
    return inline_tx_entries[idx];
  }

  /*
   * Methods for inline metadata
   */

  // allocate one block; return the index of allocated block
  // accept a hint for which bit to start searching
  // usually hint can just be the last idx return by this function
  BitmapLocalIdx inline_alloc_one(BitmapLocalIdx hint = 0) {
    return Bitmap::alloc_one(inline_bitmaps, NUM_INLINE_BITMAP, hint);
  }

  // 64 blocks are considered as one batch; return the index of the first
  // block
  BitmapLocalIdx inline_alloc_batch(BitmapLocalIdx hint = 0) {
    return Bitmap::alloc_batch(inline_bitmaps, NUM_INLINE_BITMAP, hint);
  }

  TxLocalIdx find_tail(TxLocalIdx hint = 0) {
    return TxEntry::find_tail<NUM_INLINE_TX_ENTRY>(inline_tx_entries, hint);
  }

  uint64_t try_append(TxEntry entry, TxLocalIdx idx) {
    return TxEntry::try_append<NUM_INLINE_TX_ENTRY>(inline_tx_entries, entry,
                                                    idx);
  }

  friend std::ostream& operator<<(std::ostream& out, const MetaBlock& block) {
    out << "MetaBlock: \n";
    out << "\tsignature: \"" << block.signature << "\"\n";
    out << "\tfilesize: " << block.file_size << "\n";
    out << "\tnum_blocks: " << block.num_blocks << "\n";
    out << "\tnext_tx_block: " << block.next_tx_block << "\n";
    out << "\ttx_log_tail: " << block.tx_log_tail << "\n";
    return out;
  }
};

union Block {
  MetaBlock meta_block;
  BitmapBlock bitmap_block;
  TxLogBlock tx_log_block;
  LogEntryBlock log_entry_block;
  DataBlock data_block;
  char data[BLOCK_SIZE];
};

static_assert(sizeof(MetaBlock) == BLOCK_SIZE,
              "MetaBlock must be of size BLOCK_SIZE");
static_assert(sizeof(BitmapBlock) == BLOCK_SIZE,
              "BitmapBlock must be of size BLOCK_SIZE");
static_assert(sizeof(TxLogBlock) == BLOCK_SIZE,
              "TxLogBlock must be of size BLOCK_SIZE");
static_assert(sizeof(LogEntryBlock) == BLOCK_SIZE,
              "LogEntryBlock must be of size BLOCK_SIZE");
static_assert(sizeof(DataBlock) == BLOCK_SIZE,
              "DataBlock must be of size BLOCK_SIZE");
static_assert(sizeof(Block) == BLOCK_SIZE, "Block must be of size BLOCK_SIZE");

}  // namespace ulayfs::pmem
