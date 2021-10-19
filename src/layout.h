#pragma once

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "config.h"
#include "futex.h"

namespace ulayfs::pmem {

// block index within a file; the meta block has a LogicalBlockIdx of 0
using LogicalBlockIdx = uint32_t;
// block index seen by applications
using VirtualBlockIdx = uint32_t;
// local index within a block; this can be -1 to indicate an error
using BlockLocalIdx = int32_t;
// identifier of bitmap blocks; checkout BitmapBlock's doc to see more
using BitmapBlockId = uint32_t;

// All member functions are thread-safe and require no locks
class Bitmap {
  uint64_t bitmap;

 public:
  constexpr static uint64_t BITMAP_ALL_USED = 0xffffffffffffffff;

  // return the index of the bit (0-63); -1 if fail
  BlockLocalIdx alloc_one() {
  retry:
    uint64_t b = __atomic_load_n(&bitmap, __ATOMIC_ACQUIRE);
    if (b == BITMAP_ALL_USED) return -1;
    uint64_t allocated = (~b) & (b + 1);  // which bit is allocated
    // if bitmap is exactly the same as we saw previously, set it allocated
    if (!__atomic_compare_exchange_n(&bitmap, &b, b & allocated, true,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
      goto retry;
    return std::countr_zero(b);
  }

  // allocate all blocks in this bit; return 0 if succeeds, -1 otherwise
  BlockLocalIdx alloc_all() {
    uint64_t expected = 0;
    if (__atomic_load_n(&bitmap, __ATOMIC_ACQUIRE) != 0) return -1;
    if (!__atomic_compare_exchange_n(&bitmap, &expected, BITMAP_ALL_USED, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
      return -1;
    return 0;
  }

  void set_allocated(uint32_t idx) { bitmap |= (1 << idx); }

  // get a read-only snapshot of bitmap
  [[nodiscard]] uint64_t get() const { return bitmap; }
};

class TxEntry {
 public:
  uint64_t entry;
};

class TxBeginEntry : public TxEntry {};

class TxCommitEntry : public TxEntry {};

enum LogOp : uint8_t {
  LOG_OVERWRITE,
};

// Since allocator can only guarantee to allocate 64 contiguous blocks (by
// single CAS), log entry must organize as an linked list in case of a large
// size transcation.
// next_entry_block_idx and next_entry_offset points to the location of the next
// entry.
class LogEntry {
  enum LogOp op;
  uint8_t num_blocks;
  uint8_t next_entry_offset;
  uint8_t padding;
  LogicalBlockIdx next_entry_block_idx;
  LogicalBlockIdx file_offset;
  LogicalBlockIdx block_offset;
};

static_assert(sizeof(LogEntry) == 16, "LogEntry must of size 16 bytes");

// signature
constexpr static int SIGNATURE_LEN = 16;
constexpr static char FILE_SIGNATURE[SIGNATURE_LEN] = "ULAYFS";

// hardware configuration
constexpr static uint32_t BLOCK_SHIFT = 12;
constexpr static uint32_t BLOCK_SIZE = 1 << BLOCK_SHIFT;
constexpr static uint32_t CACHELINE_SHIFT = 6;
constexpr static uint32_t CACHELINE_SIZE = 1 << CACHELINE_SHIFT;

// how many blocks a bitmap can manage
// (that's why call it "capacity" instead of "size")
constexpr static uint32_t BITMAP_CAPACITY_SHIFT = 6;
constexpr static uint32_t BITMAP_CAPACITY = 1 << BITMAP_CAPACITY_SHIFT;

// number of various data structures in blocks
constexpr static uint32_t NUM_BITMAP = BLOCK_SIZE / sizeof(Bitmap);
constexpr static uint32_t NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(LogicalBlockIdx)) / sizeof(TxEntry);
constexpr static uint32_t NUM_LOG_ENTRY = BLOCK_SIZE / sizeof(LogEntry);

// inline data structure count in meta block
constexpr static uint32_t NUM_CL_BITMAP_IN_META = 32;
constexpr static uint32_t NUM_CL_TX_ENTRY_IN_META =
    ((BLOCK_SIZE / CACHELINE_SIZE) - 2) - NUM_CL_BITMAP_IN_META;
constexpr static uint32_t NUM_INLINE_BITMAP =
    NUM_CL_BITMAP_IN_META * (CACHELINE_SIZE / sizeof(Bitmap));
constexpr static uint32_t NUM_INLINE_TX_ENTRY =
    NUM_CL_TX_ENTRY_IN_META * (CACHELINE_SIZE / sizeof(TxEntry));

// how many blocks a bitmap block can manage
constexpr static uint32_t BITMAP_BLOCK_CAPACITY_SHIFT =
    BITMAP_CAPACITY_SHIFT + (BLOCK_SHIFT - 3);  // 15
constexpr static uint32_t BITMAP_BLOCK_CAPACITY =
    1 << BITMAP_BLOCK_CAPACITY_SHIFT;

// 32 cacheline corresponds to 2^14 bits
constexpr static uint32_t INLINE_BITMAP_CAPACITY =
    NUM_INLINE_BITMAP * BITMAP_CAPACITY;

/*
 * LogicalBlockIdx 0 -> MetaBlock; other blocks can be any type of blocks
 */
class MetaBlock {
 public:
  // contents in the first cache line
  union {
    struct {
      // file signature
      char signature[SIGNATURE_LEN];

      // file size in bytes (logical size to users)
      uint64_t file_size;

      // total number of blocks actually in this file (including unused ones)
      uint32_t num_blocks;

      // if inline_bitmaps is used up, this points to the next bitmap block
      LogicalBlockIdx bitmap_head;

      // if inline_tx_entries is used up, this points to the next log block
      LogicalBlockIdx log_head;

      // hint to find log tail; not necessarily up-to-date
      LogicalBlockIdx log_tail;
    };

    // padding avoid cache line contention
    char padding1[CACHELINE_SIZE];
  };

  union {
    // address for futex to lock, 4 bytes in size
    // this lock is ONLY used for ftruncate
    Futex meta_lock;

    // set futex to another cacheline to avoid futex's contention affect reading
    // the metadata above
    char padding2[CACHELINE_SIZE];
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
  // only called if a new file is created
  void init() {
    // the first block is always used (by MetaBlock itself)
    memcpy(signature, FILE_SIGNATURE, SIGNATURE_LEN);
    meta_lock.init();
  }

  // check whether the meta block is valid
  bool is_valid() {
    return std::memcmp(signature, FILE_SIGNATURE, SIGNATURE_LEN) == 0;
  }

  // acquire/release meta lock (usually only during allocation)
  void lock() { meta_lock.acquire(); }
  void unlock() { meta_lock.release(); }

  // allocate one block; return the index of allocated block
  // accept a hint for which bit to start searching
  // usually hint can just be the last idx return by this function
  BlockLocalIdx inline_alloc_one(BlockLocalIdx hint = 0);

  // 64 blocks are considered as one batch; return the index of the first block
  BlockLocalIdx inline_alloc_batch(BlockLocalIdx hint = 0);

  friend std::ostream& operator<<(std::ostream& out, const MetaBlock& block);
};

/*
 * In the current design, the inline bitmap in the meta block can manage 16k
 * blocks (64MB in total); after that, every 32k blocks (128MB) will have its
 * first block as the bitmap block that manages its allocation.
 * We assign "bitmap_block_id" to these bitmap blocks, where id=0 is the inline
 * one in the meta block (LogicalBlockIdx=0); bitmap block id=1 is the block
 * with LogicalBlockIdx 16384; id=2 is the one with LogicalBlockIdx 32768, etc.
 */
class BitmapBlock {
 public:
  Bitmap bitmaps[NUM_BITMAP];

 public:
  // first bit of is the bitmap block itself
  void init() { bitmaps[0].set_allocated(0); }

  // allocate one block; return the index of allocated block
  // accept a hint for which bit to start searching
  // usually hint can just be the last idx return by this function
  BlockLocalIdx alloc_one(BlockLocalIdx hint = 0);

  // 64 blocks are considered as one batch; return the index of the first block
  BlockLocalIdx alloc_batch(BlockLocalIdx hint = 0);

  // map `bitmap_local_idx` from alloc_one/all to the LogicalBlockIdx
  static LogicalBlockIdx get_block_idx(BitmapBlockId bitmap_block_id,
                                       BlockLocalIdx bitmap_local_idx) {
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

class TxLogBlock {
  LogicalBlockIdx prev;
  LogicalBlockIdx next;
  TxEntry tx_entries[NUM_TX_ENTRY];

 public:
  // FIXME: this one is actually wrong. In OCC, we have to verify there is no
  // new transcation overlap with our range
  BlockLocalIdx try_commit(TxCommitEntry commit_entry,
                           BlockLocalIdx hint_tail = 0) {
    for (BlockLocalIdx idx = hint_tail; idx < NUM_LOG_ENTRY; ++idx) {
      uint64_t expected = 0;
      if (__atomic_load_n(&tx_entries[idx].entry, __ATOMIC_ACQUIRE)) continue;
      if (__atomic_compare_exchange_n(&tx_entries[idx].entry, &expected,
                                      commit_entry.entry, false,
                                      __ATOMIC_RELEASE, __ATOMIC_ACQUIRE))
        return idx;
    }
    return -1;
  }
};

class LogEntryBlock {
  LogEntry log_entries[NUM_LOG_ENTRY];
};

class DataBlock {
  char data[BLOCK_SIZE];
};

union Block {
  MetaBlock meta_block;
  BitmapBlock bitmap_block;
  TxLogBlock tx_log_block;
  LogEntryBlock log_entry_block;
  DataBlock data_block;
  char padding[BLOCK_SIZE];
};

static_assert(sizeof(Bitmap) == 8, "Bitmap must of 64 bits");
static_assert(sizeof(TxEntry) == 8, "TxEntry must be 64 bits");
static_assert(sizeof(LogEntry) == 16, "LogEntry must of size 16 bytes");
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

};  // namespace ulayfs::pmem
