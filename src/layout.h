#pragma once

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "config.h"
#include "futex.h"
#include "params.h"
#include "utils.h"

namespace ulayfs::pmem {

// block index within a file; the meta block has a LogicalBlockIdx of 0
using LogicalBlockIdx = uint32_t;
// block index seen by applications
using VirtualBlockIdx = uint32_t;

// local index within a block; this can be -1 to indicate an error
using BitmapLocalIdx = int32_t;
using TxLocalIdx = int32_t;
using LogLocalIdx = int32_t;

// identifier of bitmap blocks; checkout BitmapBlock's doc to see more
using BitmapBlockId = uint32_t;

/**
 * A log entry is identified by the index of the LogEntryBlock and the local
 * index within the block
 *
 * 5 bytes (40 bits) in size
 */
class __attribute__((packed)) LogEntryIdx {
  LogicalBlockIdx block_idx;
  LogLocalIdx local_idx : 8;
};

// All member functions are thread-safe and require no locks
class Bitmap {
 private:
  uint64_t bitmap;

 public:
  constexpr static uint64_t BITMAP_ALL_USED = 0xffffffffffffffff;

  // return the index of the bit (0-63); -1 if fail
  inline BitmapLocalIdx alloc_one() {
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

  // allocate all blocks in this bit; return 0 if succeeds, -1 otherwise
  inline BitmapLocalIdx alloc_all() {
    uint64_t expected = 0;
    if (__atomic_load_n(&bitmap, __ATOMIC_ACQUIRE) != 0) return -1;
    if (!__atomic_compare_exchange_n(&bitmap, &expected, BITMAP_ALL_USED, false,
                                     __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE))
      return -1;
    persist_cl_fenced(&bitmap);
    return 0;
  }

  inline void set_allocated(uint32_t idx) {
    bitmap |= (1 << idx);
    persist_cl_fenced(&bitmap);
  }

  // get a read-only snapshot of bitmap
  [[nodiscard]] uint64_t get() const { return bitmap; }
};

enum TxEntryType : bool {
  TX_BEGIN = false,
  TX_COMMIT = true,
};

using TxEntry = uint64_t;

struct TxBeginEntry {
  union {
    struct {
      // should always be TX_BEGIN for TxBeginEntry
      enum TxEntryType type : 1;

      // This transaction affects the blocks [block_idx_start, block_idx_end]
      // We have 32 bits for block_id_end instead of 31 because we want to use
      // -1 to represent the end of the file
      VirtualBlockIdx block_idx_start : 31;
      VirtualBlockIdx block_idx_end;
    };

    TxEntry data;
  };
};

struct TxCommitEntry {
  union {
    struct {
      // should always be TX_COMMIT for TxCommitEntry
      enum TxEntryType type : 1;

      // how many entries ahead is the corresponding TxBeginEntry
      // the value stored should always be positive
      uint32_t begin_offset : 23;

      // the first log entry for this transaction, 40 bits in size
      // The rest of the log entries are organized as a linked list
      LogEntryIdx log_entry_idx;
    };

    TxEntry data;
  };
};

enum LogOp {
  // we start the enum from 1 so that a LogOp with value 0 is invalid
  LOG_OVERWRITE = 1,
};

// Since allocator can only guarantee to allocate 64 contiguous blocks (by
// single CAS), log entry must organize as a linked list in case of a large
// size transaction.
class LogEntry {
 public:
  // the first word (8 bytes) is used to check if the log entry is valid or not
  union {
    struct {
      // we use bitfield to pack `op` and `last_remaining` into 16 bits
      enum LogOp op : 4;

      // the remaining number of bytes that are not used in this log entry
      // only the last log entry for a tx can have non-zero value for this field
      // the maximum number of remaining bytes is BLOCK_SIZE - 1
      uint16_t last_remaining : 12;

      // the number of blocks within a log entry is at most 64
      uint8_t num_blocks;

      // the index of the next log entry
      LogEntryIdx next;
    };
    uint64_t word1;
  };

  union {
    struct {
      // we map the logical blocks [logical_idx, logical_idx + num_blocks)
      // to the virtual blocks [virtual_idx, virtual_idx + num_blocks)
      VirtualBlockIdx virtual_idx;
      LogicalBlockIdx logical_idx;
    };
    uint64_t word2;
  };
};

static_assert(sizeof(LogEntry) == 16, "LogEntry must of size 16 bytes");

// signature
constexpr static int SIGNATURE_LEN = 16;
constexpr static char FILE_SIGNATURE[SIGNATURE_LEN] = "ULAYFS";

// how many blocks a bitmap can manage
// (that's why call it "capacity" instead of "size")
constexpr static uint32_t BITMAP_CAPACITY_SHIFT = 6;
constexpr static uint32_t BITMAP_CAPACITY = 1 << BITMAP_CAPACITY_SHIFT;

// number of various data structures in blocks
constexpr static uint16_t NUM_BITMAP = BLOCK_SIZE / sizeof(Bitmap);
constexpr static uint16_t NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(LogicalBlockIdx)) / sizeof(TxEntry);
constexpr static uint16_t NUM_LOG_ENTRY = BLOCK_SIZE / sizeof(LogEntry);

static_assert(NUM_LOG_ENTRY <= 1 << 8,
              "NUM_LOG_ENTRY should be representable in less than 8 bits");

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
 * In the current design, the inline bitmap in the meta block can manage 16k
 * blocks (64 MB in total); after that, every 32k blocks (128 MB) will have its
 * first block as the bitmap block that manages its allocation.
 * We assign "bitmap_block_id" to these bitmap blocks, where id=0 is the inline
 * one in the meta block (LogicalBlockIdx=0); bitmap block id=1 is the block
 * with LogicalBlockIdx 16384; id=2 is the one with LogicalBlockIdx 32768, etc.
 */
class BitmapBlock {
 private:
  Bitmap bitmaps[NUM_BITMAP];

 public:
  /**
   * a static helper function for allocating a single block in the bitmap
   * also used for managing MetaBlock::inline_bitmap
   *
   * @param bitmaps a pointer to an array of bitmaps
   * @param num_bitmaps the total number of bitmaps in the array
   * @param hint hint to the empty bit
   */
  inline static BitmapLocalIdx alloc_one(Bitmap bitmaps[], uint16_t num_bitmaps,
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
   * @return the BitmapLocalIdx and whether the operation is successful
   */
  inline static BitmapLocalIdx alloc_batch(Bitmap bitmaps[],
                                           uint16_t num_bitmaps,
                                           BitmapLocalIdx hint) {
    BitmapLocalIdx idx = hint >> BITMAP_CAPACITY_SHIFT;
    // we cannot allocate a whole batch from the first bitmap
    if (idx == 0) ++idx;
    for (; idx < num_bitmaps; ++idx) {
      BitmapLocalIdx ret = bitmaps[idx].alloc_all();
      if (ret < 0) continue;
      return idx << BITMAP_CAPACITY_SHIFT;
    }
    return -1;
  }

  // first bit of is the bitmap block itself
  inline void init() { bitmaps[0].set_allocated(0); }

  // allocate one block; return the index of allocated block
  // accept a hint for which bit to start searching
  // usually hint can just be the last idx return by this function
  inline BitmapLocalIdx alloc_one(BitmapLocalIdx hint = 0) {
    return BitmapBlock::alloc_one(bitmaps, NUM_BITMAP, hint);
  }

  // 64 blocks are considered as one batch; return the index of the first block
  inline BitmapLocalIdx alloc_batch(BitmapLocalIdx hint = 0) {
    return BitmapBlock::alloc_batch(bitmaps, NUM_BITMAP, hint);
  }

  // map `bitmap_local_idx` from alloc_one/all to the LogicalBlockIdx
  inline static LogicalBlockIdx get_block_idx(BitmapBlockId bitmap_block_id,
                                              BitmapLocalIdx bitmap_local_idx) {
    return (bitmap_block_id << BITMAP_BLOCK_CAPACITY_SHIFT) +
           INLINE_BITMAP_CAPACITY + bitmap_local_idx;
  }

  // make bitmap id to its block idx
  inline static LogicalBlockIdx get_bitmap_block_idx(
      BitmapBlockId bitmap_block_id) {
    return get_block_idx(bitmap_block_id, 0);
  }

  // reverse mapping of get_bitmap_block_idx
  inline static BitmapBlockId get_bitmap_block_id(LogicalBlockIdx idx) {
    return (idx - INLINE_BITMAP_CAPACITY) >> BITMAP_BLOCK_CAPACITY_SHIFT;
  }
};

class TxLogBlock {
  LogicalBlockIdx prev;
  LogicalBlockIdx next;
  TxEntry tx_entries[NUM_TX_ENTRY];

 public:
  /**
   * a static helper function for appending a TxEntry
   * also used for managing MetaBlock::inline_tx_entries
   *
   * @param entries a pointer to an array of tx entries
   * @param num_entries the total number of entries in the array
   * @param entry the target entry to be appended
   * @param hint hint to the tail of the log
   * @return the TxEntry local index and whether the operation is successful
   */
  inline static TxLocalIdx try_append(TxEntry entries[], uint16_t num_entries,
                                      TxEntry entry, TxLocalIdx hint) {
    for (TxLocalIdx idx = hint; idx <= num_entries - 1; ++idx) {
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

  inline TxLocalIdx try_begin(TxBeginEntry begin_entry,
                              TxLocalIdx hint_tail = 0) {
    return try_append(tx_entries, NUM_TX_ENTRY, begin_entry.data, hint_tail);
  }

  inline TxLocalIdx try_commit(TxCommitEntry commit_entry,
                               TxLocalIdx hint_tail = 0) {
    // FIXME: this one is actually wrong. In OCC, we have to verify there is no
    // new transaction overlap with our range
    return try_append(tx_entries, NUM_TX_ENTRY, commit_entry.data, hint_tail);
  }
};

class LogEntryBlock {
  LogEntry log_entries[NUM_LOG_ENTRY];

  /**
   * @param log_entry the log entry to be appended to the block
   * @param hint_tail a hint for which log local idx to start searching
   * @return the log local idx and whether the operation is successful
   */
  inline std::pair<LogLocalIdx, bool> try_append(LogEntry log_entry,
                                                 LogLocalIdx hint_tail = 0) {
    for (LogLocalIdx idx = hint_tail; idx <= NUM_LOG_ENTRY - 1; ++idx) {
      // we use the first word if a log entry is valid or not
      if (__atomic_load_n(&log_entries[idx].word1, __ATOMIC_ACQUIRE)) continue;

      // try to write the 1st word using CAS and write the 2nd word normally
      uint64_t expected = 0;
      if (__atomic_compare_exchange_n(&log_entries[idx].word1, &expected,
                                      log_entry.word1, false, __ATOMIC_RELEASE,
                                      __ATOMIC_ACQUIRE)) {
        log_entries[idx].word2 = log_entry.word2;
        persist_cl_fenced(&log_entries[idx]);
        return {idx, true};
      }
    }
    return {0, false};
  }
};

class DataBlock {
  char data[BLOCK_SIZE];
};

/*
 * LogicalBlockIdx 0 -> MetaBlock; other blocks can be any type of blocks
 */
class MetaBlock {
 private:
  // contents in the first cache line
  union {
    struct {
      // file signature
      char signature[SIGNATURE_LEN];

      // file size in bytes (logical size to users)
      uint64_t file_size;

      // total number of blocks actually in this file (including unused ones)
      uint32_t num_blocks;

      // if inline_tx_entries is used up, this points to the next log block
      LogicalBlockIdx tx_log_head;

      // hint to find log tail; not necessarily up-to-date
      LogicalBlockIdx tx_log_tail;
    };

    // padding avoid cache line contention
    char cl1[CACHELINE_SIZE];
  };

  union {
    // address for futex to lock, 4 bytes in size
    // this lock is ONLY used for ftruncate
    Futex meta_lock;

    // set futex to another cacheline to avoid futex's contention affect reading
    // the metadata above
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
  // only called if a new file is created
  void init() {
    // the first block is always used (by MetaBlock itself)
    meta_lock.init();
    memcpy(signature, FILE_SIGNATURE, SIGNATURE_LEN);

    persist_cl_fenced(&cl1);
  }

  // check whether the meta block is valid
  bool is_valid() {
    return std::memcmp(signature, FILE_SIGNATURE, SIGNATURE_LEN) == 0;
  }

  // acquire/release meta lock (usually only during allocation)
  // we don't need to call persistence since futex is robust to crash
  void lock() { meta_lock.acquire(); }
  void unlock() { meta_lock.release(); }

  // called by other public functions with lock held
  void set_num_blocks_no_lock(uint32_t num_blocks) {
    this->num_blocks = num_blocks;
    persist_cl_fenced(&num_blocks);
  }

  [[nodiscard]] uint32_t get_num_blocks() const { return num_blocks; }

  // allocate one block; return the index of allocated block
  // accept a hint for which bit to start searching
  // usually hint can just be the last idx return by this function
  inline BitmapLocalIdx inline_alloc_one(BitmapLocalIdx hint = 0) {
    return BitmapBlock::alloc_one(inline_bitmaps, NUM_INLINE_BITMAP, hint);
  }

  // 64 blocks are considered as one batch; return the index of the first block
  inline BitmapLocalIdx inline_alloc_batch(BitmapLocalIdx hint = 0) {
    return BitmapBlock::alloc_batch(inline_bitmaps, NUM_INLINE_BITMAP, hint);
  }

  inline TxLocalIdx inline_try_begin(TxBeginEntry begin_entry,
                                     TxLocalIdx hint_tail = 0) {
    return TxLogBlock::try_append(inline_tx_entries, NUM_INLINE_TX_ENTRY,
                                  begin_entry.data, hint_tail);
  }

  inline TxLocalIdx inline_try_commit(TxCommitEntry commit_entry,
                                      TxLocalIdx hint_tail = 0) {
    // TODO: OCC
    return TxLogBlock::try_append(inline_tx_entries, NUM_INLINE_TX_ENTRY,
                                  commit_entry.data, hint_tail);
  }

  friend std::ostream& operator<<(std::ostream& out, const MetaBlock& block) {
    out << "MetaBlock: \n";
    out << "\tsignature: \"" << block.signature << "\"\n";
    out << "\tfilesize: " << block.file_size << "\n";
    out << "\tnum_blocks: " << block.num_blocks << "\n";
    return out;
  }
};

union Block {
  MetaBlock meta_block;
  BitmapBlock bitmap_block;
  TxLogBlock tx_log_block;
  LogEntryBlock log_entry_block;
  DataBlock data_block;
  char padding[BLOCK_SIZE];
};

static_assert(sizeof(LogEntryIdx) == 5, "LogEntryIdx must of 5 bytes");
static_assert(sizeof(Bitmap) == 8, "Bitmap must of 64 bits");
static_assert(sizeof(TxEntry) == 8, "TxEntry must be 64 bits");
static_assert(sizeof(TxBeginEntry) == 8, "TxEntry must be 64 bits");
static_assert(sizeof(TxCommitEntry) == 8, "TxEntry must be 64 bits");
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
}  // namespace ulayfs::pmem
