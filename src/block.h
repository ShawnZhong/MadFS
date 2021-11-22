#pragma once

#include <pthread.h>

#include <cassert>
#include <cstring>

#include "bitmap.h"
#include "entry.h"
#include "idx.h"
#include "params.h"
#include "utils.h"

namespace ulayfs {
// signature
constexpr static int SIGNATURE_SIZE = 16;
constexpr static char FILE_SIGNATURE[SIGNATURE_SIZE] = "ULAYFS";

// number of various data structures in blocks
constexpr static uint16_t NUM_BITMAP = BLOCK_SIZE / sizeof(pmem::Bitmap);
constexpr static uint16_t NUM_TX_ENTRY =
    (BLOCK_SIZE - 2 * sizeof(LogicalBlockIdx)) / sizeof(pmem::TxEntry);
constexpr static uint16_t NUM_LOG_ENTRY = BLOCK_SIZE / sizeof(pmem::LogEntry);

static_assert(NUM_BITMAP - 1 <= std::numeric_limits<BitmapLocalIdx>::max(),
              "NUM_BITMAP - 1 should be representable with BitmapLocalIdx");
static_assert(NUM_TX_ENTRY - 1 <= std::numeric_limits<TxLocalIdx>::max(),
              "NUM_TX_ENTRY - 1 should be representable with TxLocalIdx");
static_assert(
    NUM_LOG_ENTRY - 1 <= std::numeric_limits<LogLocalUnpackIdx>::max(),
    "NUM_LOG_ENTRY - 1 should be representable with LogLocalUnpackIdx");

const static uint16_t NUM_TX_ENTRY_PER_CL =
    CACHELINE_SIZE / sizeof(pmem::TxEntry);
const static uint16_t NUM_BITMAP_PER_CL = CACHELINE_SIZE / sizeof(pmem::Bitmap);
const static uint16_t NUM_CL_PER_BLOCK = BLOCK_SIZE / CACHELINE_SIZE;

// inline data structure count in meta block
constexpr static uint16_t NUM_CL_BITMAP_IN_META = 32;
constexpr static uint16_t NUM_CL_TX_ENTRY_IN_META =
    ((BLOCK_SIZE / CACHELINE_SIZE) - 2) - NUM_CL_BITMAP_IN_META;
constexpr static uint16_t NUM_INLINE_BITMAP =
    NUM_CL_BITMAP_IN_META * NUM_BITMAP_PER_CL;
constexpr static uint16_t NUM_INLINE_TX_ENTRY =
    NUM_CL_TX_ENTRY_IN_META * NUM_TX_ENTRY_PER_CL;

// how many blocks a bitmap block can manage
constexpr static uint32_t BITMAP_BLOCK_CAPACITY_SHIFT =
    BITMAP_CAPACITY_SHIFT + (BLOCK_SHIFT - 3);  // 15
constexpr static uint32_t BITMAP_BLOCK_CAPACITY =
    1 << BITMAP_BLOCK_CAPACITY_SHIFT;

// 32 cacheline corresponds to 2^14 bits
constexpr static uint32_t INLINE_BITMAP_CAPACITY =
    NUM_INLINE_BITMAP * BITMAP_CAPACITY;

// how many data blocks can be covered per CAS
// TODO: put this constant somewhere else?
constexpr static uint8_t MAX_BLOCKS_PER_BODY = 64;

namespace pmem {
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

class TxBlock : public BaseBlock {
  TxEntry tx_entries[NUM_TX_ENTRY];
  // next is placed after tx_entires so that it could be flushed with tx_entries
  LogicalBlockIdx next;
  // seq is used to construct total order between tx entries, so it must
  // increase monotically
  // when compare two TxEntryIdx
  // if within same block, compare local index
  // if not, compare their block's seq number
  uint32_t tx_seq;

 public:
  TxLocalIdx find_tail(TxLocalIdx hint = 0) {
    return TxEntry::find_tail<NUM_TX_ENTRY>(tx_entries, hint);
  }

  TxEntry try_append(TxEntry entry, TxLocalIdx idx) {
    return TxEntry::try_append(tx_entries, entry, idx);
  }

  [[nodiscard]] TxEntry get(TxLocalIdx idx) {
    assert(idx >= 0 && idx < NUM_TX_ENTRY);
    return tx_entries[idx];
  }

  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const { return next; }

  void set_tx_seq(uint32_t seq) { tx_seq = seq; }
  [[nodiscard]] uint32_t get_tx_seq() const { return tx_seq; }

  /**
   * Set the next block index
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    bool success = __atomic_compare_exchange_n(
        &next, &expected, block_idx, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    return success;
  }

  /**
   * flush the current block starting from `begin_idx` (including two pointers)
   *
   * @param begin_idx where to start flush
   */
  void flush_tx_block(TxLocalIdx begin_idx = 0) {
    persist_unfenced(&tx_entries[begin_idx],
                     sizeof(TxEntry) * (NUM_TX_ENTRY - begin_idx) +
                         2 * sizeof(LogicalBlockIdx));
  }

  /**
   * flush a range of tx entries
   *
   * @param begin_idx
   */
  void flush_tx_entries(TxLocalIdx begin_idx, TxLocalIdx end_idx) {
    assert(end_idx > begin_idx);
    persist_unfenced(&tx_entries[begin_idx],
                     sizeof(TxEntry) * (end_idx - begin_idx));
  }
};

// LogEntryBlock is per-thread to avoid contention
class LogEntryBlock : public BaseBlock {
  LogEntry log_entries[NUM_LOG_ENTRY];

 public:
  [[nodiscard]] LogEntry* get(LogLocalUnpackIdx idx) {
    assert(idx >= 0 && idx < NUM_LOG_ENTRY);
    return &log_entries[idx];
  }

  void persist(LogLocalUnpackIdx start_idx, LogLocalUnpackIdx end_idx,
               bool fenced = true) {
    size_t len = (end_idx - start_idx) * sizeof(LogEntry);
    if (fenced)
      persist_fenced(&log_entries[start_idx], len);
    else
      persist_unfenced(&log_entries[start_idx], len);
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
      // all tx entries before it must be flushed
      TxEntryIdx tx_tail;
    };

    // padding avoid cache line contention
    char cl1[CACHELINE_SIZE];
  };

  // move mutex to another cache line to avoid contention on reading the
  // metadata above
  union {
    struct {
      // this lock is ONLY used for ftruncate
      pthread_mutex_t mutex;

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

    VALGRIND_PMC_REMOVE_PMEM_MAPPING(&mutex, sizeof(mutex));

    // initialize the mutex
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&mutex, &attr);

    // initialize the signature
    memcpy(signature, FILE_SIGNATURE, SIGNATURE_SIZE);
    persist_cl_fenced(&cl1);
  }

  // check whether the meta block is valid
  bool is_valid() {
    return std::memcmp(signature, FILE_SIGNATURE, SIGNATURE_SIZE) == 0;
  }

  // acquire/release meta lock (usually only during allocation)
  // we don't need to call persistence since mutex is robust to crash
  void lock() {
    int rc = pthread_mutex_lock(&mutex);
    if (rc == EOWNERDEAD) {
      WARN("Mutex owner died");
      rc = pthread_mutex_consistent(&mutex);
      PANIC_IF(rc != 0, "pthread_mutex_consistent failed");
    }
  }
  void unlock() {
    int rc = pthread_mutex_unlock(&mutex);
    PANIC_IF(rc != 0, "Mutex unlock failed");
  }

  /*
   * Getters and setters
   */

  [[nodiscard]] size_t get_file_size() const { return file_size; }

  // called by other public functions with lock held
  void set_num_blocks_no_lock(uint32_t num_blocks) {
    __atomic_store_n(&this->num_blocks, num_blocks, __ATOMIC_RELAXED);
    persist_cl_fenced(&cl2);
  }

  [[nodiscard]] uint32_t get_tx_seq() const { return 0; }

  /**
   * Set the next tx block index
   * No flush+fence but leave it to flush_tx_block
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    bool success =
        __atomic_compare_exchange_n(&next_tx_block, &expected, block_idx, false,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
    return success;
  }

  /**
   * Set the tx tail
   * tx_tail is mostly just a hint, so it's fine to be not up-to-date; thus by
   * default, we don't do concurrency control and no fence by default
   *
   * @param tx_tail tail value to set
   * @param fenced whether use fence
   */
  void set_tx_tail(TxEntryIdx tx_tail, bool fenced = false) {
    this->tx_tail = tx_tail;
    persist_cl(&cl1, fenced);
  }

  /**
   * similar to the one in TxBlock:
   * flush the current block starting from `begin_idx` (including two pointers)
   *
   * @param begin_idx where to start flush
   */
  void flush_tx_block(TxLocalIdx begin_idx = 0) {
    persist_unfenced(&inline_tx_entries[begin_idx],
                     sizeof(TxEntry) * (NUM_INLINE_TX_ENTRY - begin_idx));
    persist_cl_unfenced(cl1);
  }

  /**
   * flush a range of tx entries
   *
   * @param begin_idx
   */
  void flush_tx_entries(TxLocalIdx begin_idx, TxLocalIdx end_idx) {
    persist_unfenced(&inline_tx_entries[begin_idx],
                     sizeof(TxEntry) * (end_idx - begin_idx));
  }

  [[nodiscard]] uint32_t get_num_blocks() const {
    return __atomic_load_n(&this->num_blocks, __ATOMIC_ACQUIRE);
  }
  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const {
    return next_tx_block;
  }
  [[nodiscard]] TxEntryIdx get_tx_tail() const { return tx_tail; }

  [[nodiscard]] TxEntry get_tx_entry(TxLocalIdx idx) const {
    assert(idx >= 0 && idx < NUM_INLINE_TX_ENTRY);
    return __atomic_load_n(&inline_tx_entries[idx].raw_bits, __ATOMIC_ACQUIRE);
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

  TxEntry try_append(TxEntry entry, TxLocalIdx idx) {
    return TxEntry::try_append(inline_tx_entries, entry, idx);
  }

  friend std::ostream& operator<<(std::ostream& out, const MetaBlock& block) {
    out << "MetaBlock: \n";
    out << "\tsignature: \"" << block.signature << "\"\n";
    out << "\tfilesize: " << block.file_size << "\n";
    out << "\tnum_blocks: " << block.num_blocks << "\n";
    out << "\tnext_tx_block: " << block.next_tx_block << "\n";
    out << "\ttx_tail: " << block.tx_tail << "\n";
    return out;
  }
};

union Block {
  MetaBlock meta_block;
  BitmapBlock bitmap_block;
  TxBlock tx_block;
  LogEntryBlock log_entry_block;
  DataBlock data_block;

  // view a block as an array of cache line
  struct {
    char cl[CACHELINE_SIZE];
  } cl_view[NUM_CL_PER_BLOCK];

  [[nodiscard]] char* data_rw() { return data_block.data; }
  [[nodiscard]] const char* data_ro() const { return data_block.data; }

  bool zero_init_cl(uint16_t cl_idx) {
    constexpr static const char* zero_cl[CACHELINE_SIZE] = {};
    if (memcmp(&cl_view[cl_idx], zero_cl, CACHELINE_SIZE) != 0) {
      memset(&cl_view[cl_idx], 0, CACHELINE_SIZE);
      persist_cl_unfenced(&cl_view[cl_idx]);
      return true;
    }
    return false;
  }

  // memset a block to zero from a given byte offset
  // offset must be cacheline-aligned
  // return whether a memset happen (may need fence if true)
  bool zero_init(uint16_t cl_idx_begin = 0,
                 uint16_t cl_idx_end = NUM_CL_PER_BLOCK) {
    bool do_memset = false;
    for (uint16_t cl_idx = cl_idx_begin; cl_idx < cl_idx_end; ++cl_idx)
      do_memset |= zero_init_cl(cl_idx);
    if (do_memset) _mm_sfence();
    return do_memset;
  }
};

static_assert(sizeof(MetaBlock) == BLOCK_SIZE,
              "MetaBlock must be of size BLOCK_SIZE");
static_assert(sizeof(BitmapBlock) == BLOCK_SIZE,
              "BitmapBlock must be of size BLOCK_SIZE");
static_assert(sizeof(TxBlock) == BLOCK_SIZE,
              "TxBlock must be of size BLOCK_SIZE");
static_assert(sizeof(LogEntryBlock) == BLOCK_SIZE,
              "LogEntryBlock must be of size BLOCK_SIZE");
static_assert(sizeof(DataBlock) == BLOCK_SIZE,
              "DataBlock must be of size BLOCK_SIZE");
static_assert(sizeof(Block) == BLOCK_SIZE, "Block must be of size BLOCK_SIZE");

}  // namespace pmem
}  // namespace ulayfs
