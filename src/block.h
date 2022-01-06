#pragma once

#include <pthread.h>

#include <atomic>
#include <cassert>
#include <cstring>

#include "bitmap.h"
#include "const.h"
#include "entry.h"
#include "idx.h"
#include "posix.h"
#include "utils.h"

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

class TxBlock : public BaseBlock {
  std::atomic<TxEntry> tx_entries[NUM_TX_ENTRY];
  // next is placed after tx_entires so that it could be flushed with tx_entries
  std::atomic<LogicalBlockIdx> next;
  // seq is used to construct total order between tx entries, so it must
  // increase monotonically
  // when compare two TxEntryIdx
  // if within same block, compare local index
  // if not, compare their block's seq number
  uint32_t tx_seq;

 public:
  TxLocalIdx find_tail(TxLocalIdx hint = 0) const {
    return TxEntry::find_tail<NUM_TX_ENTRY>(tx_entries, hint);
  }

  TxEntry try_append(TxEntry entry, TxLocalIdx idx) {
    return TxEntry::try_append(tx_entries, entry, idx);
  }

  // THIS FUNCTION IS NOT THREAD SAFE
  void store(TxEntry entry, TxLocalIdx idx) {
    tx_entries[idx].store(entry, std::memory_order_relaxed);
  }

  [[nodiscard]] TxEntry get(TxLocalIdx idx) const {
    assert(idx >= 0 && idx < NUM_TX_ENTRY);
    return tx_entries[idx].load(std::memory_order_acquire);
  }

  // it should be fine not to use any fence since there will be fence for flush
  void set_tx_seq(uint32_t seq) { tx_seq = seq; }
  [[nodiscard]] uint32_t get_tx_seq() const { return tx_seq; }

  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const {
    return next.load(std::memory_order_acquire);
  }

  /**
   * Set the next block index
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    return next.compare_exchange_strong(expected, block_idx,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
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

      // hint to find tx log tail; not necessarily up-to-date
      // all tx entries before it must be flushed
      std::atomic<TxEntryIdx64> tx_tail;

      // if inline_tx_entries is used up, this points to the next log block
      std::atomic<LogicalBlockIdx> next_tx_block;
    } cl1_meta;

    // padding avoid cache line contention
    char cl1[CACHELINE_SIZE];
  };

  static_assert(sizeof(cl1_meta) <= CACHELINE_SIZE,
                "cl1_meta must be no larger than a cache line");

  // move mutex to another cache line to avoid contention on reading the
  // metadata above
  union {
    struct {
      // this lock is ONLY used for bitmap rebuild
      pthread_mutex_t mutex;

      // total number of blocks actually in this file (including unused ones)
      // modifications to this should be through the getter/setter functions
      // that use atomic instructions
      std::atomic_uint32_t num_blocks;
    } cl2_meta;

    // padding
    char cl2[CACHELINE_SIZE];
  };

  static_assert(sizeof(cl2_meta) <= CACHELINE_SIZE,
                "cl1_meta must be no larger than a cache line");

  // 62 cache lines for tx log (~120 txs)
  std::atomic<TxEntry> inline_tx_entries[NUM_INLINE_TX_ENTRY];

  static_assert(sizeof(inline_tx_entries) == 62 * CACHELINE_SIZE,
                "inline_tx_entries must be 30 cache lines");

 public:
  /**
   * only called if a new file is created
   * We can assume that all other fields are zero-initialized upon fallocate
   */
  void init() {
    // initialize the mutex
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&cl2_meta.mutex, &attr);
    VALGRIND_PMC_REMOVE_PMEM_MAPPING(&cl2_meta.mutex, sizeof(cl2_meta.mutex));

    // initialize the signature
    memcpy(cl1_meta.signature, FILE_SIGNATURE, SIGNATURE_SIZE);
    persist_cl_fenced(&cl1);
  }

  // check whether the meta block is valid
  [[nodiscard]] bool is_valid() const {
    return std::strncmp(cl1_meta.signature, FILE_SIGNATURE, SIGNATURE_SIZE) ==
           0;
  }

  // acquire/release meta lock (usually only during allocation)
  // we don't need to call persistence since mutex is robust to crash
  void lock() {
    int rc = pthread_mutex_lock(&cl2_meta.mutex);
    if (rc == EOWNERDEAD) {
      WARN("Mutex owner died");
      rc = pthread_mutex_consistent(&cl2_meta.mutex);
      PANIC_IF(rc != 0, "pthread_mutex_consistent failed");
    }
  }
  void unlock() {
    int rc = pthread_mutex_unlock(&cl2_meta.mutex);
    PANIC_IF(rc != 0, "Mutex unlock failed");
  }

  /*
   * Getters and setters
   */
  // called by other public functions with lock held
  void set_num_blocks_if_larger(uint32_t new_num_blocks) {
    uint32_t old_num_blocks =
        cl2_meta.num_blocks.load(std::memory_order_acquire);
  retry:
    if (unlikely(old_num_blocks >= new_num_blocks)) return;
    if (!cl2_meta.num_blocks.compare_exchange_strong(
            old_num_blocks, new_num_blocks, std::memory_order_acq_rel,
            std::memory_order_acquire))
      goto retry;
    // if num_blocks is out-of-date, it's fine...
    // in the worst case, we just do unnecessary fallocate...
    // so we don't wait for it to persist
    persist_cl_unfenced(&cl2);
  }

  [[nodiscard]] uint32_t get_tx_seq() const { return 0; }

  /**
   * Set the next tx block index
   * No flush+fence but leave it to flush_tx_block
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx,
                         LogicalBlockIdx expected = 0) {
    return cl1_meta.next_tx_block.compare_exchange_strong(
        expected, block_idx, std::memory_order_acq_rel,
        std::memory_order_acquire);
  }

  /**
   * Set the tx tail
   * tx_tail is mostly just a hint, so it's fine to be not up-to-date; thus by
   * default, we don't do concurrency control and no fence by default
   *
   * @param tx_tail tail value to set
   * @param fenced whether use fence
   */
  void set_tx_tail(TxEntryIdx64 tx_tail, bool fenced = false) {
    cl1_meta.tx_tail.store(tx_tail, std::memory_order_relaxed);
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
    return cl2_meta.num_blocks.load(std::memory_order_acquire);
  }
  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const {
    return cl1_meta.next_tx_block.load(std::memory_order_acquire);
  }
  [[nodiscard]] TxEntryIdx get_tx_tail() const {
    return cl1_meta.tx_tail.load(std::memory_order_relaxed).tx_entry_idx;
  }

  [[nodiscard]] TxEntry get_tx_entry(TxLocalIdx idx) const {
    assert(idx >= 0 && idx < NUM_INLINE_TX_ENTRY);
    return inline_tx_entries[idx].load(std::memory_order_acquire);
  }

  /*
   * Methods for inline metadata
   */
  TxLocalIdx find_tail(TxLocalIdx hint = 0) const {
    return TxEntry::find_tail<NUM_INLINE_TX_ENTRY>(inline_tx_entries, hint);
  }

  TxEntry try_append(TxEntry entry, TxLocalIdx idx) {
    return TxEntry::try_append(inline_tx_entries, entry, idx);
  }

  // for garbage collection
  void invalidate_tx_entries() {
    for (auto& inline_tx_entrie : inline_tx_entries)
      inline_tx_entrie.store(TxEntry::TxEntryDummy, std::memory_order_relaxed);
    persist_fenced(inline_tx_entries, sizeof(TxEntry) * NUM_INLINE_TX_ENTRY);
  }

  friend std::ostream& operator<<(std::ostream& out, const MetaBlock& block) {
    out << "MetaBlock: \n";
    out << "\tnum_blocks: "
        << block.cl2_meta.num_blocks.load(std::memory_order_acquire) << "\n";
    out << "\tnext_tx_block: "
        << block.cl1_meta.next_tx_block.load(std::memory_order_acquire) << "\n";
    out << "\ttx_tail: "
        << block.cl1_meta.tx_tail.load(std::memory_order_acquire).tx_entry_idx
        << "\n";
    return out;
  }
};

// TODO: we no longer have bitmap_block in PMEM
union Block {
  MetaBlock meta_block;
  TxBlock tx_block;
  LogEntryBlock log_entry_block;
  DataBlock data_block;

  // view a block as an array of cache line
  char cache_lines[NUM_CL_PER_BLOCK][CACHELINE_SIZE];

  [[nodiscard]] char* data_rw() { return data_block.data; }
  [[nodiscard]] const char* data_ro() const { return data_block.data; }

  /**
   * zero-out a cache line
   * @param cl_idx which cache line to zero out
   * @return whether memset happened
   */
  bool zero_init_cl(uint16_t cl_idx) {
    constexpr static const char zero_cl[CACHELINE_SIZE]{};
    if (memcmp(&cache_lines[cl_idx], zero_cl, CACHELINE_SIZE) == 0)
      return false;
    memset(&cache_lines[cl_idx], 0, CACHELINE_SIZE);
    persist_cl_unfenced(&cache_lines[cl_idx]);
    return true;
  }

  /**
   * zero-initialize a block within a given cache-line range
   * @param cl_idx_begin the beginning cache-line index
   * @param cl_idx_end the ending cache-line index
   * @return whether memset happened
   */
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
static_assert(sizeof(TxBlock) == BLOCK_SIZE,
              "TxBlock must be of size BLOCK_SIZE");
static_assert(sizeof(LogEntryBlock) == BLOCK_SIZE,
              "LogEntryBlock must be of size BLOCK_SIZE");
static_assert(sizeof(DataBlock) == BLOCK_SIZE,
              "DataBlock must be of size BLOCK_SIZE");
static_assert(sizeof(Block) == BLOCK_SIZE, "Block must be of size BLOCK_SIZE");

}  // namespace ulayfs::pmem
