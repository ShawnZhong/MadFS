#pragma once

#include <pthread.h>

#include <atomic>
#include <cassert>
#include <cstring>

#include "const.h"
#include "entry.h"
#include "idx.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::pmem {

/*
 * LogicalBlockIdx 0 -> MetaBlock; other blocks can be any type of blocks
 */
class MetaBlock : public noncopyable {
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

  static_assert(std::atomic<TxEntryIdx64>::is_always_lock_free,
                "cl1_meta.tx_tail must be lock-free");

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
    VALGRIND_PMC_REMOVE_PMEM_MAPPING(&cl2_meta.mutex, sizeof(cl2_meta.mutex));
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&cl2_meta.mutex, &attr);

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
      LOG_WARN("Mutex owner died");
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
  void set_tx_tail(TxEntryIdx tx_tail) {
    cl1_meta.tx_tail.store(tx_tail, std::memory_order_relaxed);
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
    auto tx_tail = cl1_meta.tx_tail.load(std::memory_order_relaxed);
    return tx_tail.tx_entry_idx;
  }

  [[nodiscard]] TxEntry get_tx_entry(TxLocalIdx idx) const {
    assert(idx >= 0 && idx < NUM_INLINE_TX_ENTRY);
    return inline_tx_entries[idx].load(std::memory_order_acquire);
  }

  /*
   * Methods for inline metadata
   */
  [[nodiscard]] TxLocalIdx find_tail(TxLocalIdx hint = 0) const {
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

static_assert(sizeof(MetaBlock) == BLOCK_SIZE,
              "MetaBlock must be of size BLOCK_SIZE");

}  // namespace ulayfs::pmem
