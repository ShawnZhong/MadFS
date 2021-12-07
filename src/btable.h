#pragma once

#include <pthread.h>
#include <tbb/concurrent_vector.h>

#include <cstdint>
#include <ostream>

#include "block.h"
#include "idx.h"
#include "log.h"
#include "tx.h"
#include "utils.h"

namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  File* file;
  TxMgr* tx_mgr;

  tbb::concurrent_vector<LogicalBlockIdx> table;

  // keep track of the next TxEntry to apply
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;

  struct spinlock {
    std::atomic<bool> lock_{false};

    void lock() noexcept {
      for (;;) {
        // Optimistically assume the lock is free on the first try
        if (!lock_.exchange(true, std::memory_order_acquire)) {
          return;
        }
        // Wait for lock to be released without generating cache misses
        while (lock_.load(std::memory_order_relaxed)) {
          // Issue X86 PAUSE or ARM YIELD instruction to reduce contention
          // between hyper-threads
          __builtin_ia32_pause();
        }
      }
    }

    void unlock() noexcept { lock_.store(false, std::memory_order_release); }
  } spinlock;

 public:
  explicit BlkTable(File* file, TxMgr* tx_mgr)
      : file(file), tx_mgr(tx_mgr), tail_tx_idx(), tail_tx_block(nullptr) {
    table.resize(16);
  }

  /**
   * @return the logical block index corresponding the the virtual block index
   *  0 is returned if the virtual block index is not allocated yet
   */
  [[nodiscard]] LogicalBlockIdx get(VirtualBlockIdx virtual_block_idx) const {
    if (virtual_block_idx >= table.size()) return 0;
    return table[virtual_block_idx];
  }

  /**
   * Update the block table by applying the transactions
   *
   * @param[out] tx_idx the index of the current transaction tail
   * @param[out] tx_block the log block corresponding to the transaction
   * @param[in] do_alloc whether we allow allocation when iterating the tx_idx
   */
  void update(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block, bool do_alloc);

 private:
  void resize_to_fit(VirtualBlockIdx idx);

  // TODO: handle writev requests
  /**
   * Apply a transaction to the block table
   *
   * @param tx_commit_entry the entry to be applied
   * @param log_mgr a thread-local log_mgr to be used
   */
  void apply_tx(pmem::TxCommitEntry tx_commit_entry, LogMgr* log_mgr);

  void apply_tx(pmem::TxCommitInlineEntry tx_commit_inline_entry);

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b);
};

}  // namespace ulayfs::dram
