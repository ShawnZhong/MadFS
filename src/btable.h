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
  uint64_t file_size;
  pthread_spinlock_t spinlock;

 public:
  explicit BlkTable(File* file, TxMgr* tx_mgr)
      : file(file), tx_mgr(tx_mgr), tail_tx_idx(), tail_tx_block(nullptr) {
    table.resize(16);
    pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
  }

  ~BlkTable() { pthread_spin_destroy(&spinlock); }

  // FIXME: WARNING: this function will be removed in the future
  // getting the file size should be associated with the tx application states
  // currently only introduced for lseek's unfixed bug on file offset
  uint64_t get_file_size() { return file_size; }

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
   * @param[out] file_size the file size after update (nullptr don't care)
   * @param[in] do_alloc whether we allow allocation when iterating the tx_idx
   */
  void update(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
              uint64_t* new_file_size, bool do_alloc);

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
