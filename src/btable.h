#pragma once

#include <pthread.h>
#include <tbb/concurrent_vector.h>

#include <atomic>
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
  std::atomic<TxEntryIdx> tail_tx_idx;
  std::atomic<pmem::TxBlock*> tail_tx_block;
  std::atomic_uint64_t file_size;

  /**
   * Version of the three fields above.
   * It can only be updated with file->spinlock held. Before a writer (of
   * BlkTable) tries to update the three fields above, it must increment the
   * version; after it's done, it must increment it again.
   * Thus, when this version is odd, it means someone is updating three fields
   * above and they are in a temporary inconsistent state.
   */
  std::atomic_uint64_t version;

 public:
  explicit BlkTable(File* file, TxMgr* tx_mgr)
      : file(file),
        tx_mgr(tx_mgr),
        tail_tx_idx({0, 0}),
        tail_tx_block(nullptr),
        file_size(0),
        version(0) {
    table.resize(16);
  }

  ~BlkTable() = default;

  /**
   * @return the logical block index corresponding the the virtual block index
   *  0 is returned if the virtual block index is not allocated yet
   */
  [[nodiscard]] LogicalBlockIdx get(VirtualBlockIdx virtual_block_idx) const {
    if (virtual_block_idx >= table.size()) return 0;
    return table[virtual_block_idx.get()];
  }

  /**
   * Update the block table by applying the transactions; not thread-safe
   *
   * @param do_alloc whether we allow allocation when iterating the tx_idx
   * @param init_bitmap whether we need to initialize the bitmap
   */
  uint64_t update(bool do_alloc, bool init_bitmap = false);

  /**
   * Quick check if update is necessary; thread safe
   * This check is guarantee to not write any shared data structure so avoid
   * cache coherence traffic. If this function return true, do not acquire
   * spinlock in file.
   *
   * @param[out] tx_idx if no need to update, return tail_tx_idx
   * @param[out] tx_block if no need to update, return tail_tx_block
   * @param[out] f_size if no need to update, return file_size
   * @param[in] do_alloc whether do allocation
   * @return whether update is necessary; if false, set tx_idx and tx_block;
   * otherwise leave them undefined.
   */
  [[nodiscard]] bool need_update(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                                 uint64_t& f_size, bool do_alloc) const {
    uint64_t curr_ver = version.load(std::memory_order_acquire);
    if (curr_ver & 1) return true;  // old version means inconsistency

    tx_idx = tail_tx_idx.load(std::memory_order_relaxed);
    tx_block = tail_tx_block.load(std::memory_order_relaxed);
    f_size = file_size.load(std::memory_order_relaxed);

    if (curr_ver != version.load(std::memory_order_release)) return true;
    if (!tx_mgr->handle_idx_overflow(tx_idx, tx_block, do_alloc)) return false;
    // if it's not valid, there is no new tx to the tx history, thus no need to
    // acquire spinlock to update
    return tx_mgr->get_entry_from_block(tx_idx, tx_block).is_valid();
  }

  [[nodiscard]] TxEntryIdx get_tx_idx() const {
    return tail_tx_idx.load(std::memory_order_relaxed);
  }
  [[nodiscard]] pmem::TxBlock* get_tx_block() const {
    return tail_tx_block.load(std::memory_order_relaxed);
  }

 private:
  void resize_to_fit(VirtualBlockIdx idx);

  // TODO: handle writev requests
  /**
   * Apply a transaction to the block table
   *
   * @param tx_entry the entry to be applied
   * @param log_mgr the log_mgr to be used
   * @param init_bitmap whether we need to initialize the bitmap object
   */
  void apply_tx(pmem::TxEntryIndirect tx_entry, LogMgr* log_mgr,
                bool init_bitmap);

  void apply_tx(pmem::TxEntryInline tx_entry);

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b);
};

}  // namespace ulayfs::dram
