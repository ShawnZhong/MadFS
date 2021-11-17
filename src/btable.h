#pragma once

#include <pthread.h>
#include <tbb/concurrent_vector.h>

#include <cstdint>
#include <ostream>

#include "block.h"
#include "idx.h"
#include "layout.h"
#include "log.h"
#include "tx.h"
#include "utils.h"

namespace ulayfs::dram {

// read logs and update mapping from virtual blocks to logical blocks
class BlkTable {
  pmem::MetaBlock* meta;

  MemTable* mem_table;
  LogMgr* log_mgr;
  TxMgr* tx_mgr;

  tbb::concurrent_vector<LogicalBlockIdx> table;

  // keep track of the next TxEntry to apply
  TxEntryIdx tail_tx_idx;
  pmem::TxBlock* tail_tx_block;
  pthread_spinlock_t spinlock;

 public:
  BlkTable() = default;
  explicit BlkTable(pmem::MetaBlock* meta, MemTable* mem_table, LogMgr* log_mgr,
                    TxMgr* tx_mgr)
      : meta(meta),
        mem_table(mem_table),
        log_mgr(log_mgr),
        tx_mgr(tx_mgr),
        tail_tx_idx(),
        tail_tx_block(nullptr) {
    table.resize(16);
    pthread_spin_init(&spinlock, PTHREAD_PROCESS_PRIVATE);
  }

  ~BlkTable() { pthread_spin_destroy(&spinlock); }

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
   * @param do_alloc whether we allow allocation when iterating the tx_idx.
   * default is false, and only set to true when write permission is granted
   */
  void update(bool do_alloc = false) {
    pthread_spin_lock(&spinlock);
    // it's possible that the previous update move idx to overflow state
    if (!tx_mgr->handle_idx_overflow(tail_tx_idx, tail_tx_block, do_alloc)) {
      // if still overflow, do_alloc must be unset
      assert(!do_alloc);
      // if still overflow, we must have reached the tail already
      return;
    }

    while (true) {
      auto tx_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
      if (!tx_entry.is_valid()) break;
      if (tx_entry.is_commit()) apply_tx(tx_entry.commit_entry);
      // FIXME: handle race condition??
      if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc)) break;
    }
    pthread_spin_unlock(&spinlock);
  }

  /**
   * Get the next tx to apply
   *
   * @param[out] tx_idx the index of the current transaction tail
   * @param[out] tx_block the log block corresponding to the transaction
   */
  void get_tail_tx(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block) const {
    tx_idx = tail_tx_idx;
    tx_block = tail_tx_block;
  }

 private:
  void resize_to_fit(VirtualBlockIdx idx) {
    if (table.size() > idx) return;
    // ref: https://jameshfisher.com/2018/03/30/round-up-power-2/
    int next_pow2 = 1 << (64 - __builtin_clzl(idx - 1));
    table.resize(next_pow2);
  }

  // TODO: handle writev requests
  /**
   * Apply a transaction to the block table
   */
  void apply_tx(pmem::TxCommitEntry tx_commit_entry) {
    auto log_entry_idx = tx_commit_entry.log_entry_idx;

    uint32_t num_blocks;
    VirtualBlockIdx begin_virtual_idx;
    std::vector<LogicalBlockIdx> begin_logical_idxs;
    log_mgr->get_coverage(log_entry_idx, begin_virtual_idx, num_blocks,
                          &begin_logical_idxs);

    size_t now_logical_idx_off = 0;
    VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
    VirtualBlockIdx end_virtual_idx = begin_virtual_idx + num_blocks;
    resize_to_fit(end_virtual_idx);

    while (now_virtual_idx < end_virtual_idx) {
      uint16_t chunk_blocks =
          end_virtual_idx > now_virtual_idx + MAX_BLOCKS_PER_BODY
              ? MAX_BLOCKS_PER_BODY
              : end_virtual_idx - now_virtual_idx;
      for (uint32_t i = 0; i < chunk_blocks; ++i) {
        table[now_virtual_idx + i] =
            begin_logical_idxs[now_logical_idx_off] + i;
      }
      now_virtual_idx += chunk_blocks;
      now_logical_idx_off++;
    }
  }

  friend std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
    out << "BlkTable:\n";
    for (size_t i = 0; i < b.table.size(); ++i) {
      if (b.table[i] != 0) {
        out << "\t" << i << " -> " << b.table[i] << "\n";
      }
    }
    return out;
  }
};

/**
 * A sugar function to map vidx to addr
 */
static inline pmem::Block* tables_vidx_to_addr(MemTable* mem_table,
                                               const BlkTable* blk_table,
                                               VirtualBlockIdx idx) {
  return mem_table->get(blk_table->get(idx));
}

}  // namespace ulayfs::dram
