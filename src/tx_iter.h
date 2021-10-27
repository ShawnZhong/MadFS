#pragma once

#include "layout.h"

namespace ulayfs::dram {
struct TxIter {
 private:
  pmem::MetaBlock* meta;
  MemTable* mem_table;

  pmem::TxLogBlock* block;
  pmem::TxEntryIdx idx;
  pmem::TxEntry entry;

  /**
   * Move to the next transaction entry
   */
  void forward() {
    // the current one is an inline tx entry
    if (idx.block_idx == 0) {
      // the next entry is still an inline tx entry
      if (idx.local_idx + 1 < pmem::NUM_INLINE_TX_ENTRY) {
        idx.local_idx++;
        return;
      }

      // move to the tx block
      idx = meta->get_tx_log_head();
      return;
    }

    // the current on is in tx_log_block, and the next one is in the same block
    if (idx.local_idx + 1 < pmem::NUM_TX_ENTRY) {
      idx.local_idx++;
      return;
    }

    // move to the next block
    block = &mem_table->get_addr(idx.block_idx)->tx_log_block;
    idx.block_idx = block->get_next_block_idx();
    idx.local_idx = idx.block_idx == 0 ? -1 : 0;
  }

  /**
   * Read the entry from the MetaBlock or TxLogBlock and update the internal var
   */
  void read_entry() {
    if (!is_valid()) return;
    entry = idx.block_idx == 0 ? meta->get_inline_tx_entry(idx.local_idx)
                               : block->get_entry(idx.local_idx);

    if (entry.data == 0) {
      idx.block_idx = 0;
      idx.local_idx = -1;
    }
  }

 public:
  TxIter(pmem::MetaBlock* meta, MemTable* mem_table, pmem::TxEntryIdx idx)
      : meta(meta), mem_table(mem_table), idx(idx) {
    read_entry();
  }

  [[nodiscard]] inline bool is_valid() const { return idx.local_idx >= 0; }
  [[nodiscard]] pmem::TxEntryIdx get_idx() const { return idx; }
  pmem::TxEntry operator*() const { return entry; }

  TxIter& operator++() {
    forward();
    read_entry();
    return *this;
  }

  TxIter operator++(int) {
    TxIter tmp = *this;
    ++(*this);
    return tmp;
  }

  friend bool operator==(const TxIter& a, const TxIter& b) {
    return a.idx == b.idx;
  };

  friend bool operator!=(const TxIter& a, const TxIter& b) {
    return a.idx != b.idx;
  };
};

}  // namespace ulayfs::dram
