#pragma once

#include "layout.h"

namespace ulayfs::dram {
struct TxIter {
 private:
  pmem::MetaBlock* meta;
  MemTable* mem_table;

  pmem::TxLogBlock* block;
  pmem::TxEntryIdx idx;

 public:
  TxIter(pmem::MetaBlock* meta, MemTable* mem_table, pmem::TxEntryIdx idx)
      : meta(meta), mem_table(mem_table), idx(idx) {}

  pmem::TxEntry operator*() const {
    if (idx.block_idx == 0) {
      return meta->get_inline_tx_entry(idx.local_idx);
    } else {
      return block->get_entry(idx.local_idx);
    }
  }

  TxIter& operator++() {
    if (idx.block_idx == 0) {
      if (idx.local_idx < pmem::NUM_INLINE_TX_ENTRY) {
        idx.local_idx++;
      } else {
        idx = meta->get_tx_log_head();
      }
    } else {
      if (idx.local_idx < pmem::NUM_TX_ENTRY) {
        idx.local_idx++;
      } else {
        block = &mem_table->get_addr(idx.block_idx)->tx_log_block;
        idx.block_idx = block->get_next_block_idx();
        idx.local_idx = idx.block_idx == 0 ? -1 : 0;
      }
    }

    if (this->operator*().data == 0) {
      idx.block_idx = 0;
      idx.local_idx = -1;
    }

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