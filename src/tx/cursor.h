#pragma once

#include "block/tx.h"
#include "idx.h"

namespace ulayfs::dram {

/**
 * @brief A TxCursor is a pointer to a transaction entry
 *
 * It is 16 bytes in size and can be passed around by value in the registers.
 */
struct TxCursor {
  TxEntryIdx idx;
  pmem::TxBlock* block;

  friend bool operator==(const TxCursor& lhs, const TxCursor& rhs) {
    return lhs.idx == rhs.idx && lhs.block == rhs.block;
  }

  friend bool operator!=(const TxCursor& lhs, const TxCursor& rhs) {
    return !(rhs == lhs);
  }

  friend bool operator<(const TxCursor& lhs, const TxCursor& rhs) {
    if (lhs.idx.block_idx == rhs.idx.block_idx)
      return lhs.idx.local_idx < rhs.idx.local_idx;
    if (lhs.idx.block_idx == 0) return true;
    if (rhs.idx.block_idx == 0) return false;
    return lhs.block->get_tx_seq() < rhs.block->get_tx_seq();
  }

  friend bool operator>(const TxCursor& lhs, const TxCursor& rhs) {
    return rhs < lhs;
  }

  friend bool operator<=(const TxCursor& lhs, const TxCursor& rhs) {
    return !(rhs < lhs);
  }

  friend bool operator>=(const TxCursor& lhs, const TxCursor& rhs) {
    return !(lhs < rhs);
  }
};

static_assert(sizeof(TxCursor) == 16);
}  // namespace ulayfs::dram
