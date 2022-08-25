#pragma once

#include "block/tx.h"
#include "idx.h"
#include "timer.h"

namespace ulayfs::dram {

/**
 * @brief A TxCursor is a pointer to a transaction entry
 *
 * It is 16 bytes in size and can be passed around by value in the registers.
 */
struct TxCursor {
  TxEntryIdx idx;
  union {
    pmem::TxBlock* block;
    pmem::MetaBlock* meta;
    void* addr;
  };

  TxCursor() : idx(), addr(nullptr) {}
  TxCursor(pmem::MetaBlock* meta) : idx(), addr(meta) {}
  TxCursor(LogicalBlockIdx block_idx, pmem::TxBlock* block)
      : idx(block_idx, 0), addr(block) {}
  TxCursor(TxEntryIdx idx, pmem::TxBlock* block) : idx(idx), addr(block) {}

  /**
   * @return the tx entry pointed to by this cursor
   */
  pmem::TxEntry get_entry() const {
    TimerGuard<Event::TX_ENTRY_LOAD> timer_guard;
    assert(addr != nullptr);
    std::atomic<pmem::TxEntry>* entries =
        idx.is_inline() ? meta->tx_entries : block->tx_entries;
    assert(idx.local_idx < idx.get_capacity());
    return entries[idx.local_idx].load(std::memory_order_acquire);
  }

  /**
   * try to append an entry to a slot in an array of TxEntry; fail if the slot
   * is taken (likely due to a race condition)
   *
   * @param entries a pointer to an array of tx entries
   * @param entry the entry to append
   * @param hint hint to start the search
   * @return if success, return 0; otherwise, return the entry on the slot
   */
  pmem::TxEntry try_append(pmem::TxEntry entry) {
    TimerGuard<Event::TX_ENTRY_STORE> timer_guard;
    assert(addr != nullptr);
    std::atomic<pmem::TxEntry>* entries =
        idx.is_inline() ? meta->tx_entries : block->tx_entries;

    pmem::TxEntry expected = 0;
    entries[idx.local_idx].compare_exchange_strong(
        expected, entry, std::memory_order_acq_rel, std::memory_order_acquire);
    // if CAS fails, `expected` will be stored the value in entries[idx]
    // if success, it will return 0
    return expected;
  }

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
