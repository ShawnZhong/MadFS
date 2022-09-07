#pragma once

#include "alloc.h"
#include "block/tx.h"
#include "idx.h"
#include "mem_table.h"
#include "timer.h"

namespace ulayfs::dram {

/**
 * @brief A TxCursor is a pointer to a transaction entry. It contains a
 * TxEntryIdx and a pointer to the block containing the entry. It is 16 bytes in
 * size and can be passed around by value in the registers.
 */
struct TxCursor {
  TxEntryIdx idx;
  union {
    pmem::TxBlock* block;
    pmem::MetaBlock* meta;
    void* addr;
  };

  TxCursor() : idx(), addr(nullptr) {}
  TxCursor(TxEntryIdx idx, void* addr) : idx(idx), addr(addr) {}

  /**
   * @return the tx entry pointed to by this cursor
   */
  [[nodiscard]] pmem::TxEntry get_entry() const {
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
  [[nodiscard]] pmem::TxEntry try_append(pmem::TxEntry entry) const {
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

  /**
   * If the given cursor is in an overflow state, update it if allowed.
   *
   * @param[in] allocator allocator to allocate new blocks
   * @param[in] mem_table used to find the memory address of the next block
   * @param[out] into_new_block if not nullptr, return whether the cursor is
   * advanced into a new tx block
   * @return true if the idx is not in overflow state; false otherwise
   */
  bool handle_overflow(MemTable* mem_table, Allocator* allocator = nullptr,
                       bool* is_overflow = nullptr) {
    const bool is_inline = idx.is_inline();
    uint16_t capacity = idx.get_capacity();
    if (unlikely(idx.local_idx >= capacity)) {
      if (is_overflow) *is_overflow = true;
      LogicalBlockIdx block_idx =
          is_inline ? meta->get_next_tx_block() : block->get_next_tx_block();
      if (block_idx == 0) {
        if (!allocator) return false;
        const auto& [new_block_idx, new_block] =
            is_inline ? allocator->alloc_next_tx_block(meta)
                      : allocator->alloc_next_tx_block(block);
        idx.block_idx = new_block_idx;
        block = new_block;
        idx.local_idx -= capacity;
      } else {
        idx.block_idx = block_idx;
        idx.local_idx -= capacity;
        block = &mem_table->lidx_to_addr_rw(idx.block_idx)->tx_block;
      }
    } else {
      if (is_overflow) *is_overflow = false;
    }
    return true;
  }

  /**
   * Advance cursor to the next transaction entry
   *
   * @param[in] cursor the cursor to advance
   * @param[in] allocator if given, allocate new blocks when reaching the end of
   * a block
   * @param[out] into_new_block if not nullptr, return whether the cursor is
   * advanced into a new tx block
   *
   * @return true on success; false when reaches the end of a block and
   * allocator is not given. The advance would happen anyway but in the case
   * of false, it is in a overflow state
   */
  bool advance(MemTable* mem_table, Allocator* allocator = nullptr,
               bool* into_new_block = nullptr) {
    idx.local_idx++;
    return handle_overflow(mem_table, allocator, into_new_block);
  }

  static void flush_up_to(MemTable* mem_table, TxCursor end) {
    pmem::MetaBlock* meta = mem_table->get_meta();
    TxEntryIdx begin_idx = meta->get_tx_tail();
    void* addr =
        begin_idx.is_inline()
            ? static_cast<void*>(meta)
            : mem_table->lidx_to_addr_rw(begin_idx.block_idx)->data_rw();

    flush_range(mem_table, TxCursor(begin_idx, addr), end);
  }

  static void flush_range(MemTable* mem_table, TxCursor begin, TxCursor end) {
    if (begin >= end) return;
    pmem::TxBlock* tx_block_begin;
    // handle special case of inline tx
    if (begin.idx.block_idx == 0) {
      if (end.idx.block_idx == 0) {
        begin.meta->flush_tx_entries(begin.idx.local_idx, end.idx.local_idx);
        goto done;
      }
      begin.meta->flush_tx_block(begin.idx.local_idx);
      // now the next block is the "new begin"
      begin.idx = {begin.meta->get_next_tx_block(), 0};
    }
    while (begin.idx.block_idx != end.idx.block_idx) {
      tx_block_begin =
          &mem_table->lidx_to_addr_rw(begin.idx.block_idx)->tx_block;
      tx_block_begin->flush_tx_block(begin.idx.local_idx);
      begin.idx = {tx_block_begin->get_next_tx_block(), 0};
      // special case: tx_idx_end is the first entry of the next block, which
      // means we only need to flush the current block and no need to
      // dereference to get the last block
    }
    if (begin.idx.local_idx == end.idx.local_idx) goto done;
    end.block->flush_tx_entries(begin.idx.local_idx, end.idx.local_idx);

  done:
    fence();
  }

  /**
   * Move along the linked list of TxBlock and find the tail. The returned
   * tail may not be up-to-date due to race condition. No new blocks will be
   * allocated. If the end of TxBlock is reached, just return NUM_TX_ENTRY as
   * the TxLocalIdx.
   */
  static TxCursor find_tail(MemTable* mem_table, TxCursor cursor) {
    LogicalBlockIdx next_block_idx;

    if (cursor.idx.block_idx == 0) {  // search from meta
      if ((next_block_idx = cursor.meta->get_next_tx_block()) != 0) {
        cursor.idx.local_idx = cursor.meta->find_tail(cursor.idx.local_idx);
        return cursor;
      } else {
        cursor.idx.block_idx = next_block_idx;
        cursor.idx.local_idx = 0;
        cursor.block =
            &mem_table->lidx_to_addr_rw(cursor.idx.block_idx)->tx_block;
      }
    }

    while ((next_block_idx = cursor.block->get_next_tx_block()) != 0) {
      cursor.idx.block_idx = next_block_idx;
      cursor.block = &(mem_table->lidx_to_addr_rw(next_block_idx)->tx_block);
    }
    cursor.idx.local_idx = cursor.block->find_tail(cursor.idx.local_idx);
    return cursor;
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
