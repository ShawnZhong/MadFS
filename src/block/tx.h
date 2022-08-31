#pragma once

#include <atomic>
#include <cassert>
#include <cstring>

#include "const.h"
#include "entry.h"
#include "idx.h"
#include "logging.h"
#include "utils.h"

namespace ulayfs::dram {
struct TxCursor;
}

namespace ulayfs::pmem {

class TxBlock : public noncopyable {
  std::atomic<TxEntry> tx_entries[NUM_TX_ENTRY_PER_BLOCK];
  // next is placed after tx_entires so that it could be flushed with tx_entries
  std::atomic<LogicalBlockIdx> next;
  // outdated blocks are organized as a to-be-freed list, which will be recycled
  // once they are not referenced
  // if this tx block is the latest, this field must be 0.
  std::atomic<LogicalBlockIdx> next_outdated;

  // tx seq is used to construct total order between tx entries, so it must
  // increase monotonically
  // when compare two TxEntryIdx
  // if within same block, compare local index
  // if not, compare their block's seq number
  uint32_t tx_seq;
  // each garbage collection operation has a unique, monotonically increasing
  // sequence number; this can be viewed as a "version number" of tx_seq so that
  // tx seq number can be reused
  uint32_t gc_seq;

 public:
  [[nodiscard]] TxLocalIdx find_tail(TxLocalIdx hint = 0) const {
    return TxEntry::find_tail<NUM_TX_ENTRY_PER_BLOCK>(tx_entries, hint);
  }

  // THIS FUNCTION IS NOT THREAD SAFE
  void store(TxEntry entry, TxLocalIdx idx) {
    tx_entries[idx].store(entry, std::memory_order_relaxed);
  }

  // it should be fine not to use any fence since there will be fence for flush
  // gc_seq must be zero for apps; it can only be set to nonzero by gc threads
  void set_tx_seq(uint32_t tx_seq, uint32_t gc_seq = 0) {
    this->tx_seq = tx_seq;
    this->gc_seq = gc_seq;
  }
  // return tx_seq (which is mostly commonly used) as return value; if user asks
  // for gc_seq, return it through pointer
  [[nodiscard]] uint32_t get_tx_seq(uint32_t* gc_seq_addr = nullptr) const {
    if (gc_seq_addr) *gc_seq_addr = gc_seq;
    return tx_seq;
  }

  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const {
    return next.load(std::memory_order_acquire);
  }

  [[nodiscard]] LogicalBlockIdx get_next_outdated_tx_block() const {
    return next_outdated.load(std::memory_order_acquire);
  }

  /**
   * Set the next block index
   * @return true on success, false if there is a race condition
   */
  bool set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    return next.compare_exchange_strong(expected, block_idx,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
  }

  /**
   * flush the current block starting from `begin_idx` (including two pointers)
   *
   * @param begin_idx where to start flush
   */
  void flush_tx_block(TxLocalIdx begin_idx = 0) {
    persist_unfenced(&tx_entries[begin_idx],
                     sizeof(TxEntry) * (NUM_TX_ENTRY_PER_BLOCK - begin_idx) +
                         2 * sizeof(LogicalBlockIdx));
  }

  /**
   * flush a range of tx entries
   *
   * @param begin_idx
   */
  void flush_tx_entries(TxLocalIdx begin_idx, TxLocalIdx end_idx) {
    assert(end_idx > begin_idx);
    persist_unfenced(&tx_entries[begin_idx],
                     sizeof(TxEntry) * (end_idx - begin_idx));
  }

  friend struct ::ulayfs::dram::TxCursor;
};

static_assert(sizeof(TxBlock) == BLOCK_SIZE,
              "TxBlock must be of size BLOCK_SIZE");

}  // namespace ulayfs::pmem
