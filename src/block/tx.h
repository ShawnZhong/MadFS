#pragma once

#include <atomic>
#include <cassert>
#include <cstring>
#include <ostream>
#include <unordered_set>

#include "const.h"
#include "entry.h"
#include "idx.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace madfs::dram {
struct TxCursor;
}

namespace madfs::utility {
class GarbageCollector;
}

namespace madfs::pmem {

class TxBlock : public noncopyable {
  std::atomic<TxEntry> tx_entries[NUM_TX_ENTRY_PER_BLOCK];
  // next is placed after tx_entires so that it could be flushed with tx_entries
  std::atomic<LogicalBlockIdx> next;
  // orphan blocks are organized as an orphan list, which will be recycled
  // once they are not referenced
  // if this tx block is the latest, this field must be 0.
  std::atomic<LogicalBlockIdx> next_orphan;

  // tx seq is used to construct total order between tx entries, so it must
  // increase monotonically
  // when compare two TxEntryIdx
  // if within same block, compare local index
  // if not, compare their block's seq number
  uint32_t tx_seq;
  // unused uint32_t for padding
  uint32_t unused;

  friend utility::GarbageCollector;

 public:
  [[nodiscard]] TxLocalIdx find_tail(TxLocalIdx hint = 0) const {
    return TxEntry::find_tail<NUM_TX_ENTRY_PER_BLOCK>(tx_entries, hint);
  }

  // THIS FUNCTION IS NOT THREAD SAFE
  void store(TxEntry entry, TxLocalIdx idx) {
    tx_entries[idx].store(entry, std::memory_order_relaxed);
  }

  void set_tx_seq(uint32_t tx_seq) {
    assert(tx_seq > 0);  // 0 is an invalid tx_seq
    this->tx_seq = tx_seq;
  }

  [[nodiscard]] uint32_t get_tx_seq() const {
    assert(tx_seq > 0);
    return tx_seq;
  }

  [[nodiscard]] LogicalBlockIdx get_next_tx_block() const {
    return next.load(std::memory_order_acquire);
  }

  /**
   * Set the next block index
   * @return true on success, false if there is a race condition
   */
  bool try_set_next_tx_block(LogicalBlockIdx block_idx) {
    LogicalBlockIdx expected = 0;
    return next.compare_exchange_strong(expected, block_idx,
                                        std::memory_order_acq_rel,
                                        std::memory_order_acquire);
  }

  [[nodiscard]] LogicalBlockIdx get_next_orphan_block() const {
    return next_orphan.load(std::memory_order_acquire);
  }

  void set_next_orphan_block(LogicalBlockIdx block_idx) {
    next_orphan.store(block_idx, std::memory_order_release);
    persist_cl_unfenced(&next_orphan);
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

  friend struct ::madfs::dram::TxCursor;

  friend std::ostream &operator<<(std::ostream &os, const TxBlock &block) {
    os << "TxBlock{tx_seq=" << block.tx_seq << ", next=" << block.next
       << ", next_orphan=" << block.next_orphan << "}";
    return os;
  }
};

static_assert(sizeof(TxBlock) == BLOCK_SIZE,
              "TxBlock must be of size BLOCK_SIZE");

}  // namespace madfs::pmem
