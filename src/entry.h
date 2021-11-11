#pragma once

#include <cassert>
#include <iostream>
#include <tuple>

#include "idx.h"
#include "utils.h"

namespace ulayfs::pmem {

enum class TxEntryType : bool {
  TX_BEGIN = false,
  TX_COMMIT = true,
};

struct TxCommitEntry {
 private:
  static constexpr int NUM_BLOCKS_BITS = 6;
  static constexpr int BEGIN_VIRTUAL_IDX_BITS = 17;

  static constexpr int NUM_BLOCKS_MAX = (1 << NUM_BLOCKS_BITS) - 1;
  static constexpr int BEGIN_VIRTUAL_IDX_MAX =
      (1 << BEGIN_VIRTUAL_IDX_BITS) - 1;

  enum TxEntryType type : 1 = TxEntryType::TX_COMMIT;

  friend union TxEntry;

 public:
  // optionally, set these bits so OCC conflict detection can be done inline
  uint32_t num_blocks : NUM_BLOCKS_BITS;
  uint32_t begin_virtual_idx : BEGIN_VIRTUAL_IDX_BITS;

  // the first log entry for this transaction, 40 bits in size
  // The rest of the log entries are organized as a linked list
  LogEntryIdx log_entry_idx;

  // It's an optimization that num_blocks and virtual_block_idx could inline
  // with TxCommitEntry, but only if they could fit in.
  TxCommitEntry(uint32_t num_blocks, uint32_t begin_virtual_idx,
                LogEntryIdx log_entry_idx)
      : num_blocks(0), begin_virtual_idx(0), log_entry_idx(log_entry_idx) {
    assert(num_blocks <= NUM_BLOCKS_MAX);
    assert(begin_virtual_idx <= BEGIN_VIRTUAL_IDX_MAX);
    this->num_blocks = num_blocks;
    this->begin_virtual_idx = begin_virtual_idx;
  }

  friend std::ostream& operator<<(std::ostream& out,
                                  const TxCommitEntry& entry) {
    out << "TxCommitEntry{n_blk=" << entry.num_blocks
        << ", vidx=" << entry.begin_virtual_idx << "}";
    return out;
  }
};

union TxEntry {
 private:
  uint64_t raw_bits;

 public:
  // WARN: begin_entry is deprecated
  TxCommitEntry commit_entry;

  TxEntry(){};
  TxEntry(uint64_t raw_bits) : raw_bits(raw_bits) {}
  TxEntry(TxCommitEntry commit_entry) : commit_entry(commit_entry) {}

  [[nodiscard]] bool is_commit() const {
    return commit_entry.type == TxEntryType::TX_COMMIT;
  }

  [[nodiscard]] bool is_valid() const { return raw_bits != 0; }

  [[nodiscard]] static bool is_last_entry_in_cacheline(TxLocalIdx idx) {
    auto offset = 2 * sizeof(LogicalBlockIdx) + (idx + 1) * sizeof(TxEntry);
    return (offset & (CACHELINE_SIZE - 1)) == 0;
  }

  /**
   * find the tail (next unused slot) in an array of TxEntry
   *
   * @tparam NUM_ENTRIES the total number of entries in the array
   * @param entries a pointer to an array of tx entries
   * @param hint hint to start the search
   * @return the local index of next available TxEntry; NUM_ENTRIES if not found
   */
  template <uint16_t NUM_ENTRIES>
  static TxLocalIdx find_tail(TxEntry entries[], TxLocalIdx hint = 0) {
    for (TxLocalIdx idx = hint; idx < NUM_ENTRIES; ++idx)
      if (!entries[idx].is_valid()) return idx;
    return NUM_ENTRIES;
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
  static TxEntry try_append(TxEntry entries[], TxEntry entry, TxLocalIdx idx) {
    uint64_t expected = 0;
    if (__atomic_compare_exchange_n(&entries[idx].raw_bits, &expected,
                                    entry.raw_bits, false, __ATOMIC_RELEASE,
                                    __ATOMIC_ACQUIRE))
      // only persist if it's the last entry in a cacheline
      if (is_last_entry_in_cacheline(idx)) persist_cl_fenced(&entries[idx]);
    // if CAS fails, `expected` will be stored the value in entries[idx]
    // if success, it will return 0
    return expected;
  }

  friend std::ostream& operator<<(std::ostream& out, const TxEntry& tx_entry) {
    if (tx_entry.is_commit()) out << tx_entry.commit_entry;
    return out;
  }
};

static_assert(sizeof(TxEntry) == 8, "TxEntry must be 64 bits");
static_assert(sizeof(TxCommitEntry) == 8, "TxEntry must be 64 bits");

enum class LogOp {
  LOG_INVALID = 0,
  // we start the enum from 1 so that a LogOp with value 0 is invalid
  LOG_OVERWRITE = 1,
};

// Since allocator can only guarantee to allocate 64 contiguous blocks (by
// single CAS), log entry must organize as a linked list in case of a large
// size transaction.
struct LogEntry {
  // we use bitfield to pack `op` and `last_remaining` into 16 bits
  enum LogOp op : 4;

  // the remaining number of bytes that are not used in this log entry
  // only the last log entry for a tx can have non-zero value for this field
  // the maximum number of remaining bytes is BLOCK_SIZE - 1
  uint16_t last_remaining : 12;

  // the number of blocks within a log entry is at most 64
  uint8_t num_blocks;

  // the index of the next log entry
  LogEntryIdx next;

  // we map the logical blocks [logical_idx, logical_idx + num_blocks)
  // to the virtual blocks [virtual_idx, virtual_idx + num_blocks)
  VirtualBlockIdx begin_virtual_idx;
  LogicalBlockIdx begin_logical_idx;

  friend std::ostream& operator<<(std::ostream& out, const LogEntry& entry) {
    out << "LogEntry{";
    out << "vidx=" << entry.begin_virtual_idx << ", ";
    out << "lidx=" << entry.begin_logical_idx << ", ";
    out << "n_blk=" << unsigned(entry.num_blocks);
    out << "}";
    return out;
  }
};

static_assert(sizeof(LogEntry) == 16, "LogEntry must of size 16 bytes");

}  // namespace ulayfs::pmem
