#pragma once

#include <iostream>
#include <tuple>

#include "idx.h"
#include "utils.h"

namespace ulayfs::pmem {

/**
 * A log entry is identified by the index of the LogEntryBlock and the local
 * index within the block
 *
 * 5 bytes (40 bits) in size
 */
struct __attribute__((packed)) LogEntryIdx {
  LogicalBlockIdx block_idx;
  LogLocalIdx local_idx : 8;

  friend std::ostream& operator<<(std::ostream& out, const LogEntryIdx& idx) {
    out << "{ block_idx = " << idx.block_idx
        << ", local_idx = " << unsigned(idx.local_idx) << " }";
    return out;
  }
};

static_assert(sizeof(LogEntryIdx) == 5, "LogEntryIdx must of size 5 bytes");

/**
 * A transaction entry is identified by the block index and the local index
 */
struct TxEntryIdx {
  LogicalBlockIdx block_idx;
  TxLocalIdx local_idx;

  bool operator==(const TxEntryIdx& rhs) const {
    return block_idx == rhs.block_idx && local_idx == rhs.local_idx;
  }
  bool operator!=(const TxEntryIdx& rhs) const { return !(rhs == *this); }
  bool operator<(const TxEntryIdx& rhs) const {
    return std::tie(block_idx, local_idx) <
           std::tie(rhs.block_idx, rhs.local_idx);
  }
  bool operator>(const TxEntryIdx& rhs) const { return rhs < *this; }
  bool operator<=(const TxEntryIdx& rhs) const { return !(rhs < *this); }
  bool operator>=(const TxEntryIdx& rhs) const { return !(*this < rhs); }

  friend std::ostream& operator<<(std::ostream& out, const TxEntryIdx& idx) {
    out << "{ block_idx = " << idx.block_idx
        << ", local_idx = " << idx.local_idx << " }";
    return out;
  }
};

static_assert(sizeof(TxEntryIdx) == 8, "TxEntryIdx must be 64 bits");

enum class TxEntryType : bool {
  TX_BEGIN = false,
  TX_COMMIT = true,
};

struct TxBeginEntry {
  enum TxEntryType type : 1 = TxEntryType::TX_BEGIN;

  VirtualBlockIdx block_idx_start : 31;
  VirtualBlockIdx block_idx_end;

  /**
   * Construct a tx begin_tx entry for the range
   * [block_idx_start, block_idx_start + num_blocks]
   */
  explicit TxBeginEntry(VirtualBlockIdx block_idx_start, uint32_t num_blocks)
      : block_idx_start(block_idx_start) {
    block_idx_end = block_idx_start + num_blocks;
  }

  friend std::ostream& operator<<(std::ostream& os, const TxBeginEntry& entry) {
    os << "TX_BEGIN "
       << "{ block_idx_start = " << entry.block_idx_start
       << ", block_idx_end: " << entry.block_idx_end << " }";
    return os;
  }
};

struct TxCommitEntry {
  enum TxEntryType type : 1 = TxEntryType::TX_COMMIT;

  // how many entries ahead is the corresponding TxBeginEntry
  // the value stored should always be positive
  uint32_t begin_offset : 23;

  // the first log entry for this transaction, 40 bits in size
  // The rest of the log entries are organized as a linked list
  LogEntryIdx log_entry_idx;

  TxCommitEntry(uint32_t begin_offset, const LogEntryIdx log_entry_idx)
      : begin_offset(begin_offset), log_entry_idx(log_entry_idx) {}

  friend std::ostream& operator<<(std::ostream& out,
                                  const TxCommitEntry& entry) {
    out << "TX_COMMIT "
        << "{ begin_offset: " << entry.begin_offset
        << ", log_entry_idx: " << entry.log_entry_idx << " }";
    return out;
  }
};

union TxEntry {
  TxBeginEntry begin_entry;
  TxCommitEntry commit_entry;
  uint64_t raw_bits;

  TxEntry(){};
  TxEntry(uint64_t raw_bits) : raw_bits(raw_bits) {}
  TxEntry(TxBeginEntry begin_entry) : begin_entry(begin_entry) {}
  TxEntry(TxCommitEntry commit_entry) : commit_entry(commit_entry) {}

  [[nodiscard]] bool is_begin() const {
    return begin_entry.type == TxEntryType::TX_BEGIN;
  }
  [[nodiscard]] bool is_commit() const {
    return commit_entry.type == TxEntryType::TX_COMMIT;
  }

  [[nodiscard]] bool is_valid() const { return raw_bits != 0; }

  [[nodiscard]] static bool is_last_entry_in_cacheline(TxLocalIdx idx) {
    constexpr uint16_t num_entries_in_cl = CACHELINE_SIZE / sizeof(TxEntry);
    return (idx + 1) % num_entries_in_cl == 0;
  }

  /**
   * find the tail (next unused slot) in an array of TxEntry
   *
   * @tparam NUM_ENTRIES the total number of entries in the array
   * @param entries a pointer to an array of tx entries
   * @param hint hint to start the search
   * @return the local index of next available TxEntry; -1 if not found
   */
  template <uint16_t NUM_ENTRIES>
  static TxLocalIdx find_tail(TxEntry entries[], TxLocalIdx hint) {
    for (TxLocalIdx idx = hint; idx < NUM_ENTRIES; ++idx)
      if (entries[idx].raw_bits == 0) return idx;
    return -1;
  }

  /**
   * try to append an entry to a slot in an array of TxEntry; fail if the slot
   * is taken (likely due to a race condition)
   *
   * @tparam NUM_ENTRIES the total number of entries in the array
   * @param entries a pointer to an array of tx entries
   * @param entry the entry to append
   * @param hint hint to start the search
   * @return if success, return 0; otherwise, return the entry on the slot (in
   * raw bits)
   */
  template <uint16_t NUM_ENTRIES>
  static uint64_t try_append(TxEntry entries[], TxEntry entry, TxLocalIdx idx) {
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
    if (tx_entry.is_begin())
      out << tx_entry.begin_entry;
    else if (tx_entry.is_commit())
      out << tx_entry.commit_entry;
    return out;
  }
};

static_assert(sizeof(TxEntry) == 8, "TxEntry must be 64 bits");
static_assert(sizeof(TxBeginEntry) == 8, "TxEntry must be 64 bits");
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
};

static_assert(sizeof(LogEntry) == 16, "LogEntry must of size 16 bytes");

}  // namespace ulayfs::pmem
