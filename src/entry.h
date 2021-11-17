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

  // points to the first LogHeadEntry for the group of log entries for this
  // transaction
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

union LogHeadNext {
 private:
  uint32_t raw_bits;

 public:
  // if saturate, stores block idx of the next LogBlock, and next local idx
  // must be zero
  LogicalBlockIdx next_block_idx;
  // if !saturate, stores local idx of the next head entry since it must be
  // in the current LogBlock
  LogLocalUnpackIdx next_local_idx;

  [[nodiscard]] bool is_valid() const { return raw_bits != 0; }
};

static_assert(sizeof(LogHeadNext) == 4, "LogHeadNext must be 32 bits");

struct LogHeadEntry {
 private:
  friend union LogEntry;

 public:
  // true if there is an overflow segment in another LogBlock following me
  bool overflow : 1;
  // true if my segment fills the current LogBlock; overflow implies saturate
  bool saturate : 1;

  // the operation code, e.g., LOG_OVERWRITE
  enum LogOp op : 2;

  // the remaining number of bytes that are not used in this log entry
  // only the last log entry for a tx can have non-zero value for this field
  // the maximum number of remaining bytes is BLOCK_SIZE - 1
  uint16_t leftover_bytes : 12;

  // the number of blocks recorded in my segment (not the entire transaction
  // if there are overflow segments)
  // number of blocks per body entry is 64 except for the last body entry,
  // so num_blocks = 64 * num_local_entries - leftover_blocks
  uint16_t num_blocks;

  // use 4 bytes to record where is the next LogHeadEntry
  // if saturate, then next is a LogicalBlockIdx, otherwise a LogLocalIdx
  // if overflow, then next points to the next overflow segment of the same
  // transaction; if !overflow and next is not null, then next points to
  // the next separate request of the same transaction in e.g. writev; if
  // next is null, then this is the end of a transaction
  union LogHeadNext next;

  friend std::ostream& operator<<(std::ostream& out,
                                  const LogHeadEntry& entry) {
    out << "LogHeadEntry{";
    out << "overflow=" << (entry.overflow ? "T" : "F") << ", ";
    out << "saturate=" << (entry.saturate ? "T" : "F") << ", ";
    out << "op=" << unsigned(entry.op) << ", ";
    out << "left_bytes" << entry.leftover_bytes << ", ";
    out << "n_blk=" << entry.num_blocks << ", ";
    out << "has_next=" << (entry.next.is_valid() ? "T" : "F");
    out << "}";
    return out;
  }
};

struct LogBodyEntry {
 private:
  friend union LogEntry;

 public:
  // represents length of 64 blocks except for the last body entry in a
  // log segment
  // we map the logical blocks [logical_idx, logical_idx + length)
  // to the virtual blocks [virtual_idx, virtual_idx + length)
  VirtualBlockIdx begin_virtual_idx;
  LogicalBlockIdx begin_logical_idx;

  friend std::ostream& operator<<(std::ostream& out,
                                  const LogBodyEntry& entry) {
    out << "LogBodyEntry{";
    out << "vidx=" << entry.begin_virtual_idx << ", ";
    out << "lidx=" << entry.begin_logical_idx;
    out << "}";
    return out;
  }
};

union LogEntry {
 private:
  uint64_t raw_bits;

 public:
  LogHeadEntry head_entry;
  LogBodyEntry body_entry;

  [[nodiscard]] bool is_valid() const { return raw_bits != 0; }

  // subtypes of LogEntry does not have a type field to differentiate
  // between head and body due to size limit, but the caller must know
  // which entry is being worked on
};

static_assert(sizeof(LogEntry) == 8, "LogEntry must be 64 bits");
static_assert(sizeof(LogHeadEntry) == 8, "LogHeadEntry must be 64 bits");
static_assert(sizeof(LogBodyEntry) == 8, "LogBodyEntry must be 64 bits");

}  // namespace ulayfs::pmem
