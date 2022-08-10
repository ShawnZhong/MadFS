#pragma once

#include <atomic>
#include <cassert>
#include <iostream>
#include <tuple>

#include "const.h"
#include "idx.h"
#include "persist.h"

namespace ulayfs::pmem {

// NOTE: assume for a linked list of LogEntry is sorted by their virtual index
// BlkTable uses some assumption to simply implementation
struct LogEntry {
  /*** define LogEntry-specific struct ***/
  enum class Op {
    LOG_INVALID = 0,
    // we start the enum from 1 so that a LogOp with value 0 is invalid
    LOG_OVERWRITE = 1,
  };

  /*** define actual LogEntry layout ***/
  // this log entry describles a mapping from virtual range [begin_vidx,
  // begin_vidx + num_blocks) to several logical blocks

  // op/leftover_bytes are read/written by external caller
  // has_next/is_next_same_block/next are read/written by allocator
  // num_blocks are read/written by both

  // the operation code, e.g., LOG_OVERWRITE.
  enum Op op : 2;

  // whether this log entry has a next entry (as a linked list).
  // `next` is only meaningful if this bit is set
  bool has_next : 1;

  // whether the next entry is in the same block.
  // if true, `next` shall be interpreted as `local_offset`.
  // otherwise, `next` shall be interpreted as `block_idx`.
  bool is_next_same_block : 1;

  // the leftover bytes in the last block that does not below to this file.
  // this happens in a block-unaligned append where the not all bytes in the
  // last block belongs to this file.
  // the maximum number of leftover bytes is BLOCK_SIZE - 1.
  uint16_t leftover_bytes : 12;

  // the number of blocks described in this log entry
  // every 64 blocks corresponds to one entry in begin_lidxs
  uint16_t num_blocks;

  union {
    LogicalBlockIdx block_idx;
    uint32_t local_offset : 12;
  } next;

  VirtualBlockIdx begin_vidx;
  LogicalBlockIdx begin_lidxs[];

  // this corresponds to the size of all fields except the varying
  // variable-length array `begin_lidxs`
  constexpr static uint32_t FIXED_SIZE = 12;

  /*** some helper functions ***/
  [[nodiscard]] constexpr uint32_t get_lidxs_len() const {
    return ALIGN_UP(static_cast<uint32_t>(num_blocks), BITMAP_BLOCK_CAPACITY) >>
           BITMAP_BLOCK_CAPACITY_SHIFT;
  }

  // every element in lidxs corresponds to a mapping of length 64 blocks except
  // the last one
  [[nodiscard]] constexpr uint32_t get_last_lidx_num_blocks() const {
    return num_blocks % BITMAP_BLOCK_CAPACITY;
  }

  void persist() {
    auto size = FIXED_SIZE + sizeof(LogicalBlockIdx) * get_lidxs_len();
    persist_unfenced(this, size);
  }
  // more advanced iteration helpers are in TxMgr, since they require MemTable

  friend std::ostream& operator<<(std::ostream& out, const LogEntry& entry) {
    out << "LogEntry{";
    out << "n_blk=" << entry.num_blocks << ", ";
    out << "vidx=" << entry.begin_vidx << ", ";
    out << "lidxs=[" << entry.begin_lidxs[0];
    for (uint32_t i = 1; i < entry.get_lidxs_len(); ++i)
      out << "," << entry.begin_lidxs[i];
    out << "]}";
    return out;
  }
};

static_assert(sizeof(LogEntry) == LogEntry::FIXED_SIZE,
              "LogEntry::FIXED_SIZE must match its actual size");

/**
 * Points to the head of a linked list of LogEntry
 */
struct __attribute__((packed)) TxEntryIndirect {
 private:
  friend union TxEntry;

 private:
  bool is_inline : 1 = false;

 public:
  uint32_t unused : 19 = 0;
  // we enforce this must be 12-bit to ensure corrupted TxEntry won't cause
  // buffer-overflow issues
  LogLocalOffset local_offset : 12 = 0;
  uint32_t block_idx;

  explicit TxEntryIndirect(LogEntryIdx log_entry_idx) {
    local_offset = log_entry_idx.local_offset;
    block_idx = log_entry_idx.block_idx.get();
  }

  LogEntryIdx get_log_entry_idx() { return {block_idx, local_offset}; }

  friend std::ostream& operator<<(std::ostream& out, const TxEntryIndirect& e) {
    out << "TxEntryIndirect{" << e.block_idx << "," << e.local_offset << "}";
    return out;
  }
};

static_assert(sizeof(TxEntryIndirect) == TX_ENTRY_SIZE,
              "TxEntryIndirect must be 64 bits");

struct __attribute__((packed)) TxEntryInline {
 private:
  constexpr static const int NUM_BLOCKS_BITS = 6;
  constexpr static const int BEGIN_VIRTUAL_IDX_BITS = 28;
  constexpr static const int BEGIN_LOGICAL_IDX_BITS = 29;

  constexpr static const int NUM_BLOCKS_MAX = (1 << NUM_BLOCKS_BITS) - 1;
  constexpr static const int BEGIN_VIRTUAL_IDX_MAX =
      (1 << BEGIN_VIRTUAL_IDX_BITS) - 1;
  constexpr static const int BEGIN_LOGICAL_IDX_MAX =
      (1 << BEGIN_LOGICAL_IDX_BITS) - 1;

  friend union TxEntry;

 private:
  bool is_inline : 1 = true;

 public:
  uint32_t num_blocks : NUM_BLOCKS_BITS;
  uint32_t begin_virtual_idx : BEGIN_VIRTUAL_IDX_BITS;
  uint32_t begin_logical_idx : BEGIN_LOGICAL_IDX_BITS;

  static bool can_inline(uint32_t num_blocks, VirtualBlockIdx begin_virtual_idx,
                         LogicalBlockIdx begin_logical_idx) {
    return num_blocks <= NUM_BLOCKS_MAX &&
           begin_virtual_idx <= BEGIN_VIRTUAL_IDX_MAX &&
           begin_logical_idx <= BEGIN_LOGICAL_IDX_MAX;
  }

  constexpr TxEntryInline()
      : num_blocks(0), begin_virtual_idx(0), begin_logical_idx(0) {}

  TxEntryInline(uint32_t num_blocks, VirtualBlockIdx begin_virtual_idx,
                LogicalBlockIdx begin_logical_idx)
      : num_blocks(num_blocks),
        begin_virtual_idx(begin_virtual_idx.get()),
        begin_logical_idx(begin_logical_idx.get()) {
    assert(can_inline(num_blocks, begin_virtual_idx, begin_logical_idx));
  }

  friend std::ostream& operator<<(std::ostream& out,
                                  const TxEntryInline& entry) {
    out << "TxEntryInline{";
    out << "n_blk=" << entry.num_blocks << ", ";
    out << "vidx=" << entry.begin_virtual_idx << ", ";
    out << "lidx=" << entry.begin_logical_idx;
    out << "}";
    return out;
  }
};

static_assert(sizeof(TxEntryInline) == TX_ENTRY_SIZE,
              "TxEntryInline must be 64 bits");

union TxEntry {
 public:
  uint64_t raw_bits;
  TxEntryIndirect indirect_entry;
  TxEntryInline inline_entry;
  struct {
    bool is_inline : 1;
    uint64_t payload : 63;
  } fields;

  constexpr static const pmem::TxEntryInline TxEntryDummy{};

  TxEntry(){};
  TxEntry(uint64_t raw_bits) : raw_bits(raw_bits) {}
  TxEntry(TxEntryIndirect indirect_entry) : indirect_entry(indirect_entry) {}
  TxEntry(TxEntryInline inline_entry) : inline_entry(inline_entry) {}

  [[nodiscard]] bool is_inline() const { return fields.is_inline; }

  [[nodiscard]] bool is_valid() const { return raw_bits != 0; }

  [[nodiscard]] bool is_dummy() const {
    return is_inline() && inline_entry.num_blocks == 0;
  };

  /**
   * find the tail (next unused slot) in an array of TxEntry
   *
   * @tparam NUM_ENTRIES the total number of entries in the array
   * @param entries a pointer to an array of tx entries
   * @param hint hint to start the search
   * @return the local index of next available TxEntry; NUM_ENTRIES if not found
   */
  template <uint16_t NUM_ENTRIES>
  static TxLocalIdx find_tail(const std::atomic<TxEntry> entries[],
                              TxLocalIdx hint = 0) {
    for (TxLocalIdx idx = hint; idx < NUM_ENTRIES; ++idx)
      if (!entries[idx].load(std::memory_order_acquire).is_valid()) return idx;
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
  static TxEntry try_append(std::atomic<TxEntry> entries[], TxEntry entry,
                            TxLocalIdx idx) {
    TxEntry expected = 0;
    entries[idx].compare_exchange_strong(
        expected, entry, std::memory_order_acq_rel, std::memory_order_acquire);
    // if CAS fails, `expected` will be stored the value in entries[idx]
    // if success, it will return 0
    return expected;
  }

  // if we are touching a new cacheline, we must flush everything before it
  // if only flush at fsync, always return false
  static bool need_flush(TxLocalIdx idx) {
    if constexpr (BuildOptions::tx_flush_only_fsync) return false;
    return IS_ALIGNED(sizeof(TxEntry) * idx, CACHELINE_SIZE);
  }

  friend std::ostream& operator<<(std::ostream& out, const TxEntry& tx_entry) {
    return tx_entry.is_inline() ? out << tx_entry.inline_entry
                                : out << tx_entry.indirect_entry;
  }
};

static_assert(sizeof(TxEntry) == TX_ENTRY_SIZE, "TxEntry must be 64 bits");
}  // namespace ulayfs::pmem
