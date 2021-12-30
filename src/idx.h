#pragma once

#include <cassert>
#include <cstdint>
#include <iostream>

namespace ulayfs {

// block index within a file; the meta block has a LogicalBlockIdx of 0
using LogicalBlockIdx = uint32_t;
// block index seen by applications
using VirtualBlockIdx = uint32_t;
// each bit in the bitmap corresponds to a logical block
using BitmapLocalIdx = int32_t;
// TODO: this may be helpful for dynamically growing DRAM bitmap
// using BitmapBlockId = uint32_t;

// local index within a block; this can be -1 to indicate an error
using TxLocalIdx = int16_t;
// Note: LogLocalIdx will persist and the valid range is [0, 255]
using LogLocalIdx = uint16_t;
// TODO: this will no longer be needed when we deprecate the head-body design
// for 8-byte aligned positions, the valid range is [0, 511]
using LogLocalUnpackIdx = uint16_t;

// this ensure 32-bit idx won't overflow
#define BLOCK_IDX_TO_SIZE(idx) (static_cast<uint64_t>(idx) << BLOCK_SHIFT)
// this is applied for some signed type
#define BLOCK_SIZE_TO_IDX(size) \
  (static_cast<uint32_t>((static_cast<uint64_t>(size) >> BLOCK_SHIFT)))

/**
 * A log entry is identified by the index of the LogEntryBlock and the local
 * index within the block. Use this to locate log head entries only, since
 * body entries may be on 8-byte boundaries
 *
 * 5 bytes (40 bits) in size
 */
struct __attribute__((packed)) LogEntryIdx {
  LogicalBlockIdx block_idx;
  LogLocalIdx local_idx : 8;

  friend std::ostream& operator<<(std::ostream& out, const LogEntryIdx& idx) {
    out << "LogEntryIdx{" << idx.block_idx << "," << unsigned(idx.local_idx)
        << "}";
    return out;
  }
};

// TODO: this will no longer be needed when we deprecate the head-body design
/**
 * An unpacked log entry index points to 8-byte aligned positions, so can be
 * used to locate both head entries and body entries. Used by the log manager
 */
struct __attribute__((packed)) LogEntryUnpackIdx {
  LogicalBlockIdx block_idx;
  LogLocalUnpackIdx local_idx;

  static LogEntryUnpackIdx from_pack_idx(LogEntryIdx idx) {
    auto local_idx = LogLocalUnpackIdx(idx.local_idx << 1);
    return LogEntryUnpackIdx{idx.block_idx, local_idx};
  }

  static LogEntryIdx to_pack_idx(LogEntryUnpackIdx idx) {
    assert(idx.local_idx % 2 == 0);
    auto local_idx = LogLocalIdx(idx.local_idx >> 1);
    return LogEntryIdx{idx.block_idx, local_idx};
  }

  friend std::ostream& operator<<(std::ostream& out,
                                  const LogEntryUnpackIdx& idx) {
    out << "LogEntryUnpackIdx{" << idx.block_idx << ","
        << unsigned(idx.local_idx) << "}";
    return out;
  }
};

static_assert(sizeof(LogEntryIdx) == 5, "LogEntryIdx must be 40 bits");
static_assert(sizeof(LogEntryUnpackIdx) == 6,
              "LogEntryUnpackIdx must be 48 bits");

/**
 * A transaction entry is identified by the block index and the local index
 */
struct TxEntryIdx {
  LogicalBlockIdx block_idx;
  TxLocalIdx local_idx;

  [[nodiscard]] bool is_inline() const { return block_idx == 0; }

  bool operator==(const TxEntryIdx& rhs) const {
    return block_idx == rhs.block_idx && local_idx == rhs.local_idx;
  }
  bool operator!=(const TxEntryIdx& rhs) const { return !(rhs == *this); }

  friend std::ostream& operator<<(std::ostream& out, const TxEntryIdx& idx) {
    out << "TxEntryIdx{" << idx.block_idx << "," << idx.local_idx << "}";
    return out;
  }
};

/**
 * TxEntryIdx padded to 64-bit to support atomic operations
 */
union TxEntryIdx64 {
  TxEntryIdx tx_entry_idx;
  std::uint64_t raw_bits;

  TxEntryIdx64(TxEntryIdx tx_entry_idx) : tx_entry_idx(tx_entry_idx) {}
};

static_assert(sizeof(TxEntryIdx) == 8, "TxEntryIdx must be 64 bits");

}  // namespace ulayfs
