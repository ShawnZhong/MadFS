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
using BitmapIdx = int32_t;
// TODO: this may be helpful for dynamically growing DRAM bitmap

// local index within a block; this can be -1 to indicate an error
using TxLocalIdx = int32_t;
// Note: LogLocalIdx will persist and the valid range is [0, 255]
using LogLocalOffset = uint32_t;

// this ensure 32-bit idx won't overflow
#define BLOCK_IDX_TO_SIZE(idx) (static_cast<uint64_t>(idx) << BLOCK_SHIFT)
// this is applied for some signed type
#define BLOCK_SIZE_TO_IDX(size) \
  (static_cast<uint32_t>((static_cast<uint64_t>(size) >> BLOCK_SHIFT)))

/**
 * A log entry is identified by the index of the LogEntryBlock and the local
 * offset within the block.
 */
struct LogEntryIdx {
  LogicalBlockIdx block_idx;
  LogLocalOffset local_offset;
  friend std::ostream& operator<<(std::ostream& out, const LogEntryIdx& idx) {
    out << "LogEntryIdx{" << idx.block_idx << "," << unsigned(idx.local_offset)
        << "}";
    return out;
  }
};

static_assert(sizeof(LogEntryIdx) == 8, "LogEntryIdx must be 8 bytes");

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

static_assert(sizeof(TxEntryIdx) == 8, "TxEntryIdx must be 64 bits");

}  // namespace ulayfs
