#pragma once

#include <cstdint>
#include <iostream>

namespace ulayfs {

// block index within a file; the meta block has a LogicalBlockIdx of 0
using LogicalBlockIdx = uint32_t;
// block index seen by applications
using VirtualBlockIdx = uint32_t;

// local index within a block; this can be -1 to indicate an error
using BitmapLocalIdx = int16_t;
using TxLocalIdx = int16_t;
// Note: LogLocalIdx will persist and the valid range is [0, 255]
using LogLocalIdx = uint8_t;
// TODO: this will no longer be needed when we deprecate the head-body design
// for 8-byte aligned positions, the valid range is [0, 511]
using LogLocalUnpackIdx = uint16_t;

// identifier of bitmap blocks; checkout BitmapBlock's doc to see more
using BitmapBlockId = uint32_t;

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
};

static_assert(sizeof(LogEntryIdx) == 5, "LogEntryIdx must be 40 bits");
static_assert(sizeof(LogEntryUnpackIdx) == 6, "LogEntryUnpackIdx must be 48 bits");

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

  friend std::ostream& operator<<(std::ostream& out, const TxEntryIdx& idx) {
    out << "TxEntryIdx{" << idx.block_idx << "," << idx.local_idx << "}";
    return out;
  }
};

static_assert(sizeof(TxEntryIdx) == 8, "TxEntryIdx must be 64 bits");

}  // namespace ulayfs
