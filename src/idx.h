#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <type_traits>

#include "const.h"

namespace ulayfs {

enum class IdxType { LOGICAL_BLOCK_IDX, VIRTUAL_BLOCK_IDX };

/**
 * Base class for index.
 * This enforce typechecking of different index type (e.g. LogicalBlockIdx,
 * VirtualBlockIdx)
 *
 * @tparam T underlying type of the index, e.g. uint32_t
 * @tparam D dummy paramters to enforce typecheck between different instance
 * types of BaseIdx
 */
template <typename T, IdxType D>
class BaseIdx {
  T idx;

 public:
  using numeric_type = T;

  BaseIdx(T i) : idx(i) {}
  BaseIdx& operator=(const T& i) {
    idx = i;
    return *this;
  };
  BaseIdx& operator=(T&& i) {
    idx = i;
    return *this;
  }

  BaseIdx() = default;
  BaseIdx(const BaseIdx& other) = default;
  BaseIdx(BaseIdx&& other) = default;
  BaseIdx& operator=(const BaseIdx&) = default;
  BaseIdx& operator=(BaseIdx&&) = default;

  BaseIdx operator+(T rhs) const { return idx + rhs; }
  BaseIdx operator-(T rhs) const { return idx - rhs; }
  T operator-(BaseIdx rhs) const { return idx - rhs.idx; }

  // fall back to the row type when dealing with bit-wise op
  T operator&(T rhs) const { return idx & rhs; }
  T operator|(T rhs) const { return idx | rhs; }
  T operator>>(T rhs) const { return idx >> rhs; }
  // explicitly delete left-shift to avoid overflow
  T operator<<(T rhs) const = delete;

  T& operator++() { return ++idx; }
  T& operator--() { return --idx; }
  T operator++(int) { return idx++; }
  T operator--(int) { return idx--; }
  T& operator+=(const T& rhs) { return idx += rhs; }
  T& operator-=(const T& rhs) { return idx -= rhs; }

  bool operator==(T rhs) const { return idx == rhs; }
  bool operator!=(T rhs) const { return idx != rhs; }
  bool operator<(T rhs) const { return idx < rhs; }
  bool operator>(T rhs) const { return idx > rhs; }
  bool operator<=(T rhs) const { return idx <= rhs; }
  bool operator>=(T rhs) const { return idx >= rhs; }

  bool operator==(BaseIdx rhs) const { return idx == rhs.idx; }
  bool operator!=(BaseIdx rhs) const { return idx != rhs.idx; }
  bool operator<(BaseIdx rhs) const { return idx < rhs.idx; }
  bool operator>(BaseIdx rhs) const { return idx > rhs.idx; }
  bool operator<=(BaseIdx rhs) const { return idx <= rhs.idx; }
  bool operator>=(BaseIdx rhs) const { return idx >= rhs.idx; }

  [[nodiscard]] T get() const { return idx; }

  friend std::ostream& operator<<(std::ostream& out, const BaseIdx& base_idx) {
    return out << base_idx.idx;
  }
};

// block index within a file; the meta block has a LogicalBlockIdx of 0;
// LogicalBlockIdx 0 can be used as "invalid block index" for non-meta block
using LogicalBlockIdx = BaseIdx<uint32_t, IdxType::LOGICAL_BLOCK_IDX>;

// block index seen by applications
using VirtualBlockIdx = BaseIdx<uint32_t, IdxType::VIRTUAL_BLOCK_IDX>;

// each bit in the bitmap corresponds to a logical block
using BitmapIdx = uint32_t;

// local index within a block
using TxLocalIdx = uint32_t;

// Note: LogLocalOffset will persist and the valid range is [0, 4096]
using LogLocalOffset = uint16_t;

static_assert(sizeof(LogicalBlockIdx) == LOGICAL_BLOCK_IDX_SIZE,
              "LogicalBlockIdx should be of size LOGICAL_BLOCK_IDX_SIZE");
static_assert(sizeof(VirtualBlockIdx) == VIRTUAL_BLOCK_IDX_SIZE,
              "VirtualBlockIdx should be of size VIRTUAL_BLOCK_IDX_SIZE");
static_assert(std::is_standard_layout<LogicalBlockIdx>::value,
              "LogicalBlockIdx must be a standard layout type");
static_assert(std::is_standard_layout<VirtualBlockIdx>::value,
              "VirtualBlockIdx must be a standard layout type");
static_assert(std::is_trivial<LogicalBlockIdx>::value,
              "LogicalBlockIdx must be a trivial type");
static_assert(std::is_trivial<VirtualBlockIdx>::value,
              "VirtualBlockIdx must be a trivial type");
static_assert(NUM_TX_ENTRY_PER_BLOCK - 1 <=
                  std::numeric_limits<TxLocalIdx>::max(),
              "NUM_TX_ENTRY - 1 should be representable with TxLocalIdx");
static_assert(
    NUM_BITMAP_PER_BLOCK - 1 <= std::numeric_limits<BitmapIdx>::max(),
    "NUM_BITMAP_PER_BLOCK - 1 should be representable with BitmapIdx");

// this ensures 32-bit idx won't overflow
#define BLOCK_IDX_TO_SIZE(idx) \
  (static_cast<uint64_t>((idx).get()) << BLOCK_SHIFT)
#define BLOCK_NUM_TO_SIZE(num) (static_cast<uint64_t>((num)) << BLOCK_SHIFT)
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
    out << "LogEntryIdx{" << idx.block_idx << "," << idx.local_offset << "}";
    return out;
  }
};

static_assert(sizeof(LogEntryIdx) == 8, "LogEntryIdx must be 8 bytes");
static_assert(std::is_standard_layout<LogEntryIdx>::value,
              "TxEntryIdx must be a standard layout type");
static_assert(std::is_trivial<LogEntryIdx>::value,
              "TxEntryIdx must be a trival type");

/**
 * A transaction entry is identified by the block index and the local index
 */
struct alignas(8) TxEntryIdx {
  LogicalBlockIdx block_idx;
  TxLocalIdx local_idx;

  TxEntryIdx() = default;
  TxEntryIdx(LogicalBlockIdx block_idx, TxLocalIdx local_idx)
      : block_idx(block_idx), local_idx(local_idx) {}

  [[nodiscard]] bool is_inline() const { return block_idx == 0; }
  [[nodiscard]] uint16_t get_capacity() const {
    return is_inline() ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY_PER_BLOCK;
  }

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
static_assert(std::is_standard_layout<TxEntryIdx>::value,
              "TxEntryIdx must be a standard layout type");
static_assert(std::is_trivial<TxEntryIdx>::value,
              "TxEntryIdx must be a trivial type");

}  // namespace ulayfs
