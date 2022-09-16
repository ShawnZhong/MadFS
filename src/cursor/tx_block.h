#pragma once

#include "alloc/alloc.h"
#include "block/tx.h"
#include "cursor/tx_entry.h"
#include "idx.h"
#include "mem_table.h"
#include "utils/timer.h"

namespace ulayfs::dram {

struct TxBlockCursor {
  LogicalBlockIdx idx;
  union {
    pmem::TxBlock* block;
    pmem::MetaBlock* meta;
    void* addr;
  };

  explicit TxBlockCursor(pmem::MetaBlock* meta) : idx(), meta(meta) {}

  explicit TxBlockCursor(TxCursor cursor)
      : idx(cursor.idx.block_idx), block(cursor.block) {}

  TxBlockCursor(LogicalBlockIdx idx, MemTable* mem_table)
      : idx(idx),
        addr(idx == LogicalBlockIdx::max() ? nullptr
                                           : mem_table->lidx_to_addr_rw(idx)) {}

  static TxBlockCursor max() { return {LogicalBlockIdx::max(), nullptr}; }

  /**
   * Advance to the first entry of the next tx block
   * @param mem_table used to find the memory address of the next block
   * @return true on success; false when reaches the end
   */
  bool advance_to_next_block(MemTable* mem_table) {
    assert(addr != nullptr);
    LogicalBlockIdx next_idx =
        idx == 0 ? meta->get_next_tx_block() : block->get_next_tx_block();
    if (next_idx == 0) return false;
    idx = next_idx;
    addr = mem_table->lidx_to_addr_rw(idx);
    return true;
  }

  /**
   * Advance to the next orphan tx block
   * @param mem_table used to find the memory address of the next block
   * @return true on success; false when reaches the end
   */
  bool advance_to_next_orphan(MemTable* mem_table) {
    assert(addr != nullptr);
    LogicalBlockIdx next_idx = idx == 0 ? meta->get_next_orphan_block()
                                        : block->get_next_orphan_block();
    if (next_idx == 0) return false;
    idx = next_idx;
    addr = mem_table->lidx_to_addr_rw(idx);
    return true;
  }

  void set_next_orphan_block(LogicalBlockIdx block_idx) const {
    assert(addr != nullptr);
    return idx == 0 ? meta->set_next_orphan_block(block_idx)
                    : block->set_next_orphan_block(block_idx);
  }

  [[nodiscard]] LogicalBlockIdx get_next_orphan_block() const {
    assert(addr != nullptr);
    return idx == 0 ? meta->get_next_orphan_block()
                    : block->get_next_orphan_block();
  }

  /**
   * NOTE: the ordering of two tx_blocks denotes "at some points, one block is
   * ahead of the other block on the linked list," and this is only determined
   * by their tx_seq. two tx blocks may never on the same linked list, which
   * makes such comparison meaningless. this function would return true/false
   * anyway, but it is the caller's responsibility to ensure such ordering is
   * meaningful.
   */
  friend bool operator<(const TxBlockCursor& lhs, const TxBlockCursor& rhs) {
    if (lhs.idx == rhs.idx) return false;
    if (lhs.idx == LogicalBlockIdx::max()) return false;
    if (rhs.idx == LogicalBlockIdx::max()) return true;
    if (lhs.idx == 0) return true;
    if (rhs.idx == 0) return false;
    return lhs.block->get_tx_seq() < rhs.block->get_tx_seq();
  }
};

static_assert(sizeof(TxBlockCursor) == 16);
}  // namespace ulayfs::dram
