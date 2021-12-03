#pragma once

#include <cstddef>
#include <ostream>
#include <vector>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "idx.h"
#include "log.h"
#include "mtable.h"
#include "utils.h"

namespace ulayfs::dram {

class TxMgr {
 private:
  File* file;
  pmem::MetaBlock* meta;
  MemTable* mem_table;

  class Tx;
  class AlignedTx;
  class CoWTx;
  class SingleBlockTx;
  class MultiBlockTx;

 public:
  TxMgr(File* file, pmem::MetaBlock* meta, MemTable* mem_table)
      : file(file), meta(meta), mem_table(mem_table) {}

  bool tx_idx_greater(TxEntryIdx lhs, TxEntryIdx rhs) {
    if (lhs.block_idx == rhs.block_idx) return lhs.local_idx > rhs.local_idx;
    if (lhs.block_idx == 0) return false;
    if (rhs.block_idx == 0) return true;
    return mem_table->get(lhs.block_idx)->tx_block.get_tx_seq() >
           mem_table->get(rhs.block_idx)->tx_block.get_tx_seq();
  }

  /**
   * Same arguments as pwrite
   */
  void do_write(const char* buf, size_t count, size_t offset);

  /**
   * Same arguments as pread
   */
  ssize_t do_read(char* buf, size_t count, size_t offset);

  /**
   * Move to the next transaction entry
   *
   * @param[in,out] tx_idx the current index, will be changed to the next index
   * @param[in,out] tx_block output parameter, change to the TxBlock
   * corresponding to the next idx
   * @param[in] do_alloc whether allocation is allowed when reaching the end of
   * a block
   *
   * @return true on success; false when reaches the end of a block and do_alloc
   * is false. The advance would happen anyway but in the case of false, it is
   * in a overflow state
   */
  [[nodiscard]] bool advance_tx_idx(TxEntryIdx& tx_idx,
                                    pmem::TxBlock*& tx_block,
                                    bool do_alloc) const {
    assert(tx_idx.local_idx >= 0);
    __atomic_fetch_add(&tx_idx.local_idx, 1, __ATOMIC_ACQ_REL);
    return handle_idx_overflow(tx_idx, tx_block, do_alloc);
  }

  /**
   * Read the entry from the MetaBlock or TxBlock
   */
  [[nodiscard]] pmem::TxEntry get_entry_from_block(
      TxEntryIdx idx, pmem::TxBlock* tx_block) const {
    const auto [block_idx, local_idx] = idx;
    assert(local_idx < (block_idx == 0 ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY));
    if (block_idx == 0) return meta->get_tx_entry(local_idx);
    return tx_block->get(local_idx);
  }

  /**
   * Try to commit an entry
   *
   * @param[in] entry entry to commit
   * @param[in,out] tx_idx idx of entry to commit; will be updated to the index
   * of success slot if cont_if_fail is set
   * @param[in,out] tx_block block pointer of the block by tx_idx
   * @param[in] cont_if_fail whether continue to the next tx entry if fail
   * @return empty entry on success; conflict entry otherwise
   */
  pmem::TxEntry try_commit(pmem::TxEntry entry, TxEntryIdx& tx_idx,
                           pmem::TxBlock*& tx_block, bool cont_if_fail);

  /**
   * @tparam B MetaBlock or TxBlock
   * @param block the block that needs a next block to be allocated
   * @return the block id of the allocated block
   */
  template <class B>
  LogicalBlockIdx alloc_next_block(B* block) const;

  /**
   * If the given idx is in an overflow state, update it if allowed.
   *
   * @param[in,out] tx_idx the transaction index to be handled, might be updated
   * @param[in,out] tx_block the block corresponding to the tx, might be updated
   * @param[in] do_alloc whether allocation is allowed
   * @return whether if it's in a non-overflow state now
   */
  bool handle_idx_overflow(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                           bool do_alloc) const {
    const bool is_inline = tx_idx.is_inline();
    uint16_t capacity = is_inline ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
    if (unlikely(tx_idx.local_idx >= capacity)) {
      LogicalBlockIdx block_idx =
          is_inline ? meta->get_next_tx_block() : tx_block->get_next_tx_block();
      if (block_idx == 0) {
        if (!do_alloc) return false;
        block_idx =
            is_inline ? alloc_next_block(meta) : alloc_next_block(tx_block);
      }
      tx_idx.block_idx = block_idx;
      tx_idx.local_idx -= capacity;
      tx_block = &mem_table->get(tx_idx.block_idx)->tx_block;
    }
    return true;
  }

  /**
   * Flush tx entries from tx_idx_begin to tx_idx_end
   * A typical use pattern is use meta->tx_tail as begin and the latest tail as
   * end. Thus, we usually don't know the block address that corresponds to
   * tx_idx_begin, but we know the block address that corresponds to tx_idx_end
   *
   * @param tx_idx_begin which tx entry to begin
   * @param tx_idx_end which tx entry to stop (non-inclusive)
   * @param tx_block_end if tx_idx_end is known, could optionally provide to
   * save one access to mem_table (this should be a common case)
   */
  void flush_tx_entries(TxEntryIdx tx_idx_begin, TxEntryIdx tx_idx_end,
                        pmem::TxBlock* tx_block_end = nullptr) {
    if (!tx_idx_greater(tx_idx_end, tx_idx_begin)) return;
    pmem::TxBlock* tx_block_begin;
    // handle special case of inline tx
    if (tx_idx_begin.block_idx == 0) {
      if (tx_idx_end.block_idx == 0) {
        meta->flush_tx_entries(tx_idx_begin.local_idx, tx_idx_end.local_idx);
        goto done;
      }
      meta->flush_tx_block(tx_idx_begin.local_idx);
      // now the next block is the "new begin"
      tx_idx_begin = {meta->get_next_tx_block(), 0};
    }
    while (tx_idx_begin.block_idx != tx_idx_end.block_idx) {
      tx_block_begin = &mem_table->get(tx_idx_begin.block_idx)->tx_block;
      tx_block_begin->flush_tx_block(tx_idx_begin.local_idx);
      tx_idx_begin = {tx_block_begin->get_next_tx_block(), 0};
      // special case: tx_idx_end is the first entry of the next block, which
      // means we only need to flush the current block and no need to
      // dereference to get the last block
    }
    if (tx_idx_begin.local_idx == tx_idx_end.local_idx) goto done;
    if (!tx_block_end)
      tx_block_end = &mem_table->get(tx_idx_end.block_idx)->tx_block;
    tx_block_end->flush_tx_entries(tx_idx_begin.local_idx,
                                   tx_idx_end.local_idx);

  done:
    _mm_sfence();
  }

 private:
  /**
   * Move along the linked list of TxBlock and find the tail. The returned
   * tail may not be up-to-date due to race conditon. No new blocks will be
   * allocated. If the end of TxBlock is reached, just return NUM_TX_ENTRY as
   * the TxLocalIdx.
   */
  void find_tail(TxEntryIdx& curr_idx, pmem::TxBlock*& curr_block) const;

  /**
   * Move to the real tx and update first/last_src_block to indicate whether to
   * redo
   *
   * @param[in] curr_entry the last entry returned by try_commit; this should be
   * what dereferenced from tail_tx_idx, and we only take it to avoid one more
   * dereference to some shared memory
   * @param[in] first_vidx the first block's virtual idx; ignored if !copy_first
   * @param[in] last_vidx the last block's virtual idx; ignored if !copy_last
   * @param[in,out] tail_tx_idx current tail index
   * @param[in,out] tail_tx_block current tail block
   * @param[in] is_range whether it is range conflict: if false, only handle the
   * conflict with the first/last; if true, any conflict in the range will be
   * handled
   * @param[out] redo_first whether redo the first; only valid if !is_range
   * @param[out] redo_last whether redo the lsat; only valid if !is_range
   * @param[out] first_lidx if redo_first, which logical block to copy from
   * @param[out] last_lidx if redo_last, which logical block to copy from
   * @param[out] redo_image if is_range and need redo, copy from the image
   * @return true if needs redo; false otherwise
   */
  bool handle_conflict(pmem::TxEntry curr_entry, VirtualBlockIdx first_vidx,
                       VirtualBlockIdx last_vidx, TxEntryIdx& tail_tx_idx,
                       pmem::TxBlock*& tail_tx_block, bool is_range,
                       bool* redo_first, bool* redo_last,
                       LogicalBlockIdx* first_lidx, LogicalBlockIdx* last_lidx,
                       LogicalBlockIdx redo_image[]);

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

/**
 * Tx is an inner class of TxMgr that represents a single transaction
 */
class TxMgr::Tx {
 public:
  Tx(File* file, const char* buf, size_t count, size_t offset);

 protected:
  // pointer to the outer class
  File* file;
  TxMgr* tx_mgr;
  LogMgr* log_mgr;
  Allocator* allocator;

  /*
   * Input (read-only) properties
   */
  const char* const buf;
  const size_t count;
  const size_t offset;

  /*
   * Derived (read-only) properties
   */

  // the byte range to be written is [offset, end_offset), and the byte at
  // end_offset is NOT included
  const size_t end_offset;

  // the index of the virtual block that contains the beginning offset
  const VirtualBlockIdx begin_vidx;
  // the block index to be written is [begin_vidx, end_vidx), and the block with
  // index end_vidx is NOT included
  const VirtualBlockIdx end_vidx;

  // total number of blocks
  const size_t num_blocks;

  // the logical index of the destination data block
  const LogicalBlockIdx dst_idx;
  // the pointer to the destination data block
  pmem::Block* const dst_blocks;

  // the index of the first LogHeadEntry, can be used to locate the whole
  // group of log entries for this transaction
  LogEntryIdx log_idx;

  /*
   * Mutable states
   */

  // the index of the current transaction tail
  TxEntryIdx tail_tx_idx;
  // the log block corresponding to the transaction
  pmem::TxBlock* tail_tx_block;
};

class TxMgr::AlignedTx : public TxMgr::Tx {
 public:
  AlignedTx(File* file, const char* buf, size_t count, size_t offset);
  void do_write();
};

class TxMgr::CoWTx : public TxMgr::Tx {
 protected:
  CoWTx(File* file, const char* buf, size_t count, size_t offset);

  // the tx entry to be committed
  const pmem::TxCommitEntry entry;

  /*
   * Read-only properties
   */

  // the index of the first virtual block that needs to be copied entirely
  const VirtualBlockIdx begin_full_vidx;

  // the index of the last virtual block that needs to be copied entirely
  const VirtualBlockIdx end_full_vidx;

  // full blocks are blocks that can be written from buf directly without
  // copying the src data
  size_t num_full_blocks;

  /*
   * Mutable states
   */

  // whether copy the first block
  bool copy_first;
  // whether copy the last block
  bool copy_last;

  // if copy_first, which logical block to copy from
  LogicalBlockIdx src_first_lidx;
  // if copy_last, which logical block to copy from
  LogicalBlockIdx src_last_lidx;
};

class TxMgr::SingleBlockTx : public TxMgr::CoWTx {
 public:
  SingleBlockTx(File* file, const char* buf, size_t count, size_t offset);
  void do_write();

 private:
  // the starting offset within the block
  const size_t local_offset;
};

class TxMgr::MultiBlockTx : public TxMgr::CoWTx {
 public:
  MultiBlockTx(File* file, const char* buf, size_t count, size_t offset);
  void do_write();

 private:
  // number of bytes to be written in the beginning.
  // If the offset is 4097, then this var should be 4095.
  const size_t first_block_local_offset;

  // number of bytes to be written for the last block
  // If the end_offset is 4097, then this var should be 1.
  const size_t last_block_local_offset;
};
}  // namespace ulayfs::dram
