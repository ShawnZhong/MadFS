#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <unordered_set>
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

  class Tx;
  class ReadTx;
  class WriteTx;
  class AlignedTx;
  class CoWTx;
  class SingleBlockTx;
  class MultiBlockTx;

 public:
  TxMgr(File* file, pmem::MetaBlock* meta) : file(file), meta(meta) {}

  ssize_t do_pread(char* buf, size_t count, size_t offset);
  ssize_t do_read(char* buf, size_t count);

  ssize_t do_pwrite(const char* buf, size_t count, size_t offset);
  ssize_t do_write(const char* buf, size_t count);

  bool tx_idx_greater(const TxEntryIdx lhs_idx, const TxEntryIdx rhs_idx,
                      const pmem::TxBlock* lhs_block = nullptr,
                      const pmem::TxBlock* rhs_block = nullptr);

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
  bool advance_tx_idx(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                      bool do_alloc) const {
    assert(tx_idx.local_idx >= 0);
    tx_idx.local_idx++;
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
   * @param[in,out] tx_idx idx of entry to commit
   * @param[in,out] tx_block block pointer of the block by tx_idx
   * @return empty entry on success; conflict entry otherwise
   */
  pmem::TxEntry try_commit(pmem::TxEntry entry, TxEntryIdx& tx_idx,
                           pmem::TxBlock*& tx_block);

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
   * @return true if the idx is not in overflow state; false otherwise
   */
  bool handle_idx_overflow(TxEntryIdx& tx_idx, pmem::TxBlock*& tx_block,
                           bool do_alloc) const;

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
                        pmem::TxBlock* tx_block_end = nullptr);

  /**
   * Garbage collecting transaction blocks and log blocks. This function builds
   * a new transaction history from block table and uses it to replace the old
   * transaction history. We assume that a dedicated single-threaded process
   * will run this function so it is safe to directly access blk_table. Note
   * that old tx blocks and log blocks are not immediately recycled but when
   * rebuilding the bitmap
   *
   * @param tail_tx_block the tail transaction block index: this and following
   * transaction blocks will be appended to the new transaction history and will
   * not be touched
   * @param file_size size of this file
   */
  void gc(const LogicalBlockIdx tail_tx_block, uint64_t file_size);

 private:
  /**
   * Move along the linked list of TxBlock and find the tail. The returned
   * tail may not be up-to-date due to race conditon. No new blocks will be
   * allocated. If the end of TxBlock is reached, just return NUM_TX_ENTRY as
   * the TxLocalIdx.
   */
  void find_tail(TxEntryIdx& curr_idx, pmem::TxBlock*& curr_block) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

/**
 * Tx is an inner class of TxMgr that represents a single transaction
 */
class TxMgr::Tx {
 protected:
  Tx(File* file, TxMgr* tx_mgr, size_t count, size_t offset);
  friend TxMgr;

  /**
   * Move to the real tx and update first/last_src_block to indicate whether to
   * redo
   *
   * @param[in] curr_entry the last entry returned by try_commit; this should be
   * what dereferenced from tail_tx_idx, and we only take it to avoid one more
   * dereference to some shared memory
   * @param[in] first_vidx the first block's virtual idx; ignored if !copy_first
   * @param[in] last_vidx the last block's virtual idx; ignored if !copy_last
   * @param[out] conflict_image a list of lidx that conflict with the current tx
   * @return true if there exits conflict and requires redo
   */
  bool handle_conflict(pmem::TxEntry curr_entry, VirtualBlockIdx first_vidx,
                       VirtualBlockIdx last_vidx,
                       LogicalBlockIdx conflict_image[]);

  /**
   * Check if [first_vidx, last_vidx] has any overlap with [le_first_vidx,
   * le_first_vidx + num_blocks - 1]; populate overlapped mapping if any
   * "first/last" here means inclusive range "[first, last]"; "begin/end" means
   * the range excluding the end "[begin, end)"
   *
   * @param[in] first_vidx the virtual index range (first) to check
   * @param[in] last_vidx the virtual index range (last) to check
   * @param[in] le_first_vidx the virtual range begin in log entry
   * @param[in] le_begin_lidx the logical range begin in log entry
   * @param[in] num_blocks number of blocks in the log entry mapping
   * @param[out] conflict_image return overlapped mapping; if no overlapping,
   * the corresponding array element is guaranteed untouched
   * @return whether this is any overlap
   */
  bool get_conflict_image(VirtualBlockIdx first_vidx, VirtualBlockIdx last_vidx,
                          VirtualBlockIdx le_first_vidx,
                          LogicalBlockIdx le_begin_lidx, uint32_t num_blocks,
                          LogicalBlockIdx conflict_image[]) {
    VirtualBlockIdx le_last_vidx = le_first_vidx + num_blocks - 1;
    if (last_vidx < le_first_vidx || first_vidx > le_last_vidx) return false;

    VirtualBlockIdx overlap_first_vidx = std::max(le_first_vidx, first_vidx);
    VirtualBlockIdx overlap_last_vidx = std::min(le_last_vidx, last_vidx);

    for (VirtualBlockIdx vidx = overlap_first_vidx; vidx <= overlap_last_vidx;
         ++vidx) {
      auto offset = vidx - first_vidx;
      conflict_image[offset] = le_begin_lidx + offset;
    }
    return true;
  }

  // pointer to the outer class
  File* file;
  TxMgr* tx_mgr;
  LogMgr* log_mgr;

  /*
   * Input properties
   * In the case of partial read/write, count will be changed, so does end_*
   */
  size_t count;
  const size_t offset;

  /*
   * Derived properties
   */

  // the byte range to be written is [offset, end_offset), and the byte at
  // end_offset is NOT included
  size_t end_offset;

  // the index of the virtual block that contains the beginning offset
  const VirtualBlockIdx begin_vidx;
  // the block index to be written is [begin_vidx, end_vidx), and the block with
  // index end_vidx is NOT included
  VirtualBlockIdx end_vidx;

  // total number of blocks
  const size_t num_blocks;

  // in the case of read/write with offset change, update is done first
  bool is_offset_depend;
  uint64_t ticket;

  /*
   * Mutable states
   */
  // the index of the current transaction tail
  TxEntryIdx tail_tx_idx;
  // the log block corresponding to the transaction
  pmem::TxBlock* tail_tx_block;
};

class TxMgr::ReadTx : public TxMgr::Tx {
 protected:
  ReadTx(File* file, TxMgr* tx_mgr, char* buf, size_t count, size_t offset)
      : Tx(file, tx_mgr, count, offset), buf(buf) {}
  ReadTx(File* file, TxMgr* tx_mgr, char* buf, size_t count, size_t offset,
         TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block,
         uint64_t file_size, uint64_t ticket)
      : ReadTx(file, tx_mgr, buf, count, offset) {
    is_offset_depend = true;
    this->tail_tx_idx = tail_tx_idx;
    this->tail_tx_block = tail_tx_block;
    this->file_size = file_size;
    this->ticket = ticket;
  }
  ssize_t do_read();

  friend TxMgr;

  /*
   * read-specific arguments
   */
  char* const buf;
  uint64_t file_size;
};

class TxMgr::WriteTx : public TxMgr::Tx {
 protected:
  WriteTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
          size_t offset);
  WriteTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
          size_t offset, TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block,
          uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset) {
    is_offset_depend = true;
    this->tail_tx_idx = tail_tx_idx;
    this->tail_tx_block = tail_tx_block;
    this->ticket = ticket;
  }
  ssize_t do_write();

  template <typename TX>
  static ssize_t do_write_and_validate_offset(
      File* file, TxMgr* tx_mgr, const char* buf, size_t count, size_t offset,
      TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block, uint64_t ticket);

  friend TxMgr;

  /*
   * write-specific arguments
   */
  const char* const buf;

  Allocator* allocator;

  // the logical index of the destination data block
  std::vector<LogicalBlockIdx> dst_lidxs;
  // the pointer to the destination data block
  std::vector<pmem::Block*> dst_blocks;

  // the tx entry to be committed (may or may not inline)
  pmem::TxEntry commit_entry;
};

class TxMgr::AlignedTx : public TxMgr::WriteTx {
 public:
  AlignedTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
            size_t offset)
      : WriteTx(file, tx_mgr, buf, count, offset) {}
  AlignedTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
            size_t offset, TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block,
            uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset, tail_tx_idx, tail_tx_block,
                ticket) {}
  ssize_t do_write();
};

class TxMgr::CoWTx : public TxMgr::WriteTx {
 protected:
  CoWTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count, size_t offset)
      : WriteTx(file, tx_mgr, buf, count, offset),
        begin_full_vidx(BLOCK_SIZE_TO_IDX(ALIGN_UP(offset, BLOCK_SIZE))),
        end_full_vidx(BLOCK_SIZE_TO_IDX(end_offset)),
        num_full_blocks(end_full_vidx - begin_full_vidx) {}
  CoWTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count, size_t offset,
        TxEntryIdx tail_tx_idx, pmem::TxBlock* tail_tx_block, uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset, tail_tx_idx, tail_tx_block,
                ticket),
        begin_full_vidx(BLOCK_SIZE_TO_IDX(ALIGN_UP(offset, BLOCK_SIZE))),
        end_full_vidx(BLOCK_SIZE_TO_IDX(end_offset)),
        num_full_blocks(end_full_vidx - begin_full_vidx) {}

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
};

class TxMgr::SingleBlockTx : public TxMgr::CoWTx {
 public:
  SingleBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
                size_t offset)
      : CoWTx(file, tx_mgr, buf, count, offset),
        local_offset(offset - BLOCK_IDX_TO_SIZE(begin_vidx)) {
    assert(num_blocks == 1);
  }
  SingleBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
                size_t offset, TxEntryIdx tail_tx_idx,
                pmem::TxBlock* tail_tx_block, uint64_t ticket)
      : CoWTx(file, tx_mgr, buf, count, offset, tail_tx_idx, tail_tx_block,
              ticket),
        local_offset(offset - BLOCK_IDX_TO_SIZE(begin_vidx)) {
    assert(num_blocks == 1);
  }
  ssize_t do_write();

 private:
  // the starting offset within the block
  const size_t local_offset;
};

class TxMgr::MultiBlockTx : public TxMgr::CoWTx {
 public:
  MultiBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
               size_t offset)
      : CoWTx(file, tx_mgr, buf, count, offset),
        first_block_local_offset(ALIGN_UP(offset, BLOCK_SIZE) - offset),
        last_block_local_offset(end_offset -
                                ALIGN_DOWN(end_offset, BLOCK_SIZE)) {}
  MultiBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
               size_t offset, TxEntryIdx tail_tx_idx,
               pmem::TxBlock* tail_tx_block, uint64_t ticket)
      : CoWTx(file, tx_mgr, buf, count, offset, tail_tx_idx, tail_tx_block,
              ticket),
        first_block_local_offset(ALIGN_UP(offset, BLOCK_SIZE) - offset),
        last_block_local_offset(end_offset -
                                ALIGN_DOWN(end_offset, BLOCK_SIZE)) {}
  ssize_t do_write();

 private:
  // number of bytes to be written in the beginning.
  // If the offset is 4097, then this var should be 4095.
  const size_t first_block_local_offset;

  // number of bytes to be written for the last block
  // If the end_offset is 4097, then this var should be 1.
  const size_t last_block_local_offset;
};
}  // namespace ulayfs::dram
