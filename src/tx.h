#pragma once

#include <cstddef>
#include <ostream>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "idx.h"
#include "log.h"
#include "mtable.h"

namespace ulayfs::dram {

// forward declaration
class BlkTable;

class TxMgr {
 private:
  pmem::MetaBlock* meta;
  Allocator* allocator;
  MemTable* mem_table;
  LogMgr* log_mgr;
  BlkTable* blk_table;

  class Tx;

 public:
  TxMgr() = default;
  TxMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table,
        LogMgr* log_mgr, BlkTable* blk_table)
      : meta(meta),
        allocator(allocator),
        mem_table(mem_table),
        log_mgr(log_mgr),
        blk_table(blk_table) {}

  /**
   * Move to the next transaction entry
   *
   * @param[in,out] tx_idx the current index, will be changed to the next index
   * @param[in,out] tx_block output parameter, change to the TxLogBlock
   * corresponding to the next idx
   * @return bool if advance succeed; if reach the end of a block, it may return
   * false
   */
  bool advance_tx_idx(pmem::TxEntryIdx& tx_idx, pmem::TxLogBlock*& tx_block,
                      bool do_alloc = false) const {
    assert(tx_idx.local_idx >= 0);

    bool is_inline = tx_idx.block_idx == 0;

    // next index is within the same block, just increment local index
    uint16_t capacity = is_inline ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
    if (tx_idx.local_idx < capacity - 1) {
      tx_idx.local_idx++;
      return true;
    }

    // get the index of the next block
    LogicalBlockIdx block_idx =
        is_inline ? meta->get_next_tx_block() : tx_block->get_next_tx_block();

    // check if the next index is valid; allocate the next block if allowed
    if (block_idx == 0) {
      if (!do_alloc) return false;
      block_idx =
          is_inline ? alloc_next_block(meta) : alloc_next_block(tx_block);
    }

    tx_idx.block_idx = block_idx;
    tx_idx.local_idx = 0;
    tx_block = &mem_table->get_addr(tx_idx.block_idx)->tx_log_block;
    return true;
  }

  /**
   * @return the current transaction entry
   */
  [[nodiscard]] pmem::TxEntry get_entry(pmem::TxEntryIdx tx_idx,
                                        pmem::TxLogBlock* tx_block) const {
    return get_entry_from_block(tx_idx, tx_block);
  }

  /**
   * Try to commit an entry
   *
   * @param[in] entry entry to commit
   * @param[in,out] tx_idx idx of entry to commit; will be updated to the index
   * of success slot if cont_if_fail is set
   * @param[in,out] tx_block block pointer of the block by tx_idx
   * @param[in] cont_if_fail whether continue to the next tx entry if fail
   * @return empty entry on success; raw bits of conflict entry otherwise
   */
  pmem::TxEntry try_commit(pmem::TxEntry entry, pmem::TxEntryIdx& tx_idx,
                           pmem::TxLogBlock*& tx_block, bool cont_if_fail);

  /**
   * Same argurments as pwrite
   */
  void do_cow(const void* buf, size_t count, size_t offset);

 private:
  /**
   * Read the entry from the MetaBlock or TxLogBlock
   */
  [[nodiscard]] pmem::TxEntry get_entry_from_block(
      pmem::TxEntryIdx idx, pmem::TxLogBlock* tx_log_block) const {
    const auto [block_idx, local_idx] = idx;
    if (block_idx == 0) return meta->get_tx_entry(local_idx);
    return tx_log_block->get(local_idx);
  }

  [[nodiscard]] pmem::LogEntry get_log_entry_from_commit(
      pmem::TxCommitEntry commit_entry) const {
    pmem::LogEntryBlock* log_block =
        &mem_table->get_addr(commit_entry.log_entry_idx.block_idx)
             ->log_entry_block;
    return log_block->get(commit_entry.log_entry_idx.local_idx);
  }

  /**
   * @tparam B MetaBlock or TxLogBlock
   * @param block the block that needs a next block to be allocated
   * @return the block id of the allocated block
   */
  template <class B>
  LogicalBlockIdx alloc_next_block(B* block) const;

  /**
   * Move along the linked list of TxLogBlock and find the tail. The returned
   * tail may not be up-to-date due to race conditon. No new blocks will be
   * allocated. If the end of TxLogBlock is reached, just return NUM_TX_ENTRY as
   * the TxLocalIdx.
   */
  void find_tail(pmem::TxEntryIdx& curr_idx,
                 pmem::TxLogBlock*& curr_block) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

/**
 * Tx is an inner class of TxMgr that represents a single transaction
 */
class TxMgr::Tx {
 public:
  Tx(TxMgr* tx_mgr, const void* buf, size_t count, size_t offset);
  void do_cow();

 private:
  void do_cow_aligned() const;
  void do_cow_single_block();
  void do_cow_multiple_blocks();

  /**
   * Move to the real tx and update first/last_src_block to indicate whether to
   * redo
   *
   * @param[in] curr_entry the last entry returned by try_commit; this should be
   * what dereferenced from tail_tx_idx, and we only take it to avoid one more
   * dereference to some shared memory
   *
   * @param[in,out] tail_tx_idx the idx to the tx tail (probably out-of-date)
   * @param[in,out] tail_tx_block the corresponding tx block
   *
   * @param[in,out] copy_first whether to copy the first block (will be updated)
   * @param[in,out] copy_last whether to copy the last block (will be updated)
   *
   * @param[in] first_vidx the first block's virtual idx; ignored if !copy_first
   * @param[in] last_vidx the last block's virtual idx; ignored if !copy_last
   *
   * @param[out] first_src_block updated if need to redo copy of first block
   * @param[out] last_src_block updated if need to redo copy of last block
   *
   *
   * @return true if needs redo; false otherwise
   */
  bool handle_conflict(pmem::TxEntry curr_entry, pmem::TxEntryIdx& tail_tx_idx,
                       pmem::TxLogBlock*& tail_tx_block, bool& copy_first,
                       bool& copy_last, VirtualBlockIdx first_vidx,
                       VirtualBlockIdx last_vidx, pmem::Block*& first_src_block,
                       pmem::Block*& last_src_block);

  bool handle_conflict(pmem::TxEntry curr_entry, pmem::TxEntryIdx& tail_tx_idx,
                       pmem::TxLogBlock*& tail_tx_block, bool& copy,
                       VirtualBlockIdx vidx, pmem::Block*& src_block);

 private:
  // pointer to the outer class
  TxMgr* tx_mgr;

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

  // the index of the current log entry
  const pmem::LogEntryIdx log_idx;
};
}  // namespace ulayfs::dram
