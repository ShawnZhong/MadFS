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
   * @param tx_idx the current index, will be changed to the next index
   * @param tx_block output parameter, change to the TxLogBlock
   * corresponding to the next idx
   * @return bool if advance succeed; if reach the end of a block, it may return
   * false
   */
  bool advance_tx_idx(pmem::TxEntryIdx& tx_idx, pmem::TxLogBlock*& tx_block,
                      bool do_alloc = false) const {
    assert(tx_idx.local_idx >= 0);

    // next index is within the same block, just increment local index
    uint16_t capacity =
        tx_idx.block_idx == 0 ? NUM_INLINE_TX_ENTRY : NUM_TX_ENTRY;
    if (tx_idx.local_idx < capacity - 1) {
      tx_idx.local_idx++;
      return true;
    }

    if (tx_idx.block_idx == 0) {
      tx_idx.block_idx = meta->get_next_tx_block();
      if (tx_idx.block_idx == 0) {
        if (do_alloc)
          tx_idx.block_idx = alloc_next_block(meta);
        else
          return false;
      }
    } else {
      tx_idx.block_idx = tx_block->get_next_tx_block();
      if (tx_idx.block_idx == 0) {
        if (do_alloc)
          tx_idx.block_idx = alloc_next_block(tx_block);
        else
          return false;
      }
    }
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
   * @param entry entry to commit
   * @param tx_idx idx of entry to commit; will be updated to the index of
   * success slot if cont_if_fail is set
   * @param tx_block block pointer of the block by tx_idx
   * @param cont_if_fail whether continue to the next tx entry if fail
   * @return uint64_t 0 if success; raw bits of conflict entry otherwise
   */
  uint64_t try_commit(pmem::TxEntry entry, pmem::TxEntryIdx& tx_idx,
                      pmem::TxLogBlock*& tx_block, bool cont_if_fail);

  /**
   * Commit a transaction
   *
   * @param tx_begin_idx the index of the corresponding begin transaction
   * @param log_entry_idx the first log entry that corresponds to the tx
   * @return the index of the committed transaction
   */
  pmem::TxEntryIdx commit_tx(pmem::TxEntryIdx tx_begin_idx,
                             pmem::LogEntryIdx log_entry_idx);

  /**
   * Same argurments as pwrite
   */
  void do_cow(const void* buf, size_t count, size_t offset);

 private:
  /**
   * Move to the real tx and update first/last_src_block to indicate whether to
   * redo
   *
   * @param curr_entry the last entry returned by try_commit; this should be
   * what dereferenced from tail_tx_idx, and we only take it to avoid one more
   * dereference to some shared memory
   * @param tail_tx_idx the index to the tail of tx (probably out-of-date)
   * @param tail_tx_block the corresponding tx block
   * @param copy_first whether to copy the first block (will be updated)
   * @param copy_last whether to copy the last block (will be updated)
   * @param first_vidx the first block's virtual index; ignored if !copy_first
   * @param last_vidx the last block's virtual index; ignored if !copy_last
   * @param first_src_block updated if the copy of first block need to redo
   * @param last_src_block updated if the copy of last block need to redo
   * @return true need redo
   * @return false need not redo
   */
  bool handle_conflict(pmem::TxEntry curr_entry, pmem::TxEntryIdx& tail_tx_idx,
                       pmem::TxLogBlock*& tail_tx_block, bool& copy_first,
                       bool& copy_last, VirtualBlockIdx first_vidx,
                       VirtualBlockIdx last_vidx, pmem::Block*& first_src_block,
                       pmem::Block*& last_src_block);

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
}  // namespace ulayfs::dram
