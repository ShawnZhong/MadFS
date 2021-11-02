#pragma once

#include <ostream>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "mtable.h"

namespace ulayfs::dram {

class TxMgr {
 private:
  pmem::MetaBlock* meta;
  Allocator* allocator;
  MemTable* mem_table;

  // NOTE: the local tail indicates the tail of **APPLIED** entry
  //       the tail should not move except by blk_table
  //       also, the tail should be defined as **the next to check**
  // the tail of the local tx entry
  pmem::TxEntryIdx local_tx_tail;
  // the block for the local_tx_tail (a cached copy without access mem_table)
  pmem::TxLogBlock* local_tx_tail_block;

 public:
  TxMgr() = default;
  TxMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table)
      : meta(meta),
        allocator(allocator),
        mem_table(mem_table),
        local_tx_tail(),
        local_tx_tail_block(nullptr) {}

  /**
   * Move to the next tx index
   */
  void advance() { advance_tx_idx(local_tx_tail, local_tx_tail_block); }

  /**
   * @return the current transaction entry
   */
  [[nodiscard]] pmem::TxEntry get_entry() const {
    return get_entry_from_block(local_tx_tail, local_tx_tail_block);
  }

  /**
   * Begin a transaction that affects the range of blocks
   * [begin_virtual_idx, begin_virtual_idx + num_blocks)
   */
  pmem::TxEntryIdx begin_tx(VirtualBlockIdx begin_virtual_idx,
                            uint32_t num_blocks);

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
   * @param buf the buffer given by the user
   * @param count number of bytes in the buffer
   * @param local_offset the start offset within the first block
   * @param begin_dst_idx the index of the first destination block
   * @param begin_src_idx the index of the first source block, used when
   * local_offset is not 0, since we need to copy the data before the offset
   * to destination
   */
  void copy_data(const void* buf, size_t count, uint64_t local_offset,
                 LogicalBlockIdx& begin_dst_idx,
                 LogicalBlockIdx& begin_src_idx);

 private:
  /**
   * Read the entry from the MetaBlock or TxLogBlock
   */
  pmem::TxEntry get_entry_from_block(pmem::TxEntryIdx idx,
                                     pmem::TxLogBlock* tx_log_block) const {
    const auto [block_idx, local_idx] = idx;
    if (block_idx == 0) return meta->get_tx_entry(local_idx);
    return tx_log_block->get(local_idx);
  }

  /**
   * Move to the next transaction entry
   *
   * @param idx the current index, will be changed to the next index
   * @param tx_log_block output parameter, change to the TxLogBlock
   * corresponding to the next idx
   */
  void advance_tx_idx(pmem::TxEntryIdx& idx,
                      pmem::TxLogBlock*& tx_log_block) const;

  /**
   * @tparam B MetaBlock or TxLogBlock
   * @param block the block that needs a next block to be allocated
   * @return the block id of the allocated block
   */
  template <class B>
  LogicalBlockIdx alloc_next_block(B* block) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};
}  // namespace ulayfs::dram
