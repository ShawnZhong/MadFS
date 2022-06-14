#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <vector>

#include "block.h"
#include "entry.h"
#include "idx.h"

namespace ulayfs::dram {

class File;

class TxMgr {
 private:
  File* file;
  pmem::MetaBlock* meta;

 public:
  TxMgr(File* file, pmem::MetaBlock* meta) : file(file), meta(meta) {}

  ssize_t do_pread(char* buf, size_t count, size_t offset);
  ssize_t do_read(char* buf, size_t count);

  ssize_t do_pwrite(const char* buf, size_t count, size_t offset);
  ssize_t do_write(const char* buf, size_t count);

  bool tx_idx_greater(TxEntryIdx lhs_idx, TxEntryIdx rhs_idx,
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
                      bool do_alloc) const;

  /**
   * Read the entry from the MetaBlock or TxBlock
   */
  [[nodiscard]] pmem::TxEntry get_entry_from_block(
      TxEntryIdx idx, pmem::TxBlock* tx_block) const;

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
   * @param[in] block the block that needs a next block to be allocated
   * @param[out] new_tx_block the new tx block allocated (can be same as block)
   * @return the block id of the allocated block
   */
  template <class B>
  LogicalBlockIdx alloc_next_block(B* block,
                                   pmem::TxBlock*& new_tx_block) const;

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
  void gc(LogicalBlockIdx tail_tx_block, uint64_t file_size);

 private:
  /**
   * Move along the linked list of TxBlock and find the tail. The returned
   * tail may not be up-to-date due to race condition. No new blocks will be
   * allocated. If the end of TxBlock is reached, just return NUM_TX_ENTRY as
   * the TxLocalIdx.
   */
  void find_tail(TxEntryIdx& curr_idx, pmem::TxBlock*& curr_block) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

}  // namespace ulayfs::dram
