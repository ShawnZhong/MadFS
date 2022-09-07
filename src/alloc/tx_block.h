#pragma once

#include "alloc/block.h"
#include "shm.h"

namespace ulayfs::dram {

class TxBlockAllocator {
  BlockAllocator* block_allocator;
  MemTable* mem_table;
  ShmMgr* shm_mgr;
  size_t shm_thread_idx;

  // a tx block may be allocated but unused when another thread does that first
  // this tx block will then be saved here for future use
  // a tx block candidate must have all bytes zero except the sequence number
  pmem::Block* avail_tx_block{nullptr};
  LogicalBlockIdx avail_tx_block_idx{0};

  // each thread will pin a tx block so that the garbage collector will not
  // reclaim this block and blocks after it
  LogicalBlockIdx pinned_tx_block_idx{0};
  LogicalBlockIdx* notify_addr{nullptr};

 public:
  TxBlockAllocator(BlockAllocator* block_allocator, MemTable* mem_table,
                   ShmMgr* shm_mgr)
      : block_allocator(block_allocator),
        mem_table(mem_table),
        shm_mgr(shm_mgr),
        shm_thread_idx(shm_mgr->get_next_shm_thread_idx()) {}

  [[nodiscard]] LogicalBlockIdx get_pinned_idx() const {
    return pinned_tx_block_idx;
  }

  /**
   * Update the shared memory to notify this thread has moved to a new tx block;
   * do nothing if there is no moving
   * @param tx_block_idx the index of the currently referenced tx block; zero if
   * this is the first time this thread try to pin a tx block. if this is the
   * first time this thread tries to pin, it must acquire a slot on the shared
   * memory and then publish a zero. this notify gc threads that there is a
   * thread performing the initial log replaying and do not reclaim any blocks.
   */
  void pin(LogicalBlockIdx tx_block_idx) {
    //    if (!notify_addr) {
    //      assert(tx_block_idx == 0);
    //      // TODO: allocate from shared memory!
    //      return;
    //    }
    //    if (pinned_tx_block_idx == tx_block_idx) return;
    pinned_tx_block_idx = tx_block_idx;
    // TODO: uncomment this line after setting shared memory
    // *notify_addr = tx_block_idx;
  }

  /**
   * @tparam B MetaBlock or TxBlock
   * @param block the block that needs a next block to be allocated
   * @return the block id of the allocated block and the new tx block allocated
   */
  template <class B>
  std::tuple<LogicalBlockIdx, pmem::TxBlock*> alloc_next(B* block) {
    auto [new_block_idx, new_block] = this->alloc(block->get_tx_seq() + 1);

    bool success = block->try_set_next_tx_block(new_block_idx);
    if (!success) {  // race condition for adding the new blocks
      this->free(new_block_idx, new_block);
      new_block_idx = block->get_next_tx_block();
      new_block = &mem_table->lidx_to_addr_rw(new_block_idx)->tx_block;
    }

    shm_mgr->get_per_thread_data(shm_thread_idx)->tx_block_idx =
        new_block_idx.get();

    return {new_block_idx, new_block};
  }

 private:
  /**
   * Allocate a tx block
   * @param tx_seq the tx sequence number of the tx block (gc_seq = 0)
   * @return a tuple of the block index and the block address
   */
  std::tuple<LogicalBlockIdx, pmem::TxBlock*> alloc(uint32_t tx_seq) {
    if (avail_tx_block) {
      pmem::Block* tx_block = avail_tx_block;
      avail_tx_block = nullptr;
      tx_block->tx_block.set_tx_seq(tx_seq);
      pmem::persist_cl_fenced(&tx_block->cache_lines[NUM_CL_PER_BLOCK - 1]);
      return {avail_tx_block_idx, &tx_block->tx_block};
    }

    LogicalBlockIdx new_block_idx = block_allocator->alloc(1);
    pmem::Block* tx_block = mem_table->lidx_to_addr_rw(new_block_idx);
    memset(&tx_block->cache_lines[NUM_CL_PER_BLOCK - 1], 0, CACHELINE_SIZE);
    tx_block->tx_block.set_tx_seq(tx_seq);
    pmem::persist_cl_unfenced(&tx_block->cache_lines[NUM_CL_PER_BLOCK - 1]);
    tx_block->zero_init(0, NUM_CL_PER_BLOCK - 1);
    return {new_block_idx, &tx_block->tx_block};
  }

  /**
   * Free a tx block
   * @param tx_block_idx the index of the tx block
   * @param tx_block the address of the tx block to free
   */
  void free(LogicalBlockIdx tx_block_idx, pmem::TxBlock* tx_block) {
    assert(!avail_tx_block);
    avail_tx_block_idx = tx_block_idx;
    avail_tx_block = reinterpret_cast<pmem::Block*>(tx_block);
  }
};
}  // namespace ulayfs::dram
