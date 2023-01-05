#pragma once

#include "alloc/block.h"
#include "shm.h"

namespace ulayfs::dram {

class TxBlockAllocator {
  BlockAllocator* block_allocator;
  MemTable* mem_table;
  PerThreadData* per_thread_data;

  // a tx block may be allocated but unused when another thread does that first
  // this tx block will then be saved here for future use
  // a tx block candidate must have all bytes zero except the sequence number
  pmem::Block* avail_tx_block{nullptr};
  LogicalBlockIdx avail_tx_block_idx{0};

 public:
  TxBlockAllocator(BlockAllocator* block_allocator, MemTable* mem_table,
                   PerThreadData* per_thread_data)
      : block_allocator(block_allocator),
        mem_table(mem_table),
        per_thread_data(per_thread_data) {}

  ~TxBlockAllocator() {
    if (avail_tx_block) block_allocator->free(avail_tx_block_idx);
    reset_per_thread_data();
  }

  void reset_per_thread_data() {
    per_thread_data->reset();
  }

  [[nodiscard]] LogicalBlockIdx get_pinned_idx() const {
    return per_thread_data->get_tx_block_idx();
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
    per_thread_data->set_tx_block_idx(tx_block_idx);
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

    return {new_block_idx, new_block};
  }

 private:
  /**
   * Allocate a tx block
   * @param tx_seq the tx sequence number of the tx block
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
