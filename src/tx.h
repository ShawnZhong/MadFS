#pragma once

namespace ulayfs::dram {

class TxMgr {
 private:
  pmem::MetaBlock* meta;
  Allocator* allocator;
  BlkTable* blk_table;
  MemTable* mem_table;

  // current view of the log_tail, used as a hint
  pmem::LogEntryIdx log_tail;

  /**
   * given a current tx_log_block, return the next block id
   * allocate one if the next one doesn't exist
   */
  inline pmem::LogicalBlockIdx get_next_tx_log_block_idx(
      pmem::TxLogBlock* tx_log_block) {
    auto block_idx = tx_log_block->get_next_block_idx();
    if (block_idx != 0) return block_idx;

    // allocate the next block
    auto new_block_id = allocator->alloc(1);
    tx_log_block->set_next_block_idx(new_block_id);
    return new_block_id;
  }

  /**
   * append a transaction begin entry to the tx_log
   */
  pmem::TxEntryIdx append_tx_begin_entry(pmem::TxBeginEntry tx_begin_entry) {
    auto [block_idx_hint, local_idx_hint] = meta->get_tx_log_tail();

    // append to the inline tx_entries
    if (block_idx_hint == 0) {
      auto local_idx = meta->inline_try_begin(tx_begin_entry, local_idx_hint);
      if (local_idx >= 0) return {0, local_idx};
    }

    // append to the tx log blocks
    while (true) {
      auto block = mem_table->get_addr(block_idx_hint);
      auto tx_log_block = &block->tx_log_block;

      auto local_idx = tx_log_block->try_begin(tx_begin_entry, local_idx_hint);
      if (local_idx >= 0) return {block_idx_hint, local_idx};

      block_idx_hint = get_next_tx_log_block_idx(tx_log_block);
      local_idx_hint = 0;
    }
  };

 public:
  TxMgr() : meta{nullptr} {}
  void init(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table,
            BlkTable* blk_table) {
    this->meta = meta;
    this->allocator = allocator;
    this->blk_table = blk_table;
    this->mem_table = mem_table;
  }

  void overwrite(const void* buf, size_t count, off_t offset) {
    pmem::VirtualBlockIdx start_virtual_idx = ALIGN_DOWN(offset, BLOCK_SIZE);
    uint32_t num_blocks = ALIGN_UP(count, BLOCK_SIZE);

    // add a begin entry to the tx_log
    pmem::TxBeginEntry tx_begin_entry{start_virtual_idx, num_blocks};
    append_tx_begin_entry(tx_begin_entry);
  }
};
}  // namespace ulayfs::dram