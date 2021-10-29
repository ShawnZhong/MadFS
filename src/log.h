#pragma once

#include <ostream>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "mtable.h"

namespace ulayfs::dram {

class LogMgr {
 private:
  pmem::MetaBlock* meta;

  Allocator* allocator;
  MemTable* mem_table;

  std::vector<pmem::LogEntryIdx> free_list;

 public:
  LogMgr() = default;
  LogMgr(pmem::MetaBlock* meta, Allocator* allocator, MemTable* mem_table)
      : meta(meta), allocator(allocator), mem_table(mem_table), free_list() {}

  pmem::LogEntry* get(pmem::LogEntryIdx idx) {
    return &mem_table->get_addr(idx.block_idx)
                ->log_entry_block.get(idx.local_idx);
  }

  pmem::LogEntryIdx append(pmem::LogOp op, VirtualBlockIdx begin_virtual_idx,
                           LogicalBlockIdx begin_logical_idx,
                           uint8_t num_blocks, uint16_t last_remaining) {
    pmem::LogEntryIdx idx = free_list.back();
    free_list.pop_back();

    pmem::LogEntryBlock* block =
        &mem_table->get_addr(idx.block_idx)->log_entry_block;
    block->set(idx.local_idx, op, begin_virtual_idx, begin_logical_idx,
               num_blocks, last_remaining);
    return idx;
  }

 private:
  // TODO: allocate LogEntryIdx into from allocator to free_list
};
}  // namespace ulayfs::dram
