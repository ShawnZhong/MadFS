#pragma once

#include <ostream>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "idx.h"
#include "layout.h"
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
      : meta(meta), allocator(allocator), mem_table(mem_table), free_list() {
    free_list.reserve(NUM_LOG_ENTRY);
  }

  const pmem::LogEntry* get_entry(pmem::LogEntryIdx idx) {
    return &mem_table->get_addr(idx.block_idx)
                ->log_entry_block.get(idx.local_idx);
  }

  // TODO: handle linked list
  // TODO: avoid using the same cacheline for the next op, but use same
  // cacheline for linked list
  pmem::LogEntryIdx append(pmem::LogEntry entry) {
    if (free_list.empty()) alloc();
    pmem::LogEntryIdx idx = free_list.back();
    free_list.pop_back();

    pmem::LogEntryBlock* block =
        &mem_table->get_addr(idx.block_idx)->log_entry_block;
    block->set(idx.local_idx, entry);
    return idx;
  }

 private:
  // TODO: allocate LogEntryIdx into from allocator to free_list
  void alloc() {
    LogicalBlockIdx idx = allocator->alloc(1);
    for (LogLocalIdx i = 0; i < NUM_LOG_ENTRY; ++i)
      free_list.push_back({idx, i});
  }
};
}  // namespace ulayfs::dram
