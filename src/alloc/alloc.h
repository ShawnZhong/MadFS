#pragma once

#include "alloc/block.h"
#include "alloc/log_entry.h"
#include "alloc/tx_block.h"
#include "shm.h"

namespace madfs::dram {
class Allocator {
 public:
  BlockAllocator block;
  TxBlockAllocator tx_block;
  LogEntryAllocator log_entry;

  Allocator(MemTable* mem_table, BitmapMgr* bitmap_mgr,
            PerThreadData* per_thread_data)
      : block(mem_table, bitmap_mgr),
        tx_block(&block, mem_table, per_thread_data),
        log_entry(&block, mem_table) {}
};

}  // namespace madfs::dram
