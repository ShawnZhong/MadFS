#pragma once

#include "alloc/block.h"
#include "alloc/log_entry.h"
#include "alloc/tx_block.h"

namespace ulayfs::dram {
class Allocator {
 public:
  BlockAllocator block;
  TxBlockAllocator tx_block;
  LogEntryAllocator log_entry;

  Allocator(MemTable* mem_table, BitmapMgr* bitmap_mgr, ShmMgr* shm_mgr)
      : block(mem_table, bitmap_mgr),
        tx_block(&block, mem_table, shm_mgr),
        log_entry(&block, mem_table) {}
};

}  // namespace ulayfs::dram
