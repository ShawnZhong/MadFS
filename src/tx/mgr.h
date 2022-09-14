#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <vector>

#include "alloc/alloc.h"
#include "blk_table.h"
#include "block/block.h"
#include "entry.h"
#include "idx.h"
#include "lock.h"
#include "mem_table.h"
#include "offset.h"

namespace ulayfs::dram {

class TxMgr {
 public:
  MemTable* mem_table;
  BlkTable* blk_table;
  OffsetMgr* offset_mgr;

  Lock lock;  // nop lock is used by default

  TxMgr(MemTable* mem_table, BlkTable* blk_table, OffsetMgr* offset_mgr)
      : mem_table(mem_table), blk_table(blk_table), offset_mgr(offset_mgr) {}

  ssize_t do_pread(char* buf, size_t count, size_t offset,
                   Allocator* allocator);
  ssize_t do_read(char* buf, size_t count, Allocator* allocator);
  ssize_t do_pwrite(const char* buf, size_t count, size_t offset,
                    Allocator* allocator);
  ssize_t do_write(const char* buf, size_t count, Allocator* allocator);

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

}  // namespace ulayfs::dram
