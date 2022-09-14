#pragma once

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <vector>

#include "alloc/alloc.h"
#include "block/block.h"
#include "entry.h"
#include "idx.h"
#include "lock.h"
#include "offset.h"

namespace ulayfs::dram {

class TxMgr {
 public:
  File* file;
  MemTable* mem_table;
  OffsetMgr* offset_mgr;

  Lock lock;  // nop lock is used by default

  TxMgr(File* file, MemTable* mem_table, OffsetMgr* offset_mgr)
      : file(file), mem_table(mem_table), offset_mgr(offset_mgr) {}

  ssize_t do_pread(char* buf, size_t count, size_t offset);
  ssize_t do_read(char* buf, size_t count);
  ssize_t do_pwrite(const char* buf, size_t count, size_t offset);
  ssize_t do_write(const char* buf, size_t count);

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

}  // namespace ulayfs::dram
