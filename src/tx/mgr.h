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

  Lock lock;  // nop lock is used by default
  OffsetMgr offset_mgr;

  TxMgr(File* file, MemTable* mem_table)
      : file(file), mem_table(mem_table), offset_mgr(this) {}

  ssize_t do_pread(char* buf, size_t count, size_t offset);
  ssize_t do_read(char* buf, size_t count);
  ssize_t do_pwrite(const char* buf, size_t count, size_t offset);
  ssize_t do_write(const char* buf, size_t count);

  /**
   * populate log entries required by a single transaction; do persist but not
   * fenced
   *
   * @param allocator allocator to use for allocating log entries
   * @param op operation code, e.g., LOG_OVERWRITE
   * @param leftover_bytes remaining empty bytes in the last block
   * @param num_blocks total number blocks touched
   * @param begin_vidx start of virtual index
   * @param begin_lidxs ordered list of logical indices for each chunk of
   * virtual index
   * @return index of the first LogHeadEntry for later retrival of the whole
   *         group of entries
   */
  LogCursor append_log_entry(
      Allocator* allocator, pmem::LogEntry::Op op, uint16_t leftover_bytes,
      uint32_t num_blocks, VirtualBlockIdx begin_vidx,
      const std::vector<LogicalBlockIdx>& begin_lidxs) const;

 public:
  friend std::ostream& operator<<(std::ostream& out, const TxMgr& tx_mgr);
};

}  // namespace ulayfs::dram
