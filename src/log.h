#pragma once

#include <cstdint>
#include <ostream>
#include <vector>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "idx.h"
#include "mtable.h"

namespace ulayfs::dram {

// forward declaration
class File;

class LogMgr {
 private:
  File* file;

 public:
  explicit LogMgr(File* file) : file(file) {}

  /**
   * get the given entry
   *
   * @param[in] idx the log entry index of the wantted entry
   * @param[out] curr_block return which log entry block
   * @param[in] init_bitmap whether set related LogEntryBlock as used in bitmap
   * @return the wantted log entry
   */
  [[nodiscard]] pmem::LogEntry* get_entry(LogEntryIdx idx,
                                          pmem::LogEntryBlock*& curr_block,
                                          bool init_bitmap = false);

  /**
   * get the next entry
   *
   * @param[in] curr_entry the current log entry
   * @param[in,out] curr_block the current log entry block; will be updated if
   * move on to the next block
   * @param[in] init_bitmap whether set related LogEntryBlock as used in bitmap
   * @return the next log entry
   */
  [[nodiscard]] pmem::LogEntry* get_next_entry(const pmem::LogEntry* curr_entry,
                                               pmem::LogEntryBlock*& curr_block,
                                               bool init_bitmap = false);

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
  LogEntryIdx append(Allocator* allocator, pmem::LogEntry::Op op,
                     uint16_t leftover_bytes, uint32_t num_blocks,
                     VirtualBlockIdx begin_vidx,
                     const std::vector<LogicalBlockIdx>& begin_lidxs);
};
}  // namespace ulayfs::dram
