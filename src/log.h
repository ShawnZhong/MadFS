#pragma once

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
  pmem::MetaBlock* meta;

 public:
  LogMgr(File* file, pmem::MetaBlock* meta) : file(file), meta(meta) {}

 private:
  /**
   * get pointer to entry data from 6-byte unpacked index
   */
  const pmem::LogEntry* get_entry(LogEntryUnpackIdx idx);

  // syntax sugar for union dispatching
  const pmem::LogHeadEntry* get_head_entry(LogEntryUnpackIdx idx) {
    return &get_entry(idx)->head_entry;
  }

  const pmem::LogBodyEntry* get_body_entry(LogEntryUnpackIdx idx) {
    return &get_entry(idx)->body_entry;
  }

 public:
  // other parts only have hold access to head entries, which are identified
  // by the 5-byte LogEntryIdx, so only this variant of get_head_entry() is
  // public
  const pmem::LogHeadEntry* get_head_entry(LogEntryIdx idx) {
    return get_head_entry(LogEntryUnpackIdx::from_pack_idx(idx));
  }

  // TODO: return op
  // TODO: handle writev requests
  /**
   * get total coverage of the group of log entries starting at the head at idx
   *
   * @param first_head_idx LogEntryIdx of the first head entry
   * @param[out] begin_virtual_idx begin_virtual_idx of the coverage
   * @param[out] num_blocks number of blocks in the coverage
   * @param[out] begin_logical_idxs pointer to vector of logical indices,
   *                                if nonnull, pushes a list of corresponding
   *                                logical indices; pass one when applying
   *                                the transaction, and pass nullptr when
   *                                first checking OCC
   * @param[out] leftover_bytes pointer to an uint16_t, if nonnull, will be
   *                            filled with the number of leftover bytes for
   *                            this transaction
   */
  void get_coverage(LogEntryIdx first_head_idx,
                    VirtualBlockIdx& begin_virtual_idx, uint32_t& num_blocks,
                    std::vector<LogicalBlockIdx>* begin_logical_idxs = nullptr,
                    uint16_t* leftover_bytes = nullptr,
                    bool init_bitmap = false);

  // TODO: handle writev requests
  /**
   * populate log entries required by a single transaction
   *
   * @param allocator allocator to use for allocating log entries
   * @param op operation code, e.g., LOG_OVERWRITE
   * @param leftover_bytes remaining empty bytes in the last block
   * @param total_blocks total number blocks touched
   * @param begin_virtual_idx start of virtual index
   * @param begin_logical_idxs ordered list of logical indices for each chunk
   *                           of virtual index
   * @param fenced whether to force memory fencing of log block
   * @return index of the first LogHeadEntry for later retrival of the whole
   *         group of entries
   */
  LogEntryIdx append(Allocator* allocator, pmem::LogOp op,
                     uint16_t leftover_bytes, uint32_t total_blocks,
                     VirtualBlockIdx begin_virtual_idx,
                     const std::vector<LogicalBlockIdx>& begin_logical_idxs,
                     bool fenced = true);
};
}  // namespace ulayfs::dram
