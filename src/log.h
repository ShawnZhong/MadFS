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
  LogMgr(File* file) : file(file) {}

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

  /**
   * read the head entry and return number of blocks
   *
   * @param[in,out] unpack_idx the unpack index of the head entry
   * @param[out] num_blocks number of block mapping these entries contain
   * @param[in] init_bitmap whether init bitmap
   * @return address of the head entry (will be used by read_body)
   */
  const pmem::LogHeadEntry* read_head(LogEntryUnpackIdx& unpack_idx,
                                      uint32_t& num_blocks,
                                      bool init_bitmap = false);

  /**
   * read body entries following the given head_entry
   *
   * @param[in,out] unpack_idx unpacked head_idx (will be updated)
   * @param[in] head_entry address of head entry (returned from read_head)
   * @param[in] num_blocks number of block mapping (returned from read_head)
   * @param[out] begin_vidx begin virtual index of this mapping
   * @param[out] begin_lidxs an array of logical indexes to store mapping, must
   * be at least `ALIGN_UP(num_blocks, 64) / 64` large
   * @param[out] leftover_bytes number of leftover bytes in the last block
   * @return whether there are more entries
   */
  bool read_body(LogEntryUnpackIdx& unpack_idx,
                 const pmem::LogHeadEntry* head_entry, uint32_t num_blocks,
                 VirtualBlockIdx* begin_vidx = nullptr,
                 LogicalBlockIdx begin_lidxs[] = nullptr,
                 uint16_t* leftover_bytes = nullptr);

  /**
   * Only get the begin virtual index and number of blocks (assuming no vector
   * write) but don't care actual mapping
   *
   * @param[in] first_head_idx the first head index (from TxEntryIndirect)
   * @param[out] begin_vidx begin of this virtual-logical mapping
   * @param[out] num_blocks length of this virtual-logical mapping
   */
  void get_coverage(LogEntryIdx first_head_idx, VirtualBlockIdx& begin_vidx,
                    uint32_t& num_blocks) {
    LogEntryUnpackIdx unpack_idx =
        LogEntryUnpackIdx::from_pack_idx(first_head_idx);
    const pmem::LogHeadEntry* head_entry = read_head(unpack_idx, num_blocks);
    if (!read_body(unpack_idx, head_entry, num_blocks, &begin_vidx)) return;

    // if there are multiple segments...
    uint32_t num_blocks_segment;
    do {
      head_entry = read_head(unpack_idx, num_blocks_segment);
      num_blocks += num_blocks_segment;
    } while (read_body(unpack_idx, head_entry, num_blocks_segment));
  }

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
