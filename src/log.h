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
  MemTable* mem_table;

  // blocks for storing log entries, max 512 entries per block
  std::vector<LogicalBlockIdx> log_blocks;
  // pointer to current LogBlock == the one identified by log_blocks.back()
  pmem::LogEntryBlock* curr_block;
  // local index of the first free entry slot in the last block
  // might equal NUM_LOCAL_ENTREIS when a new log block is not allocated yet
  LogLocalUnpackIdx free_local_idx;

 public:
  LogMgr(File* file, pmem::MetaBlock* meta, MemTable* mem_table)
      : file(file),
        meta(meta),
        mem_table(mem_table),
        log_blocks(),
        curr_block(nullptr),
        free_local_idx(NUM_LOG_ENTRY) {}

  [[nodiscard]] pmem::MetaBlock* get_meta() const { return meta; }

 private:
  /**
   * get pointer to entry data from 6-byte unpacked index
   */
  const pmem::LogEntry* get_entry(LogEntryUnpackIdx idx) {
    return mem_table->get(idx.block_idx)->log_entry_block.get(idx.local_idx);
  }

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
                    uint16_t* leftover_bytes = nullptr);

  // TODO: handle writev requests
  /**
   * populate log entries required by a single transaction
   *
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
  LogEntryIdx append(pmem::LogOp op, uint16_t leftover_bytes,
                     uint32_t total_blocks, VirtualBlockIdx begin_virtual_idx,
                     const std::vector<LogicalBlockIdx>& begin_logical_idxs,
                     bool fenced = true);

 private:
  /**
   * allocate a log entry, possibly triggering allocating a new LogBlock
   */
  pmem::LogEntry* alloc_entry(bool pack_align = false,
                              pmem::LogHeadEntry* prev_head_entry = nullptr);

  // syntax sugar for union dispatching
  pmem::LogHeadEntry* alloc_head_entry(
      pmem::LogHeadEntry* prev_head_entry = nullptr) {
    return &alloc_entry(/*pack_align*/ true, prev_head_entry)->head_entry;
  }

  pmem::LogBodyEntry* alloc_body_entry() { return &alloc_entry()->body_entry; }

  /**
   * get the number of free entries in the current LogBlock
   */
  [[nodiscard]] uint16_t num_free_entries() const {
    return NUM_LOG_ENTRY - free_local_idx;
  }

  /**
   * get the last allocated entry's local index
   */
  [[nodiscard]] LogLocalUnpackIdx last_local_idx() const {
    return free_local_idx - 1;
  }
};
}  // namespace ulayfs::dram
