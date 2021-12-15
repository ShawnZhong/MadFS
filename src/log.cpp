#include "log.h"

#include <ostream>
#include <vector>

#include "entry.h"
#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

const pmem::LogEntry* LogMgr::get_entry(LogEntryUnpackIdx idx) {
  return file->lidx_to_addr_rw(idx.block_idx)
      ->log_entry_block.get(idx.local_idx);
}

void LogMgr::get_coverage(LogEntryIdx first_head_idx,
                          VirtualBlockIdx& begin_virtual_idx,
                          uint32_t& num_blocks,
                          std::vector<LogicalBlockIdx>* begin_logical_idxs,
                          uint16_t* leftover_bytes) {
  LogEntryUnpackIdx idx = LogEntryUnpackIdx::from_pack_idx(first_head_idx);
  const pmem::LogHeadEntry* head_entry = get_head_entry(first_head_idx);

  // a head entry at the last slot of a LogBlock could have 0 body entries
  if (head_entry->num_blocks == 0) {
    assert(head_entry->overflow);
    idx = LogEntryUnpackIdx{head_entry->next.next_block_idx, 0};
    head_entry = get_head_entry(idx);
  }

  num_blocks = 0;
  while (head_entry != nullptr) {
    // the first body entry must be read, to get begin_virtual_idx
    if (num_blocks == 0) {
      idx.local_idx++;
      const pmem::LogBodyEntry* body_entry = get_body_entry(idx);
      begin_virtual_idx = body_entry->begin_virtual_idx;
    }

    // now idx points to a body entry
    if (begin_logical_idxs) {
      uint32_t segment_blocks = 0;
      while (segment_blocks < head_entry->num_blocks) {
        const pmem::LogBodyEntry* body_entry = get_body_entry(idx);
        begin_logical_idxs->push_back(body_entry->begin_logical_idx);
        segment_blocks += MAX_BLOCKS_PER_BODY;
        idx.local_idx++;
      }
    }
    num_blocks += head_entry->num_blocks;

    if (head_entry->overflow) {
      idx = LogEntryUnpackIdx{head_entry->next.next_block_idx, 0};
      head_entry = get_head_entry(idx);
    } else {
      // last segment holds the true leftover_bytes for this group
      if (leftover_bytes) *leftover_bytes = head_entry->leftover_bytes;
      head_entry = nullptr;
    }
  }
}

LogEntryIdx LogMgr::append(
    pmem::LogOp op, uint16_t leftover_bytes, uint32_t total_blocks,
    VirtualBlockIdx begin_virtual_idx,
    const std::vector<LogicalBlockIdx>& begin_logical_idxs, bool fenced) {
  // allocate the first head entry, whose LogEntryIdx will be returned back
  // to the transaction
  pmem::LogHeadEntry* head_entry = alloc_head_entry();
  LogEntryUnpackIdx first_head_idx{log_blocks.back(), last_local_idx()};
  VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
  size_t now_logical_idx_off = 0;

  while (head_entry != nullptr) {
    LogLocalUnpackIdx persist_start_idx = last_local_idx();
    head_entry->op = op;

    uint32_t num_blocks = total_blocks;
    uint32_t max_blocks = num_free_entries() * MAX_BLOCKS_PER_BODY;
    if (num_blocks > max_blocks) {
      num_blocks = max_blocks;
      head_entry->overflow = true;
      head_entry->saturate = true;
    } else {
      if (num_blocks > max_blocks - MAX_BLOCKS_PER_BODY)
        head_entry->saturate = true;
      head_entry->leftover_bytes = leftover_bytes;
    }

    head_entry->num_blocks = num_blocks;
    total_blocks -= num_blocks;

    // populate body entries until done or until current LogBlock filled up
    while (num_blocks > 0) {
      pmem::LogBodyEntry* body_entry = alloc_body_entry();
      assert(now_logical_idx_off < begin_logical_idxs.size());
      body_entry->begin_virtual_idx = now_virtual_idx;
      body_entry->begin_logical_idx = begin_logical_idxs[now_logical_idx_off++];
      now_virtual_idx += MAX_BLOCKS_PER_BODY;
      num_blocks = num_blocks <= MAX_BLOCKS_PER_BODY
                       ? 0
                       : num_blocks - MAX_BLOCKS_PER_BODY;
    }

    curr_block->persist(persist_start_idx, free_local_idx, fenced);
    if (head_entry->overflow)
      head_entry = alloc_head_entry(head_entry);
    else
      head_entry = nullptr;
  }

  return LogEntryUnpackIdx::to_pack_idx(first_head_idx);
}

pmem::LogEntry* LogMgr::alloc_entry(bool pack_align,
                                    pmem::LogHeadEntry* prev_head_entry) {
  // if need 16-byte alignment, maybe skip one 8-byte slot
  if (pack_align) free_local_idx = ALIGN_UP(free_local_idx, 2);

  if (free_local_idx == NUM_LOG_ENTRY) {
    LogicalBlockIdx idx = file->get_local_allocator()->alloc(1);
    log_blocks.push_back(idx);
    curr_block = &file->lidx_to_addr_rw(idx)->log_entry_block;
    free_local_idx = 0;
    if (prev_head_entry) prev_head_entry->next.next_block_idx = idx;
  } else {
    if (prev_head_entry) prev_head_entry->next.next_local_idx = free_local_idx;
  }

  assert(curr_block != nullptr);
  pmem::LogEntry* entry = curr_block->get(free_local_idx);
  memset(entry, 0, sizeof(pmem::LogEntry));   // zero-out at alloc

  free_local_idx++;
  return entry;
}

}  // namespace ulayfs::dram
