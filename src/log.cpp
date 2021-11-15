#include "log.h"

#include <ostream>
#include <vector>

#include "alloc.h"
#include "block.h"
#include "entry.h"
#include "idx.h"
#include "layout.h"
#include "mtable.h"

namespace ulayfs::dram {

void LogMgr::get_coverage(LogEntryIdx first_head_idx, bool need_logical_idxs,
                          VirtualBlockIdx& begin_virtual_idx,
                          uint32_t& num_blocks,
                          std::vector<LogicalBlockIdx>* begin_logical_idxs) {
  LogEntryIdx idx = first_head_idx;
  const pmem::LogHeadEntry* head_entry = get_head_entry(first_head_idx);

  // a head entry at the last slot of a LogBlock could have 0 body entries
  if (head_entry->num_blocks == 0) {
    PANIC_IF(!head_entry->overflow, "should have overflow segment");
    idx = LogEntryIdx{head_entry->next.next_block_idx, 0};
    head_entry = get_head_entry(idx);
  }

  num_blocks = 0;
  while (head_entry != nullptr) {
    if (num_blocks == 0) {
      idx.local_idx++;
      const pmem::LogBodyEntry* body_entry = get_body_entry(idx);
      begin_virtual_idx = body_entry->begin_virtual_idx;
    }

    // now idx points to a body entry
    if (need_logical_idxs) {
      PANIC_IF(begin_logical_idxs == nullptr, "begin_logical_idxs is null");
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
      idx = LogEntryIdx{head_entry->next.next_block_idx, 0};
      head_entry = get_head_entry(idx);
    } else
      head_entry = nullptr;
  }
}

LogEntryIdx LogMgr::append(
    pmem::LogOp op, uint16_t leftover_bytes, uint32_t total_blocks,
    VirtualBlockIdx begin_virtual_idx,
    const std::vector<LogicalBlockIdx>& begin_logical_idxs, bool fenced) {
  pmem::LogHeadEntry* head_entry = alloc_head_entry();
  LogEntryIdx first_head_idx =
      LogEntryIdx{log_blocks.back(), LogLocalIdx(free_local_idx - 1)};
  VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
  size_t now_logical_idx_off = 0;

  while (head_entry != nullptr) {
    LogLocalIdx persist_start_idx = LogLocalIdx(free_local_idx - 1);
    head_entry->op = op;

    uint32_t num_blocks = total_blocks;
    uint32_t max_blocks = num_free_entries() * MAX_BLOCKS_PER_BODY;
    if (num_blocks > max_blocks) {
      num_blocks = max_blocks;
      head_entry->overflow = true;
      head_entry->saturate = true;
    } else if (num_blocks > max_blocks - MAX_BLOCKS_PER_BODY) {
      head_entry->saturate = true;
      head_entry->leftover_bytes = leftover_bytes;
    }

    head_entry->num_blocks = num_blocks;
    total_blocks -= num_blocks;

    // populate body entries until done or until current LogBlock filled up
    while (num_blocks > 0) {
      pmem::LogBodyEntry* body_entry = alloc_body_entry();
      PANIC_IF(now_logical_idx_off >= begin_logical_idxs.size(),
               "begin_logical_idxs vector not long enough");
      body_entry->begin_virtual_idx = now_virtual_idx;
      body_entry->begin_logical_idx = begin_logical_idxs[now_logical_idx_off++];
      now_virtual_idx += MAX_BLOCKS_PER_BODY;
      num_blocks = num_blocks <= MAX_BLOCKS_PER_BODY
                       ? 0
                       : num_blocks - MAX_BLOCKS_PER_BODY;
    }

    // FIXME: better debugging message when future implementation is done
    std::ostringstream osstream;
    osstream << *head_entry;
    DEBUG("new head entry %s", osstream.str().c_str());

    curr_block->persist(persist_start_idx, free_local_idx, fenced);
    if (head_entry->overflow)
      head_entry = alloc_head_entry(head_entry);
    else
      head_entry = nullptr;
  }

  return first_head_idx;
}

}  // namespace ulayfs::dram
