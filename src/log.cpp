#include "log.h"

#include <cstdint>
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

const pmem::LogHeadEntry* LogMgr::read_head(LogEntryUnpackIdx& unpack_idx,
                                            uint32_t& num_blocks,
                                            bool init_bitmap) {
  // mark the first log entry block in bitmap
  // note we don't mark bitmap in read_body because body entries always in the
  // same block as the head entry
  if (init_bitmap) file->set_allocated(unpack_idx.block_idx);
  const pmem::LogHeadEntry* head_entry = get_head_entry(unpack_idx);

  // a head entry at the last slot of a LogBlock could have 0 body entries
  if (head_entry->num_blocks == 0) {
    assert(head_entry->overflow);
    unpack_idx = LogEntryUnpackIdx{head_entry->next.next_block_idx, 0};
    head_entry = get_head_entry(unpack_idx);
  }

  num_blocks = head_entry->num_blocks;
  return head_entry;
}

bool LogMgr::read_body(LogEntryUnpackIdx& unpack_idx,
                       const pmem::LogHeadEntry* head_entry,
                       uint32_t num_blocks, VirtualBlockIdx* begin_vidx,
                       LogicalBlockIdx begin_lidxs[],
                       uint16_t* leftover_bytes) {
  // move unpack_idx from head to the first body entry
  unpack_idx.local_idx++;
  const pmem::LogBodyEntry* body_entry = get_body_entry(unpack_idx);
  if (begin_vidx) *begin_vidx = body_entry->begin_virtual_idx;

  if (begin_lidxs) {
    uint32_t seg_blocks, len;
    for (seg_blocks = 0, len = 0; seg_blocks < num_blocks;
         seg_blocks += MAX_BLOCKS_PER_BODY, ++len, unpack_idx.local_idx++) {
      begin_lidxs[len] = get_body_entry(unpack_idx)->begin_logical_idx;
    }
    assert(len == ALIGN_UP(num_blocks, 64) / 64);
  }

  if (head_entry->overflow) {
    unpack_idx = LogEntryUnpackIdx{head_entry->next.next_block_idx, 0};
    return true;
  }

  // last segment holds the true leftover_bytes for this group
  if (leftover_bytes) *leftover_bytes = head_entry->leftover_bytes;
  return false;
}

LogEntryIdx LogMgr::append(
    Allocator* allocator, pmem::LogOp op, uint16_t leftover_bytes,
    uint32_t total_blocks, VirtualBlockIdx begin_virtual_idx,
    const std::vector<LogicalBlockIdx>& begin_logical_idxs, bool fenced) {
  // allocate the first head entry, whose LogEntryIdx will be returned back
  // to the transaction
  pmem::LogHeadEntry* head_entry = allocator->alloc_head_entry();
  LogEntryIdx first_head_idx = allocator->get_first_head_idx();
  VirtualBlockIdx now_virtual_idx = begin_virtual_idx;
  size_t now_logical_idx_off = 0;

  while (head_entry != nullptr) {
    LogLocalUnpackIdx persist_start_idx = allocator->last_log_local_idx();
    head_entry->op = op;

    uint32_t num_blocks = total_blocks;
    uint32_t max_blocks =
        allocator->num_free_log_entries() * MAX_BLOCKS_PER_BODY;
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
      pmem::LogBodyEntry* body_entry = allocator->alloc_body_entry();
      assert(now_logical_idx_off < begin_logical_idxs.size());
      body_entry->begin_virtual_idx = now_virtual_idx;
      body_entry->begin_logical_idx = begin_logical_idxs[now_logical_idx_off++];
      now_virtual_idx += MAX_BLOCKS_PER_BODY;
      num_blocks = num_blocks <= MAX_BLOCKS_PER_BODY
                       ? 0
                       : num_blocks - MAX_BLOCKS_PER_BODY;
    }

    allocator->get_curr_log_block()->persist(
        persist_start_idx, allocator->last_log_local_idx() + 1, fenced);
    if (head_entry->overflow)
      head_entry = allocator->alloc_head_entry(head_entry);
    else
      head_entry = nullptr;
  }

  return first_head_idx;
}

}  // namespace ulayfs::dram
