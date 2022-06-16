#include "log.h"

#include <cstdint>
#include <vector>

#include "alloc.h"
#include "block.h"
#include "const.h"
#include "entry.h"
#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

pmem::LogEntry* LogMgr::get_entry(LogEntryIdx idx,
                                  pmem::LogEntryBlock*& curr_block,
                                  bool init_bitmap) {
  if (init_bitmap) file->set_allocated(idx.block_idx);
  curr_block = &file->lidx_to_addr_rw(idx.block_idx)->log_entry_block;
  return curr_block->get(idx.local_offset);
}

[[nodiscard]] pmem::LogEntry* LogMgr::get_next_entry(
    const pmem::LogEntry* curr_entry, pmem::LogEntryBlock*& curr_block,
    bool init_bitmap) {
  if (!curr_entry->has_next) return nullptr;
  if (curr_entry->is_next_same_block)
    return curr_block->get(curr_entry->next.local_offset);
  if (init_bitmap) file->set_allocated(curr_entry->next.block_idx);
  // if the next entry is on another block, it must be from the first byte
  curr_block =
      &file->lidx_to_addr_rw(curr_entry->next.block_idx)->log_entry_block;
  return curr_block->get(0);
}

LogEntryIdx LogMgr::append(Allocator* allocator, pmem::LogEntry::Op op,
                           uint16_t leftover_bytes, uint32_t num_blocks,
                           VirtualBlockIdx begin_vidx,
                           const std::vector<LogicalBlockIdx>& begin_lidxs) {
  LogEntryIdx first_idx;
  pmem::LogEntryBlock* first_block;
  pmem::LogEntry* first_entry =
      allocator->alloc_log_entry(num_blocks, first_idx, first_block);
  pmem::LogEntry* curr_entry = first_entry;
  pmem::LogEntryBlock* curr_block = first_block;

  // i to iterate through begin_lidxs across entries
  // j to iteratr within each entry
  uint32_t i, j;
  i = 0;
  while (true) {
    curr_entry->op = op;
    curr_entry->begin_vidx = begin_vidx;
    for (j = 0; j < curr_entry->get_lidxs_len(); ++j)
      curr_entry->begin_lidxs[j] = begin_lidxs[i + j];
    auto next_entry = get_next_entry(curr_entry, curr_block);
    if (next_entry) {
      curr_entry->leftover_bytes = 0;
      curr_entry->persist();
      curr_entry = next_entry;
      i += j;
      begin_vidx += (j << BITMAP_BLOCK_CAPACITY_SHIFT);
    } else {
      curr_entry->leftover_bytes = leftover_bytes;
      curr_entry->persist();
      break;
    }
  }
  return first_idx;
}

}  // namespace ulayfs::dram
