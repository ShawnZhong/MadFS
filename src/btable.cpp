#include "btable.h"

#include "const.h"
#include "file.h"
#include "idx.h"

namespace ulayfs::dram {

void BlkTable::update(TxEntryIdx* tx_idx, pmem::TxBlock** tx_block,
                      uint64_t* new_file_size, bool do_alloc,
                      bool init_bitmap) {
  // it's possible that the previous update move idx to overflow state
  if (!tx_mgr->handle_idx_overflow(tail_tx_idx, tail_tx_block, do_alloc)) {
    // if still overflow, do_alloc must be unset
    assert(!do_alloc);
    // if still overflow, we must have reached the tail already
    return;
  }

  LogicalBlockIdx prev_tx_block_idx = 0;

  while (true) {
    auto tx_entry = tx_mgr->get_entry_from_block(tail_tx_idx, tail_tx_block);
    if (!tx_entry.is_valid()) break;
    if (init_bitmap && tail_tx_idx.block_idx != prev_tx_block_idx)
      file->set_allocated(tail_tx_idx.block_idx);
    if (tx_entry.is_inline())
      apply_tx(tx_entry.commit_inline_entry);
    else
      apply_tx(tx_entry.commit_entry, file->get_log_mgr(), init_bitmap);
    prev_tx_block_idx = tail_tx_idx.block_idx;
    if (!tx_mgr->advance_tx_idx(tail_tx_idx, tail_tx_block, do_alloc)) break;
  }

  // mark all live data blocks in bitmap
  if (init_bitmap)
    for (const auto logical_idx : table) file->set_allocated(logical_idx);

  // return it out
  if (tx_idx) *tx_idx = tail_tx_idx;
  if (tx_block) *tx_block = tail_tx_block;
  if (new_file_size) *new_file_size = file_size;
}

void BlkTable::resize_to_fit(VirtualBlockIdx idx) {
  if (table.size() > idx) return;
  // countl_zero counts the number of leading 0-bits
  // if idx is already a pow of 2, it will be rounded to the next pow of 2
  // so that the table has enough space to hold this index
  int next_pow2 = 1 << (sizeof(idx) * 8 - std::countl_zero(idx));
  table.resize(next_pow2);
}

void BlkTable::apply_tx(pmem::TxEntryIndirect tx_commit_entry, LogMgr* log_mgr,
                        bool init_bitmap) {
  // first get begin_vidx and num_blocks for resizing
  uint32_t num_blocks;
  VirtualBlockIdx begin_vidx;
  log_mgr->get_coverage(tx_commit_entry.log_entry_idx, begin_vidx, num_blocks);
  VirtualBlockIdx end_vidx = begin_vidx + num_blocks;
  resize_to_fit(end_vidx);

  // then read and populate the actual mappings
  const pmem::LogHeadEntry* head_entry;
  LogEntryUnpackIdx unpack_idx =
      LogEntryUnpackIdx::from_pack_idx(tx_commit_entry.log_entry_idx);
  uint16_t leftover_bytes;
  bool has_more;

  do {
    head_entry = log_mgr->read_head(unpack_idx, num_blocks, init_bitmap);
    LogicalBlockIdx le_begin_lidxs[num_blocks / MAX_BLOCKS_PER_BODY + 1];
    has_more = log_mgr->read_body(unpack_idx, head_entry, num_blocks,
                                  &begin_vidx, le_begin_lidxs, &leftover_bytes);

    for (uint32_t offset = 0; offset < num_blocks; ++offset)
      table[begin_vidx + offset] =
          le_begin_lidxs[offset / MAX_BLOCKS_PER_BODY] +
          offset % MAX_BLOCKS_PER_BODY;

  } while (has_more);

  uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx) - leftover_bytes;
  if (now_file_size > file_size) file_size = now_file_size;
}

void BlkTable::apply_tx(pmem::TxEntryInline tx_commit_inline_entry) {
  uint32_t num_blocks = tx_commit_inline_entry.num_blocks;
  // for dummy entry, do nothing
  if (num_blocks == 0) return;
  VirtualBlockIdx begin_vidx = tx_commit_inline_entry.begin_virtual_idx;
  LogicalBlockIdx begin_lidx = tx_commit_inline_entry.begin_logical_idx;
  VirtualBlockIdx end_vidx = begin_vidx + num_blocks;
  resize_to_fit(end_vidx);

  // update block table mapping
  for (uint32_t i = 0; i < num_blocks; ++i)
    table[begin_vidx + i] = begin_lidx + i;

  // update file size if this write exceeds current file size
  // inline tx must be aligned to BLOCK_SIZE boundary
  uint64_t now_file_size = BLOCK_IDX_TO_SIZE(end_vidx);
  if (now_file_size > file_size) file_size = now_file_size;
}

std::ostream& operator<<(std::ostream& out, const BlkTable& b) {
  out << "BlkTable:\n";
  out << "\tfile_size: " << b.file_size << "\n";
  out << "\ttail_tx_idx: " << b.tail_tx_idx << "\n";
  for (size_t i = 0; i < b.table.size(); ++i) {
    if (b.table[i] != 0) {
      out << "\t" << i << " -> " << b.table[i] << "\n";
    }
  }
  return out;
}

}  // namespace ulayfs::dram
