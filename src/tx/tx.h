#pragma once

#include "cursor/log.h"
#include "file/file.h"
#include "utils/timer.h"

namespace madfs::dram {

/**
 * Tx represents a single ongoing transaction.
 */
class Tx {
 protected:
  // pointer to the outer class
  File* file;
  Lock* lock;
  OffsetMgr* offset_mgr;
  MemTable* mem_table;
  BlkTable* blk_table;
  Allocator* allocator;  // local

  /*
   * Input properties
   * In the case of partial read/write, count will be changed, so does end_*
   */
  size_t count;
  const size_t offset;

  /*
   * Derived properties
   */
  // the byte range to be written is [offset, end_offset), and the byte at
  // end_offset is NOT included
  size_t end_offset;

  // the index of the virtual block that contains the beginning offset
  const VirtualBlockIdx begin_vidx;
  // the block index to be written is [begin_vidx, end_vidx), and the block with
  // index end_vidx is NOT included
  VirtualBlockIdx end_vidx;

  // total number of blocks
  const size_t num_blocks;

  // in the case of read/write with offset change, update is done first
  bool is_offset_depend;
  // if update is done first, we must know file_size already
  uint64_t ticket;

  FileState state;

  Tx(File* file, size_t count, size_t offset)
      : file(file),
        lock(&file->lock),
        offset_mgr(&file->offset_mgr),
        mem_table(&file->mem_table),
        blk_table(&file->blk_table),
        allocator(file->get_local_allocator()),

        // input properties
        count(count),
        offset(offset),

        // derived properties
        end_offset(offset + count),
        begin_vidx(BLOCK_SIZE_TO_IDX(offset)),
        end_vidx(BLOCK_SIZE_TO_IDX(ALIGN_UP(end_offset, BLOCK_SIZE))),
        num_blocks(end_vidx - begin_vidx),
        is_offset_depend(false) {}

  ~Tx() { lock->unlock(); }

 public:
  template <typename TX, typename... Params>
  static ssize_t exec_and_release_offset(Params&&... params) {
    TX tx(std::forward<Params>(params)...);
    ssize_t ret = tx.exec();
    tx.offset_mgr->release(tx.ticket, tx.state.cursor);
    return ret;
  }

 protected:
  /**
   * Move to the real tx and update first/last_src_block to indicate whether to
   * redo; update file_size if necessary
   *
   * @param[in] curr_entry the last entry returned by try_commit; this should be
   * what dereferenced from tail_tx_idx, and we only take it to avoid one more
   * dereference to some shared memory
   * @param[in] first_vidx the first block's virtual idx; ignored if !copy_first
   * @param[in] last_vidx the last block's virtual idx; ignored if !copy_last
   * @param[out] conflict_image a list of lidx that conflict with the current tx
   * @param[out] into_new_block if not nullptr, return whether the cursor has
   * been advanced into a new tx block
   * @return true if there exits conflict and requires redo
   */
  bool handle_conflict(pmem::TxEntry curr_entry, VirtualBlockIdx first_vidx,
                       VirtualBlockIdx last_vidx,
                       std::vector<LogicalBlockIdx>& conflict_image,
                       bool* into_new_block = nullptr) {
    bool has_conflict = false;
    if (into_new_block) *into_new_block = false;
    do {
      if (curr_entry.is_inline()) {  // inline tx entry
        has_conflict |= get_conflict_image(
            first_vidx, last_vidx, curr_entry.inline_entry.begin_virtual_idx,
            curr_entry.inline_entry.begin_logical_idx,
            curr_entry.inline_entry.num_blocks, conflict_image);
        VirtualBlockIdx end_vidx = curr_entry.inline_entry.begin_virtual_idx +
                                   curr_entry.inline_entry.num_blocks;
        uint64_t possible_file_size = BLOCK_IDX_TO_SIZE(end_vidx);
        if (possible_file_size > state.file_size)
          state.file_size = possible_file_size;
      } else {  // non-inline tx entry
        LogCursor log_cursor(curr_entry.indirect_entry, mem_table);

        do {
          uint32_t i;
          for (i = 0; i < log_cursor->get_lidxs_len() - 1; ++i) {
            has_conflict |= get_conflict_image(
                first_vidx, last_vidx,
                log_cursor->begin_vidx +
                    (i << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT),
                log_cursor->begin_lidxs[i], BITMAP_ENTRY_BLOCKS_CAPACITY,
                conflict_image);
          }
          has_conflict |= get_conflict_image(
              first_vidx, last_vidx,
              log_cursor->begin_vidx +
                  (i << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT),
              log_cursor->begin_lidxs[i],
              log_cursor->get_last_lidx_num_blocks(), conflict_image);
          VirtualBlockIdx end_vidx = log_cursor->begin_vidx +
                                     (i << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT) +
                                     log_cursor->get_last_lidx_num_blocks();
          uint64_t possible_file_size =
              BLOCK_IDX_TO_SIZE(end_vidx) - log_cursor->leftover_bytes;
          if (possible_file_size > state.file_size)
            state.file_size = possible_file_size;

        } while (log_cursor.advance(mem_table));
      }
      // only update into_new_block if it is not nullptr and not set true yet
      if (!state.cursor.advance(
              mem_table,
              /*allocator=*/nullptr,
              into_new_block && !*into_new_block ? into_new_block : nullptr))
        break;
      curr_entry = state.cursor.get_entry();
    } while (curr_entry.is_valid());

    return has_conflict;
  }

 private:
  /**
   * Check if [first_vidx, last_vidx] has any overlap with [le_first_vidx,
   * le_first_vidx + num_blocks - 1]; populate overlapped mapping if any
   * "first/last" here means inclusive range "[first, last]"; "begin/end" means
   * the range excluding the end "[begin, end)"
   *
   * @param[in] first_vidx the virtual index range (first) to check
   * @param[in] last_vidx the virtual index range (last) to check
   * @param[in] le_first_vidx the virtual range begin in log entry
   * @param[in] le_begin_lidx the logical range begin in log entry
   * @param[in] num_blocks number of blocks in the log entry mapping
   * @param[out] conflict_image return overlapped mapping; if no overlapping,
   * the corresponding array element is guaranteed untouched
   * @return whether this is any overlap
   */
  static bool get_conflict_image(VirtualBlockIdx first_vidx,
                                 VirtualBlockIdx last_vidx,
                                 VirtualBlockIdx le_first_vidx,
                                 LogicalBlockIdx le_begin_lidx,
                                 uint32_t num_blocks,
                                 std::vector<LogicalBlockIdx>& conflict_image) {
    VirtualBlockIdx le_last_vidx = le_first_vidx + num_blocks - 1;
    if (last_vidx < le_first_vidx || first_vidx > le_last_vidx) return false;

    VirtualBlockIdx overlap_first_vidx = std::max(le_first_vidx, first_vidx);
    VirtualBlockIdx overlap_last_vidx = std::min(le_last_vidx, last_vidx);

    for (VirtualBlockIdx vidx = overlap_first_vidx; vidx <= overlap_last_vidx;
         ++vidx) {
      auto offset = vidx - first_vidx;
      conflict_image[offset] = le_begin_lidx + offset;
    }
    return true;
  }
};
}  // namespace madfs::dram
