#include "write.h"

namespace ulayfs::dram {
class CoWTx : public WriteTx {
 protected:
  // the index of the first virtual block that needs to be copied entirely
  const VirtualBlockIdx begin_full_vidx;

  // the index of the last virtual block that needs to be copied entirely
  const VirtualBlockIdx end_full_vidx;

  // full blocks are blocks that can be written from buf directly without
  // copying the src data
  const size_t num_full_blocks;

  CoWTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count, size_t offset)
      : WriteTx(file, tx_mgr, buf, count, offset),
        begin_full_vidx(BLOCK_SIZE_TO_IDX(ALIGN_UP(offset, BLOCK_SIZE))),
        end_full_vidx(BLOCK_SIZE_TO_IDX(end_offset)),
        num_full_blocks(end_full_vidx - begin_full_vidx) {}
  CoWTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count, size_t offset,
        FileState state, uint64_t ticket)
      : WriteTx(file, tx_mgr, buf, count, offset, state, ticket),
        begin_full_vidx(BLOCK_SIZE_TO_IDX(ALIGN_UP(offset, BLOCK_SIZE))),
        end_full_vidx(BLOCK_SIZE_TO_IDX(end_offset)),
        num_full_blocks(end_full_vidx - begin_full_vidx) {}
};

class SingleBlockTx : public CoWTx {
 private:
  // the starting offset within the block
  const size_t local_offset;

 public:
  SingleBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
                size_t offset)
      : CoWTx(file, tx_mgr, buf, count, offset),
        local_offset(offset - BLOCK_IDX_TO_SIZE(begin_vidx)) {
    assert(num_blocks == 1);
  }

  SingleBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
                size_t offset, FileState state, uint64_t ticket)
      : CoWTx(file, tx_mgr, buf, count, offset, state, ticket),
        local_offset(offset - BLOCK_IDX_TO_SIZE(begin_vidx)) {
    assert(num_blocks == 1);
  }

  ssize_t exec() {
    timer.count<Event::SINGLE_BLOCK_TX_START>();
    bool need_redo;

    // must acquire the tx tail before any get
    if (!is_offset_depend) file->update(&state, /*do_alloc*/ true);

    prepare_commit_entry();

    recycle_image[0] = file->vidx_to_lidx(begin_vidx);
    assert(recycle_image[0] != dst_lidxs[0]);

    // copy data from buf
    pmem::memcpy_persist(dst_blocks[0]->data_rw() + local_offset, buf, count);

  redo:
    assert(dst_blocks.size() == 1);

    // copy original data
    {
      TimerGuard<Event::SINGLE_BLOCK_TX_COPY> timer_guard;

      char* dst_block = dst_blocks[0]->data_rw();
      const char* src_block =
          file->lidx_to_addr_ro(recycle_image[0])->data_ro();

      // copy the left part of the block
      if (local_offset != 0) {
        pmem::memcpy_persist(dst_block, src_block, local_offset);
      }

      // copy the right part of the block
      if (size_t len = BLOCK_SIZE - (local_offset + count); len != 0) {
        char* dst = dst_block + local_offset + count;
        const char* src = src_block + local_offset + count;
        pmem::memcpy_persist(dst, src, len);
      }
    }

    if (is_offset_depend) tx_mgr->offset_mgr.wait_offset(ticket);

  retry:
    if constexpr (BuildOptions::cc_occ) {
      timer.count<Event::SINGLE_BLOCK_TX_COMMIT>();
      // try to commit the tx entry
      pmem::TxEntry conflict_entry =
          tx_mgr->try_commit(commit_entry, &state.cursor);
      if (!conflict_entry.is_valid()) goto done;  // success, no conflict

      // we just treat begin_vidx as both first and last vidx
      need_redo = handle_conflict(conflict_entry, begin_vidx, begin_vidx,
                                  recycle_image);
      recheck_commit_entry();
      if (!need_redo)
        goto retry;
      else
        goto redo;
    } else {
      tx_mgr->try_commit(commit_entry, &state.cursor);
    }

  done:
    allocator->free(recycle_image[0]);  // it has only single block
    return static_cast<ssize_t>(count);
  }
};

class MultiBlockTx : public CoWTx {
 private:
  // number of bytes to be written in the beginning.
  // If the offset is 4097, then this var should be 4095.
  const size_t first_block_overlap_size;

  // number of bytes to be written for the last block
  // If the end_offset is 4097, then this var should be 1.
  const size_t last_block_overlap_size;

 public:
  MultiBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
               size_t offset)
      : CoWTx(file, tx_mgr, buf, count, offset),
        first_block_overlap_size(ALIGN_UP(offset, BLOCK_SIZE) - offset),
        last_block_overlap_size(end_offset -
                                ALIGN_DOWN(end_offset, BLOCK_SIZE)) {}
  MultiBlockTx(File* file, TxMgr* tx_mgr, const char* buf, size_t count,
               size_t offset, FileState state, uint64_t ticket)
      : CoWTx(file, tx_mgr, buf, count, offset, state, ticket),
        first_block_overlap_size(ALIGN_UP(offset, BLOCK_SIZE) - offset),
        last_block_overlap_size(end_offset -
                                ALIGN_DOWN(end_offset, BLOCK_SIZE)) {}
  ssize_t exec() {
    timer.count<Event::MULTI_BLOCK_TX_START>();
    // if need_copy_first/last is false, this means it is handled by the full
    // block copy and never need redo
    const bool need_copy_first = begin_full_vidx != begin_vidx;
    const bool need_copy_last = end_full_vidx != end_vidx;
    // do_copy_first/last indicates do we actually need to do copy; in the case
    // of redo, we may skip if no change is made
    bool do_copy_first = true;
    bool do_copy_last = true;
    bool need_redo;
    pmem::TxEntry conflict_entry;
    LogicalBlockIdx src_first_lidx, src_last_lidx;

    // copy full blocks first
    if (num_full_blocks > 0) {
      const char* rest_buf = buf;
      size_t rest_full_count = BLOCK_NUM_TO_SIZE(num_full_blocks);
      for (size_t i = 0; i < dst_blocks.size(); ++i) {
        // get logical block pointer for this iter
        // first block in first chunk could start from partial
        pmem::Block* full_blocks = dst_blocks[i];
        if (i == 0) {
          full_blocks += (begin_full_vidx - begin_vidx);
          rest_buf += first_block_overlap_size;
        }
        // calculate num of full block bytes to be copied in this iter
        // takes care of last block in last chunk which might be partial
        size_t num_bytes = rest_full_count;
        if (dst_blocks.size() > 1) {
          if (i == 0 && need_copy_first)
            num_bytes = BITMAP_ENTRY_BYTES_CAPACITY - BLOCK_SIZE;
          else if (i < dst_blocks.size() - 1)
            num_bytes = BITMAP_ENTRY_BYTES_CAPACITY;
        }
        // actual memcpy
        pmem::memcpy_persist(full_blocks->data_rw(), rest_buf, num_bytes);
        rest_buf += num_bytes;
        rest_full_count -= num_bytes;
      }
    }

    // only get a snapshot of the tail when starting critical piece
    if (!is_offset_depend) file->update(&state, /*do_alloc*/ true);

    prepare_commit_entry();

    for (uint32_t i = 0; i < num_blocks; ++i)
      recycle_image[i] = file->vidx_to_lidx(begin_vidx + i);
    src_first_lidx = recycle_image[0];
    src_last_lidx = recycle_image[num_blocks - 1];

    // write data from the buf to the first block
    {
      char* dst =
          dst_blocks[0]->data_rw() + BLOCK_SIZE - first_block_overlap_size;
      pmem::memcpy_persist(dst, buf, first_block_overlap_size);
    }

    // write data from the buf to the last block
    pmem::Block* last_dst_block =
        dst_blocks.back() + (end_full_vidx - begin_vidx) -
        BITMAP_ENTRY_BLOCKS_CAPACITY * (dst_blocks.size() - 1);
    const char* buf_src = buf + (count - last_block_overlap_size);
    pmem::memcpy_persist(last_dst_block->data_rw(), buf_src,
                         last_block_overlap_size);

  redo:
    timer.count<Event::MULTI_BLOCK_TX_COPY>();
    // copy the data from the first source block if exists
    if (need_copy_first && do_copy_first) {
      char* dst = dst_blocks[0]->data_rw();
      const char* src = file->lidx_to_addr_ro(src_first_lidx)->data_ro();
      size_t size = BLOCK_SIZE - first_block_overlap_size;
      pmem::memcpy_persist(dst, src, size);
    }

    // copy the data from the last source block if exits
    if (need_copy_last && do_copy_last) {
      char* dst = last_dst_block->data_rw() + last_block_overlap_size;
      const char* src = file->lidx_to_addr_ro(src_last_lidx)->data_ro() +
                        last_block_overlap_size;
      size_t size = BLOCK_SIZE - last_block_overlap_size;
      pmem::memcpy_persist(dst, src, size);
    }
    fence();

    if (is_offset_depend) tx_mgr->offset_mgr.wait_offset(ticket);

  retry:
    if constexpr (BuildOptions::cc_occ) {
      timer.count<Event::MULTI_BLOCK_TX_COMMIT>();
      // try to commit the transaction
      conflict_entry = tx_mgr->try_commit(commit_entry, &state.cursor);
      if (!conflict_entry.is_valid()) goto done;  // success
      // make a copy of the first and last again
      src_first_lidx = recycle_image[0];
      src_last_lidx = recycle_image[num_blocks - 1];
      need_redo = handle_conflict(conflict_entry, begin_vidx, end_full_vidx,
                                  recycle_image);
      recheck_commit_entry();
      if (!need_redo)
        goto retry;  // we have moved to the new tail, retry commit
      else {
        do_copy_first = src_first_lidx != recycle_image[0];
        do_copy_last = src_last_lidx != recycle_image[num_blocks - 1];
        if (do_copy_first || do_copy_last)
          goto redo;
        else
          goto retry;
      }
    } else {
      tx_mgr->try_commit(commit_entry, &state.cursor);
    }

  done:
    // recycle the data blocks being overwritten
    allocator->free(recycle_image);
    return static_cast<ssize_t>(count);
  }
};

}  // namespace ulayfs::dram
