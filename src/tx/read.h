#pragma once

#include "tx.h"

namespace ulayfs::dram {
class ReadTx : public Tx {
  char* const buf;

 public:
  ReadTx(const TxArgs& tx_args, char* buf) : Tx(tx_args, true), buf(buf) {
    lock->rdlock();  // nop lock is used by default
  }

  ssize_t exec() {
    timer.stop<Event::READ_TX_CTOR>();

    size_t first_block_offset = offset & (BLOCK_SIZE - 1);
    size_t first_block_size = BLOCK_SIZE - first_block_offset;
    if (first_block_size > count) first_block_size = count;

    std::vector<LogicalBlockIdx>& redo_image = local_buf_image_lidxs;
    redo_image.clear();
    redo_image.resize(num_blocks, 0);

    {
      TimerGuard<Event::READ_TX_UPDATE> timer_guard;
      if (!is_offset_depend) blk_table->update(&state);
    }

    // reach EOF
    if (offset >= state.file_size) {
      count = 0;
      goto done;
    }
    if (offset + count > state.file_size) {  // partial read; recalculate end_*
      count = state.file_size - offset;
      end_offset = offset + count;
      end_vidx = BLOCK_SIZE_TO_IDX(ALIGN_UP(end_offset, BLOCK_SIZE));
    }

    // copy the blocks
    {
      TimerGuard<Event::READ_TX_COPY> timer_guard;

      const char* addr =
          mem_table->lidx_to_addr_ro(blk_table->vidx_to_lidx(begin_vidx))
              ->data_ro();
      addr += first_block_offset;
      size_t contiguous_bytes = first_block_size;
      size_t buf_offset = 0;

      for (VirtualBlockIdx vidx = begin_vidx + 1; vidx < end_vidx; ++vidx) {
        const pmem::Block* curr_block =
            mem_table->lidx_to_addr_ro(blk_table->vidx_to_lidx(vidx));
        if (addr + contiguous_bytes == curr_block->data_ro()) {
          contiguous_bytes += BLOCK_SIZE;
          continue;
        }
        dram::memcpy(buf + buf_offset, addr, contiguous_bytes);
        buf_offset += contiguous_bytes;
        contiguous_bytes = BLOCK_SIZE;
        addr = curr_block->data_ro();
      }
      dram::memcpy(buf + buf_offset, addr,
                   std::min(contiguous_bytes, count - buf_offset));
    }

  redo:
    timer.start<Event::READ_TX_VALIDATE>();
    while (true) {
      // check the tail is still tail
      if (bool success = state.cursor.handle_overflow(mem_table); !success) {
        break;
      }
      pmem::TxEntry curr_entry = state.cursor.get_entry();
      if (!curr_entry.is_valid()) break;

      // then scan the log and build redo_image; if no redo needed, we are done
      if (!handle_conflict(curr_entry, begin_vidx, end_vidx - 1, redo_image))
        break;

      // redo:
      LogicalBlockIdx redo_lidx;

      // first handle the first block (which might not be full block)
      redo_lidx = redo_image[0];
      if (redo_lidx != 0) {
        const pmem::Block* curr_block = mem_table->lidx_to_addr_ro(redo_lidx);
        dram::memcpy(buf, curr_block->data_ro() + first_block_offset,
                     first_block_size);
        redo_image[0] = 0;
      }
      size_t buf_offset = first_block_size;

      // then handle middle full blocks (which might not exist)
      VirtualBlockIdx curr_vidx;
      for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
        redo_lidx = redo_image[curr_vidx - begin_vidx];
        if (redo_lidx != 0) {
          const pmem::Block* curr_block = mem_table->lidx_to_addr_ro(redo_lidx);
          dram::memcpy(buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
          redo_image[curr_vidx - begin_vidx] = 0;
        }
        buf_offset += BLOCK_SIZE;
      }

      // last handle the last block (which might not be full block)
      if (begin_vidx != end_vidx - 1) {
        redo_lidx = redo_image[curr_vidx - begin_vidx];
        if (redo_lidx != 0) {
          const pmem::Block* curr_block = mem_table->lidx_to_addr_ro(redo_lidx);
          dram::memcpy(buf + buf_offset, curr_block->data_ro(),
                       count - buf_offset);
          redo_image[curr_vidx - begin_vidx] = 0;
        }
      }
    }

    // we actually don't care what's the previous tx's tail, because we will
    // need to validate against the latest tail anyway
    if (is_offset_depend) {
      if (!offset_mgr->validate(ticket, state.cursor)) {
        // we don't need to revalidate after redo
        is_offset_depend = false;
        goto redo;
      }
    }

    timer.stop<Event::READ_TX_VALIDATE>();

  done:
    allocator->tx_block.pin(state.get_tx_block_idx());
    return static_cast<ssize_t>(count);
  }
};
}  // namespace ulayfs::dram
