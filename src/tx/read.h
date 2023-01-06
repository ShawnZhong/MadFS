#pragma once

#include "tx.h"

namespace ulayfs::dram {
class ReadTx : public Tx {
 public:
  ReadTx(TxArg& args) : Tx(args) {}

  ssize_t exec() {
    timer.stop<Event::READ_TX_CTOR>();

    size_t first_block_offset = arg.offset & (BLOCK_SIZE - 1);
    size_t first_block_size = BLOCK_SIZE - first_block_offset;
    if (first_block_size > arg.count) first_block_size = arg.count;

    std::vector<LogicalBlockIdx>& redo_image = local_buf_image_lidxs;
    redo_image.clear();
    redo_image.resize(num_blocks, 0);

    {
      TimerGuard<Event::READ_TX_UPDATE> timer_guard;
      if (!arg.is_offset_depend) arg.blk_table->update(&arg.state);
    }

    // reach EOF
    if (arg.offset >= arg.state.file_size) {
      arg.count = 0;
      goto done;
    }
    if (arg.offset + arg.count >
        arg.state.file_size) {  // partial read; recalculate end_*
      arg.count = arg.state.file_size - arg.offset;
      end_offset = arg.offset + arg.count;
      end_vidx = BLOCK_SIZE_TO_IDX(ALIGN_UP(end_offset, BLOCK_SIZE));
    }

    // copy the blocks
    {
      TimerGuard<Event::READ_TX_COPY> timer_guard;

      const char* addr =
          arg.mem_table
              ->lidx_to_addr_ro(arg.blk_table->vidx_to_lidx(begin_vidx))
              ->data_ro();
      addr += first_block_offset;
      size_t contiguous_bytes = first_block_size;
      size_t buf_offset = 0;

      for (VirtualBlockIdx vidx = begin_vidx + 1; vidx < end_vidx; ++vidx) {
        const pmem::Block* curr_block =
            arg.mem_table->lidx_to_addr_ro(arg.blk_table->vidx_to_lidx(vidx));
        if (addr + contiguous_bytes == curr_block->data_ro()) {
          contiguous_bytes += BLOCK_SIZE;
          continue;
        }
        dram::memcpy(arg.buf + buf_offset, addr, contiguous_bytes);
        buf_offset += contiguous_bytes;
        contiguous_bytes = BLOCK_SIZE;
        addr = curr_block->data_ro();
      }
      dram::memcpy(arg.buf + buf_offset, addr,
                   std::min(contiguous_bytes, arg.count - buf_offset));
    }

  redo:
    timer.start<Event::READ_TX_VALIDATE>();
    while (true) {
      // check the tail is still tail
      if (bool success = arg.state.cursor.handle_overflow(arg.mem_table);
          !success) {
        break;
      }
      pmem::TxEntry curr_entry = arg.state.cursor.get_entry();
      if (!curr_entry.is_valid()) break;

      // then scan the log and build redo_image; if no redo needed, we are done
      if (!handle_conflict(curr_entry, begin_vidx, end_vidx - 1, redo_image))
        break;

      // redo:
      LogicalBlockIdx redo_lidx;

      // first handle the first block (which might not be full block)
      redo_lidx = redo_image[0];
      if (redo_lidx != 0) {
        const pmem::Block* curr_block =
            arg.mem_table->lidx_to_addr_ro(redo_lidx);
        dram::memcpy(arg.buf, curr_block->data_ro() + first_block_offset,
                     first_block_size);
        redo_image[0] = 0;
      }
      size_t buf_offset = first_block_size;

      // then handle middle full blocks (which might not exist)
      VirtualBlockIdx curr_vidx;
      for (curr_vidx = begin_vidx + 1; curr_vidx < end_vidx - 1; ++curr_vidx) {
        redo_lidx = redo_image[curr_vidx - begin_vidx];
        if (redo_lidx != 0) {
          const pmem::Block* curr_block =
              arg.mem_table->lidx_to_addr_ro(redo_lidx);
          dram::memcpy(arg.buf + buf_offset, curr_block->data_ro(), BLOCK_SIZE);
          redo_image[curr_vidx - begin_vidx] = 0;
        }
        buf_offset += BLOCK_SIZE;
      }

      // last handle the last block (which might not be full block)
      if (begin_vidx != end_vidx - 1) {
        redo_lidx = redo_image[curr_vidx - begin_vidx];
        if (redo_lidx != 0) {
          const pmem::Block* curr_block =
              arg.mem_table->lidx_to_addr_ro(redo_lidx);
          dram::memcpy(arg.buf + buf_offset, curr_block->data_ro(),
                       arg.count - buf_offset);
          redo_image[curr_vidx - begin_vidx] = 0;
        }
      }
    }

    // we actually don't care what's the previous tx's tail, because we will
    // need to validate against the latest tail anyway
    if (arg.is_offset_depend) {
      if (!arg.offset_mgr->validate(arg.ticket, arg.state.cursor)) {
        // we don't need to revalidate after redo
        arg.is_offset_depend = false;
        goto redo;
      }
    }

    timer.stop<Event::READ_TX_VALIDATE>();

  done:
    arg.allocator->tx_block.pin(arg.state.get_tx_block_idx());
    return static_cast<ssize_t>(arg.count);
  }
};
}  // namespace ulayfs::dram
