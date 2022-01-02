#include <cstring>

#include "alloc.h"
#include "bitmap.h"
#include "block.h"
#include "entry.h"
#include "file.h"
#include "flock.h"
#include "idx.h"
#include "params.h"
#include "posix.h"
#include "utils.h"

namespace ulayfs::utility {

class Transformer {
  // transform a normal file to a uLayFS file
  // fd must be opened with both read and write permission
  static dram::File* transform_to(int fd) {
    if (!flock::try_acquire(fd)) {
      WARN("Target file locked, cannot perform transformation");
      return nullptr;
    }

    int ret;
    struct stat stat_buf;
    ret = posix::fstat(fd, &stat_buf);
    PANIC_IF(ret, "Fail to fstat");

    if (stat_buf.st_size == 0) {
      dram::File* file = new dram::File(fd, stat_buf, O_RDWR, false);
      // release exclusive lock and acquire shared lock
      flock::release(fd);
      flock::flock_guard(fd);
      return file;
    }

    uint64_t block_align_size = ALIGN_UP(stat_buf.st_size, BLOCK_SIZE);
    uint16_t leftover_bytes = block_align_size - stat_buf.st_size;
    uint32_t num_blocks = BLOCK_SIZE_TO_IDX(block_align_size);

    // we expect size to change as below:
    stat_buf.st_size = ALIGN_UP(block_align_size + BLOCK_SIZE, GROW_UNIT_SIZE);
    ret = posix::fallocate(fd, 0, 0, static_cast<off_t>(stat_buf.st_size));
    PANIC_IF(ret, "Fail to fallocate");

    char block_buf[BLOCK_SIZE];
    ret = posix::pread(fd, block_buf, BLOCK_SIZE, 0);
    PANIC_IF(ret, "Fail to pread");
    posix::pwrite(fd, block_buf, BLOCK_SIZE, block_align_size);

    pmem::MetaBlock* meta = static_cast<pmem::MetaBlock*>(
        posix::mmap(nullptr, BLOCK_SIZE, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, 0));
    PANIC_IF(meta == MAP_FAILED, "Fail to mmap");

    memset(meta, 0, BLOCK_SIZE);
    meta->init();
    posix::munmap(meta, BLOCK_SIZE);

    // first mark all these blocks as used, so that they won't be occupied by
    // allocator when preparing log entries
    dram::File* file = new dram::File(fd, stat_buf, O_RDWR, false);
    dram::Allocator* allocator = file->get_local_allocator();
    allocator->return_free_list();
    uint32_t num_bitmaps_full = (num_blocks + 1) >> BITMAP_CAPACITY_SHIFT;
    uint32_t num_bits_left = (num_blocks + 1) % BITMAP_CAPACITY;
    for (uint32_t i = 0; i < num_bitmaps_full; ++i)
      file->bitmap[i].set_allocated_all();
    for (uint32_t i = 0; i < num_bits_left; ++i)
      file->bitmap[num_bitmaps_full].set_allocated(i);

    TxEntryIdx tx_idx{0, 0};
    pmem::TxBlock* tx_block = nullptr;
    bool need_le_block = false;

    // handle special case with only one block
    if (num_blocks == 1) {
      if (leftover_bytes == 0)
        file->tx_mgr.try_commit(
            pmem::TxEntryInline(/*num_blocks*/ 1, /*begin_vidx*/ 0,
                                /*begin_lidx*/ 1),
            tx_idx, tx_block);
      else {
        auto log_entry_idx = file->log_mgr.append(
            allocator, pmem::LogOp::LOG_OVERWRITE, leftover_bytes,
            /*total_blocks*/ 1, /*begin_vidx*/ 0, /*begin_lidxs*/ {1},
            /*fenced*/ false);
        file->tx_mgr.try_commit(pmem::TxEntryIndirect(1, 0, log_entry_idx),
                                tx_idx, tx_block);
      }
      goto done;
    }

    // can we inline the mapping of the first virtual block?
    if (pmem::TxEntryInline::can_inline(/*num_blocks*/ 1, /*begin_vidx*/ 0,
                                        /*begin_lidx*/ num_blocks)) {
      file->tx_mgr.try_commit(pmem::TxEntryInline(1, 0, num_blocks), tx_idx,
                              tx_block);
    } else {
      need_le_block = true;
      auto log_entry_idx = file->log_mgr.append(
          allocator, pmem::LogOp::LOG_OVERWRITE, /*leftover_bytes*/ 0,
          /*total_blocks*/ 1, /*begin_vidx*/ 0, /*begin_lidxs*/ {num_blocks},
          /*fenced*/ false);
      file->tx_mgr.try_commit(pmem::TxEntryIndirect(1, 0, log_entry_idx),
                              tx_idx, tx_block);
    }

    // if it's not full block or MetaBlock does not have the capacity, we still
    // need LogEntryBlock
    need_le_block |=
        leftover_bytes != 0 |
        (num_blocks - 1) > ((NUM_INLINE_TX_ENTRY - 1) << BITMAP_CAPACITY_SHIFT);

    if (!need_le_block) {
      for (VirtualBlockIdx begin_vidx = 1; begin_vidx < num_blocks;
           begin_vidx += BITMAP_CAPACITY) {
        uint32_t len = std::min(num_blocks - begin_vidx, BITMAP_CAPACITY);
        file->tx_mgr.try_commit(
            pmem::TxEntryInline(len, begin_vidx, begin_vidx), tx_idx, tx_block);
      }
    } else {
      auto log_entry_idx = file->log_mgr.append(
          allocator, pmem::LogOp::LOG_OVERWRITE, leftover_bytes,
          /*total_blocks*/ num_blocks - 1, /*begin_vidx*/ 1,
          /*begin_lidxs*/ {1}, /*fenced*/ false);
      file->tx_mgr.try_commit(
          pmem::TxEntryIndirect(num_blocks - 1, 1, log_entry_idx), tx_idx,
          tx_block);
    }

  done:
    _mm_sfence();
    return file;
  }

  // transform a uLayFS file to a normal file
  static void transform_from(dram::File* file) {}
};
}  // namespace ulayfs::utility
