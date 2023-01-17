#include <cstring>

#include "alloc/alloc.h"
#include "bitmap.h"
#include "block/block.h"
#include "const.h"
#include "entry.h"
#include "file/file.h"
#include "idx.h"
#include "posix.h"
#include "utils/persist.h"
#include "utils/utils.h"

namespace madfs::utility {

class Converter {
 public:
  // convert a normal file to a MadFS file
  // fd must be opened with both read and write permission
  static dram::File* convert_to(int fd, const char* pathname) {
    if (!try_acquire_flock(fd)) {
      LOG_WARN("Target file locked, cannot perform conversion");
      return nullptr;
    }

    int ret;
    struct stat stat_buf;
    ret = posix::fstat(fd, &stat_buf);
    PANIC_IF(ret, "Fail to fstat");

    if (stat_buf.st_size == 0) {
      dram::File* file = new dram::File(fd, stat_buf, O_RDWR, pathname);
      // release exclusive lock and acquire shared lock
      release_flock(fd);
      flock_guard(fd);
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
    PANIC_IF(ret != BLOCK_SIZE, "Fail to pread");
    ret = posix::pwrite(fd, block_buf, BLOCK_SIZE, block_align_size);
    PANIC_IF(ret != BLOCK_SIZE, "Fail to pwrite");

    void* addr = posix::mmap(nullptr, BLOCK_SIZE, PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_POPULATE, fd, 0);
    PANIC_IF(addr == MAP_FAILED, "Fail to mmap");
    memset(addr, 0, BLOCK_SIZE);
    pmem::MetaBlock* meta = static_cast<pmem::MetaBlock*>(addr);

    meta->init();
    posix::munmap(meta, BLOCK_SIZE);

    // first mark all these blocks as used, so that they won't be occupied by
    // allocator when preparing log entries
    dram::File* file = new dram::File(fd, stat_buf, O_RDWR, pathname);
    dram::Allocator* allocator = file->get_local_allocator();
    allocator->block.return_free_list();
    uint32_t num_bitmaps_full =
        (num_blocks + 1) >> BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT;
    uint32_t num_bits_left = (num_blocks + 1) % BITMAP_ENTRY_BLOCKS_CAPACITY;
    for (uint32_t i = 0; i < num_bitmaps_full; ++i)
      file->bitmap_mgr.entries[i].set_allocated_all();
    for (uint32_t i = 0; i < num_bits_left; ++i)
      file->bitmap_mgr.entries[num_bitmaps_full].set_allocated(i);

    dram::MemTable* mem_table = &file->mem_table;
    dram::TxCursor tx_cursor{};
    bool need_le_block = false;

    // handle special case with only one block
    if (num_blocks == 1) {
      if (leftover_bytes == 0)
        tx_cursor.try_commit(
            pmem::TxEntryInline(/*num_blocks*/ 1, /*begin_vidx*/ 0,
                                /*begin_lidx*/ 1),
            mem_table, allocator);
      else {
        dram::LogCursor log_cursor = allocator->log_entry.append(
            pmem::LogEntry::Op::LOG_OVERWRITE, leftover_bytes,
            /*total_blocks*/ 1, /*begin_vidx*/ 0, /*begin_lidxs*/ {1});
        tx_cursor.try_commit(pmem::TxEntryIndirect(log_cursor.idx), mem_table,
                             allocator);
      }
      goto done;
    }

    // can we inline the mapping of the first virtual block?
    if (pmem::TxEntryInline::can_inline(/*num_blocks*/ 1, /*begin_vidx*/ 0,
                                        /*begin_lidx*/ num_blocks)) {
      tx_cursor.try_commit(pmem::TxEntryInline(1, 0, num_blocks), mem_table,
                           allocator);
    } else {
      need_le_block = true;
      dram::LogCursor log_cursor = allocator->log_entry.append(
          pmem::LogEntry::Op::LOG_OVERWRITE, /*leftover_bytes*/ 0,
          /*total_blocks*/ 1, /*begin_vidx*/ 0, /*begin_lidxs*/ {num_blocks});
      tx_cursor.try_commit(pmem::TxEntryIndirect(log_cursor.idx), mem_table,
                           allocator);
    }
    tx_cursor.advance(mem_table, allocator);

    // if it's not full block or MetaBlock does not have the capacity, we still
    // need LogEntryBlock
    need_le_block |=
        (leftover_bytes != 0) |
        ((num_blocks - 1) >
         ((NUM_INLINE_TX_ENTRY - 1) << BITMAP_ENTRY_BLOCKS_CAPACITY_SHIFT));

    if (!need_le_block) {
      for (VirtualBlockIdx begin_vidx = 1; begin_vidx < num_blocks;
           begin_vidx += BITMAP_ENTRY_BLOCKS_CAPACITY) {
        uint32_t len = std::min(num_blocks - begin_vidx.get(),
                                BITMAP_ENTRY_BLOCKS_CAPACITY);
        tx_cursor.try_commit(
            pmem::TxEntryInline(len, begin_vidx,
                                LogicalBlockIdx(begin_vidx.get())),
            mem_table, allocator);
        tx_cursor.advance(mem_table, allocator);
      }
    } else {
      dram::LogCursor log_cursor = allocator->log_entry.append(
          pmem::LogEntry::Op::LOG_OVERWRITE, leftover_bytes,
          /*total_blocks*/ num_blocks - 1, /*begin_vidx*/ 1,
          /*begin_lidxs*/ {1});
      tx_cursor.try_commit(pmem::TxEntryIndirect(log_cursor.idx), mem_table,
                           allocator);
    }

  done:
    pmem::persist_fenced(file->meta, BLOCK_SIZE);
    release_flock(fd);
    flock_guard(fd);
    return file;
  }

  static int convert_from(dram::File* file) {
    int ret;
    int fd = file->fd;
    release_flock(fd);
    if (!try_acquire_flock(file->fd)) {
      LOG_WARN("Target file locked, cannot perform conversion");
      flock_guard(fd);
      return -1;
    }

    uint64_t virtual_size = file->blk_table.update_unsafe();
    uint64_t virtual_size_aligned = ALIGN_UP(virtual_size, BLOCK_SIZE);
    uint32_t virtual_num_blocks =
        BLOCK_SIZE_TO_IDX(ALIGN_UP(virtual_size_aligned, BLOCK_SIZE));

    uint32_t logical_num_blocks = file->meta->get_num_logical_blocks();
    LogicalBlockIdx new_begin_lidx =
        ALIGN_UP(logical_num_blocks + 1, NUM_BLOCKS_PER_GROW);
    ret = posix::fallocate(fd, 0, BLOCK_IDX_TO_SIZE(new_begin_lidx),
                           virtual_size_aligned);
    PANIC_IF(ret, "Fail to fallocate the new region");

    // map new region
    pmem::Block* new_region = static_cast<pmem::Block*>(posix::mmap(
        nullptr, virtual_size_aligned, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_POPULATE, fd, BLOCK_IDX_TO_SIZE(new_begin_lidx)));
    PANIC_IF(new_region == MAP_FAILED, "Fail to mmap the new region");

    // copy data to the new region
    for (VirtualBlockIdx vidx = 0; vidx < virtual_num_blocks; ++vidx)
      pmem::memcpy_persist(
          new_region[vidx.get()].data_rw(),
          file->mem_table.lidx_to_addr_ro(file->blk_table.vidx_to_lidx(vidx))
              ->data_ro(),
          BLOCK_SIZE);

    fence();

    // unmap the new region
    ret = posix::munmap(new_region, virtual_size_aligned);
    PANIC_IF(ret, "Fail to munmap the new region");

    // destroy everything about MadFS on pmem
    ret = posix::fallocate(fd, FALLOC_FL_COLLAPSE_RANGE, 0,
                           BLOCK_IDX_TO_SIZE(new_begin_lidx));
    PANIC_IF(ret, "Fail to fallocate collapse the old region");

    if (virtual_size != virtual_size_aligned) {
      ret = posix::ftruncate(fd, virtual_size);
      PANIC_IF(ret, "Fail to ftruncate to the right size");
    }

    // we steal fd here so it won't be destroyed with File
    file->fd = -1;
    release_flock(fd);
    return fd;
  }
};
}  // namespace madfs::utility
