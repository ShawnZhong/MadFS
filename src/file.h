#pragma once

#include <iostream>
#include <stdexcept>

#include "alloc.h"
#include "block.h"
#include "btable.h"
#include "config.h"
#include "entry.h"
#include "layout.h"
#include "log.h"
#include "mtable.h"
#include "posix.h"
#include "tx.h"
#include "utils.h"

// data structure under this namespace must be in volatile memory (DRAM)
namespace ulayfs::dram {

class File {
  int fd;
  int open_flags;
  bool valid;

  pmem::MetaBlock* meta;
  Allocator allocator;
  MemTable mem_table;
  BlkTable blk_table;
  LogMgr log_mgr;
  TxMgr tx_mgr;

 private:
  /**
   * @param virtual_block_idx the virtual block index for a data block
   * @return the char pointer pointing to the memory location of the data block
   */
  char* get_data_block_ptr(VirtualBlockIdx virtual_block_idx) {
    auto logical_block_idx = blk_table.get(virtual_block_idx);
    assert(logical_block_idx != 0);
    auto block = mem_table.get_addr(logical_block_idx);
    return block->data;
  }

 public:
  File(const char* pathname, int flags, mode_t mode)
      : open_flags(flags), valid(false) {
    fd = posix::open(pathname, flags, mode);
    if (fd < 0) return;  // fail to open the file

    // TODO: support read-only / write-only files
    if ((open_flags & O_ACCMODE) != O_RDWR) {
      WARN("File %s opened with %s", pathname,
           (open_flags & O_ACCMODE) == O_RDONLY ? "O_RDONLY" : "O_WRONLY");
      return;
    }

    struct stat stat_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    int ret = posix::fstat(fd, &stat_buf);
    PANIC_IF(ret, "fstat failed");

    // we don't handle non-normal file (e.g., socket, directory, block dev)
    if (!S_ISREG(stat_buf.st_mode) && !S_ISLNK(stat_buf.st_mode)) return;

    if (!IS_ALIGNED(stat_buf.st_size, BLOCK_SIZE)) {
      WARN("File size not aligned for \"%s\". Fall back to syscall", pathname);
      return;
    }

    mem_table = MemTable(fd, stat_buf.st_size);
    meta = mem_table.get_meta();
    allocator = Allocator(fd, meta, &mem_table);
    log_mgr = LogMgr(meta, &allocator, &mem_table);
    tx_mgr = TxMgr(meta, &allocator, &mem_table);
    blk_table = BlkTable(meta, &mem_table, &log_mgr, &tx_mgr);
    blk_table.update();

    if (stat_buf.st_size == 0) meta->init();

    valid = true;
  }

  [[nodiscard]] bool is_valid() const { return valid; }
  [[nodiscard]] pmem::MetaBlock* get_meta() { return meta; }
  [[nodiscard]] int get_fd() const { return fd; }

  /**
   * write the content in buf to the byte range [offset, offset + count)
   */
  ssize_t pwrite(const void* buf, size_t count, size_t offset) {
    VirtualBlockIdx begin_virtual_idx = ALIGN_DOWN(offset, BLOCK_SIZE);

    uint64_t local_offset = offset - begin_virtual_idx * BLOCK_SIZE;
    uint32_t num_blocks =
        ALIGN_UP(count + local_offset, BLOCK_SIZE) >> BLOCK_SHIFT;

    auto tx_begin_idx = tx_mgr.begin_tx(begin_virtual_idx, num_blocks);

    // TODO: handle the case where num_blocks > 64

    LogicalBlockIdx begin_dst_idx = allocator.alloc(num_blocks);
    LogicalBlockIdx begin_src_idx = blk_table.get(begin_virtual_idx);
    tx_mgr.copy_data(buf, count, local_offset, begin_dst_idx, begin_src_idx);

    uint16_t last_remaining = num_blocks * BLOCK_SIZE - count - local_offset;
    auto log_entry_idx =
        log_mgr.append(pmem::LogOp::LOG_OVERWRITE, begin_virtual_idx,
                       begin_dst_idx, num_blocks, last_remaining);
    tx_mgr.commit_tx(tx_begin_idx, log_entry_idx);
    blk_table.update();

    return static_cast<ssize_t>(count);
  }

  /**
   * read the byte range [offset, offset + count) to buf
   */
  ssize_t pread(void* buf, size_t count, off_t offset) {
    VirtualBlockIdx begin_virtual_idx = ALIGN_DOWN(offset, BLOCK_SIZE);

    uint64_t local_offset = offset - begin_virtual_idx * BLOCK_SIZE;
    uint32_t num_blocks =
        ALIGN_UP(count + local_offset, BLOCK_SIZE) >> BLOCK_SHIFT;
    uint16_t last_remaining = num_blocks * BLOCK_SIZE - count - local_offset;

    char* dst = static_cast<char*>(buf);
    for (size_t i = 0; i < num_blocks; ++i) {
      size_t num_bytes = BLOCK_SIZE;
      if (i == 0) num_bytes -= local_offset;
      if (i == num_blocks - 1) num_bytes -= last_remaining;

      char* ptr = get_data_block_ptr(begin_virtual_idx + i);
      char* src = i == 0 ? ptr + local_offset : ptr;

      memcpy(dst, src, num_bytes);
      dst += num_bytes;
    }

    return static_cast<ssize_t>(count);
  }

  friend std::ostream& operator<<(std::ostream& out, const File& f) {
    out << "File: fd = " << f.fd << "\n";
    out << *f.meta;
    out << f.mem_table;
    out << f.tx_mgr;
    out << f.blk_table;
    out << "\n";

    return out;
  }
};

}  // namespace ulayfs::dram
